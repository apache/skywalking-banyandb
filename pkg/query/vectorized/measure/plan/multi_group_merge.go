// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package plan

import (
	"cmp"
	"context"
	"fmt"
	"math"
	"slices"

	"google.golang.org/protobuf/proto"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
)

// MultiGroupMerge merges the per-group batch results of a local multi-group
// measure query. It derives the per-group request, so every group returns enough
// rows and the order-by column, runs the shared k-way row merger over the groups'
// batches, and exposes the merged batches before final row serialization.
//
// A result that keeps the series-id, timestamp and version columns (plain rows,
// Top and raw GroupBy) is merged by sort key with version dedup. A time-bucketed
// aggregate keeps only the timestamp, so it is merged by time without dedup. Any
// other Agg output has no ordering column. Its groups are dispatched in partial mode
// without Top, offset or limit and reduced across groups with the liaison's reducer;
// Top, offset and limit are then applied once to the reduced rows. A caller that
// itself emits partials (a distributed data node) opts out with ConcatAggregates, and
// its per-group results are concatenated in group order, as each group's plan already
// applied its own limit. COUNT_DISTINCT is not additive across groups, so it is always
// concatenated.
//
// Every batch the groups hand over, the reshaped copies and the merged output are
// charged to one memory tracker sized by the query memory budget.
type MultiGroupMerge struct {
	req       *measurev1.QueryRequest
	groupReq  *measurev1.QueryRequest
	orderBy   *resolvedOrderByTag
	hidden    logical.HiddenTagSet
	tracker   *vectorized.MemoryTracker
	cfg       vmeasure.VectorizedConfig
	offset    uint32
	limit     uint32
	desc      bool
	indexMode bool
	plainRows bool
	reduceAgg bool
}

// MultiGroupOption customizes NewMultiGroupMerge.
type MultiGroupOption func(*multiGroupOptions)

type multiGroupOptions struct {
	concatAgg bool
}

// ConcatAggregates keeps each group's aggregation result as is and concatenates them,
// for callers whose own result is itself a partial that is reduced elsewhere.
func ConcatAggregates() MultiGroupOption {
	return func(o *multiGroupOptions) { o.concatAgg = true }
}

// NewMultiGroupMerge resolves how the groups of req are merged. schemas and
// measureSchemas are the per-group logical and measure schemas in request order.
func NewMultiGroupMerge(
	ctx context.Context,
	req *measurev1.QueryRequest,
	schemas []logical.Schema,
	measureSchemas []*databasev1.Measure,
	cfg vmeasure.VectorizedConfig,
	opts ...MultiGroupOption,
) (*MultiGroupMerge, error) {
	var options multiGroupOptions
	for _, opt := range opts {
		opt(&options)
	}
	merge := &MultiGroupMerge{
		req:       req,
		groupReq:  req,
		cfg:       cfg,
		hidden:    logical.NewHiddenTagSet(),
		tracker:   vectorized.QueryMemoryTracker(ctx, int64(cfg.QueryMemoryMiB)*1024*1024),
		plainRows: req.GetGroupBy() == nil && req.GetAgg() == nil && req.GetTop() == nil,
		offset:    req.GetOffset(),
		limit:     req.GetLimit(),
		desc:      req.GetOrderBy().GetSort() == modelv1.Sort_SORT_DESC,
	}
	if req.GetAgg() != nil {
		if !options.concatAgg && req.GetAgg().GetFunction() != modelv1.AggregationFunction_AGGREGATION_FUNCTION_COUNT_DISTINCT {
			merge.reduceAgg = true
			groupReq := proto.Clone(req).(*measurev1.QueryRequest)
			// A group cannot prune: a key's winner is only known after the groups are reduced.
			groupReq.Top = nil
			groupReq.Offset = 0
			groupReq.Limit = math.MaxUint32
			merge.groupReq = groupReq
			merge.hideCriteriaOnlyKeys(req, schemas)
		}
		return merge, nil
	}
	merge.indexMode = len(measureSchemas) > 0
	for _, ms := range measureSchemas {
		if !ms.GetIndexMode() {
			merge.indexMode = false
			break
		}
	}
	groupReq := proto.Clone(req).(*measurev1.QueryRequest)
	merge.groupReq = groupReq
	if merge.plainRows {
		if merge.limit == 0 {
			merge.limit = defaultLimit
		}
		// A group can only return the first offset+limit rows of the merged order, and the
		// offset is applied once, after the merge.
		groupReq.Offset = 0
		groupReq.Limit = uint32(min(uint64(merge.offset)+uint64(merge.limit), math.MaxUint32))
	}
	if indexRule := req.GetOrderBy().GetIndexRuleName(); indexRule != "" {
		resolved, resolveErr := resolveMultiGroupOrderBy(schemas, measureSchemas, indexRule)
		if resolveErr != nil {
			return nil, resolveErr
		}
		merge.orderBy = &resolved
		if !orderByProjectionVisible(req.GetTagProjection(), resolved) {
			// The merge sorts on this column, so every group must project it; it is
			// stripped again after the merge.
			groupReq.TagProjection = appendOrderByToProjection(groupReq.GetTagProjection(), resolved)
			merge.hidden.Add(resolved.tag)
		}
	}
	return merge, nil
}

// resolveMultiGroupOrderBy finds the tag the index rule sorts on in the first
// group that defines the rule.
func resolveMultiGroupOrderBy(schemas []logical.Schema, measureSchemas []*databasev1.Measure, indexRuleName string) (resolvedOrderByTag, error) {
	for groupIdx, schema := range schemas {
		if schema == nil || groupIdx >= len(measureSchemas) {
			continue
		}
		defined, rule := schema.IndexRuleDefined(indexRuleName)
		if !defined {
			continue
		}
		return resolveOrderByTag(measureSchemas[groupIdx], []*databasev1.IndexRule{rule}, indexRuleName)
	}
	return resolvedOrderByTag{}, fmt.Errorf("index rule %s not found", indexRuleName)
}

// hideCriteriaOnlyKeys records the grouping keys that only the criteria names: they are projected for the reduce, and
// the response leaves them out, as the per-group plan's hidden criteria tags were.
func (m *MultiGroupMerge) hideCriteriaOnlyKeys(req *measurev1.QueryRequest, schemas []logical.Schema) {
	criteriaTags := make(map[string]struct{})
	logical.CollectCriteriaTagNames(req.GetCriteria(), criteriaTags)
	projected := projectedNames(req.GetTagProjection())
	for _, family := range req.GetGroupBy().GetTagProjection().GetTagFamilies() {
		for _, tag := range family.GetTags() {
			if _, named := criteriaTags[tag]; !named {
				continue
			}
			if _, visible := projected[tag]; visible {
				continue
			}
			for _, schema := range schemas {
				if schema != nil && !slices.Contains(schema.EntityList(), tag) {
					m.hidden.Add(tag)
					break
				}
			}
		}
	}
}

// GroupRequest returns the request every group is dispatched with.
func (m *MultiGroupMerge) GroupRequest() *measurev1.QueryRequest { return m.groupReq }

// ReducesAggregates reports whether the groups must be dispatched in partial mode, because
// MergeBatches reduces their aggregation partials across groups.
func (m *MultiGroupMerge) ReducesAggregates() bool { return m.reduceAgg }

// Tracker returns the shared query memory tracker used by collection and merging.
func (m *MultiGroupMerge) Tracker() *vectorized.MemoryTracker { return m.tracker }

// Merge returns a native iterator over merged batches, transferring Close ownership.
func (m *MultiGroupMerge) Merge(ctx context.Context, groups []*GroupBatches) (executor.MIterator, error) {
	result, err := m.MergeBatches(ctx, groups)
	if err != nil {
		return nil, err
	}
	return result.NativeIterator(ctx)
}

// MergeBatches takes ownership of groups and returns their merged native batches.
// Groups and reservations are released on failure or when the result is closed.
func (m *MultiGroupMerge) MergeBatches(ctx context.Context, groups []*GroupBatches) (*BatchResult, error) {
	if m.reduceAgg {
		return m.reduceGroups(ctx, groups)
	}
	var reserved int64
	fail := func(err error) (*BatchResult, error) {
		m.tracker.Release(reserved)
		for _, group := range groups {
			group.Release()
		}
		return nil, err
	}
	var schemas []*vectorized.BatchSchema
	hidden := logical.NewHiddenTagSet()
	for tag := range m.hidden {
		hidden.Add(tag)
	}
	for _, group := range groups {
		if group.Schema != nil {
			schemas = append(schemas, group.Schema)
		}
		for tag := range group.Hidden {
			hidden.Add(tag)
		}
	}
	if len(schemas) == 0 {
		for _, group := range groups {
			group.Release()
		}
		return NewBatchResult(ctx, &batchSliceSource{schema: vectorized.NewBatchSchema(nil)}, vectorized.NewBatchSchema(nil), m.cfg.BatchSize)
	}
	merged, unionErr := unionGroupSchemas(schemas)
	if unionErr != nil {
		return fail(unionErr)
	}
	sources, sourceErr := m.reshapeGroupBatches(ctx, groups, merged, &reserved)
	if sourceErr != nil {
		return fail(sourceErr)
	}
	offset, limit := uint32(0), uint32(math.MaxUint32)
	var output []*vectorized.RecordBatch
	if merged.TimestampIndex() >= 0 {
		if m.plainRows {
			offset, limit = m.offset, m.limit
		}
		spec := distributedRowsSpec{
			Desc:          m.desc,
			IndexMode:     m.indexMode,
			BatchSize:     m.cfg.BatchSize,
			OrderByColIdx: -1,
			Interrupt:     ctx.Err,
			Tracker:       m.tracker,
		}
		if m.orderBy != nil {
			spec.OrderByFamily = m.orderBy.family
			spec.OrderByTagName = m.orderBy.tag
		}
		// The merged output can be as large as the reshaped input; reserve that bound while both
		// exist so the peak stays inside the budget.
		reshaped := reserved
		if reserveErr := m.tracker.Reserve(reshaped); reserveErr != nil {
			releaseSources(sources)
			return fail(fmt.Errorf("vec multi-group merge: %w", reserveErr))
		}
		reserved += reshaped
		// Index-order merging copies every row's sort key.
		keyBytes := m.sortKeyBytes(sources, merged)
		if reserveErr := m.tracker.Reserve(keyBytes); reserveErr != nil {
			releaseSources(sources)
			return fail(fmt.Errorf("vec multi-group merge: %w", reserveErr))
		}
		reserved += keyBytes
		// Keep the shared merger's transient batch guard in addition to retained output.
		var mergeErr error
		output, mergeErr = mergeBatchSources(ctx, sources, merged, nil, spec)
		if mergeErr != nil {
			return fail(fmt.Errorf("vec multi-group merge: %w", mergeErr))
		}
		// The reshaped batches are released; the merged output keeps what it actually holds.
		var outBytes int64
		for _, batch := range output {
			outBytes += batchBytes(batch)
		}
		outBytes = min(outBytes, reshaped)
		m.tracker.Release(reserved - outBytes)
		reserved = outBytes
	} else {
		for _, src := range sources {
			output = append(output, src.batch)
		}
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		releaseBatches(output)
		return fail(fmt.Errorf("vec multi-group merge: %w", ctxErr))
	}
	result, resultErr := m.resultFromBatches(ctx, output, merged, offset, limit, groups, reserved)
	if resultErr != nil {
		return nil, resultErr
	}
	if !hidden.IsEmpty() {
		projected := newHiddenBatchProjection(result, hidden)
		return NewBatchResult(ctx, projected, projected.OutputSchema(), m.cfg.BatchSize)
	}
	return result, nil
}

// partialAggRequest is the request a group of a multi-group aggregation is dispatched with in partial mode. The
// projections name only what the group declares, so every group emits the partial layout a data node would and the
// reduce can unify them. The GroupBy keys are projected, so a criteria-only tag that is also a key is not hidden
// and stripped from the partial before the reduce needs it.
func partialAggRequest(req *measurev1.QueryRequest, measureSchema *databasev1.Measure) *measurev1.QueryRequest {
	out := proto.Clone(req).(*measurev1.QueryRequest)
	families := slices.Clone(out.GetTagProjection().GetTagFamilies())
	for _, keyFamily := range out.GetGroupBy().GetTagProjection().GetTagFamilies() {
		idx := slices.IndexFunc(families, func(f *modelv1.TagProjection_TagFamily) bool { return f.GetName() == keyFamily.GetName() })
		if idx < 0 {
			families = append(families, &modelv1.TagProjection_TagFamily{Name: keyFamily.GetName(), Tags: slices.Clone(keyFamily.GetTags())})
			continue
		}
		tags := slices.Clone(families[idx].GetTags())
		for _, tag := range keyFamily.GetTags() {
			if !slices.Contains(tags, tag) {
				tags = append(tags, tag)
			}
		}
		families[idx] = &modelv1.TagProjection_TagFamily{Name: keyFamily.GetName(), Tags: tags}
	}
	if len(families) > 0 {
		out.TagProjection = &modelv1.TagProjection{TagFamilies: families}
	}
	out.TagProjection = intersectTagProjection(out.GetTagProjection(), measureSchema)
	out.FieldProjection = intersectFieldProjection(out.GetFieldProjection(), measureSchema)
	return out
}

// reduceGroups reduces the aggregation partials of the groups into one result, applies Top to it and then
// paginates once. It takes ownership of groups, which are released when it returns or fails. The reduced rows are
// charged to the tracker until the returned result is closed.
func (m *MultiGroupMerge) reduceGroups(ctx context.Context, groups []*GroupBatches) (result *BatchResult, err error) {
	defer func() {
		for _, group := range groups {
			group.Release()
		}
	}()
	spec, specErr := newAggReduceSpec(m.req)
	if specErr != nil {
		return nil, specErr
	}
	// The per-group plans no longer carry Top, so its field is validated here, whether or not rows come back.
	if spec.top != nil && spec.top.FieldName != spec.aggSpecs[0].OutputName {
		return nil, fmt.Errorf("vec multi-group merge: plan.Top.Build: field %q not present in schema", spec.top.FieldName)
	}
	partialSchemas := make([]*vectorized.BatchSchema, 0, len(groups))
	for _, group := range groups {
		if group.Schema != nil {
			partialSchemas = append(partialSchemas, group.Schema)
		}
	}
	// The layout comes from the groups' schemas, not from whichever groups returned rows.
	target, targetErr := multiGroupAggTarget(partialSchemas, len(groups), m.req.GetTagProjection())
	if targetErr != nil {
		return nil, fmt.Errorf("vec multi-group merge: %w", targetErr)
	}
	reducer := spec.newReducer(target, m.cfg.BatchSize, m.tracker)
	defer reducer.Close()
	for groupIdx, group := range groups {
		for _, batch := range group.Batches {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, fmt.Errorf("vec multi-group merge: %w", ctxErr)
			}
			// nolint:contextcheck // pure in-memory reducer; no cancelable I/O downstream
			if consumeErr := reducer.ConsumeScoped(batch, int64(groupIdx)); consumeErr != nil {
				return nil, fmt.Errorf("vec multi-group merge: reduce group %d: %w", groupIdx, consumeErr)
			}
		}
	}
	// The hold is what this call keeps reserved until it hands it to the result; handing it over zeroes it, so a
	// failure after that point is released once, by the result.
	hold := &reduceHold{tracker: m.tracker}
	defer func() { hold.release(hold.held) }()
	// The reduced rows are reserved before they are materialized: a bound from the group count and the merged
	// layout's width, replaced by what they actually hold.
	width := 1
	if target != nil {
		width = max(width, len(target.Columns))
	} else if len(partialSchemas) > 0 {
		width = max(width, len(partialSchemas[0].Columns))
	}
	finishBound := int64(reducer.Groups()) * int64(width) * 16
	if reserveErr := hold.reserve(finishBound); reserveErr != nil {
		return nil, reserveErr
	}
	batches, _, reduceErr := reducer.FinishContext(ctx)
	if reduceErr != nil {
		return nil, fmt.Errorf("vec multi-group merge: %w", reduceErr)
	}
	schema := reducer.OutputSchema()
	finished := reduceRowsBytes(batches)
	if finished > finishBound {
		if reserveErr := hold.reserve(finished - finishBound); reserveErr != nil {
			return nil, reserveErr
		}
	} else {
		hold.release(finishBound - finished)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, fmt.Errorf("vec multi-group merge: %w", ctxErr)
	}
	if spec.top != nil && len(batches) > 0 {
		var topErr error
		if batches, topErr = m.topReduced(ctx, *spec.top, batches, hold); topErr != nil {
			return nil, topErr
		}
	}
	if schema == nil {
		schema = target
	}
	if schema == nil {
		schema = vectorized.NewBatchSchema(nil)
	}
	limit := m.limit
	if limit == 0 {
		limit = defaultLimit
	}
	owned := hold.held
	hold.held = 0
	built, buildErr := m.resultFromBatches(ctx, batches, schema, m.offset, limit, nil, owned)
	if buildErr != nil {
		return nil, buildErr
	}
	if m.hidden.IsEmpty() {
		return built, nil
	}
	// The grouping keys that only the criteria named were projected for the reduce; the response omits them.
	projected := newHiddenBatchProjection(built, m.hidden)
	return NewBatchResult(ctx, projected, projected.OutputSchema(), m.cfg.BatchSize)
}

// reduceHold is the part of the tracker a reduce holds until it hands it to its result.
type reduceHold struct {
	tracker *vectorized.MemoryTracker
	held    int64
}

func (h *reduceHold) reserve(size int64) error {
	if reserveErr := h.tracker.Reserve(size); reserveErr != nil {
		return fmt.Errorf("vec multi-group merge: %w", reserveErr)
	}
	h.held += size
	return nil
}

func (h *reduceHold) release(size int64) {
	h.tracker.Release(size)
	h.held -= size
}

func reduceRowsBytes(rows []*vectorized.RecordBatch) int64 {
	var size int64
	for _, batch := range rows {
		size += batchBytes(batch)
	}
	return size
}

// topReduced keeps the top rows of the reduced batches. A non-positive Top keeps none, as BatchTop does. What Top
// allocates on top of the reduced rows is reserved before it runs; afterwards only the top rows stay held.
func (m *MultiGroupMerge) topReduced(ctx context.Context, top vmeasure.ReduceTopSpec, batches []*vectorized.RecordBatch,
	hold *reduceHold,
) ([]*vectorized.RecordBatch, error) {
	var topped []*vectorized.RecordBatch
	if top.N > 0 {
		if reserveErr := hold.reserve(topPeakBytes(batches, top.N)); reserveErr != nil {
			return nil, reserveErr
		}
		var topErr error
		topped, topErr = vmeasure.ApplyTopToReduceContext(ctx, batches, top, m.cfg.BatchSize)
		if topErr != nil {
			return nil, fmt.Errorf("vec multi-group merge: top reduced groups: %w", topErr)
		}
	}
	hold.release(hold.held - min(reduceRowsBytes(topped), hold.held))
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, fmt.Errorf("vec multi-group merge: %w", ctxErr)
	}
	return topped, nil
}

// Top copies every row it retains into a row of its own, one single-row column per cell, and emits the retained
// rows into new batches while those copies are still alive.
const (
	topRowOverheadBytes  = 64
	topCellOverheadBytes = 80
)

// topPeakBytes bounds what Top over the reduced batches allocates while the batches are alive: the cells of the n
// largest rows twice, once retained and once emitted, plus the retained rows' and cells' headers.
func topPeakBytes(batches []*vectorized.RecordBatch, n int) int64 {
	var sizes []int64
	width := 0
	for _, batch := range batches {
		width = max(width, len(batch.Columns))
		rows := make([]int64, batch.Len)
		for _, col := range batch.Columns {
			addCellBytes(rows, col)
		}
		sizes = append(sizes, rows...)
	}
	kept := min(n, len(sizes))
	if kept < len(sizes) {
		slices.SortFunc(sizes, func(a, b int64) int { return cmp.Compare(b, a) })
	}
	var cells int64
	for _, size := range sizes[:kept] {
		cells += size
	}
	return 2*cells + int64(kept)*(topRowOverheadBytes+int64(width)*topCellOverheadBytes)
}

// addCellBytes adds each row's cell of col to rows, sized as columnBytes sizes the whole column.
func addCellBytes(rows []int64, col vectorized.Column) {
	switch c := col.(type) {
	case *vectorized.TypedColumn[int64], *vectorized.TypedColumn[float64]:
		for i := range rows {
			rows[i] += 8
		}
	case *vectorized.TypedColumn[string]:
		for i, v := range c.Data() {
			rows[i] += 16 + int64(len(v))
		}
	case *vectorized.TypedColumn[[]byte]:
		for i, v := range c.Data() {
			rows[i] += 24 + int64(len(v))
		}
	case *vectorized.TypedColumn[[]int64]:
		for i, v := range c.Data() {
			rows[i] += 24 + int64(len(v))*8
		}
	case *vectorized.TypedColumn[[]string]:
		for i, v := range c.Data() {
			rows[i] += 24
			for _, e := range v {
				rows[i] += 16 + int64(len(e))
			}
		}
	case *vectorized.TypedColumn[*modelv1.TagValue]:
		for i, v := range c.Data() {
			rows[i] += 16 + int64(proto.Size(v))
		}
	case *vectorized.TypedColumn[*modelv1.FieldValue]:
		for i, v := range c.Data() {
			rows[i] += 16 + int64(proto.Size(v))
		}
	default:
		for i := range rows {
			rows[i] += 16
		}
	}
}

// sortKeyBytes estimates the sort keys the merge allocates: the order-by column's payload plus a
// slice header and marker per row. Time ordering allocates a fixed 8 bytes per emitted row, which
// the output reservation covers.
func (m *MultiGroupMerge) sortKeyBytes(sources []decodedBatchSource, merged *vectorized.BatchSchema) int64 {
	if m.orderBy == nil {
		return 0
	}
	colIdx, ok := merged.TagIndex(m.orderBy.family, m.orderBy.tag)
	if !ok {
		return 0
	}
	var total int64
	for _, src := range sources {
		total += columnBytes(src.batch.Columns[colIdx]) + int64(src.batch.Len)*25
	}
	return total
}

// resultFromBatches applies pagination while keeping the result in batches.
func (m *MultiGroupMerge) resultFromBatches(ctx context.Context, batches []*vectorized.RecordBatch,
	schema *vectorized.BatchSchema, offset, limit uint32, groups []*GroupBatches, reserved int64,
) (*BatchResult, error) {
	source := &groupReleasingSource{
		PullOperator: &batchSliceSource{batches: batches, schema: schema}, groups: groups, tracker: m.tracker, reserved: reserved,
	}
	builder := vectorized.NewPipelineBuilder().WithMemoryTracker(m.tracker).From(source)
	builder.Apply(vmeasure.NewBatchLimit(schema, offset, limit))
	pipeline, err := builder.Build()
	if err != nil {
		_ = source.Close()
		return nil, fmt.Errorf("vec multi-group merge: build output pipeline: %w", err)
	}
	if err = pipeline.Init(ctx); err != nil {
		_ = pipeline.Close()
		return nil, fmt.Errorf("vec multi-group merge: init output pipeline: %w", err)
	}
	return NewBatchResult(ctx, &pipelineBatchSource{pipeline: pipeline, schema: schema}, schema, m.cfg.BatchSize)
}

// reshapeGroupBatches aligns every group's batches to the merged schema, null-filling
// the columns a group lacks, and charges each reshaped batch to the tracker (added to
// *reserved). Sources are numbered in group order so rows that tie on the sort key keep
// a deterministic order. On error the reshaped batches are released and the groups are
// left for the caller.
func (m *MultiGroupMerge) reshapeGroupBatches(
	ctx context.Context,
	groups []*GroupBatches,
	merged *vectorized.BatchSchema,
	reserved *int64,
) ([]decodedBatchSource, error) {
	var sources []decodedBatchSource
	for groupIdx, group := range groups {
		for batchIdx, raw := range group.Batches {
			if ctxErr := ctx.Err(); ctxErr != nil {
				releaseSources(sources)
				return nil, fmt.Errorf("vec multi-group merge: %w", ctxErr)
			}
			if raw == nil || raw.ActiveLen() == 0 {
				continue
			}
			// Cells are shared or copied by value, and columns the group lacks are null-filled.
			bytes := batchBytes(raw) + int64(raw.Len)*16*int64(max(0, len(merged.Columns)-len(raw.Schema.Columns)))
			if reserveErr := m.tracker.Reserve(bytes); reserveErr != nil {
				releaseSources(sources)
				return nil, fmt.Errorf("vec multi-group merge: %w", reserveErr)
			}
			*reserved += bytes
			reshaped, reshapeErr := reshapeToMergedSchema(raw, merged)
			if reshapeErr != nil {
				releaseSources(sources)
				return nil, fmt.Errorf("vec multi-group merge: reshape group %d batch %d: %w", groupIdx, batchIdx, reshapeErr)
			}
			sources = append(sources, decodedBatchSource{batch: reshaped, source: len(sources), group: groupIdx})
		}
	}
	return sources, nil
}

// unionGroupSchemas builds the merged schema of the groups' output schemas: the
// columns in first-seen order, keyed by role, family and name. A tag or field whose
// type differs between groups is promoted to the passthrough column type.
func unionGroupSchemas(schemas []*vectorized.BatchSchema) (*vectorized.BatchSchema, error) {
	var cols []vectorized.ColumnDef
	index := make(map[string]int)
	for _, schema := range schemas {
		for _, def := range schema.Columns {
			key := schemaColKey(def)
			colIdx, seen := index[key]
			if !seen {
				index[key] = len(cols)
				cols = append(cols, def)
				continue
			}
			if cols[colIdx].Type == def.Type {
				continue
			}
			switch def.Role {
			case vectorized.RoleTag:
				cols[colIdx].Type = vectorized.ColumnTypeTagValue
			case vectorized.RoleField:
				cols[colIdx].Type = vectorized.ColumnTypeFieldValue
			default:
				return nil, fmt.Errorf("vec multi-group merge: column %q type diverges across groups on role %v: %v vs %v",
					def.Name, def.Role, cols[colIdx].Type, def.Type)
			}
		}
	}
	return vectorized.NewBatchSchema(cols), nil
}

// groupReleasingSource keeps storage owners alive through final batch consumption.
type groupReleasingSource struct {
	vectorized.PullOperator
	tracker  *vectorized.MemoryTracker
	groups   []*GroupBatches
	reserved int64
}

func (g *groupReleasingSource) Close() error {
	err := g.PullOperator.Close()
	for _, group := range g.groups {
		group.Release()
	}
	g.groups = nil
	g.tracker.Release(g.reserved)
	g.reserved = 0
	return err
}

// GroupBatches owns copies of one group's borrowed batch descriptors and columns.
// The native result stays open until Release to retain any borrowed cell payloads.
type GroupBatches struct {
	tracker    *vectorized.MemoryTracker
	closer     *BatchResult
	Schema     *vectorized.BatchSchema
	Hidden     logical.HiddenTagSet
	PlanString string
	Batches    []*vectorized.RecordBatch
	reserved   int64
}

// Release closes the native group and releases copied output and reservations.
func (g *GroupBatches) Release() {
	if g == nil {
		return
	}
	releaseBatches(g.Batches)
	g.Batches = nil
	if g.closer != nil {
		_ = g.closer.Close()
		g.closer = nil
	}
	if g.tracker != nil {
		g.tracker.Release(g.reserved)
		g.reserved = 0
	}
}

// Collect takes ownership of a native group result without materializing rows.
func (m *MultiGroupMerge) Collect(ctx context.Context, result *BatchResult, planStr string) (*GroupBatches, error) {
	group := &GroupBatches{closer: result, tracker: m.tracker, Schema: result.OutputSchema(), PlanString: planStr}
	for {
		raw, err := result.NextBatch(ctx)
		if err != nil {
			group.Release()
			return nil, fmt.Errorf("vec dispatch: read batches: %w", err)
		}
		if raw == nil {
			return group, nil
		}
		bytes := batchBytes(raw)
		if err = m.tracker.Reserve(bytes); err != nil {
			group.Release()
			return nil, fmt.Errorf("vec dispatch: read batches: %w", err)
		}
		group.reserved += bytes
		// NextBatch lends descriptors and columns only until the next pull. Copy both
		// before advancing and transfer the owners that pin borrowed cell payloads.
		copyBatch, err := reshapeToMergedSchema(raw, raw.Schema)
		if err != nil {
			group.Release()
			return nil, err
		}
		copyBatch.Selection = append([]uint16(nil), raw.Selection...)
		if raw.Selection != nil && len(raw.Selection) == 0 {
			copyBatch.Selection = []uint16{}
		}
		group.Batches = append(group.Batches, copyBatch)
	}
}
