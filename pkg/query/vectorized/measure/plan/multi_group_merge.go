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
	"context"
	"fmt"
	"math"

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
// other Agg output has no ordering column, so its per-group results are concatenated
// in group order, as each group's plan already applied its own limit; reducing them
// across groups is #14069.
//
// Every batch the groups hand over, the reshaped copies and the merged output are
// charged to one memory tracker sized by the query memory budget.
type MultiGroupMerge struct {
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
}

// NewMultiGroupMerge resolves how the groups of req are merged. schemas and
// measureSchemas are the per-group logical and measure schemas in request order.
func NewMultiGroupMerge(
	ctx context.Context,
	req *measurev1.QueryRequest,
	schemas []logical.Schema,
	measureSchemas []*databasev1.Measure,
	cfg vmeasure.VectorizedConfig,
) (*MultiGroupMerge, error) {
	merge := &MultiGroupMerge{
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

// GroupRequest returns the request every group is dispatched with.
func (m *MultiGroupMerge) GroupRequest() *measurev1.QueryRequest { return m.groupReq }

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
