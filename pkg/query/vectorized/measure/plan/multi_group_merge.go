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
// batches, and converts the merged batches to an MIterator exactly once.
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
	req *measurev1.QueryRequest,
	schemas []logical.Schema,
	measureSchemas []*databasev1.Measure,
	cfg vmeasure.VectorizedConfig,
) (*MultiGroupMerge, error) {
	merge := &MultiGroupMerge{
		groupReq:  req,
		cfg:       cfg,
		hidden:    logical.NewHiddenTagSet(),
		tracker:   vectorized.NewMemoryTracker(int64(cfg.QueryMemoryMiB) * 1024 * 1024),
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

// Tracker returns the memory tracker to hand to DispatchBatches for every group.
func (m *MultiGroupMerge) Tracker() *vectorized.MemoryTracker { return m.tracker }

// Merge merges the groups' batches and returns the result as one MIterator. It
// takes ownership of groups: they are released when Merge fails and when the
// returned iterator is closed. The context is checked while reshaping and merging.
func (m *MultiGroupMerge) Merge(ctx context.Context, groups []*GroupBatches) (executor.MIterator, error) {
	var reserved int64
	fail := func(err error) (executor.MIterator, error) {
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
		if group.Schema != nil && len(group.Batches) > 0 {
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
		return emptyMIterator{}, nil
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
		var mergeErr error
		output, mergeErr = mergeBatchSources(sources, merged, spec)
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
	iter, iterErr := m.iteratorFromBatches(ctx, output, merged, offset, limit)
	if iterErr != nil {
		return fail(iterErr)
	}
	var result executor.MIterator = &groupReleasingIterator{inner: iter, groups: groups, tracker: m.tracker, reserved: reserved}
	if !hidden.IsEmpty() {
		result = &hiddenTagsMIterator{inner: result, hiddenTags: hidden}
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

// iteratorFromBatches turns the merged batches into an MIterator, applying the
// offset and limit once. It owns batches: a failed build releases them.
func (m *MultiGroupMerge) iteratorFromBatches(
	ctx context.Context,
	batches []*vectorized.RecordBatch,
	schema *vectorized.BatchSchema,
	offset, limit uint32,
) (*vmeasure.VectorizedMIterator, error) {
	builder := vectorized.NewPipelineBuilder().WithMemoryTracker(vectorized.NewMemoryTracker(int64(m.cfg.QueryMemoryMiB) * 1024 * 1024))
	builder.From(&batchSliceSource{batches: batches, schema: schema})
	builder.Apply(vmeasure.NewBatchLimit(schema, offset, limit))
	pipeline, buildErr := builder.Build()
	if buildErr != nil {
		releaseBatches(batches)
		return nil, fmt.Errorf("vec multi-group merge: build output pipeline: %w", buildErr)
	}
	if initErr := pipeline.Init(ctx); initErr != nil {
		_ = pipeline.Close()
		return nil, fmt.Errorf("vec multi-group merge: init output pipeline: %w", initErr)
	}
	pool := vectorized.NewBatchPool(schema, m.cfg.BatchSize)
	return vmeasure.NewIteratorFromPipeline(ctx, pipeline, schema, pool), nil
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

// groupReleasingIterator closes the groups' plans once the merged result is closed.
type groupReleasingIterator struct {
	tracker  *vectorized.MemoryTracker
	inner    *vmeasure.VectorizedMIterator
	groups   []*GroupBatches
	reserved int64
}

func (g *groupReleasingIterator) Next() bool { return g.inner.Next() }

func (g *groupReleasingIterator) Current() []*measurev1.InternalDataPoint { return g.inner.Current() }

// Pipeline implements vmeasure.RawFrameSource: the merged batches' output pipeline.
func (g *groupReleasingIterator) Pipeline() *vectorized.Pipeline { return g.inner.Pipeline() }

// Schema implements vmeasure.RawFrameSource: the merged schema every emitted batch carries.
func (g *groupReleasingIterator) Schema() *vectorized.BatchSchema { return g.inner.Schema() }

// EmitFrame implements vmeasure.FrameEmitter by draining the merged pipeline.
func (g *groupReleasingIterator) EmitFrame(ctx context.Context) ([]byte, error) {
	return g.inner.EmitFrame(ctx)
}

func (g *groupReleasingIterator) Close() error {
	closeErr := g.inner.Close()
	for _, group := range g.groups {
		group.Release()
	}
	g.tracker.Release(g.reserved)
	g.reserved = 0
	return closeErr
}
