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

package stream

import (
	"context"
	"fmt"

	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// mergeSourceColumnName names the hidden column that records which staged batch a merged
// row came from. It is a field column, so neither the tag projection nor the element egress
// sees it.
const mergeSourceColumnName = "_merge_source"

// tagColumnKey identifies a tag column by family and name.
type tagColumnKey struct {
	family string
	name   string
}

// AlignedStreamBatches holds the staged batches of one merge rebuilt onto one shared schema,
// and the tags each staged batch defined. The shared schema matters because SortedMerge
// validates batch schemas by pointer, and batches decoded from separate wire frames each
// carry their own schema instance.
//
// Every staged batch is copied onto the union schema and stamped with a hidden source column;
// defined and sourceIdx then record which tags each source may return, so materializing
// Elements can drop a tag a batch never carried instead of emitting it as null. Each copy is
// charged against the query budget before it is allocated, so a copy the lease cannot cover
// is rejected instead of allocated.
type AlignedStreamBatches struct {
	Schema    *vectorized.BatchSchema
	Batches   []*vectorized.RecordBatch
	defined   []map[tagColumnKey]struct{}
	sourceIdx int
}

// AlignStreamBatches rebuilds the given staged batches onto one schema usable by
// SortedMerge. Empty batches are skipped. The merged schema carries the common columns, the
// union of the batches' tag columns in first-seen order, the order key when every batch has
// one and the merge is not by time, and the hidden source column.
//
// Variable-sized cells may still reference the source batches. Keep the staged batches
// and their owners alive through merge and materialization, then release both sets.
// Alignment charges stay on the enclosing query reservation through response/transport
// completion: the returned Elements may retain source tag values. Merge.Close refunds
// only SortedMerge's separate retention charges. Without an admitted lease, charging
// is a no-op.
func AlignStreamBatches(ctx context.Context, staged []*vectorized.RecordBatch,
	sortByTime bool,
) (*AlignedStreamBatches, error) {
	nonEmpty := make([]*vectorized.RecordBatch, 0, len(staged))
	for _, batch := range staged {
		if batch != nil && batch.ActiveLen() > 0 {
			nonEmpty = append(nonEmpty, batch)
		}
	}
	if len(nonEmpty) == 0 {
		return &AlignedStreamBatches{sourceIdx: -1}, nil
	}
	return copyOntoUnionSchema(ctx, nonEmpty, sortByTime)
}

// copyOntoUnionSchema rebuilds every batch onto one shared schema. A tag a batch lacks is
// null-filled, and a hidden source column records the batch so the null-filled tags can be
// dropped again when Elements are built.
func copyOntoUnionSchema(ctx context.Context, nonEmpty []*vectorized.RecordBatch,
	sortByTime bool,
) (*AlignedStreamBatches, error) {
	cols := []vectorized.ColumnDef{
		{Name: StreamColumnNameTimestamp, Role: vectorized.RoleTimestamp, Type: vectorized.ColumnTypeInt64},
		{Name: StreamColumnNameElementID, Role: vectorized.RoleElementID, Type: vectorized.ColumnTypeInt64},
		{Name: StreamColumnNameSeriesID, Role: vectorized.RoleSeriesID, Type: vectorized.ColumnTypeInt64},
	}
	seen := make(map[tagColumnKey]struct{})
	withOrderKey := !sortByTime
	defined := make([]map[tagColumnKey]struct{}, len(nonEmpty))
	for sourceIdx, batch := range nonEmpty {
		defined[sourceIdx] = make(map[tagColumnKey]struct{})
		for _, def := range batch.Schema.Columns {
			if def.Role != vectorized.RoleTag {
				continue
			}
			key := tagColumnKey{family: def.TagFamily, name: def.Name}
			defined[sourceIdx][key] = struct{}{}
			if _, ok := seen[key]; !ok {
				seen[key] = struct{}{}
				cols = append(cols, def)
			}
		}
		if batch.Schema.OrderKeyIndex() < 0 {
			withOrderKey = false
		}
	}
	if withOrderKey {
		cols = append(cols, vectorized.ColumnDef{Name: StreamColumnNameOrderKey, Role: vectorized.RoleOrderKey, Type: vectorized.ColumnTypeBytes})
	}
	sourceColIdx := len(cols)
	cols = append(cols, vectorized.ColumnDef{Name: mergeSourceColumnName, Role: vectorized.RoleField, Type: vectorized.ColumnTypeInt64})
	schema := vectorized.NewBatchSchema(cols)

	aligned := make([]*vectorized.RecordBatch, 0, len(nonEmpty))
	for idx, batch := range nonEmpty {
		// The copy holds the batch's retained bytes again, the source column, and one slot per
		// column the batch lacked.
		extraCols := uint64(len(schema.Columns) - len(batch.Schema.Columns) + 1)
		chargeBytes := vectorized.EstimatedRetainedBytes(batch) + uint64(batch.ActiveLen())*extraCols*8
		if chargeErr := query.Charge(ctx, chargeBytes); chargeErr != nil {
			return nil, chargeErr
		}
		out, alignErr := copyOntoUnionSchemaBatch(schema, batch, idx, sourceColIdx)
		if alignErr != nil {
			return nil, alignErr
		}
		aligned = append(aligned, out)
	}
	return &AlignedStreamBatches{Schema: schema, Batches: aligned, defined: defined, sourceIdx: sourceColIdx}, nil
}

// copyOntoUnionSchemaBatch copies the active rows of src onto target, null-filling the columns
// src lacks and stamping every row with the source index.
func copyOntoUnionSchemaBatch(target *vectorized.BatchSchema, src *vectorized.RecordBatch,
	source, sourceColIdx int,
) (*vectorized.RecordBatch, error) {
	rows := src.ActiveLen()
	out := vectorized.NewRecordBatch(target, rows)
	for dstIdx, def := range target.Columns {
		if dstIdx == sourceColIdx {
			sourceCol := out.Columns[dstIdx].(*vectorized.TypedColumn[int64])
			for range rows {
				sourceCol.Append(int64(source))
			}
			continue
		}
		srcIdx := -1
		switch def.Role {
		case vectorized.RoleTimestamp:
			srcIdx = src.Schema.TimestampIndex()
		case vectorized.RoleElementID:
			srcIdx = src.Schema.ElementIDIndex()
		case vectorized.RoleSeriesID:
			srcIdx = src.Schema.SeriesIDIndex()
		case vectorized.RoleOrderKey:
			srcIdx = src.Schema.OrderKeyIndex()
		case vectorized.RoleTag:
			if tagIdx, ok := src.Schema.TagIndex(def.TagFamily, def.Name); ok {
				srcIdx = tagIdx
			}
		default:
			// Stream batches carry no version, shard or field columns, so the source column is
			// the only one handled above and anything else stays unmatched.
		}
		if srcIdx < 0 {
			if def.Role != vectorized.RoleTag {
				return nil, fmt.Errorf("align stream batches: source %d lacks the %q column", source, def.Name)
			}
			for range rows {
				out.Columns[dstIdx].AppendNull()
			}
			continue
		}
		if src.Selection == nil {
			if appendErr := vectorized.AppendColumnRange(out.Columns[dstIdx], src.Columns[srcIdx], 0, src.Len); appendErr != nil {
				return nil, appendErr
			}
			continue
		}
		for _, row := range src.Selection {
			if appendErr := vectorized.AppendColumnRange(out.Columns[dstIdx], src.Columns[srcIdx], int(row), 1); appendErr != nil {
				return nil, appendErr
			}
		}
	}
	out.Len = rows
	return out, nil
}

// Merge runs the shared merge → distinct → limit pipeline over the aligned batches and returns
// the batches of the client window, in output order. A nil or empty alignment returns nil.
func (a *AlignedStreamBatches) Merge(ctx context.Context, desc bool, offset, limit uint32) ([]*vectorized.RecordBatch, error) {
	if a == nil || a.Schema == nil || len(a.Batches) == 0 {
		return nil, nil
	}
	pipeline, buildErr := BuildStreamMergePipeline(
		NewBatchSource(a.Schema, a.Batches),
		a.Schema, desc, offset, limit, mergeOutputBatchSize(offset, limit), 0)
	if buildErr != nil {
		return nil, buildErr
	}
	defer func() {
		_ = pipeline.Close()
	}()
	if initErr := pipeline.Init(ctx); initErr != nil {
		return nil, initErr
	}
	var merged []*vectorized.RecordBatch
	for {
		batch, nextErr := pipeline.Next(ctx)
		if nextErr != nil {
			return nil, nextErr
		}
		if batch == nil {
			return merged, nil
		}
		if batch.ActiveLen() > 0 {
			merged = append(merged, batch)
		}
	}
}

// mergeOutputBatchSize sizes the merge's output batches to the client window instead of the
// canonical batch size. The window is how many rows the merge has to emit before Limit is
// satisfied (offset rows skipped plus limit rows kept), so a small window would otherwise
// allocate a full DefaultBatchSize-wide batch per column for a handful of rows. A window at or
// above the canonical size, and an unbounded one, keep DefaultBatchSize. Sizing by the window
// rather than the limit alone keeps deep pagination — a large offset with a small limit — from
// producing a long tail of tiny batches.
func mergeOutputBatchSize(offset, limit uint32) int {
	window := uint64(offset) + uint64(limit)
	if window > 0 && window < uint64(vectorized.DefaultBatchSize) {
		return int(window)
	}
	return vectorized.DefaultBatchSize
}

// ElementBuilder materializes the active rows of one batch using the given projection.
type ElementBuilder func(*vectorized.RecordBatch, []model.TagProjection) ([]*streamv1.Element, error)

// BuildElements materializes the merged batches, then drops from each element the tags
// its source batch did not define, so a schema-evolved source keeps its own element shape
// instead of gaining null tags for another source's tags.
// A nil builder uses BuildElementsFromBatch.
func (a *AlignedStreamBatches) BuildElements(batches []*vectorized.RecordBatch, build ElementBuilder) ([]*streamv1.Element, error) {
	if a == nil || a.Schema == nil {
		return nil, nil
	}
	projection := ProjectionFromBatchSchema(&vectorized.RecordBatch{Schema: a.Schema})
	if build == nil {
		build = BuildElementsFromBatch
	}
	var result []*streamv1.Element
	for _, batch := range batches {
		elements, buildErr := build(batch, projection)
		if buildErr != nil {
			return nil, buildErr
		}
		if a.sourceIdx >= 0 {
			sourceCol := batch.Columns[a.sourceIdx].(*vectorized.TypedColumn[int64]).Data()
			for active, element := range elements {
				row := active
				if batch.Selection != nil {
					row = int(batch.Selection[active])
				}
				dropUndefinedTags(element, a.defined[sourceCol[row]])
			}
		}
		result = append(result, elements...)
	}
	return result, nil
}

// dropUndefinedTags removes the tags outside defined, and any family left empty.
func dropUndefinedTags(element *streamv1.Element, defined map[tagColumnKey]struct{}) {
	families := element.TagFamilies[:0]
	for _, family := range element.TagFamilies {
		tags := family.Tags[:0]
		for _, tag := range family.Tags {
			if _, ok := defined[tagColumnKey{family: family.Name, name: tag.Key}]; ok {
				tags = append(tags, tag)
			}
		}
		if len(tags) == 0 {
			continue
		}
		family.Tags = tags
		families = append(families, family)
	}
	element.TagFamilies = families
}
