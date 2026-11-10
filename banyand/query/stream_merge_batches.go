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

package query

import (
	"context"
	"fmt"

	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vstream "github.com/apache/skywalking-banyandb/pkg/query/vectorized/stream"
)

// streamGroupColumnName names the hidden column that records which group a merged row came
// from. It is a field column, so neither the tag projection nor the element egress sees it.
const streamGroupColumnName = "_merge_group"

type streamTagKey struct {
	family string
	name   string
}

// alignedStreamGroups holds the staged batch of every non-empty group, rebuilt onto one shared
// schema, and the tags each group defines.
type alignedStreamGroups struct {
	schema  *vectorized.BatchSchema
	batches []*vectorized.RecordBatch
	// defined[g] is the set of tags group g's own batch carried. A tag outside it was
	// null-filled by the alignment and must not reach the client.
	defined []map[streamTagKey]struct{}
	// groupIdx is the index of the hidden group column in schema.
	groupIdx int
}

// alignStreamGroupBatches rebuilds each staged group batch onto one merged schema: the common
// columns, the union of the groups' tag columns in first-seen order, the order key when every
// group carries one and the merge is not by time, and a hidden group column. A tag a group lacks
// is null-filled. SortedMerge, Distinct and Limit need a single schema, compared by pointer.
// Empty batches are skipped. Each aligned copy is charged against the query budget before it is
// allocated, so a copy the lease cannot cover is rejected instead of allocated.
func alignStreamGroupBatches(ctx context.Context, staged []*vectorized.RecordBatch, sortByTime bool) (*alignedStreamGroups, error) {
	nonEmpty := make([]*vectorized.RecordBatch, 0, len(staged))
	for _, batch := range staged {
		if batch != nil && batch.ActiveLen() > 0 {
			nonEmpty = append(nonEmpty, batch)
		}
	}
	cols := []vectorized.ColumnDef{
		{Name: vstream.StreamColumnNameTimestamp, Role: vectorized.RoleTimestamp, Type: vectorized.ColumnTypeInt64},
		{Name: vstream.StreamColumnNameElementID, Role: vectorized.RoleElementID, Type: vectorized.ColumnTypeInt64},
		{Name: vstream.StreamColumnNameSeriesID, Role: vectorized.RoleSeriesID, Type: vectorized.ColumnTypeInt64},
	}
	seen := make(map[streamTagKey]struct{})
	withOrderKey := !sortByTime && len(nonEmpty) > 0
	defined := make([]map[streamTagKey]struct{}, len(nonEmpty))
	for groupIdx, batch := range nonEmpty {
		defined[groupIdx] = make(map[streamTagKey]struct{})
		for _, def := range batch.Schema.Columns {
			if def.Role != vectorized.RoleTag {
				continue
			}
			key := streamTagKey{family: def.TagFamily, name: def.Name}
			defined[groupIdx][key] = struct{}{}
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
		cols = append(cols, vectorized.ColumnDef{Name: vstream.StreamColumnNameOrderKey, Role: vectorized.RoleOrderKey, Type: vectorized.ColumnTypeBytes})
	}
	groupIdx := len(cols)
	cols = append(cols, vectorized.ColumnDef{Name: streamGroupColumnName, Role: vectorized.RoleField, Type: vectorized.ColumnTypeInt64})
	schema := vectorized.NewBatchSchema(cols)

	aligned := make([]*vectorized.RecordBatch, 0, len(nonEmpty))
	for idx, batch := range nonEmpty {
		// The copy holds the batch's retained bytes again, the group column, and one slot per
		// column the batch lacked.
		extraCols := uint64(len(schema.Columns) - len(batch.Schema.Columns) + 1)
		chargeBytes := vectorized.EstimatedRetainedBytes(batch) + uint64(batch.ActiveLen())*extraCols*8
		if chargeErr := query.Charge(ctx, chargeBytes); chargeErr != nil {
			return nil, chargeErr
		}
		out, alignErr := alignStreamBatch(schema, batch, idx, groupIdx)
		if alignErr != nil {
			return nil, alignErr
		}
		aligned = append(aligned, out)
	}
	return &alignedStreamGroups{schema: schema, batches: aligned, defined: defined, groupIdx: groupIdx}, nil
}

// alignStreamBatch copies the active rows of src onto target, null-filling the columns src lacks
// and stamping every row with the group index.
func alignStreamBatch(target *vectorized.BatchSchema, src *vectorized.RecordBatch, group, groupIdx int) (*vectorized.RecordBatch, error) {
	rows := src.ActiveLen()
	out := vectorized.NewRecordBatch(target, rows)
	for dstIdx, def := range target.Columns {
		if dstIdx == groupIdx {
			groupCol := out.Columns[dstIdx].(*vectorized.TypedColumn[int64])
			for range rows {
				groupCol.Append(int64(group))
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
			// Stream batches carry no version, shard or field columns, so the group
			// column is the only one handled above and anything else stays unmatched.
		}
		if srcIdx < 0 {
			if def.Role != vectorized.RoleTag {
				return nil, fmt.Errorf("align stream batches: group %d lacks the %q column", group, def.Name)
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

// mergeAlignedStreamGroups runs the aligned batches of every group through one SortedMerge,
// Distinct and Limit(offset, limit) pipeline and returns the surviving batches.
func mergeAlignedStreamGroups(ctx context.Context, groups *alignedStreamGroups, desc bool, offset, limit uint32,
) ([]*vectorized.RecordBatch, error) {
	pipeline, buildErr := vstream.BuildStreamMergePipeline(
		vstream.NewBatchSource(groups.schema, groups.batches),
		groups.schema, desc, offset, limit, vectorized.DefaultBatchSize, 0)
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

// buildAlignedStreamElements materializes the merged batches once, then drops from each element
// the tags its group did not define, so a schema-evolved group's elements keep their own shape
// instead of gaining null tags for another group's tags.
func buildAlignedStreamElements(groups *alignedStreamGroups, batches []*vectorized.RecordBatch) ([]*streamv1.Element, error) {
	projection := vstream.ProjectionFromBatchSchema(&vectorized.RecordBatch{Schema: groups.schema})
	var result []*streamv1.Element
	for _, batch := range batches {
		elements, buildErr := vstream.BuildElementsFromBatch(batch, projection)
		if buildErr != nil {
			return nil, buildErr
		}
		groupCol := batch.Columns[groups.groupIdx].(*vectorized.TypedColumn[int64]).Data()
		for active, element := range elements {
			row := active
			if batch.Selection != nil {
				row = int(batch.Selection[active])
			}
			dropUndefinedStreamTags(element, groups.defined[groupCol[row]])
		}
		result = append(result, elements...)
	}
	return result, nil
}

// dropUndefinedStreamTags removes the tags outside defined, and any family left empty.
func dropUndefinedStreamTags(element *streamv1.Element, defined map[streamTagKey]struct{}) {
	families := element.TagFamilies[:0]
	for _, family := range element.TagFamilies {
		tags := family.Tags[:0]
		for _, tag := range family.Tags {
			if _, ok := defined[streamTagKey{family: family.Name, name: tag.Key}]; ok {
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
