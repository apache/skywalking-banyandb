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

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// TagFilter narrows a batch's selection to the rows a criteria tag filter keeps.
// Applied BEFORE SortedMerge, it is what makes a merge cap sound for a criteria
// query: the merge then bounds the top-N of the FILTERED set instead of filtering
// a set that was already truncated.
//
// It evaluates the same logical.TagFilter the egress evaluates, over the same
// cells. Hidden-tag stripping is NOT done here — it operates on materialized
// Elements and stays at the egress.
type TagFilter struct {
	filter   logical.TagFilter
	registry logical.TagSpecRegistry
	schema   *vectorized.BatchSchema
	cols     [][]int
	closed   bool
}

// NewTagFilter builds the pre-merge criteria filter. projection MUST be the same
// []model.TagProjection the egress materializes from and registry MUST be the
// projected schema the *tagFilterPlan carries: together they define the
// (tagFamilyIdx, tagIdx) space the filter indexes with.
//
// Columns are resolved by NAME, exactly as BuildElementsFromBatch resolves them.
// Positional resolution would be wrong wherever the executed schema carries a
// column the client projection does not (an ordered tag added for the OrderKey).
func NewTagFilter(schema *vectorized.BatchSchema, projection []model.TagProjection,
	filter logical.TagFilter, registry logical.TagSpecRegistry,
) *TagFilter {
	cols := make([][]int, 0, len(projection))
	for _, proj := range projection {
		family := make([]int, 0, len(proj.Names))
		for _, tagName := range proj.Names {
			colIdx, ok := schema.TagIndex(proj.Family, tagName)
			if !ok {
				// The egress substitutes NullTagValue for a projected tag with no
				// column rather than failing; mirror that instead of erroring.
				colIdx = -1
			}
			family = append(family, colIdx)
		}
		cols = append(cols, family)
	}
	return &TagFilter{schema: schema, filter: filter, registry: registry, cols: cols}
}

// Init is a no-op: the filter carries no cross-batch state.
func (t *TagFilter) Init(context.Context) error { return nil }

// OutputSchema returns the unchanged input schema.
func (t *TagFilter) OutputSchema() *vectorized.BatchSchema { return t.schema }

// Process rewrites the selection to the matching rows. A Match error aborts the
// query, matching the egress filter.
func (t *TagFilter) Process(_ context.Context, batch *vectorized.RecordBatch) error {
	// Same guard SortedMerge.Consume applies. It matters here because this operator
	// is the first to touch a raw batch's columns, so without it a foreign schema
	// would panic on the column type assertion instead of erroring in the merge.
	if batch.Schema != t.schema {
		return fmt.Errorf("TagFilter: foreign batch schema")
	}
	acc := &tagRowAccessor{batch: batch, cols: t.cols}
	// activeIndices is deliberately not used: it materializes a []uint16 for a nil
	// selection, which every raw scan batch has, so it would allocate per batch.
	if batch.Selection == nil {
		out := make([]uint16, 0, batch.Len)
		for row := 0; row < batch.Len; row++ {
			acc.row = row
			matched, matchErr := t.filter.Match(acc, t.registry)
			if matchErr != nil {
				return matchErr
			}
			if matched {
				// Safe: VectorizedConfig.Validate caps BatchSize at MaxUint16.
				out = append(out, uint16(row))
			}
		}
		batch.Selection = out
		return nil
	}
	out := make([]uint16, 0, len(batch.Selection))
	for _, sel := range batch.Selection {
		acc.row = int(sel)
		matched, matchErr := t.filter.Match(acc, t.registry)
		if matchErr != nil {
			return matchErr
		}
		if matched {
			out = append(out, sel)
		}
	}
	batch.Selection = out
	return nil
}

// Close is idempotent and a no-op.
func (t *TagFilter) Close() error {
	if t.closed {
		return nil
	}
	t.closed = true
	return nil
}

// tagRowAccessor presents one batch row to a logical.TagFilter. It is the
// egress' per-cell resolution (BuildElementsFromBatch) hoisted to the columnar
// side, so the filter sees the identical *modelv1.TagValue the egress would put
// in the Element.
type tagRowAccessor struct {
	batch *vectorized.RecordBatch
	cols  [][]int
	row   int
}

// GetTagValue implements logical.TagValueIndexAccessor.
func (a *tagRowAccessor) GetTagValue(tagFamilyIdx, tagIdx int) *modelv1.TagValue {
	// Out of range returns nil, exactly as logical.TagFamilies does, so tagExpr
	// raises the same ErrTagNotDefined rather than silently failing to match.
	if tagFamilyIdx < 0 || tagFamilyIdx >= len(a.cols) {
		return nil
	}
	family := a.cols[tagFamilyIdx]
	if tagIdx < 0 || tagIdx >= len(family) {
		return nil
	}
	colIdx := family[tagIdx]
	if colIdx < 0 {
		return pbv1.NullTagValue
	}
	col := a.batch.Columns[colIdx].(*vectorized.TypedColumn[*modelv1.TagValue])
	// The validity bitmap is the source of truth: AppendColumnRange appends the
	// source pointer and only then marks the row null, so a null row can retain a
	// stale non-nil value. The nil check guards producers that leave a cell empty
	// without marking it. Both collapse to the singleton the egress substitutes —
	// returning nil here would make tagExpr fail the whole query instead.
	if col.IsNull(a.row) {
		return pbv1.NullTagValue
	}
	if value := col.Data()[a.row]; value != nil {
		return value
	}
	return pbv1.NullTagValue
}
