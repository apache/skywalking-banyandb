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
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

const dupClosedState = "closed"

// dupCorpus is the duplicate-`ElementID` corpus in ascending order-key order.
// Every element exercises one clause of the contract:
//
//	element 1 — its first row in sort order FAILS the criteria and its second
//	    passes, so a matching version alone makes the element eligible.
//	element 2 — BOTH rows match, at different order keys, so the selected version
//	    is decided by the requested sort direction and flips between ASC and DESC.
//	element 3 — a matching row at a low key and a FAILING row at the highest key,
//	    so descending meets its failing version first and must still admit it.
//	element 4 — no matching row at all, so it must never appear.
//
// The service tag is the version marker: it is not part of the criteria, so
// asserting it names WHICH row of a duplicate the query selected.
func dupCorpus() []filterRow {
	return []filterRow{
		{orderKey: dupKey(0), elemID: 1, state: strTagValue(dupClosedState), service: "e1-first"},
		{orderKey: dupKey(1), elemID: 1, state: strTagValue(filterWantState), service: "e1-second"},
		{orderKey: dupKey(2), elemID: 2, state: strTagValue(filterWantState), service: "e2-first"},
		{orderKey: dupKey(3), elemID: 2, state: strTagValue(filterWantState), service: "e2-second"},
		{orderKey: dupKey(4), elemID: 3, state: strTagValue(filterWantState), service: "e3-only"},
		{orderKey: dupKey(5), elemID: 4, state: strTagValue(dupClosedState), service: "e4-only"},
		{orderKey: dupKey(6), elemID: 3, state: strTagValue(dupClosedState), service: "e3-late-fail"},
	}
}

func dupKey(n int) []byte { return []byte(orderKeyFor(n)) }

// dupResult is one selected element: its id, the version marker and criteria
// value of the row the query kept, and that row's sort key.
type dupResult struct {
	service  string
	state    string
	orderKey string
	id       uint64
}

// runDupQuery drives the filtered index-order pipeline over the split corpus and
// returns what the client receives, in order: the pipeline, then the egress
// criteria re-check, then the enclosing limit node's offset:offset+limit slice.
//
// capped is the production shape, bounding the merge at limit+offset. Uncapped is
// the control: it must select the same versions, which is what pins the bound as
// an optimization rather than part of the semantics.
func runDupQuery(t *testing.T, split [][]filterRow, desc, capped bool, offset, limit uint32) []dupResult {
	t.Helper()
	// One schema instance for every batch: SortedMerge validates batch.Schema by
	// pointer identity, so a per-batch filterSchema() call fails as a foreign
	// schema rather than exercising the merge.
	schema := filterSchema()
	batches := make([]*vectorized.RecordBatch, 0, len(split))
	for _, rows := range split {
		batches = append(batches, buildFilterBatch(schema, rows))
	}
	limitRows := uint32(math.MaxUint32)
	mergeCap := 0
	if capped {
		limitRows = offset + limit
		mergeCap = int(offset + limit)
	}
	pipeline, err := BuildStreamMergePipeline(
		newStaticBatchSource(schema, batches...),
		schema, desc, 0, limitRows, len(dupCorpus()), mergeCap,
		NewTagFilter(schema, filterProjection(), eqStateFilter(t), filterRegistry()))
	require.NoError(t, err)
	defer func() { require.NoError(t, pipeline.Close()) }()

	// The egress criteria re-check, which production still runs. Under the
	// filter-first contract every emitted row already passed, so it is a no-op
	// here; keeping it pins that it never removes a row the pushdown selected.
	kept := make([]dupResult, 0, len(dupCorpus()))
	for _, row := range drainDupRows(t, pipeline) {
		if row.state == filterWantState {
			kept = append(kept, row)
		}
	}
	if int(offset) >= len(kept) {
		return []dupResult{}
	}
	return kept[offset:min(int(offset+limit), len(kept))]
}

// drainDupRows drains a pipeline into the selected elements, honoring each output
// batch's active selection.
func drainDupRows(t *testing.T, p *vectorized.Pipeline) []dupResult {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, p.Init(ctx))
	schema := filterSchema()
	serviceIdx, ok := schema.TagIndex(testTagFamily, testTagName)
	require.True(t, ok)
	stateIdx, ok := schema.TagIndex(testTagFamily, testFilterTagName)
	require.True(t, ok)
	var rows []dupResult
	for {
		batch, err := p.Next(ctx)
		require.NoError(t, err)
		if batch == nil {
			return rows
		}
		idData := streamElementIDs(batch).Data()
		serviceData := batch.Columns[serviceIdx].(*vectorized.TypedColumn[*modelv1.TagValue]).Data()
		stateData := batch.Columns[stateIdx].(*vectorized.TypedColumn[*modelv1.TagValue]).Data()
		keyData := streamOrderKeys(batch).Data()
		for _, rowIdx := range activeIndices(batch) {
			rows = append(rows, dupResult{
				id:       ColumnToElementID(idData[rowIdx]),
				service:  serviceData[rowIdx].GetStr().GetValue(),
				state:    stateData[rowIdx].GetStr().GetValue(),
				orderKey: string(keyData[rowIdx]),
			})
		}
	}
}

// dupWant names an expected selection. Every selected row carries the criteria
// value by construction, so state is not a per-case parameter.
func dupWant(id uint64, service string, key int) dupResult {
	return dupResult{id: id, service: service, state: filterWantState, orderKey: orderKeyFor(key)}
}

// TestTagFilterDuplicateWinnerSemantics pins the duplicate-`ElementID` contract
// for a filtered index-order query: evaluate the criteria first, then among the
// MATCHING rows sharing an ElementID keep the first in the requested sort order.
// A matching version makes the element eligible even when another version fails.
//
// It asserts the selected version — its non-criteria tag value and its sort key —
// alongside the ordered ids, because an element can be right and its representative
// row wrong. Element 2 carries two matching versions, so the selected one flips
// between ASC and DESC; that is the clause a membership-only assertion misses.
//
// No other fixture in the tree combines repeated element ids WITH the criteria
// filter, so nothing else can catch a regression in this rule.
func TestTagFilterDuplicateWinnerSemantics(t *testing.T) {
	splits := []struct {
		name  string
		split [][]filterRow
	}{
		{
			name:  "single batch",
			split: [][]filterRow{dupCorpus()},
		},
		{
			// Each duplicate pair straddles the boundary and the batches arrive out
			// of order-key order, so the merge — not the batch layout — decides which
			// version is first in sort order.
			name: "duplicates straddle a batch boundary",
			split: [][]filterRow{
				{dupCorpus()[0], dupCorpus()[2], dupCorpus()[5], dupCorpus()[6]},
				{dupCorpus()[1], dupCorpus()[3], dupCorpus()[4]},
			},
		},
	}
	cases := []struct {
		name   string
		want   []dupResult
		offset uint32
		limit  uint32
		desc   bool
		capped bool
	}{
		{
			// Element 1 enters on its second row, element 2 on its earlier matching
			// row, and element 4 never matches.
			name:   "ascending keeps the earliest matching version",
			limit:  4,
			capped: true,
			want: []dupResult{
				dupWant(1, "e1-second", 1),
				dupWant(2, "e2-first", 2),
				dupWant(3, "e3-only", 4),
			},
		},
		{
			// Descending reverses the requested sort order, so element 2 is now
			// represented by its LATER matching row. Element 1 has only one matching
			// row, so its version does not move.
			name:   "descending keeps the latest matching version",
			limit:  4,
			desc:   true,
			capped: true,
			want: []dupResult{
				dupWant(3, "e3-only", 4),
				dupWant(2, "e2-second", 3),
				dupWant(1, "e1-second", 1),
			},
		},
		{
			name:  "the merge cap does not change the selected versions",
			limit: 4,
			want: []dupResult{
				dupWant(1, "e1-second", 1),
				dupWant(2, "e2-first", 2),
				dupWant(3, "e3-only", 4),
			},
		},
		{
			// A cap of 3 binds against 4 matching rows, so a cap counting ROWS rather
			// than distinct ElementIDs spends a slot on element 2's second version and
			// returns two elements instead of three.
			name:   "the cap counts distinct elements, not rows",
			limit:  3,
			capped: true,
			want: []dupResult{
				dupWant(1, "e1-second", 1),
				dupWant(2, "e2-first", 2),
				dupWant(3, "e3-only", 4),
			},
		},
		{
			// The cap is limit+offset while the client slice is offset:offset+limit,
			// so an off-by-one in either shows up as the wrong single element.
			name:   "offset and limit select one element from the middle",
			offset: 1,
			limit:  1,
			capped: true,
			want:   []dupResult{dupWant(2, "e2-first", 2)},
		},
		{
			name:   "offset and limit descending",
			offset: 1,
			limit:  1,
			desc:   true,
			capped: true,
			want:   []dupResult{dupWant(2, "e2-second", 3)},
		},
	}
	for _, split := range splits {
		for _, tt := range cases {
			t.Run(split.name+"/"+tt.name, func(t *testing.T) {
				require.Equal(t, tt.want, runDupQuery(t, split.split, tt.desc, tt.capped, tt.offset, tt.limit))
			})
		}
	}
}

// TestTagFilterDuplicateTieOnSortKey pins the clause the main matrix cannot reach.
// Two versions of one element normally carry the SAME ordered-tag value, so their
// sort keys tie and "first in the requested sort order" does not discriminate. The
// merge breaks such ties on arrival order, which is direction-independent — so the
// same version is selected ascending and descending.
func TestTagFilterDuplicateTieOnSortKey(t *testing.T) {
	corpus := []filterRow{
		{orderKey: dupKey(0), elemID: 1, state: strTagValue(dupClosedState), service: "e1-fail"},
		{orderKey: dupKey(1), elemID: 1, state: strTagValue(filterWantState), service: "e1-tie-a"},
		{orderKey: dupKey(1), elemID: 1, state: strTagValue(filterWantState), service: "e1-tie-b"},
		{orderKey: dupKey(2), elemID: 2, state: strTagValue(filterWantState), service: "e2-only"},
	}
	split := [][]filterRow{corpus}
	require.Equal(t, []dupResult{dupWant(1, "e1-tie-a", 1), dupWant(2, "e2-only", 2)},
		runDupQuery(t, split, false, true, 0, 2), "ascending")
	require.Equal(t, []dupResult{dupWant(2, "e2-only", 2), dupWant(1, "e1-tie-a", 1)},
		runDupQuery(t, split, true, true, 0, 2), "descending")
}
