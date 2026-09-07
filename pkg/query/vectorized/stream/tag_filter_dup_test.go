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

// dupCorpus is a conflicting-duplicate corpus in ascending index order. Elements
// 1 and 3 each carry two rows whose criteria values DISAGREE, and in both cases
// the row that sorts first is the one that fails the criteria. Elements 2 and 4
// are single-row controls that every rule keeps.
//
// The losing row of each pair sorts before its winning row, so the corpus reads
// the same under the row path's storage-order rule as under the vec path's
// sort-order rule. What the two pipelines disagree about is therefore the
// duplicate-winner rule alone — not the merge cap, and not the batch layout.
func dupCorpus() []filterRow {
	return []filterRow{
		{orderKey: dupKey(0), elemID: 1, state: strTagValue(dupClosedState)},
		{orderKey: dupKey(1), elemID: 1, state: strTagValue(filterWantState)},
		{orderKey: dupKey(2), elemID: 2, state: strTagValue(filterWantState)},
		{orderKey: dupKey(3), elemID: 3, state: strTagValue(dupClosedState)},
		{orderKey: dupKey(4), elemID: 3, state: strTagValue(filterWantState)},
		{orderKey: dupKey(5), elemID: 4, state: strTagValue(filterWantState)},
	}
}

func dupKey(n int) []byte { return []byte(orderKeyFor(n)) }

// dupArm is one of the pipeline shapes under comparison. preMerge runs the
// criteria as a pre-merge fusible (this PR); capped bounds the merge at
// limit+offset. The pre-change shape is neither: it ran the merge UNCAPPED with a
// pass-through Limit and applied the criteria at the egress.
type dupArm struct {
	name     string
	preMerge bool
	capped   bool
}

// runDupArm drives one pipeline shape over the split corpus and returns the
// element ids the client receives, in order: the pipeline, then the egress
// criteria re-check, then the enclosing limit node's offset:offset+limit slice.
func runDupArm(t *testing.T, arm dupArm, split [][]filterRow, offset, limit uint32) []uint64 {
	t.Helper()
	// One schema instance for every batch: SortedMerge validates batch.Schema by
	// pointer identity, so a per-batch filterSchema() call fails as a foreign
	// schema rather than exercising the merge.
	schema := filterSchema()
	batches := make([]*vectorized.RecordBatch, 0, len(split))
	for _, rows := range split {
		batches = append(batches, buildFilterBatch(schema, rows))
	}
	maxElementSize := int(offset + limit)
	limitRows := uint32(math.MaxUint32)
	mergeCap := 0
	if arm.capped {
		limitRows = uint32(maxElementSize)
		mergeCap = maxElementSize
	}
	var preMerge []vectorized.FusibleOperator
	if arm.preMerge {
		preMerge = append(preMerge, NewTagFilter(schema, filterProjection(), eqStateFilter(t), filterRegistry()))
	}
	pipeline, err := BuildStreamMergePipeline(
		newStaticBatchSource(schema, batches...),
		schema, false, 0, limitRows, len(dupCorpus()), mergeCap, preMerge...)
	require.NoError(t, err)
	defer func() { require.NoError(t, pipeline.Close()) }()

	ids, states := drainDupRows(t, pipeline)
	// The egress criteria re-check, which runs on both shapes. On the pushdown
	// arm every row already passed, so it is a no-op there by construction.
	kept := make([]uint64, 0, len(ids))
	for idx, id := range ids {
		if states[idx] == filterWantState {
			kept = append(kept, id)
		}
	}
	if int(offset) >= len(kept) {
		return []uint64{}
	}
	return kept[offset:min(int(offset+limit), len(kept))]
}

// drainDupRows drains a pipeline into parallel element-id and criteria-value
// slices, honoring each output batch's active selection.
func drainDupRows(t *testing.T, p *vectorized.Pipeline) (ids []uint64, states []string) {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, p.Init(ctx))
	stateIdx, ok := filterSchema().TagIndex(testTagFamily, testFilterTagName)
	require.True(t, ok)
	for {
		batch, err := p.Next(ctx)
		require.NoError(t, err)
		if batch == nil {
			return ids, states
		}
		idData := streamElementIDs(batch).Data()
		stateData := batch.Columns[stateIdx].(*vectorized.TypedColumn[*modelv1.TagValue]).Data()
		for _, rowIdx := range activeIndices(batch) {
			ids = append(ids, ColumnToElementID(idData[rowIdx]))
			states = append(states, stateData[rowIdx].GetStr().GetValue())
		}
	}
}

// TestTagFilterDuplicateWinnerSemantics pins the exact ordered element ids a
// filtered index-order query returns when one element has several rows that
// disagree about the criteria, which is the case apache/skywalking#14056 R1
// ("same elements and order") turns on and which no other test in the tree
// covers — every fixture elsewhere carries unique element ids.
//
// The two shapes implement different rules and this test states both:
//
//	egress filter (pre-change): an element survives iff its FIRST row in sort
//	    order matches, because the merge and Distinct pick that row before the
//	    criteria is ever evaluated.
//	pre-merge filter (this PR): an element survives iff ANY of its rows matches,
//	    because the rows that fail are gone before the merge picks a winner.
//
// The uncapped pushdown arm is the control: it returns the same ids as the capped
// one, so the merge cap does not cause the divergence and no cap can repair it.
//
// Once the duplicate-`ElementID` contract is settled the losing arm goes away and
// the survivor becomes the single assertion.
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
			// of index order, so the merge — not the batch layout — picks the winner.
			name: "duplicates straddle a batch boundary",
			split: [][]filterRow{
				{dupCorpus()[0], dupCorpus()[2], dupCorpus()[3]},
				{dupCorpus()[1], dupCorpus()[4], dupCorpus()[5]},
			},
		},
	}
	cases := []struct {
		name   string
		arm    dupArm
		want   []uint64
		offset uint32
		limit  uint32
	}{
		{
			// Winners in index order are 1(closed) 2(open) 3(closed) 4(open); the
			// criteria keeps the two controls.
			name:  "egress filter keeps only elements whose first row matches",
			arm:   dupArm{name: "egress"},
			limit: 3,
			want:  []uint64{2, 4},
		},
		{
			// Both losing rows are dropped before the merge, so elements 1 and 3
			// re-enter on their second row and displace element 4 under the cap.
			name:  "pushdown admits elements on a later matching row",
			arm:   dupArm{name: "pushdown", preMerge: true, capped: true},
			limit: 3,
			want:  []uint64{1, 2, 3},
		},
		{
			name:  "pushdown without the cap returns the same ids",
			arm:   dupArm{name: "pushdown-uncapped", preMerge: true},
			limit: 3,
			want:  []uint64{1, 2, 3},
		},
		{
			// The offset slices a different element off each rule's answer, so the
			// two shapes return disjoint single-element pages.
			name:   "offset and limit slice disjoint pages",
			arm:    dupArm{name: "egress"},
			offset: 1,
			limit:  1,
			want:   []uint64{4},
		},
		{
			name:   "offset and limit slice disjoint pages under the pushdown",
			arm:    dupArm{name: "pushdown", preMerge: true, capped: true},
			offset: 1,
			limit:  1,
			want:   []uint64{2},
		},
	}
	for _, split := range splits {
		for _, tt := range cases {
			t.Run(split.name+"/"+tt.name, func(t *testing.T) {
				require.Equal(t, tt.want, runDupArm(t, tt.arm, split.split, tt.offset, tt.limit))
			})
		}
	}
}
