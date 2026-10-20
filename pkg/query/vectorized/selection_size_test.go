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

package vectorized

import (
	"context"
	"fmt"
	"math/rand"
	"slices"
	"testing"
)

// selectionCase names one way of addressing the rows of a batch and carries the
// row order a selection-aware consumer must observe for it.
type selectionCase struct {
	name      string
	selection []uint16
	order     []int
}

// selectionCases returns the selections a lent scan batch can carry: none, the
// descending scan's [n-1..0], a shuffled permutation and a reversed subset.
func selectionCases(n int) []selectionCase {
	identity := make([]int, n)
	reversedSel := make([]uint16, n)
	reversedOrder := make([]int, n)
	for i := range n {
		identity[i] = i
		reversedSel[i] = uint16(n - 1 - i)
		reversedOrder[i] = n - 1 - i
	}
	perm := rand.New(rand.NewSource(int64(n))).Perm(n) //nolint:gosec // deterministic test data
	shuffledSel := make([]uint16, n)
	for i, row := range perm {
		shuffledSel[i] = uint16(row)
	}
	subsetSel := make([]uint16, 0, n/2)
	subsetOrder := make([]int, 0, n/2)
	for i := n - 1; i >= 0; i-- {
		if i%2 == 1 {
			subsetSel = append(subsetSel, uint16(i))
			subsetOrder = append(subsetOrder, i)
		}
	}
	return []selectionCase{
		{name: "nil", order: identity},
		{name: "reversed", selection: reversedSel, order: reversedOrder},
		{name: "shuffled", selection: shuffledSel, order: perm},
		{name: "reversed-subset", selection: subsetSel, order: subsetOrder},
	}
}

func selectionTestSchema() *BatchSchema {
	return NewBatchSchema([]ColumnDef{
		{Role: RoleTimestamp, Type: ColumnTypeInt64},
		{Role: RoleTag, TagFamily: "default", Name: "g", Type: ColumnTypeString},
	})
}

// selectionTestBatch holds n rows whose timestamp is the storage row index, so a
// consumer's output identifies the storage row it came from. Row 3 is null in g.
func selectionTestBatch(schema *BatchSchema, n int, selection []uint16) *RecordBatch {
	b := NewRecordBatch(schema, n)
	ts := b.Columns[0].(*TypedColumn[int64])
	g := b.Columns[1].(*TypedColumn[string])
	for i := range n {
		ts.Append(int64(i))
		if i == 3 {
			g.AppendNull()
			continue
		}
		g.Append(fmt.Sprintf("g%d", i))
	}
	b.Len = n
	if selection != nil {
		b.Selection = slices.Clone(selection)
	}
	return b
}

func readOrder(t *testing.T, b *RecordBatch) []int {
	t.Helper()
	ts := b.Columns[0].(*TypedColumn[int64])
	g := b.Columns[1].(*TypedColumn[string])
	got := make([]int, 0, b.Len)
	for i := range b.Len {
		row := int(ts.Data()[i])
		got = append(got, row)
		if wantNull := row == 3; g.IsNull(i) != wantNull {
			t.Fatalf("output row %d (storage row %d): null=%v, want %v", i, row, g.IsNull(i), wantNull)
		}
		if !g.IsNull(i) && g.Data()[i] != fmt.Sprintf("g%d", row) {
			t.Fatalf("output row %d: g=%q does not belong to storage row %d", i, g.Data()[i], row)
		}
	}
	return got
}

// TestAppendActiveRows_FollowsSelectionOrder pins that the flatten helper emits
// rows in selection order, whatever that order and the batch size are.
func TestAppendActiveRows_FollowsSelectionOrder(t *testing.T) {
	schema := selectionTestSchema()
	for _, n := range []int{7, 1025, 8192, 65536} {
		for _, tc := range selectionCases(n) {
			t.Run(fmt.Sprintf("%s/%d", tc.name, n), func(t *testing.T) {
				src := selectionTestBatch(schema, n, tc.selection)
				dst := NewRecordBatch(schema, 0)
				if err := AppendActiveRows(dst, src); err != nil {
					t.Fatal(err)
				}
				if dst.Len != len(tc.order) {
					t.Fatalf("Len: want %d, got %d", len(tc.order), dst.Len)
				}
				if got := readOrder(t, dst); !slices.Equal(got, tc.order) {
					t.Fatalf("row order differs from selection order (first rows got %v want %v)", got[:min(5, len(got))], tc.order[:min(5, len(tc.order))])
				}
			})
		}
	}
}

// collectOp is a BreakerOperator that records the rows of every batch it consumes.
type collectOp struct {
	rows      []int
	maxChunk  int
	chunkHits int
}

func (*collectOp) Init(context.Context) error                      { return nil }
func (*collectOp) OutputSchema() *BatchSchema                      { return nil }
func (*collectOp) Close() error                                    { return nil }
func (*collectOp) Finalize(context.Context) error                  { return nil }
func (*collectOp) NextBatch(context.Context) (*RecordBatch, error) { return nil, nil }

func (c *collectOp) Consume(_ context.Context, b *RecordBatch) error {
	c.chunkHits++
	c.maxChunk = max(c.maxChunk, b.ActiveLen())
	ts := b.Columns[0].(*TypedColumn[int64])
	g := b.Columns[1].(*TypedColumn[string])
	if b.Selection != nil {
		return fmt.Errorf("a chunk must be dense, got a selection of %d", len(b.Selection))
	}
	for i := range b.Len {
		row := int(ts.Data()[i])
		if g.IsNull(i) != (row == 3) {
			return fmt.Errorf("null bit of storage row %d lost", row)
		}
		c.rows = append(c.rows, row)
	}
	return nil
}

// TestConsumeChunked_FollowsSelectionOrderAndBoundsChunks pins that chunked
// consumption visits the active rows once, in selection order, in chunks of at
// most batchSize rows, including for batches far larger than the chunk.
func TestConsumeChunked_FollowsSelectionOrderAndBoundsChunks(t *testing.T) {
	schema := selectionTestSchema()
	for _, n := range []int{7, 1025, 8192, 65536} {
		for _, tc := range selectionCases(n) {
			t.Run(fmt.Sprintf("%s/%d", tc.name, n), func(t *testing.T) {
				src := selectionTestBatch(schema, n, tc.selection)
				scratch := NewRecordBatch(schema, DefaultBatchSize)
				op := &collectOp{}
				if err := ConsumeChunked(op, scratch, src, DefaultBatchSize); err != nil {
					t.Fatal(err)
				}
				if !slices.Equal(op.rows, tc.order) {
					t.Fatalf("rows visited differ from selection order (got %d rows, want %d)", len(op.rows), len(tc.order))
				}
				if op.maxChunk > DefaultBatchSize {
					t.Fatalf("chunk of %d rows exceeds batchSize %d", op.maxChunk, DefaultBatchSize)
				}
			})
		}
	}
}
