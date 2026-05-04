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

package measure

import (
	"container/heap"
	"context"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// BatchTop is a BreakerOperator that retains the top-N rows by a designated
// field column. asc=true keeps the lowest N; asc=false keeps the highest N.
//
// Tie-break is stable on insertion order — earlier rows win. Nulls in the key
// column are treated as the lowest value (kept first in asc, evicted first in desc).
type BatchTop struct {
	schema     *vectorized.BatchSchema
	pool       *vectorized.BatchPool
	heapState  *topHeap
	sorted     []*topRow
	fieldCol   int
	n          int
	batchSize  int
	inputCount int
	cursor     int
	asc        bool
	closed     bool
}

// topRow materializes one input row plus its sort key for heap comparisons.
type topRow struct {
	cols     []vectorized.Column // schema-shaped, exactly 1 row each
	floatVal float64
	intVal   int64
	seq      int
	isNull   bool
	isFloat  bool
}

type topHeap struct {
	rows []*topRow
	asc  bool
}

func (h *topHeap) Len() int { return len(h.rows) }

func (h *topHeap) Less(i, j int) bool {
	c := cmpTopVal(h.rows[i], h.rows[j])
	if c != 0 {
		// asc → max-heap (largest at root, evict on overflow);
		// desc → min-heap (smallest at root).
		if h.asc {
			return c > 0
		}
		return c < 0
	}
	// Tie: prefer the later (larger seq) row at root so it gets evicted /
	// popped first. After Finalize reverses the pop order, ties surface in
	// insertion order — earlier-row-wins.
	return h.rows[i].seq > h.rows[j].seq
}

func (h *topHeap) Swap(i, j int) { h.rows[i], h.rows[j] = h.rows[j], h.rows[i] }

func (h *topHeap) Push(x any) { h.rows = append(h.rows, x.(*topRow)) }

func (h *topHeap) Pop() any {
	n := len(h.rows)
	x := h.rows[n-1]
	h.rows = h.rows[:n-1]
	return x
}

// cmpTopVal returns -1 / 0 / +1 for a < b / == / >. Nulls sort lowest.
func cmpTopVal(a, b *topRow) int {
	if a.isNull && b.isNull {
		return 0
	}
	if a.isNull {
		return -1
	}
	if b.isNull {
		return 1
	}
	if a.isFloat {
		switch {
		case a.floatVal < b.floatVal:
			return -1
		case a.floatVal > b.floatVal:
			return 1
		}
		return 0
	}
	switch {
	case a.intVal < b.intVal:
		return -1
	case a.intVal > b.intVal:
		return 1
	}
	return 0
}

// NewBatchTop constructs a BatchTop. fieldCol is the index of the int64 or
// float64 column to sort by; n is the bound; asc selects ascending or descending order.
func NewBatchTop(schema *vectorized.BatchSchema, fieldCol, n int, asc bool, batchSize int) *BatchTop {
	return &BatchTop{
		schema:    schema,
		pool:      vectorized.NewBatchPool(schema, batchSize),
		fieldCol:  fieldCol,
		n:         n,
		batchSize: batchSize,
		asc:       asc,
	}
}

// Init initialises the heap.
func (t *BatchTop) Init(_ context.Context) error {
	t.heapState = &topHeap{asc: t.asc}
	return nil
}

// OutputSchema returns the unchanged input schema.
func (t *BatchTop) OutputSchema() *vectorized.BatchSchema { return t.schema }

// Consume considers each active row for inclusion in the top-N heap.
//
// n <= 0 is a no-op (matches the row-path's top-N convention) — without this
// guard the bounded-heap logic would dereference an empty heap on the first row.
func (t *BatchTop) Consume(_ context.Context, b *vectorized.RecordBatch) error {
	if t.n <= 0 {
		return nil
	}
	active := activeIndices(b)
	isFloat := t.schema.Columns[t.fieldCol].Type == vectorized.ColumnTypeFloat64
	for _, rowIdx := range active {
		candidate := t.materialize(b, int(rowIdx), isFloat)
		candidate.seq = t.inputCount
		t.inputCount++
		if t.heapState.Len() < t.n {
			heap.Push(t.heapState, candidate)
			continue
		}
		root := t.heapState.rows[0]
		if t.shouldReplace(candidate, root) {
			t.heapState.rows[0] = candidate
			heap.Fix(t.heapState, 0)
		}
	}
	return nil
}

// shouldReplace returns true iff candidate is strictly better than root for
// the configured order. Strict comparison is what gives us stable tie-break:
// equal values do not replace.
func (t *BatchTop) shouldReplace(candidate, root *topRow) bool {
	c := cmpTopVal(candidate, root)
	if t.asc {
		return c < 0
	}
	return c > 0
}

// Finalize drains the heap into a sorted slice in user-facing order.
func (t *BatchTop) Finalize(_ context.Context) error {
	out := make([]*topRow, 0, t.heapState.Len())
	for t.heapState.Len() > 0 {
		out = append(out, heap.Pop(t.heapState).(*topRow))
	}
	// Pop order is reverse of desired: max-heap pops largest first (asc wants
	// smallest first); min-heap pops smallest first (desc wants largest first).
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	t.sorted = out
	t.heapState = &topHeap{asc: t.asc} // free heap backing slice
	return nil
}

// NextBatch emits the sorted rows in batches of batchSize.
func (t *BatchTop) NextBatch(_ context.Context) (*vectorized.RecordBatch, error) {
	if t.cursor >= len(t.sorted) {
		return nil, nil
	}
	out := t.pool.Get()
	for out.Len < t.batchSize && t.cursor < len(t.sorted) {
		row := t.sorted[t.cursor]
		for colIdx := range out.Columns {
			copyOneValue(out.Columns[colIdx], row.cols[colIdx], 0)
		}
		out.Len++
		t.cursor++
	}
	if out.Len == 0 {
		t.pool.Put(out)
		return nil, nil
	}
	return out, nil
}

// Close releases the heap and sorted buffer. Idempotent.
func (t *BatchTop) Close() error {
	if t.closed {
		return nil
	}
	t.closed = true
	t.heapState = nil
	t.sorted = nil
	return nil
}

// materialize copies row rowIdx of b into a new topRow, reading the sort key
// from the configured field column.
func (t *BatchTop) materialize(b *vectorized.RecordBatch, rowIdx int, isFloat bool) *topRow {
	cols := make([]vectorized.Column, len(t.schema.Columns))
	for i, def := range t.schema.Columns {
		cols[i] = vectorized.NewColumnForType(def.Type, 1)
		copyOneValue(cols[i], b.Columns[i], rowIdx)
	}
	row := &topRow{cols: cols, isFloat: isFloat}
	keyCol := b.Columns[t.fieldCol]
	if keyCol.IsNull(rowIdx) {
		row.isNull = true
		return row
	}
	if isFloat {
		row.floatVal = keyCol.(*vectorized.TypedColumn[float64]).Data()[rowIdx]
	} else {
		row.intVal = keyCol.(*vectorized.TypedColumn[int64]).Data()[rowIdx]
	}
	return row
}
