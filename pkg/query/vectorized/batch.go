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

// DefaultBatchSize is the canonical batch row capacity of operator output.
// uint16 selection vectors cap batch size at 65536. Scan batches lent from
// storage blocks may be larger (up to a block, 8,192 rows) and are never padded
// or split to this size, so an operator must size itself from Len/ActiveLen, not
// from this constant.
const DefaultBatchSize = 1024

// RecordBatch is the unit of work flowing through the pipeline.
// All columns share Len logical rows. Selection optionally narrows that to a subset.
//
// Selection contract:
//   - nil  → all rows in [0, Len) are active
//   - []   → zero rows active (post-fusible-filter empty case)
//   - non-empty → only the listed row indices are active
//
// Operators must use ActiveLen() to compute the effective row count.
type RecordBatch struct {
	Schema    *BatchSchema
	Columns   []Column
	Selection []uint16
	// Owners pins the memory that string, bytes and string-array cells borrow
	// without copying. Reset and BatchPool.Put release it; an operator that
	// keeps a cell beyond the batch's life must clone it first (see
	// vectorized/measure's cloneOneValue) or pin the same owners.
	Owners Owners
	// Hints describes the batch's rows to an operator that can exploit it. A
	// producer sets it only when it is exact, and an operator that copies rows into
	// another batch does not carry it over (see BatchHints).
	Hints BatchHints
	Len   int
	// Lent marks a batch whose Columns were lent by a storage batch that the
	// batch's Owners hold. The columns return to their pools when Owners are
	// released, so a lent batch is single-use: Reset drops its columns instead
	// of clearing them and BatchPool.Put discards it rather than pooling it.
	Lent bool
}

// NewRecordBatch allocates a batch with column capacities sized to capacity rows.
func NewRecordBatch(schema *BatchSchema, capacity int) *RecordBatch {
	cols := make([]Column, len(schema.Columns))
	for i, def := range schema.Columns {
		cols[i] = NewColumnForType(def.Type, capacity)
	}
	return &RecordBatch{Schema: schema, Columns: cols}
}

// ReleaseOwners drops the references this batch holds on borrowed memory.
// After it returns no borrowed string, bytes or string-array cell of the batch
// may be read. It is idempotent.
func (b *RecordBatch) ReleaseOwners() { b.Owners.Release() }

// Reset clears every column and the selection vector and releases the batch's
// owners. Schema is preserved.
func (b *RecordBatch) Reset() {
	b.Owners.Release()
	b.Len = 0
	b.Selection = nil
	b.Hints = BatchHints{}
	if b.Lent {
		// The columns went back to their pools with the owners; clearing them
		// here would race with whoever acquired them next.
		b.Columns = nil
		return
	}
	for _, c := range b.Columns {
		c.Reset()
	}
}

// ActiveLen returns the number of rows operators should process.
//
// nil Selection → Len. Non-nil Selection → len(Selection), even when zero.
func (b *RecordBatch) ActiveLen() int {
	if b.Selection == nil {
		return b.Len
	}
	return len(b.Selection)
}

// TimeOrder is the direction in which timestamps run within a single series of a
// batch's active rows.
type TimeOrder int8

// TimeOrder values.
const (
	// TimeOrderUnknown promises nothing.
	TimeOrderUnknown TimeOrder = 0
	// TimeOrderAsc means timestamps never decrease from one active row to the next
	// of the same series run.
	TimeOrderAsc TimeOrder = 1
	// TimeOrderDesc means timestamps never increase.
	TimeOrderDesc TimeOrder = -1
)

// BatchHints are facts a producer knows about a batch and a consumer would have to
// scan the rows to rediscover. They let an aggregation split a batch into runs of
// rows that share a grouping key (see SeriesRuns) without looking at every row.
//
// Every hint is relative to the rows as the producer emitted them. A Selection
// that only narrows the batch keeps them true: it keeps the order of the rows it
// names. An operator that reorders rows, or copies them into a batch of its own,
// must not pass a hint on.
type BatchHints struct {
	// SeriesEnds lists, for a batch that holds several series, the exclusive end
	// row index (in column order) of every maximal run of rows of one series. It is
	// meaningful only when SeriesKnown is set; empty then means the batch is a
	// single series. The slice is borrowed from the producer and valid until the
	// batch's Owners are released.
	SeriesEnds []int
	// TimeOrder is the direction of time within each series run, in the order the
	// batch's active rows are read.
	TimeOrder TimeOrder
	// SeriesKnown is set when SeriesEnds is exact. Without it nothing is known about
	// which series the rows belong to, and an empty SeriesEnds means nothing.
	SeriesKnown bool
}

// SeriesRuns appends to dst the exclusive end of every maximal run of consecutive
// active rows that belong to one series, as positions in the active order (the
// i-th active row is row i of a nil Selection, Selection[i] otherwise), and reports
// true. It reports false, leaving dst alone, when the batch does not say which
// series its rows belong to (Hints.SeriesKnown is unset). A batch with no active
// rows yields no runs.
//
// The runs are the generic description of where a grouping key changes: a key that
// is a function of the series is constant within each run, so an aggregation looks
// its group up once per run instead of once per row.
func (b *RecordBatch) SeriesRuns(dst []int) ([]int, bool) {
	if !b.Hints.SeriesKnown {
		return dst, false
	}
	n := b.ActiveLen()
	if n == 0 {
		return dst, true
	}
	ends := b.Hints.SeriesEnds
	if len(ends) == 0 {
		return append(dst, n), true
	}
	if b.Selection == nil {
		for _, end := range ends {
			if end >= n {
				break
			}
			dst = append(dst, end)
		}
		return append(dst, n), true
	}
	// Walk the selection, tracking the series run of the row before. The selection
	// may run forwards or backwards, so a row outside the cached run is located by
	// binary search.
	runLo, runHi := -1, -1
	for pos, row := range b.Selection {
		r := int(row)
		if r >= runLo && r < runHi {
			continue
		}
		if pos > 0 {
			dst = append(dst, pos)
		}
		i := searchEnds(ends, r+1)
		runHi = ends[i]
		runLo = 0
		if i > 0 {
			runLo = ends[i-1]
		}
	}
	return append(dst, n), true
}

// searchEnds returns the index of the first element of ends that is >= v, or the
// last index when v is past them all.
func searchEnds(ends []int, v int) int {
	lo, hi := 0, len(ends)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if ends[mid] < v {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo == len(ends) {
		return len(ends) - 1
	}
	return lo
}
