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
	Len    int
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
