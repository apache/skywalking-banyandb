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
	"bytes"
	"context"
	"fmt"
	"sort"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// mergeRowRef points at a single active row inside a consumed input batch. It
// buffers a reference (batch pointer + row index) rather than deep-copying the
// row, so the sort operates on lightweight handles and the row payload is
// gathered lazily during emission.
type mergeRowRef struct {
	batch  *vectorized.RecordBatch
	arrive int
	row    int
}

// SortedMerge is a globally-ordering breaker over stream RecordBatches.
//
// It Consumes every input batch (buffering row references), Finalize sorts them
// stably by the schema order key, and NextBatch emits key-ordered output batches
// sized to batchSize. Time-order keys on the Timestamp int64 column (stream
// timestamps are non-negative UnixNano, so a direct int64 compare matches the
// row path's convert.Uint64ToBytes big-endian byte order); index-order keys on
// the OrderKey comparable-bytes column (byte-lexicographic, matching the row
// path's MarshalTagValue bytes). asc/desc is honored; ties preserve input
// arrival order so downstream first-seen dedup stays deterministic.
type SortedMerge struct {
	schema    *vectorized.BatchSchema
	pool      *vectorized.BatchPool
	rows      []mergeRowRef
	batchSize int
	maxRows   int
	pos       int
	arriveSeq int
	orderKey  int
	tsIdx     int
	elemIdx   int
	desc      bool
	eof       bool
	closed    bool
}

// NewSortedMerge constructs a stream global-merge breaker over the given schema.
func NewSortedMerge(schema *vectorized.BatchSchema, desc bool, batchSize int) *SortedMerge {
	return &SortedMerge{
		schema:    schema,
		desc:      desc,
		batchSize: batchSize,
		orderKey:  schema.OrderKeyIndex(),
		tsIdx:     schema.TimestampIndex(),
		elemIdx:   schema.ElementIDIndex(),
	}
}

// NewSortedMergeWithCap constructs a stream global-merge breaker that keeps only
// the first maxRows rows in sort order after the stable sort — the correct
// in-order top-N. A maxRows of 0 means unbounded (equivalent to NewSortedMerge).
// The cap is applied AFTER the merge sorts, so it matches the row path's
// cap-after-in-order-merge semantics (blockHeap.merge / MergeStreamResults).
func NewSortedMergeWithCap(schema *vectorized.BatchSchema, desc bool, batchSize, maxRows int) *SortedMerge {
	s := NewSortedMerge(schema, desc, batchSize)
	if maxRows > 0 {
		s.maxRows = maxRows
	}
	return s
}

// Init sizes the batch size and prepares the output pool.
func (s *SortedMerge) Init(context.Context) error {
	if s.batchSize <= 0 {
		s.batchSize = vectorized.DefaultBatchSize
	}
	s.pool = vectorized.NewBatchPool(s.schema, s.batchSize)
	return nil
}

// OutputSchema returns the shared input/output schema.
func (s *SortedMerge) OutputSchema() *vectorized.BatchSchema {
	return s.schema
}

// Consume buffers references to every active row in the batch. The batch is
// retained (not copied); callers must not recycle a consumed batch until this
// operator is closed.
func (s *SortedMerge) Consume(_ context.Context, batch *vectorized.RecordBatch) error {
	if batch == nil || batch.ActiveLen() == 0 {
		return nil
	}
	if batch.Schema != s.schema {
		return fmt.Errorf("SortedMerge: foreign batch schema")
	}
	if batch.Selection == nil {
		for row := 0; row < batch.Len; row++ {
			s.rows = append(s.rows, mergeRowRef{batch: batch, row: row, arrive: s.arriveSeq})
			s.arriveSeq++
		}
		return nil
	}
	for _, sel := range batch.Selection {
		s.rows = append(s.rows, mergeRowRef{batch: batch, row: int(sel), arrive: s.arriveSeq})
		s.arriveSeq++
	}
	return nil
}

// Finalize stably sorts the buffered rows by the schema order key, then applies
// the in-order top-N cap.
//
// The cap keeps the first maxRows rows counted by UNIQUE ElementID, in sort
// order. This matches the row path's oracle exactly: MergeStreamResults /
// blockHeap.merge dedup by ElementID DURING their in-order merge and stop once
// the DEDUPED count reaches the limit (mergedResult.Len() < topN). Capping on
// the raw pre-dedup row count would drop unique rows the oracle keeps whenever a
// duplicate ElementID falls inside the retained window (an ElementID can span
// parts). Rows are truncated at the sort position where the maxRows-th distinct
// ElementID first appears, so the retained prefix carries every row (including
// still-undeduped duplicates) up to that boundary; the downstream Distinct then
// removes the duplicates from that already-correct top-N prefix.
func (s *SortedMerge) Finalize(context.Context) error {
	sort.SliceStable(s.rows, func(i, j int) bool {
		cmp := s.compare(s.rows[i], s.rows[j])
		if cmp == 0 {
			return s.rows[i].arrive < s.rows[j].arrive
		}
		if s.desc {
			return cmp > 0
		}
		return cmp < 0
	})
	if s.maxRows > 0 {
		s.capByDistinctElementID()
	}
	return nil
}

// capByDistinctElementID truncates the sorted rows at the first position past
// which maxRows distinct ElementIDs have been seen, so the retained prefix
// contains exactly the top-N unique elements in sort order (plus any duplicate
// rows interleaved before that boundary, which the downstream Distinct drops).
// When the schema carries no ElementID column it falls back to a raw row cap.
func (s *SortedMerge) capByDistinctElementID() {
	if s.elemIdx < 0 {
		if len(s.rows) > s.maxRows {
			s.rows = s.rows[:s.maxRows]
		}
		return
	}
	// Size the hint by the actual row count, not maxRows: a client "max limit"
	// query passes maxRows ≈ MaxUint32, and make(map, maxRows) would try to
	// pre-allocate billions of buckets (multi-GB) and hang. The distinct-ID count
	// can never exceed len(s.rows).
	capHint := s.maxRows
	if capHint > len(s.rows) {
		capHint = len(s.rows)
	}
	seen := make(map[int64]struct{}, capHint)
	for i := range s.rows {
		ref := s.rows[i]
		id := ref.batch.Columns[s.elemIdx].(*vectorized.TypedColumn[int64]).Data()[ref.row]
		if _, ok := seen[id]; ok {
			continue
		}
		if len(seen) == s.maxRows {
			s.rows = s.rows[:i]
			return
		}
		seen[id] = struct{}{}
	}
}

// compare returns -1/0/1 comparing the sort keys of two row refs. Index-order
// keys compare the OrderKey bytes column lexicographically; time-order keys
// compare the Timestamp int64 column directly.
func (s *SortedMerge) compare(a, b mergeRowRef) int {
	if s.orderKey >= 0 {
		ak := a.batch.Columns[s.orderKey].(*vectorized.TypedColumn[[]byte]).Data()[a.row]
		bk := b.batch.Columns[s.orderKey].(*vectorized.TypedColumn[[]byte]).Data()[b.row]
		return bytes.Compare(ak, bk)
	}
	at := a.batch.Columns[s.tsIdx].(*vectorized.TypedColumn[int64]).Data()[a.row]
	bt := b.batch.Columns[s.tsIdx].(*vectorized.TypedColumn[int64]).Data()[b.row]
	switch {
	case at < bt:
		return -1
	case at > bt:
		return 1
	default:
		return 0
	}
}

// NextBatch emits the next key-ordered output batch. Empty input returns EOF
// immediately.
func (s *SortedMerge) NextBatch(ctx context.Context) (*vectorized.RecordBatch, error) {
	if s.eof || s.pos >= len(s.rows) {
		s.eof = true
		return nil, nil
	}
	batch := s.pool.Get()
	for batch.Len < s.batchSize && s.pos < len(s.rows) {
		if ctxErr := ctx.Err(); ctxErr != nil {
			s.pool.Put(batch)
			return nil, ctxErr
		}
		ref := s.rows[s.pos]
		if appendErr := appendMergeRow(batch, ref); appendErr != nil {
			s.pool.Put(batch)
			return nil, appendErr
		}
		s.pos++
	}
	if batch.Len == 0 {
		s.pool.Put(batch)
		s.eof = true
		return nil, nil
	}
	return batch, nil
}

// Close is idempotent and releases buffered references.
func (s *SortedMerge) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	s.rows = nil
	return nil
}

// appendMergeRow gathers one full source row into the destination batch by
// appending each column cell through AppendColumnRange (one-row range).
func appendMergeRow(dst *vectorized.RecordBatch, ref mergeRowRef) error {
	for col := range dst.Columns {
		if appendErr := vectorized.AppendColumnRange(dst.Columns[col], ref.batch.Columns[col], ref.row, 1); appendErr != nil {
			return appendErr
		}
	}
	dst.Len++
	return nil
}
