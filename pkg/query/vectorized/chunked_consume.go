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

import "context"

// ConsumeChunked feeds b's active rows to op.Consume in slices of at most
// batchSize rows, copying each slice into scratch instead of handing op the
// whole batch.
//
// It exists because RecordBatch.Selection is []uint16, so an operator that
// materializes its active rows as a selection over [0, Len) wraps silently
// once Len exceeds 65,536: early rows are consumed a second time and the rows
// past the wrap are never visited, with no error. Any BreakerOperator handed a
// batch it did not size itself shares that exposure, so calling this rather
// than op.Consume(b) is what keeps such a batch from reaching it whole. See
// docs/design/0.12.0/limit-after-aggregation §5.
//
// Widening Selection to []uint32 was considered and rejected: that would let a
// consumer materialize an arbitrarily large working set and merely survive
// indexing it, whereas chunking keeps the per-step footprint bounded by
// batchSize regardless of what the producer sent. Note the bound is on this
// function's working set only -- b itself stays fully resident for the
// duration of the call, and only one chunk is copied at a time.
//
// A non-nil Selection on b is walked rather than discarded, so a caller's
// pre-selection is preserved. scratch must share b's column layout; it is
// Reset before each chunk, so every row it carries is active by construction.
func ConsumeChunked(op BreakerOperator, scratch, b *RecordBatch, batchSize int) error {
	if b.Selection != nil {
		for start := 0; start < len(b.Selection); start += batchSize {
			end := min(start+batchSize, len(b.Selection))
			scratch.Reset()
			for _, rowIdx := range b.Selection[start:end] {
				for colIdx := range b.Columns {
					if appendErr := AppendColumnRange(scratch.Columns[colIdx], b.Columns[colIdx], int(rowIdx), 1); appendErr != nil {
						return appendErr
					}
				}
				scratch.Len++
			}
			if consumeErr := op.Consume(context.Background(), scratch); consumeErr != nil {
				return consumeErr
			}
		}
		return nil
	}
	// Selection is nil, so b's active rows are implicitly [0, b.Len) -- and
	// b.Len is exactly the unbounded quantity described above, so it is walked
	// with a plain int and never materialized as a []uint16 selection itself.
	// Each chunk is contiguous, so one AppendColumnRange per column copies it.
	for start := 0; start < b.Len; start += batchSize {
		end := min(start+batchSize, b.Len)
		scratch.Reset()
		for colIdx := range b.Columns {
			if appendErr := AppendColumnRange(scratch.Columns[colIdx], b.Columns[colIdx], start, end-start); appendErr != nil {
				return appendErr
			}
		}
		scratch.Len = end - start
		if consumeErr := op.Consume(context.Background(), scratch); consumeErr != nil {
			return consumeErr
		}
	}
	return nil
}
