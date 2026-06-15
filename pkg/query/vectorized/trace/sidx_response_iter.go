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

package trace

import (
	"fmt"
)

// SidxRowBatch is the sidx row shape consumed by vectorized trace Phase 1.
type SidxRowBatch struct {
	Error   error
	Keys    []int64
	Data    [][]byte
	SIDs    []int64
	PartIDs []int64
}

// Len returns the number of rows in the batch.
func (b *SidxRowBatch) Len() int {
	return len(b.Keys)
}

// SidxResponseIterator adapts ordered sidx row batches to merge items.
type SidxResponseIterator struct {
	curr      *MergeItem
	err       error
	responses []*SidxRowBatch
	batchIdx  int
	rowIdx    int
	closed    bool
}

// NewSidxResponseIterator constructs a merge iterator over sidx row batches.
func NewSidxResponseIterator(responses []*SidxRowBatch) *SidxResponseIterator {
	return &SidxResponseIterator{responses: responses, rowIdx: -1}
}

// Next advances to the next well-formed row.
func (i *SidxResponseIterator) Next() bool {
	if i.err != nil || i.closed {
		return false
	}
	for i.batchIdx < len(i.responses) {
		resp := i.responses[i.batchIdx]
		if resp == nil {
			i.batchIdx++
			i.rowIdx = -1
			continue
		}
		if resp.Error != nil {
			i.err = resp.Error
			return false
		}
		nextRowIdx := i.rowIdx + 1
		if nextRowIdx >= resp.Len() {
			i.batchIdx++
			i.rowIdx = -1
			continue
		}
		if validateErr := validateSidxResponseRow(resp, nextRowIdx); validateErr != nil {
			i.err = validateErr
			return false
		}
		i.rowIdx = nextRowIdx
		i.curr = NewMergeItem(resp.Keys[i.rowIdx], resp.SIDs[i.rowIdx], resp.PartIDs[i.rowIdx], resp.Data[i.rowIdx])
		return true
	}
	return false
}

// Val returns the current merge item.
func (i *SidxResponseIterator) Val() *MergeItem {
	return i.curr
}

// Close marks the iterator closed.
func (i *SidxResponseIterator) Close() error {
	i.closed = true
	return nil
}

// Error returns the first response or shape error observed by Next.
func (i *SidxResponseIterator) Error() error {
	return i.err
}

func validateSidxResponseRow(resp *SidxRowBatch, rowIdx int) error {
	if rowIdx >= len(resp.Keys) {
		return fmt.Errorf("sidx response row %d missing key", rowIdx)
	}
	if rowIdx >= len(resp.Data) {
		return fmt.Errorf("sidx response row %d missing data", rowIdx)
	}
	if rowIdx >= len(resp.SIDs) {
		return fmt.Errorf("sidx response row %d missing series id", rowIdx)
	}
	if rowIdx >= len(resp.PartIDs) {
		return fmt.Errorf("sidx response row %d missing part id", rowIdx)
	}
	if _, decodeErr := decodeTraceIDPayload(resp.Data[rowIdx]); decodeErr != nil {
		return fmt.Errorf("sidx response row %d invalid trace id payload: %w", rowIdx, decodeErr)
	}
	return nil
}
