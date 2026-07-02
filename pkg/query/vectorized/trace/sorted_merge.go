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
	"context"

	itersort "github.com/apache/skywalking-banyandb/pkg/iter/sort"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// MergeItem is one sorted Phase-1 trace candidate consumed by SortedMerge.
type MergeItem struct {
	TraceID   string
	Payload   []byte
	Key       int64
	SeriesID  int64
	PartID    int64
	sortField [8]byte
}

// NewMergeItem builds a merge item whose SortedField preserves numeric int64 order.
func NewMergeItem(key, seriesID, partID int64, payload []byte) *MergeItem {
	return &MergeItem{
		sortField: encodeInt64SortKey(key),
		Key:       key,
		SeriesID:  seriesID,
		PartID:    partID,
		Payload:   payload,
	}
}

// SortedField returns the order-preserving encoded key for itersort.
func (m *MergeItem) SortedField() []byte {
	return m.sortField[:]
}

// SortedMerge lazily merges sorted trace candidate iterators and deduplicates payloads.
type SortedMerge struct {
	schema    *vectorized.BatchSchema
	pool      *vectorized.BatchPool
	seen      map[string]struct{}
	merger    itersort.Iterator[*MergeItem]
	iters     []itersort.Iterator[*MergeItem]
	batchSize int
	desc      bool
	eof       bool
	closed    bool
}

// NewSortedMerge returns a streaming pull operator backed by itersort.NewItemIter.
func NewSortedMerge(iters []itersort.Iterator[*MergeItem], desc bool, batchSize int) *SortedMerge {
	return &SortedMerge{
		schema:    NewPhase1Schema(),
		iters:     iters,
		desc:      desc,
		batchSize: batchSize,
		seen:      make(map[string]struct{}),
	}
}

// Init initializes the merge heap and output pool.
func (s *SortedMerge) Init(context.Context) error {
	if s.batchSize <= 0 {
		s.batchSize = vectorized.DefaultBatchSize
	}
	s.pool = vectorized.NewBatchPool(s.schema, s.batchSize)
	s.merger = itersort.NewItemIter(s.iters, s.desc)
	return nil
}

// OutputSchema returns the Phase-1 schema.
func (s *SortedMerge) OutputSchema() *vectorized.BatchSchema {
	return s.schema
}

// NextBatch returns the next demand-bounded merged batch.
func (s *SortedMerge) NextBatch(ctx context.Context) (*vectorized.RecordBatch, error) {
	if s.eof {
		return nil, nil
	}
	batch := s.pool.Get()
	for batch.Len < s.batchSize {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		if !s.merger.Next() {
			if iterErr := s.iteratorError(); iterErr != nil {
				return nil, iterErr
			}
			s.eof = true
			break
		}
		item := s.merger.Val()
		payloadKey := string(item.Payload)
		if _, ok := s.seen[payloadKey]; ok {
			continue
		}
		s.seen[payloadKey] = struct{}{}
		appendMergeItem(batch, item)
	}
	if batch.Len == 0 {
		s.pool.Put(batch)
		return nil, nil
	}
	return batch, nil
}

type errorIterator interface {
	Error() error
}

func (s *SortedMerge) iteratorError() error {
	for _, iter := range s.iters {
		errorIter, ok := iter.(errorIterator)
		if !ok {
			continue
		}
		if iterErr := errorIter.Error(); iterErr != nil {
			return iterErr
		}
	}
	return nil
}

// Close releases the source iterators through the underlying merger.
func (s *SortedMerge) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	if s.merger != nil {
		return s.merger.Close()
	}
	var closeErr error
	for _, iter := range s.iters {
		if iterErr := iter.Close(); iterErr != nil && closeErr == nil {
			closeErr = iterErr
		}
	}
	return closeErr
}

func appendMergeItem(batch *vectorized.RecordBatch, item *MergeItem) {
	batch.Columns[phase1ColumnKey].(*vectorized.TypedColumn[int64]).Append(item.Key)
	batch.Columns[phase1ColumnSeriesID].(*vectorized.TypedColumn[int64]).Append(item.SeriesID)
	batch.Columns[phase1ColumnPartID].(*vectorized.TypedColumn[int64]).Append(item.PartID)
	batch.Columns[phase1ColumnPayload].(*vectorized.TypedColumn[[]byte]).Append(item.Payload)
	batch.Columns[phase1ColumnTraceID].(*vectorized.TypedColumn[string]).Append(item.TraceID)
	batch.Len++
}
