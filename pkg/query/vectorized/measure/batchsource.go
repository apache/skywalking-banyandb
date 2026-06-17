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
	"context"
	"fmt"

	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/tracelabels"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// BatchSourceFromBatchResult is a PullOperator that drives a
// model.MeasureBatchResult through PullBatch and accumulates rows into
// *vectorized.RecordBatch instances of the configured batchSize.
//
// G5c (US-007 partial) — provides the wiring path that lets NewMIterator
// consume PullBatch output directly. Storage's queryResult /
// indexSortResult both implement MeasureBatchResult; the columns inside
// the *MeasureBatch are typed (passthrough today; native after
// US-005). BatchSourceFromBatchResult column-copies into a RecordBatch
// whose schema matches the source. extract.go is bypassed entirely on
// this path — the source columns are already typed.
//
// Schema contract: the supplied schema must match every *MeasureBatch
// produced by br.PullBatch. Enforced loosely — the column-copy helper
// returns an error on TypedColumn[T] mismatches, which surfaces as a
// pipeline error.
type BatchSourceFromBatchResult struct {
	br         model.MeasureBatchResult
	schema     *vectorized.BatchSchema
	pool       *vectorized.BatchPool
	span       *query.Span
	pending    *model.MeasureBatch
	err        error
	batchSize  int
	pendPos    int
	rowsOut    int64
	batchesOut int64
	eof        bool
	closed     bool
}

// NewBatchSourceFromBatchResult constructs the source. Init is required
// before NextBatch (no-op today, kept to satisfy PullOperator).
func NewBatchSourceFromBatchResult(br model.MeasureBatchResult, schema *vectorized.BatchSchema,
	pool *vectorized.BatchPool, batchSize int,
) *BatchSourceFromBatchResult {
	return &BatchSourceFromBatchResult{
		br:        br,
		schema:    schema,
		pool:      pool,
		batchSize: batchSize,
	}
}

// Init satisfies PullOperator.
func (s *BatchSourceFromBatchResult) Init(ctx context.Context) error {
	tracer := query.GetTracer(ctx)
	if tracer != nil {
		s.span, _ = tracer.StartSpan(ctx, "scan")
	}
	return nil
}

// OutputSchema returns the schema declared at construction.
func (s *BatchSourceFromBatchResult) OutputSchema() *vectorized.BatchSchema { return s.schema }

// NextBatch pulls *MeasureBatch instances from the underlying
// MeasureBatchResult and copies their rows into a fresh RecordBatch from
// the pool, up to batchSize rows or source EOF, whichever comes first.
//
// EOF is sticky: once observed, subsequent calls return (nil, nil)
// without re-entering the source. Errors are sticky too.
func (s *BatchSourceFromBatchResult) NextBatch(ctx context.Context) (*vectorized.RecordBatch, error) {
	if s.err != nil {
		return nil, s.err
	}
	if s.eof {
		return nil, nil
	}
	out := s.pool.Get()
	for out.Len < s.batchSize {
		if s.pending == nil || s.pendPos >= s.pending.RowCount() {
			// Return the exhausted batch to the per-type column pool
			// before pulling the next one; otherwise the storage layer's
			// allocations leak to GC every batch boundary.
			if s.pending != nil {
				s.pending.Release()
				s.pending = nil
			}
			mb, pullErr := s.br.PullBatch(ctx)
			if pullErr != nil {
				s.err = pullErr
				return nil, pullErr
			}
			if mb == nil {
				s.eof = true
				break
			}
			if mb.RowCount() == 0 {
				mb.Release()
				s.pendPos = 0
				continue
			}
			s.pending = mb
			s.pendPos = 0
		}
		rowsAvail := s.pending.RowCount() - s.pendPos
		rowsTake := min(s.batchSize-out.Len, rowsAvail)
		if copyErr := s.copyRowsInto(out, s.pending, s.pendPos, rowsTake); copyErr != nil {
			s.err = copyErr
			return nil, copyErr
		}
		out.Len += rowsTake
		s.pendPos += rowsTake
	}
	if out.Len == 0 {
		s.pool.Put(out)
		return nil, nil
	}
	return out, nil
}

// copyRowsInto copies n rows starting at srcPos from mb into the
// RecordBatch out (appending). Metadata roles read mb's parallel slices;
// tag/field roles delegate to AppendColumnRange for each typed column.
func (s *BatchSourceFromBatchResult) copyRowsInto(out *vectorized.RecordBatch,
	mb *model.MeasureBatch, srcPos, n int,
) error {
	if tsIdx := s.schema.TimestampIndex(); tsIdx >= 0 {
		c := out.Columns[tsIdx].(*vectorized.TypedColumn[int64])
		for k := range n {
			c.Append(mb.Timestamps[srcPos+k])
		}
	}
	if vIdx := s.schema.VersionIndex(); vIdx >= 0 {
		c := out.Columns[vIdx].(*vectorized.TypedColumn[int64])
		for k := range n {
			c.Append(mb.Versions[srcPos+k])
		}
	}
	if sidIdx := s.schema.SeriesIDIndex(); sidIdx >= 0 {
		c := out.Columns[sidIdx].(*vectorized.TypedColumn[int64])
		for k := range n {
			c.Append(int64(mb.SeriesIDs[srcPos+k]))
		}
	}
	if shIdx := s.schema.ShardIDIndex(); shIdx >= 0 {
		c := out.Columns[shIdx].(*vectorized.TypedColumn[int64])
		for k := range n {
			c.Append(int64(mb.ShardIDs[srcPos+k]))
		}
	}
	tagIdx, fieldIdx := 0, 0
	for outColIdx, def := range s.schema.Columns {
		switch def.Role {
		case vectorized.RoleTag:
			if tagIdx >= len(mb.Tags) {
				return fmt.Errorf("BatchSourceFromBatchResult: schema declares %d tag columns but MeasureBatch has %d",
					tagIdx+1, len(mb.Tags))
			}
			if copyErr := AppendColumnRange(out.Columns[outColIdx], mb.Tags[tagIdx], srcPos, n); copyErr != nil {
				return fmt.Errorf("tag %s.%s: %w", def.TagFamily, def.Name, copyErr)
			}
			tagIdx++
		case vectorized.RoleField:
			if fieldIdx >= len(mb.Fields) {
				return fmt.Errorf("BatchSourceFromBatchResult: schema declares %d field columns but MeasureBatch has %d",
					fieldIdx+1, len(mb.Fields))
			}
			if copyErr := AppendColumnRange(out.Columns[outColIdx], mb.Fields[fieldIdx], srcPos, n); copyErr != nil {
				return fmt.Errorf("field %s: %w", def.Name, copyErr)
			}
			fieldIdx++
		case vectorized.RoleTimestamp, vectorized.RoleVersion,
			vectorized.RoleSeriesID, vectorized.RoleShardID:
			// Metadata roles handled above via parallel slices.
		}
	}
	return nil
}

// Close releases any in-flight pending MeasureBatch back to the column pool
// and then the underlying MeasureBatchResult exactly once. Idempotent.
func (s *BatchSourceFromBatchResult) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	if s.span != nil {
		s.span.Tagf(tracelabels.TagRowsOut, "%d", s.rowsOut)
		s.span.Tagf(tracelabels.TagBatchesOut, "%d", s.batchesOut)
		if s.schema != nil {
			s.span.Tagf(tracelabels.TagSchemaCols, "%d", len(s.schema.Columns))
		}
		s.span.Stop()
	}
	if s.pending != nil {
		s.pending.Release()
		s.pending = nil
	}
	if s.br != nil {
		s.br.Release()
		s.br = nil
	}
	return nil
}

// AppendColumnRange delegates to the shared vectorized helper.
func AppendColumnRange(dst, src vectorized.Column, srcPos, n int) error {
	return vectorized.AppendColumnRange(dst, src, srcPos, n)
}
