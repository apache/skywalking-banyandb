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
	"math"
	"sync/atomic"

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

// maxLentRows is the largest storage batch handed on without copying: a lent
// batch may carry a uint16 Selection, which addresses at most this many rows.
const maxLentRows = math.MaxUint16 + 1

// NextBatch pulls *MeasureBatch instances from the underlying
// MeasureBatchResult and returns them as RecordBatches.
//
// A storage batch of at least model.SmallBatchRows rows is passed on without
// copying (lend): its columns become the RecordBatch's columns, at the size
// storage produced them (up to a block, 8,192 rows), and the RecordBatch
// pins the storage batch until it is released. Smaller batches, which would
// let per-batch overhead dominate, are copied together into one pooled
// RecordBatch of up to batchSize rows. Row order is preserved either way.
//
// EOF is sticky: once observed, subsequent calls return (nil, nil)
// without re-entering the source. Errors are sticky too.
func (s *BatchSourceFromBatchResult) NextBatch(ctx context.Context) (*vectorized.RecordBatch, error) {
	if s.err != nil {
		return nil, s.err
	}
	if s.eof && s.pending == nil {
		return nil, nil
	}
	var out *vectorized.RecordBatch
	for out == nil || out.Len < s.batchSize {
		if s.pending == nil || s.pendPos >= s.pending.ActiveRows() {
			// Return the exhausted batch to the per-type column pool
			// before pulling the next one; otherwise the storage layer's
			// allocations leak to GC every batch boundary.
			if s.pending != nil {
				s.pending.Release()
				s.pending = nil
			}
			if s.eof {
				break
			}
			mb, pullErr := s.br.PullBatch(ctx)
			if pullErr != nil {
				s.err = pullErr
				if out != nil {
					out.ReleaseOwners()
				}
				return nil, pullErr
			}
			if mb == nil {
				s.eof = true
				break
			}
			if mb.ActiveRows() == 0 {
				mb.Release()
				s.pendPos = 0
				continue
			}
			s.pending = mb
			s.pendPos = 0
		}
		if s.pendPos == 0 && s.lendable(s.pending) {
			if out != nil && out.Len > 0 {
				// Rows already gathered come first; the lendable batch waits.
				break
			}
			lent := s.lend(s.pending)
			s.pending = nil
			return lent, nil
		}
		if out == nil {
			out = s.pool.Get()
		}
		rowsAvail := s.pending.ActiveRows() - s.pendPos
		rowsTake := min(s.batchSize-out.Len, rowsAvail)
		// The copied string, bytes and string-array cells may borrow storage
		// memory the pending MeasureBatch pins. Pin it for out too, before the
		// pending batch is released, so the cells outlive it.
		out.Owners.PinAll(&s.pending.Owners)
		if copyErr := s.copyRowsInto(out, s.pending, s.pendPos, rowsTake); copyErr != nil {
			s.err = copyErr
			out.ReleaseOwners()
			return nil, copyErr
		}
		out.Len += rowsTake
		s.pendPos += rowsTake
	}
	if out == nil {
		return nil, nil
	}
	if out.Len == 0 {
		s.pool.Put(out)
		return nil, nil
	}
	return out, nil
}

// lendable reports whether mb is handed on as it is: it is at least
// model.SmallBatchRows rows (or already addressed by a Selection), and a uint16
// Selection can address all its rows.
func (s *BatchSourceFromBatchResult) lendable(mb *model.MeasureBatch) bool {
	if mb.RowCount() > maxLentRows {
		return false
	}
	return mb.Selection != nil || mb.ActiveRows() >= model.SmallBatchRows
}

// lend wraps mb in a RecordBatch that uses its columns directly. Nothing but the
// series and shard id columns, which storage keeps in a narrower type, is
// copied. The RecordBatch holds mb through a reference-counted owner, so mb's
// columns return to their pools only when the last consumer of those rows,
// including a batch that took over the owner, has released it.
func (s *BatchSourceFromBatchResult) lend(mb *model.MeasureBatch) *vectorized.RecordBatch {
	n := mb.RowCount()
	holder := &lentBatch{mb: mb}
	holder.refs.Store(1)
	cols := make([]vectorized.Column, len(s.schema.Columns))
	metaColumn := func(rows []int64) vectorized.Column {
		c := vectorized.AcquireColumn(vectorized.ColumnTypeInt64, 0).(*vectorized.TypedColumn[int64])
		holder.columns = append(holder.columns, c)
		c.Borrow(rows)
		return c
	}
	convertedColumn := func(convert func(c *vectorized.TypedColumn[int64])) vectorized.Column {
		c := vectorized.AcquireColumn(vectorized.ColumnTypeInt64, n).(*vectorized.TypedColumn[int64])
		holder.columns = append(holder.columns, c)
		convert(c)
		return c
	}
	if i := s.schema.TimestampIndex(); i >= 0 {
		cols[i] = metaColumn(mb.Timestamps)
	}
	if i := s.schema.VersionIndex(); i >= 0 {
		cols[i] = metaColumn(mb.Versions)
	}
	if i := s.schema.SeriesIDIndex(); i >= 0 {
		cols[i] = convertedColumn(func(c *vectorized.TypedColumn[int64]) {
			for _, sid := range mb.SeriesIDs {
				c.Append(int64(sid))
			}
		})
	}
	if i := s.schema.ShardIDIndex(); i >= 0 {
		cols[i] = convertedColumn(func(c *vectorized.TypedColumn[int64]) {
			for _, shard := range mb.ShardIDs {
				c.Append(int64(shard))
			}
		})
	}
	tagIdx, fieldIdx := 0, 0
	for outColIdx, def := range s.schema.Columns {
		switch def.Role {
		case vectorized.RoleTag:
			cols[outColIdx] = mb.Tags[tagIdx]
			tagIdx++
		case vectorized.RoleField:
			cols[outColIdx] = mb.Fields[fieldIdx]
			fieldIdx++
		case vectorized.RoleTimestamp, vectorized.RoleVersion,
			vectorized.RoleSeriesID, vectorized.RoleShardID,
			vectorized.RoleElementID, vectorized.RoleOrderKey:
		}
	}
	out := &vectorized.RecordBatch{
		Schema:    s.schema,
		Columns:   cols,
		Selection: mb.Selection,
		Len:       n,
		Lent:      true,
	}
	if mb.SeriesKnown {
		// The series ends are borrowed from mb, which out's owner keeps alive.
		out.Hints = vectorized.BatchHints{SeriesKnown: true, SeriesEnds: mb.SeriesBoundaries, TimeOrder: mb.TimeOrder}
	}
	out.Owners.Pin(holder)
	holder.Release()
	return out
}

// lentBatch is the owner a lent RecordBatch pins: it keeps the storage batch
// whose columns the RecordBatch uses, and the metadata columns made for it,
// alive until the last reference is released.
type lentBatch struct {
	mb      *model.MeasureBatch
	columns []vectorized.Column
	refs    atomic.Int32
}

// Retain implements vectorized.Owner.
func (l *lentBatch) Retain() { l.refs.Add(1) }

// Release implements vectorized.Owner.
func (l *lentBatch) Release() {
	if l.refs.Add(-1) != 0 {
		return
	}
	for _, c := range l.columns {
		vectorized.ReleaseColumn(c)
	}
	l.columns = nil
	l.mb.Release()
	l.mb = nil
}

// copyRowsInto copies n rows starting at srcPos from mb into the
// RecordBatch out (appending). Metadata roles read mb's parallel slices;
// tag/field roles delegate to AppendColumnRange for each typed column.
func (s *BatchSourceFromBatchResult) copyRowsInto(out *vectorized.RecordBatch,
	mb *model.MeasureBatch, srcPos, n int,
) error {
	if mb.Selection != nil {
		return s.copySelectedRowsInto(out, mb, srcPos, n)
	}
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
			vectorized.RoleSeriesID, vectorized.RoleShardID,
			vectorized.RoleElementID, vectorized.RoleOrderKey:
			// Metadata roles handled above via parallel slices. ElementID/OrderKey
			// are stream-only roles that never appear in a measure schema.
		}
	}
	return nil
}

// copySelectedRowsInto is copyRowsInto for a batch with a Selection: it copies
// the n rows the selection lists from position srcPos on, in selection order.
func (s *BatchSourceFromBatchResult) copySelectedRowsInto(out *vectorized.RecordBatch,
	mb *model.MeasureBatch, srcPos, n int,
) error {
	for _, row := range mb.Selection[srcPos : srcPos+n] {
		r := int(row)
		if i := s.schema.TimestampIndex(); i >= 0 {
			out.Columns[i].(*vectorized.TypedColumn[int64]).Append(mb.Timestamps[r])
		}
		if i := s.schema.VersionIndex(); i >= 0 {
			out.Columns[i].(*vectorized.TypedColumn[int64]).Append(mb.Versions[r])
		}
		if i := s.schema.SeriesIDIndex(); i >= 0 {
			out.Columns[i].(*vectorized.TypedColumn[int64]).Append(int64(mb.SeriesIDs[r]))
		}
		if i := s.schema.ShardIDIndex(); i >= 0 {
			out.Columns[i].(*vectorized.TypedColumn[int64]).Append(int64(mb.ShardIDs[r]))
		}
		tagIdx, fieldIdx := 0, 0
		for outColIdx, def := range s.schema.Columns {
			var src vectorized.Column
			switch def.Role {
			case vectorized.RoleTag:
				src = mb.Tags[tagIdx]
				tagIdx++
			case vectorized.RoleField:
				src = mb.Fields[fieldIdx]
				fieldIdx++
			case vectorized.RoleTimestamp, vectorized.RoleVersion,
				vectorized.RoleSeriesID, vectorized.RoleShardID,
				vectorized.RoleElementID, vectorized.RoleOrderKey:
				continue
			}
			if copyErr := AppendColumnRange(out.Columns[outColIdx], src, r, 1); copyErr != nil {
				return fmt.Errorf("column %s: %w", def.Name, copyErr)
			}
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
