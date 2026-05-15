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

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// Singletons reused for null fills in passthrough columns. Matches what the
// row path emits for projection entries the active result lacks.
var (
	pbv1NullTagValueRef   = pbv1.NullTagValue
	pbv1NullFieldValueRef = pbv1.NullFieldValue
)

// BatchScan is the v1 PullOperator that wraps a MeasureQueryResult and
// produces RecordBatches. It uses a SeriesCursor to manage cross-series
// boundaries and bulk-extracts metadata, tags, and fields per series fill.
type BatchScan struct {
	qr        model.MeasureQueryResult
	schema    *vectorized.BatchSchema
	pool      *vectorized.BatchPool
	cursor    SeriesCursor
	batchSize int
	closed    bool
}

// NewBatchScan returns a BatchScan; call Init before NextBatch.
func NewBatchScan(qr model.MeasureQueryResult, schema *vectorized.BatchSchema,
	pool *vectorized.BatchPool, batchSize int,
) *BatchScan {
	return &BatchScan{qr: qr, schema: schema, pool: pool, batchSize: batchSize}
}

// Init prepares the cursor and pulls the first non-empty MeasureResult.
func (s *BatchScan) Init(_ context.Context) error {
	s.cursor.Init(s.qr)
	return nil
}

// OutputSchema returns the schema declared at construction.
func (s *BatchScan) OutputSchema() *vectorized.BatchSchema { return s.schema }

// Close releases the underlying cursor exactly once.
func (s *BatchScan) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	s.cursor.Close()
	return nil
}

// NextBatch fills a fresh batch up to batchSize rows. Returns:
//   - (batch, nil) for a valid batch with Len > 0;
//   - (nil, nil) for clean EOF;
//   - (nil, err) for storage error (the partial batch is dropped to GC).
func (s *BatchScan) NextBatch(_ context.Context) (*vectorized.RecordBatch, error) {
	b := s.pool.Get()
	for b.Len < s.batchSize && !s.cursor.Exhausted() && s.cursor.Err() == nil {
		remaining := s.cursor.RemainingInSeries()
		if remaining == 0 {
			break
		}
		n := remaining
		if avail := s.batchSize - b.Len; n > avail {
			n = avail
		}
		if fillErr := s.fillFromCurrent(b, n); fillErr != nil {
			// R3-style discard: do not return b to the pool.
			return nil, fillErr
		}
		b.Len += n
		s.cursor.Advance(n)
		// Advance may trigger an internal advanceSeries that observes a
		// storage error on the next MeasureResult. Discard the partial
		// batch immediately so the error is never silently buried — the
		// caller must learn about the failure on this call, not the next.
		if cursorErr := s.cursor.Err(); cursorErr != nil {
			return nil, cursorErr
		}
	}
	if b.Len == 0 {
		s.pool.Put(b)
		if cursorErr := s.cursor.Err(); cursorErr != nil {
			return nil, cursorErr
		}
		return nil, nil
	}
	return b, nil
}

// fillFromCurrent copies n rows from the cursor's current MeasureResult into
// b starting at b.Len. Caller must ensure n <= cursor.RemainingInSeries().
func (s *BatchScan) fillFromCurrent(b *vectorized.RecordBatch, n int) error {
	cur, pos := s.cursor.Current()
	offset := b.Len
	if fillErr := fillMetadata(b, s.schema, cur, pos, n); fillErr != nil {
		return fillErr
	}
	if fillErr := fillTags(b, s.schema, cur, pos, offset, n); fillErr != nil {
		return fillErr
	}
	return fillFields(b, s.schema, cur, pos, offset, n)
}

// fillMetadata populates the timestamp, version, series-id, and shard-id
// columns for n rows starting at pos. Uses copy() for parallel int64 arrays;
// SID is a constant per series and is filled with n repetitions.
func fillMetadata(b *vectorized.RecordBatch, schema *vectorized.BatchSchema,
	cur *model.MeasureResult, pos, n int,
) error {
	offset := b.Len
	if i := schema.TimestampIndex(); i >= 0 {
		c := b.Columns[i].(*vectorized.TypedColumn[int64])
		appendInt64Zeros(c, n)
		copy(c.Data()[offset:offset+n], cur.Timestamps[pos:pos+n])
	}
	if i := schema.VersionIndex(); i >= 0 {
		c := b.Columns[i].(*vectorized.TypedColumn[int64])
		appendInt64Zeros(c, n)
		copy(c.Data()[offset:offset+n], cur.Versions[pos:pos+n])
	}
	if i := schema.SeriesIDIndex(); i >= 0 {
		c := b.Columns[i].(*vectorized.TypedColumn[int64])
		sid := int64(cur.SID)
		for range n {
			c.Append(sid)
		}
	}
	if i := schema.ShardIDIndex(); i >= 0 {
		c := b.Columns[i].(*vectorized.TypedColumn[int64])
		for k := range n {
			var v int64
			if pos+k < len(cur.ShardIDs) {
				v = int64(cur.ShardIDs[pos+k])
			}
			c.Append(v)
		}
	}
	return nil
}

// fillTags extracts every tag column for n rows starting at pos into the batch
// at offset. Schema-declared tag columns missing from cur are grown with
// explicit nulls so a downstream serializer sees the same shape the row path
// emits when the projected tag is absent (which the multi-group flow
// produces — one group's schema may lack a tag the other group has).
//
// Passthrough columns (ColumnTypeTagValue) take a fast path: the original
// *modelv1.TagValue pointer is stored directly, no decode/re-encode.
func fillTags(b *vectorized.RecordBatch, schema *vectorized.BatchSchema,
	cur *model.MeasureResult, pos, offset, n int,
) error {
	resultTags := make(map[int]*model.Tag, len(cur.TagFamilies))
	for tfIdx := range cur.TagFamilies {
		tf := &cur.TagFamilies[tfIdx]
		for tagIdx := range tf.Tags {
			tag := &tf.Tags[tagIdx]
			if colIdx, ok := schema.TagIndex(tf.Name, tag.Name); ok {
				resultTags[colIdx] = tag
			}
		}
	}
	for colIdx, def := range schema.Columns {
		if def.Role != vectorized.RoleTag {
			continue
		}
		col := b.Columns[colIdx]
		if pc, ok := col.(*vectorized.TypedColumn[*modelv1.TagValue]); ok {
			tag, present := resultTags[colIdx]
			if !present {
				appendNullTagValues(pc, n)
				continue
			}
			appendTagValuesPassthrough(pc, tag.Values[pos:pos+n])
			continue
		}
		growColumn(col, n)
		tag, present := resultTags[colIdx]
		if !present {
			markRowsNull(col, offset, n)
			continue
		}
		if extractErr := extractTagBulk(col, offset, tag.Values[pos:pos+n], n); extractErr != nil {
			return extractErr
		}
	}
	return nil
}

// fillFields is the field-side counterpart of fillTags. Same null-fill rule
// applies for projection entries that the active result lacks. Same fast
// path for passthrough columns.
func fillFields(b *vectorized.RecordBatch, schema *vectorized.BatchSchema,
	cur *model.MeasureResult, pos, offset, n int,
) error {
	resultFields := make(map[int]*model.Field, len(cur.Fields))
	for fIdx := range cur.Fields {
		f := &cur.Fields[fIdx]
		if colIdx, ok := schema.FieldIndex(f.Name); ok {
			resultFields[colIdx] = f
		}
	}
	for colIdx, def := range schema.Columns {
		if def.Role != vectorized.RoleField {
			continue
		}
		col := b.Columns[colIdx]
		if pc, ok := col.(*vectorized.TypedColumn[*modelv1.FieldValue]); ok {
			f, present := resultFields[colIdx]
			if !present {
				appendNullFieldValues(pc, n)
				continue
			}
			appendFieldValuesPassthrough(pc, f.Values[pos:pos+n])
			continue
		}
		growColumn(col, n)
		f, present := resultFields[colIdx]
		if !present {
			markRowsNull(col, offset, n)
			continue
		}
		if extractErr := extractFieldBulk(col, offset, f.Values[pos:pos+n], n); extractErr != nil {
			return extractErr
		}
	}
	return nil
}

// appendTagValuesPassthrough copies n *modelv1.TagValue pointers from src into
// the column, advancing its length by n. No protobuf decoding happens.
func appendTagValuesPassthrough(c *vectorized.TypedColumn[*modelv1.TagValue], src []*modelv1.TagValue) {
	for _, v := range src {
		c.Append(v)
	}
}

func appendFieldValuesPassthrough(c *vectorized.TypedColumn[*modelv1.FieldValue], src []*modelv1.FieldValue) {
	for _, v := range src {
		c.Append(v)
	}
}

// appendNullTagValues / appendNullFieldValues grow the passthrough column by
// n rows pinned to pbv1.Null{Tag,Field}Value singletons. Matches the row
// path's null fill for projections absent from the source.
func appendNullTagValues(c *vectorized.TypedColumn[*modelv1.TagValue], n int) {
	for range n {
		c.Append(pbv1NullTagValueRef)
	}
}

func appendNullFieldValues(c *vectorized.TypedColumn[*modelv1.FieldValue], n int) {
	for range n {
		c.Append(pbv1NullFieldValueRef)
	}
}

// markRowsNull marks rows [offset, offset+n) in col as null without otherwise
// touching the underlying data slice.
func markRowsNull(col vectorized.Column, offset, n int) {
	for k := range n {
		col.MarkNullAt(offset + k)
	}
}

func appendInt64Zeros(c *vectorized.TypedColumn[int64], n int) {
	for range n {
		c.Append(0)
	}
}

// growColumn appends n zero-valued rows so subsequent extract* calls have
// pre-existing slots to overwrite.
func growColumn(col vectorized.Column, n int) {
	switch c := col.(type) {
	case *vectorized.TypedColumn[int64]:
		for range n {
			c.Append(0)
		}
	case *vectorized.TypedColumn[float64]:
		for range n {
			c.Append(0)
		}
	case *vectorized.TypedColumn[string]:
		for range n {
			c.Append("")
		}
	case *vectorized.TypedColumn[[]byte]:
		for range n {
			c.Append(nil)
		}
	case *vectorized.TypedColumn[[]int64]:
		for range n {
			c.Append(nil)
		}
	case *vectorized.TypedColumn[[]string]:
		for range n {
			c.Append(nil)
		}
	}
}
