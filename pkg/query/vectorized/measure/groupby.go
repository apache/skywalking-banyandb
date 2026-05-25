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
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math"
	"slices"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/tracelabels"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// BatchGroupBy is a BreakerOperator that partitions input rows by the values
// of one or more key columns. Output preserves the input schema; rows are
// emitted in group-insertion order, with all rows of the first group flushed
// before the next group begins.
//
// When firstOnly is set the operator keeps only the first-seen row of each
// group and drops subsequent rows. This reproduces the row path's raw
// GroupBy egress: the row aggregator wraps every group in a single
// InternalDataPoint slice and banyand/query/processor.go reads only
// current[0] per Next, so a raw GroupBy surfaces exactly one row (the
// first-seen) per group in group-insertion order.
//
// Memory accounting follows a pessimistic-reserve, refund-unused pattern:
//
//   - entrySize is the per-new-group bucket overhead.
//   - rowSize  is the per-row data cost (independent of group identity).
//   - On Consume, reserve worst-case = activeRows * (entrySize + rowSize).
//   - After actual consumption, refund (worstCaseNewGroups - actualNewGroups) * entrySize.
//
// Close releases every outstanding reservation, even mid-Consume cancellations.
type BatchGroupBy struct {
	schema       *vectorized.BatchSchema
	pool         *vectorized.BatchPool
	tracker      *vectorized.MemoryTracker
	span         *query.Span
	groups       map[uint64][]*groupBucket
	insertion    []*groupBucket
	keyIndices   []int
	keyEncBuf    []byte // scratch buffer reused across rows during Consume
	entrySize    int64
	rowSize      int64
	reserved     int64
	rowsIn       int64
	rowsOut      int64
	batchSize    int
	cursorBucket int
	cursorRow    int
	closed       bool
	firstOnly    bool
}

type groupBucket struct {
	cols     []vectorized.Column
	keyBytes []byte
}

// NewBatchGroupBy constructs a BatchGroupBy.
//
//   - entrySize: per-new-group bucket overhead.
//   - rowSize:   per-row data cost.
//
// Pass rowSize=0 to charge only per-new-group; pass entrySize=0 to charge
// only per-row.
func NewBatchGroupBy(
	schema *vectorized.BatchSchema, keyIndices []int,
	pool *vectorized.BatchPool, batchSize int,
	tracker *vectorized.MemoryTracker, entrySize, rowSize int64,
) *BatchGroupBy {
	return &BatchGroupBy{
		schema:     schema,
		keyIndices: slices.Clone(keyIndices),
		pool:       pool,
		batchSize:  batchSize,
		tracker:    tracker,
		entrySize:  entrySize,
		rowSize:    rowSize,
	}
}

// NewBatchGroupByFirst constructs a BatchGroupBy that emits only the
// first-seen row of each group (raw GroupBy without aggregation). It
// matches the row path: groupBy.Execute produces a groupIterator whose
// Current() returns the whole group, and processor.go keeps only
// current[0], so each group surfaces a single row in group-insertion
// order with the input schema unchanged.
func NewBatchGroupByFirst(
	schema *vectorized.BatchSchema, keyIndices []int,
	pool *vectorized.BatchPool, batchSize int,
	tracker *vectorized.MemoryTracker, entrySize int64,
) *BatchGroupBy {
	g := NewBatchGroupBy(schema, keyIndices, pool, batchSize, tracker, entrySize, 0)
	g.firstOnly = true
	return g
}

// Init prepares the group map.
func (g *BatchGroupBy) Init(ctx context.Context) error {
	g.groups = make(map[uint64][]*groupBucket)
	tracer := query.GetTracer(ctx)
	if tracer != nil {
		spanName := "groupby"
		if g.firstOnly {
			spanName = "groupby-first"
		}
		g.span, _ = tracer.StartSpan(ctx, "%s", spanName)
	}
	return nil
}

// OutputSchema returns the unchanged input schema.
func (g *BatchGroupBy) OutputSchema() *vectorized.BatchSchema { return g.schema }

// Consume reserves the worst-case bytes for new groups + per-row cost,
// accumulates rows into per-group buckets, then refunds the unused reservation.
func (g *BatchGroupBy) Consume(_ context.Context, b *vectorized.RecordBatch) error {
	active := activeIndices(b)
	n := int64(len(active))
	if g.span != nil {
		g.rowsIn += n
	}
	estBytes := n*g.entrySize + n*g.rowSize
	if estBytes > 0 {
		if reserveErr := g.tracker.Reserve(estBytes); reserveErr != nil {
			return fmt.Errorf("group-by memory budget exceeded: %w", reserveErr)
		}
		g.reserved += estBytes
	}
	actualBytes := g.consumeRows(b, active)
	if actualBytes < estBytes {
		refund := estBytes - actualBytes
		g.tracker.Release(refund)
		g.reserved -= refund
	}
	return nil
}

func (g *BatchGroupBy) consumeRows(b *vectorized.RecordBatch, active []uint16) int64 {
	var newGroups int64
	var copiedRows int64
	for _, rowIdx := range active {
		bucket, isNew := g.findOrCreate(b, int(rowIdx))
		if isNew {
			newGroups++
		}
		if g.firstOnly && !isNew {
			// Raw GroupBy keeps only the first-seen row per group;
			// drop every later row in the same group.
			continue
		}
		for colIdx, srcCol := range b.Columns {
			copyOneValue(bucket.cols[colIdx], srcCol, int(rowIdx))
		}
		copiedRows++
	}
	return newGroups*g.entrySize + copiedRows*g.rowSize
}

// findOrCreate locates the bucket for the row's key, creating it if absent.
// Hash collisions are resolved by linear scan over the bucket chain — the
// keyBytes are compared exactly, so two distinct keys that hash to the same
// uint64 still map to different buckets.
func (g *BatchGroupBy) findOrCreate(b *vectorized.RecordBatch, rowIdx int) (*groupBucket, bool) {
	g.keyEncBuf = g.encodeKey(g.keyEncBuf[:0], b, rowIdx)
	h := hashKey(g.keyEncBuf)
	candidates := g.groups[h]
	for _, candidate := range candidates {
		if bytes.Equal(candidate.keyBytes, g.keyEncBuf) {
			return candidate, false
		}
	}
	bucket := newGroupBucket(g.schema)
	bucket.keyBytes = slices.Clone(g.keyEncBuf)
	g.groups[h] = append(candidates, bucket)
	g.insertion = append(g.insertion, bucket)
	return bucket, true
}

// Finalize is a no-op for v1 — groups are already materialized.
func (g *BatchGroupBy) Finalize(_ context.Context) error { return nil }

// NextBatch emits accumulated rows in group-insertion order, paginated by
// batchSize. Returns (nil, nil) when all groups are drained.
func (g *BatchGroupBy) NextBatch(_ context.Context) (*vectorized.RecordBatch, error) {
	if g.cursorBucket >= len(g.insertion) {
		return nil, nil
	}
	out := g.pool.Get()
	for out.Len < g.batchSize && g.cursorBucket < len(g.insertion) {
		bucket := g.insertion[g.cursorBucket]
		bucketLen := bucket.cols[0].Len()
		for g.cursorRow < bucketLen && out.Len < g.batchSize {
			for colIdx := range out.Columns {
				copyOneValue(out.Columns[colIdx], bucket.cols[colIdx], g.cursorRow)
			}
			out.Len++
			g.cursorRow++
			if g.span != nil {
				g.rowsOut++
			}
		}
		if g.cursorRow >= bucketLen {
			g.cursorBucket++
			g.cursorRow = 0
		}
	}
	if out.Len == 0 {
		g.pool.Put(out)
		return nil, nil
	}
	return out, nil
}

// Close releases every outstanding memory reservation. Idempotent.
func (g *BatchGroupBy) Close() error {
	if g.closed {
		return nil
	}
	g.closed = true
	if g.reserved > 0 {
		g.tracker.Release(g.reserved)
		g.reserved = 0
	}
	if g.span != nil {
		g.span.Tagf(tracelabels.TagRowsIn, "%d", g.rowsIn)
		g.span.Tagf(tracelabels.TagRowsOut, "%d", g.rowsOut)
		g.span.Tagf(tracelabels.TagGroupsOut, "%d", len(g.insertion))
		if g.firstOnly {
			g.span.Tagf(tracelabels.TagDroppedRows, "%d", g.rowsIn-g.rowsOut)
			g.span.Tag(tracelabels.TagDropReason, "groupby-first")
		}
		g.span.Stop()
	}
	g.groups = nil
	g.insertion = nil
	return nil
}

// encodeKey serializes the configured key column values at rowIdx into a
// length-stable byte sequence suitable both as a hash input and as an
// exact-equality comparand on hash collisions.
//
// Variable-width components (string, bytes) are length-prefixed so embedded
// NUL bytes cannot collapse distinct tuples into the same encoding. Fixed-
// width components (int64, float64) need no prefix; float zero is
// canonicalised so +0.0 and -0.0 produce identical bytes.
func (g *BatchGroupBy) encodeKey(dst []byte, b *vectorized.RecordBatch, rowIdx int) []byte {
	for _, kIdx := range g.keyIndices {
		dst = appendKeyComponent(dst, b.Columns[kIdx], rowIdx)
	}
	return dst
}

// appendKeyComponent encodes one column's value at rowIdx into a length-
// stable byte representation:
//   - int64:    8 raw little-endian bytes.
//   - float64:  8 little-endian bytes of math.Float64bits, with -0.0 → +0.0.
//   - string:   4-byte little-endian length prefix + raw bytes.
//   - bytes:    4-byte little-endian length prefix + raw bytes.
//   - TagValue/FieldValue passthrough: same encoding as the scalar payload type.
//
// This helper is shared by BatchGroupBy.encodeKey and BatchAggregation.computeKey
// so the two operators agree on key equivalence (Copilot G3 review issues 1+2).
func appendKeyComponent(dst []byte, col vectorized.Column, rowIdx int) []byte {
	switch c := col.(type) {
	case *vectorized.TypedColumn[int64]:
		return appendIntKey(dst, c.Data()[rowIdx])
	case *vectorized.TypedColumn[float64]:
		return appendFloatKey(dst, c.Data()[rowIdx])
	case *vectorized.TypedColumn[string]:
		return appendStringKey(dst, c.Data()[rowIdx])
	case *vectorized.TypedColumn[[]byte]:
		return appendBytesKey(dst, c.Data()[rowIdx])
	case *vectorized.TypedColumn[*modelv1.TagValue]:
		return appendTagValueKey(dst, c.Data()[rowIdx])
	case *vectorized.TypedColumn[*modelv1.FieldValue]:
		return appendFieldValueKey(dst, c.Data()[rowIdx])
	}
	return dst
}

func appendIntKey(dst []byte, value int64) []byte {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], uint64(value))
	return append(dst, b[:]...)
}

func appendFloatKey(dst []byte, value float64) []byte {
	if value == 0 {
		value = 0 // canonicalise -0.0 → +0.0 so they hash identically
	}
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], math.Float64bits(value))
	return append(dst, b[:]...)
}

func appendStringKey(dst []byte, value string) []byte {
	var lb [4]byte
	binary.LittleEndian.PutUint32(lb[:], uint32(len(value)))
	dst = append(dst, lb[:]...)
	return append(dst, value...)
}

func appendBytesKey(dst []byte, value []byte) []byte {
	var lb [4]byte
	binary.LittleEndian.PutUint32(lb[:], uint32(len(value)))
	dst = append(dst, lb[:]...)
	return append(dst, value...)
}

func appendTagValueKey(dst []byte, value *modelv1.TagValue) []byte {
	if value == nil {
		return dst
	}
	switch payload := value.GetValue().(type) {
	case *modelv1.TagValue_Int:
		return appendIntKey(dst, payload.Int.GetValue())
	case *modelv1.TagValue_Str:
		return appendStringKey(dst, payload.Str.GetValue())
	case *modelv1.TagValue_BinaryData:
		return appendBytesKey(dst, payload.BinaryData)
	}
	return dst
}

func appendFieldValueKey(dst []byte, value *modelv1.FieldValue) []byte {
	if value == nil {
		return dst
	}
	switch payload := value.GetValue().(type) {
	case *modelv1.FieldValue_Int:
		return appendIntKey(dst, payload.Int.GetValue())
	case *modelv1.FieldValue_Float:
		return appendFloatKey(dst, payload.Float.GetValue())
	case *modelv1.FieldValue_Str:
		return appendStringKey(dst, payload.Str.GetValue())
	case *modelv1.FieldValue_BinaryData:
		return appendBytesKey(dst, payload.BinaryData)
	}
	return dst
}

// hashKey returns a stable uint64 fingerprint of the encoded key bytes.
// fnv64a is fast and has good dispersion for the typical 8–64-byte keys we see.
func hashKey(b []byte) uint64 {
	h := fnv.New64a()
	_, _ = h.Write(b)
	return h.Sum64()
}

func newGroupBucket(schema *vectorized.BatchSchema) *groupBucket {
	cols := make([]vectorized.Column, len(schema.Columns))
	for i, def := range schema.Columns {
		cols[i] = vectorized.NewColumnForType(def.Type, 8)
	}
	return &groupBucket{cols: cols}
}

// copyOneValue appends src[rowIdx] to dst, preserving null status. The
// passthrough TagValue / FieldValue cases let BatchAggregation carry
// non-key projected tag columns forward unchanged (the pointer is
// owned by the upstream MeasureBatch, which the pipeline keeps live for
// the duration of aggregation).
func copyOneValue(dst, src vectorized.Column, rowIdx int) {
	if src.IsNull(rowIdx) {
		dst.AppendNull()
		return
	}
	switch s := src.(type) {
	case *vectorized.TypedColumn[int64]:
		dst.(*vectorized.TypedColumn[int64]).Append(s.Data()[rowIdx])
	case *vectorized.TypedColumn[float64]:
		dst.(*vectorized.TypedColumn[float64]).Append(s.Data()[rowIdx])
	case *vectorized.TypedColumn[string]:
		dst.(*vectorized.TypedColumn[string]).Append(s.Data()[rowIdx])
	case *vectorized.TypedColumn[[]byte]:
		dst.(*vectorized.TypedColumn[[]byte]).Append(s.Data()[rowIdx])
	case *vectorized.TypedColumn[[]int64]:
		dst.(*vectorized.TypedColumn[[]int64]).Append(s.Data()[rowIdx])
	case *vectorized.TypedColumn[[]string]:
		dst.(*vectorized.TypedColumn[[]string]).Append(s.Data()[rowIdx])
	case *vectorized.TypedColumn[*modelv1.TagValue]:
		dst.(*vectorized.TypedColumn[*modelv1.TagValue]).Append(s.Data()[rowIdx])
	case *vectorized.TypedColumn[*modelv1.FieldValue]:
		dst.(*vectorized.TypedColumn[*modelv1.FieldValue]).Append(s.Data()[rowIdx])
	}
}
