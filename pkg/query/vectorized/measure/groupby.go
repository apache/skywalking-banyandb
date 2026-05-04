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

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// BatchGroupBy is a BreakerOperator that partitions input rows by the values
// of one or more key columns. Output preserves the input schema; rows are
// emitted in group-insertion order, with all rows of the first group flushed
// before the next group begins.
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
	groups       map[uint64][]*groupBucket
	insertion    []*groupBucket
	keyIndices   []int
	keyEncBuf    []byte // scratch buffer reused across rows during Consume
	entrySize    int64
	rowSize      int64
	reserved     int64
	batchSize    int
	cursorBucket int
	cursorRow    int
	closed       bool
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

// Init prepares the group map.
func (g *BatchGroupBy) Init(_ context.Context) error {
	g.groups = make(map[uint64][]*groupBucket)
	return nil
}

// OutputSchema returns the unchanged input schema.
func (g *BatchGroupBy) OutputSchema() *vectorized.BatchSchema { return g.schema }

// Consume reserves the worst-case bytes for new groups + per-row cost,
// accumulates rows into per-group buckets, then refunds the unused reservation.
func (g *BatchGroupBy) Consume(_ context.Context, b *vectorized.RecordBatch) error {
	active := activeIndices(b)
	n := int64(len(active))
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
	for _, rowIdx := range active {
		bucket, isNew := g.findOrCreate(b, int(rowIdx))
		if isNew {
			newGroups++
		}
		for colIdx, srcCol := range b.Columns {
			copyOneValue(bucket.cols[colIdx], srcCol, int(rowIdx))
		}
	}
	return newGroups*g.entrySize + int64(len(active))*g.rowSize
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
//
// This helper is shared by BatchGroupBy.encodeKey and BatchAggregation.computeKey
// so the two operators agree on key equivalence (Copilot G3 review issues 1+2).
func appendKeyComponent(dst []byte, col vectorized.Column, rowIdx int) []byte {
	switch c := col.(type) {
	case *vectorized.TypedColumn[int64]:
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], uint64(c.Data()[rowIdx]))
		return append(dst, b[:]...)
	case *vectorized.TypedColumn[float64]:
		v := c.Data()[rowIdx]
		if v == 0 {
			v = 0 // canonicalise -0.0 → +0.0 so they hash identically
		}
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], math.Float64bits(v))
		return append(dst, b[:]...)
	case *vectorized.TypedColumn[string]:
		s := c.Data()[rowIdx]
		var lb [4]byte
		binary.LittleEndian.PutUint32(lb[:], uint32(len(s)))
		dst = append(dst, lb[:]...)
		return append(dst, s...)
	case *vectorized.TypedColumn[[]byte]:
		bs := c.Data()[rowIdx]
		var lb [4]byte
		binary.LittleEndian.PutUint32(lb[:], uint32(len(bs)))
		dst = append(dst, lb[:]...)
		return append(dst, bs...)
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

// copyOneValue appends src[rowIdx] to dst, preserving null status.
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
	}
}
