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
	"errors"
	"math"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// RawFrameSource is the capability a vec MIterator exposes when the
// caller wants to short-circuit per-row proto serialization and instead
// drain the underlying Pipeline into a single columnar raw frame body
// (G9f.5.b). The data-node processor type-asserts to this interface
// when data.MeasureWireModeRaw() is true on TopicInternalMeasureQuery,
// calls DrainPipelineToFrame(ctx, src.Pipeline(), src.Schema()), then
// Close()s the wrapping iterator to release pooled batches + the
// pipeline. The row-path iterators do NOT implement this interface, so
// the type assertion failing is the correct fall-through signal.
//
// IMPORTANT: a caller that drains the Pipeline directly MUST NOT also
// drive Next() on the MIterator afterwards — the pipeline is now empty
// and the iterator's internal cursor is stale. The data-node processor
// honors this by branching on RawFrameSource BEFORE the iterator's
// proto-collection loop.
type RawFrameSource interface {
	// Pipeline returns the vec pipeline the iterator wraps. Drain it via
	// vectorized.Pipeline.Next until nil; the iterator's own Close still
	// owns the pipeline lifecycle.
	Pipeline() *vectorized.Pipeline
	// Schema returns the terminal-operator output schema — the same
	// schema every emitted batch carries.
	Schema() *vectorized.BatchSchema
}

// vectorizedMIterator adapts a vectorized Pipeline to the executor.MIterator
// interface so the existing gRPC handler can drive it without knowing it
// is internally columnar.
//
// Contract match: the row-path resultMIterator advances one DataPoint per
// Next() and exposes it as a single-element slice via Current(); the gRPC
// collector at banyand/query/processor.go::collectInternalDataPoints reads
// only current[0]. This adapter mirrors that semantics: Next() advances one
// row at a time, pulling a new batch from the pipeline whenever the
// previously-serialized batch is exhausted, returning the consumed batch to
// pool so allocations stay flat.
//
// Implements RawFrameSource: the data-node Rev under flag-on bypasses
// serializeBatchToProto and drains the pipeline directly via
// DrainPipelineToFrame.
type vectorizedMIterator struct {
	ctx           context.Context
	pipeline      *vectorized.Pipeline
	schema        *vectorized.BatchSchema
	pool          *vectorized.BatchPool
	prevBatch     *vectorized.RecordBatch
	err           error
	batch         []*measurev1.InternalDataPoint
	frameReleases []func()
	pos           int
	done          bool
	reserved      int64
}

// Pipeline implements RawFrameSource: returns the underlying vec pipeline
// so the caller can drain it directly into a raw frame body. Calling this
// transfers iteration ownership — the iterator's own Next() must not be
// called afterwards, only Close().
func (i *vectorizedMIterator) Pipeline() *vectorized.Pipeline { return i.pipeline }

// EmitFrame implements FrameEmitter: drain the underlying vec pipeline
// directly via DrainPipelineToFrame. This is the throughput-optimal
// wire-emit path — no proto materialization, columnar end-to-end. The
// data-node Rev prefers this over the row-side reverse-serialize that
// wrapper iterators fall back on.
func (i *vectorizedMIterator) EmitFrame(ctx context.Context) ([]byte, error) {
	return drainPipelineToFrame(ctx, i.pipeline, i.schema, func(release func()) {
		i.frameReleases = append(i.frameReleases, release)
	})
}

// Schema implements RawFrameSource: returns the terminal-operator output
// schema, the same schema every emitted batch carries.
func (i *vectorizedMIterator) Schema() *vectorized.BatchSchema { return i.schema }

// newVectorizedMIterator constructs an adapter bound to ctx. The pool is the
// BatchPool that BatchScan draws from; consumed batches are returned there
// after serialization. schema is the terminal-operator output schema
// (passed through to the RawFrameSource capability so the data-node raw
// frame emit path knows what shape every emitted batch will carry).
func newVectorizedMIterator(ctx context.Context, p *vectorized.Pipeline, schema *vectorized.BatchSchema, pool *vectorized.BatchPool) *vectorizedMIterator {
	return &vectorizedMIterator{ctx: ctx, pipeline: p, schema: schema, pool: pool, pos: -1}
}

// Next advances by exactly one DataPoint. Whenever the cached serialization
// is exhausted, the previous batch is returned to the pool and a new one is
// pulled from the pipeline. Empty-active batches (e.g., from BatchLimit
// emitting an empty selection) are silently recycled — they are not
// surfaced as zero-row Current() reads. EOF and errors are terminal.
func (i *vectorizedMIterator) Next() bool {
	if i.done {
		return false
	}
	i.pos++
	for i.pos >= len(i.batch) {
		i.recyclePrev()
		b, pullErr := i.pipeline.Next(i.ctx)
		if pullErr != nil {
			vectorized.FinishResponseEncoding(i.ctx, false)
			i.err = pullErr
			i.done = true
			return false
		}
		if b == nil {
			vectorized.FinishResponseEncoding(i.ctx, true)
			i.done = true
			return false
		}
		i.prevBatch = b
		// Reserve final protobuf objects, pointer slabs and cloned variable payloads
		// before materialization. The parent lease remains charged until request completion.
		protoBytes, wireBound, covered, chargeErr := serializationPreflight(b)
		if chargeErr == nil {
			if tracker := i.pipeline.Tracker(); tracker != nil {
				chargeErr = tracker.Reserve(protoBytes)
				if chargeErr == nil {
					i.reserved += protoBytes
				}
			} else {
				chargeErr = query.Charge(i.ctx, uint64(protoBytes))
			}
		}
		if chargeErr != nil {
			vectorized.FinishResponseEncoding(i.ctx, false)
			i.err = chargeErr
			i.done = true
			return false
		}
		recordBatchSerialization(i.ctx, b.ActiveLen())
		dst := i.batch[:0]
		if vectorized.HasResponseEncoding(i.ctx) {
			// Coverage retains each pointer slab until finalization; later batches
			// must not overwrite the identities used to validate the bound.
			dst = nil
		}
		i.batch = serializeBatchToProto(b, dst)
		if recordErr := vectorized.RecordResponseEncoding(i.ctx, i.batch, wireBound, covered); recordErr != nil {
			vectorized.FinishResponseEncoding(i.ctx, false)
			i.err, i.done = recordErr, true
			return false
		}
		i.pos = 0
	}
	return true
}

// recyclePrev returns the last consumed batch to the pool. Safe to call when
// no batch is held. The serializer defensive-copies slice-typed values
// (see columnValueToTagValue / columnValueToFieldValue) so reusing the batch
// cannot corrupt previously-emitted DataPoints.
func (i *vectorizedMIterator) recyclePrev() {
	if i.prevBatch == nil || i.pool == nil {
		i.prevBatch = nil
		return
	}
	i.pool.Put(i.prevBatch)
	i.prevBatch = nil
}

// Current returns a single-element slice containing the row most recently
// advanced into via Next(). Matches the row-path contract; collectors that
// read only Current()[0] see every row.
func (i *vectorizedMIterator) Current() []*measurev1.InternalDataPoint {
	if i.pos < 0 || i.pos >= len(i.batch) {
		return nil
	}
	return i.batch[i.pos : i.pos+1]
}

// Err returns the storage error that terminated iteration, or nil.
func (i *vectorizedMIterator) Err() error {
	return i.err
}

// Close releases pooled batches and the pipeline (which closes the BatchScan
// and releases the underlying MeasureQueryResult). Returns the join of any
// sticky iteration error and the pipeline-close error, mirroring the row-path
// resultMIterator.Close contract that surfaces ei.err.
func (i *vectorizedMIterator) Close() error {
	if !i.done || i.err != nil {
		vectorized.FinishResponseEncoding(i.ctx, false)
	}
	for _, release := range i.frameReleases {
		release()
	}
	i.frameReleases = nil
	i.recyclePrev()
	if i.reserved != 0 {
		tracker, reserved := i.pipeline.Tracker(), i.reserved
		i.reserved = 0
		release := query.RegisterTransportRelease(i.ctx, func() { tracker.Release(reserved) })
		release()
	}
	closeErr := i.pipeline.Close()
	return errors.Join(i.err, closeErr)
}

func serializationReservationBytes(batch *vectorized.RecordBatch) (int64, error) {
	memory, _, _, err := serializationPreflight(batch)
	return memory, err
}

func serializationPreflight(batch *vectorized.RecordBatch) (int64, uint64, bool, error) {
	retained := vectorized.EstimatedRetainedBytes(batch)
	rows := uint64(batch.ActiveLen())
	ordered := orderedSerializationSelection(batch)
	memoryRetained := retained
	if !ordered && rows > 1 {
		// Only duplicate indices require additional copy residency. A separate,
		// bounded scratch frame keeps normal ascending serialization small.
		factor := serializationDuplicateFactor(batch.Selection)
		if retained > math.MaxInt64/factor {
			return 0, 0, false, vectorized.ErrMemoryBudgetExceeded
		}
		memoryRetained *= factor
	}
	constantBytes, err := serializedConstantPayload(batch, rows)
	if err != nil || constantBytes > math.MaxInt64-memoryRetained {
		return 0, 0, false, vectorized.ErrMemoryBudgetExceeded
	}
	memoryRetained += constantBytes
	if memoryRetained > math.MaxInt64/4 || uint64(len(batch.Columns)) > (math.MaxInt64-512)/128 {
		return 0, 0, false, vectorized.ErrMemoryBudgetExceeded
	}
	perRow := uint64(512) + uint64(len(batch.Columns))*128
	for _, def := range batch.Schema.Columns {
		if def.Type == vectorized.ColumnTypeDecimal {
			if perRow > math.MaxInt64-40 {
				return 0, 0, false, vectorized.ErrMemoryBudgetExceeded
			}
			perRow += 40
		}
	}
	fixed := memoryRetained * 4
	if rows > (math.MaxInt64-fixed)/perRow {
		return 0, 0, false, vectorized.ErrMemoryBudgetExceeded
	}
	memory := int64(fixed + rows*perRow)
	if !ordered {
		return memory, 0, false, nil
	}
	wire, covered, wireErr := serializationWireBound(batch, rows, retained, constantBytes)
	return memory, wire, covered, wireErr
}

func orderedSerializationSelection(batch *vectorized.RecordBatch) bool {
	for idx, row := range batch.Selection {
		if int(row) >= batch.Len || (idx > 0 && row <= batch.Selection[idx-1]) {
			return false
		}
	}
	return true
}

// serializationDuplicateFactor bounds the largest index multiplicity without
// per-row heap allocations. The uint16 selection domain needs an 8 KiB bitmap.
func serializationDuplicateFactor(selection []uint16) uint64 {
	var seen [1024]uint64
	var distinct uint64
	for _, row := range selection {
		word, mask := row/64, uint64(1)<<(row%64)
		if seen[word]&mask == 0 {
			seen[word] |= mask
			distinct++
		}
	}
	return uint64(len(selection)) - distinct + 1
}

func serializedConstantPayload(batch *vectorized.RecordBatch, rows uint64) (uint64, error) {
	var total uint64
	for _, column := range batch.Columns {
		constant, ok := column.(*vectorized.ConstColumn)
		if !ok || constant.IsNullValue() {
			continue
		}
		payload := uint64(len(constant.Str())) + uint64(len(constant.Bytes()))
		if rows != 0 && payload > (math.MaxInt64-total)/rows {
			return 0, vectorized.ErrMemoryBudgetExceeded
		}
		total += payload * rows
	}
	return total, nil
}

func serializationWireBound(batch *vectorized.RecordBatch, rows, retained, constantBytes uint64) (uint64, bool, error) {
	// Two retained bytes cover worst-case ten-byte int64 varints and array
	// framing. Per-cell slack covers the nested protobuf messages; schema
	// names and constant payloads are repeated on the wire for every row.
	if retained > math.MaxInt64/2 || constantBytes > math.MaxInt64/2-retained {
		return 0, false, vectorized.ErrMemoryBudgetExceeded
	}
	bound := 2 * (retained + constantBytes)
	if uint64(len(batch.Columns)) > (math.MaxInt64-64)/64 {
		return 0, false, vectorized.ErrMemoryBudgetExceeded
	}
	perRow := uint64(64) + uint64(len(batch.Columns))*64
	for _, def := range batch.Schema.Columns {
		if def.Type == vectorized.ColumnTypeTagValue || def.Type == vectorized.ColumnTypeFieldValue {
			return 0, false, nil
		}
		if def.Type == vectorized.ColumnTypeDecimal {
			if perRow > math.MaxInt64-40 {
				return 0, false, vectorized.ErrMemoryBudgetExceeded
			}
			perRow += 40
		}
		names := uint64(len(def.Name))
		if names > math.MaxInt64-perRow {
			return 0, false, vectorized.ErrMemoryBudgetExceeded
		}
		perRow += names
	}
	for _, group := range batch.Schema.TagFamilyGroups {
		if len(group.Columns) == 0 {
			return 0, false, nil
		}
		metadata := uint64(len(group.Family)) + 32
		if metadata > math.MaxInt64-perRow {
			return 0, false, vectorized.ErrMemoryBudgetExceeded
		}
		perRow += metadata
	}
	if rows > (math.MaxInt64-bound)/perRow {
		return 0, false, vectorized.ErrMemoryBudgetExceeded
	}
	bound += rows * perRow
	if bound > math.MaxInt64/2 {
		return 0, false, vectorized.ErrMemoryBudgetExceeded
	}
	return bound, true, nil
}
