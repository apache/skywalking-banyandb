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

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
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
// honours this by branching on RawFrameSource BEFORE the iterator's
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
	ctx       context.Context
	pipeline  *vectorized.Pipeline
	schema    *vectorized.BatchSchema
	pool      *vectorized.BatchPool
	prevBatch *vectorized.RecordBatch
	err       error
	batch     []*measurev1.InternalDataPoint
	pos       int
	done      bool
}

// Pipeline implements RawFrameSource: returns the underlying vec pipeline
// so the caller can drain it directly into a raw frame body. Calling this
// transfers iteration ownership — the iterator's own Next() must not be
// called afterwards, only Close().
func (i *vectorizedMIterator) Pipeline() *vectorized.Pipeline { return i.pipeline }

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
			i.err = pullErr
			i.done = true
			return false
		}
		if b == nil {
			i.done = true
			return false
		}
		i.prevBatch = b
		i.batch = serializeBatchToProto(b, i.batch[:0])
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
	i.recyclePrev()
	closeErr := i.pipeline.Close()
	return errors.Join(i.err, closeErr)
}
