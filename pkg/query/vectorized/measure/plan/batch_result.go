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

package plan

import (
	"context"
	"errors"
	"fmt"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/tracelabels"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
)

// BatchResult owns eager native output, its schema and retained buffers.
// NextBatch lends descriptors until the next pull or Close. Consumers retaining views
// must retain owners and copy descriptors before another adapter can recycle them. Close is idempotent.
// Native execution remains eager: this handle does not imply streaming fanout.
type BatchResult struct {
	source         vectorized.PullOperator
	schema         *vectorized.BatchSchema
	iteratorSpan   *query.Span
	tracedIterator *batchTraceIterator
	traceFinalizer func() *commonv1.Trace
	Trace          *commonv1.Trace
	plan           *DistributedPlan
	request        *measurev1.QueryRequest
	tracker        *vectorized.MemoryTracker
	onClose        []func()
	batchSize      int
	reserved       int64
	closed         bool
}

// NewBatchResult wraps an initialized local batch source and takes its Close ownership.
// The source retains responsibility for accounting buffers until disposal.
func NewBatchResult(ctx context.Context, source vectorized.PullOperator, schema *vectorized.BatchSchema, batchSize int) (*BatchResult, error) {
	if err := ctx.Err(); err != nil {
		_ = source.Close()
		return nil, err
	}
	vmeasure.RecordBatchResultOpened()
	return &BatchResult{source: source, schema: schema, batchSize: batchSize, tracker: vectorized.QueryMemoryTracker(ctx, 256<<20)}, nil
}

func newBatchResult(ctx context.Context, p *DistributedPlan, batches []*vectorized.RecordBatch,
	req *measurev1.QueryRequest, schema *vectorized.BatchSchema,
) (*BatchResult, error) {
	tracker := vectorized.QueryMemoryTracker(ctx, int64(p.cfg.QueryMemoryMiB)*1024*1024)
	vmeasure.RecordBatchResultOpened()
	return &BatchResult{
		source: &batchSliceSource{schema: schema, batches: batches}, schema: schema, batchSize: p.cfg.BatchSize,
		plan: p, request: req, tracker: tracker,
	}, nil
}

// Init implements PullOperator; execution has already initialized the result.
func (r *BatchResult) Init(ctx context.Context) error { return ctx.Err() }

// OutputSchema returns the explicit native output schema, including hidden inputs.
func (r *BatchResult) OutputSchema() *vectorized.BatchSchema { return r.schema }

// NextBatch lends the next native batch without protobuf serialization.
func (r *BatchResult) NextBatch(ctx context.Context) (*vectorized.RecordBatch, error) {
	if err := ctx.Err(); err != nil {
		_ = r.Close()
		return nil, err
	}
	if r.closed {
		return nil, nil
	}
	return r.source.NextBatch(ctx)
}

// Close releases all retained output owners and live memory reservations.
func (r *BatchResult) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	vmeasure.RecordBatchResultClosed()
	err := r.source.Close()
	r.source = nil
	if r.tracker != nil {
		r.tracker.Release(r.reserved)
	}
	r.reserved = 0
	// The scans end after the source stops reading them, and before the trace is finalized.
	for _, closer := range r.onClose {
		closer()
	}
	r.onClose = nil
	if r.tracedIterator != nil {
		if !r.tracedIterator.closing {
			if err != nil {
				r.iteratorSpan.Error(err)
			}
			return errors.Join(err, r.tracedIterator.Close())
		}
	} else {
		if r.iteratorSpan != nil {
			if err != nil {
				r.iteratorSpan.Error(err)
			}
			r.iteratorSpan.Stop()
		}
		r.finalizeTrace()
	}
	return err
}

// NativeIterator applies native pagination and hidden projection semantics.
// The iterator takes ownership of r and closes it on Close.
func (r *BatchResult) NativeIterator(ctx context.Context) (executor.MIterator, error) {
	return r.NativeIteratorFrom(ctx, r)
}

// NativeIteratorFrom serializes a batch consumer wrapping r, transferring Close ownership.
func (r *BatchResult) NativeIteratorFrom(ctx context.Context, source vectorized.PullOperator) (executor.MIterator, error) {
	builder := vectorized.NewPipelineBuilder().WithMemoryTracker(r.tracker).From(source)
	if r.request != nil {
		limit := r.request.GetLimit()
		if limit == 0 {
			limit = defaultLimit
		}
		builder.Apply(vmeasure.NewBatchLimit(r.OutputSchema(), r.request.GetOffset(), limit))
	}
	pipeline, err := builder.Build()
	if err != nil {
		if r.iteratorSpan != nil {
			r.iteratorSpan.Error(err)
		}
		_ = r.Close()
		return nil, fmt.Errorf("vec distributed plan: build output pipeline: %w", err)
	}
	if err = pipeline.Init(ctx); err != nil {
		if r.iteratorSpan != nil {
			r.iteratorSpan.Error(err)
		}
		_ = pipeline.Close()
		return nil, err
	}
	pool := vectorized.NewBatchPool(r.OutputSchema(), r.batchSize)
	var iter executor.MIterator = vmeasure.NewIteratorFromPipeline(vmeasure.WithFinalBatchSerialization(ctx), pipeline, r.OutputSchema(), pool)
	if r.plan != nil && !r.plan.hiddenOrderBy.IsEmpty() {
		iter = &hiddenTagsMIterator{inner: iter, hiddenTags: r.plan.hiddenOrderBy}
	}
	if r.plan != nil && r.plan.hiddenTopField != "" {
		iter = &hiddenFieldsMIterator{inner: iter, hiddenField: r.plan.hiddenTopField}
	}
	if r.iteratorSpan != nil {
		r.tracedIterator = &batchTraceIterator{MIterator: iter, ctx: ctx, result: r, span: r.iteratorSpan}
		iter = r.tracedIterator
	}
	return iter, nil
}

// QueryTrace returns the completed native execution trace after Close.
// Calling it before Close does not complete or snapshot an unfinished trace.
func (r *BatchResult) QueryTrace() *commonv1.Trace { return r.Trace }

// TraceIterator records final serialized consumption under the query processor's span.
// It must be configured before NativeIteratorFrom transfers consumption ownership.
func (r *BatchResult) TraceIterator(ctx context.Context) {
	if tracer := query.GetTracer(ctx); tracer != nil {
		r.iteratorSpan, _ = tracer.StartSpan(ctx, "iterator")
	}
}

// SetTraceFinalizer defers trace completion until batch consumption or Close has finished.
func (r *BatchResult) SetTraceFinalizer(finalizer func() *commonv1.Trace) {
	r.traceFinalizer = finalizer
}

func (r *BatchResult) finalizeTrace() {
	if r.traceFinalizer != nil {
		finalize := r.traceFinalizer
		r.traceFinalizer = nil
		r.Trace = finalize()
	}
}

type batchTraceIterator struct {
	executor.MIterator
	ctx      context.Context
	result   *BatchResult
	span     *query.Span
	closeErr error
	rounds   int
	size     int
	stopped  bool
	closing  bool
}

func (i *batchTraceIterator) Next() bool {
	if !i.MIterator.Next() {
		return false
	}
	i.rounds++
	if len(i.Current()) > 0 {
		i.size++
	}
	return true
}

func (i *batchTraceIterator) Err() error {
	if source, ok := i.MIterator.(interface{ Err() error }); ok {
		return source.Err()
	}
	return nil
}

func (i *batchTraceIterator) Close() error {
	if i.closing {
		return i.closeErr
	}
	i.closing = true
	i.closeErr = errors.Join(i.MIterator.Close(), i.ctx.Err())
	i.stopTrace(i.closeErr)
	i.result.finalizeTrace()
	return i.closeErr
}

func (i *batchTraceIterator) stopTrace(closeErr error) {
	if i.stopped {
		return
	}
	i.stopped = true
	for _, err := range []error{i.Err(), i.ctx.Err(), closeErr} {
		if err != nil {
			i.span.Error(err)
		}
	}
	i.span.Tagf(tracelabels.TagRounds, "%d", i.rounds)
	i.span.Tagf(tracelabels.TagSize, "%d", i.size)
	i.span.Stop()
}
