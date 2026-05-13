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

package vectorized

import (
	"context"
	"errors"
)

// Pipeline is the composed sequence of stages from source to final breaker.
// It exposes a single PullOperator-shaped Next method to the driver.
type Pipeline struct {
	head    PullOperator
	tracker *MemoryTracker
	closed  bool
}

// Next returns the next batch from the head stage.
func (p *Pipeline) Next(ctx context.Context) (*RecordBatch, error) {
	return p.head.NextBatch(ctx)
}

// Tracker returns the shared per-pipeline MemoryTracker, or nil if the builder
// did not set one. Operators that bookkeep memory should be constructed with
// this tracker so they all draw from a single budget.
func (p *Pipeline) Tracker() *MemoryTracker { return p.tracker }

// Close closes the head stage. Idempotent — repeat calls are no-ops.
func (p *Pipeline) Close() error {
	if p.closed {
		return nil
	}
	p.closed = true
	return p.head.Close()
}

// PipelineBuilder fluently composes a Pipeline.
type PipelineBuilder struct {
	source       PullOperator
	tracker      *MemoryTracker
	pendingFused []FusibleOperator
	breakers     []BreakerOperator
}

// NewPipelineBuilder starts a builder.
func NewPipelineBuilder() *PipelineBuilder { return &PipelineBuilder{} }

// From sets the leaf source.
func (b *PipelineBuilder) From(p PullOperator) *PipelineBuilder { b.source = p; return b }

// WithMemoryTracker attaches a shared MemoryTracker to the pipeline. Operators
// that bookkeep memory (BatchGroupBy, BatchAggregation) should be constructed
// with this same tracker so reservations stack against a single budget.
func (b *PipelineBuilder) WithMemoryTracker(t *MemoryTracker) *PipelineBuilder {
	b.tracker = t
	return b
}

// Apply queues a FusibleOperator to fold into the next stage.
func (b *PipelineBuilder) Apply(f FusibleOperator) *PipelineBuilder {
	b.pendingFused = append(b.pendingFused, f)
	return b
}

// Break appends a breaker, finalizing the current fused-stage prefix.
func (b *PipelineBuilder) Break(br BreakerOperator) *PipelineBuilder {
	b.breakers = append(b.breakers, br)
	return b
}

// Build validates and constructs the Pipeline.
func (b *PipelineBuilder) Build() (*Pipeline, error) {
	if b.source == nil {
		return nil, errors.New("vectorized: pipeline missing source (use From)")
	}
	var head PullOperator = newFusedStage(b.source, b.pendingFused)
	for _, br := range b.breakers {
		head = newBreakerStage(head, br)
	}
	return &Pipeline{head: head, tracker: b.tracker}, nil
}

// breakerStage wraps a BreakerOperator so it acts as a PullOperator for the next stage.
// It drains the upstream PullOperator into the breaker's Consume, calls Finalize once,
// then serves the breaker's output via NextBatch.
//
// Error stickiness: if upstream Pull, Consume, or Finalize fails, the error is
// stored and returned on every subsequent NextBatch call. drained is set as
// soon as the consume loop is entered, so a retry never re-runs Consume or
// Finalize. This guarantees Finalize executes at most once per stage.
type breakerStage struct {
	err      error
	upstream PullOperator
	breaker  BreakerOperator
	drained  bool
	closed   bool
}

func newBreakerStage(upstream PullOperator, br BreakerOperator) *breakerStage {
	return &breakerStage{upstream: upstream, breaker: br}
}

func (s *breakerStage) Init(ctx context.Context) error {
	if initErr := s.upstream.Init(ctx); initErr != nil {
		return initErr
	}
	return s.breaker.Init(ctx)
}

func (s *breakerStage) OutputSchema() *BatchSchema { return s.breaker.OutputSchema() }

func (s *breakerStage) NextBatch(ctx context.Context) (*RecordBatch, error) {
	if s.err != nil {
		return nil, s.err
	}
	if !s.drained {
		s.drained = true
		for {
			b, pullErr := s.upstream.NextBatch(ctx)
			if pullErr != nil {
				s.err = pullErr
				return nil, pullErr
			}
			if b == nil {
				break
			}
			if consumeErr := s.breaker.Consume(ctx, b); consumeErr != nil {
				s.err = consumeErr
				return nil, consumeErr
			}
		}
		if finalizeErr := s.breaker.Finalize(ctx); finalizeErr != nil {
			s.err = finalizeErr
			return nil, finalizeErr
		}
	}
	return s.breaker.NextBatch(ctx)
}

// Close is idempotent — children are invoked once across repeated calls.
func (s *breakerStage) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	var firstErr error
	if closeErr := s.upstream.Close(); closeErr != nil {
		firstErr = closeErr
	}
	if closeErr := s.breaker.Close(); closeErr != nil && firstErr == nil {
		firstErr = closeErr
	}
	return firstErr
}
