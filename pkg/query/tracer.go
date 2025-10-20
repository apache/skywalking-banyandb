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

package query

import (
	"context"
	"fmt"
	"sync"

	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
)

const (
	// maxChildSpans is the maximum number of direct child spans allowed under a parent span.
	maxChildSpans = 20
)

var (
	spanKey   = spanContextKey{}
	tracerKey = tracerContextKey{}
)

type (
	spanContextKey   struct{}
	tracerContextKey struct{}
)

// Tracer is a simple tracer for query.
// Thread-safety: StartSpan and span mutations are thread-safe and can be called
// concurrently from multiple goroutines. ToProto() is safe to call concurrently
// with span operations - it will wait for any async operations to complete.
type Tracer struct {
	data *commonv1.Trace
	mu   sync.Mutex
	// wg tracks async operations that may mutate spans after they're created
	wg sync.WaitGroup
}

// NewTracer creates a new tracer.
func NewTracer(ctx context.Context, id string) (*Tracer, context.Context) {
	tracer := GetTracer(ctx)
	if tracer != nil {
		return tracer, ctx
	}
	t := &Tracer{
		data: &commonv1.Trace{
			TraceId: id,
		},
	}
	return t, context.WithValue(ctx, tracerKey, t)
}

// GetTracer returns the tracer from the context.
func GetTracer(ctx context.Context) *Tracer {
	tv := ctx.Value(tracerKey)
	if tv == nil {
		return nil
	}
	tracer, ok := ctx.Value(tracerKey).(*Tracer)
	if ok {
		return tracer
	}
	panic(fmt.Errorf("invalid tracer context value: %v", tv))
}

// StartSpan starts a new span.
func (t *Tracer) StartSpan(ctx context.Context, format string, args ...interface{}) (*Span, context.Context) {
	s := &Span{
		data: &commonv1.Span{
			Message:   fmt.Sprintf(format, args...),
			StartTime: timestamppb.Now(),
		},
		tracer: t,
	}
	sv := ctx.Value(spanKey)
	if sv == nil {
		t.mu.Lock()
		t.data.Spans = append(t.data.Spans, s.data)
		t.mu.Unlock()
		return s, context.WithValue(ctx, spanKey, s)
	}
	parentSpan, ok := ctx.Value(spanKey).(*Span)
	if ok {
		parentSpan.addChild(s.data)
	} else {
		t.mu.Lock()
		t.data.Spans = append(t.data.Spans, s.data)
		t.mu.Unlock()
	}
	return s, context.WithValue(ctx, spanKey, s)
}

// ToProto returns the proto representation of the tracer.
// This method waits for any async span operations to complete before returning,
// ensuring thread-safe access to the trace data.
func (t *Tracer) ToProto() *commonv1.Trace {
	// Wait for any async operations that might still be mutating span data
	t.wg.Wait()
	return t.data
}

// AddAsyncOp registers an async operation that will mutate span data.
// Must be paired with a call to DoneAsyncOp() when the operation completes.
func (t *Tracer) AddAsyncOp() {
	t.wg.Add(1)
}

// DoneAsyncOp marks an async operation as complete.
func (t *Tracer) DoneAsyncOp() {
	t.wg.Done()
}

// Span is a span of the tracer.
type Span struct {
	data            *commonv1.Span
	tracer          *Tracer
	mu              sync.Mutex
	ignoredChildren int
}

func (s *Span) addChild(child *commonv1.Span) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Check if we've reached the maximum number of child spans
	if len(s.data.Children) >= maxChildSpans {
		s.ignoredChildren++
		return
	}
	s.data.Children = append(s.data.Children, child)
	if child.Error {
		// Inline error handling to avoid recursive lock
		if !s.data.Error {
			s.data.Error = true
			s.data.Tags = append(s.data.Tags, &commonv1.Tag{
				Key:   "error_msg",
				Value: "sub span error",
			})
			s.tracer.mu.Lock()
			s.tracer.data.Error = true
			s.tracer.mu.Unlock()
		}
	}
}

// AddSubTrace adds a sub trace to the span.
func (s *Span) AddSubTrace(trace *commonv1.Trace) {
	if trace == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	hasError := false
	for i := range trace.Spans {
		// Check if we've reached the maximum number of child spans
		if len(s.data.Children) >= maxChildSpans {
			s.ignoredChildren++
			continue
		}
		s.data.Children = append(s.data.Children, trace.Spans[i])
		if trace.Spans[i].Error {
			hasError = true
		}
	}
	if hasError && !s.data.Error {
		s.data.Error = true
		s.data.Tags = append(s.data.Tags, &commonv1.Tag{
			Key:   "error_msg",
			Value: "sub span error",
		})
		s.tracer.mu.Lock()
		s.tracer.data.Error = true
		s.tracer.mu.Unlock()
	}
}

// Tag adds a tag to the span.
func (s *Span) Tag(key, value string) *Span {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data.Tags = append(s.data.Tags, &commonv1.Tag{
		Key:   key,
		Value: value,
	})
	return s
}

// Tagf adds a formatted tag to the span.
func (s *Span) Tagf(key, format string, args ...any) *Span {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data.Tags = append(s.data.Tags, &commonv1.Tag{
		Key:   key,
		Value: fmt.Sprintf(format, args...),
	})
	return s
}

// Error marks the span as an error span.
func (s *Span) Error(err error) *Span {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.data.Error {
		return s
	}
	s.data.Error = true
	// Inline Tag() to avoid recursive lock
	s.data.Tags = append(s.data.Tags, &commonv1.Tag{
		Key:   "error_msg",
		Value: err.Error(),
	})
	s.tracer.mu.Lock()
	s.tracer.data.Error = true
	s.tracer.mu.Unlock()
	return s
}

// recordIgnoredChildren adds a tag to record the number of ignored child spans.
// Must be called with s.mu held.
func (s *Span) recordIgnoredChildren() {
	if s.ignoredChildren == 0 {
		return
	}
	// Check for existing "ignored_child_spans" tag to avoid duplicates
	for _, tag := range s.data.Tags {
		if tag.Key == "ignored_child_spans" {
			return
		}
	}
	s.data.Tags = append(s.data.Tags, &commonv1.Tag{
		Key:   "ignored_child_spans",
		Value: fmt.Sprintf("%d", s.ignoredChildren),
	})
}

// Stop stops the span.
func (s *Span) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recordIgnoredChildren()
	s.data.EndTime = timestamppb.Now()
	s.data.Duration = s.data.EndTime.AsTime().Sub(s.data.StartTime.AsTime()).Nanoseconds()
}
