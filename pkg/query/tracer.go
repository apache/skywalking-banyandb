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
	spanMap map[*commonv1.Span]*Span // map from span data to span wrapper
	data    *commonv1.Trace
	spans   []*Span // all spans created by this tracer
	mu      sync.Mutex
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
		spanMap: make(map[*commonv1.Span]*Span),
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

	// Track span in tracer
	t.mu.Lock()
	t.spans = append(t.spans, s)
	t.spanMap[s.data] = s
	t.mu.Unlock()

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
func (t *Tracer) ToProto() *commonv1.Trace {
	return t.DeepCopy()
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

// DeepCopy creates a deep copy of the trace.
// This method is thread-safe and can be called concurrently with span operations.
// It captures a consistent snapshot of the trace tree by locking the tracer and each span as needed.
func (t *Tracer) DeepCopy() *commonv1.Trace {
	if t == nil || t.data == nil {
		return nil
	}

	// Lock the tracer to snapshot the structure, then unlock before locking spans
	// to avoid deadlock (other methods lock span -> tracer)
	t.mu.Lock()
	traceID := t.data.TraceId
	traceError := t.data.Error
	spansToCopy := t.data.Spans
	spanMap := t.spanMap
	t.mu.Unlock()

	result := &commonv1.Trace{
		TraceId: traceID,
		Error:   traceError,
	}

	if len(spansToCopy) > 0 {
		result.Spans = make([]*commonv1.Span, len(spansToCopy))
		for i, span := range spansToCopy {
			result.Spans[i] = deepCopySpanWithLock(span, spanMap)
		}
	}

	return result
}

// deepCopySpanWithLock creates a deep copy of a span with proper locking.
// Locks the span wrapper if available to ensure thread-safe reading.
func deepCopySpanWithLock(src *commonv1.Span, spanMap map[*commonv1.Span]*Span) *commonv1.Span {
	if src == nil {
		return nil
	}

	// Look up the span wrapper and lock it while copying
	if wrapper, ok := spanMap[src]; ok {
		wrapper.mu.Lock()
		defer wrapper.mu.Unlock()
	}

	result := &commonv1.Span{
		Error:    src.Error,
		Message:  src.Message,
		Duration: src.Duration,
	}

	// Deep copy StartTime
	if src.StartTime != nil {
		result.StartTime = timestamppb.New(src.StartTime.AsTime())
	}

	// Deep copy EndTime
	if src.EndTime != nil {
		result.EndTime = timestamppb.New(src.EndTime.AsTime())
	}

	// Deep copy Tags
	if len(src.Tags) > 0 {
		result.Tags = make([]*commonv1.Tag, len(src.Tags))
		for i, tag := range src.Tags {
			result.Tags[i] = deepCopyTag(tag)
		}
	}

	// Deep copy Children recursively
	if len(src.Children) > 0 {
		result.Children = make([]*commonv1.Span, len(src.Children))
		for i, child := range src.Children {
			result.Children[i] = deepCopySpanWithLock(child, spanMap)
		}
	}

	return result
}

// deepCopyTag creates a deep copy of a tag.
func deepCopyTag(src *commonv1.Tag) *commonv1.Tag {
	if src == nil {
		return nil
	}

	return &commonv1.Tag{
		Key:   src.Key,
		Value: src.Value,
	}
}
