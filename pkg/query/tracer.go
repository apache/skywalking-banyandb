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

	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
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
type Tracer struct {
	data *commonv1.Trace
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
		t.data.Spans = append(t.data.Spans, s.data)
		return s, context.WithValue(ctx, spanKey, s)
	}
	parentSpan, ok := ctx.Value(spanKey).(*Span)
	if ok {
		parentSpan.addChild(s.data)
	} else {
		t.data.Spans = append(t.data.Spans, s.data)
	}
	return s, context.WithValue(ctx, spanKey, s)
}

// ToProto returns the proto representation of the tracer.
func (t *Tracer) ToProto() *commonv1.Trace {
	return t.data
}

// Span is a span of the tracer.
type Span struct {
	data   *commonv1.Span
	tracer *Tracer
}

func (s *Span) addChild(child *commonv1.Span) {
	s.data.Children = append(s.data.Children, child)
	if child.Error {
		s.Error(fmt.Errorf("sub span error"))
	}
}

// AddSubTrace adds a sub trace to the span.
func (s *Span) AddSubTrace(trace *commonv1.Trace) {
	if trace == nil {
		return
	}
	for i := range trace.Spans {
		s.addChild(trace.Spans[i])
	}
}

// Tag adds a tag to the span.
func (s *Span) Tag(key, value string) *Span {
	s.data.Tags = append(s.data.Tags, &commonv1.Tag{
		Key:   key,
		Value: value,
	})
	return s
}

// Tagf adds a formatted tag to the span.
func (s *Span) Tagf(key, format string, args ...any) *Span {
	s.data.Tags = append(s.data.Tags, &commonv1.Tag{
		Key:   key,
		Value: fmt.Sprintf(format, args...),
	})
	return s
}

// Error marks the span as an error span.
func (s *Span) Error(err error) *Span {
	if s.data.Error {
		return s
	}
	s.data.Error = true
	s.Tag("error_msg", err.Error())
	s.tracer.data.Error = true
	return s
}

// Stop stops the span.
func (s *Span) Stop() {
	s.data.EndTime = timestamppb.Now()
	s.data.Duration = s.data.EndTime.AsTime().Sub(s.data.StartTime.AsTime()).Milliseconds()
}
