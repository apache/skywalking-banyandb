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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
)

func TestNewTracer(t *testing.T) {
	ctx := context.Background()
	tracer, newCtx := NewTracer(ctx, "test-trace-id")
	assert.NotNil(t, tracer)
	assert.NotEqual(t, ctx, newCtx, "context should be different after adding a tracer")
}

func TestGetTracer(t *testing.T) {
	ctx := context.Background()
	var tracer *Tracer
	tracer, ctx = NewTracer(ctx, "test-trace-id")

	retrievedTracer := GetTracer(ctx)
	assert.Equal(t, tracer, retrievedTracer, "retrieved tracer should be the same as the original")
}

func TestStartSpan(t *testing.T) {
	ctx := context.Background()
	var tracer *Tracer
	tracer, ctx = NewTracer(ctx, "test-trace-id")
	span, spanCtx := tracer.StartSpan(ctx, "test span %s", "1")

	assert.NotNil(t, span)
	assert.NotEqual(t, ctx, spanCtx, "context should be different after starting a span")
	assert.Equal(t, "test span 1", span.data.Message)
	assert.NotNil(t, span.data.StartTime)
}

func TestSpan_AddChild(t *testing.T) {
	ctx := context.Background()
	var tracer *Tracer
	tracer, ctx = NewTracer(ctx, "test-trace-id")
	var parentSpan *Span
	parentSpan, ctx = tracer.StartSpan(ctx, "parent span")
	childSpan, _ := tracer.StartSpan(ctx, "child span")

	assert.Contains(t, parentSpan.data.Children, childSpan.data, "parent span should contain the child span")
}

func TestSpan_AddSubTrace(t *testing.T) {
	ctx := context.Background()
	var tracer *Tracer
	tracer, ctx = NewTracer(ctx, "test-trace-id")
	span, _ := tracer.StartSpan(ctx, "span")

	subTrace := &commonv1.Trace{
		Spans: []*commonv1.Span{
			{Message: "sub span 1"},
			{Message: "sub span 2"},
		},
	}

	span.AddSubTrace(subTrace)

	assert.Equal(t, 2, len(span.data.Children), "span should contain two children")
	assert.Equal(t, "sub span 1", span.data.Children[0].Message)
	assert.Equal(t, "sub span 2", span.data.Children[1].Message)
}

func TestSpan_Tag(t *testing.T) {
	ctx := context.Background()
	var tracer *Tracer
	tracer, ctx = NewTracer(ctx, "test-trace-id")
	span, _ := tracer.StartSpan(ctx, "span")

	span.Tag("key", "value")

	assert.Equal(t, 1, len(span.data.Tags), "span should have one tag")
	assert.Equal(t, "key", span.data.Tags[0].Key)
	assert.Equal(t, "value", span.data.Tags[0].Value)

	span.Tagf("key", "value %s", "formatted")
	assert.Equal(t, 2, len(span.data.Tags), "span should have two tags")
	assert.Equal(t, "key", span.data.Tags[1].Key)
	assert.Equal(t, "value formatted", span.data.Tags[1].Value)
}

func TestSpan_Error(t *testing.T) {
	ctx := context.Background()
	var tracer *Tracer
	tracer, ctx = NewTracer(ctx, "test-trace-id")
	span, _ := tracer.StartSpan(ctx, "span")

	span.Error(fmt.Errorf("test error"))

	assert.True(t, span.data.Error, "span should be marked as error")
	assert.True(t, tracer.data.Error, "tracer should be marked as error")
	assert.Equal(t, "test error", span.data.Tags[0].Value, "error message should be added as a tag")
}

func TestSpan_Stop(t *testing.T) {
	ctx := context.Background()
	tracer, _ := NewTracer(ctx, "test-trace-id")
	span, _ := tracer.StartSpan(ctx, "span")
	time.Sleep(10 * time.Millisecond)

	span.Stop()

	assert.NotNil(t, span.data.EndTime, "span end time should be set")
	assert.Greater(t, span.data.Duration, int64(0), "span duration should be greater than 0")
}
