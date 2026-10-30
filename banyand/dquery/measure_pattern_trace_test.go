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
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package dquery

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
)

type patternTraceService struct{ traceMeasureService }

func (patternTraceService) Measure(*commonv1.Metadata) (measure.Measure, error) {
	return patternTraceContext{}, nil
}

type patternTraceContext struct{ traceMeasureContext }

func (patternTraceContext) GetIndexRules() []*databasev1.IndexRule {
	return []*databasev1.IndexRule{{Metadata: &commonv1.Metadata{Name: "service", Id: 7}, Tags: []string{"service_id"}, Type: databasev1.IndexRule_TYPE_INVERTED}}
}

type patternRejectedFuture struct {
	bus.Future
	err error
}

func (f patternRejectedFuture) Get() (bus.Message, error) { return bus.Message{}, f.err }

type patternMixedWorkers struct {
	body  []byte
	topic bus.Topic
}

func (b *patternMixedWorkers) Broadcast(_ time.Duration, topic bus.Topic, _ bus.Message) ([]bus.Future, error) {
	b.topic = topic
	return []bus.Future{traceMeasureFuture{body: b.body}, patternRejectedFuture{err: errors.New("invalid topic")}}, nil
}

func TestPatternTracedOldWorkerRejectsWholeQuery(t *testing.T) {
	processor, request := newTraceMeasureProcessor(t, nil)
	processor.measureService = patternTraceService{}
	workers := &patternMixedWorkers{body: processor.broadcaster.(traceMeasureBroadcaster).body}
	processor.broadcaster = workers
	tracer, ctx := query.NewTracer(t.Context(), t.Name())
	ctx = vectorized.WithQueryMemoryTracker(ctx, 4<<20)
	response := processor.Rev(ctx, bus.NewMessage(1, &vmeasure.BatchQueryRequest{
		Request: request, Patterns: []model.IndexedPattern{{TagFamily: "default", TagName: "service_id", Pattern: "%a%", Escape: "\\", IgnoreCase: true}},
	}))
	resultErr, ok := response.Data().(error)
	require.True(t, ok, "partial frames must not become a successful traced result")
	require.ErrorContains(t, resultErr, "invalid topic")
	require.Equal(t, data.TopicInternalMeasurePatternQuery, workers.topic)
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
	root := findMeasureTraceSpan(tracer.ToProto().Spans, "distributed-test")
	require.NotNil(t, root)
	require.NotNil(t, root.EndTime)
	require.True(t, root.Error)
}
