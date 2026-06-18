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

package querybench

import (
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
)

func TestBuildScenarioQueryScanAllMatchesFixtureShape(t *testing.T) {
	req, buildErr := buildScenarioQuery(ScenarioScanAll, 1024, time.Unix(0, 0))
	if buildErr != nil {
		t.Fatalf("buildScenarioQuery() failed: %v", buildErr)
	}
	if req.GetName() != benchMeasureName || req.GetGroups()[0] != benchGroupName || req.GetLimit() != 1024 {
		t.Fatalf("unexpected scan-all request identity: %+v", req)
	}
	if len(req.GetTagProjection().GetTagFamilies()[0].GetTags()) != 2 || len(req.GetFieldProjection().GetNames()) != 2 {
		t.Fatalf("scan-all projection does not match all.yaml: %+v", req)
	}
}

func TestBuildScenarioQueryTopWithFilterMatchesFixtureShape(t *testing.T) {
	req, buildErr := buildScenarioQuery(ScenarioTopWithFilter, 1024, time.Unix(0, 0))
	if buildErr != nil {
		t.Fatalf("buildScenarioQuery() failed: %v", buildErr)
	}
	condition := req.GetCriteria().GetCondition()
	if condition.GetName() != benchTagID || condition.GetOp() != modelv1.Condition_BINARY_OP_NE || condition.GetValue().GetStr().GetValue() != "svc3" {
		t.Fatalf("top-with-filter condition does not match fixture: %+v", condition)
	}
	agg := req.GetAgg()
	top := req.GetTop()
	if agg.GetFunction() != modelv1.AggregationFunction_AGGREGATION_FUNCTION_MEAN ||
		top.GetNumber() != 2 ||
		top.GetFieldValueSort() != modelv1.Sort_SORT_DESC {
		t.Fatalf("top-with-filter agg/top does not match fixture: %+v", req)
	}
}

func TestHashResponseStable(t *testing.T) {
	dpA := &measurev1.DataPoint{Timestamp: timestamppb.New(time.Unix(1, 0))}
	dpB := &measurev1.DataPoint{Timestamp: timestamppb.New(time.Unix(2, 0))}
	respForward := &measurev1.QueryResponse{DataPoints: []*measurev1.DataPoint{dpA, dpB}}
	respReverse := &measurev1.QueryResponse{DataPoints: []*measurev1.DataPoint{dpB, dpA}}
	hashForward := hashResponse(respForward)
	hashReverse := hashResponse(respReverse)
	if hashForward != hashReverse {
		t.Fatalf("hashResponse must be order-independent: forward=%d reverse=%d", hashForward, hashReverse)
	}
	if hashForward == 0 {
		t.Fatalf("hashResponse must be non-zero for non-empty response")
	}
	respSingle := &measurev1.QueryResponse{DataPoints: []*measurev1.DataPoint{dpA}}
	if hashResponse(respSingle) == hashForward {
		t.Fatalf("hashResponse must differ when point set differs")
	}
	emptyA := &measurev1.QueryResponse{}
	emptyB := &measurev1.QueryResponse{}
	if hashResponse(emptyA) != hashResponse(emptyB) {
		t.Fatalf("hashResponse must be deterministic for empty response")
	}
}

func TestBuildTraceScenarioQueryByIDPinsLimitToBatch(t *testing.T) {
	cfg := Config{TraceIDBatch: 10, SpansPerTrace: 20, SpanDist: spanDistUniform}
	shape := deriveTraceShape(1000, 20, spanDistUniform)
	req, buildErr := buildTraceScenarioQuery(ScenarioTraceByID, cfg, shape, time.Unix(0, 0))
	if buildErr != nil {
		t.Fatalf("buildTraceScenarioQuery() failed: %v", buildErr)
	}
	if req.GetName() != benchTraceName || req.GetGroups()[0] != benchTraceGroupName || req.GetLimit() != 10 {
		t.Fatalf("unexpected trace-id request identity: %+v", req)
	}
	condition := req.GetCriteria().GetCondition()
	if condition.GetName() != traceTagTraceID || condition.GetOp() != modelv1.Condition_BINARY_OP_IN || len(condition.GetValue().GetStrArray().GetValue()) != 10 {
		t.Fatalf("trace-id criteria does not use trace_id IN batch: %+v", condition)
	}
}

func TestBuildTraceScenarioQueryTagFilterRequiresOrderAndLimit(t *testing.T) {
	cfg := Config{TraceIDBatch: 1, SpansPerTrace: 20, SpanDist: spanDistUniform}
	shape := deriveTraceShape(1000, 20, spanDistUniform)
	req, buildErr := buildTraceScenarioQuery(ScenarioTraceTagFilter, cfg, shape, time.Unix(0, 0))
	if buildErr != nil {
		t.Fatalf("buildTraceScenarioQuery() failed: %v", buildErr)
	}
	condition := req.GetCriteria().GetCondition()
	if condition.GetName() != traceTagServiceID || condition.GetOp() != modelv1.Condition_BINARY_OP_EQ ||
		condition.GetValue().GetStr().GetValue() != "svc-0" {
		t.Fatalf("trace tag-filter condition does not match benchmark shape: %+v", condition)
	}
	if req.GetOrderBy().GetIndexRuleName() != traceIndexTimestamp || req.GetOrderBy().GetSort() != modelv1.Sort_SORT_DESC || req.GetLimit() != traceQueryLimit {
		t.Fatalf("trace tag-filter must pin order_by and limit: %+v", req)
	}
}

func TestExpectedTraceResultCount(t *testing.T) {
	shape := deriveTraceShape(10000, 20, spanDistUniform)
	cfg := Config{TraceIDBatch: 10, FilterSelectivity: 0.01}
	if got := expectedTraceResultCount(ScenarioTraceByID, cfg, shape); got != 10 {
		t.Fatalf("trace-id expected result count = %d, want 10", got)
	}
	if got := expectedTraceResultCount(ScenarioTraceTagFilter, cfg, shape); got != 5 {
		t.Fatalf("tag-filter expected result count = %d, want 5", got)
	}
	cfg.FilterSelectivity = 1
	if got := expectedTraceResultCount(ScenarioTraceTagFilter, cfg, shape); got != traceQueryLimit {
		t.Fatalf("tag-filter expected result count must cap at query limit: got %d want %d", got, traceQueryLimit)
	}
}

func TestSplitTraceWriteChunksBoundsLargeUniformShape(t *testing.T) {
	shape := deriveTraceShape(2000000, 20, spanDistUniform)
	chunks := splitTraceWriteChunks(shape, 0, shape.TraceCount)
	if len(chunks) != 100 {
		t.Fatalf("splitTraceWriteChunks() produced %d chunks, want 100", len(chunks))
	}
	seenTraces := 0
	for chunkIdx, chunk := range chunks {
		if chunk.startTrace != seenTraces {
			t.Fatalf("chunk %d starts at %d, want %d", chunkIdx, chunk.startTrace, seenTraces)
		}
		chunkSpans := 0
		for traceIdx := chunk.startTrace; traceIdx < chunk.endTrace; traceIdx++ {
			chunkSpans += shape.SpansByTrace[traceIdx]
		}
		if chunkSpans > traceWriteBatchSpans {
			t.Fatalf("chunk %d has %d spans, exceeds %d", chunkIdx, chunkSpans, traceWriteBatchSpans)
		}
		seenTraces = chunk.endTrace
	}
	if seenTraces != shape.TraceCount {
		t.Fatalf("splitTraceWriteChunks() covered %d traces, want %d", seenTraces, shape.TraceCount)
	}
}

func TestHashTraceResponseStable(t *testing.T) {
	spanA := &tracev1.Span{SpanId: "span-a", Span: []byte("a")}
	spanB := &tracev1.Span{SpanId: "span-b", Span: []byte("b")}
	traceA := &tracev1.Trace{TraceId: "trace-a", Spans: []*tracev1.Span{spanA, spanB}}
	traceB := &tracev1.Trace{TraceId: "trace-b", Spans: []*tracev1.Span{spanB}}
	respForward := &tracev1.QueryResponse{Traces: []*tracev1.Trace{traceA, traceB}}
	respReverse := &tracev1.QueryResponse{
		Traces: []*tracev1.Trace{
			{TraceId: "trace-b", Spans: []*tracev1.Span{spanB}},
			{TraceId: "trace-a", Spans: []*tracev1.Span{spanB, spanA}},
		},
	}
	hashForward := hashTraceResponse(respForward, false)
	hashReverse := hashTraceResponse(respReverse, false)
	if hashForward != hashReverse {
		t.Fatalf("unordered hashTraceResponse must be order-independent: forward=%d reverse=%d", hashForward, hashReverse)
	}
	if hashForward == 0 {
		t.Fatalf("hashTraceResponse must be non-zero for non-empty response")
	}
	if hashTraceResponse(&tracev1.QueryResponse{Traces: []*tracev1.Trace{traceA}}, false) == hashForward {
		t.Fatalf("hashTraceResponse must differ when trace set differs")
	}
}

func TestHashTraceResponsePreservesOrderedTraceOrder(t *testing.T) {
	traceA := &tracev1.Trace{TraceId: "trace-a", Spans: []*tracev1.Span{{SpanId: "span-a", Span: []byte("a")}}}
	traceB := &tracev1.Trace{TraceId: "trace-b", Spans: []*tracev1.Span{{SpanId: "span-b", Span: []byte("b")}}}
	respForward := &tracev1.QueryResponse{Traces: []*tracev1.Trace{traceA, traceB}}
	respReverse := &tracev1.QueryResponse{Traces: []*tracev1.Trace{traceB, traceA}}
	hashForward := hashTraceResponse(respForward, true)
	hashReverse := hashTraceResponse(respReverse, true)
	if hashForward == hashReverse {
		t.Fatalf("ordered hashTraceResponse must differ when trace order differs: hash=%d", hashForward)
	}
}
