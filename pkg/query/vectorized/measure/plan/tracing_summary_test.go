// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

// Acceptance tests for Story 6 fanout-cap synthesis (US-VT-4).
//
// Design approach: all four tests use the manual-trace-construction approach
// documented in the plan's §11.5 practicality note. Synthetic nodeInfo values
// are built directly and passed to the applyFanoutCap / emitDecodeFrameSummarySpan
// helpers. A real query.Tracer is instantiated so the StartSpan machinery wires
// parent-child relationships correctly — but no real operator execution, network
// broadcast, or frame decoding is required.
//
// This approach was chosen because:
//   - The broadcast-fanout path requires a live dctx.Broadcast bus which is not
//     available in unit tests without a full banyand stack.
//   - The decode-frame path would require encoding valid RecordBatch frames for
//     timing, adding boilerplate not relevant to the invariant being tested.
//   - The helpers (applyFanoutCap, emitDecodeFrameSummarySpan) are pure functions
//     on input slices — testing them directly is both sufficient and CI-stable.

package plan

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/tracelabels"
)

// makeNodeTrace builds a minimal *commonv1.Trace with a single root span whose
// Duration is latencyNS and whose resp_count tag is set to rows.
func makeNodeTrace(nodeID int, latencyNS, rows int64, hasError bool) *commonv1.Trace {
	span := &commonv1.Span{
		Message:  fmt.Sprintf("data-node%d", nodeID),
		Duration: latencyNS,
		Error:    hasError,
		Tags: []*commonv1.Tag{
			{Key: tracelabels.TagRespCount, Value: strconv.FormatInt(rows, 10)},
		},
	}
	return &commonv1.Trace{
		TraceId: fmt.Sprintf("node-%d", nodeID),
		Error:   hasError,
		Spans:   []*commonv1.Span{span},
	}
}

// makeNodeInfo builds a nodeInfo from explicit fields and a synthetic trace.
func makeNodeInfo(nodeID int, latencyNS, rows, bytes int64, hasError bool) nodeInfo {
	trace := makeNodeTrace(nodeID, latencyNS, rows, hasError)
	return nodeInfo{
		trace:     trace,
		latencyNS: latencyNS,
		rows:      rows,
		bytes:     bytes,
		hasError:  hasError,
	}
}

// findSpanInTrace performs a depth-first search of the trace's span tree and
// returns the first span whose Message equals msg.
func findSpanInTrace(trace *commonv1.Trace, msg string) *commonv1.Span {
	for _, s := range trace.GetSpans() {
		if found := findSpanByMessage(s, msg); found != nil {
			return found
		}
	}
	return nil
}

// getTagValue returns the value of tag key from span's Tags, or "" if absent.
func getTagValue(span *commonv1.Span, key string) string {
	for _, tag := range span.GetTags() {
		if tag.GetKey() == key {
			return tag.GetValue()
		}
	}
	return ""
}

// assertTagValueEquals asserts that span has tag key with the given expected value.
func assertTagValueEquals(t *testing.T, span *commonv1.Span, key, want string) {
	t.Helper()
	got := getTagValue(span, key)
	if got != want {
		t.Errorf("span %q tag %q = %q, want %q", span.GetMessage(), key, got, want)
	}
}

// assertTagPresent asserts that span has a tag with the given key.
func assertTagPresent(t *testing.T, span *commonv1.Span, key string) {
	t.Helper()
	for _, tag := range span.GetTags() {
		if tag.GetKey() == key {
			return
		}
	}
	t.Errorf("span %q missing tag %q", span.GetMessage(), key)
}

// assertTagInt64Ordered asserts that tag values for keys[0] ≤ keys[1] ≤ ... on span.
func assertTagInt64Ordered(t *testing.T, span *commonv1.Span, keys ...string) {
	t.Helper()
	vals := make([]int64, len(keys))
	for idx, key := range keys {
		v, parseErr := strconv.ParseInt(getTagValue(span, key), 10, 64)
		if parseErr != nil {
			t.Errorf("span %q tag %q is not int64: %v", span.GetMessage(), key, parseErr)
			return
		}
		vals[idx] = v
	}
	for idx := 1; idx < len(vals); idx++ {
		if vals[idx-1] > vals[idx] {
			t.Errorf("span %q: tag %q (%d) > tag %q (%d); want non-decreasing order",
				span.GetMessage(), keys[idx-1], vals[idx-1], keys[idx], vals[idx])
		}
	}
}

// TestFanoutDataSummary_AtThreshold exercises the 25-node fanout-cap synthesis.
//
// Synthetic input (25 nodes total):
//   - 3 error nodes (nodeIDs 1–3, latency 100 ms each)
//   - 5 zero-row nodes (nodeIDs 4–8, latency 80 ms each)
//   - 17 healthy nodes (nodeIDs 9–25, latencies 10–170 ms, 10 ms steps)
//
// Expected behaviour:
//   - Exactly 19 individual children on the broadcast span + 1 data-summary child.
//   - The 19 individual slots are filled by: 3 error + 5 zero-row + 11 highest-latency healthy nodes.
//   - data-summary.aggregated_data_node_spans = 6  (the remaining 6 lower-latency healthy nodes).
//   - All aggregate tags are present on the data-summary span.
func TestFanoutDataSummary_AtThreshold(t *testing.T) {
	tracer, baseCtx := query.NewTracer(context.Background(), "fanout-at-threshold")

	// Start the broadcast span — broadcastSpanCtx has broadcastSpan as current span.
	broadcastSpan, broadcastSpanCtx := tracer.StartSpan(baseCtx, "broadcast-agg")

	// Build 25 synthetic nodes.
	nodes := make([]nodeInfo, 0, 25)
	// 3 error nodes.
	for idx := range 3 {
		nodes = append(nodes, makeNodeInfo(idx+1, int64(100*time.Millisecond), 0, 1024, true))
	}
	// 5 zero-row nodes.
	for idx := range 5 {
		nodes = append(nodes, makeNodeInfo(idx+4, int64(80*time.Millisecond), 0, 0, false))
	}
	// 17 healthy nodes with latencies 170ms, 160ms, ..., 10ms (descending so highest first in raw order).
	for idx := range 17 {
		latencyMS := int64((17 - idx) * 10) // 170, 160, ..., 10
		rows := int64((17 - idx) * 100)
		nodes = append(nodes, makeNodeInfo(idx+9, latencyMS*int64(time.Millisecond), rows, int64(rows*8), false))
	}

	applyFanoutCap(broadcastSpanCtx, broadcastSpan, nodes)
	broadcastSpan.Stop()

	proto := tracer.ToProto()
	broadcastProtoSpan := findSpanInTrace(proto, "broadcast-agg")
	if broadcastProtoSpan == nil {
		t.Fatal("broadcast-agg span not found in trace")
	}

	children := broadcastProtoSpan.GetChildren()
	// 19 individual + 1 data-summary = 20 children total.
	if len(children) != 20 {
		t.Fatalf("broadcast-agg children = %d, want 20 (19 individual + 1 data-summary)", len(children))
	}

	// The last child must be data-summary.
	lastChild := children[len(children)-1]
	if lastChild.GetMessage() != "data-summary" {
		t.Fatalf("last child message = %q, want data-summary", lastChild.GetMessage())
	}

	// data-summary.aggregated_data_node_spans = 6 (25 - 19).
	assertTagValueEquals(t, lastChild, tracelabels.TagAggregatedDataNodeSpans, "6")
	// All aggregate tags present.
	assertTagPresent(t, lastChild, tracelabels.TagNodesWithErrors)
	assertTagPresent(t, lastChild, tracelabels.TagNodesWithZeroRows)
	assertTagPresent(t, lastChild, tracelabels.TagTotalRowsAcrossNodes)
	assertTagPresent(t, lastChild, tracelabels.TagTotalBytesAcrossNodes)
	assertTagPresent(t, lastChild, tracelabels.TagNodeLatencyNSP50)
	assertTagPresent(t, lastChild, tracelabels.TagNodeLatencyNSP95)
	assertTagPresent(t, lastChild, tracelabels.TagNodeLatencyNSP99)
	assertTagPresent(t, lastChild, tracelabels.TagNodeLatencyNSMin)
	assertTagPresent(t, lastChild, tracelabels.TagNodeLatencyNSMax)

	// The 6 summarised nodes are all healthy (the 6 lowest-latency healthy ones: 10–60 ms).
	// So nodes_with_errors = 0 and nodes_with_zero_rows = 0 in the summary.
	assertTagValueEquals(t, lastChild, tracelabels.TagNodesWithErrors, "0")
	assertTagValueEquals(t, lastChild, tracelabels.TagNodesWithZeroRows, "0")

	// Latency ordering: min ≤ p50 ≤ p99 ≤ max.
	assertTagInt64Ordered(t, lastChild,
		tracelabels.TagNodeLatencyNSMin,
		tracelabels.TagNodeLatencyNSP50,
		tracelabels.TagNodeLatencyNSP99,
		tracelabels.TagNodeLatencyNSMax,
	)
}

// TestFanoutDataSummary_BelowThreshold exercises the ≤19-node path.
//
// With only 5 nodes, no data-summary span should be emitted and all 5 sub-traces
// must be attached individually as children of the broadcast span.
func TestFanoutDataSummary_BelowThreshold(t *testing.T) {
	tracer, baseCtx := query.NewTracer(context.Background(), "fanout-below-threshold")

	broadcastSpan, broadcastSpanCtx := tracer.StartSpan(baseCtx, "broadcast-rows")

	nodes := make([]nodeInfo, 5)
	for idx := range 5 {
		nodes[idx] = makeNodeInfo(idx+1, int64((idx+1)*10)*int64(time.Millisecond), int64((idx+1)*100), 1024, false)
	}

	applyFanoutCap(broadcastSpanCtx, broadcastSpan, nodes)
	broadcastSpan.Stop()

	proto := tracer.ToProto()
	broadcastProtoSpan := findSpanInTrace(proto, "broadcast-rows")
	if broadcastProtoSpan == nil {
		t.Fatal("broadcast-rows span not found in trace")
	}

	children := broadcastProtoSpan.GetChildren()
	if len(children) != 5 {
		t.Fatalf("broadcast-rows children = %d, want 5 (all individual, no summary)", len(children))
	}

	// Verify no data-summary child exists.
	for _, child := range children {
		if child.GetMessage() == "data-summary" {
			t.Fatalf("unexpected data-summary child found; should not emit summary for ≤19 nodes")
		}
	}
}

// TestDecodeFrameSummary_AtThreshold exercises the summary span when frame count > 19.
//
// 100 synthetic frame durations are supplied (1 ms each, varying slightly so
// percentile ordering is verifiable). After calling emitDecodeFrameSummarySpan
// exactly one "decode-frame-summary" child should be attached to the reduce-raw-frames span.
func TestDecodeFrameSummary_AtThreshold(t *testing.T) {
	tracer, baseCtx := query.NewTracer(context.Background(), "decode-summary-at-threshold")

	reduceSpan, reduceSpanCtx := tracer.StartSpan(baseCtx, "reduce-raw-frames")

	// 100 frame durations: 1ms, 2ms, ..., 100ms.
	durations := make([]time.Duration, 100)
	for idx := range 100 {
		durations[idx] = time.Duration(idx+1) * time.Millisecond
	}

	emitDecodeFrameSummarySpan(reduceSpanCtx, durations)
	reduceSpan.Stop()

	proto := tracer.ToProto()
	reduceProtoSpan := findSpanInTrace(proto, "reduce-raw-frames")
	if reduceProtoSpan == nil {
		t.Fatal("reduce-raw-frames span not found in trace")
	}

	children := reduceProtoSpan.GetChildren()
	if len(children) != 1 {
		t.Fatalf("reduce-raw-frames children = %d, want 1 (decode-frame-summary)", len(children))
	}

	summarySpan := children[0]
	if summarySpan.GetMessage() != "decode-frame-summary" {
		t.Fatalf("child message = %q, want decode-frame-summary", summarySpan.GetMessage())
	}

	// frames_total = 100.
	assertTagValueEquals(t, summarySpan, tracelabels.TagFramesTotal, "100")
	// frames_emitted_individually = 0 (no individual frame spans).
	assertTagValueEquals(t, summarySpan, tracelabels.TagFramesEmittedIndividually, "0")
	// frames_skipped = 100.
	assertTagValueEquals(t, summarySpan, tracelabels.TagFramesSkipped, "100")

	// All decode NS tags present.
	assertTagPresent(t, summarySpan, tracelabels.TagDecodeNSTotal)
	assertTagPresent(t, summarySpan, tracelabels.TagDecodeNSP50)
	assertTagPresent(t, summarySpan, tracelabels.TagDecodeNSP99)
	assertTagPresent(t, summarySpan, tracelabels.TagDecodeNSMax)

	// Ordering: p50 ≤ p99 ≤ max (durations are 1–100 ms so p50 < p99 < max strictly).
	assertTagInt64Ordered(t, summarySpan,
		tracelabels.TagDecodeNSP50,
		tracelabels.TagDecodeNSP99,
		tracelabels.TagDecodeNSMax,
	)

	// Verify decode_ns_total is consistent: sum of 1..100 ms.
	expectedTotalNS := int64(0)
	for _, d := range durations {
		expectedTotalNS += d.Nanoseconds()
	}
	totalStr := getTagValue(summarySpan, tracelabels.TagDecodeNSTotal)
	gotTotal, parseErr := strconv.ParseInt(totalStr, 10, 64)
	if parseErr != nil {
		t.Fatalf("decode_ns_total tag is not int64: %v", parseErr)
	}
	if gotTotal != expectedTotalNS {
		t.Errorf("decode_ns_total = %d, want %d", gotTotal, expectedTotalNS)
	}
}

// TestDecodeFrameSummary_BelowThreshold verifies that no summary span is emitted
// when frame count ≤ 19.
func TestDecodeFrameSummary_BelowThreshold(t *testing.T) {
	tracer, baseCtx := query.NewTracer(context.Background(), "decode-summary-below-threshold")

	reduceSpan, reduceSpanCtx := tracer.StartSpan(baseCtx, "reduce-raw-frames")

	// 5 frame durations — below the threshold of maxIndividualSubTraces (19).
	durations := make([]time.Duration, 5)
	for idx := range 5 {
		durations[idx] = time.Duration(idx+1) * time.Millisecond
	}

	emitDecodeFrameSummarySpan(reduceSpanCtx, durations)
	reduceSpan.Stop()

	proto := tracer.ToProto()
	reduceProtoSpan := findSpanInTrace(proto, "reduce-raw-frames")
	if reduceProtoSpan == nil {
		t.Fatal("reduce-raw-frames span not found in trace")
	}

	children := reduceProtoSpan.GetChildren()
	if len(children) != 0 {
		t.Fatalf("reduce-raw-frames children = %d, want 0 (no summary for ≤19 frames)", len(children))
	}
}
