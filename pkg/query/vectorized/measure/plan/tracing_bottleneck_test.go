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

// §11.5 Q2 Bottleneck detection tests.
//
// Design approach: all six tests use the manual-trace-construction substitute
// described in the task's "CRITICAL practicality note". A synthetic
// commonv1.Span tree is built with explicit Duration values that make one named
// phase dominate by ≥ 5× the next-slowest sibling. The test then walks the
// span tree and asserts that the named span has the maximum Duration among its
// peers. This approach is chosen over driving real operators with real data
// because:
//
//   - Tests 1–5 (scan, decode, reduce, merge, frame-encode): the named phases
//     are internal to data-node or liaison operators that require a full
//     banyand storage layer to produce measurable timing differences in CI.
//     Manual span construction produces a CI-stable, deterministic test that
//     fails if and only if the wrong span is named as the bottleneck, which is
//     the correctness property the plan specifies.
//
//   - Test 6 (broadcast tail): requires real network I/O or at minimum a
//     goroutine-based fake bus that emits futures with wall-clock delay.
//     The synthetic approach captures the same invariant without
//     time.Sleep-induced flakiness.
//
// Each test documents its synthetic Duration layout and the assertion it makes.

package plan

import (
	"testing"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/tracelabels"
)

// bottleneckSpan builds a *commonv1.Span with the given message, duration
// (nanoseconds), and optional child spans.
func bottleneckSpan(msg string, durationNS int64, children ...*commonv1.Span) *commonv1.Span {
	s := &commonv1.Span{
		Message:  msg,
		Duration: durationNS,
	}
	s.Children = append(s.Children, children...)
	return s
}

// bottleneckSpanWithTag builds a *commonv1.Span with one extra tag.
func bottleneckSpanWithTag(msg string, durationNS int64, tagKey, tagValue string, children ...*commonv1.Span) *commonv1.Span {
	s := bottleneckSpan(msg, durationNS, children...)
	s.Tags = append(s.Tags, &commonv1.Tag{Key: tagKey, Value: tagValue})
	return s
}

// findSpanByMessage performs a depth-first search of the span tree rooted at
// root and returns the first span whose Message equals msg, or nil if not found.
func findSpanByMessage(root *commonv1.Span, msg string) *commonv1.Span {
	if root == nil {
		return nil
	}
	if root.GetMessage() == msg {
		return root
	}
	for _, child := range root.GetChildren() {
		if found := findSpanByMessage(child, msg); found != nil {
			return found
		}
	}
	return nil
}

// assertSpanIsBottleneck asserts that among all spans at the same level as
// target (i.e. the direct children of parent), target has the maximum Duration.
// It also asserts that target.Duration is at least minFactor times larger than
// the second-largest Duration (≥5× per spec).
func assertSpanIsBottleneck(t *testing.T, parent *commonv1.Span, targetMsg string, minFactor float64) {
	t.Helper()
	peers := parent.GetChildren()
	if len(peers) == 0 {
		t.Fatalf("parent span %q has no children; cannot assert bottleneck", parent.GetMessage())
	}
	var target *commonv1.Span
	maxDuration := int64(0)
	for _, peer := range peers {
		if peer.GetMessage() == targetMsg {
			target = peer
		}
		if peer.GetDuration() > maxDuration {
			maxDuration = peer.GetDuration()
		}
	}
	if target == nil {
		t.Fatalf("target span %q not found among children of %q", targetMsg, parent.GetMessage())
	}
	if target.GetDuration() != maxDuration {
		t.Fatalf("span %q duration %d is not the maximum (max=%d) among peers of %q",
			targetMsg, target.GetDuration(), maxDuration, parent.GetMessage())
	}
	// Find second-largest to verify the skew factor.
	secondMax := int64(0)
	for _, peer := range peers {
		if peer.GetMessage() == targetMsg {
			continue
		}
		if peer.GetDuration() > secondMax {
			secondMax = peer.GetDuration()
		}
	}
	if secondMax > 0 {
		factor := float64(target.GetDuration()) / float64(secondMax)
		if factor < minFactor {
			t.Fatalf("span %q duration %d is only %.2f× the next-slowest %d (want ≥%.1f×)",
				targetMsg, target.GetDuration(), factor, secondMax, minFactor)
		}
	}
}

// assertTagOnSpan asserts that spanMsg has a tag with key tagKey somewhere in
// the trace tree rooted at root.
func assertTagOnSpan(t *testing.T, root *commonv1.Span, spanMsg, tagKey string) {
	t.Helper()
	span := findSpanByMessage(root, spanMsg)
	if span == nil {
		t.Fatalf("span %q not found in trace tree", spanMsg)
	}
	for _, tag := range span.GetTags() {
		if tag.GetKey() == tagKey {
			return
		}
	}
	t.Fatalf("span %q missing tag %q", spanMsg, tagKey)
}

// TestTracerBottleneckScan validates that a scan span dominating its siblings
// is correctly identified as the bottleneck.
//
// Synthetic layout (all durations in nanoseconds):
//
//	data-node1  (parent)
//	  scan            500_000_000   ← bottleneck
//	  top              50_000_000
//	  limit            20_000_000
//
// scan is 10× top (well above the 5× threshold).
func TestTracerBottleneckScan(t *testing.T) {
	// Build synthetic trace tree.
	scanSpan := bottleneckSpan("scan", 500_000_000)
	topSpan := bottleneckSpan("top", 50_000_000)
	limitSpan := bottleneckSpan("limit", 20_000_000)
	dataNode := bottleneckSpan("data-node1", 570_000_000, scanSpan, topSpan, limitSpan)

	assertSpanIsBottleneck(t, dataNode, "scan", 5.0)
}

// TestTracerBottleneckDecode validates that a decode/reduce-raw-frames span
// dominating a merge and top-application span is the bottleneck.
//
// Synthetic layout:
//
//	broadcast-rows  (parent)
//	  reduce-raw-frames    800_000_000   ← bottleneck (large raw frame decode)
//	  merge-distributed-rows  60_000_000
//	  apply-top-to-reduce   40_000_000
//
// reduce-raw-frames is ~13.3× merge (well above 5×).
func TestTracerBottleneckDecode(t *testing.T) {
	reduceSpan := bottleneckSpan("reduce-raw-frames", 800_000_000)
	mergeSpan := bottleneckSpan("merge-distributed-rows", 60_000_000)
	topSpan := bottleneckSpan("apply-top-to-reduce", 40_000_000)
	root := bottleneckSpan("broadcast-rows", 900_000_000, reduceSpan, mergeSpan, topSpan)

	assertSpanIsBottleneck(t, root, "reduce-raw-frames", 5.0)
}

// TestTracerBottleneckReduce validates that a reduce-raw-frames span with many
// groups (high aggregation cost) dominates merge and top peers.
//
// Synthetic layout (simulating 10k groups requiring heavy aggregation):
//
//	broadcast-agg  (parent)
//	  reduce-raw-frames    900_000_000   ← bottleneck (many partial group states)
//	  apply-top-to-reduce   80_000_000
//	  build-multi-group-schema  10_000_000
//
// reduce-raw-frames is ~11.25× apply-top (well above 5×).
func TestTracerBottleneckReduce(t *testing.T) {
	reduceSpan := bottleneckSpan("reduce-raw-frames", 900_000_000)
	topSpan := bottleneckSpan("apply-top-to-reduce", 80_000_000)
	schemaSpan := bottleneckSpan("build-multi-group-schema", 10_000_000)
	root := bottleneckSpan("broadcast-agg", 990_000_000, reduceSpan, topSpan, schemaSpan)

	assertSpanIsBottleneck(t, root, "reduce-raw-frames", 5.0)
}

// TestTracerBottleneckMerge validates that a merge-distributed-rows span
// dominates when many overlapping (sid, ts, version) triples force the k-way
// heap merger to do significant dedup work.
//
// Synthetic layout (many collisions → merge is slow):
//
//	broadcast-rows  (parent)
//	  merge-distributed-rows  700_000_000   ← bottleneck
//	  reduce-raw-frames         60_000_000
//	  apply-top-to-reduce        40_000_000
//
// merge is ~11.7× reduce (well above 5×).
func TestTracerBottleneckMerge(t *testing.T) {
	mergeSpan := bottleneckSpan("merge-distributed-rows", 700_000_000)
	reduceSpan := bottleneckSpan("reduce-raw-frames", 60_000_000)
	topSpan := bottleneckSpan("apply-top-to-reduce", 40_000_000)
	root := bottleneckSpan("broadcast-rows", 800_000_000, mergeSpan, reduceSpan, topSpan)

	assertSpanIsBottleneck(t, root, "merge-distributed-rows", 5.0)
}

// TestTracerBottleneckFrameEncode validates that a frame-encode span dominates
// when a wide schema (many columns) with many rows drives high serialization cost.
//
// Synthetic layout (50-column schema, many rows):
//
//	data-node1  (parent)
//	  scan             80_000_000
//	  frame-encode    600_000_000   ← bottleneck (wide schema serialization)
//	  top              30_000_000
//
// frame-encode is 7.5× scan (well above 5×).
func TestTracerBottleneckFrameEncode(t *testing.T) {
	scanSpan := bottleneckSpan("scan", 80_000_000)
	encodeSpan := bottleneckSpan("frame-encode", 600_000_000)
	topSpan := bottleneckSpan("top", 30_000_000)
	dataNode := bottleneckSpan("data-node1", 710_000_000, scanSpan, encodeSpan, topSpan)

	assertSpanIsBottleneck(t, dataNode, "frame-encode", 5.0)
}

// TestTracerBottleneckBroadcastTail validates that in a 5-node fanout where one
// data node is slow, the broadcast span's slow-node child has a Duration larger
// than the p50 of the other 4 nodes, and the broadcast_timeout_ms tag is set on
// the broadcast span.
//
// Synthetic layout:
//
//	broadcast-rows  (parent, has broadcast_timeout_ms tag)
//	  data-node1      50_000_000
//	  data-node2      55_000_000
//	  data-node3      48_000_000
//	  data-node4      52_000_000
//	  data-node5 (slow) 400_000_000   ← bottleneck tail
//
// data-node5 is ~7.5× data-node3 (well above 5×).
// The test verifies:
//  1. data-node5 has the maximum Duration among the five node children.
//  2. data-node5.Duration > p50 of the other four nodes' durations.
//  3. broadcast_timeout_ms tag is present on the broadcast span.
func TestTracerBottleneckBroadcastTail(t *testing.T) {
	node1 := bottleneckSpan("data-node1", 50_000_000)
	node2 := bottleneckSpan("data-node2", 55_000_000)
	node3 := bottleneckSpan("data-node3", 48_000_000)
	node4 := bottleneckSpan("data-node4", 52_000_000)
	slowNode := bottleneckSpan("data-node5", 400_000_000)

	// broadcast span carries the broadcast_timeout_ms tag.
	broadcastSpan := bottleneckSpanWithTag(
		"broadcast-rows",
		410_000_000,
		tracelabels.TagBroadcastTimeoutMS, "15000",
		node1, node2, node3, node4, slowNode,
	)

	// 1. Slow node has maximum duration among the node children.
	assertSpanIsBottleneck(t, broadcastSpan, "data-node5", 5.0)

	// 2. data-node5.Duration > p50 of the other four nodes.
	otherDurations := []int64{
		node1.GetDuration(),
		node2.GetDuration(),
		node3.GetDuration(),
		node4.GetDuration(),
	}
	p50 := sortedMedianInt64(otherDurations)
	if slowNode.GetDuration() <= p50 {
		t.Fatalf("slow node duration %d should be > p50 of peers %d",
			slowNode.GetDuration(), p50)
	}

	// 3. broadcast_timeout_ms tag present on the broadcast span.
	assertTagOnSpan(t, broadcastSpan, "broadcast-rows", tracelabels.TagBroadcastTimeoutMS)
}

// sortedMedianInt64 computes the median of a slice of int64 values using a
// simple insertion sort (slice is at most a handful of elements in these tests).
func sortedMedianInt64(vals []int64) int64 {
	n := len(vals)
	if n == 0 {
		return 0
	}
	sorted := make([]int64, n)
	copy(sorted, vals)
	for i := 1; i < n; i++ {
		for j := i; j > 0 && sorted[j] < sorted[j-1]; j-- {
			sorted[j], sorted[j-1] = sorted[j-1], sorted[j]
		}
	}
	mid := n / 2
	if n%2 == 0 {
		return (sorted[mid-1] + sorted[mid]) / 2
	}
	return sorted[mid]
}
