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
	"slices"
	"sort"
	"strconv"
	"time"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/tracelabels"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure/frame"
)

// maxIndividualSubTraces is the cap on individually-attached per-node sub-traces
// before a data-summary aggregate span is emitted for the excess children.
// Set to 19 so the 20th slot is reserved for the summary span itself
// (tracer.maxChildSpans = 20).
const maxIndividualSubTraces = 19

// nodeInfo carries the per-node metadata extracted from a bus.Future response.
// It is populated by collectRawFrameResponses and consumed by applyFanoutCap.
type nodeInfo struct {
	// trace is the sub-trace returned by the data node (may be nil for raw []byte responses).
	trace *commonv1.Trace
	// latencyNS is the duration of the root span of the sub-trace (the "data-{nodeID}" span).
	latencyNS int64
	// rows is the row count reported by the data node's root span resp_count tag, if available.
	rows int64
	// bytes is the wire-byte count for this node's frame body.
	bytes int64
	// hasError reports whether the sub-trace carries an error flag or the future returned an error.
	hasError bool
}

// extractNodeLatency returns the duration of the first (root) span in trace, or 0 if unavailable.
func extractNodeLatency(trace *commonv1.Trace) int64 {
	if trace == nil || len(trace.GetSpans()) == 0 {
		return 0
	}
	return trace.GetSpans()[0].GetDuration()
}

// extractNodeTagInt64 reads the int64 value of tag key from the first span of trace.
// Returns 0 if not found or not parseable.
func extractNodeTagInt64(trace *commonv1.Trace, key string) int64 {
	if trace == nil || len(trace.GetSpans()) == 0 {
		return 0
	}
	for _, tag := range trace.GetSpans()[0].GetTags() {
		if tag.GetKey() == key {
			v, parseErr := strconv.ParseInt(tag.GetValue(), 10, 64)
			if parseErr != nil {
				return 0
			}
			return v
		}
	}
	return 0
}

// applyFanoutCap attaches per-node sub-traces to broadcastSpan using
// broadcastSpanCtx (the context returned by startTraceSpan for the broadcast
// span, so broadcastSpan is the current-span in that context).
//
// When len(nodes) ≤ maxIndividualSubTraces every sub-trace is attached
// individually via AddSubTrace — unchanged behavior.
//
// When len(nodes) > maxIndividualSubTraces:
//   - The first 19 individual slots are filled by priority order:
//     (1) error nodes, (2) zero-row nodes, (3) healthy nodes desc-by-latency.
//   - The remaining (N−19) nodes are summarized into one "data-summary" child
//     span emitted via tracer.StartSpan on broadcastSpanCtx so the tracer
//     wires it as a child of broadcastSpan automatically.
func applyFanoutCap(broadcastSpanCtx context.Context, broadcastSpan *query.Span, nodes []nodeInfo) {
	if broadcastSpan == nil {
		return
	}
	if len(nodes) <= maxIndividualSubTraces {
		for _, ni := range nodes {
			if ni.trace != nil {
				broadcastSpan.AddSubTrace(ni.trace)
			}
		}
		return
	}

	// Partition into three priority buckets.
	errorNodes := make([]nodeInfo, 0, len(nodes))
	zeroRowNodes := make([]nodeInfo, 0, len(nodes))
	healthyNodes := make([]nodeInfo, 0, len(nodes))
	for _, ni := range nodes {
		switch {
		case ni.hasError:
			errorNodes = append(errorNodes, ni)
		case ni.rows == 0:
			zeroRowNodes = append(zeroRowNodes, ni)
		default:
			healthyNodes = append(healthyNodes, ni)
		}
	}
	// Sort healthy nodes by descending latency so the slowest get individual slots.
	sort.SliceStable(healthyNodes, func(i, j int) bool {
		return healthyNodes[i].latencyNS > healthyNodes[j].latencyNS
	})

	// Build the prioritized ordering: error → zero-row → healthy(desc latency).
	ordered := make([]nodeInfo, 0, len(nodes))
	ordered = append(ordered, errorNodes...)
	ordered = append(ordered, zeroRowNodes...)
	ordered = append(ordered, healthyNodes...)

	// Attach the first 19 individually.
	for _, ni := range ordered[:maxIndividualSubTraces] {
		if ni.trace != nil {
			broadcastSpan.AddSubTrace(ni.trace)
		}
	}

	// Summarize the remaining nodes.
	summarized := ordered[maxIndividualSubTraces:]
	emitDataSummarySpan(broadcastSpanCtx, summarized)
}

// emitDataSummarySpan opens a "data-summary" child span via tracer.StartSpan on
// broadcastSpanCtx (where the broadcast span is the current span) so the tracer
// automatically wires it as a direct child of the broadcast span.
// It tags the span with aggregate statistics over the summarized node slice.
func emitDataSummarySpan(broadcastSpanCtx context.Context, summarized []nodeInfo) {
	if len(summarized) == 0 {
		return
	}
	tracer := query.GetTracer(broadcastSpanCtx)
	if tracer == nil {
		return
	}

	var errCount, zeroRowCount int64
	var totalRows, totalBytes int64
	latencies := make([]int64, 0, len(summarized))
	for _, ni := range summarized {
		if ni.hasError {
			errCount++
		}
		if ni.rows == 0 {
			zeroRowCount++
		}
		totalRows += ni.rows
		totalBytes += ni.bytes
		latencies = append(latencies, ni.latencyNS)
	}

	p50, p95, p99, nsMin, nsMax := percentilesInt64(latencies)

	summarySpan, _ := tracer.StartSpan(broadcastSpanCtx, "data-summary")
	summarySpan.Tagf(tracelabels.TagAggregatedDataNodeSpans, "%d", len(summarized))
	summarySpan.Tagf(tracelabels.TagNodesWithErrors, "%d", errCount)
	summarySpan.Tagf(tracelabels.TagNodesWithZeroRows, "%d", zeroRowCount)
	summarySpan.Tagf(tracelabels.TagTotalRowsAcrossNodes, "%d", totalRows)
	summarySpan.Tagf(tracelabels.TagTotalBytesAcrossNodes, "%d", totalBytes)
	summarySpan.Tagf(tracelabels.TagNodeLatencyNSP50, "%d", p50)
	summarySpan.Tagf(tracelabels.TagNodeLatencyNSP95, "%d", p95)
	summarySpan.Tagf(tracelabels.TagNodeLatencyNSP99, "%d", p99)
	summarySpan.Tagf(tracelabels.TagNodeLatencyNSMin, "%d", nsMin)
	summarySpan.Tagf(tracelabels.TagNodeLatencyNSMax, "%d", nsMax)
	summarySpan.Stop()
}

// emitDecodeFrameSummarySpan attaches a "decode-frame-summary" child span to
// the current span in parentSpanCtx when len(frameDecodeDurations) > maxIndividualSubTraces.
// The span records aggregate decode statistics derived from the per-frame durations.
//
// Design choice: the reduce/merge path does NOT emit per-frame `decode-frame[i]`
// spans — frame decoding lives inside ReduceRawFrames / mergeDistributedRows and
// the per-frame durations are captured via the probe pass in
// collectFrameDecodeDurations. Therefore the summary fully represents every
// frame: `TagFramesEmittedIndividually` is always 0 and `TagFramesSkipped`
// equals the total frame count. The p50/p99/max/total tags convey the same
// per-frame distribution that 19 individual spans would have provided, without
// the 19× tracer overhead. If a future iteration introduces real per-frame
// spans (e.g. via streamed decode), the two tags should be re-anchored to
// "individually emitted" vs "summarized only" semantics.
//
// When frameCount ≤ maxIndividualSubTraces no span is emitted (no behavior change).
func emitDecodeFrameSummarySpan(parentSpanCtx context.Context, frameDecodeDurations []time.Duration) {
	frameCount := len(frameDecodeDurations)
	if frameCount <= maxIndividualSubTraces {
		return
	}
	tracer := query.GetTracer(parentSpanCtx)
	if tracer == nil {
		return
	}

	latencies := make([]int64, frameCount)
	var totalNS int64
	for idx, d := range frameDecodeDurations {
		ns := d.Nanoseconds()
		latencies[idx] = ns
		totalNS += ns
	}

	p50, _, p99, _, nsMax := percentilesInt64(latencies)

	summarySpan, _ := tracer.StartSpan(parentSpanCtx, "decode-frame-summary")
	summarySpan.Tagf(tracelabels.TagFramesTotal, "%d", frameCount)
	summarySpan.Tagf(tracelabels.TagFramesEmittedIndividually, "%d", 0)
	summarySpan.Tagf(tracelabels.TagFramesSkipped, "%d", frameCount)
	summarySpan.Tagf(tracelabels.TagDecodeNSTotal, "%d", totalNS)
	summarySpan.Tagf(tracelabels.TagDecodeNSP50, "%d", p50)
	summarySpan.Tagf(tracelabels.TagDecodeNSP99, "%d", p99)
	summarySpan.Tagf(tracelabels.TagDecodeNSMax, "%d", nsMax)
	summarySpan.Stop()
}

// percentilesInt64 computes p50, p95, p99, min, and max over the supplied
// nanosecond latency slice. The input slice is sorted in place. Returns all
// zeros for an empty input.
func percentilesInt64(vals []int64) (p50, p95, p99, nsMin, nsMax int64) {
	n := len(vals)
	if n == 0 {
		return 0, 0, 0, 0, 0
	}
	slices.Sort(vals)
	nsMin = vals[0]
	nsMax = vals[n-1]
	p50 = vals[percentileIdx(n, 50)]
	p95 = vals[percentileIdx(n, 95)]
	p99 = vals[percentileIdx(n, 99)]
	return p50, p95, p99, nsMin, nsMax
}

// percentileIdx returns the 0-based index into a sorted slice of length n
// corresponding to the pth percentile (0–100). Uses nearest-rank rounding,
// clamped to [0, n−1].
func percentileIdx(n, p int) int {
	if n <= 1 {
		return 0
	}
	idx := (p*n+99)/100 - 1
	if idx < 0 {
		return 0
	}
	if idx >= n {
		return n - 1
	}
	return idx
}

// collectFrameDecodeDurations times a frame.Decode call for each non-empty
// body in frames and returns the per-frame durations. This is a lightweight
// probe decode: the real decode happens inside ReduceRawFrames /
// mergeDistributedRows; here we only measure the decode cost for tracing
// purposes and discard the decoded batches immediately.
// Empty bodies are skipped (consistent with ReduceRawFrames behavior).
func collectFrameDecodeDurations(frames [][]byte) []time.Duration {
	durations := make([]time.Duration, 0, len(frames))
	for _, body := range frames {
		if len(body) == 0 {
			continue
		}
		start := time.Now()
		_, _ = frame.Decode(body)
		durations = append(durations, time.Since(start))
	}
	return durations
}
