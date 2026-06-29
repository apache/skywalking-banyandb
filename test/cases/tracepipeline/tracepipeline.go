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

// Package tracepipeline_test contains integration test cases for the trace pipeline filter.
package tracepipeline_test

import (
	"context"
	"fmt"
	"os"
	"time"

	g "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
	grpclib "google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	tracepipelinedata "github.com/apache/skywalking-banyandb/test/cases/tracepipeline/data"
)

const (
	// pipelineGroup is the schema group used by the filter trace.
	pipelineGroup = "test-trace-pipeline"

	// batch1File is the first batch fixture: 4 traces (2 drops + 2 keeps) → one mem-part.
	batch1File = "batch1.json"

	// batch2File is the second batch fixture: 3 keeps → second mem-part, triggers the merge.
	batch2File = "batch2.json"
)

var (
	// SharedContext is the parallel execution context shared between test cases.
	SharedContext helpers.SharedContext

	verify = func(innerGm gm.Gomega, args helpers.Args) {
		tracepipelinedata.VerifyFn(innerGm, SharedContext, args)
	}
)

const (
	// seedOffset places both batches 2 hours in the past relative to BaseTime.
	// This ensures isMergeHot (graceNs=0) returns false at merge time, so the
	// sampler filter is applied. All queries use the same offset + duration window.
	seedOffset = -2 * time.Hour

	// queryOffset and queryDuration define the query window covering the seed data.
	queryOffset   = -2 * time.Hour
	queryDuration = 2 * time.Hour
)

// SeedBatch1 writes batch1.json as a single write stream (one mem-part).
// Batch 1 contains 4 traces (2 drops + 2 keeps): t-drop-1 (dur=100, status=success),
// t-drop-2 (dur=499, status=success), t-keep-boundary (dur=500, status=success),
// t-keep-highlat (dur=800, status=success). The 4/3 split with batch2 keeps the two
// parts near-equal so the real merge-balance gate fires (no merge-policy tuning needed).
// Timestamps are placed seedOffset before baseTime so isMergeHot returns false.
func SeedBatch1(baseTime time.Time) {
	tracepipelinedata.WriteBatch(SharedContext.Connection, batch1File, pipelineGroup, baseTime.Add(seedOffset), time.Millisecond)
}

// SeedBatch2 writes batch2.json as a single write stream (one mem-part).
// Batch 2 contains 3 keeps: t-keep-errfast (dur=50, status=error),
// t-keep-errslow (dur=900, status=error), t-keep-nostatus (dur=100, status=null/fail-open keep).
// Writing this second part triggers the filtering compaction (max-merge-parts=2).
// Timestamps are placed slightly after batch1 but still in the past so isMergeHot returns false.
func SeedBatch2(baseTime time.Time) {
	tracepipelinedata.WriteBatch(SharedContext.Connection, batch2File, pipelineGroup, baseTime.Add(seedOffset).Add(time.Second), time.Millisecond)
}

// AssertDropCandidatesVisible queries t-drop-1 and t-drop-2 and asserts BOTH are visible.
// This is the Phase-0 check: with only one part written, no merge has happened yet,
// so the drops must be present (proving the server received them before filtering).
func AssertDropCandidatesVisible(_ time.Time) {
	args1 := helpers.Args{Input: "t_drop_1", Offset: queryOffset, Duration: queryDuration}
	args2 := helpers.Args{Input: "t_drop_2", Offset: queryOffset, Duration: queryDuration}
	gm.Eventually(func(innerGm gm.Gomega) {
		verify(innerGm, args1)
	}, flags.EventuallyTimeout).Should(gm.Succeed(), "t-drop-1 must be visible before merge (Phase-0)")
	gm.Eventually(func(innerGm gm.Gomega) {
		verify(innerGm, args2)
	}, flags.EventuallyTimeout).Should(gm.Succeed(), "t-drop-2 must be visible before merge (Phase-0)")
}

// mergeFilterEntries lists all 7 test cases for the merge-filter table.
// The 5 kept traces have want files; the 2 dropped traces use WantEmpty.
var mergeFilterEntries = []any{
	// Drops: duration < 500 AND status == "success" → expect empty after merge.
	g.Entry("drop: duration=100 status=success", helpers.Args{Input: "t_drop_1", Offset: queryOffset, Duration: queryDuration, WantEmpty: true}),
	g.Entry("drop: duration=499 status=success", helpers.Args{Input: "t_drop_2", Offset: queryOffset, Duration: queryDuration, WantEmpty: true}),
	// Keeps: boundary, high-latency, error-fast (all in batch 1).
	g.Entry("keep: duration=500 status=success (boundary)", helpers.Args{Input: "t_keep_boundary", Offset: queryOffset, Duration: queryDuration}),
	g.Entry("keep: duration=800 status=success (high-latency)", helpers.Args{Input: "t_keep_highlat", Offset: queryOffset, Duration: queryDuration}),
	g.Entry("keep: duration=50 status=error (fast error)", helpers.Args{Input: "t_keep_errfast", Offset: queryOffset, Duration: queryDuration}),
	// Keeps: slow error, missing status (both in batch 2).
	g.Entry("keep: duration=900 status=error (slow error)", helpers.Args{Input: "t_keep_errslow", Offset: queryOffset, Duration: queryDuration}),
	g.Entry("keep: duration=100 status=null (fail-open)", helpers.Args{Input: "t_keep_nostatus", Offset: queryOffset, Duration: queryDuration}),
}

// RegisterMergeFilterTable registers a Ginkgo DescribeTable for the merge-filter cases
// under the given description string. It does NOT auto-register at init time; the
// caller (runner) must invoke this after seeding data and triggering a merge.
func RegisterMergeFilterTable(description string) bool {
	return g.DescribeTable(description, append([]any{func(args helpers.Args) {
		gm.Eventually(func(innerGm gm.Gomega) {
			verify(innerGm, args)
		}, flags.EventuallyTimeout).Should(gm.Succeed())
	}}, mergeFilterEntries...)...)
}

// soakDefaultDuration is the default soak duration when TRACE_PIPELINE_SOAK_DURATION is unset.
const soakDefaultDuration = 3 * time.Minute

// soakGracePad is the minimum age for soak timestamps relative to now.
// Writes are placed this far in the past so isMergeHot returns false for any
// configured --trace-pipeline-merge-grace-default up to this value.
// The instant suite uses grace=0; the soak validates the grace path by writing
// timestamps older than 30 s, which is well beyond the server's default 30 s grace.
const soakGracePad = 2 * time.Minute

// RunSoak runs a bounded write→merge→verify soak loop for the given duration.
// It is intended to be called from a Ginkgo spec gated by TRACE_PIPELINE_SOAK;
// if that env var is not set the caller should ginkgo.Skip before invoking.
//
// Each iteration writes two batches with DISTINCT trace_ids (suffixed by the iteration
// counter) using timestamps older than soakGracePad so isMergeHot is false regardless
// of the server's configured grace value. After each pair, the loop asserts the
// drop candidates become absent (filtered by the sampler) and the keeps remain visible.
//
// Design notes:
//   - Correctness relies only on LIVE-server queries; no shutdown-time flush is assumed.
//   - The iteration count is tracked and logged periodically so progress is visible.
//   - TRACE_PIPELINE_SOAK_DURATION overrides the default duration (default: 3 m).
func RunSoak(duration time.Duration) {
	deadline := time.Now().Add(duration)
	// baseTime anchors all soak timestamps well in the past so isMergeHot is false
	// for any grace value ≤ soakGracePad (the instant suite uses grace=0; the soak
	// validates the non-zero grace path by using timestamps older than 2 min).
	baseTime := time.Now().Add(-soakGracePad).Truncate(time.Millisecond)

	var iteration int
	for time.Now().Before(deadline) {
		iteration++
		// Build unique trace_ids for this iteration to avoid cross-iteration interference.
		dropID1 := fmt.Sprintf("soak-drop-1-iter%d", iteration)
		dropID2 := fmt.Sprintf("soak-drop-2-iter%d", iteration)
		keepID1 := fmt.Sprintf("soak-keep-1-iter%d", iteration)
		keepID2 := fmt.Sprintf("soak-keep-2-iter%d", iteration)

		// Batch A: one drop + one keep → first mem-part.
		batchA := []tracepipelinedata.TraceRow{
			soakDropRow(dropID1, 100, "success"),
			soakKeepRow(keepID1, 800, "success"),
		}
		// Batch B: one drop + one keep → second mem-part; triggers the filtering merge (max-merge-parts=2).
		batchB := []tracepipelinedata.TraceRow{
			soakDropRow(dropID2, 300, "success"),
			soakKeepRow(keepID2, 50, "error"),
		}

		// Each batch gets a slightly different base time so the two parts are distinct.
		iterBase := baseTime.Add(-time.Duration(iteration) * time.Millisecond)
		tracepipelinedata.WriteBatchEntries(SharedContext.Connection, pipelineGroup, iterBase, time.Millisecond, batchA)
		tracepipelinedata.WriteBatchEntries(SharedContext.Connection, pipelineGroup, iterBase.Add(10*time.Millisecond), time.Millisecond, batchB)

		// Assert that the drop candidates for this iteration become absent after the merge.
		assertSoakDropAbsent(dropID1, iterBase)
		assertSoakDropAbsent(dropID2, iterBase)

		// Assert that the keeps for this iteration remain visible.
		assertSoakKeepPresent(keepID1, iterBase)
		assertSoakKeepPresent(keepID2, iterBase)

		if iteration%10 == 0 {
			g.GinkgoWriter.Printf("[soak] iteration %d complete, %.1fs remaining\n",
				iteration, time.Until(deadline).Seconds())
		}
	}
	g.GinkgoWriter.Printf("[soak] completed %d iterations in %s\n", iteration, duration)
}

// soakDropRow builds a TraceRow that the sampler will DROP (duration < 500, status == success).
func soakDropRow(traceID string, durationMs int64, status string) tracepipelinedata.TraceRow {
	return tracepipelinedata.TraceRow{
		Span: "span-" + traceID,
		Tags: []*modelv1.TagValue{
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: traceID}}},
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "span-" + traceID}}},
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "svc-" + traceID}}},
			{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: durationMs}}},
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: status}}},
		},
	}
}

// soakKeepRow builds a TraceRow that the sampler will KEEP.
func soakKeepRow(traceID string, durationMs int64, status string) tracepipelinedata.TraceRow {
	return tracepipelinedata.TraceRow{
		Span: "span-" + traceID,
		Tags: []*modelv1.TagValue{
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: traceID}}},
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "span-" + traceID}}},
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "svc-" + traceID}}},
			{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: durationMs}}},
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: status}}},
		},
	}
}

// assertSoakDropAbsent queries by trace_id and asserts the trace is absent (filtered by sampler).
func assertSoakDropAbsent(traceID string, baseTime time.Time) {
	conn := SharedContext.Connection
	gm.Eventually(func(innerGm gm.Gomega) {
		resp := querySoakByTraceID(innerGm, conn, traceID, baseTime)
		innerGm.Expect(resp.GetTraces()).To(gm.BeEmpty(),
			"soak drop %q must be absent after merge (sampler should have filtered it)", traceID)
	}, flags.EventuallyTimeout, 500*time.Millisecond).Should(gm.Succeed())
}

// assertSoakKeepPresent queries by trace_id and asserts the trace is present (sampler kept it).
func assertSoakKeepPresent(traceID string, baseTime time.Time) {
	conn := SharedContext.Connection
	gm.Eventually(func(innerGm gm.Gomega) {
		resp := querySoakByTraceID(innerGm, conn, traceID, baseTime)
		innerGm.Expect(resp.GetTraces()).NotTo(gm.BeEmpty(),
			"soak keep %q must remain visible after merge", traceID)
	}, flags.EventuallyTimeout, 500*time.Millisecond).Should(gm.Succeed())
}

// querySoakByTraceID queries the filter trace for a single trace_id over a broad window
// that covers the full soak history (baseTime-soakGracePad to now+1s).
// Both endpoints are millisecond-truncated (BanyanDB rejects sub-millisecond timestamps).
func querySoakByTraceID(innerGm gm.Gomega, conn *grpclib.ClientConn, traceID string, baseTime time.Time) *tracev1.QueryResponse {
	windowStart := baseTime.Add(-soakGracePad).Truncate(time.Millisecond)
	windowEnd := time.Now().Add(time.Second).Truncate(time.Millisecond)

	req := &tracev1.QueryRequest{
		Groups: []string{pipelineGroup},
		Name:   "filter",
		TimeRange: &modelv1.TimeRange{
			Begin: timestamppb.New(windowStart),
			End:   timestamppb.New(windowEnd),
		},
		Criteria: &modelv1.Criteria{
			Exp: &modelv1.Criteria_Condition{
				Condition: &modelv1.Condition{
					Name: "trace_id",
					Op:   modelv1.Condition_BINARY_OP_EQ,
					Value: &modelv1.TagValue{
						Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: traceID}},
					},
				},
			},
		},
		Limit: 10,
	}

	c := tracev1.NewTraceServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, queryErr := c.Query(ctx, req)
	innerGm.Expect(queryErr).NotTo(gm.HaveOccurred())
	return resp
}

// RegisterSoak registers a soak spec under the given description, gated by TRACE_PIPELINE_SOAK.
// Set TRACE_PIPELINE_SOAK=1 to enable; set TRACE_PIPELINE_SOAK_DURATION to override the
// default duration (default: 3 m, e.g. "20s", "5m").
//
// The soak reuses the standalone SharedContext (grace=0 on the instant server) and validates
// the non-zero grace path by writing timestamps older than soakGracePad (2 min), which ensures
// isMergeHot returns false for any grace ≤ 2 min including the server default of 30 s.
func RegisterSoak(description string) bool {
	return g.Describe(description, func() {
		g.It("soak: write→merge→verify loop", func() {
			if os.Getenv("TRACE_PIPELINE_SOAK") == "" {
				g.Skip("set TRACE_PIPELINE_SOAK=1 to run the soak (opt-in only)")
				return
			}
			duration := soakDefaultDuration
			if raw := os.Getenv("TRACE_PIPELINE_SOAK_DURATION"); raw != "" {
				parsed, parseErr := time.ParseDuration(raw)
				if parseErr != nil {
					g.Fail(fmt.Sprintf("invalid TRACE_PIPELINE_SOAK_DURATION %q: %v", raw, parseErr))
					return
				}
				duration = parsed
			}
			g.GinkgoWriter.Printf("[soak] starting soak for %s (set TRACE_PIPELINE_SOAK_DURATION to override)\n", duration)
			RunSoak(duration)
		})
	})
}
