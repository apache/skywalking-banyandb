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
	"runtime"
	"time"

	g "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
	grpclib "google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	tracepipelinedata "github.com/apache/skywalking-banyandb/test/cases/tracepipeline/data"
)

const (
	// PipelineGroup is the schema group used by the filter trace.
	// Exported so that integration suites (standalone/distributed) can reference
	// the group name when calling RegisterSamplerRuntime / UpdateSamplerRuntime.
	PipelineGroup = "test-trace-pipeline"

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
	tracepipelinedata.WriteBatch(SharedContext.Connection, batch1File, PipelineGroup, baseTime.Add(seedOffset), time.Millisecond)
}

// SeedBatch2 writes batch2.json as a single write stream (one mem-part).
// Batch 2 contains 3 keeps: t-keep-errfast (dur=50, status=error),
// t-keep-errslow (dur=900, status=error), t-keep-nostatus (dur=100, status=null/fail-open keep).
// Writing this second part triggers the filtering compaction (max-merge-parts=2).
// Timestamps are placed slightly after batch1 but still in the past so isMergeHot returns false.
func SeedBatch2(baseTime time.Time) {
	tracepipelinedata.WriteBatch(SharedContext.Connection, batch2File, PipelineGroup, baseTime.Add(seedOffset).Add(time.Second), time.Millisecond)
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
			soakDropRow(dropID1, 100),
			soakKeepRow(keepID1, 800, "success"),
		}
		// Batch B: one drop + one keep → second mem-part; triggers the filtering merge (max-merge-parts=2).
		batchB := []tracepipelinedata.TraceRow{
			soakDropRow(dropID2, 300),
			soakKeepRow(keepID2, 50, "error"),
		}

		// Each batch gets a slightly different base time so the two parts are distinct.
		iterBase := baseTime.Add(-time.Duration(iteration) * time.Millisecond)
		tracepipelinedata.WriteBatchEntries(SharedContext.Connection, PipelineGroup, iterBase, time.Millisecond, batchA)
		tracepipelinedata.WriteBatchEntries(SharedContext.Connection, PipelineGroup, iterBase.Add(10*time.Millisecond), time.Millisecond, batchB)

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
func soakDropRow(traceID string, durationMs int64) tracepipelinedata.TraceRow {
	return tracepipelinedata.TraceRow{
		Span: "span-" + traceID,
		Tags: []*modelv1.TagValue{
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: traceID}}},
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "span-" + traceID}}},
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "svc-" + traceID}}},
			{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: durationMs}}},
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "success"}}},
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
		Groups: []string{PipelineGroup},
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

// soakChurnInterval is how often the soak loop rotates the sampler config
// (Register → Update → Remove → Register …) during the dynamic soak.
const soakChurnInterval = 15

// soakPanicIteration is the iteration at which the soak injects the panicking
// sampler to verify engine fail-open resilience. Must be > 0 and < the
// total iteration count for any soak duration ≥ soakDefaultDuration.
const soakPanicIteration = 5

// soakHeapGrowthLimit is the maximum allowed growth in HeapInuse bytes from the
// start of the dynamic soak to the end, as a fraction of the initial measurement.
// 3× is conservative — a leak would produce orders-of-magnitude growth.
const soakHeapGrowthLimit = 3.0

// RunSoakDynamic runs the US-010 soak: continuous writes + periodic
// Register/Update/Remove sampler churn + a mid-run panicking-sampler injection,
// with goroutine-leak, no-merge-stall, and bounded-heap gates.
//
// Gates verified (per the US-010 ACs):
//   - No merge stall: after every churn phase, a drop-eligible trace eventually
//     becomes absent, proving merges are still running.
//   - Node survives panic: after the panicking sampler is injected, the node
//     is still reachable (GroupRegistryService.List succeeds) and traces are
//     retained (fail-open — no sampler active during panic).
//   - Bounded heap: the test-driver HeapInuse does not grow beyond
//     soakHeapGrowthLimit× its initial value.
//   - No goroutine leak: verified by the suite's ReportAfterSuite (suite-level
//     gleak.Goroutines snapshot taken in BeforeSuite); NOT re-checked here to
//     avoid double-accounting.
//   - No close-balance gate (R4): plugins are immortal; Close() is never called
//     on registry mutation.
func RunSoakDynamic(conn *grpclib.ClientConn, group, soPath string, duration time.Duration) {
	deadline := time.Now().Add(duration)
	baseTime := time.Now().Add(-soakGracePad).Truncate(time.Millisecond)

	// Snapshot initial test-driver heap for the bounded-heap gate.
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)
	heapBefore := memBefore.HeapInuse
	g.GinkgoWriter.Printf("[soak-dyn] start: HeapInuse=%d bytes, duration=%s\n", heapBefore, duration)

	// Phase 0: ensure the base config (thresholdMs=500) is active before the loop.
	regCtx, regCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer regCancel()
	RegisterSamplerRuntime(regCtx, conn, group, NewBasePipelineConfig(soPath, DefaultMergeGrace))

	// churning tracks which config variant is currently active so the loop can
	// rotate: base(500) → variant(200) → remove → base(500) → …
	type configState int
	const (
		stateBase    configState = iota // base thresholdMs=500
		stateVariant                    // variant thresholdMs=200
		stateRemoved                    // sampler removed
	)
	state := stateBase
	panicInjected := false

	var iteration int
	for time.Now().Before(deadline) {
		iteration++
		iterBase := baseTime.Add(-time.Duration(iteration) * time.Millisecond)

		// --- Churn phase: rotate config BEFORE writing this iteration's data, so the
		// merge that processes those parts runs under the new config. This mirrors the
		// instant dynamic specs (change config, THEN write the parts whose verdict it
		// governs): writing first and churning after races the merge against async
		// config convergence and can leave a part merged-and-retained under the old
		// config — which is never re-filtered without a fresh merge trigger.
		if iteration%soakChurnInterval == 0 {
			churnCtx, churnCancel := context.WithTimeout(context.Background(), 30*time.Second)
			switch state {
			case stateBase:
				// Rotate to variant (thresholdMs=200).
				UpdateSamplerRuntime(churnCtx, conn, group,
					NewVariantPipelineConfig(soPath, DefaultMergeGrace))
				state = stateVariant
				g.GinkgoWriter.Printf("[soak-dyn] iteration %d: rotated to variant (thresholdMs=200)\n", iteration)
			case stateVariant:
				// Remove sampler.
				RemoveSamplerRuntime(churnCtx, conn, group)
				state = stateRemoved
				g.GinkgoWriter.Printf("[soak-dyn] iteration %d: sampler removed\n", iteration)
			case stateRemoved:
				// Re-register base config.
				RegisterSamplerRuntime(churnCtx, conn, group,
					NewBasePipelineConfig(soPath, DefaultMergeGrace))
				state = stateBase
				g.GinkgoWriter.Printf("[soak-dyn] iteration %d: sampler re-registered (base)\n", iteration)
			}
			churnCancel()
		}

		// --- Write phase: two batches to trigger a merge ---
		dropID := fmt.Sprintf("soak-dyn-drop-iter%d", iteration)
		keepID := fmt.Sprintf("soak-dyn-keep-iter%d", iteration)

		batchA := []tracepipelinedata.TraceRow{
			soakDropRow(dropID, 100),
			soakKeepRow(keepID, 800, "success"),
		}
		batchB := []tracepipelinedata.TraceRow{
			soakDropRow(fmt.Sprintf("soak-dyn-drop2-iter%d", iteration), 300),
			soakKeepRow(fmt.Sprintf("soak-dyn-keep2-iter%d", iteration), 50, "error"),
		}
		tracepipelinedata.WriteBatchEntries(conn, group, iterBase, time.Millisecond, batchA)
		tracepipelinedata.WriteBatchEntries(conn, group, iterBase.Add(10*time.Millisecond), time.Millisecond, batchB)

		// --- Panic injection: mid-run, once, before churn rotation ---
		if !panicInjected && iteration == soakPanicIteration {
			g.GinkgoWriter.Printf("[soak-dyn] iteration %d: injecting panicking sampler\n", iteration)
			panicCtx, panicCancel := context.WithTimeout(context.Background(), 30*time.Second)
			RegisterSamplerRuntime(panicCtx, conn, group,
				NewPanicPipelineConfig(soPath, DefaultMergeGrace))
			panicCancel()
			panicInjected = true

			// Write a trace that the base sampler would drop (dur=100 success) but
			// the panicking sampler must retain (fail-open on panic → no sampler →
			// retain all).
			panicBase := iterBase.Add(-50 * time.Millisecond)
			panicRetainID := fmt.Sprintf("soak-dyn-panic-retain-iter%d", iteration)
			panicKeepID := fmt.Sprintf("soak-dyn-panic-keep-iter%d", iteration)
			tracepipelinedata.WriteBatchEntries(conn, group, panicBase,
				time.Millisecond, []tracepipelinedata.TraceRow{
					soakDropRow(panicRetainID, 100),
					soakKeepRow(panicKeepID, 900, "success"),
				})
			tracepipelinedata.WriteBatchEntries(conn, group, panicBase.Add(10*time.Millisecond),
				time.Millisecond, []tracepipelinedata.TraceRow{
					soakKeepRow(fmt.Sprintf("soak-dyn-panic-companion-iter%d", iteration), 600, "error"),
					soakKeepRow(fmt.Sprintf("soak-dyn-panic-comp2-iter%d", iteration), 700, "error"),
				})

			// Assert node is still alive (fail-open: no crash from the panic).
			dbc := databasev1.NewGroupRegistryServiceClient(conn)
			aliveCtx, aliveCancel := context.WithTimeout(context.Background(), 10*time.Second)
			_, aliveErr := dbc.List(aliveCtx, &databasev1.GroupRegistryServiceListRequest{})
			aliveCancel()
			gm.Expect(aliveErr).NotTo(gm.HaveOccurred(),
				"node must remain reachable after panicking sampler injection (fail-open)")

			// Assert the panic-mode trace is retained (fail-open: sampler panics → retain all).
			assertSoakKeepPresent(panicRetainID, panicBase)
			assertSoakKeepPresent(panicKeepID, panicBase)

			// Restore the base config so subsequent churn iterations are meaningful.
			restoreCtx, restoreCancel := context.WithTimeout(context.Background(), 30*time.Second)
			RegisterSamplerRuntime(restoreCtx, conn, group,
				NewBasePipelineConfig(soPath, DefaultMergeGrace))
			restoreCancel()
			state = stateBase

			g.GinkgoWriter.Printf("[soak-dyn] iteration %d: panicking-sampler injection passed; base config restored\n", iteration)
			continue
		}

		// --- Verify phase: assert drop/keep verdicts match the active state ---
		// No-merge-stall gate: a drop-eligible trace eventually becomes absent,
		// proving the merge pipeline is running (not stalled by the panic or churn).
		switch state {
		case stateBase:
			// thresholdMs=500: dur=100 success → DROP; dur=800 success → KEEP.
			assertSoakDropAbsent(dropID, iterBase)
			assertSoakKeepPresent(keepID, iterBase)
		case stateVariant:
			// thresholdMs=200: dur=100 success → DROP; dur=800 success → KEEP.
			assertSoakDropAbsent(dropID, iterBase)
			assertSoakKeepPresent(keepID, iterBase)
		case stateRemoved:
			// No sampler: all traces retained (fail-safe).
			assertSoakKeepPresent(dropID, iterBase)
			assertSoakKeepPresent(keepID, iterBase)
		}

		if iteration%10 == 0 {
			g.GinkgoWriter.Printf("[soak-dyn] iteration %d complete, %.1fs remaining\n",
				iteration, time.Until(deadline).Seconds())
		}
	}

	// Bounded-heap gate: test-driver HeapInuse must not grow beyond soakHeapGrowthLimit×.
	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)
	heapAfter := memAfter.HeapInuse
	g.GinkgoWriter.Printf("[soak-dyn] end: HeapInuse=%d bytes after %d iterations\n", heapAfter, iteration)
	if heapBefore > 0 {
		growthFactor := float64(heapAfter) / float64(heapBefore)
		gm.Expect(growthFactor).To(gm.BeNumerically("<=", soakHeapGrowthLimit),
			"test-driver heap grew %.1fx (limit %.1fx): possible gRPC resource leak in soak loop",
			growthFactor, soakHeapGrowthLimit)
	}

	g.GinkgoWriter.Printf("[soak-dyn] completed %d iterations in %s\n", iteration, duration)
}

// RegisterSoakDynamic registers the US-010 dynamic soak spec under the given
// description. The spec is gated by TRACE_PIPELINE_SOAK and is Serial (it
// mutates the group's pipeline config and must not race with other specs).
//
// The soak performs:
//   - Continuous writes + merges with drop/keep verification.
//   - Periodic Register/Update/Remove churn (every soakChurnInterval iterations).
//   - Mid-run panicking-sampler injection (iteration soakPanicIteration): asserts
//     the node survives (fail-open), traces are retained, merges are not stalled.
//   - Bounded test-driver heap growth gate (soakHeapGrowthLimit×).
//   - No close-balance gate (R4): plugins are immortal.
//
// soPath is a provider resolved at spec-run time, not a bare string: the
// trusted-dir-relative .so path is only assigned by the suite's
// SynchronizedBeforeSuite, which runs AFTER this registration's arguments are
// evaluated at package-init time. Capturing the string here would freeze its
// empty zero value and fail validation (SamplerPlugin.Path must be non-empty).
func RegisterSoakDynamic(description string, soPath func() string) bool {
	return g.Describe(description, g.Serial, func() {
		g.It("soak-dynamic: write+churn+panic loop (US-010)", func() {
			if os.Getenv("TRACE_PIPELINE_SOAK") == "" {
				g.Skip("set TRACE_PIPELINE_SOAK=1 to run the dynamic soak (opt-in only)")
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
			g.GinkgoWriter.Printf("[soak-dyn] starting dynamic soak for %s\n", duration)
			RunSoakDynamic(SharedContext.Connection, PipelineGroup, soPath(), duration)
		})
	})
}
