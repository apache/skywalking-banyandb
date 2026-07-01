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

//go:build trace_pipeline

package pipeline_test

import (
	"context"
	"fmt"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	tracepipeline "github.com/apache/skywalking-banyandb/test/cases/tracepipeline"
	tracepipelinedata "github.com/apache/skywalking-banyandb/test/cases/tracepipeline/data"
)

// dynSeedOffset places dynamic-spec timestamps well in the past so
// isMergeHot (grace=0) returns false at merge time.
const dynSeedOffset = -2 * time.Hour

// queryByTraceID queries the "filter" trace for a single trace_id over a window
// covering [baseTime+dynSeedOffset-1m, now+1s] (millisecond-truncated).
func queryByTraceID(innerGm gomega.Gomega, conn *grpc.ClientConn, traceID string, baseTime time.Time) *tracev1.QueryResponse {
	windowStart := baseTime.Add(dynSeedOffset - time.Minute).Truncate(time.Millisecond)
	windowEnd := time.Now().Add(time.Second).Truncate(time.Millisecond)

	req := &tracev1.QueryRequest{
		Groups: []string{tracepipeline.PipelineGroup},
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
	innerGm.Expect(queryErr).NotTo(gomega.HaveOccurred())
	return resp
}

// assertTraceAbsent asserts the named trace_id is absent (dropped by sampler).
func assertTraceAbsent(conn *grpc.ClientConn, traceID string, baseTime time.Time) {
	gomega.Eventually(func(innerGm gomega.Gomega) {
		resp := queryByTraceID(innerGm, conn, traceID, baseTime)
		innerGm.Expect(resp.GetTraces()).To(gomega.BeEmpty(),
			"trace %q should be absent (filtered by sampler)", traceID)
	}, flags.EventuallyTimeout, 500*time.Millisecond).Should(gomega.Succeed())
}

// assertTracePresent asserts the named trace_id is present (kept / not dropped).
func assertTracePresent(conn *grpc.ClientConn, traceID string, baseTime time.Time) {
	gomega.Eventually(func(innerGm gomega.Gomega) {
		resp := queryByTraceID(innerGm, conn, traceID, baseTime)
		innerGm.Expect(resp.GetTraces()).NotTo(gomega.BeEmpty(),
			"trace %q should be present (not filtered)", traceID)
	}, flags.EventuallyTimeout, 500*time.Millisecond).Should(gomega.Succeed())
}

// writeTwoPartMerge writes two batches to trigger a filtering merge
// (max-merge-parts=2, grace=0).  It writes rowA as part-1, asserts rowA is
// visible (one part, no merge yet), then writes rowB as part-2 to trigger
// the merge.  Both rows share the same base time offset so isMergeHot is
// false.
func writeTwoPartMerge(
	conn *grpc.ClientConn,
	rowA tracepipelinedata.TraceRow, rowB tracepipelinedata.TraceRow,
	baseTime time.Time,
) {
	// Part 1: rowA at baseTime.
	tracepipelinedata.WriteBatchEntries(conn, tracepipeline.PipelineGroup,
		baseTime, time.Millisecond, []tracepipelinedata.TraceRow{rowA})

	// Assert rowA is visible before any merge (proves server received it).
	traceIDA := rowA.Tags[0].GetStr().GetValue()
	gomega.Eventually(func(innerGm gomega.Gomega) {
		resp := queryByTraceID(innerGm, conn, traceIDA, baseTime)
		innerGm.Expect(resp.GetTraces()).NotTo(gomega.BeEmpty(),
			"part-1 trace %q must be visible before merge", traceIDA)
	}, flags.EventuallyTimeout, 500*time.Millisecond).Should(gomega.Succeed())

	// Part 2: rowB at baseTime+10ms — triggers the merge.
	tracepipelinedata.WriteBatchEntries(conn, tracepipeline.PipelineGroup,
		baseTime.Add(10*time.Millisecond), time.Millisecond, []tracepipelinedata.TraceRow{rowB})
}

// makeDynRow is a convenience builder for TraceRow entries in dynamic specs.
func makeDynRow(traceID string, durationMs int64, status string) tracepipelinedata.TraceRow {
	return tracepipelinedata.TraceRow{
		Span: "span-" + traceID,
		Tags: []*modelv1.TagValue{
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: traceID}}},
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "span-" + traceID}}},
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "svc-dyn"}}},
			{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: durationMs}}},
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: status}}},
		},
	}
}

// Dynamic sampler registration lifecycle specs (US-008).
//
// These specs run Ordered so each step builds on the previous state.
// The base config (thresholdMs=500) is activated in common.go's BeforeSuite
// via RegisterSamplerRuntime; the RegisterMergeFilterTable Describe in
// common.go verifies the base drop/keep assertions.
//
// Steps here:
//  2. Update (variant, thresholdMs=200): assert drop-set changes.
//  3. Remove: assert previously-dropped traces are retained (Correction-C AC).
//  4. InvalidConfig: assert fail-open — node alive, traces retained.
//  5. Restart: assert config re-applied from schema store on reboot.
var _ = ginkgo.Describe("Dynamic sampler registration (standalone)", ginkgo.Ordered, func() {
	// dynBase anchors all dynamic-spec timestamps in the past (grace=0 → isMergeHot=false).
	var dynBase time.Time

	ginkgo.BeforeAll(func() {
		dynBase = now
	})

	// --- Step 2: Update (variant thresholdMs=200) ---
	//
	// Sampler decision: drop if dur < thresholdMs AND status == "success".
	//   base (500): dur=300 success → DROP (300 < 500).
	//   variant (200): dur=300 success → KEEP (300 >= 200).
	// So dur=300 success is the pivot trace that changes verdict between configs.
	//   dur=100 success → dropped under both.
	ginkgo.Describe("Update: variant threshold (thresholdMs=200)", ginkgo.Ordered, func() {
		const (
			pivotID    = "dyn-upd-pivot-300ms"     // dropped under base, kept under variant
			bothDropID = "dyn-upd-both-drop-100ms" // dropped under both configs
		)

		ginkgo.BeforeAll(func() {
			ctx := context.Background()

			// Switch to variant config (thresholdMs=200) via UpdateSamplerRuntime.
			upCtx, upCancel := context.WithTimeout(ctx, 30*time.Second)
			defer upCancel()
			tracepipeline.UpdateSamplerRuntime(upCtx, connection,
				tracepipeline.PipelineGroup,
				tracepipeline.NewVariantPipelineConfig(svrSoPath, tracepipeline.DefaultMergeGrace))

			// Write pivot trace (dur=300 success) + keep companion → two-part merge.
			// Under variant (thresholdMs=200): 300 >= 200 → KEPT.
			writeTwoPartMerge(connection,
				makeDynRow(pivotID, 300, "success"),
				makeDynRow("dyn-upd-keep-companion1", 900, "success"),
				dynBase.Add(dynSeedOffset))

			// Write both-drop trace (dur=100 success) + keep companion → two-part merge.
			// Under variant (thresholdMs=200): 100 < 200 → DROPPED.
			writeTwoPartMerge(connection,
				makeDynRow(bothDropID, 100, "success"),
				makeDynRow("dyn-upd-keep-companion2", 800, "error"),
				dynBase.Add(dynSeedOffset-time.Second))
		})

		ginkgo.It("dur=300 success: KEPT under variant (threshold=200)", func() {
			// Variant keeps 300ms traces (300 >= 200); base would have dropped them.
			assertTracePresent(connection, pivotID, dynBase)
		})

		ginkgo.It("dur=100 success: still DROPPED under variant (100 < 200)", func() {
			assertTraceAbsent(connection, bothDropID, dynBase)
		})
	})

	// --- Step 3: Remove → stop dropping (Correction-C measurement AC) ---
	//
	// After RemoveSamplerRuntime the registry for the group is empty.
	// Traces written after removal that WOULD be dropped (dur < 200, success)
	// must be retained in the next merge.  With grace=0, STOP latency is at most
	// one merge pass — no nudge needed (Correction C).
	ginkgo.Describe("Remove: stop dropping within merge_grace+one cycle", ginkgo.Ordered, func() {
		const retainedID = "dyn-rem-retained-100ms"

		ginkgo.BeforeAll(func() {
			ctx := context.Background()

			// Remove the sampler — registry becomes empty for the group.
			rmCtx, rmCancel := context.WithTimeout(ctx, 30*time.Second)
			defer rmCancel()
			tracepipeline.RemoveSamplerRuntime(rmCtx, connection, tracepipeline.PipelineGroup)

			// Write a trace that WOULD be dropped by the variant (dur=100, success).
			// After removal it must be retained (fail-safe: no sampler → retain all).
			writeTwoPartMerge(connection,
				makeDynRow(retainedID, 100, "success"),
				makeDynRow("dyn-rem-keep-companion", 800, "error"),
				dynBase.Add(dynSeedOffset-2*time.Second))
		})

		ginkgo.It("previously-dropped shape (dur=100 success) is RETAINED after remove", func() {
			// Correction-C measurement AC: STOP latency ≤ merge_grace + one merge cycle.
			// With grace=0: after the first merge pass following removal, the trace must
			// be present.  We assert eventual presence (no nudge, no time bound beyond
			// EventuallyTimeout which exceeds the merge cycle).
			assertTracePresent(connection, retainedID, dynBase)
		})
	})

	// --- Step 4: Invalid config → fail-open, node alive, traces retained ---
	//
	// US-008 AC(c): an invalid config (missing .so) leaves prior set (empty after
	// Remove) active, retains all traces, node stays alive.
	ginkgo.Describe("InvalidConfig: missing .so → fail-open, node alive, traces retained", ginkgo.Ordered, func() {
		const invalidRetainedID = "dyn-invalid-retained-100ms"

		ginkgo.BeforeAll(func() {
			ctx := context.Background()

			// Register with a path that does not exist inside the trusted dir.
			// The data node must reject/fail-open: no .so at that path → load error.
			badCfg := tracepipeline.NewBasePipelineConfig(
				"nonexistent_sampler_dyn.so", // not staged in the trusted dir
				tracepipeline.DefaultMergeGrace,
			)
			badCtx, badCancel := context.WithTimeout(ctx, 30*time.Second)
			defer badCancel()
			tracepipeline.RegisterSamplerRuntime(badCtx, connection,
				tracepipeline.PipelineGroup, badCfg)

			// Write a trace that the base sampler would drop (dur=100, success).
			// Because load failed (fail-open: previous set = empty), no sampler
			// is active → trace retained.
			writeTwoPartMerge(connection,
				makeDynRow(invalidRetainedID, 100, "success"),
				makeDynRow("dyn-invalid-keep-companion", 800, "success"),
				dynBase.Add(dynSeedOffset-3*time.Second))
		})

		ginkgo.It("node is alive after invalid config", func() {
			// Prove the node is still serving by listing groups — a schema RPC that
			// does not require Order/TraceIDs like a trace query.
			c := databasev1.NewGroupRegistryServiceClient(connection)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			resp, listErr := c.List(ctx, &databasev1.GroupRegistryServiceListRequest{})
			gomega.Expect(listErr).NotTo(gomega.HaveOccurred(),
				"node must remain queryable (GroupRegistryService.List) after invalid config load")
			gomega.Expect(resp.GetGroup()).NotTo(gomega.BeEmpty(),
				"at least the pipeline group must be listed")
		})

		ginkgo.It("traces are retained (fail-open) when .so load fails", func() {
			assertTracePresent(connection, invalidRetainedID, dynBase)
		})
	})

	// --- Step 5: Restart → config re-applied from schema store ---
	//
	// US-008 AC(d): the node re-applies pipeline config from the schema store via
	// the initial KindGroup syncLoop reconcile on boot — no re-write needed.
	//
	// Design: a dedicated second server is booted on its own ports so the primary
	// server (used by all other specs) is not disturbed.  The restart server:
	//   1. Boots with no static config (dynamic-only).
	//   2. Receives a RegisterSamplerRuntime via UpdateGroup (schema store write).
	//   3. Is stopped then relaunched from the SAME data dir.
	//   4. Is asserted to drop traces without a second RegisterSamplerRuntime call.
	ginkgo.Describe("Restart: config re-applied from schema store", ginkgo.Ordered, func() {
		const (
			dropAfterRestartID = "dyn-restart-drop-100ms"
			keepAfterRestartID = "dyn-restart-keep-900ms"
		)

		var (
			restartConn    *grpc.ClientConn
			stopRestartSvr func()
		)

		ginkgo.BeforeAll(func() {
			// Allocate a fresh set of ports for the restart server so it does not
			// conflict with the primary server (still running on svrPorts).
			rPorts, portsErr := test.AllocateFreePorts(5)
			gomega.Expect(portsErr).NotTo(gomega.HaveOccurred())

			// Fresh data directory — schema is seeded below via PreloadSchemaViaProperty.
			rDataPath, _, dataErr := test.NewSpace()
			gomega.Expect(dataErr).NotTo(gomega.HaveOccurred())
			rLogDir, _, logErr := test.NewSpace()
			gomega.Expect(logErr).NotTo(gomega.HaveOccurred())

			// Build a cluster config for the restart server.
			rTmpDir, _, tmpErr := test.NewSpace()
			gomega.Expect(tmpErr).NotTo(gomega.HaveOccurred())
			rDFWriter := setup.NewDiscoveryFileWriter(rTmpDir)
			rConfig := setup.PropertyClusterConfig(rDFWriter)

			// Boot the restart server (first boot).
			// --schema-server-flush-timeout=500ms ensures schemas written via
			// PreloadSchemaViaProperty and RegisterSamplerRuntime are persisted to
			// disk within the flush interval so the restarted node can replay them.
			rGRPCAddr, _, rClose := setup.ExternalStandalone(
				rConfig,
				svrBinPath,
				rDataPath,
				rLogDir,
				rPorts,
				"--trace-pipeline-native-plugin-enabled=true",
				"--trace-pipeline-trusted-plugin-dir="+svrTrustedDir,
				"--trace-pipeline-merge-grace-default=0",
				"--trace-max-merge-parts=2",
				"--trace-flush-timeout=500ms",
				"--schema-server-flush-timeout=500ms",
			)
			stopRestartSvr = rClose

			// Load the pipeline group + trace schema onto the restart server.
			setup.PreloadSchemaViaProperty(rConfig, tracepipeline.PreloadSchema)
			waitForSchemaSync(rGRPCAddr)

			// Open a connection and register the base sampler config.
			rConn, connErr := grpchelper.Conn(rGRPCAddr, 10*time.Second,
				grpc.WithTransportCredentials(insecure.NewCredentials()))
			gomega.Expect(connErr).NotTo(gomega.HaveOccurred())

			regCtx, regCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer regCancel()
			tracepipeline.RegisterSamplerRuntime(regCtx, rConn,
				tracepipeline.PipelineGroup,
				tracepipeline.NewBasePipelineConfig(svrSoPath, tracepipeline.DefaultMergeGrace))

			// Wait for the trace schema to be visible via TraceRegistryService.Get —
			// this proves the schema has been written to the property store (not just
			// the in-memory watch cache).  Then wait an extra second so the 500ms
			// flush interval has time to persist the data to disk before we stop.
			waitForTraceSchema(rGRPCAddr)
			time.Sleep(time.Second)
			_ = rConn.Close()

			// Stop the first boot — simulates the restart.
			stopRestartSvr()

			// Relaunch from the same data dir using captured flags (no duplicate
			// AddSchemaServerAddr — BuildStandaloneFlags does not call it).
			rRelaunchFlags := append(
				setup.BuildStandaloneFlags(rConfig, rDataPath, rPorts),
				"--trace-pipeline-native-plugin-enabled=true",
				"--trace-pipeline-trusted-plugin-dir="+svrTrustedDir,
				"--trace-pipeline-merge-grace-default=0",
				"--trace-max-merge-parts=2",
				"--trace-flush-timeout=500ms",
				"--schema-server-flush-timeout=500ms",
			)
			rRestartLogPath := rLogDir + "/standalone-restart.log"
			newTeardown, launchErr := setup.ExternalCMD(svrBinPath, rRestartLogPath,
				append([]string{"standalone"}, rRelaunchFlags...)...)
			gomega.Expect(launchErr).NotTo(gomega.HaveOccurred())
			stopRestartSvr = newTeardown

			// Wait for restarted node health checks.
			rNewGRPCAddr := fmt.Sprintf("localhost:%d", rPorts[0])
			rNewHTTPAddr := fmt.Sprintf("localhost:%d", rPorts[1])
			gomega.Eventually(
				helpers.HealthCheck(rNewGRPCAddr, 10*time.Second, 10*time.Second,
					grpc.WithTransportCredentials(insecure.NewCredentials())),
				flags.EventuallyTimeout).Should(gomega.Succeed())
			gomega.Eventually(helpers.HTTPHealthCheck(rNewHTTPAddr, ""),
				flags.EventuallyTimeout).Should(gomega.Succeed())

			// Wait for schema re-sync (groups + trace) on the restarted node.
			waitForSchemaSync(rNewGRPCAddr)
			waitForTraceSchema(rNewGRPCAddr)

			// Connect to the restarted node.
			restartConn, connErr = grpchelper.Conn(rNewGRPCAddr, 10*time.Second,
				grpc.WithTransportCredentials(insecure.NewCredentials()))
			gomega.Expect(connErr).NotTo(gomega.HaveOccurred())

			// Write test traces — sampler must be active from the schema store alone.
			//   dropAfterRestartID: dur=100 success → DROP (100 < 500).
			//   keepAfterRestartID: dur=900 success → KEEP (900 ≥ 500).
			writeTwoPartMerge(restartConn,
				makeDynRow(dropAfterRestartID, 100, "success"),
				makeDynRow(keepAfterRestartID, 900, "success"),
				dynBase.Add(dynSeedOffset-4*time.Second))
		})

		ginkgo.AfterAll(func() {
			if restartConn != nil {
				_ = restartConn.Close()
			}
			if stopRestartSvr != nil {
				stopRestartSvr()
			}
		})

		ginkgo.It("drop-eligible trace is DROPPED after restart (sampler from store)", func() {
			assertTraceAbsent(restartConn, dropAfterRestartID, dynBase)
		})

		ginkgo.It("keep-eligible trace is PRESENT after restart", func() {
			assertTracePresent(restartConn, keepAfterRestartID, dynBase)
		})
	})
})
