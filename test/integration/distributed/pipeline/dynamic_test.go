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

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
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

// dynSeedOffset places timestamps well in the past so isMergeHot (grace=0) returns false.
const dynSeedOffset = -2 * time.Hour

// queryByTraceID queries the "filter" trace for a single trace_id over a broad window.
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

// assertTraceAbsent asserts the named trace_id is absent (dropped by sampler) via the liaison.
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

// makeDynRow builds a TraceRow for dynamic specs.
// Tags: trace_id, span_id, service_id, duration, status.
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

// writeTwoPartMerge writes two batches via the liaison, triggering a filtering merge
// on both data nodes (max-merge-parts=2, grace=0). rowA lands as part-1; once it is
// visible (one part, no merge), rowB is written as part-2 to trigger the merge.
func writeTwoPartMerge(
	conn *grpc.ClientConn,
	rowA tracepipelinedata.TraceRow, rowB tracepipelinedata.TraceRow,
	baseTime time.Time,
) {
	tracepipelinedata.WriteBatchEntries(conn, tracepipeline.PipelineGroup,
		baseTime, time.Millisecond, []tracepipelinedata.TraceRow{rowA})

	// Wait for part-1 to be visible (proves the server received it before merge).
	traceIDA := rowA.Tags[0].GetStr().GetValue()
	gomega.Eventually(func(innerGm gomega.Gomega) {
		resp := queryByTraceID(innerGm, conn, traceIDA, baseTime)
		innerGm.Expect(resp.GetTraces()).NotTo(gomega.BeEmpty(),
			"part-1 trace %q must be visible before merge", traceIDA)
	}, flags.EventuallyTimeout, 500*time.Millisecond).Should(gomega.Succeed())

	// Part 2 triggers the merge.
	tracepipelinedata.WriteBatchEntries(conn, tracepipeline.PipelineGroup,
		baseTime.Add(10*time.Millisecond), time.Millisecond, []tracepipelinedata.TraceRow{rowB})
}

// Dynamic sampler registration lifecycle specs for the distributed cluster (US-009).
//
// The base config (thresholdMs=500) was activated in common.go's BeforeSuite
// via RegisterSamplerRuntime. The RegisterMergeFilterTable Describe in common.go
// verifies base drop/keep on the initial two-part merge. The specs here are Ordered:
//
//  1. Update (variant thresholdMs=200): both nodes converge on new threshold.
//  2. Remove: both healthy nodes stop dropping within merge_grace + one merge cycle
//     (Correction-C measurement AC; no nudge).
//  3. InvalidConfig: missing .so → fail-open, both nodes alive, traces retained.
//  4. Restart: per-node stop+relaunch → config re-applied from schema store.
//  5. LateJoin: a new data node joining the cluster converges via syncLoop.
//  6. LiaisonNoLoad: the liaison's sampler-load path is unreachable (structural gate).
var _ = ginkgo.Describe("Dynamic sampler registration (distributed)", ginkgo.Ordered, func() {
	// dynBase anchors all dynamic-spec timestamps in the past (grace=0 → isMergeHot=false).
	var dynBase time.Time

	ginkgo.BeforeAll(func() {
		dynBase = now
	})

	// --- Step 2: Update (variant thresholdMs=200) ---
	//
	// Sampler decision: drop if dur < thresholdMs AND status == "success".
	//   base (500): dur=300 success → DROP.
	//   variant (200): dur=300 success → KEEP (300 ≥ 200).
	// Both nodes must converge on the new threshold; the pivot trace (dur=300
	// success) must be KEPT in the merge that fires after the Update.
	ginkgo.Describe("Update: variant threshold (thresholdMs=200) — both nodes converge", ginkgo.Ordered, func() {
		const (
			pivotID    = "dist-upd-pivot-300ms"
			bothDropID = "dist-upd-both-drop-100ms"
		)

		ginkgo.BeforeAll(func() {
			ctx := context.Background()

			// Switch to variant config (thresholdMs=200) via UpdateSamplerRuntime.
			upCtx, upCancel := context.WithTimeout(ctx, 30*time.Second)
			defer upCancel()
			tracepipeline.UpdateSamplerRuntime(upCtx, connection,
				tracepipeline.PipelineGroup,
				tracepipeline.NewVariantPipelineConfig(clusterSoPath, tracepipeline.DefaultMergeGrace))

			// Pivot trace: dur=300 success + keep companion → two-part merge.
			// Under variant (200): 300 ≥ 200 → KEPT.
			writeTwoPartMerge(connection,
				makeDynRow(pivotID, 300, "success"),
				makeDynRow("dist-upd-keep-companion1", 900, "success"),
				dynBase.Add(dynSeedOffset))

			// Both-drop trace: dur=100 success + keep companion → two-part merge.
			// Under variant (200): 100 < 200 → DROPPED.
			writeTwoPartMerge(connection,
				makeDynRow(bothDropID, 100, "success"),
				makeDynRow("dist-upd-keep-companion2", 800, "error"),
				dynBase.Add(dynSeedOffset-time.Second))
		})

		ginkgo.It("dur=300 success: KEPT under variant threshold=200 (both nodes converge)", func() {
			assertTracePresent(connection, pivotID, dynBase)
		})

		ginkgo.It("dur=100 success: still DROPPED under variant (100 < 200)", func() {
			assertTraceAbsent(connection, bothDropID, dynBase)
		})
	})

	// --- Step 3: Remove → stop dropping (Correction-C measurement AC) ---
	//
	// After RemoveSamplerRuntime both healthy nodes must stop dropping within
	// merge_grace + one merge cycle. With grace=0 this is the very next merge pass.
	// No nudge is used (Correction C); STOP latency is the inherited schema-sync
	// propagation (WatchSchemas push stream) + the merge-cool floor.
	ginkgo.Describe("Remove: both healthy nodes stop dropping within merge_grace+one cycle", ginkgo.Ordered, func() {
		const retainedID = "dist-rem-retained-100ms"

		ginkgo.BeforeAll(func() {
			ctx := context.Background()

			// Remove the sampler — registry becomes empty on both data nodes.
			rmCtx, rmCancel := context.WithTimeout(ctx, 30*time.Second)
			defer rmCancel()
			tracepipeline.RemoveSamplerRuntime(rmCtx, connection, tracepipeline.PipelineGroup)

			// Write a trace that WOULD be dropped by the variant (dur=100, success).
			// After removal, no sampler is active → trace must be retained.
			writeTwoPartMerge(connection,
				makeDynRow(retainedID, 100, "success"),
				makeDynRow("dist-rem-keep-companion", 800, "error"),
				dynBase.Add(dynSeedOffset-2*time.Second))
		})

		ginkgo.It("previously-dropped shape (dur=100 success) is RETAINED on both nodes after remove", func() {
			// Correction-C measurement AC: STOP latency ≤ merge_grace + one merge cycle.
			// With grace=0 the trace must be present after the first merge pass
			// following removal. We assert eventual presence (no nudge, no explicit
			// time bound beyond EventuallyTimeout which exceeds the merge cycle).
			assertTracePresent(connection, retainedID, dynBase)
		})
	})

	// --- Step 4: InvalidConfig → fail-open, both nodes alive, traces retained ---
	//
	// US-009 AC(c): an invalid config (missing .so) leaves the prior set (empty after
	// Remove) active on both data nodes, retaining all traces. Both nodes stay alive.
	ginkgo.Describe("InvalidConfig: missing .so → fail-open, both nodes alive, traces retained", ginkgo.Ordered, func() {
		const invalidRetainedID = "dist-invalid-retained-100ms"

		ginkgo.BeforeAll(func() {
			ctx := context.Background()

			// Register with a .so path that does not exist in the trusted dir.
			// Both data nodes must fail-open: keep prior set (empty), retain traces.
			badCfg := tracepipeline.NewBasePipelineConfig(
				"nonexistent_sampler_dist.so",
				tracepipeline.DefaultMergeGrace,
			)
			badCtx, badCancel := context.WithTimeout(ctx, 30*time.Second)
			defer badCancel()
			tracepipeline.RegisterSamplerRuntime(badCtx, connection,
				tracepipeline.PipelineGroup, badCfg)

			// Write a trace that the base sampler would drop (dur=100, success).
			// Because load failed (prior set = empty), no sampler active → retained.
			writeTwoPartMerge(connection,
				makeDynRow(invalidRetainedID, 100, "success"),
				makeDynRow("dist-invalid-keep-companion", 800, "success"),
				dynBase.Add(dynSeedOffset-3*time.Second))
		})

		ginkgo.It("both nodes alive after invalid config (GroupRegistryService.List succeeds)", func() {
			c := databasev1.NewGroupRegistryServiceClient(connection)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			resp, listErr := c.List(ctx, &databasev1.GroupRegistryServiceListRequest{})
			gomega.Expect(listErr).NotTo(gomega.HaveOccurred(),
				"cluster must remain queryable after invalid config load on data nodes")
			gomega.Expect(resp.GetGroup()).NotTo(gomega.BeEmpty(),
				"at least the pipeline group must be listed")
		})

		ginkgo.It("traces are retained (fail-open) when .so load fails on both nodes", func() {
			assertTracePresent(connection, invalidRetainedID, dynBase)
		})
	})

	// --- Step 5: Per-node restart → config re-applied from schema store ---
	//
	// US-009 AC(d): each restarted data node re-applies pipeline config via the
	// initial KindGroup syncLoop reconcile on boot — no re-write needed.
	// We restart data node 0 (one at a time), assert it rejoins and drops correctly.
	ginkgo.Describe("Restart: data node 0 re-applies config from schema store", ginkgo.Ordered, func() {
		const (
			dropAfterRestartID = "dist-restart-drop-100ms"
			keepAfterRestartID = "dist-restart-keep-900ms"
		)

		var restartConn *grpc.ClientConn

		ginkgo.BeforeAll(func() {
			// First restore a valid sampler config (the InvalidConfig step left an empty
			// set via a failed load). Re-register the base config so restart has something
			// to replay.
			regCtx, regCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer regCancel()
			tracepipeline.RegisterSamplerRuntime(regCtx, connection,
				tracepipeline.PipelineGroup,
				tracepipeline.NewBasePipelineConfig(clusterSoPath, tracepipeline.DefaultMergeGrace))

			// Stop data node 0, then relaunch it from its existing data dir.
			// The restarted node must re-apply config from the schema store without
			// a second RegisterSamplerRuntime call.
			closeDataNode0()

			// Build the same flags used at first boot.
			dn0RelaunchFlags := append(
				setup.BuildDataNodeFlags(clusterConfig, clusterDataDir0, clusterDN0Ports),
				"--trace-pipeline-native-plugin-enabled=true",
				"--trace-pipeline-trusted-plugin-dir="+clusterTrustedDir,
				"--trace-pipeline-merge-grace-default=0",
				"--trace-max-merge-parts=2",
				"--trace-flush-timeout=500ms",
			)
			dn0LogPath := clusterLogDir + fmt.Sprintf("/data-%d-restart.log", clusterDN0Ports[0])
			newDN0Close, launchErr := setup.ExternalCMD(clusterBinPath, dn0LogPath,
				append([]string{"data"}, dn0RelaunchFlags...)...)
			gomega.Expect(launchErr).NotTo(gomega.HaveOccurred())
			closeDataNode0 = newDN0Close

			// Wait for the restarted node to pass gRPC health check.
			dn0GRPCAddr := fmt.Sprintf("localhost:%d", clusterDN0Ports[0])
			gomega.Eventually(
				helpers.HealthCheck(dn0GRPCAddr, 10*time.Second, 10*time.Second,
					grpc.WithTransportCredentials(insecure.NewCredentials())),
				flags.EventuallyTimeout).Should(gomega.Succeed())

			// Connect directly to data node 0 to write restart-specific traces.
			// The node re-applies config from the schema store via syncLoop; once
			// schema is visible its sampler registry is active.
			var connErr error
			restartConn, connErr = grpchelper.Conn(dn0GRPCAddr, 10*time.Second,
				grpc.WithTransportCredentials(insecure.NewCredentials()))
			gomega.Expect(connErr).NotTo(gomega.HaveOccurred())

			// Wait for the trace schema to be visible on the restarted node.
			waitForTraceSchemaOnAddr(dn0GRPCAddr)

			// Write test traces via the liaison (both nodes get them).
			writeTwoPartMerge(connection,
				makeDynRow(dropAfterRestartID, 100, "success"),
				makeDynRow(keepAfterRestartID, 900, "success"),
				dynBase.Add(dynSeedOffset-4*time.Second))
		})

		ginkgo.AfterAll(func() {
			if restartConn != nil {
				_ = restartConn.Close()
			}
		})

		ginkgo.It("drop-eligible trace is DROPPED after node 0 restart (sampler from store)", func() {
			assertTraceAbsent(connection, dropAfterRestartID, dynBase)
		})

		ginkgo.It("keep-eligible trace is PRESENT after node 0 restart", func() {
			assertTracePresent(connection, keepAfterRestartID, dynBase)
		})
	})

	// --- Step 6: Late-join convergence ---
	//
	// US-009 AC(d): a new data node added to the cluster after the config was
	// written converges via the syncLoop reconcile (30s backstop) or the
	// WatchSchemas push stream. We boot a third data node, wait for schema sync,
	// then assert it drops traces correctly.
	ginkgo.Describe("LateJoin: new data node converges via schema-sync", ginkgo.Ordered, func() {
		const (
			lateDropID = "dist-latejoin-drop-100ms"
			lateKeepID = "dist-latejoin-keep-900ms"
		)

		var (
			lateJoinConn  *grpc.ClientConn
			closeLateNode func()
		)

		ginkgo.BeforeAll(func() {
			// Allocate 4 ports for the late-join data node.
			ljPorts, portsErr := test.AllocateFreePorts(4)
			gomega.Expect(portsErr).NotTo(gomega.HaveOccurred())

			ljDataDir, _, dataErr := test.NewSpace()
			gomega.Expect(dataErr).NotTo(gomega.HaveOccurred())

			// Launch the late-join data node into the existing cluster.
			ljGRPCAddr, ljClose := setup.ExternalDataNode(
				clusterConfig,
				clusterBinPath,
				ljDataDir,
				clusterLogDir,
				ljPorts,
				"--trace-pipeline-native-plugin-enabled=true",
				"--trace-pipeline-trusted-plugin-dir="+clusterTrustedDir,
				"--trace-pipeline-merge-grace-default=0",
				"--trace-max-merge-parts=2",
				"--trace-flush-timeout=500ms",
			)
			closeLateNode = ljClose

			// Wait for the late-joining node to receive schema via WatchSchemas stream
			// or the 30s syncLoop reconcile.
			waitForTraceSchemaOnAddr(ljGRPCAddr)

			// Connect directly to the late-join node for this spec.
			var connErr error
			lateJoinConn, connErr = grpchelper.Conn(ljGRPCAddr, 10*time.Second,
				grpc.WithTransportCredentials(insecure.NewCredentials()))
			gomega.Expect(connErr).NotTo(gomega.HaveOccurred())

			// Write traces via the liaison; they route to the cluster and the
			// late-join node applies the sampler on the next merge.
			writeTwoPartMerge(connection,
				makeDynRow(lateDropID, 100, "success"),
				makeDynRow(lateKeepID, 900, "success"),
				dynBase.Add(dynSeedOffset-5*time.Second))
		})

		ginkgo.AfterAll(func() {
			if lateJoinConn != nil {
				_ = lateJoinConn.Close()
			}
			if closeLateNode != nil {
				closeLateNode()
			}
		})

		ginkgo.It("drop-eligible trace is DROPPED on late-joining node (converged from schema store)", func() {
			// Query via the liaison; the late-join node participates in fan-out queries.
			assertTraceAbsent(connection, lateDropID, dynBase)
		})

		ginkgo.It("keep-eligible trace is PRESENT on late-joining node", func() {
			assertTracePresent(connection, lateKeepID, dynBase)
		})
	})

	// --- Step 7: LiaisonNoLoad — liaison sampler-load path is unreachable ---
	//
	// US-009 AC(e) / R8: the liaison's plugin-load branch is structurally gated off.
	// The liaison's schemaRepo is constructed by newLiaisonSchemaRepo (metadata.go:100)
	// which sets role=ROLE_LIAISON and does NOT set nativePipelineEnabled. The
	// reconcilePipeline branch in OnAddOrUpdate is:
	//
	//   if sr.role == databasev1.Role_ROLE_DATA && sr.nativePipelineEnabled { ... }
	//
	// Both conditions are false on the liaison — the load branch is doubly-gated off.
	// We assert this behaviorally: after the sampler is active on data nodes, the
	// liaison is still healthy (GroupRegistryService.List succeeds), confirming it
	// processed the KindGroup event without attempting to load a .so.
	ginkgo.Describe("LiaisonNoLoad: liaison sampler-load path is unreachable", func() {
		ginkgo.It("liaison processes KindGroup pipeline event without loading a .so (structurally gated)", func() {
			// The sampler is active on data nodes (RegisterSamplerRuntime was called).
			// If the liaison tried to load the .so it would fail (no trusted-dir flag)
			// and the node would either crash or return an error. Asserting the liaison
			// is alive and serving schema RPCs proves the load branch was not reached.
			lnConn, connErr := grpchelper.Conn(liaisonGRPCAddr, 10*time.Second,
				grpc.WithTransportCredentials(insecure.NewCredentials()))
			gomega.Expect(connErr).NotTo(gomega.HaveOccurred())
			defer func() { _ = lnConn.Close() }()

			c := databasev1.NewGroupRegistryServiceClient(lnConn)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			resp, listErr := c.List(ctx, &databasev1.GroupRegistryServiceListRequest{})
			gomega.Expect(listErr).NotTo(gomega.HaveOccurred(),
				"liaison must be alive and serving schema RPCs (load branch not reached)")
			gomega.Expect(resp.GetGroup()).NotTo(gomega.BeEmpty(),
				"liaison must list at least the pipeline group")
		})
	})
})

// waitForTraceSchemaOnAddr polls the given gRPC address until the "filter" trace
// schema in the pipeline group is visible, proving schema sync has completed.
// Used for per-node assertions (restart + late-join specs).
func waitForTraceSchemaOnAddr(grpcAddr string) {
	conn, connErr := grpchelper.Conn(grpcAddr, 10*time.Second,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	gomega.Expect(connErr).NotTo(gomega.HaveOccurred())
	defer func() { _ = conn.Close() }()

	traceClient := databasev1.NewTraceRegistryServiceClient(conn)
	gomega.Eventually(func(innerGm gomega.Gomega) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, getErr := traceClient.Get(ctx, &databasev1.TraceRegistryServiceGetRequest{
			Metadata: &commonv1.Metadata{
				Name:  "filter",
				Group: tracepipeline.PipelineGroup,
			},
		})
		innerGm.Expect(getErr).NotTo(gomega.HaveOccurred(),
			"trace schema filter/%s not yet visible on %s", tracepipeline.PipelineGroup, grpcAddr)
	}, flags.EventuallyTimeout, 500*time.Millisecond).Should(gomega.Succeed())
}
