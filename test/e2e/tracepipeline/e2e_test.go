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

//go:build trace_pipeline_e2e

// Package tracepipeline_e2e drives a real banyand-server process (launched by
// run.sh) through the trace pipeline native plugin lifecycle. It replaces the
// in-process integration suite (test/integration/standalone/pipeline) that
// forked the server via pkg/test/setup/external.go, which conflicted with the
// project's "integration tests run in a single Go process" principle.
//
// The bash orchestrator (run.sh) builds the .so + CGO banyand-server, starts
// the standalone, and exports the gRPC endpoint + the property-schema
// endpoint + the trusted-dir + the staged .so name + the http/metrics port +
// the data dir as env vars. This test drives the running process over gRPC
// and asserts the dynamic-sampler lifecycle end-to-end. The process-restart
// replay (the property only a process kill+relaunch can prove) is exercised
// by run.sh stopping the server and relaunching it from the same data dir
// before the second invocation.
//
// Phases (selected via E2E_PHASE):
//   - "lifecycle": Register → Update → Remove → InvalidConfig on a live
//                  server.
//   - "restart":   assert sampler_active_count > 0 after process restart
//                  with no further RegisterSamplerRuntime calls (the schema
//                  store must have replayed the pipeline on boot).
package tracepipeline_e2e

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
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
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema/property"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	tracepipeline "github.com/apache/skywalking-banyandb/test/cases/tracepipeline"
	tracepipelinedata "github.com/apache/skywalking-banyandb/test/cases/tracepipeline/data"
)

var (
	grpcAddr     string
	propertyAddr string
	httpPort     int
	trustedDir   string
	soName       string
	dataDir      string
	pluginSOPath string
	phase        string
)

func TestE2E(t *testing.T) {
	grpcAddr = os.Getenv("GRPC_ADDR")
	propertyAddr = os.Getenv("PROPERTY_ADDR")
	trustedDir = os.Getenv("TRUSTED_DIR")
	soName = os.Getenv("SO_NAME")
	dataDir = os.Getenv("DATA_DIR")
	pluginSOPath = os.Getenv("PLUGIN_SO_PATH")
	phase = os.Getenv("E2E_PHASE")
	httpPortStr := os.Getenv("HTTP_PORT")
	if httpPortStr != "" {
		var parseErr error
		httpPort, parseErr = strconv.Atoi(httpPortStr)
		gomega.Expect(parseErr).NotTo(gomega.HaveOccurred(), "HTTP_PORT=%q", httpPortStr)
	}

	if grpcAddr == "" || propertyAddr == "" || trustedDir == "" || soName == "" || phase == "" {
		t.Skip("E2E env not set; expected to be invoked by test/e2e/tracepipeline/run.sh")
	}

	gomega.RegisterTestingT(t)
	ginkgo.RunSpecs(t, "Trace Pipeline E2E", ginkgo.Label("e2e", "trace-pipeline"))
}

// dynSeedOffset places dynamic-spec timestamps well in the past so
// mergeMayContainMatureTrace (merge grace 1ns) returns true at merge time.
const dynSeedOffset = -2 * time.Hour

// makeDynRow builds a TraceRow for dynamic specs.
// Tags: trace_id, span_id, service_id, duration, status.
func makeDynRow(traceID string, durationMs int64, status string) tracepipelinedata.TraceRow {
	return tracepipelinedata.TraceRow{
		Span: "span-" + traceID,
		Tags: []*modelv1.TagValue{
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: traceID}}},
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "span-" + traceID}}},
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "svc-e2e"}}},
			{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: durationMs}}},
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: status}}},
		},
	}
}

// writeTwoPartMerge writes two batches to trigger a filtering merge
// (max-merge-parts=2, merge grace 1ns). rowA lands as part-1; once it is
// visible (one part, no merge), rowB is written as part-2 to trigger the
// merge.
func writeTwoPartMerge(conn *grpc.ClientConn, rowA, rowB tracepipelinedata.TraceRow, baseTime time.Time) {
	tracepipelinedata.WriteBatchEntries(conn, tracepipeline.PipelineGroup,
		baseTime, time.Millisecond, []tracepipelinedata.TraceRow{rowA})
	traceIDA := rowA.Tags[0].GetStr().GetValue()
	gomega.Eventually(func(innerGm gomega.Gomega) {
		_ = queryByTraceID(innerGm, conn, traceIDA, baseTime).GetTraces()
	}, 30*time.Second, 500*time.Millisecond).ShouldNot(gomega.BeEmpty(),
		"part-1 trace %q must be visible before merge", traceIDA)
	tracepipelinedata.WriteBatchEntries(conn, tracepipeline.PipelineGroup,
		baseTime.Add(10*time.Millisecond), time.Millisecond, []tracepipelinedata.TraceRow{rowB})
}

// queryByTraceID queries the "filter" trace for a single trace_id over a
// broad window. Millisecond-truncated timestamps per the validator.
func queryByTraceID(innerGm gomega.Gomega, conn *grpc.ClientConn, traceID string, baseTime time.Time) *tracev1.QueryResponse {
	windowStart := baseTime.Add(-time.Minute).Truncate(time.Millisecond)
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
	}, 30*time.Second, 500*time.Millisecond).Should(gomega.Succeed())
}

// assertTracePresent asserts the named trace_id is present (kept / not dropped).
func assertTracePresent(conn *grpc.ClientConn, traceID string, baseTime time.Time) {
	gomega.Eventually(func(innerGm gomega.Gomega) {
		resp := queryByTraceID(innerGm, conn, traceID, baseTime)
		innerGm.Expect(resp.GetTraces()).NotTo(gomega.BeEmpty(),
			"trace %q should be present (not filtered)", traceID)
	}, 30*time.Second, 500*time.Millisecond).Should(gomega.Succeed())
}

// dialGRPC opens a gRPC connection to the standalone server's public endpoint.
func dialGRPC() *grpc.ClientConn {
	conn, err := grpchelper.Conn(grpcAddr, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials()))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return conn
}

// waitForTraceSchema polls the property endpoint until the "filter" trace
// schema in the pipeline group is visible, proving the property→standalone
// schema sync has completed.
func waitForTraceSchema() {
	conn, err := grpchelper.Conn(propertyAddr, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials()))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer func() { _ = conn.Close() }()

	c := databasev1.NewTraceRegistryServiceClient(conn)
	gomega.Eventually(func(innerGm gomega.Gomega) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, getErr := c.Get(ctx, &databasev1.TraceRegistryServiceGetRequest{
			Metadata: &commonv1.Metadata{Name: "filter", Group: tracepipeline.PipelineGroup},
		})
		innerGm.Expect(getErr).NotTo(gomega.HaveOccurred(),
			"trace schema filter/%s not yet visible on the standalone", tracepipeline.PipelineGroup)
	}, 60*time.Second, 500*time.Millisecond).Should(gomega.Succeed())
}

// waitForServerReachable polls the gRPC endpoint until the standalone
// responds. Used after run.sh kills+relaunches the server to gate the
// "restart" phase specs.
func waitForServerReachable() {
	gomega.Eventually(func() bool {
		conn, err := net.DialTimeout("tcp", grpcAddr, 2*time.Second)
		if err != nil {
			return false
		}
		_ = conn.Close()
		return true
	}, 60*time.Second, time.Second).Should(gomega.BeTrue(),
		"standalone did not become reachable on %s after restart", grpcAddr)
}

// preloadSchemas uses the property-schema registry client to load the
// pipeline group + trace schema fixtures. tracepipeline.PreloadSchema is the
// same loader the deleted in-process integration suite used; the property
// client adapts the in-process metadata.SchemaRegistry interface to a remote
// schema server, so the fixtures stay the single source of truth.
//
// Only used in the lifecycle phase — the restart phase reads the schemas that
// the lifecycle phase wrote.
func preloadSchemas() {
	reg, regErr := property.NewSchemaRegistryClient(&property.ClientConfig{
		GRPCTimeout: 10 * time.Second,
		NodeRegistry: &e2eNodeRegistry{addr: propertyAddr},
	})
	gomega.Expect(regErr).NotTo(gomega.HaveOccurred())
	defer func() { _ = reg.Close() }()

	gomega.Eventually(func() int { return len(reg.ActiveNodeNames()) }).
		WithTimeout(30*time.Second).WithPolling(200*time.Millisecond).
		Should(gomega.Equal(1), "property schema server must become active")

	ctx := context.Background()
	gomega.Expect(tracepipeline.PreloadSchema(ctx, reg)).To(gomega.Succeed(),
		"tracepipeline.PreloadSchema must succeed via the property-schema client")
}

// samplerActiveCount scrapes the standalone's Prometheus metrics endpoint
// for sampler_active_count{group="<group>"}. Returns 0 when the series is
// absent or the metric cannot be scraped.
//
// Used by the restart phase to prove the schema store replayed the pipeline
// without a fresh RegisterSamplerRuntime call.
func samplerActiveCount(group string) float64 {
	if httpPort == 0 {
		return 0
	}
	host, _, splitErr := net.SplitHostPort(grpcAddr)
	gomega.Expect(splitErr).NotTo(gomega.HaveOccurred(), "grpcAddr=%q", grpcAddr)
	url := fmt.Sprintf("http://%s:%d/metrics", host, httpPort)

	httpClient := &http.Client{Timeout: 5 * time.Second}
	resp, err := httpClient.Get(url) // #nosec G107 -- URL is composed from grpcAddr host + caller-supplied httpPort.
	if err != nil {
		return 0
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return 0
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0
	}
	prefix := fmt.Sprintf(`banyandb_trace_pipeline_sampler_active_count{group="%s"`, group)
	for _, line := range strings.Split(string(body), "\n") {
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		v, parseErr := strconv.ParseFloat(fields[len(fields)-1], 64)
		if parseErr != nil {
			continue
		}
		return v
	}
	return 0
}

// e2eNodeRegistry is a stub metadata.NodeRegistry pointing at the property
// schema server exposed by the standalone. property.NewSchemaRegistryClient
// only uses ListNode to discover active schema servers.
type e2eNodeRegistry struct {
	addr string
}

func (r *e2eNodeRegistry) ListNode(_ context.Context, role databasev1.Role) ([]*databasev1.Node, error) {
	return []*databasev1.Node{{
		Metadata:                  &commonv1.Metadata{Name: "standalone"},
		Roles:                     []databasev1.Role{databasev1.Role_ROLE_META, role},
		PropertySchemaGrpcAddress: r.addr,
	}}, nil
}

func (r *e2eNodeRegistry) RegisterNode(_ context.Context, _ *databasev1.Node, _ bool) error {
	return nil
}

func (r *e2eNodeRegistry) GetNode(_ context.Context, _ string) (*databasev1.Node, error) {
	return nil, nil
}

func (r *e2eNodeRegistry) UpdateNode(_ context.Context, _ *databasev1.Node) error { return nil }

var _ = ginkgo.Describe("Trace Pipeline E2E", func() {
	var conn *grpc.ClientConn
	var baseTime time.Time

	ginkgo.BeforeAll(func() {
		baseTime = time.Now()

		// Sanity: the .so file referenced by the schema must exist where
		// the orchestrator claims it lives. If run.sh set SO_NAME but the
		// file is missing, fail loud rather than silently drop every spec.
		_, statErr := os.Stat(pluginSOPath)
		gomega.Expect(statErr).NotTo(gomega.HaveOccurred(),
			"plugin .so not found at %s; check run.sh PLUGIN_SO_PATH", pluginSOPath)

		switch phase {
		case "lifecycle":
			preloadSchemas()
			waitForTraceSchema()
			conn = dialGRPC()
		case "restart":
			waitForServerReachable()
			waitForTraceSchema()
			conn = dialGRPC()
		default:
			ginkgo.Fail(fmt.Sprintf("unknown E2E_PHASE=%q (want lifecycle|restart)", phase))
		}
	})

	ginkgo.AfterAll(func() {
		if conn != nil {
			_ = conn.Close()
		}
	})

	ginkgo.When("E2E_PHASE=lifecycle", func() {
		ginkgo.It("registers, updates, removes, and fail-opens the sampler", func() {
			ctx := context.Background()

			// Step 1: Register the base sampler (thresholdMs=500). The
			// drop-eligible trace must be dropped; the keep trace must
			// survive.
			regCtx, regCancel := context.WithTimeout(ctx, 30*time.Second)
			defer regCancel()
			tracepipeline.RegisterSamplerRuntime(regCtx, conn,
				tracepipeline.PipelineGroup,
				tracepipeline.NewBasePipelineConfig(soName, tracepipeline.DefaultMergeGrace))

			dynBase := baseTime.Add(dynSeedOffset)
			writeTwoPartMerge(conn,
				makeDynRow("e2e-lc-drop-100ms", 100, "success"),
				makeDynRow("e2e-lc-keep-companion1", 900, "success"),
				dynBase)
			assertTraceAbsent(conn, "e2e-lc-drop-100ms", dynBase)

			// Step 2: Update to the variant (thresholdMs=200). Pivot
			// trace dur=300 success changes verdict.
			upCtx, upCancel := context.WithTimeout(ctx, 30*time.Second)
			defer upCancel()
			tracepipeline.UpdateSamplerRuntime(upCtx, conn,
				tracepipeline.PipelineGroup,
				tracepipeline.NewVariantPipelineConfig(soName, tracepipeline.DefaultMergeGrace))
			writeTwoPartMerge(conn,
				makeDynRow("e2e-lc-pivot-300ms", 300, "success"),
				makeDynRow("e2e-lc-keep-companion2", 900, "success"),
				dynBase.Add(-time.Second))
			assertTracePresent(conn, "e2e-lc-pivot-300ms", dynBase)
			writeTwoPartMerge(conn,
				makeDynRow("e2e-lc-both-drop-100ms", 100, "success"),
				makeDynRow("e2e-lc-keep-companion3", 800, "error"),
				dynBase.Add(-2*time.Second))
			assertTraceAbsent(conn, "e2e-lc-both-drop-100ms", dynBase)

			// Step 3: Remove the sampler. No sampler -> retain all.
			rmCtx, rmCancel := context.WithTimeout(ctx, 30*time.Second)
			defer rmCancel()
			tracepipeline.RemoveSamplerRuntime(rmCtx, conn, tracepipeline.PipelineGroup)
			writeTwoPartMerge(conn,
				makeDynRow("e2e-lc-retained-100ms", 100, "success"),
				makeDynRow("e2e-lc-keep-companion4", 800, "error"),
				dynBase.Add(-3*time.Second))
			assertTracePresent(conn, "e2e-lc-retained-100ms", dynBase)

			// Step 4: InvalidConfig — register with a path that doesn't
			// exist in the trusted dir. Standalone must fail-open.
			badCfg := tracepipeline.NewBasePipelineConfig(
				"nonexistent_sampler_e2e.so",
				tracepipeline.DefaultMergeGrace,
			)
			badCtx, badCancel := context.WithTimeout(ctx, 30*time.Second)
			defer badCancel()
			tracepipeline.RegisterSamplerRuntime(badCtx, conn,
				tracepipeline.PipelineGroup, badCfg)

			c := databasev1.NewGroupRegistryServiceClient(conn)
			lsCtx, lsCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer lsCancel()
			resp, listErr := c.List(lsCtx, &databasev1.GroupRegistryServiceListRequest{})
			gomega.Expect(listErr).NotTo(gomega.HaveOccurred(),
				"standalone must remain healthy after invalid config load")
			gomega.Expect(resp.GetGroup()).NotTo(gomega.BeEmpty(),
				"at least the pipeline group must be listed")

			writeTwoPartMerge(conn,
				makeDynRow("e2e-lc-invalid-retained-100ms", 100, "success"),
				makeDynRow("e2e-lc-keep-companion5", 800, "success"),
				dynBase.Add(-4*time.Second))
			assertTracePresent(conn, "e2e-lc-invalid-retained-100ms", dynBase)

			// Re-register the base config so the schema store has a valid
			// pipeline for the restart phase to replay. Without this, the
			// restart would test an empty pipeline (still useful, but
			// not what we want to prove).
			reg2Ctx, reg2Cancel := context.WithTimeout(ctx, 30*time.Second)
			defer reg2Cancel()
			tracepipeline.RegisterSamplerRuntime(reg2Ctx, conn,
				tracepipeline.PipelineGroup,
				tracepipeline.NewBasePipelineConfig(soName, tracepipeline.DefaultMergeGrace))
		})
	})

	ginkgo.When("E2E_PHASE=restart", func() {
		ginkgo.It("replays the pipeline config from the schema store after process restart", func() {
			// No further RegisterSamplerRuntime call. The schema store
			// should have replayed the pipeline we registered in the
			// lifecycle phase, so the drop-eligible trace must be dropped
			// again. sampler_active_count>0 is the cheaper pre-check.
			gomega.Eventually(func() float64 {
				return samplerActiveCount(tracepipeline.PipelineGroup)
			}, 30*time.Second, time.Second).Should(gomega.BeNumerically(">", 0),
				"sampler_active_count must be > 0 after restart (schema store replay)")

			dynBase := baseTime.Add(dynSeedOffset - 5*time.Second)
			writeTwoPartMerge(conn,
				makeDynRow("e2e-restart-drop-100ms", 100, "success"),
				makeDynRow("e2e-restart-keep-companion", 900, "success"),
				dynBase)
			assertTraceAbsent(conn, "e2e-restart-drop-100ms", dynBase)
		})
	})
})

// Compile-time check: e2eNodeRegistry implements schema.Node.
var _ schema.Node = (*e2eNodeRegistry)(nil)