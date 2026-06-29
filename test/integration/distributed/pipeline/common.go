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
	"io"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/onsi/gomega/gleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	pipelinev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/pipeline/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/pool"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/gmatcher"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	test_measure "github.com/apache/skywalking-banyandb/pkg/test/measure"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	test_stream "github.com/apache/skywalking-banyandb/pkg/test/stream"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
	tracepipeline "github.com/apache/skywalking-banyandb/test/cases/tracepipeline"
)

var (
	connection *grpc.ClientConn
	goods      []gleak.Goroutine
	stopFunc   func()
	now        time.Time
)

var _ = ginkgo.SynchronizedBeforeSuite(func() []byte {
	goods = gleak.Goroutines()
	pool.EnableStackTracking(true)
	gomega.Expect(logger.Init(logger.Logging{
		Env:   "dev",
		Level: flags.LogLevel,
	})).To(gomega.Succeed())

	// Resolve the server binary — skip cleanly if not built.
	binPath, binErr := setup.ResolveBanyandBinary()
	if binErr != nil {
		ginkgo.Skip(fmt.Sprintf("banyand binary unavailable (%v); build with `make build-trace-pipeline-server`", binErr))
		return nil
	}

	// Resolve the .so plugin — skip cleanly if not built.
	pluginSrc := resolvePlugin()
	if pluginSrc == "" {
		ginkgo.Skip("latencystatussampler.so not found; build with `make build-trace-pipeline-plugin` or set BANYAND_TRACE_PLUGIN")
		return nil
	}

	// Shared trusted dir for both data nodes (same config + .so).
	trustedDir, _, trustedErr := test.NewSpace()
	gomega.Expect(trustedErr).NotTo(gomega.HaveOccurred())

	// Copy the .so into the trusted dir under its bare filename.
	soPath := filepath.Join(trustedDir, "latencystatussampler.so")
	gomega.Expect(copyFile(pluginSrc, soPath)).To(gomega.Succeed())

	// Write the TracePipelineConfig protojson file.
	configFile := filepath.Join(trustedDir, "pipeline.json")
	gomega.Expect(writePipelineConfig(configFile)).To(gomega.Succeed())

	// Discovery file writer and property cluster config.
	tmpDir, tmpDirCleanup, tmpErr := test.NewSpace()
	gomega.Expect(tmpErr).NotTo(gomega.HaveOccurred())
	dfWriter := setup.NewDiscoveryFileWriter(tmpDir)
	config := setup.PropertyClusterConfig(dfWriter)

	// Allocate ports: data node 0 needs [grpc, gossip, http, schema] (4 ports),
	// data node 1 needs [grpc, gossip, http, schema] (4 ports),
	// liaison needs [grpc, http, liaison-server] (3 ports).
	// Total: 11 ports. Both data nodes need a schema server port because the
	// schema server starts by default (port 17916) and the two data nodes would
	// collide if we don't override the port on dn1.
	ports, portsErr := test.AllocateFreePorts(11)
	gomega.Expect(portsErr).NotTo(gomega.HaveOccurred())

	dn0Ports := ports[0:4] // grpc, gossip, http, schema
	dn1Ports := ports[4:8] // grpc, gossip, http, schema
	lnPorts := ports[8:11] // grpc, http, liaison-server

	// Data dirs and log dirs.
	dataDir0, _, dataErr0 := test.NewSpace()
	gomega.Expect(dataErr0).NotTo(gomega.HaveOccurred())
	dataDir1, _, dataErr1 := test.NewSpace()
	gomega.Expect(dataErr1).NotTo(gomega.HaveOccurred())
	logDir, _, logErr := test.NewSpace()
	gomega.Expect(logErr).NotTo(gomega.HaveOccurred())
	liaisonDir, _, liaisonDirErr := test.NewSpace()
	gomega.Expect(liaisonDirErr).NotTo(gomega.HaveOccurred())

	// Start data node 0 with schema server (dn0Ports[3]) and pipeline plugin flags.
	ginkgo.By("Starting data node 0 with pipeline plugin")
	_, closeDataNode0 := setup.ExternalDataNode(
		config,
		binPath,
		dataDir0,
		logDir,
		dn0Ports,
		"--trace-pipeline-native-plugin-enabled=true",
		"--trace-pipeline-trusted-plugin-dir="+trustedDir,
		"--trace-pipeline-config="+configFile,
		"--trace-pipeline-merge-grace-default=0",
		"--trace-max-merge-parts=2",
		"--trace-flush-timeout=500ms",
	)

	// Start data node 1 (no schema server) with pipeline plugin flags.
	ginkgo.By("Starting data node 1 with pipeline plugin")
	_, closeDataNode1 := setup.ExternalDataNode(
		config,
		binPath,
		dataDir1,
		logDir,
		dn1Ports,
		"--trace-pipeline-native-plugin-enabled=true",
		"--trace-pipeline-trusted-plugin-dir="+trustedDir,
		"--trace-pipeline-config="+configFile,
		"--trace-pipeline-merge-grace-default=0",
		"--trace-max-merge-parts=2",
		"--trace-flush-timeout=500ms",
	)

	// Preload trace schemas into the property schema servers.
	// This MUST happen before the liaison starts so data nodes receive the schema.
	ginkgo.By("Preloading schemas via property")
	setup.PreloadSchemaViaProperty(config, test_stream.PreloadSchema, test_measure.PreloadSchema, tracepipeline.PreloadSchema)
	config.AddLoadedKinds(schema.KindStream, schema.KindMeasure, schema.KindTrace)

	// Start liaison node — NO plugin flags (liaison has no merger).
	ginkgo.By("Starting liaison node")
	liaisonGRPCAddr, closeLiaisonNode := setup.ExternalLiaisonNode(
		config,
		binPath,
		liaisonDir,
		logDir,
		lnPorts,
	)

	ns := timestamp.NowMilli().UnixNano()
	now = time.Unix(0, ns-ns%int64(time.Minute))

	stopFunc = func() {
		closeLiaisonNode()
		closeDataNode1()
		closeDataNode0()
		tmpDirCleanup()
	}

	// Wait until the cluster is fully ready: schemas visible, route tables populated
	// with active liaison + data nodes, and the filter trace schema queryable.
	// This mirrors the logic in setup.waitForActiveDataNodes (setup.go:857).
	ginkgo.By("Waiting for cluster to be ready")
	waitForClusterReady(liaisonGRPCAddr)

	return []byte(liaisonGRPCAddr)
}, func(address []byte) {
	var connErr error
	connection, connErr = grpchelper.Conn(string(address), 10*time.Second,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	gomega.Expect(connErr).NotTo(gomega.HaveOccurred())

	tracepipeline.SharedContext = helpers.SharedContext{
		Connection: connection,
		BaseTime:   now,
	}
})

var _ = ginkgo.SynchronizedAfterSuite(func() {
	if connection != nil {
		gomega.Expect(connection.Close()).To(gomega.Succeed())
	}
}, func() {})

var _ = ginkgo.ReportAfterSuite("Distributed Trace Pipeline Integration Suite", func(report ginkgo.Report) {
	if report.SuiteSucceeded {
		if stopFunc != nil {
			stopFunc()
		}
		gomega.Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
		gomega.Eventually(pool.AllRefsCount, flags.EventuallyTimeout).Should(gmatcher.HaveZeroRef())
	}
})

// The Ordered Describe block seeds data and registers the merge-filter table.
// CRITICAL distributed nuance: with 1-second sync-interval on the liaison,
// batch1 must be fully synced as its own part on the owning data node BEFORE
// batch2 is written. We wait 2 seconds between Phase-0 and SeedBatch2 so
// batch1 lands as a distinct file-part. If both batches arrive in the same
// sync window they coalesce into one part and the merge never fires.
var _ = ginkgo.Describe("In-Merge Filter via .so plugin (distributed)", ginkgo.Ordered, func() {
	ginkgo.BeforeAll(func() {
		// Phase-0: seed batch 1 (2 drops + 3 keeps → one mem-part on liaison),
		// then assert both drop candidates are visible before any merge.
		tracepipeline.SeedBatch1(now)
		tracepipeline.AssertDropCandidatesVisible(now)

		// Wait > 1 sync-interval so batch1 syncs to the data node as its own
		// file-part before batch2 arrives. 2 seconds comfortably exceeds the
		// 1s --trace-sync-interval already set by liaisonFlags in external.go.
		ginkgo.By("Waiting for batch1 to sync to data node as a distinct part")
		time.Sleep(2 * time.Second)

		// Seed batch 2 (2 keeps → second part, triggers filtering compaction).
		tracepipeline.SeedBatch2(now)
	})

	tracepipeline.RegisterMergeFilterTable("Distributed (.so plugin): In-Merge Filter")
})

// resolvePlugin returns the absolute path to latencystatussampler.so.
// It checks BANYAND_TRACE_PLUGIN first, then falls back to the default
// build output: <module-root>/build/bin/plugins/latencystatussampler.so.
// Returns "" when neither source yields an existing file.
func resolvePlugin() string {
	if env := os.Getenv("BANYAND_TRACE_PLUGIN"); env != "" {
		if _, statErr := os.Stat(env); statErr == nil {
			return env
		}
	}
	// runtime.Caller(0) gives the path to this source file at compile time.
	// test/integration/distributed/pipeline → 4 dirs up → module root.
	_, srcFile, _, ok := runtime.Caller(0)
	if !ok {
		return ""
	}
	moduleRoot := filepath.Join(filepath.Dir(srcFile), "..", "..", "..", "..")
	candidate := filepath.Join(moduleRoot, "build", "bin", "plugins", "latencystatussampler.so")
	abs, absErr := filepath.Abs(candidate)
	if absErr != nil {
		return ""
	}
	if _, statErr := os.Stat(abs); statErr == nil {
		return abs
	}
	return ""
}

// copyFile copies the file at src to dst (creates dst).
func copyFile(src, dst string) error {
	srcF, openErr := os.Open(src) //nolint:gosec
	if openErr != nil {
		return fmt.Errorf("open %q: %w", src, openErr)
	}
	defer func() { _ = srcF.Close() }()

	dstF, createErr := os.Create(dst) //nolint:gosec
	if createErr != nil {
		return fmt.Errorf("create %q: %w", dst, createErr)
	}
	defer func() { _ = dstF.Close() }()

	if _, copyErr := io.Copy(dstF, srcF); copyErr != nil {
		return fmt.Errorf("copy %q → %q: %w", src, dst, copyErr)
	}
	return nil
}

// writePipelineConfig marshals a TracePipelineConfig (protojson) to cfgPath.
// The plugin path uses only the bare filename; the server resolves it against
// the trusted dir passed via --trace-pipeline-trusted-plugin-dir.
func writePipelineConfig(cfgPath string) error {
	pluginConfig, structErr := structpb.NewStruct(map[string]interface{}{
		"thresholdMs":  float64(500),
		"successValue": "success",
	})
	if structErr != nil {
		return fmt.Errorf("build plugin config struct: %w", structErr)
	}

	cfg := &pipelinev1.TracePipelineConfig{
		Metadata: &commonv1.Metadata{
			Group: "test-trace-pipeline",
		},
		Enabled: true,
		EnabledEvents: []pipelinev1.PipelineEvent{
			pipelinev1.PipelineEvent_PIPELINE_EVENT_MERGE,
		},
		Plugins: []*pipelinev1.Plugin{
			{
				Name: "latencystatussampler",
				Kind: &pipelinev1.Plugin_Sampler{
					Sampler: &pipelinev1.SamplerPlugin{
						Path:       "latencystatussampler.so",
						Symbol:     "NewSampler",
						AbiVersion: 1,
						Config:     pluginConfig,
					},
				},
			},
		},
	}

	data, marshalErr := protojson.Marshal(cfg)
	if marshalErr != nil {
		return fmt.Errorf("marshal pipeline config: %w", marshalErr)
	}
	return os.WriteFile(cfgPath, data, 0o600)
}

// waitForClusterReady polls the liaison until the cluster is fully ready to accept
// writes. It mirrors setup.waitForActiveDataNodes (setup.go:857):
//  1. Groups visible in liaison schema registry.
//  2. tire1 (liaison) and tire2 (data nodes) route tables have active nodes.
//  3. The filter trace schema is queryable (guards WriteBatch's silent-return on schemaErr).
//  4. Waits 5 s for the routing table to fully settle before returning.
func waitForClusterReady(grpcAddr string) {
	conn, connErr := grpchelper.Conn(grpcAddr, 10*time.Second,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	gomega.Expect(connErr).NotTo(gomega.HaveOccurred())
	defer func() { _ = conn.Close() }()

	// Step 1: groups visible.
	groupClient := databasev1.NewGroupRegistryServiceClient(conn)
	gomega.Eventually(func(innerGm gomega.Gomega) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		resp, listErr := groupClient.List(ctx, &databasev1.GroupRegistryServiceListRequest{})
		innerGm.Expect(listErr).NotTo(gomega.HaveOccurred())
		innerGm.Expect(resp.GetGroup()).NotTo(gomega.BeEmpty(), "no groups visible on liaison yet")
	}, flags.EventuallyTimeout, 500*time.Millisecond).Should(gomega.Succeed())

	// Step 2: tire1 (liaison) and tire2 (data node) route tables populated.
	clusterClient := databasev1.NewClusterStateServiceClient(conn)
	gomega.Eventually(func(innerGm gomega.Gomega) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		state, stateErr := clusterClient.GetClusterState(ctx, &databasev1.GetClusterStateRequest{})
		innerGm.Expect(stateErr).NotTo(gomega.HaveOccurred())
		tire1Table := state.GetRouteTables()["tire1"]
		innerGm.Expect(tire1Table).NotTo(gomega.BeNil(), "tire1 route table not found")
		innerGm.Expect(tire1Table.GetActive()).NotTo(gomega.BeEmpty(), "no active liaison nodes in tire1")
		tire2Table := state.GetRouteTables()["tire2"]
		innerGm.Expect(tire2Table).NotTo(gomega.BeNil(), "tire2 route table not found")
		innerGm.Expect(tire2Table.GetActive()).NotTo(gomega.BeEmpty(), "no active data nodes in tire2")
	}, flags.EventuallyTimeout, 500*time.Millisecond).Should(gomega.Succeed())

	// Step 3: filter trace schema queryable.
	traceRegClient := databasev1.NewTraceRegistryServiceClient(conn)
	gomega.Eventually(func(innerGm gomega.Gomega) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		resp, getErr := traceRegClient.Get(ctx, &databasev1.TraceRegistryServiceGetRequest{
			Metadata: &commonv1.Metadata{
				Name:  "filter",
				Group: "test-trace-pipeline",
			},
		})
		innerGm.Expect(getErr).NotTo(gomega.HaveOccurred(), "filter trace not yet visible on liaison")
		innerGm.Expect(resp.GetTrace()).NotTo(gomega.BeNil(), "filter trace schema is nil")
	}, flags.EventuallyTimeout, 500*time.Millisecond).Should(gomega.Succeed())

	// Step 4: settle wait matching setup.waitForActiveDataNodes.
	time.Sleep(5 * time.Second)
}
