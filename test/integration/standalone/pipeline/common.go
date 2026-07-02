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

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/pool"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/gmatcher"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
	tracepipeline "github.com/apache/skywalking-banyandb/test/cases/tracepipeline"
)

var (
	connection *grpc.ClientConn
	goods      []gleak.Goroutine
	closeSvr   func()
	now        time.Time

	// svrSoPath is the bare filename of the staged .so (trusted-dir-relative path).
	// The validator requires a relative path; the data node resolves it against
	// --trace-pipeline-trusted-plugin-dir at load time.
	// Exposed for dynamic_test.go (Update/Remove/InvalidConfig/Restart specs).
	svrSoPath string

	// svrBinPath is the banyand binary used to boot the external process.
	// Exposed for the Restart spec so it can relaunch the same binary.
	svrBinPath string

	// svrTrustedDir is the trusted-plugin directory passed to the server.
	// The Restart spec re-uses it when relaunching.
	svrTrustedDir string
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

	// Temp dirs: data, logs, trusted-plugin+config.
	dataPath, _, dataErr := test.NewSpace()
	gomega.Expect(dataErr).NotTo(gomega.HaveOccurred())
	logDir, _, logErr := test.NewSpace()
	gomega.Expect(logErr).NotTo(gomega.HaveOccurred())
	trustedDir, _, trustedErr := test.NewSpace()
	gomega.Expect(trustedErr).NotTo(gomega.HaveOccurred())

	// Copy the .so into the trusted dir under its bare filename.
	soPath := filepath.Join(trustedDir, tracepipeline.PluginSOName)
	gomega.Expect(copyFile(pluginSrc, soPath)).To(gomega.Succeed())

	// Allocate ports and build the cluster config (property-based schema registry).
	ports, portsErr := test.AllocateFreePorts(5)
	gomega.Expect(portsErr).NotTo(gomega.HaveOccurred())
	tmpDir, _, tmpErr := test.NewSpace()
	gomega.Expect(tmpErr).NotTo(gomega.HaveOccurred())
	dfWriter := setup.NewDiscoveryFileWriter(tmpDir)
	config := setup.PropertyClusterConfig(dfWriter)

	// Launch the external standalone server.
	// No static --trace-pipeline-config: the sampler is activated dynamically
	// via RegisterSamplerRuntime (UpdateGroup) after schema preload (US-CLEANUP-standalone).
	// ExternalStandalone calls config.AddSchemaServerAddr so that
	// PreloadSchemaViaProperty can reach the schema server after boot.
	grpcAddr, _, svClose := setup.ExternalStandalone(
		config,
		binPath,
		dataPath,
		logDir,
		ports,
		"--trace-pipeline-native-plugin-enabled=true",
		"--trace-pipeline-trusted-plugin-dir="+trustedDir,
		"--trace-pipeline-merge-grace-default=0",
		"--trace-max-merge-parts=2",
		"--trace-flush-timeout=500ms",
	)
	closeSvr = svClose

	// Populate package-level vars for use by other test files (dynamic_test.go).
	// svrSoPath is the bare (trusted-dir-relative) filename — the validator requires
	// a relative path; the data node resolves it against the trusted dir at load time.
	svrSoPath = tracepipeline.PluginSOName
	svrBinPath = binPath
	svrTrustedDir = trustedDir

	// Preload trace schemas (group test-trace-pipeline + trace filter + index rules/bindings)
	// into the running server via the property schema gRPC endpoint. The fixtures are
	// owned by the tracepipeline case package so they do not pollute the shared
	// pkg/test/trace preload set that every other integration suite loads.
	setup.PreloadSchemaViaProperty(config, tracepipeline.PreloadSchema)

	// Wait until the schema has propagated to the data node.
	waitForSchemaSync(grpcAddr)

	// Activate the sampler pipeline dynamically via UpdateGroup (US-CLEANUP-standalone).
	// The data node's KindGroup watch fires and loads the .so plugin into the registry.
	// All specs in this suite assume the base config (thresholdMs=500) is active.
	activateCtx, activateCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer activateCancel()
	activateConn, activateConnErr := grpchelper.Conn(grpcAddr, 10*time.Second,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	gomega.Expect(activateConnErr).NotTo(gomega.HaveOccurred())
	defer func() { _ = activateConn.Close() }()
	tracepipeline.RegisterSamplerRuntime(activateCtx, activateConn,
		tracepipeline.PipelineGroup,
		tracepipeline.NewBasePipelineConfig(tracepipeline.PluginSOName, tracepipeline.DefaultMergeGrace))

	ns := timestamp.NowMilli().UnixNano()
	now = time.Unix(0, ns-ns%int64(time.Minute))
	return []byte(grpcAddr)
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
}, func() {
	if closeSvr != nil {
		closeSvr()
	}
})

var _ = ginkgo.ReportAfterSuite("Trace Pipeline Integration Suite", func(report ginkgo.Report) {
	if report.SuiteSucceeded {
		gomega.Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
		gomega.Eventually(pool.AllRefsCount, flags.EventuallyTimeout).Should(gmatcher.HaveZeroRef())
	}
})

// The Ordered Describe block seeds data and registers the merge-filter table.
// BeforeAll performs the staged seed (batch1 → Phase-0 assert → batch2) so that
// by the time the table entries run, the filtering compaction has been triggered.
// RegisterMergeFilterTable is called at the top level of the Describe so Ginkgo
// collects the table entries during spec-tree construction.
var _ = ginkgo.Describe("In-Merge Filter via .so plugin (standalone)", ginkgo.Ordered, func() {
	ginkgo.BeforeAll(func() {
		// Phase-0: seed batch 1 (2 drops + 2 keeps → one mem-part), then assert
		// both drop candidates are visible before any merge has happened.
		tracepipeline.SeedBatch1(now)
		tracepipeline.AssertDropCandidatesVisible(now)

		// Seed batch 2 (3 keeps → second part, triggers filtering compaction
		// with max-merge-parts=2 and grace=0). 4/3 split passes the real
		// merger_policy size-balance gate (smaller*2 >= larger).
		tracepipeline.SeedBatch2(now)
	})

	tracepipeline.RegisterMergeFilterTable("Standalone (.so plugin): In-Merge Filter")
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
	// test/integration/standalone/pipeline → 4 dirs up → module root.
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

// waitForSchemaSync polls the gRPC endpoint until at least one group is listed,
// confirming the property→data-node schema sync has completed.
func waitForSchemaSync(grpcAddr string) {
	conn, connErr := grpchelper.Conn(grpcAddr, 10*time.Second,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	gomega.Expect(connErr).NotTo(gomega.HaveOccurred())
	defer func() { _ = conn.Close() }()

	groupClient := databasev1.NewGroupRegistryServiceClient(conn)
	gomega.Eventually(func(innerGm gomega.Gomega) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		resp, listErr := groupClient.List(ctx, &databasev1.GroupRegistryServiceListRequest{})
		innerGm.Expect(listErr).NotTo(gomega.HaveOccurred())
		innerGm.Expect(resp.GetGroup()).NotTo(gomega.BeEmpty(), "no groups visible on standalone yet")
	}, flags.EventuallyTimeout, 500*time.Millisecond).Should(gomega.Succeed())
}

// waitForTraceSchema polls until the "filter" trace schema in the pipeline group
// is visible on the node. WriteBatchEntries calls GetTrace internally, so this
// gate must pass before seeding data — especially after a restart where the
// trace schema re-syncs after the group.
func waitForTraceSchema(grpcAddr string) {
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
			"trace schema filter/%s not yet visible after restart", tracepipeline.PipelineGroup)
	}, flags.EventuallyTimeout, 500*time.Millisecond).Should(gomega.Succeed())
}
