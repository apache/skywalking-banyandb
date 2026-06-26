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
	soPath := filepath.Join(trustedDir, "latencystatussampler.so")
	gomega.Expect(copyFile(pluginSrc, soPath)).To(gomega.Succeed())

	// Write the TracePipelineConfig protojson file.
	configFile := filepath.Join(trustedDir, "pipeline.json")
	gomega.Expect(writePipelineConfig(configFile)).To(gomega.Succeed())

	// Allocate ports and build the cluster config (property-based schema registry).
	ports, portsErr := test.AllocateFreePorts(5)
	gomega.Expect(portsErr).NotTo(gomega.HaveOccurred())
	tmpDir, _, tmpErr := test.NewSpace()
	gomega.Expect(tmpErr).NotTo(gomega.HaveOccurred())
	dfWriter := setup.NewDiscoveryFileWriter(tmpDir)
	config := setup.PropertyClusterConfig(dfWriter)

	// Launch the external standalone server.
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
		"--trace-pipeline-config="+configFile,
		"--trace-pipeline-merge-grace-default=0",
		"--trace-max-merge-parts=2",
		"--trace-flush-timeout=500ms",
	)
	closeSvr = svClose

	// Preload trace schemas (group test-trace-pipeline + trace filter + index rules/bindings)
	// into the running server via the property schema gRPC endpoint. The fixtures are
	// owned by the tracepipeline case package so they do not pollute the shared
	// pkg/test/trace preload set that every other integration suite loads.
	setup.PreloadSchemaViaProperty(config, tracepipeline.PreloadSchema)

	// Wait until the schema has propagated to the data node.
	waitForSchemaSync(grpcAddr)

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
