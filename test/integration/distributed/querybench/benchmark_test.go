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

package querybench

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/apache/skywalking-banyandb/api/data"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
	testmeasure "github.com/apache/skywalking-banyandb/pkg/test/measure"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

type benchCluster struct {
	conn  *grpc.ClientConn
	close func()
}

// TestDistributedQueryBench dispatches a single test-binary invocation to
// either single-shot mode (one combo, writes a shard) or merge mode (reads
// shards, computes correctness, writes the unified report). The shell
// orchestrator runs the binary once per combo to keep heap and CPU profiles
// isolated to a single (mode, scenario, cardinality) pass.
func TestDistributedQueryBench(t *testing.T) {
	cfg := LoadConfig()
	if !cfg.RunBench {
		t.Skipf("set %s=1 and invoke test/integration/distributed/querybench/run-docker.sh to execute the distributed query benchmark", envRunBench)
	}
	if validateErr := cfg.Validate(); validateErr != nil {
		t.Fatalf("invalid distributed query benchmark config: %v", validateErr)
	}
	if cfg.Merge {
		runMergeShards(t, cfg)
		return
	}
	runSingleShotScenario(t, cfg)
}

// runSingleShotScenario boots one cluster, writes the data set
// at the configured cardinality, runs the timed phase of one scenario, and
// persists the result as a shard JSON. Each invocation is a fresh Go process
// so the captured heap and CPU profiles describe only this combo.
func runSingleShotScenario(t *testing.T, cfg Config) {
	if initErr := logger.Init(logger.Logging{Env: "dev", Level: "info"}); initErr != nil {
		t.Fatalf("initialize logger: %v", initErr)
	}
	gomega.RegisterTestingT(t)
	if cfg.Scenario == ScenarioQLBT05 {
		runQLBT05SingleShot(t, cfg)
		return
	}
	cluster, base, clusterErr := startBenchCluster(t, cfg)
	if clusterErr != nil {
		t.Fatalf("start cluster: %v", clusterErr)
	}
	defer cluster.close()
	timeout := queryTimeout(cfg.Cardinality) + 10*time.Minute
	if cfg.Scenario == ScenarioQLBT03 {
		timeout += 60 * time.Minute
	}
	if cfg.Scenario == ScenarioQLBT07 || cfg.Scenario == ScenarioQLBT08 || cfg.Scenario == ScenarioQLBT10 {
		timeout += 30 * time.Minute
	}
	if cfg.Scenario == ScenarioQLBT02 {
		// Six serial 1,000-query variants include wide protobuf responses.
		timeout += 30 * time.Minute
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var written writeSummary
	var writeErr error
	if cfg.Scenario == ScenarioQLBT12 {
		written, writeErr = writeQLBT12Data(ctx, cluster.conn, cfg, base)
	} else if cfg.Scenario == ScenarioQLBT07 || cfg.Scenario == ScenarioQLBT08 || cfg.Scenario == ScenarioQLBT10 {
		written, writeErr = writeQLBT07Data(ctx, cluster.conn, cfg, base)
	} else {
		written, writeErr = writeBenchmarkData(ctx, cluster.conn, cfg, cfg.Cardinality, base)
	}
	if writeErr != nil {
		t.Fatalf("write benchmark data: %v", writeErr)
	}
	t.Logf("loaded engine=%s cardinality=%d mode=%s rows=%d duration=%s rows_per_sec=%.2f",
		cfg.Engine, cfg.Cardinality, cfg.Mode, written.Rows, written.Duration, written.RowsPerSec)
	result, runErr := runScenarioBenchmark(ctx, cluster.conn, cfg, cfg.Scenario, cfg.Cardinality, cfg.Mode, base)
	if runErr != nil {
		t.Fatalf("run scenario: %v", runErr)
	}
	shardPath, shardErr := writeShard(result, cfg.ReportDir)
	if shardErr != nil {
		t.Fatalf("write shard: %v", shardErr)
	}
	t.Logf("shard written: %s (engine=%s mode=%s scenario=%s cardinality=%d rows=%d traces=%d spans=%d qps=%.2f)",
		shardPath, cfg.Engine, cfg.Mode, cfg.Scenario, cfg.Cardinality, result.ResponseRows, result.ResponseTraces, result.ResponseSpans, result.QPS)
}

// runMergeShards reads every shard the orchestrator produced and builds the
// unified report.
func runMergeShards(t *testing.T, cfg Config) {
	results, readErr := readShards(cfg.ReportDir)
	if readErr != nil {
		t.Fatalf("read shards: %v", readErr)
	}
	if len(results) == 0 {
		t.Fatalf("no shards found in %s/shards/", cfg.ReportDir)
	}
	if matrixErr := validateQLBT03Matrix(results); matrixErr != nil {
		t.Fatal(matrixErr)
	}
	if matrixErr := validateQLBT07Matrix(results); matrixErr != nil {
		t.Fatal(matrixErr)
	}
	if matrixErr := validateQLBT08Matrix(results); matrixErr != nil {
		t.Fatal(matrixErr)
	}
	if matrixErr := validateQLBT10Matrix(results); matrixErr != nil {
		t.Fatal(matrixErr)
	}
	if matrixErr := validateQLBT12Matrix(results); matrixErr != nil {
		t.Fatal(matrixErr)
	}
	var flattened []Result
	for _, result := range results {
		switch result.Scenario {
		case ScenarioQLBT12:
			if validationErr := validateQLBT12Result(result); validationErr != nil {
				t.Fatal(validationErr)
			}
			flattened = append(flattened, result.Variants...)
		case ScenarioQLBT10:
			if validationErr := validateQLBT10Result(result); validationErr != nil {
				t.Fatal(validationErr)
			}
			flattened = append(flattened, result.Variants...)
		case ScenarioQLBT05:
			if validationErr := validateQLBT05Result(result); validationErr != nil {
				t.Fatal(validationErr)
			}
			flattened = append(flattened, result.Variants...)
		case ScenarioQLBT08:
			if validationErr := validateQLBT08Result(result); validationErr != nil {
				t.Fatal(validationErr)
			}
			flattened = append(flattened, result.Variants...)
		case ScenarioQLBT07:
			if validationErr := validateQLBT07Result(result); validationErr != nil {
				t.Fatal(validationErr)
			}
			flattened = append(flattened, result.Variants...)
		case ScenarioQLBT03:
			if validationErr := validateQLBT03Result(result); validationErr != nil {
				t.Fatal(validationErr)
			}
			flattened = append(flattened, result.Variants...)
		case ScenarioQLBT02:
			if validationErr := validateQLBT02Result(result); validationErr != nil {
				t.Fatal(validationErr)
			}
			flattened = append(flattened, result.Variants...)
		case ScenarioQLBT01:
			if validationErr := validateQLBT01Result(result); validationErr != nil {
				t.Fatal(validationErr)
			}
			flattened = append(flattened, result.Variants...)
		default:
			flattened = append(flattened, result)
		}
	}
	results = flattened
	sort.Slice(results, func(i, j int) bool {
		a, b := results[i], results[j]
		if a.Cardinality != b.Cardinality {
			return a.Cardinality < b.Cardinality
		}
		if a.Scenario != b.Scenario {
			return a.Scenario < b.Scenario
		}
		if a.variantKey() != b.variantKey() {
			return a.variantKey() < b.variantKey()
		}
		return false
	})
	report := newReportFromShards(cfg, results)
	jsonPath, mdPath, reportErr := writeReport(report, cfg.ReportDir)
	if reportErr != nil {
		t.Fatalf("write merged report: %v", reportErr)
	}
	t.Logf("merged report: json=%s markdown=%s", jsonPath, mdPath)
}

// runScenarioBenchmark drives warmup + timed queries for a single scenario
// against an already-prepared cluster, capturing CPU and heap profiles for
// the timed phase.
func runScenarioBenchmark(ctx context.Context, conn *grpc.ClientConn, cfg Config, scenario Scenario, cardinality int, mode string, base time.Time) (Result, error) {
	if scenario == ScenarioQLBT12 {
		return runQLBT12Benchmark(ctx, conn, cfg, base)
	}
	if scenario == ScenarioQLBT10 {
		return runQLBT10Benchmark(ctx, conn, cfg, base)
	}
	if scenario == ScenarioQLBT08 {
		return runQLBT08Benchmark(ctx, conn, cfg, base)
	}
	if scenario == ScenarioQLBT07 {
		return runQLBT07Benchmark(ctx, conn, cfg, base)
	}
	if scenario == ScenarioQLBT03 {
		return runQLBT03Benchmark(ctx, conn, cfg, cardinality, mode, base)
	}
	if scenario == ScenarioQLBT02 {
		return runQLBT02Benchmark(ctx, conn, cfg, cardinality, mode, base)
	}
	if scenario == ScenarioQLBT01 {
		return runQLBT01Benchmark(ctx, conn, cfg, cardinality, mode, base)
	}
	if cfg.Engine == engineTrace {
		return runTraceScenarioBenchmark(ctx, conn, cfg, scenario, cardinality, mode, base)
	}
	entities, pointsEach := splitCardinality(cardinality)
	result := Result{
		Engine:          engineMeasure,
		Mode:            mode,
		Scenario:        scenario,
		Cardinality:     cardinality,
		Entities:        entities,
		PointsEach:      pointsEach,
		QueryIterations: cfg.QueryIterations,
		QueryWorkers:    cfg.QueryWorkers,
		Correctness:     "baseline",
	}
	req, requestErr := buildScenarioQuery(scenario, cardinality, base)
	if requestErr != nil {
		return result, requestErr
	}
	if visibilityErr := waitForScenarioVisibility(ctx, conn, req, scenario, cardinality); visibilityErr != nil {
		return result, visibilityErr
	}
	profiler, profileErr := startProfile(cfg.ReportDir, scenario, cardinality, mode, cfg.Profile)
	if profileErr != nil {
		return result, profileErr
	}
	before := captureProcessSnapshot()
	querySummary, queryErr := runScenarioQueries(ctx, conn, req, cfg, cardinality)
	after := captureProcessSnapshot()
	profiles, stopProfileErr := profiler.stop()
	if queryErr != nil {
		return result, queryErr
	}
	if stopProfileErr != nil {
		return result, stopProfileErr
	}
	latencyStats, qps := summarizeLatencies(querySummary.Latencies, querySummary.Elapsed)
	result.ResponseRows = querySummary.Rows
	result.ApproxResultHash = querySummary.Hash
	result.SampleDataPointText = querySummary.SampleDPText
	result.Latency = latencyStats
	result.QPS = qps
	result.Resources = resourceDelta(before, after)
	result.Allocations = allocationDelta(before, after, len(querySummary.Latencies))
	result.Profiles = profiles
	return result, nil
}

// startBenchCluster brings up a local distributed cluster in-process. Measure
// runs use two data nodes; trace runs use DQB_DATA_NODES.
func startBenchCluster(t *testing.T, cfg Config, additionalLiaisonFlags ...string) (benchCluster, time.Time, error) {
	t.Helper()
	savedWireModeRaw := data.MeasureWireModeRaw()
	tmpDir, cleanup, spaceErr := test.NewSpace()
	if spaceErr != nil {
		return benchCluster{}, time.Time{}, fmt.Errorf("create test space: %w", spaceErr)
	}
	dfWriter := setup.NewDiscoveryFileWriter(tmpDir)
	config := setup.PropertyClusterConfig(dfWriter)
	flags := clusterFlags(cfg)
	dataNodeCount := 2
	if cfg.Engine == engineTrace {
		dataNodeCount = cfg.DataNodes
	}
	closeDataNodes := make([]func(), 0, dataNodeCount)
	for nodeIdx := 0; nodeIdx < dataNodeCount; nodeIdx++ {
		closeDataNodes = append(closeDataNodes, setup.DataNode(config, flags...))
	}
	if cfg.Engine == engineTrace {
		setup.PreloadSchemaViaProperty(config, preloadTraceBenchSchema(cfg.ShardNum))
		config.AddLoadedKinds(schema.KindTrace)
	} else {
		preload := testmeasure.PreloadSchema
		if cfg.Scenario == ScenarioQLBT02 {
			preload = preloadQLBT02Schema
		}
		if cfg.Scenario == ScenarioQLBT07 || cfg.Scenario == ScenarioQLBT08 || cfg.Scenario == ScenarioQLBT10 {
			preload = preloadQLBT07Schema
		}
		if cfg.Scenario == ScenarioQLBT12 {
			preload = preloadQLBT12Schema
		}
		setup.PreloadSchemaViaProperty(config, preload)
		config.AddLoadedKinds(schema.KindMeasure)
	}
	liaisonAddr, closeLiaison := setup.LiaisonNode(config, append(flags, additionalLiaisonFlags...)...)
	// Bump the gRPC client's max receive size so visibility checks at
	// high cardinality (100K+ rows of proto-encoded DataPoints exceed the
	// default 4 MiB) don't fail with ResourceExhausted before the timed
	// phase starts. 512 MiB covers the stress cardinality (2M rows ≈
	// ~220 MiB of proto-wire response) with margin.
	conn, connErr := grpchelper.Conn(
		liaisonAddr, 10*time.Second,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(512<<20)),
	)
	if connErr != nil {
		closeLiaison()
		for _, closeDataNode := range closeDataNodes {
			closeDataNode()
		}
		cleanup()
		data.SetMeasureWireModeRaw(savedWireModeRaw)
		return benchCluster{}, time.Time{}, fmt.Errorf("connect liaison: %w", connErr)
	}
	// DQB_BASE_NANOS lets the orchestrator pin the write-time base across
	// every (mode, scenario, cardinality) combo of a run so the correctness
	// gate compares byte-identical responses. Without it, row and vec
	// processes drift apart by tens of seconds and produce per-DataPoint
	// timestamp deltas that diverge the proto-hash even when the logical
	// content is equal.
	ns := timestamp.NowMilli().UnixNano()
	if v := os.Getenv("DQB_BASE_NANOS"); v != "" {
		if parsed, parseErr := strconv.ParseInt(v, 10, 64); parseErr == nil {
			ns = parsed
		}
	}
	base := time.Unix(0, ns-ns%int64(time.Minute))
	closeFn := func() {
		if closeErr := conn.Close(); closeErr != nil {
			t.Logf("close benchmark connection: %v", closeErr)
		}
		closeLiaison()
		for _, closeDataNode := range closeDataNodes {
			closeDataNode()
		}
		cleanup()
		data.SetMeasureWireModeRaw(savedWireModeRaw)
	}
	return benchCluster{conn: conn, close: closeFn}, base, nil
}

func clusterFlags(cfg Config) []string {
	if cfg.Engine == engineTrace {
		return []string{fmt.Sprintf("--trace-vectorized-query-memory-mib=%d", cfg.QueryMemoryMiB)}
	}
	if cfg.Scenario == ScenarioQLBT02 || cfg.Scenario == ScenarioQLBT03 || cfg.Scenario == ScenarioQLBT05 ||
		cfg.Scenario == ScenarioQLBT07 || cfg.Scenario == ScenarioQLBT08 || cfg.Scenario == ScenarioQLBT10 || cfg.Scenario == ScenarioQLBT12 {
		return []string{fmt.Sprintf("--measure-vectorized-query-memory-mib=%d", cfg.QueryMemoryMiB)}
	}
	if cfg.Soak {
		// The measure soak writes while it queries; a short flush timeout makes the
		// parts a scan reads change every second instead of every few seconds.
		return []string{"--measure-flush-timeout=500ms"}
	}
	return nil
}
