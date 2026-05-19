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
	"strings"
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

// runSingleShotScenario boots one cluster (row or vec), writes the data set
// at the configured cardinality, runs the timed phase of one scenario, and
// persists the result as a shard JSON. Each invocation is a fresh Go process
// so the captured heap and CPU profiles describe only this combo.
func runSingleShotScenario(t *testing.T, cfg Config) {
	if initErr := logger.Init(logger.Logging{Env: "dev", Level: "info"}); initErr != nil {
		t.Fatalf("initialize logger: %v", initErr)
	}
	gomega.RegisterTestingT(t)
	cluster, base, clusterErr := startBenchCluster(t, cfg.Mode == modeVec)
	if clusterErr != nil {
		t.Fatalf("start cluster: %v", clusterErr)
	}
	defer cluster.close()
	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout(cfg.Cardinality)+10*time.Minute)
	defer cancel()
	writeSummary, writeErr := writeBenchmarkData(ctx, cluster.conn, cfg, cfg.Cardinality, base)
	if writeErr != nil {
		t.Fatalf("write benchmark data: %v", writeErr)
	}
	t.Logf("loaded cardinality=%d mode=%s rows=%d duration=%s rows_per_sec=%.2f",
		cfg.Cardinality, cfg.Mode, writeSummary.Rows, writeSummary.Duration, writeSummary.RowsPerSec)
	result, runErr := runScenarioBenchmark(ctx, cluster.conn, cfg, cfg.Scenario, cfg.Cardinality, cfg.Mode, base)
	if runErr != nil {
		t.Fatalf("run scenario: %v", runErr)
	}
	shardPath, shardErr := writeShard(result, cfg.ReportDir)
	if shardErr != nil {
		t.Fatalf("write shard: %v", shardErr)
	}
	t.Logf("shard written: %s (mode=%s scenario=%s cardinality=%d rows=%d qps=%.2f)",
		shardPath, cfg.Mode, cfg.Scenario, cfg.Cardinality, result.ResponseRows, result.QPS)
}

// runMergeShards reads every shard the orchestrator produced, computes vec
// correctness against its row counterpart, builds the unified report, and
// fails the test if any vec result diverges from its row baseline.
func runMergeShards(t *testing.T, cfg Config) {
	results, readErr := readShards(cfg.ReportDir)
	if readErr != nil {
		t.Fatalf("read shards: %v", readErr)
	}
	if len(results) == 0 {
		t.Fatalf("no shards found in %s/shards/", cfg.ReportDir)
	}
	sort.Slice(results, func(i, j int) bool {
		a, b := results[i], results[j]
		if a.Cardinality != b.Cardinality {
			return a.Cardinality < b.Cardinality
		}
		if a.Scenario != b.Scenario {
			return a.Scenario < b.Scenario
		}
		if a.Mode != b.Mode {
			return a.Mode == modeRow
		}
		return false
	})
	type comboKey struct {
		scenario    Scenario
		cardinality int
	}
	rowByCombo := make(map[comboKey]Result, len(results)/2+1)
	for _, r := range results {
		if r.Mode == modeRow {
			rowByCombo[comboKey{r.Scenario, r.Cardinality}] = r
		}
	}
	var correctnessErrs []string
	for i := range results {
		if results[i].Mode != modeVec {
			continue
		}
		rowResult, ok := rowByCombo[comboKey{results[i].Scenario, results[i].Cardinality}]
		if !ok {
			results[i].Correctness = "no row counterpart"
			correctnessErrs = append(correctnessErrs,
				fmt.Sprintf("scenario=%s cardinality=%d: vec shard without matching row shard", results[i].Scenario, results[i].Cardinality))
			continue
		}
		verdict := compareModeResults(rowResult, results[i], cfg.SmallExactRows)
		results[i].Correctness = verdict
		if verdict == "matched" || verdict == "matched rows; sampled hash differs for large result" {
			continue
		}
		if rowResult.Error != "" || results[i].Error != "" {
			continue
		}
		// Dump prototext samples so the source of a hash divergence
		// (TagFamily order, oneof variant, missing field) is visible in
		// the test log without a follow-up instrumentation pass.
		t.Logf("=== sample dump for scenario=%s cardinality=%d ===", results[i].Scenario, results[i].Cardinality)
		t.Logf("--- row sample DataPoint ---\n%s", rowResult.SampleDataPointText)
		t.Logf("--- vec sample DataPoint ---\n%s", results[i].SampleDataPointText)
		correctnessErrs = append(correctnessErrs,
			fmt.Sprintf("scenario=%s cardinality=%d: %s", results[i].Scenario, results[i].Cardinality, verdict))
	}
	report := newReportFromShards(cfg, results)
	jsonPath, mdPath, reportErr := writeReport(report, cfg.ReportDir)
	if reportErr != nil {
		t.Fatalf("write merged report: %v", reportErr)
	}
	t.Logf("merged report: json=%s markdown=%s", jsonPath, mdPath)
	if len(correctnessErrs) > 0 {
		t.Fatalf("correctness failures:\n%s", strings.Join(correctnessErrs, "\n"))
	}
}

// runScenarioBenchmark drives warmup + timed queries for a single scenario
// against an already-prepared cluster, capturing CPU and heap profiles for
// the timed phase.
func runScenarioBenchmark(ctx context.Context, conn *grpc.ClientConn, cfg Config, scenario Scenario, cardinality int, mode string, base time.Time) (Result, error) {
	entities, pointsEach := splitCardinality(cardinality)
	result := Result{
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

// startBenchCluster brings up a local distributed cluster (2 data nodes + 1
// liaison) in-process. When vectorized is true every node is started with
// --measure-vectorized-enabled=true.
func startBenchCluster(t *testing.T, vectorized bool) (benchCluster, time.Time, error) {
	t.Helper()
	savedWireModeRaw := data.MeasureWireModeRaw()
	tmpDir, cleanup, spaceErr := test.NewSpace()
	if spaceErr != nil {
		return benchCluster{}, time.Time{}, fmt.Errorf("create test space: %w", spaceErr)
	}
	dfWriter := setup.NewDiscoveryFileWriter(tmpDir)
	config := setup.PropertyClusterConfig(dfWriter)
	// --measure-vectorized-enabled defaults to true (pkg/query/vectorized/
	// measure/config.go DefaultConfig). Pass the flag explicitly for both
	// modes so "row" mode genuinely disables vec dispatch + the raw-frame
	// wire codec; without =false the row cluster silently runs the vec
	// engine and the comparison degenerates into two copies of vec.
	vecFlag := "--measure-vectorized-enabled=false"
	if vectorized {
		vecFlag = "--measure-vectorized-enabled=true"
	}
	flags := []string{vecFlag}
	closeDataNode0 := setup.DataNode(config, flags...)
	closeDataNode1 := setup.DataNode(config, flags...)
	setup.PreloadSchemaViaProperty(config, testmeasure.PreloadSchema)
	config.AddLoadedKinds(schema.KindMeasure)
	liaisonAddr, closeLiaison := setup.LiaisonNode(config, flags...)
	conn, connErr := grpchelper.Conn(liaisonAddr, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if connErr != nil {
		closeLiaison()
		closeDataNode0()
		closeDataNode1()
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
		closeDataNode0()
		closeDataNode1()
		cleanup()
		data.SetMeasureWireModeRaw(savedWireModeRaw)
	}
	return benchCluster{conn: conn, close: closeFn}, base, nil
}

// compareModeResults reports whether a vec result matches its row baseline.
// Small results (<= exactRowsLimit rows) require exact response hash equality;
// larger scans accept matching row counts with a sampled-hash mismatch since
// sample order can drift legitimately across modes.
func compareModeResults(rowResult, vecResult Result, exactRowsLimit int) string {
	if rowResult.Error != "" || vecResult.Error != "" {
		return "not compared: error"
	}
	if rowResult.ResponseRows != vecResult.ResponseRows {
		return fmt.Sprintf("mismatch: rows row=%d vec=%d", rowResult.ResponseRows, vecResult.ResponseRows)
	}
	if rowResult.Cardinality <= exactRowsLimit && rowResult.ApproxResultHash != vecResult.ApproxResultHash {
		return fmt.Sprintf("mismatch: hash row=%d vec=%d", rowResult.ApproxResultHash, vecResult.ApproxResultHash)
	}
	if rowResult.ApproxResultHash != vecResult.ApproxResultHash {
		return "matched rows; sampled hash differs for large result"
	}
	return "matched"
}
