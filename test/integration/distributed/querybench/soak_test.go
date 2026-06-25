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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"testing"
	"time"

	"github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	"github.com/apache/skywalking-banyandb/api/data"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	vtrace "github.com/apache/skywalking-banyandb/pkg/query/vectorized/trace"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// soakResult is the machine-readable artifact emitted by the soak run. CI
// gates on the go test exit status AND this file (its own artifact, distinct
// from the benchmark's summary.json).
type soakResult struct {
	Engine                    string  `json:"engine"`
	Iterations                int     `json:"iterations"`
	QueryCountDelta           int64   `json:"query_count_delta"`
	QueryCountMonotonic       bool    `json:"query_count_monotonic"`
	LivenessPass              bool    `json:"liveness_pass"`
	HeapInuseBaseline         uint64  `json:"heap_inuse_baseline_bytes"`
	HeapInuseEnd              uint64  `json:"heap_inuse_end_bytes"`
	HeapGrowthPct             float64 `json:"heap_growth_pct"`
	HeapGrowthMaxPct          int     `json:"heap_growth_max_pct"`
	HeapLeakPass              bool    `json:"heap_leak_pass"`
	BudgetScenarioResultBound bool    `json:"budget_scenario_result_bound"`
	BudgetScenarioHeapPass    bool    `json:"budget_scenario_heap_pass"`
	BudgetScenarioPass        bool    `json:"budget_scenario_pass"`
}

// soakIterCount is the number of query loop iterations run in the soak. This
// is kept small so the soak completes in a bounded time inside the container;
// the container's cgroup memory limit (set by run-docker.sh --memory) is what
// makes the heap-growth gate meaningful and reproducible.
const soakIterCount = 200

// soakBudgetMiB is the small memory budget used in the budget-engagement
// scenario. It is chosen to be well below the default 256 MiB so that the
// heavy-tail fixture (500-span traces with 1 KiB/span) reliably triggers the
// hard-stop gate and first-block exception on every query iteration.
const soakBudgetMiB = 2

// TestTraceVecSoak is the Instrument 2 sustained in-process soak for the
// trace vectorized query path. It reads vtrace.QueryCount() and
// runtime.ReadMemStats directly (same process) to prove:
//
//  1. Liveness: vtrace.QueryCount() is monotonic non-decreasing across
//     samples and the final delta >= iterations (vec fired every iteration).
//  2. No cursor-release leak: heap inuse_space growth from post-warmup
//     baseline to end is <= SOAK_HEAP_GROWTH_MAX_PCT (default 10%).
//  3. Budget-gate engagement: a small --trace-vectorized-query-memory-mib +
//     heavy-tail fat-trace fixture causes the hard-stop + first-block
//     exception to fire; results stay correct/bounded and heap stays near-flat.
//
// The test skips unless DQB_SOAK=1. When DQB_SOAK=1 it also requires
// DQB_IN_CONTAINER=1 and hard-fails if that gate is not set, mirroring the
// DQB_IN_CONTAINER guard in TestDistributedQueryBench. This ensures the soak
// runs only inside the resource-limited container launched by run-docker.sh.
func TestTraceVecSoak(t *testing.T) {
	cfg := LoadConfig()
	if !cfg.Soak {
		t.Skipf("set %s=1 and invoke test/integration/distributed/querybench/run-docker.sh to execute the trace vec soak", envSoak)
	}
	if validateErr := cfg.ValidateSoak(); validateErr != nil {
		t.Fatalf("invalid soak config: %v", validateErr)
	}

	if initErr := logger.Init(logger.Logging{Env: "dev", Level: "warn"}); initErr != nil {
		t.Fatalf("initialize logger: %v", initErr)
	}
	gomega.RegisterTestingT(t)

	// --- Phase 1: parity fixture (uniform S=20, budget never engages) ---
	t.Log("[soak] booting in-process cluster (vec-on, parity fixture)")
	parityCfg := cfg
	parityCfg.SpansPerTrace = 20
	parityCfg.SpanDist = spanDistUniform
	parityCfg.QueryMemoryMiB = defaultQueryMemoryMiB
	parityCfg.Cardinality = 1000
	parityCfg.DataNodes = 2
	parityCfg.ShardNum = defaultShardNum
	parityCfg.SpanBytes = defaultSpanBytes
	parityCfg.FilterSelectivity = defaultSelectivity
	parityCfg.TraceIDBatch = 1
	parityCfg.Writers = 4
	parityCfg.WarmupIterations = 3
	parityCluster, parityBase, parityClusterErr := startSoakCluster(t, parityCfg, true)
	if parityClusterErr != nil {
		t.Fatalf("start parity cluster: %v", parityClusterErr)
	}
	defer parityCluster.close()

	soakCtx, soakCancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer soakCancel()

	t.Log("[soak] seeding parity fixture")
	_, parityWriteErr := writeTraceData(soakCtx, parityCluster.conn, parityCfg, parityCfg.Cardinality, parityBase)
	if parityWriteErr != nil {
		t.Fatalf("write parity fixture: %v", parityWriteErr)
	}

	parityReq, parityReqErr := buildTraceScenarioQuery(ScenarioTraceTagFilter, parityCfg, deriveTraceShape(parityCfg.Cardinality, parityCfg.SpansPerTrace, parityCfg.SpanDist), parityBase)
	if parityReqErr != nil {
		t.Fatalf("build parity query: %v", parityReqErr)
	}
	expectedParityTraces := expectedTraceResultCount(ScenarioTraceTagFilter, parityCfg, deriveTraceShape(parityCfg.Cardinality, parityCfg.SpansPerTrace, parityCfg.SpanDist))
	if visErr := waitForTraceVisibility(soakCtx, parityCluster.conn, parityReq, ScenarioTraceTagFilter, parityCfg.Cardinality, expectedParityTraces); visErr != nil {
		t.Fatalf("parity fixture not visible: %v", visErr)
	}

	// Warmup: burn a few iterations so JIT-compiled paths + GC are warm before
	// we snapshot the heap baseline.
	t.Log("[soak] warming up parity path")
	parityClient := tracev1.NewTraceServiceClient(parityCluster.conn)
	for warmupIdx := 0; warmupIdx < parityCfg.WarmupIterations; warmupIdx++ {
		warmupCtx, warmupCancel := context.WithTimeout(soakCtx, queryTimeout(parityCfg.Cardinality))
		_, warmupErr := parityClient.Query(warmupCtx, proto.Clone(parityReq).(*tracev1.QueryRequest))
		warmupCancel()
		if warmupErr != nil {
			t.Fatalf("parity warmup iteration %d: %v", warmupIdx, warmupErr)
		}
	}

	// Snapshot post-warmup baseline.
	runtime.GC()
	var baseMemStats runtime.MemStats
	runtime.ReadMemStats(&baseMemStats)
	heapInuseBaseline := baseMemStats.HeapInuse
	queryCountBaseline := vtrace.QueryCount()
	t.Logf("[soak] post-warmup baseline: heap_inuse=%d bytes query_count=%d", heapInuseBaseline, queryCountBaseline)

	// Soak loop: iterate the two trace query shapes, sampling QueryCount and
	// heap every soakSampleEvery iterations to assert monotonicity.
	const soakSampleEvery = 20
	queryCountMonotonic := true
	prevQueryCount := queryCountBaseline

	t.Logf("[soak] running %d iterations over parity fixture", soakIterCount)
	for iterIdx := 0; iterIdx < soakIterCount; iterIdx++ {
		iterCtx, iterCancel := context.WithTimeout(soakCtx, queryTimeout(parityCfg.Cardinality))
		_, iterErr := parityClient.Query(iterCtx, proto.Clone(parityReq).(*tracev1.QueryRequest))
		iterCancel()
		if iterErr != nil {
			t.Fatalf("[soak] parity query iteration %d failed: %v", iterIdx, iterErr)
		}
		if (iterIdx+1)%soakSampleEvery == 0 {
			sampledCount := vtrace.QueryCount()
			if sampledCount < prevQueryCount {
				queryCountMonotonic = false
				t.Errorf("[soak] QueryCount non-monotonic at iteration %d: prev=%d current=%d", iterIdx, prevQueryCount, sampledCount)
			}
			prevQueryCount = sampledCount
		}
	}

	// Final heap snapshot — force GC first so freed objects are reclaimed.
	runtime.GC()
	var endMemStats runtime.MemStats
	runtime.ReadMemStats(&endMemStats)
	heapInuseEnd := endMemStats.HeapInuse
	queryCountEnd := vtrace.QueryCount()
	queryCountDelta := queryCountEnd - queryCountBaseline

	// Liveness gate: final delta must be >= iterations (vec fired every call).
	livenessPass := queryCountDelta >= int64(soakIterCount)
	if !livenessPass {
		t.Errorf("[soak] liveness gate FAIL: query_count_delta=%d < iterations=%d", queryCountDelta, soakIterCount)
	}

	// Heap-growth gate (authoritative leak gate per plan §Instrument 2).
	// Uses heap InUse (bytes of live objects on the heap) rather than HeapSys
	// (address space reserved) because InUse drops on GC whereas Sys ratchets.
	// Compute growth only when end exceeds baseline; otherwise the unsigned
	// subtraction underflows and reports a nonsense percentage (end<baseline
	// just means memory was reclaimed — no leak).
	heapGrowthPct := 0.0
	heapLeakPass := true
	if heapInuseEnd > heapInuseBaseline && heapInuseBaseline > 0 {
		heapGrowthPct = float64(heapInuseEnd-heapInuseBaseline) / float64(heapInuseBaseline) * 100.0
		heapLeakPass = heapGrowthPct <= float64(cfg.SoakHeapGrowthMaxPct)
		if !heapLeakPass {
			t.Errorf("[soak] heap-leak gate FAIL: inuse_baseline=%d inuse_end=%d growth=%.2f%% > max=%d%%",
				heapInuseBaseline, heapInuseEnd, heapGrowthPct, cfg.SoakHeapGrowthMaxPct)
		}
	}

	t.Logf("[soak] parity results: query_count_delta=%d monotonic=%v liveness=%v heap_inuse_baseline=%d heap_inuse_end=%d heap_growth=%.2f%% heap_leak_pass=%v",
		queryCountDelta, queryCountMonotonic, livenessPass, heapInuseBaseline, heapInuseEnd, heapGrowthPct, heapLeakPass)

	// --- Phase 2: budget-engagement scenario ---
	// Small memory budget + heavy-tail fat traces (500-span traces at 1 KiB/span
	// = ~500 KiB/trace uncompressed) so the hard-stop gate fires on every query.
	// Asserts: results stay bounded/correct AND heap stays near-flat (cursor-
	// release path does not leak when the budget hard-stops mid-scan).
	t.Log("[soak] booting in-process cluster for budget-engagement scenario (vec-on, heavytail, small budget)")
	budgetCfg := cfg
	budgetCfg.SpansPerTrace = 20
	budgetCfg.SpanDist = spanDistHeavytail
	budgetCfg.QueryMemoryMiB = soakBudgetMiB
	budgetCfg.Cardinality = 3450 // 100 traces: 5×500-span + 95×10-span = ~2750 KiB >> 2 MiB budget
	budgetCfg.DataNodes = 2
	budgetCfg.ShardNum = defaultShardNum
	budgetCfg.SpanBytes = defaultSpanBytes
	budgetCfg.FilterSelectivity = 1.0 // all traces match so the budget must actually truncate
	budgetCfg.TraceIDBatch = 1
	budgetCfg.Writers = 4
	budgetCfg.WarmupIterations = 3
	budgetCluster, budgetBase, budgetClusterErr := startSoakCluster(t, budgetCfg, true)
	if budgetClusterErr != nil {
		t.Fatalf("start budget cluster: %v", budgetClusterErr)
	}
	defer budgetCluster.close()

	budgetCtx, budgetCancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer budgetCancel()

	t.Log("[soak] seeding budget-engagement fixture")
	_, budgetWriteErr := writeTraceData(budgetCtx, budgetCluster.conn, budgetCfg, budgetCfg.Cardinality, budgetBase)
	if budgetWriteErr != nil {
		t.Fatalf("write budget fixture: %v", budgetWriteErr)
	}

	budgetShape := deriveTraceShape(budgetCfg.Cardinality, budgetCfg.SpansPerTrace, budgetCfg.SpanDist)
	budgetReq, budgetReqErr := buildTraceScenarioQuery(ScenarioTraceTagFilter, budgetCfg, budgetShape, budgetBase)
	if budgetReqErr != nil {
		t.Fatalf("build budget query: %v", budgetReqErr)
	}
	// With selectivity=1.0 all traces match; wait for at least 1 to be visible.
	if visErr := waitForTraceVisibility(budgetCtx, budgetCluster.conn, budgetReq, ScenarioTraceTagFilter, budgetCfg.Cardinality, 1); visErr != nil {
		t.Fatalf("budget fixture not visible: %v", visErr)
	}

	// Warmup the budget path.
	budgetClient := tracev1.NewTraceServiceClient(budgetCluster.conn)
	for warmupIdx := 0; warmupIdx < budgetCfg.WarmupIterations; warmupIdx++ {
		warmupCtx, warmupCancel := context.WithTimeout(budgetCtx, queryTimeout(budgetCfg.Cardinality))
		_, warmupErr := budgetClient.Query(warmupCtx, proto.Clone(budgetReq).(*tracev1.QueryRequest))
		warmupCancel()
		if warmupErr != nil {
			t.Fatalf("budget warmup iteration %d: %v", warmupIdx, warmupErr)
		}
	}

	runtime.GC()
	var budgetBaseMemStats runtime.MemStats
	runtime.ReadMemStats(&budgetBaseMemStats)
	budgetHeapBaseline := budgetBaseMemStats.HeapInuse
	budgetGoroutineBaseline := runtime.NumGoroutine()
	budgetQueryCountBaseline := vtrace.QueryCount()
	dumpHeapProfile(t, filepath.Join(cfg.ReportDir, "budget-heap-baseline.pprof"))

	// Budget soak loop: every iteration must return a bounded result set (the
	// budget truncates it) and must not raise a query error. This exercises the
	// cursor-release path that is triggered when the hard-stop skips cursors
	// mid-scan. A leak here would cause the heap to grow monotonically.
	const budgetIterCount = 50
	budgetResultBound := true
	var firstBudgetTraceCount int = -1
	t.Logf("[soak] running %d iterations over budget fixture (budget=%d MiB heavytail)", budgetIterCount, soakBudgetMiB)
	for iterIdx := 0; iterIdx < budgetIterCount; iterIdx++ {
		iterCtx, iterCancel := context.WithTimeout(budgetCtx, queryTimeout(budgetCfg.Cardinality))
		resp, iterErr := budgetClient.Query(iterCtx, proto.Clone(budgetReq).(*tracev1.QueryRequest))
		iterCancel()
		if iterErr != nil {
			t.Fatalf("[soak] budget query iteration %d failed: %v", iterIdx, iterErr)
		}
		traceCount, _ := countTraceResponse(resp)
		// Result must be bounded (at or below the query limit) on every iteration.
		if traceCount > traceQueryLimit {
			budgetResultBound = false
			t.Errorf("[soak] budget iteration %d: trace_count=%d exceeds query limit %d", iterIdx, traceCount, traceQueryLimit)
		}
		if firstBudgetTraceCount < 0 {
			firstBudgetTraceCount = traceCount
		}
		// Verify result count is stable across iterations (deterministic fixture).
		if traceCount != firstBudgetTraceCount {
			t.Errorf("[soak] budget iteration %d: trace_count=%d diverged from first=%d", iterIdx, traceCount, firstBudgetTraceCount)
		}
	}

	// Allow a short settle so in-flight gRPC transport goroutines complete before
	// the goroutine snapshot. This avoids flaky counts from temporary bursts.
	time.Sleep(200 * time.Millisecond)
	runtime.GC()
	var budgetEndMemStats runtime.MemStats
	runtime.ReadMemStats(&budgetEndMemStats)
	budgetHeapEnd := budgetEndMemStats.HeapInuse
	budgetGoroutineEnd := runtime.NumGoroutine()
	dumpHeapProfile(t, filepath.Join(cfg.ReportDir, "budget-heap-end.pprof"))
	budgetQueryCountEnd := vtrace.QueryCount()
	budgetQueryCountDelta := budgetQueryCountEnd - budgetQueryCountBaseline

	// Goroutine-leak gate: a leaked sidx producer goroutine (blocked sending on a
	// drained channel after budget early-return) would appear here as a permanent
	// goroutine count increase well above the small transient allowance.
	const budgetGoroutineAllowance = 20
	budgetGoroutineDelta := budgetGoroutineEnd - budgetGoroutineBaseline
	budgetGoroutinePass := budgetGoroutineDelta <= budgetGoroutineAllowance
	if !budgetGoroutinePass {
		t.Errorf("[soak] budget goroutine-leak gate FAIL: baseline=%d end=%d delta=%d > allowance=%d",
			budgetGoroutineBaseline, budgetGoroutineEnd, budgetGoroutineDelta, budgetGoroutineAllowance)
	}

	budgetHeapGrowthPct := 0.0
	if budgetHeapBaseline > 0 && budgetHeapEnd > budgetHeapBaseline {
		budgetHeapGrowthPct = float64(budgetHeapEnd-budgetHeapBaseline) / float64(budgetHeapBaseline) * 100.0
	}
	budgetHeapPass := budgetHeapGrowthPct <= float64(cfg.SoakHeapGrowthMaxPct)
	budgetLivenessPass := budgetQueryCountDelta >= int64(budgetIterCount)
	budgetScenarioPass := budgetResultBound && budgetHeapPass && budgetLivenessPass && budgetGoroutinePass

	if !budgetLivenessPass {
		t.Errorf("[soak] budget liveness FAIL: query_count_delta=%d < %d", budgetQueryCountDelta, budgetIterCount)
	}
	if !budgetHeapPass {
		t.Errorf("[soak] budget heap-leak gate FAIL: heap_inuse_baseline=%d end=%d growth=%.2f%% > max=%d%%",
			budgetHeapBaseline, budgetHeapEnd, budgetHeapGrowthPct, cfg.SoakHeapGrowthMaxPct)
	}

	t.Logf("[soak] budget results: query_count_delta=%d liveness=%v result_bound=%v heap_growth=%.2f%% heap_pass=%v goroutine_baseline=%d goroutine_end=%d goroutine_delta=%d goroutine_pass=%v overall=%v",
		budgetQueryCountDelta, budgetLivenessPass, budgetResultBound, budgetHeapGrowthPct, budgetHeapPass,
		budgetGoroutineBaseline, budgetGoroutineEnd, budgetGoroutineDelta, budgetGoroutinePass, budgetScenarioPass)

	// --- Emit machine-readable JSON artifact ---
	result := soakResult{
		Engine:                    engineTrace,
		Iterations:                soakIterCount,
		QueryCountDelta:           queryCountDelta,
		QueryCountMonotonic:       queryCountMonotonic,
		LivenessPass:              livenessPass,
		HeapInuseBaseline:         heapInuseBaseline,
		HeapInuseEnd:              heapInuseEnd,
		HeapGrowthPct:             heapGrowthPct,
		HeapGrowthMaxPct:          cfg.SoakHeapGrowthMaxPct,
		HeapLeakPass:              heapLeakPass,
		BudgetScenarioResultBound: budgetResultBound,
		BudgetScenarioHeapPass:    budgetHeapPass,
		BudgetScenarioPass:        budgetScenarioPass,
	}
	emitSoakResult(t, cfg, result)

	// Surface any gate failures as a final test failure summary.
	if !queryCountMonotonic {
		t.Error("[soak] GATE FAIL: QueryCount was non-monotonic during parity soak loop")
	}
	if !livenessPass {
		t.Errorf("[soak] GATE FAIL: liveness — query_count_delta=%d < %d", queryCountDelta, soakIterCount)
	}
	if !heapLeakPass {
		t.Errorf("[soak] GATE FAIL: heap-leak — growth=%.2f%% > max=%d%%", heapGrowthPct, cfg.SoakHeapGrowthMaxPct)
	}
	if !budgetScenarioPass {
		t.Error("[soak] GATE FAIL: budget-engagement scenario (see details above)")
	}
}

// dumpHeapProfile writes a post-GC heap profile for attribution analysis. It is
// diagnostic only (run runtime.GC() before calling); failures are logged, not fatal.
func dumpHeapProfile(t *testing.T, path string) {
	t.Helper()
	f, createErr := os.Create(path)
	if createErr != nil {
		t.Logf("[soak] heap profile create %s: %v", path, createErr)
		return
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			t.Logf("[soak] heap profile close %s: %v", path, closeErr)
		}
	}()
	if writeErr := pprof.WriteHeapProfile(f); writeErr != nil {
		t.Logf("[soak] heap profile write %s: %v", path, writeErr)
		return
	}
	t.Logf("[soak] heap profile written: %s", path)
}

// startSoakCluster is a thin wrapper around startBenchCluster that supplies the
// soak-specific defaults (no profiling, trace engine always).
func startSoakCluster(t *testing.T, cfg Config, vectorized bool) (benchCluster, time.Time, error) {
	t.Helper()
	savedWireModeRaw := data.MeasureWireModeRaw()
	tmpDir, cleanup, spaceErr := test.NewSpace()
	if spaceErr != nil {
		return benchCluster{}, time.Time{}, fmt.Errorf("create test space: %w", spaceErr)
	}
	dfWriter := setup.NewDiscoveryFileWriter(tmpDir)
	config := setup.PropertyClusterConfig(dfWriter)
	flags := clusterFlags(cfg, vectorized)
	dataNodeCount := cfg.DataNodes
	if dataNodeCount <= 0 {
		dataNodeCount = 2
	}
	closeDataNodes := make([]func(), 0, dataNodeCount)
	for nodeIdx := 0; nodeIdx < dataNodeCount; nodeIdx++ {
		closeDataNodes = append(closeDataNodes, setup.DataNode(config, flags...))
	}
	setup.PreloadSchemaViaProperty(config, preloadTraceBenchSchema(cfg.ShardNum))
	config.AddLoadedKinds(schema.KindTrace)
	liaisonAddr, closeLiaison := setup.LiaisonNode(config, flags...)
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
	ns := timestamp.NowMilli().UnixNano()
	base := time.Unix(0, ns-ns%int64(time.Minute))
	closeFn := func() {
		if closeErr := conn.Close(); closeErr != nil {
			t.Logf("close soak connection: %v", closeErr)
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

// emitSoakResult writes the machine-readable soak JSON artifact to the report
// directory. The file is named soak-trace-vec-result.json and lives alongside
// the benchmark's report files so CI can gate on it independently.
func emitSoakResult(t *testing.T, cfg Config, result soakResult) {
	t.Helper()
	reportDir := cfg.ReportDir
	if reportDir == "" {
		reportDir = defaultReportDir
	}
	if mkdirErr := os.MkdirAll(reportDir, 0o755); mkdirErr != nil {
		t.Logf("[soak] could not create report dir %s: %v", reportDir, mkdirErr)
		return
	}
	outPath := filepath.Join(reportDir, "soak-trace-vec-result.json")
	body, marshalErr := json.MarshalIndent(result, "", "  ")
	if marshalErr != nil {
		t.Logf("[soak] could not marshal soak result: %v", marshalErr)
		return
	}
	if writeErr := os.WriteFile(outPath, body, 0o644); writeErr != nil {
		t.Logf("[soak] could not write soak result to %s: %v", outPath, writeErr)
		return
	}
	t.Logf("[soak] result written to %s", outPath)
}
