//go:build querymetrics

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
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	bydbqlv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/bydbql/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	liaisongrpc "github.com/apache/skywalking-banyandb/banyand/liaison/grpc"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/pool"
	"github.com/apache/skywalking-banyandb/pkg/query/ql"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
)

var qlbT14ResourceStarted atomic.Bool

// TestQLBT14Resources is an opt-in capability probe, not a latency benchmark.
// Each rows/budget pair requires a fresh process so peak memory has one owner.
func TestQLBT14Resources(t *testing.T) {
	rowsText, budgetText := os.Getenv("DQB_T14_RESOURCE_ROWS"), os.Getenv("DQB_T14_RESOURCE_BUDGET_MIB")
	if rowsText == "" && budgetText == "" {
		t.Skip("set DQB_T14_RESOURCE_ROWS and DQB_T14_RESOURCE_BUDGET_MIB for the owning T14 resource probe")
	}
	cfg := LoadConfig()
	if !cfg.RunBench || !cfg.InContainer || cfg.Engine != engineMeasure {
		t.Fatal("T14 resource probe requires RUN_DISTRIBUTED_QUERY_BENCH=1, DQB_IN_CONTAINER=1 and the measure engine")
	}
	rows, rowsErr := strconv.Atoi(rowsText)
	budget, budgetErr := strconv.Atoi(budgetText)
	if rowsErr != nil || budgetErr != nil || (rows != 100000 && rows != 1000000) || (budget != 16 && budget != 256) {
		t.Fatal("T14 resource rows must be 100000 or 1000000 and budget must be 16 or 256 MiB")
	}
	if !qlbT14ResourceStarted.CompareAndSwap(false, true) {
		t.Fatal("T14 resource rows/budget pair requires a fresh test process")
	}
	cfg.Cardinality, cfg.QueryMemoryMiB = rows, budget
	cfg.Scenario, cfg.Mode, cfg.QueryWorkers = ScenarioQLBT14, modeVec, 1
	if err := logger.Init(logger.Logging{Env: "dev", Level: "info"}); err != nil {
		t.Fatal(err)
	}
	gomega.RegisterTestingT(t)
	report := map[string]any{
		"scenario": "qlb_t14_resources", "cardinality": rows, "query_memory_mib": budget, "process_id": os.Getpid(),
		"protocol":  "fresh process; full fixture visibility in <=8192-row entity windows; native/local grouped AVG HAVING50; five attempts per route",
		"readiness": "not_started", "readiness_rows_verified": 0, "readiness_queries": 0,
		"recovery": "not_run", "samples": []map[string]any{}, "throughput_evidence": false, "owning_probe_completed": false,
		"peak_scope":          "process-global sum across one liaison and two independent data-node trackers; not one tracker peak",
		"unavailable_metrics": []string{"per_tracker_peak_reserved_bytes", "native_retained_frame_bytes", "total_parent_lease_charges"},
	}
	defer saveQLBT14ResourceReport(t, cfg, report)
	cluster, base, err := startBenchCluster(t, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.close()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Minute)
	defer cancel()
	written, err := writeQLBT14Data(ctx, cluster.conn, cfg, base)
	if err != nil {
		report["readiness"], report["readiness_error"] = "write_failed", err.Error()
		t.Fatal(err)
	}
	report["written_rows"] = written.Rows
	client := bydbqlv1.NewBydbQLServiceClient(cluster.conn)
	verified, windows, readinessErr := verifyQLBT14ResourceFixture(ctx, client, rows, base)
	report["readiness_rows_verified"], report["readiness_queries"] = verified, windows
	if readinessErr != nil {
		report["readiness"], report["readiness_error"] = "blocked", readinessErr.Error()
		report["readiness_status"], report["recovery"] = status.Code(readinessErr).String(), "not_run_readiness_blocked"
		waitForQLBT02ResourceRelease(0, 0, 0, 0)
		report["readiness_cleanup"] = "pass"
		if cleanupErr := checkQLBT14ResourceCleanup(); cleanupErr != nil {
			report["readiness_cleanup"] = cleanupErr.Error()
			t.Error(cleanupErr)
		}
		report["readiness_memory"] = vectorized.QueryMemoryStats()
		report["readiness_live_resources"] = map[string]any{
			"having_bytes": ql.HavingExecutionStats().LiveBytes, "filter_bytes": ql.FilterExecutionStats().LiveBytes,
			"aggregate_bytes": ql.AggregateExecutionStats().LiveBytes, "merge_bytes": ql.NativeAggregateExecutionStats().LiveBytes,
			"native_results": vmeasure.BatchExecutionStats().LiveResults, "owner_pins": vectorized.OutstandingOwnerPins(),
			"flatten_refs": pool.AllRefsCount()["native-query-codec-flatten"],
		}
		if status.Code(readinessErr) != codes.ResourceExhausted {
			t.Fatal(readinessErr)
		}
		t.Logf("T14 prerequisite blocked after %d verified rows; no owning throughput measured: %v", verified, readinessErr)
		return
	}
	report["readiness"] = "pass"
	samples := make([]map[string]any, 0, 10)
	for _, mode := range []string{"native", "local"} {
		oracle := newQLBT14Oracle(qlbT14Spec{name: mode + "_p50", mode: mode, percent: 50}, rows)
		var branches atomic.Uint64
		restore := liaisongrpc.SetNativeAggregateHooksForTest(liaisongrpc.NativeAggregateTestHooks{
			Disabled: mode == "local",
			BeforeBranch: func(context.Context, int, *measurev1.QueryRequest) error {
				branches.Add(1)
				return nil
			},
		})
		for iteration := 0; iteration < 5; iteration++ {
			startedBranches := branches.Load()
			sample, probeErr := probeQLBT14Resource(ctx, client, cfg, base, oracle)
			sample["route"], sample["iteration"] = mode, iteration
			sample["native_aggregate_branches_started"] = branches.Load() - startedBranches
			if probeErr == nil && (mode == "local" && branches.Load() != 0 ||
				sample["status"] == codes.OK.String() && mode == "native" && branches.Load()-startedBranches != 2) {
				probeErr = fmt.Errorf("T14 resource request started unexpected native aggregate branches")
			}
			samples = append(samples, sample)
			report["samples"] = samples
			if probeErr != nil {
				restore()
				report["probe_error"] = probeErr.Error()
				t.Fatal(probeErr)
			}
		}
		restore()
	}
	request, expected := qlbT07RawVisibilityChunk(0, min(rows, 8192), 0, base)
	if err = waitForQLBT07RawChunk(ctx, client, request, expected); err != nil {
		report["recovery"], report["recovery_error"] = "failed", err.Error()
		t.Fatal(err)
	}
	waitForQLBT02ResourceRelease(0, 0, 0, 0)
	if err = checkQLBT14ResourceCleanup(); err != nil {
		t.Fatal(err)
	}
	report["recovery"], report["owning_probe_completed"] = "pass", true
}

func verifyQLBT14ResourceFixture(ctx context.Context, client bydbqlv1.BydbQLServiceClient, rows int, base time.Time) (int, int, error) {
	verified, queries := 0, 0
	for first := 0; first < rows; first += 8192 * 16 {
		for host := 0; host < 16; host++ {
			request, expected := qlbT07RawVisibilityChunk(first, min(first+8192*16, rows), host, base)
			if len(expected.DataPoints) == 0 {
				continue
			}
			queries++
			if err := waitForQLBT07RawChunk(ctx, client, request, expected); err != nil {
				return verified, queries, err
			}
			verified += len(expected.DataPoints)
		}
	}
	return verified, queries, nil
}

func probeQLBT14Resource(ctx context.Context, client bydbqlv1.BydbQLServiceClient, cfg Config, base time.Time, oracle *qlbT14Oracle) (map[string]any, error) {
	sample := map[string]any{
		"expected_input_rows": oracle.inputRows, "expected_completed_groups": oracle.groups,
		"expected_output_rows": len(oracle.expected.DataPoints), "expected_hash": oracle.hash, "correctness": "not_checked",
	}
	before, err := qlbT14MetricsStart()
	if err != nil {
		return sample, err
	}
	processBefore := captureProcessSnapshot()
	started := time.Now()
	queryCtx, cancel := context.WithTimeout(ctx, queryTimeout(cfg.Cardinality))
	response, queryErr := client.Query(queryCtx, oracle.query(cfg.Cardinality, base))
	cancel()
	sample["elapsed_ns"], sample["status"] = time.Since(started), status.Code(queryErr).String()
	after := captureProcessSnapshot()
	sample["resources"], sample["allocations"] = resourceDelta(processBefore, after), allocationDelta(processBefore, after, 1)
	if queryErr != nil {
		sample["error"] = queryErr.Error()
	}
	metrics, metricsErr := qlbT14ResourceMetrics(before, cfg.QueryMemoryMiB)
	sample["metrics"] = metrics
	if metricsErr != nil {
		return sample, metricsErr
	}
	switch status.Code(queryErr) {
	case codes.OK:
		if err = oracle.check(response.GetMeasureResult()); err != nil {
			return sample, err
		}
		sample["returned_rows"], sample["actual_hash"] = len(response.GetMeasureResult().DataPoints), qlbT01Hash(response.GetMeasureResult())
		sample["correctness"] = "pass"
		if err = validateQLBT14Stages(metrics, 1, oracle); err != nil {
			return sample, err
		}
		validate := validateQLBT14NativeWork
		if oracle.spec.mode == "local" {
			validate = validateQLBT14LocalWork
		}
		if err = validate(metrics, 1, oracle); err != nil {
			return sample, err
		}
		expectedCalls := uint64(1)
		if oracle.spec.mode == "native" {
			expectedCalls = 2
		}
		if metrics["ql_batch_executions"] != expectedCalls || metrics["native_leaf_calls"] != expectedCalls*2 ||
			metrics["final_data_points"] != uint64(len(oracle.expected.DataPoints)) {
			return sample, fmt.Errorf("T14 successful resource request has unexpected dispatch/egress work")
		}
	case codes.ResourceExhausted:
		if response.GetMeasureResult() != nil {
			return sample, fmt.Errorf("T14 resource rejection returned partial output")
		}
		sample["correctness"] = "protected_rejection_no_partial_result"
		sample["returned_rows"] = 0
	default:
		return sample, fmt.Errorf("T14 resource query returned unexpected status: %w", queryErr)
	}
	if oracle.spec.mode == "native" && (metrics["aggregate_executions"] != 0 || metrics["aggregate_reserved_bytes"] != 0) ||
		oracle.spec.mode == "local" && (metrics["merge_executions"] != 0 || metrics["merge_reserved_bytes"] != 0) {
		return sample, fmt.Errorf("T14 resource request activated the wrong aggregate route")
	}
	if metrics["native_batch_executions"] != 0 || metrics["intermediate_data_points"] != 0 || metrics["filter_input_rows"] != 0 {
		return sample, fmt.Errorf("T14 resource request used unexpected raw/protobuf predicate work")
	}
	return sample, nil
}

func qlbT14ResourceMetrics(before qlbT14MetricsSnapshot, budgetMiB int) (map[string]uint64, error) {
	baseline := before.base.local.native
	waitForQLBT02ResourceRelease(baseline.execution.LiveResults, baseline.memory.LiveReservedBytes, baseline.owners, baseline.flatten)
	cleanupErr := checkQLBT14ResourceCleanup()
	execution, memory := vmeasure.BatchExecutionStats(), vectorized.QueryMemoryStats()
	aggregate, merge := ql.AggregateExecutionStats(), ql.NativeAggregateExecutionStats()
	metrics := map[string]uint64{
		"ql_batch_executions":      execution.QLBatchExecutions - baseline.execution.QLBatchExecutions,
		"native_batch_executions":  execution.NativeBatchExecutions - baseline.execution.NativeBatchExecutions,
		"native_leaf_calls":        execution.NativeLeafCalls - baseline.execution.NativeLeafCalls,
		"native_source_rows":       execution.NativeInputRows - baseline.execution.NativeInputRows,
		"native_frame_rows":        execution.NativeFrameRows - baseline.execution.NativeFrameRows,
		"intermediate_data_points": execution.IntermediateDataPoints - baseline.execution.IntermediateDataPoints,
		"final_data_points":        execution.FinalDataPoints - baseline.execution.FinalDataPoints,
		"aggregate_executions":     aggregate.Executions - before.base.local.aggregate.Executions,
		"aggregate_input_rows":     aggregate.InputRows - before.base.local.aggregate.InputRows,
		"aggregate_groups":         aggregate.Groups - before.base.local.aggregate.Groups,
		"aggregate_state_lanes":    aggregate.StateLanes - before.base.local.aggregate.StateLanes,
		"aggregate_state_bytes":    aggregate.StateBytes - before.base.local.aggregate.StateBytes,
		"aggregate_reserved_bytes": aggregate.ReservedBytes - before.base.local.aggregate.ReservedBytes,
		"aggregate_released_bytes": aggregate.ReleasedBytes - before.base.local.aggregate.ReleasedBytes,
		"merge_executions":         merge.Executions - before.base.merge.Executions, "merge_branches": merge.Branches - before.base.merge.Branches,
		"merge_input_rows": merge.InputRows - before.base.merge.InputRows, "merge_groups": merge.Groups - before.base.merge.Groups,
		"merge_state_lanes": merge.StateLanes - before.base.merge.StateLanes, "merge_state_bytes": merge.StateBytes - before.base.merge.StateBytes,
		"merge_visits":         merge.MergeVisits - before.base.merge.MergeVisits,
		"merge_reserved_bytes": merge.ReservedBytes - before.base.merge.ReservedBytes, "merge_released_bytes": merge.ReleasedBytes - before.base.merge.ReleasedBytes,
		"process_peak_reserved_bytes": uint64(memory.PeakReservedBytes), "per_tracker_budget_bytes": uint64(budgetMiB) << 20,
		"tracker_lease_charged_bytes": memory.LeaseChargedBytes - baseline.memory.LeaseChargedBytes,
		"post_close_reserved_bytes":   uint64(memory.LiveReservedBytes), "post_close_native_live_results": uint64(execution.LiveResults),
		"post_close_owner_pins": uint64(vectorized.OutstandingOwnerPins()), "post_close_flatten_refs": uint64(pool.AllRefsCount()["native-query-codec-flatten"]),
		"aggregate_live_bytes": uint64(aggregate.LiveBytes), "merge_live_bytes": uint64(merge.LiveBytes),
	}
	qlbT14FilterMetrics(metrics, "having", before.having, ql.HavingExecutionStats())
	qlbT14FilterMetrics(metrics, "filter", before.filter, ql.FilterExecutionStats())
	cache := liaisongrpc.BydbQLPlanCacheStats()
	metrics["plan_cache_hits"], metrics["plan_cache_compiles"] = cache.Hits-before.base.local.cache.Hits, cache.Compiles-before.base.local.cache.Compiles
	if cleanupErr != nil {
		return metrics, cleanupErr
	}
	for _, prefix := range []string{"aggregate", "merge", "having", "filter"} {
		if metrics[prefix+"_reserved_bytes"] != metrics[prefix+"_released_bytes"] {
			return metrics, fmt.Errorf("T14 resource unbalanced %s reservations", prefix)
		}
	}
	if memory.PeakReservedBytes > 3*(int64(budgetMiB)<<20) {
		return metrics, fmt.Errorf("T14 resource peak exceeds three independent tracker budgets")
	}
	return metrics, nil
}

func checkQLBT14ResourceCleanup() error {
	if ql.FilterExecutionStats().LiveBytes != 0 || ql.HavingExecutionStats().LiveBytes != 0 || ql.AggregateExecutionStats().LiveBytes != 0 ||
		ql.NativeAggregateExecutionStats().LiveBytes != 0 || vmeasure.BatchExecutionStats().LiveResults != 0 ||
		vectorized.QueryMemoryStats().LiveReservedBytes != 0 || vectorized.OutstandingOwnerPins() != 0 || pool.AllRefsCount()["native-query-codec-flatten"] != 0 {
		return fmt.Errorf("T14 resource query leaked operator/native reservations, results, owners or flatten buffers")
	}
	return nil
}

func saveQLBT14ResourceReport(t *testing.T, cfg Config, report map[string]any) {
	t.Helper()
	body, err := json.MarshalIndent(report, "", "  ")
	if err == nil {
		err = os.MkdirAll(cfg.ReportDir, 0o755)
	}
	if err != nil {
		t.Error(err)
		return
	}
	path := filepath.Join(cfg.ReportDir, fmt.Sprintf("qlb_t14_resources_%drows_%dmib.json", cfg.Cardinality, cfg.QueryMemoryMiB))
	if err = os.WriteFile(path, append(body, '\n'), 0o600); err != nil {
		t.Error(err)
		return
	}
	t.Logf("T14 resource artifact: %s", path)
}
