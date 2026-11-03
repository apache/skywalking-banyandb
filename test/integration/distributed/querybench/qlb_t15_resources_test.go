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
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/pool"
	"github.com/apache/skywalking-banyandb/pkg/query/ql"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
)

var qlbT15ResourceStarted atomic.Bool

// TestQLBT15Resources probes one owning dataset in a fresh rows/budget process.
// Rejections and prerequisite blockers never count as successful throughput.
func TestQLBT15Resources(t *testing.T) {
	rowsText, budgetText := os.Getenv("DQB_T15_RESOURCE_ROWS"), os.Getenv("DQB_T15_RESOURCE_BUDGET_MIB")
	if rowsText == "" && budgetText == "" {
		t.Skip("set DQB_T15_RESOURCE_ROWS and DQB_T15_RESOURCE_BUDGET_MIB")
	}
	cfg := LoadConfig()
	rows, rowsErr := strconv.Atoi(rowsText)
	budget, budgetErr := strconv.Atoi(budgetText)
	if !cfg.RunBench || !cfg.InContainer || cfg.Engine != engineMeasure || rowsErr != nil || budgetErr != nil ||
		(rows != 100000 && rows != 1000000) || (budget != 16 && budget != 256) {
		t.Fatal("T15 resource probe requires container measure engine,100000/1000000 rows and16/256 MiB")
	}
	if !qlbT15ResourceStarted.CompareAndSwap(false, true) {
		t.Fatal("T15 resource pair requires fresh process")
	}
	cfg.Cardinality, cfg.QueryMemoryMiB, cfg.Scenario, cfg.Mode = rows, budget, ScenarioQLBT15, modeVec
	if err := logger.Init(logger.Logging{Env: "dev", Level: "info"}); err != nil {
		t.Fatal(err)
	}
	gomega.RegisterTestingT(t)
	report := map[string]any{
		"scenario": "qlb_t15_resources", "cardinality": rows, "query_memory_mib": budget, "process_id": os.Getpid(),
		"protocol":  "one100% unique narrow tuple dataset; bounded1024-row QL readiness; five attempts per DISTINCT/GROUP BY route",
		"readiness": "not_started", "readiness_rows_verified": 0, "readiness_queries": 0, "recovery": "not_run",
		"throughput_evidence": false, "owning_probe_completed": false, "samples": []map[string]any{},
		"failure_stage":       "unavailable: underlying tracker errors and global counters do not distinguish native decoder versus local table ownership",
		"peak_scope":          "process-wide inclusive sum of one liaison and two independent data-node trackers",
		"unavailable_metrics": qlbT15UnavailableMetrics(),
	}
	defer saveQLBT15ResourceReport(t, cfg, report)
	cluster, base, err := startBenchCluster(t, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.close()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Minute)
	defer cancel()
	spec := qlbT15Spec{name: "distinct_p100_narrow", percent: 100}
	if err = writeQLBT15Range(ctx, measurev1.NewMeasureServiceClient(cluster.conn), 0, rows, rows, spec, base); err != nil {
		report["readiness_error"] = err.Error()
		t.Fatal(err)
	}
	report["written_rows"] = rows
	client := bydbqlv1.NewBydbQLServiceClient(cluster.conn)
	verified, queries := 0, 0
	for first := 0; first < rows; first += 16384 {
		for host := 0; host < 16; host++ {
			request, expected := qlbT15RawChunk(first, min(first+16384, rows), host, rows, spec, base)
			if len(expected.DataPoints) == 0 {
				continue
			}
			queries++
			err = waitForQLBT07RawChunk(ctx, client, request, expected)
			if err != nil {
				break
			}
			verified += len(expected.DataPoints)
		}
		if err != nil {
			break
		}
	}
	report["readiness_rows_verified"], report["readiness_queries"] = verified, queries
	if err != nil {
		report["readiness"], report["readiness_error"], report["readiness_status"] = "blocked", err.Error(), status.Code(err).String()
		waitForQLBT02ResourceRelease(0, 0, 0, 0)
		report["readiness_memory"] = vectorized.QueryMemoryStats()
		report["readiness_cleanup"] = "pass"
		if cleanupErr := checkQLBT15ResourceCleanup(); cleanupErr != nil {
			report["readiness_cleanup"] = cleanupErr.Error()
			t.Error(cleanupErr)
		}
		report["recovery"] = "not_run_readiness_blocked"
		if status.Code(err) != codes.ResourceExhausted {
			t.Fatal(err)
		}
		t.Logf("T15 prerequisite blocked after%d checked rows:%v", verified, err)
		return
	}
	report["readiness"] = "pass"
	samples := []map[string]any{}
	for _, reference := range []bool{false, true} {
		spec.reference = reference
		if reference {
			spec.name = "group_by_p100_narrow"
		}
		// Build the full independent result only after a successful RPC, so a
		// native-stage rejection does not allocate a million-row client oracle.
		oracle := &qlbT15Oracle{spec: spec, inputRows: rows, groups: rows}
		for iteration := 0; iteration < 5; iteration++ {
			sample, probeErr := probeQLBT15Resource(ctx, client, cfg, base, oracle)
			sample["iteration"], sample["variant"] = iteration, spec.name
			samples = append(samples, sample)
			report["samples"] = samples
			if probeErr != nil {
				report["probe_error"] = probeErr.Error()
				t.Fatal(probeErr)
			}
		}
	}
	request, expected := qlbT15RawChunk(0, min(rows, 16384), 0, rows, spec, base)
	if err = waitForQLBT07RawChunk(ctx, client, request, expected); err != nil {
		report["recovery_error"] = err.Error()
		t.Fatal(err)
	}
	waitForQLBT02ResourceRelease(0, 0, 0, 0)
	if err = checkQLBT15ResourceCleanup(); err != nil {
		t.Fatal(err)
	}
	report["recovery"], report["owning_probe_completed"] = "pass", true
}

func probeQLBT15Resource(ctx context.Context, client bydbqlv1.BydbQLServiceClient, cfg Config, base time.Time, o *qlbT15Oracle) (map[string]any, error) {
	sample := map[string]any{"expected_rows": o.groups, "expected_hash": "unavailable_until_success", "correctness": "not_checked"}
	before, err := qlbT15MetricsStart()
	if err != nil {
		return sample, err
	}
	processBefore := captureProcessSnapshot()
	started := time.Now()
	queryCtx, cancel := context.WithTimeout(ctx, queryTimeout(cfg.Cardinality))
	response, queryErr := client.Query(queryCtx, o.query(cfg.Cardinality, base))
	cancel()
	sample["elapsed_ns"], sample["status"] = time.Since(started), status.Code(queryErr).String()
	after := captureProcessSnapshot()
	sample["resources"], sample["allocations"] = resourceDelta(processBefore, after), allocationDelta(processBefore, after, 1)
	waitForQLBT02ResourceRelease(0, 0, 0, 0)
	native, memory := vmeasure.BatchExecutionStats(), vectorized.QueryMemoryStats()
	aggregate, distinct := ql.AggregateExecutionStats(), ql.DistinctExecutionStats()
	group, baseline := aggregate, before.aggregate
	if !o.spec.reference {
		group, baseline = ql.AggregateExecutionCounters(distinct), ql.AggregateExecutionCounters(before.distinct)
	}
	sample["native_leaf_calls"] = native.NativeLeafCalls - before.native.execution.NativeLeafCalls
	sample["native_source_rows"] = native.NativeInputRows - before.native.execution.NativeInputRows
	sample["native_frame_rows"] = native.NativeFrameRows - before.native.execution.NativeFrameRows
	sample["group_executions"] = group.Executions - baseline.Executions
	sample["group_input_rows"], sample["groups"] = group.InputRows-baseline.InputRows, group.Groups-baseline.Groups
	sample["key_bytes"], sample["key_capacity"] = group.KeyBytes-baseline.KeyBytes, group.KeyCapacity-baseline.KeyCapacity
	sample["state_lanes"], sample["state_bytes"] = group.StateLanes-baseline.StateLanes, group.StateBytes-baseline.StateBytes
	sample["group_reserved_bytes"], sample["group_released_bytes"] = group.ReservedBytes-baseline.ReservedBytes, group.ReleasedBytes-baseline.ReleasedBytes
	sample["tracker_lease_charged_bytes"] = memory.LeaseChargedBytes - before.native.memory.LeaseChargedBytes
	sample["memory"] = memory
	sample["post_close_live_results"], sample["post_close_owner_pins"] = native.LiveResults, vectorized.OutstandingOwnerPins()
	sample["post_close_flatten_refs"] = pool.AllRefsCount()["native-query-codec-flatten"]
	sample["post_close_group_live_bytes"] = group.LiveBytes
	if err = checkQLBT15ResourceCleanup(); err != nil {
		return sample, err
	}
	if memory.PeakReservedBytes > 3*(int64(cfg.QueryMemoryMiB)<<20) || group.ReservedBytes-baseline.ReservedBytes != group.ReleasedBytes-baseline.ReleasedBytes ||
		group.StateLanes != baseline.StateLanes || group.StateBytes != baseline.StateBytes {
		return sample, fmt.Errorf("T15 resource bounds/state invariant failed")
	}
	switch status.Code(queryErr) {
	case codes.OK:
		o = newQLBT15Oracle(o.spec, cfg.Cardinality)
		sample["expected_hash"] = o.hash
		if err = o.check(response.GetMeasureResult()); err != nil {
			return sample, err
		}
		if group.InputRows-baseline.InputRows != uint64(o.inputRows) || group.Groups-baseline.Groups != uint64(o.groups) {
			return sample, fmt.Errorf("T15 resource success omitted input/groups")
		}
		sample["correctness"], sample["actual_hash"], sample["returned_rows"] = "pass", qlbT01Hash(response.GetMeasureResult()), len(response.GetMeasureResult().DataPoints)
	case codes.ResourceExhausted:
		if response.GetMeasureResult() != nil {
			return sample, fmt.Errorf("T15 protected rejection exposed partial result")
		}
		sample["correctness"], sample["error"], sample["failure_stage"] = "protected_rejection_no_partial_result", queryErr.Error(), "unavailable"
	default:
		return sample, fmt.Errorf("T15 unexpected resource status:%w", queryErr)
	}
	return sample, nil
}

func checkQLBT15ResourceCleanup() error {
	if ql.DistinctExecutionStats().LiveBytes != 0 {
		return fmt.Errorf("T15 leaked distinct reservations")
	}
	return checkQLBT14ResourceCleanup()
}

func saveQLBT15ResourceReport(t *testing.T, cfg Config, report map[string]any) {
	t.Helper()
	body, err := json.MarshalIndent(report, "", "  ")
	if err == nil {
		err = os.MkdirAll(cfg.ReportDir, 0o755)
	}
	if err != nil {
		t.Error(err)
		return
	}
	path := filepath.Join(cfg.ReportDir, fmt.Sprintf("qlb_t15_resources_%drows_%dmib.json", cfg.Cardinality, cfg.QueryMemoryMiB))
	if err = os.WriteFile(path, append(body, '\n'), 0o600); err != nil {
		t.Error(err)
		return
	}
	t.Logf("T15 resource artifact:%s", path)
}
