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

var qlbT16ClusterStarted atomic.Bool

// TestQLBT16Admission checks the complete fixed control matrix without collecting
// timings. Run it before freezing the successful-query cardinality schedule.
func TestQLBT16Admission(t *testing.T) {
	text := os.Getenv("DQB_T16_ADMISSION_ROWS")
	if text == "" {
		t.Skip("set DQB_T16_ADMISSION_ROWS for an untimed admission check")
	}
	rows, err := strconv.Atoi(text)
	cfg := LoadConfig()
	if err != nil || (rows != 1024 && rows != 32768) || !cfg.RunBench || !cfg.InContainer || cfg.Engine != engineMeasure {
		t.Fatal("T16 admission requires the container measure engine and 1024 or 32768 rows")
	}
	cfg.Cardinality, cfg.QueryMemoryMiB, cfg.Scenario, cfg.Mode = rows, 256, ScenarioQLBT16, modeVec
	initializeQLBT16ClusterTest(t)
	report := map[string]any{"scenario": "qlb_t16_admission", "cardinality": rows, "process_id": os.Getpid(), "timed": false, "correctness": "incomplete"}
	defer saveQLBT16ProbeReport(t, cfg, "admission", report)
	cluster, base, err := startBenchCluster(t, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.close()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Minute)
	defer cancel()
	if _, err = writeQLBT16Data(ctx, cluster.conn, cfg, base); err != nil {
		t.Fatal(err)
	}
	client := bydbqlv1.NewBydbQLServiceClient(cluster.conn)
	if err = waitForQLBT16Visibility(ctx, client, rows, base); err != nil {
		report["readiness_error"] = err.Error()
		t.Fatal(err)
	}
	report["readiness"] = "pass"
	variants := map[string]map[string]uint64{}
	for _, spec := range qlbT16Specs() {
		o := newQLBT16Oracle(spec, rows)
		request := o.query(rows, base)
		response, queryErr := client.Query(ctx, request)
		if queryErr != nil {
			report["failed_variant"], report["query_error"] = spec.name, queryErr.Error()
			t.Fatal(queryErr)
		}
		if err = o.check(response.GetMeasureResult()); err != nil {
			t.Fatal(err)
		}
		before, startErr := qlbT16MetricsStart()
		if startErr != nil {
			t.Fatal(startErr)
		}
		response, queryErr = client.Query(ctx, request)
		if queryErr != nil {
			t.Fatal(queryErr)
		}
		if err = o.check(response.GetMeasureResult()); err != nil {
			t.Fatal(err)
		}
		metrics, _, finishErr := qlbT16MetricsFinish(before, 1, o)
		if finishErr != nil {
			t.Fatal(finishErr)
		}
		variants[spec.name] = metrics
		report["variants"] = variants
	}
	for _, level := range []string{"low", "high"} {
		for _, key := range qlbT16EquivalentWork {
			if variants["computed_explicit_"+level][key] != variants["computed_ordinal_"+level][key] {
				t.Fatalf("T16 %s explicit/ordinal work differs: %s", level, key)
			}
		}
	}
	report["correctness"] = "pass"
}

// TestQLBT16Resources measures the owning computed high-cardinality grouping
// request at one bounded rows/budget pair. Rejections are not throughput results.
func TestQLBT16Resources(t *testing.T) {
	rowsText, budgetText := os.Getenv("DQB_T16_RESOURCE_ROWS"), os.Getenv("DQB_T16_RESOURCE_BUDGET_MIB")
	if rowsText == "" && budgetText == "" {
		t.Skip("set DQB_T16_RESOURCE_ROWS and DQB_T16_RESOURCE_BUDGET_MIB")
	}
	rows, rowsErr := strconv.Atoi(rowsText)
	budget, budgetErr := strconv.Atoi(budgetText)
	cfg := LoadConfig()
	if !cfg.RunBench || !cfg.InContainer || cfg.Engine != engineMeasure || rowsErr != nil || budgetErr != nil ||
		(rows != 100000 && rows != 1000000) || (budget != 16 && budget != 256) {
		t.Fatal("T16 resource probe requires container measure engine, 100000/1000000 rows and 16/256 MiB")
	}
	cfg.Cardinality, cfg.QueryMemoryMiB, cfg.Scenario, cfg.Mode = rows, budget, ScenarioQLBT16, modeVec
	initializeQLBT16ClusterTest(t)
	report := map[string]any{
		"scenario": "qlb_t16_resources", "cardinality": rows, "query_memory_mib": budget, "process_id": os.Getpid(),
		"protocol":  "one high-cardinality computed-key dataset; bounded 1024-row QL readiness; five owning requests",
		"readiness": "not_started", "readiness_rows_verified": 0, "readiness_queries": 0, "recovery": "not_run",
		"throughput_evidence": false, "owning_probe_completed": false, "samples": []map[string]any{},
		"failure_stage":       "unavailable: tracker errors do not distinguish native decoder and local operator ownership",
		"peak_scope":          "process-wide sum of one liaison and two independent data-node trackers",
		"unavailable_metrics": qlbT16UnavailableMetrics(),
	}
	defer saveQLBT16ProbeReport(t, cfg, "resources", report)
	cluster, base, err := startBenchCluster(t, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.close()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Minute)
	defer cancel()
	spec := qlbT16Spec{name: "computed_explicit_high", high: true, computed: true}
	if err = writeQLBT16Range(ctx, measurev1.NewMeasureServiceClient(cluster.conn), 0, rows, rows, spec, base); err != nil {
		report["readiness_error"] = err.Error()
		t.Fatal(err)
	}
	report["written_rows"] = rows
	client := bydbqlv1.NewBydbQLServiceClient(cluster.conn)
	verified, queries := 0, 0
	for first := 0; first < rows; first += 16384 {
		for host := 0; host < 16; host++ {
			request, expected := qlbT16RawChunk(first, min(first+16384, rows), host, rows, spec, base)
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
		if cleanupErr := checkQLBT16ResourceCleanup(); cleanupErr != nil {
			t.Fatal(cleanupErr)
		}
		report["readiness_cleanup"], report["recovery"] = "pass", "not_run_readiness_blocked"
		if status.Code(err) != codes.ResourceExhausted {
			t.Fatal(err)
		}
		t.Logf("T16 prerequisite blocked after %d checked rows: %v", verified, err)
		return
	}
	report["readiness"] = "pass"
	samples := []map[string]any{}
	for iteration := 0; iteration < 5; iteration++ {
		sample, probeErr := probeQLBT16Resource(ctx, client, cfg, base, spec)
		sample["iteration"] = iteration
		samples = append(samples, sample)
		report["samples"] = samples
		if probeErr != nil {
			report["probe_error"] = probeErr.Error()
			t.Fatal(probeErr)
		}
	}
	request, expected := qlbT16RawChunk(0, min(rows, 16384), 0, rows, spec, base)
	if err = waitForQLBT07RawChunk(ctx, client, request, expected); err != nil {
		report["recovery_error"] = err.Error()
		t.Fatal(err)
	}
	waitForQLBT02ResourceRelease(0, 0, 0, 0)
	if err = checkQLBT16ResourceCleanup(); err != nil {
		t.Fatal(err)
	}
	report["recovery"], report["owning_probe_completed"] = "pass", true
}

func initializeQLBT16ClusterTest(t *testing.T) {
	t.Helper()
	if !qlbT16ClusterStarted.CompareAndSwap(false, true) {
		t.Fatal("T16 probe requires a fresh process")
	}
	if err := logger.Init(logger.Logging{Env: "dev", Level: "info"}); err != nil {
		t.Fatal(err)
	}
	gomega.RegisterTestingT(t)
}

func probeQLBT16Resource(ctx context.Context, client bydbqlv1.BydbQLServiceClient, cfg Config, base time.Time, spec qlbT16Spec) (map[string]any, error) {
	o := &qlbT16Oracle{spec: spec, inputRows: cfg.Cardinality, groups: spec.groupCount(cfg.Cardinality)}
	sample := map[string]any{"variant": spec.name, "expected_rows": o.groups, "expected_hash": "unavailable_until_success", "correctness": "not_checked"}
	before, err := qlbT16MetricsStart()
	if err != nil {
		return sample, err
	}
	request := o.query(cfg.Cardinality, base)
	processBefore := captureProcessSnapshot()
	started := time.Now()
	queryCtx, cancel := context.WithTimeout(ctx, queryTimeout(cfg.Cardinality))
	response, queryErr := client.Query(queryCtx, request)
	cancel()
	sample["elapsed_ns"], sample["status"] = time.Since(started), status.Code(queryErr).String()
	after := captureProcessSnapshot()
	sample["resources"], sample["allocations"] = resourceDelta(processBefore, after), allocationDelta(processBefore, after, 1)
	waitForQLBT02ResourceRelease(0, 0, 0, 0)
	native, memory, aggregate := vmeasure.BatchExecutionStats(), vectorized.QueryMemoryStats(), ql.AggregateExecutionStats()
	m := map[string]uint64{
		"native_leaf_calls":           native.NativeLeafCalls - before.native.execution.NativeLeafCalls,
		"native_source_rows":          native.NativeInputRows - before.native.execution.NativeInputRows,
		"native_frame_rows":           native.NativeFrameRows - before.native.execution.NativeFrameRows,
		"aggregate_executions":        aggregate.Executions - before.aggregate.Executions,
		"aggregate_input_rows":        aggregate.InputRows - before.aggregate.InputRows,
		"aggregate_groups":            aggregate.Groups - before.aggregate.Groups,
		"aggregate_key_bytes":         aggregate.KeyBytes - before.aggregate.KeyBytes,
		"aggregate_key_capacity":      aggregate.KeyCapacity - before.aggregate.KeyCapacity,
		"aggregate_state_lanes":       aggregate.StateLanes - before.aggregate.StateLanes,
		"aggregate_state_bytes":       aggregate.StateBytes - before.aggregate.StateBytes,
		"aggregate_reserved_bytes":    aggregate.ReservedBytes - before.aggregate.ReservedBytes,
		"aggregate_released_bytes":    aggregate.ReleasedBytes - before.aggregate.ReleasedBytes,
		"tracker_lease_charged_bytes": memory.LeaseChargedBytes - before.native.memory.LeaseChargedBytes,
		"process_peak_reserved_bytes": uint64(memory.PeakReservedBytes),
		"post_close_reserved_bytes":   uint64(memory.LiveReservedBytes), "post_close_native_live_results": uint64(native.LiveResults),
		"post_close_owner_pins": uint64(vectorized.OutstandingOwnerPins()), "post_close_flatten_refs": uint64(pool.AllRefsCount()["native-query-codec-flatten"]),
		"aggregate_live_bytes": uint64(aggregate.LiveBytes),
	}
	qlbT16ExpressionDelta(m, before.expression, ql.GroupExpressionExecutionStats())
	sample["batch_metrics"], sample["memory"] = m, memory
	if err = checkQLBT16ResourceCleanup(); err != nil {
		return sample, err
	}
	if memory.PeakReservedBytes > 3*(int64(cfg.QueryMemoryMiB)<<20) || m["aggregate_reserved_bytes"] != m["aggregate_released_bytes"] ||
		m["group_expression_reserved_bytes"] != m["group_expression_released_bytes"] {
		return sample, fmt.Errorf("T16 resource reservation bounds or releases differ")
	}
	switch status.Code(queryErr) {
	case codes.OK:
		// Construct the potentially large independent result only on success.
		o = newQLBT16Oracle(spec, cfg.Cardinality)
		if err = o.check(response.GetMeasureResult()); err != nil {
			return sample, err
		}
		if m["aggregate_input_rows"] != uint64(o.inputRows) || m["aggregate_groups"] != uint64(o.groups) || m["group_key_evaluations"] != uint64(o.inputRows) {
			return sample, fmt.Errorf("T16 resource success omitted grouping input or evaluated duplicate keys")
		}
		sample["correctness"], sample["actual_hash"], sample["expected_hash"] = "pass", qlbT01Hash(response.GetMeasureResult()), o.hash
		sample["returned_rows"] = len(response.GetMeasureResult().DataPoints)
	case codes.ResourceExhausted:
		if response.GetMeasureResult() != nil {
			return sample, fmt.Errorf("T16 protected rejection exposed a partial result")
		}
		sample["correctness"], sample["error"], sample["failure_stage"] = "protected_rejection_no_partial_result", queryErr.Error(), "unavailable"
	default:
		return sample, fmt.Errorf("T16 unexpected resource status: %w", queryErr)
	}
	return sample, nil
}

func checkQLBT16ResourceCleanup() error {
	if ql.GroupExpressionExecutionStats().LiveBytes != 0 {
		return fmt.Errorf("T16 leaked computed group expression reservations")
	}
	return checkQLBT14ResourceCleanup()
}

func saveQLBT16ProbeReport(t *testing.T, cfg Config, kind string, report map[string]any) {
	t.Helper()
	body, err := json.MarshalIndent(report, "", "  ")
	if err == nil {
		err = os.MkdirAll(cfg.ReportDir, 0o755)
	}
	if err != nil {
		t.Error(err)
		return
	}
	path := filepath.Join(cfg.ReportDir, fmt.Sprintf("qlb_t16_%s_%drows_%dmib.json", kind, cfg.Cardinality, cfg.QueryMemoryMiB))
	if err = os.WriteFile(path, append(body, '\n'), 0o600); err != nil {
		t.Error(err)
		return
	}
	t.Logf("T16 %s artifact: %s", kind, path)
}
