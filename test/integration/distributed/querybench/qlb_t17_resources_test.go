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
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// TestQLBT17Admission checks the fixed sort pair and the owning composition at
// 1/8/32 workers before any timed protocol is frozen. It never increases budgets
// or retries a known native materialization failure as a sort-performance sample.
func TestQLBT17Admission(t *testing.T) {
	text := os.Getenv("DQB_T17_ADMISSION_ROWS")
	if text == "" {
		t.Skip("set DQB_T17_ADMISSION_ROWS for an untimed admission check")
	}
	rows, err := strconv.Atoi(text)
	cfg := LoadConfig()
	if err != nil || (rows != 1024 && rows != 32768) || !cfg.RunBench || !cfg.InContainer || cfg.Engine != engineMeasure {
		t.Fatal("T17 admission requires the container measure engine and 1024 or 32768 rows")
	}
	cfg.Cardinality, cfg.QueryMemoryMiB, cfg.Scenario, cfg.Mode = rows, 256, ScenarioQLBT17, modeVec
	_, cfg.WarmupIterations, _ = qlbT17Schedule(rows)
	if err = logger.Init(logger.Logging{Env: "dev", Level: "info"}); err != nil {
		t.Fatal(err)
	}
	gomega.RegisterTestingT(t)
	report := map[string]any{
		"scenario": "qlb_t17_admission", "cardinality": rows, "query_memory_mib": 256, "process_id": os.Getpid(), "timed": false,
		"correctness": "incomplete", "protocol": "identical finite pages; raw fullsort/topk; one changing-bind composition workers1/8/32",
		"unavailable_metrics": qlbT17UnavailableMetrics(),
	}
	defer saveQLBT17Admission(t, cfg, report)
	cluster, base, err := startBenchCluster(t, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.close()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Minute)
	defer cancel()
	if _, err = writeQLBT17Data(ctx, cluster.conn, cfg, base); err != nil {
		t.Fatal(err)
	}
	client := bydbqlv1.NewBydbQLServiceClient(cluster.conn)
	if err = waitForQLBT17Visibility(ctx, client, rows, base); err != nil {
		report["readiness_error"], report["correctness"] = err.Error(), "blocked"
		t.Fatal(err)
	}
	report["readiness"] = "pass"
	variants := map[string]map[string]uint64{}
	for _, spec := range qlbT17Specs() {
		if rows == 32768 && spec.workers == 32 && os.Getenv("DQB_T17_ADMISSION_SKIP_32") == "1" {
			report["known_blocked_variant"] = "composed_workers_32"
			continue
		}
		metrics, checkErr := checkQLBT17Admission(ctx, client, spec, rows, base)
		if checkErr != nil {
			report["failed_metrics"] = metrics
			report["failed_variant"], report["query_error"], report["query_status"], report["correctness"] = spec.name, checkErr.Error(), status.Code(checkErr).String(), "blocked"
			report["cleanup_error"] = checkQLBT16ResourceCleanup()
			if status.Code(checkErr) == codes.ResourceExhausted {
				report["blocking_boundary"] = "native materialization/admission or QL operator; current tracker error does not identify ownership"
			}
			t.Fatal(checkErr)
		}
		variants[spec.name] = metrics
		report["variants"] = variants
	}
	report["correctness"] = "pass"
	if report["known_blocked_variant"] != nil {
		report["correctness"] = "partial_known_32_blocked"
	}
}

func checkQLBT17Admission(ctx context.Context, client bydbqlv1.BydbQLServiceClient, spec qlbT17Spec, rows int, base time.Time) (map[string]uint64, error) {
	restore := qlbT17SetFullSort(spec.full)
	defer restore()
	oracles := qlbT17Oracles(spec, rows, base)
	query := func(index int) (*bydbqlv1.QueryResponse, error) {
		oracle := oracles[index%len(oracles)]
		response, err := client.Query(ctx, oracle.query(base))
		if err != nil {
			return nil, err
		}
		if err = oracle.check(response.GetMeasureResult()); err != nil {
			return nil, err
		}
		return response, nil
	}
	for binding := range oracles {
		if _, err := query(binding); err != nil {
			return nil, err
		}
	}
	before, err := qlbT17MetricsStart()
	if err != nil {
		return nil, err
	}
	samples := 1
	if spec.composed {
		samples = 3 * spec.workers
	}
	latencies := make([]time.Duration, samples)
	retained := make([]*bydbqlv1.QueryResponse, samples)
	if err = runQLBT17Queries(query, spec, oracles, latencies, retained); err != nil {
		return nil, err
	}
	metrics, _, err := qlbT17MetricsFinish(before, spec, samples, oracles)
	return metrics, err
}

func saveQLBT17Admission(t *testing.T, cfg Config, report map[string]any) {
	t.Helper()
	if err := os.MkdirAll(cfg.ReportDir, 0o755); err != nil {
		t.Error(err)
		return
	}
	body, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Error(err)
		return
	}
	path := filepath.Join(cfg.ReportDir, fmt.Sprintf("t17-admission-%d.json", cfg.Cardinality))
	if err = os.WriteFile(path, append(body, '\n'), 0o600); err != nil {
		t.Error(err)
	}
	t.Logf("T17 admission report: %s", path)
}

// TestQLBT17ConcurrencyControl compares the same 32-worker composition with the
// forced full-sort reference after top-k admission has already failed. It records
// native and operator work even on rejection; it is never throughput evidence.
func TestQLBT17ConcurrencyControl(t *testing.T) {
	if os.Getenv("DQB_T17_CONCURRENCY_CONTROL") != "1" {
		t.Skip("set DQB_T17_CONCURRENCY_CONTROL=1 for the bounded native-admission diagnostic")
	}
	cfg := LoadConfig()
	if !cfg.RunBench || !cfg.InContainer || cfg.Engine != engineMeasure {
		t.Fatal("T17 control requires the container measure engine")
	}
	cfg.Cardinality, cfg.QueryMemoryMiB, cfg.Scenario, cfg.Mode = 1024, 256, ScenarioQLBT17, modeVec
	if err := logger.Init(logger.Logging{Env: "dev", Level: "info"}); err != nil {
		t.Fatal(err)
	}
	gomega.RegisterTestingT(t)
	report := map[string]any{
		"scenario": "qlb_t17_concurrency_control", "cardinality": 1024, "query_memory_mib": 256, "workers": 32, "timed": false,
		"mode": "forced_fullsort", "protocol": "same composed SQL/bind sets as the rejected32-worker top-k; one96-query control",
		"correctness": "incomplete", "throughput_evidence": false,
	}
	defer saveQLBT17Control(t, cfg, report)
	cluster, base, err := startBenchCluster(t, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.close()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Minute)
	defer cancel()
	if _, err = writeQLBT17Data(ctx, cluster.conn, cfg, base); err != nil {
		t.Fatal(err)
	}
	client := bydbqlv1.NewBydbQLServiceClient(cluster.conn)
	if err = waitForQLBT17Visibility(ctx, client, 1024, base); err != nil {
		t.Fatal(err)
	}
	spec := qlbT17Spec{name: "composed_workers_32_fullsort_control", workers: 32, full: true, composed: true}
	restore := qlbT17SetFullSort(true)
	defer restore()
	oracles := qlbT17Oracles(spec, 1024, base)
	var current, peak, completed atomic.Int64
	query := func(index int) (*bydbqlv1.QueryResponse, error) {
		inFlight := current.Add(1)
		for previous := peak.Load(); inFlight > previous; previous = peak.Load() {
			if peak.CompareAndSwap(previous, inFlight) {
				break
			}
		}
		defer current.Add(-1)
		oracle := oracles[index%len(oracles)]
		response, queryErr := client.Query(ctx, oracle.query(base))
		if queryErr != nil {
			return nil, queryErr
		}
		if checkErr := oracle.check(response.GetMeasureResult()); checkErr != nil {
			return nil, checkErr
		}
		completed.Add(1)
		return response, nil
	}
	for binding := range oracles {
		if _, err = query(binding); err != nil {
			t.Fatal(err)
		}
	}
	peak.Store(0)
	completed.Store(0)
	before, err := qlbT17MetricsStart()
	if err != nil {
		t.Fatal(err)
	}
	latencies := make([]time.Duration, 96)
	retained := make([]*bydbqlv1.QueryResponse, 96)
	runErr := runQLBT17Queries(query, spec, oracles, latencies, retained)
	metrics, _, metricsErr := qlbT17MetricsFinish(before, spec, 96, oracles)
	report["metrics"], report["completed_queries"], report["client_peak_inflight"] = metrics, completed.Load(), peak.Load()
	if cleanupErr := checkQLBT16ResourceCleanup(); cleanupErr != nil {
		t.Fatal(cleanupErr)
	}
	report["cleanup"] = "pass"
	if runErr != nil {
		report["correctness"], report["query_error"], report["query_status"] = "blocked", runErr.Error(), status.Code(runErr).String()
		if metricsErr != nil {
			report["incomplete_work_error"] = metricsErr.Error()
		}
		if status.Code(runErr) != codes.ResourceExhausted {
			t.Fatal(runErr)
		}
		return
	}
	if metricsErr != nil {
		t.Fatal(metricsErr)
	}
	report["correctness"] = "pass"
}

func saveQLBT17Control(t *testing.T, cfg Config, report map[string]any) {
	t.Helper()
	cfg.ReportDir = filepath.Join(cfg.ReportDir, "fullsort32-control")
	saveQLBT17Admission(t, cfg, report)
}
