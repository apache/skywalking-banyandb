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
	"testing"
	"time"

	"github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	bydbqlv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/bydbql/v1"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

type qlbT17LargeCall struct {
	Cleanup  map[string]int64 `json:"cleanup"`
	Recovery *qlbT17LargeCall `json:"recovery,omitempty"`
	Variant  string           `json:"variant"`
	Status   string           `json:"status"`
	Error    string           `json:"error,omitempty"`
	Binding  int              `json:"binding"`
	Rows     int              `json:"rows"`
	Hash     uint64           `json:"ordered_hash"`
	Exact    bool             `json:"exact"`
}

// TestQLBT17LargeRPC measures feasibility separately from the frozen timing matrix.
// Resource rejection is recorded as unavailable performance evidence, never timed.
func TestQLBT17LargeRPC(t *testing.T) {
	text := os.Getenv("DQB_T17_LARGE_ROWS")
	if text == "" {
		t.Skip("set DQB_T17_LARGE_ROWS=100000 or 1000000 for untimed RPC feasibility")
	}
	rows, err := strconv.Atoi(text)
	cfg := LoadConfig()
	if err != nil || (rows != 100000 && rows != 1000000) || !cfg.RunBench || !cfg.InContainer || cfg.Engine != engineMeasure {
		t.Fatal("T17 large RPC requires container measure engine and 100000 or 1000000 rows")
	}
	cfg.Cardinality, cfg.QueryMemoryMiB, cfg.Scenario, cfg.Mode = rows, 256, ScenarioQLBT17, modeVec
	var limits qlbT17OverloadEvidence
	if err = qlbT17OverloadLimits(&limits); err != nil {
		t.Fatal(err)
	}
	if err = logger.Init(logger.Logging{Env: "dev", Level: "info"}); err != nil {
		t.Fatal(err)
	}
	gomega.RegisterTestingT(t)
	report := map[string]any{
		"scenario": "qlb_t17_large_rpc", "cardinality": rows, "query_memory_mib": 256,
		"cpu_quota": limits.CPUQuota, "cpu_period": limits.CPUPeriod,
		"memory_limit": limits.MemoryLimit, "swap_limit": limits.SwapLimit,
		"timed": false, "workers": 1, "status": "incomplete", "process_id": os.Getpid(),
	}
	defer saveQLBT17LargeRPC(t, cfg, report)
	cluster, base, err := startBenchCluster(t, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.close()
	report["fixture_base_unix_nanos"] = base.UnixNano()
	ctx, cancel := context.WithTimeout(t.Context(), 40*time.Minute)
	defer cancel()
	if _, err = writeQLBT17Data(ctx, cluster.conn, cfg, base); err != nil {
		report["write_error"] = err.Error()
		t.Fatal(err)
	}
	client := bydbqlv1.NewBydbQLServiceClient(cluster.conn)
	if err = waitForQLBT17Visibility(ctx, client, rows, base); err != nil {
		report["readiness_error"] = err.Error()
		t.Fatal(err)
	}
	report["readiness"] = "pass"
	calls := make([]qlbT17LargeCall, 0, 5)
	rejected := 0
	for _, spec := range qlbT17PerformanceSpecs()[:3] {
		cohort, cohortErr := qlbT17LargeCohort(ctx, client, spec, rows, base)
		calls = append(calls, cohort...)
		report["calls"] = calls
		if cohortErr != nil {
			report["error"] = cohortErr.Error()
			t.Fatal(cohortErr)
		}
		for _, call := range cohort {
			if call.Status == codes.ResourceExhausted.String() {
				rejected++
			}
		}
	}
	report["rejected"] = rejected
	report["status"] = "all_queries_exact"
	if rejected > 0 {
		report["status"] = "resource_limited_with_recovery"
	}
}

func qlbT17LargeCohort(ctx context.Context, client bydbqlv1.BydbQLServiceClient,
	spec qlbT17Spec, rows int, base time.Time,
) ([]qlbT17LargeCall, error) {
	restore := qlbT17SetFullSort(spec.full)
	defer restore()
	oracles := qlbT17Oracles(spec, rows, base)
	calls := make([]qlbT17LargeCall, 0, len(oracles))
	for _, oracle := range oracles {
		call, err := qlbT17LargeRPC(ctx, client, oracle, base)
		if err == nil {
			// A small bounded time range on the same fixture verifies recovery without
			// recreating the cluster or changing the sort hook, pool or tracker budget.
			recovery, recoveryErr := qlbT17LargeRPC(ctx, client, newQLBT17Oracle(spec, 1024, oracle.binding, base), base)
			call.Recovery = &recovery
			if recoveryErr != nil {
				err = recoveryErr
			} else if recovery.Status != codes.OK.String() {
				err = fmt.Errorf("T17 large RPC recovery rejected: %s", recovery.Error)
			}
		}
		calls = append(calls, call)
		if err != nil {
			return calls, err
		}
	}
	return calls, nil
}

func qlbT17LargeRPC(ctx context.Context, client bydbqlv1.BydbQLServiceClient,
	oracle *qlbT17Oracle, base time.Time,
) (qlbT17LargeCall, error) {
	queryCtx, cancel := context.WithTimeout(ctx, queryTimeout(oracle.rows))
	defer cancel()
	response, queryErr := client.Query(queryCtx, oracle.query(base))
	call, err := qlbT17LargeOutcome(oracle, response, queryErr)
	var cleanupErr error
	call.Cleanup, cleanupErr = qlbT17OverloadCleanup()
	if cleanupErr != nil {
		return call, cleanupErr
	}
	return call, err
}

func qlbT17LargeOutcome(oracle *qlbT17Oracle, response *bydbqlv1.QueryResponse, queryErr error) (qlbT17LargeCall, error) {
	call := qlbT17LargeCall{Variant: oracle.spec.name, Binding: oracle.binding, Status: status.Code(queryErr).String()}
	if queryErr != nil {
		call.Error = queryErr.Error()
		if response.GetMeasureResult() != nil {
			return call, fmt.Errorf("T17 rejected RPC returned a partial measure result")
		}
		if status.Code(queryErr) != codes.ResourceExhausted {
			return call, queryErr
		}
		return call, nil
	}
	if err := oracle.check(response.GetMeasureResult()); err != nil {
		call.Error = err.Error()
		return call, err
	}
	call.Exact, call.Rows, call.Hash = true, len(response.GetMeasureResult().DataPoints), qlbT17OrderedHash(response.GetMeasureResult())
	return call, nil
}

func saveQLBT17LargeRPC(t *testing.T, cfg Config, report map[string]any) {
	t.Helper()
	body, err := json.MarshalIndent(report, "", "  ")
	if err == nil {
		err = os.MkdirAll(cfg.ReportDir, 0o755)
	}
	path := filepath.Join(cfg.ReportDir, fmt.Sprintf("t17-large-rpc-%d.json", cfg.Cardinality))
	if err == nil {
		err = os.WriteFile(path, append(body, '\n'), 0o600)
	}
	if err != nil {
		t.Error(err)
	}
	t.Logf("T17 large RPC evidence: %s", path)
}

func TestQLBT17LargeOutcome(t *testing.T) {
	oracle := newQLBT17Oracle(qlbT17Spec{name: "topk"}, 16, 0, time.Unix(0, 0))
	exact := &bydbqlv1.QueryResponse{Result: &bydbqlv1.QueryResponse_MeasureResult{MeasureResult: oracle.expected}}
	for _, tc := range []struct {
		queryErr error
		response *bydbqlv1.QueryResponse
		name     string
		wantErr  bool
	}{
		{name: "exact", response: exact},
		{name: "bounded_rejection", queryErr: status.Error(codes.ResourceExhausted, "memory limit")},
		{name: "partial_rejection", response: exact, queryErr: status.Error(codes.ResourceExhausted, "memory limit"), wantErr: true},
		{name: "deadline", queryErr: status.Error(codes.DeadlineExceeded, "timeout"), wantErr: true},
		{name: "missing_result", wantErr: true},
		{name: "wrong_result", response: &bydbqlv1.QueryResponse{Result: &bydbqlv1.QueryResponse_MeasureResult{}}, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			call, err := qlbT17LargeOutcome(oracle, tc.response, tc.queryErr)
			if (err != nil) != tc.wantErr {
				t.Fatalf("outcome error = %v, want error %v", err, tc.wantErr)
			}
			if !tc.wantErr && call.Exact != (tc.queryErr == nil) {
				t.Fatalf("incorrect success classification: %+v", call)
			}
		})
	}
}
