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
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/onsi/gomega"

	bydbqlv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/bydbql/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

var qlbT15MemoryStarted atomic.Bool

// TestQLBT15MemoryReproduction isolates the original 16384-row wide input failure.
// Run in a fresh 4-CPU, 8-GiB container with memory-swap also limited to 8 GiB.
func TestQLBT15MemoryReproduction(t *testing.T) {
	if os.Getenv("DQB_T15_MEMORY_REPRO") != "1" {
		t.Skip("set DQB_T15_MEMORY_REPRO=1 for the bounded wide-input regression")
	}
	cfg := LoadConfig()
	if !cfg.RunBench || !cfg.InContainer || cfg.Engine != engineMeasure || cfg.QueryMemoryMiB != 256 {
		t.Fatal("T15 memory reproduction requires RUN_DISTRIBUTED_QUERY_BENCH=1, DQB_IN_CONTAINER=1, measure engine and256 MiB")
	}
	if !qlbT15MemoryStarted.CompareAndSwap(false, true) {
		t.Fatal("T15 memory reproduction requires a fresh process")
	}
	const rows = 16384
	cfg.Cardinality, cfg.Scenario, cfg.Mode = rows, ScenarioQLBT15, modeVec
	if err := logger.Init(logger.Logging{Env: "dev", Level: "info"}); err != nil {
		t.Fatal(err)
	}
	gomega.RegisterTestingT(t)
	report := map[string]any{
		"scenario": "qlb_t15_memory_reproduction", "rows": rows, "query_memory_mib": 256,
		"readiness_rows_verified": 0, "readiness_queries": 0, "recovery": "not_run", "samples": []map[string]any{},
	}
	defer saveQLBT15MemoryReport(t, cfg.ReportDir, report)
	cluster, base, err := startBenchCluster(t, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.close()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	spec := qlbT15Spec{name: "distinct_p001_wide", percent: 1, wide: true}
	if err = writeQLBT15Range(ctx, measurev1.NewMeasureServiceClient(cluster.conn), 0, rows, rows, spec, base); err != nil {
		t.Fatal(err)
	}
	client := bydbqlv1.NewBydbQLServiceClient(cluster.conn)
	verified := 0
	for host := range 16 {
		request, expected := qlbT15RawChunk(0, rows, host, rows, spec, base)
		if err = waitForQLBT07RawChunk(ctx, client, request, expected); err != nil {
			t.Fatal(err)
		}
		verified += len(expected.DataPoints)
		report["readiness_rows_verified"], report["readiness_queries"] = verified, host+1
	}
	if verified != rows {
		t.Fatalf("verified %d raw rows, want %d", verified, rows)
	}
	waitForQLBT02ResourceRelease(0, 0, 0, 0)
	if err = checkQLBT15ResourceCleanup(); err != nil {
		t.Fatal(err)
	}
	report["readiness"] = "pass"
	samples := []map[string]any{}
	for _, reference := range []bool{false, true} {
		spec.reference = reference
		if reference {
			spec.name = "group_by_p001_wide"
		}
		oracle := newQLBT15Oracle(spec, rows)
		sample, probeErr := probeQLBT15Resource(ctx, client, cfg, base, oracle)
		sample["variant"] = spec.name
		samples = append(samples, sample)
		report["samples"] = samples
		if probeErr != nil {
			t.Errorf("%s: %v", spec.name, probeErr)
			continue
		}
		if sample["status"] != "OK" {
			t.Errorf("%s failed: %v", spec.name, sample)
			continue
		}
		for key, want := range map[string]uint64{"group_executions": 1, "group_input_rows": rows, "groups": 163, "key_bytes": 46944, "state_lanes": 0, "state_bytes": 0} {
			if sample[key] != want {
				t.Errorf("%s %s = %v, want %d", spec.name, key, sample[key], want)
			}
		}
	}
	request, expected := qlbT15RawChunk(0, 16, 0, rows, spec, base)
	if err = waitForQLBT07RawChunk(ctx, client, request, expected); err != nil {
		t.Fatal(err)
	}
	waitForQLBT02ResourceRelease(0, 0, 0, 0)
	if err = checkQLBT15ResourceCleanup(); err != nil {
		t.Fatal(err)
	}
	report["recovery"] = "pass"
}

func saveQLBT15MemoryReport(t *testing.T, directory string, report map[string]any) {
	t.Helper()
	body, err := json.MarshalIndent(report, "", "  ")
	if err == nil {
		err = os.MkdirAll(directory, 0o755)
	}
	if err != nil {
		t.Error(err)
		return
	}
	path := filepath.Join(directory, "qlb_t15_memory_16384rows_256mib.json")
	if err = os.WriteFile(path, append(body, '\n'), 0o600); err != nil {
		t.Error(err)
		return
	}
	t.Logf("T15 memory reproduction artifact:%s", path)
	t.Logf("T15 memory reproduction:%s", body)
}
