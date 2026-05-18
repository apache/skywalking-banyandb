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
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSummarizeLatencies(t *testing.T) {
	stats, qps := summarizeLatencies([]time.Duration{time.Millisecond, 3 * time.Millisecond, 2 * time.Millisecond}, 3*time.Second)
	if stats.P50Ms != 2 || stats.P99Ms != 3 || stats.MeanMs != 2 {
		t.Fatalf("unexpected latency stats: %+v", stats)
	}
	if qps != 1 {
		t.Fatalf("unexpected qps: %f", qps)
	}
}

func TestWriteReportFromShards(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{ReportDir: dir, QueryWorkers: 1, QueryIterations: 5, WarmupIterations: 0, Writers: 1}
	results := []Result{
		{Mode: modeRow, Scenario: ScenarioScanAll, Cardinality: 1024, ResponseRows: 1024, Latency: LatencyStats{P50Ms: 1}, Allocations: AllocationStats{MallocsPerQuery: 2}, QueryIterations: 5, QueryWorkers: 1},
		{Mode: modeVec, Scenario: ScenarioScanAll, Cardinality: 1024, ResponseRows: 1024, Latency: LatencyStats{P50Ms: 2}, Allocations: AllocationStats{MallocsPerQuery: 4}, QueryIterations: 5, QueryWorkers: 1, Correctness: "matched"},
	}
	report := newReportFromShards(cfg, results)
	if len(report.Config.Cardinalities) != 1 || report.Config.Cardinalities[0] != 1024 {
		t.Fatalf("expected cardinalities derived from shards, got %v", report.Config.Cardinalities)
	}
	if len(report.Config.Scenarios) != 1 || report.Config.Scenarios[0] != ScenarioScanAll {
		t.Fatalf("expected scenarios derived from shards, got %v", report.Config.Scenarios)
	}
	jsonPath, mdPath, writeErr := writeReport(report, dir)
	if writeErr != nil {
		t.Fatalf("writeReport() failed: %v", writeErr)
	}
	for _, path := range []string{jsonPath, mdPath} {
		if _, statErr := os.Stat(path); statErr != nil {
			t.Fatalf("expected report file %s: %v", path, statErr)
		}
	}
	body, readErr := os.ReadFile(filepath.Clean(mdPath))
	if readErr != nil {
		t.Fatalf("read markdown report: %v", readErr)
	}
	contents := string(body)
	if !strings.Contains(contents, "Distributed Query Benchmark") || !strings.Contains(contents, "scan_all") {
		t.Fatalf("markdown report missing expected content: %s", contents)
	}
	if !strings.Contains(contents, "Vec/Row Ratios") {
		t.Fatalf("markdown report missing ratio section: %s", contents)
	}
}

func TestShardRoundTrip(t *testing.T) {
	dir := t.TempDir()
	original := Result{
		Mode:             modeRow,
		Scenario:         ScenarioTopWithFilter,
		Cardinality:      10000,
		Entities:         100,
		PointsEach:       100,
		ResponseRows:     2,
		Correctness:      "baseline",
		QueryIterations:  5,
		QueryWorkers:     4,
		Latency:          LatencyStats{P50Ms: 12.5, P95Ms: 17.2},
		QPS:              81.0,
		Allocations:      AllocationStats{MallocsPerQuery: 123, AllocBytesPerQuery: 456},
		ApproxResultHash: 0xdeadbeef,
	}
	shardPath, writeErr := writeShard(original, dir)
	if writeErr != nil {
		t.Fatalf("writeShard() failed: %v", writeErr)
	}
	expectedName := "row_top_with_filter_10000.json"
	if filepath.Base(shardPath) != expectedName {
		t.Fatalf("unexpected shard filename: got %s, want %s", filepath.Base(shardPath), expectedName)
	}
	results, readErr := readShards(dir)
	if readErr != nil {
		t.Fatalf("readShards() failed: %v", readErr)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 shard, got %d", len(results))
	}
	r := results[0]
	if r.Mode != original.Mode || r.Scenario != original.Scenario || r.Cardinality != original.Cardinality ||
		r.ResponseRows != original.ResponseRows || r.ApproxResultHash != original.ApproxResultHash ||
		r.Latency.P50Ms != original.Latency.P50Ms || r.QPS != original.QPS {
		t.Fatalf("round-trip mismatch: got %+v want %+v", r, original)
	}
}
