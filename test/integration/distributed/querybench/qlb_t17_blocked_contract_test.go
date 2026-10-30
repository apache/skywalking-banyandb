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
	"encoding/json"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func qlbT17BlockedFixture(t *testing.T, mutate func(map[string]any)) string {
	t.Helper()
	metrics := map[string]uint64{}
	for _, key := range []string{
		"sort_live_bytes", "aggregate_live_bytes", "filter_live_bytes", "having_live_bytes", "post_close_live_results_delta",
		"post_close_reserved_bytes_delta", "post_close_owner_pins_delta", "post_close_flatten_pool_refs_delta",
	} {
		metrics[key] = 0
	}
	body := map[string]any{
		"scenario": "qlb_t17_concurrency_control", "cardinality": 1024, "query_memory_mib": 256, "workers": 32, "client_peak_inflight": 32,
		"mode": "forced_fullsort", "timed": false, "correctness": "blocked", "query_status": "ResourceExhausted", "cleanup": "pass", "metrics": metrics,
		"query_error": "rpc error: code = ResourceExhausted desc = fail to execute the query plan for measure qlb_t17: " +
			"vec distributed plan: broadcast: query memory pool exhausted",
	}
	if mutate != nil {
		mutate(body)
	}
	encoded, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "blocked32.json")
	if err = os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestQLBT17BlockedReferenceRequiresOwningFailureAndCleanup(t *testing.T) {
	for _, mutate := range []func(map[string]any){
		func(m map[string]any) { m["cardinality"] = 32768 },
		func(m map[string]any) { m["query_status"] = "InvalidArgument" },
		func(m map[string]any) { m["query_error"] = "SQL parse error" },
		func(m map[string]any) { m["timed"] = true },
		func(m map[string]any) { delete(m, "timed") },
		func(m map[string]any) { m["cleanup"] = "fail" },
		func(m map[string]any) { m["metrics"].(map[string]uint64)["sort_live_bytes"] = 1 },
		func(m map[string]any) { delete(m["metrics"].(map[string]uint64), "post_close_owner_pins_delta") },
	} {
		if _, _, err := readQLBT17Blocked32(qlbT17BlockedFixture(t, mutate)); err == nil {
			t.Fatal("T17 accepted fabricated/incomplete native admission failure evidence")
		}
	}
	if _, _, err := readQLBT17Blocked32(filepath.Join(t.TempDir(), "missing.json")); err == nil {
		t.Fatal("T17 accepted missing failure artifact")
	}
}

func TestQLBT17PartialReportPreservesBlocked32WithoutMeasurements(t *testing.T) {
	path := qlbT17BlockedFixture(t, nil)
	blocked, err := newQLBT17Blocked32(Config{Mode: modeVec, Cardinality: 1024}, path)
	if err != nil {
		t.Fatal(err)
	}
	if err = validateQLBT17Blocked32(blocked, 1024); err != nil {
		t.Fatal(err)
	}
	result := qlbT17FixtureReport(1024, 1)
	result.Correctness = "partial_known_32_blocked"
	result.Variants[4] = blocked
	if err = validateQLBT17Result(result); err != nil {
		t.Fatal(err)
	}
	markdown := renderMarkdown(newReportFromShards(Config{Engine: engineMeasure}, result.Variants))
	if !strings.Contains(markdown, "Acceptance remains incomplete") || !strings.Contains(markdown, "(blocked)") || !strings.Contains(markdown, "N/A") {
		t.Fatal("partial Markdown must expose the blocked gate and absent measurements")
	}
	strict := result
	strict.Correctness = "pass"
	if validateQLBT17Result(strict) == nil {
		t.Fatal("a blocked32 reference cannot satisfy strict acceptance")
	}
	for _, mutate := range []func(*Result){
		func(v *Result) { v.EvidenceArtifacts["blocked_32_report"] = "missing.json" },
		func(v *Result) { v.EvidenceArtifacts["blocked_32_sha256"] = "fabricated" },
		func(v *Result) { v.Error = "different error" },
		func(v *Result) { v.RawLatenciesNS = []time.Duration{time.Nanosecond} },
		func(v *Result) { v.TimedSamples = 1 },
		func(v *Result) { v.Latency.P50Ms = 1 },
		func(v *Result) { v.Profiles = map[string]string{"cpu": "fabricated.pprof"} },
		func(v *Result) { v.Allocations.AllocBytesPerQuery = 1 },
		func(v *Result) { v.BatchMetrics["sort_executions"] = 1 },
	} {
		changed := blocked
		changed.EvidenceArtifacts = maps.Clone(blocked.EvidenceArtifacts)
		changed.BatchMetrics = maps.Clone(blocked.BatchMetrics)
		mutate(&changed)
		if validateQLBT17Blocked32(changed, 1024) == nil {
			t.Fatal("blocked32 accepted fabricated timing/throughput or altered evidence")
		}
	}
}
