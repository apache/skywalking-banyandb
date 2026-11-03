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
	"fmt"
	"slices"
)

func qlbT15UnavailableMetrics() []string {
	return []string{
		"storage_physical_scanned_rows", "native_retained_frame_bytes", "total_parent_lease_charges",
		"per_tracker_peak_reserved_bytes", "operator_peak_retained_bytes",
	}
}

var qlbT15RequiredMetrics = []string{
	"benchmark_process_id", "warmup_queries", "checked_timed_samples", "fixture_input_rows", "completed_groups_per_query",
	"ql_batch_executions", "native_batch_executions", "native_leaf_calls", "native_source_rows", "native_frame_rows", "intermediate_data_points", "final_data_points",
	"output_rows",
	"process_peak_reserved_bytes", "tracker_lease_charged_bytes", "post_close_live_results_delta", "post_close_reserved_bytes_delta", "post_close_owner_pins_delta",
	"post_close_flatten_pool_refs_delta",
	"execution_clock_calls", "plan_cache_hits", "plan_cache_compiles", "aggregate_executions", "distinct_executions", "group_executions", "group_input_rows",
	"group_count", "group_capacity",
	"key_bytes", "key_capacity", "state_bytes", "state_capacity", "state_lanes", "group_reserved_bytes", "group_released_bytes", "group_live_bytes",
	"checked_response_bytes", "min_response_bytes", "max_response_bytes",
}

func validateQLBT15Metrics(m map[string]uint64, samples int, o *qlbT15Oracle) error {
	calls := uint64(samples)
	if m["ql_batch_executions"] != calls || m["native_batch_executions"] != 0 || m["native_leaf_calls"] != calls*2 ||
		m["group_executions"] != calls || m["group_input_rows"] != calls*uint64(o.inputRows) || m["group_count"] != calls*uint64(o.groups) ||
		m["final_data_points"] != calls*uint64(o.groups) || m["output_rows"] != calls*uint64(o.groups) || m["execution_clock_calls"] != calls ||
		m["plan_cache_hits"] != calls || m["plan_cache_compiles"] != 0 {
		return fmt.Errorf("T15 %s dispatch/group work differs", o.spec.name)
	}
	aggregate, distinct := uint64(0), calls
	if o.spec.reference {
		aggregate, distinct = calls, 0
	}
	if m["aggregate_executions"] != aggregate || m["distinct_executions"] != distinct {
		return fmt.Errorf("T15 wrong algorithm counters")
	}
	var keyBytes uint64
	for unique := 0; unique < o.groups; unique++ {
		a, b := qlbT15Tuple(unique, o.spec.wide)
		keyBytes += uint64(20 + len(a) + len(b))
	}
	if m["key_bytes"] != keyBytes*calls || m["key_capacity"] < m["key_bytes"] || m["group_capacity"] < calls*uint64(o.groups) {
		return fmt.Errorf("T15 %s retained key storage differs from unique tuple bytes", o.spec.name)
	}
	for _, key := range []string{
		"state_bytes", "state_capacity", "state_lanes", "group_live_bytes", "intermediate_data_points",
		"post_close_live_results_delta", "post_close_reserved_bytes_delta", "post_close_owner_pins_delta", "post_close_flatten_pool_refs_delta",
	} {
		if m[key] != 0 {
			return fmt.Errorf("T15 unexpected state or leak %s", key)
		}
	}
	if m["group_reserved_bytes"] != m["group_released_bytes"] || m["process_peak_reserved_bytes"] == 0 || m["process_peak_reserved_bytes"] > 3*256*(1<<20) ||
		m["tracker_lease_charged_bytes"] == 0 {
		return fmt.Errorf("T15 unbounded/unbalanced query reservations")
	}
	if o.inputRows > 0 && (m["native_source_rows"] == 0 || m["native_frame_rows"] == 0 || m["group_reserved_bytes"] == 0) {
		return fmt.Errorf("T15 missing actual input work")
	}
	return nil
}

func validateQLBT15Result(result Result) error {
	iterations, warmup, err := qlbT15Schedule(result.Cardinality)
	if err != nil || result.Scenario != ScenarioQLBT15 || result.Engine != engineMeasure || result.Mode != modeVec || result.Correctness != "pass" {
		return fmt.Errorf("invalid T15 report")
	}
	expected := map[string]qlbT15Spec{}
	for _, spec := range qlbT15Specs() {
		expected[spec.name] = spec
	}
	var pid uint64
	for _, v := range result.Variants {
		spec, ok := expected[v.Variant]
		if !ok {
			return fmt.Errorf("unexpected/duplicate T15 variant %s", v.Variant)
		}
		delete(expected, v.Variant)
		o := newQLBT15Oracle(spec, result.Cardinality)
		if err = validateQLBT15Variant(v, result.Cardinality, iterations, o); err != nil {
			return err
		}
		if err = validateQLBT15Evidence(v, warmup, pid, o); err != nil {
			return err
		}
		m := v.BatchMetrics
		pid = m["benchmark_process_id"]
		if err = validateQLBT14ResponseBytes(m); err != nil {
			return err
		}
		if err = validateQLBT15Metrics(m, iterations, o); err != nil {
			return err
		}
	}
	if len(expected) != 0 {
		return fmt.Errorf("T15 missing %d variants", len(expected))
	}
	return nil
}

func validateQLBT15Variant(v Result, cardinality, iterations int, o *qlbT15Oracle) error {
	if v.Scenario != ScenarioQLBT15 || v.Engine != engineMeasure || v.Mode != modeVec || v.Correctness != "pass" || v.Cardinality != cardinality ||
		v.QueryWorkers != 1 || v.QueryMemoryMiB != 256 || v.QueryIterations != iterations || v.TimedSamples != iterations || len(v.RawLatenciesNS) != iterations ||
		v.ExpectedRows != o.groups || v.ResponseRows != o.groups || v.ExpectedHash != o.hash || v.ApproxResultHash != o.hash ||
		v.QPS <= 0 || v.Latency.P50Ms <= 0 || v.Latency.P95Ms < v.Latency.P50Ms || v.Latency.P99Ms < v.Latency.P95Ms ||
		v.Profiles["cpu"] == "" || v.Profiles["heap"] == "" {
		return fmt.Errorf("T15 %s changed timing/result/profile protocol", o.spec.name)
	}
	for _, latency := range v.RawLatenciesNS {
		if latency <= 0 {
			return fmt.Errorf("T15 missing raw latency")
		}
	}
	return nil
}

func validateQLBT15Evidence(v Result, warmup int, pid uint64, o *qlbT15Oracle) error {
	m := v.BatchMetrics
	for _, key := range qlbT15RequiredMetrics {
		if _, ok := m[key]; !ok {
			return fmt.Errorf("T15 missing metric %s", key)
		}
	}
	for _, key := range qlbT15UnavailableMetrics() {
		if !slices.Contains(v.UnavailableMetrics, key) {
			return fmt.Errorf("T15 missing unavailable %s", key)
		}
		if _, ok := m[key]; ok {
			return fmt.Errorf("T15 unavailable %s measured", key)
		}
	}
	if m["benchmark_process_id"] == 0 || pid != 0 && pid != m["benchmark_process_id"] || m["warmup_queries"] != uint64(warmup) || m["checked_timed_samples"] != 6 ||
		m["fixture_input_rows"] != uint64(o.inputRows) || m["completed_groups_per_query"] != uint64(o.groups) {
		return fmt.Errorf("T15 missing metadata")
	}
	return nil
}

func validateQLBT15Matrix(results []Result) error {
	cards := map[int]bool{}
	pids := map[uint64]bool{}
	for _, r := range results {
		if r.Scenario != ScenarioQLBT15 {
			continue
		}
		if cards[r.Cardinality] {
			return fmt.Errorf("duplicate T15 cardinality")
		}
		if err := validateQLBT15Result(r); err != nil {
			return err
		}
		pid := r.Variants[0].BatchMetrics["benchmark_process_id"]
		if pids[pid] {
			return fmt.Errorf("T15 reused cluster process")
		}
		cards[r.Cardinality], pids[pid] = true, true
	}
	if len(cards) != 0 && (!cards[1024] || !cards[8192] || len(cards) != 2) {
		return fmt.Errorf("T15 requires both focused cardinalities")
	}
	return nil
}
