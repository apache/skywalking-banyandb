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
	"math/bits"
	"slices"
	"time"
)

func qlbT17UnavailableMetrics() []string {
	return []string{
		"storage_physical_scanned_rows", "native_retained_frame_bytes", "total_parent_lease_charges",
		"per_tracker_peak_reserved_bytes", "aggregate_peak_retained_bytes", "planning_compile_savings_ns",
	}
}

var qlbT17RequiredMetrics = []string{
	"benchmark_process_id", "warmup_queries", "warmup_plan_compiles", "checked_timed_samples", "fixture_input_rows", "fixture_base_unix_nanos", "bind_sets",
	"ql_batch_executions", "native_batch_executions", "native_leaf_calls", "native_source_rows",
	"native_frame_rows", "intermediate_data_points", "final_data_points", "output_rows",
	"checked_response_bytes", "min_response_bytes", "max_response_bytes", "execution_clock_calls", "plan_cache_hits", "plan_cache_compiles", "plan_cache_bypass",
	"plan_cache_entries", "plan_cache_entry_bytes", "plan_cache_retained_bytes",
	"plan_cache_inflight", "plan_cache_compile_reserved_bytes", "plan_cache_waiters", "plan_cache_uncached_bytes",
	"process_peak_reserved_bytes", "tracker_lease_charged_bytes", "post_close_live_results_delta",
	"post_close_reserved_bytes_delta", "post_close_owner_pins_delta", "post_close_flatten_pool_refs_delta",
	"aggregate_executions", "aggregate_input_rows", "aggregate_groups", "aggregate_state_lanes",
	"aggregate_state_bytes", "aggregate_reserved_bytes", "aggregate_released_bytes", "aggregate_live_bytes",
	"sort_executions", "sort_batches", "sort_input_rows", "sort_output_rows", "sort_key_evaluations",
	"sort_key_root_calls", "sort_key_kernel_rows", "sort_key_scalar_evaluations", "sort_comparisons", "sort_accepted_rows",
	"sort_evicted_rows", "sort_candidates", "sort_peak_candidates", "sort_candidate_capacity", "sort_payload_bytes",
	"sort_payload_capacity", "sort_peak_payload_bytes", "sort_descriptor_bytes", "sort_peak_bytes", "sort_reserved_bytes",
	"sort_released_bytes", "sort_source_pins", "sort_k", "sort_live_bytes",
}

func qlbT17MetricKeys() []string {
	keys := slices.Clone(qlbT17RequiredMetrics)
	for _, prefix := range []string{"filter", "having"} {
		for _, suffix := range qlbT14FilterMetricKeys() {
			keys = append(keys, prefix+"_"+suffix)
		}
	}
	return keys
}

func validateQLBT17Metrics(m map[string]uint64, spec qlbT17Spec, samples int, oracles []*qlbT17Oracle) error {
	calls := uint64(samples)
	var scanned, passing, groups, sortRows, outputs uint64
	for index := 0; index < samples; index++ {
		oracle := oracles[index%len(oracles)]
		scanned += uint64(oracle.scanned)
		passing += uint64(oracle.passing)
		groups += uint64(oracle.groups)
		sortRows += uint64(oracle.sortRows)
		outputs += uint64(len(oracle.expected.DataPoints))
	}
	if err := validateQLBT17Pipeline(m, spec, calls, scanned, outputs); err != nil {
		return err
	}
	if err := validateQLBT17SortWork(m, spec, calls, sortRows, outputs, oracles[0].rows); err != nil {
		return err
	}
	if spec.composed {
		// Nullable SUM owns value/count lanes, MAX owns one value lane. The
		// repeated SUM in HAVING must reuse these three physical lanes.
		if m["filter_executions"] != calls || m["filter_input_rows"] != scanned || m["filter_output_rows"] != passing ||
			m["aggregate_executions"] != calls || m["aggregate_input_rows"] != passing || m["aggregate_groups"] != groups ||
			m["aggregate_state_lanes"] != 3*calls || m["aggregate_state_bytes"] != 24*groups || m["having_executions"] != calls ||
			m["having_input_rows"] != groups || m["having_output_rows"] != sortRows {
			return fmt.Errorf("T17 composition work differs: filter=(%d,%d,%d), aggregate=(%d,%d,%d,%d), HAVING=(%d,%d,%d); "+
				"want calls=%d scanned=%d passing=%d groups=%d sort=%d",
				m["filter_executions"], m["filter_input_rows"], m["filter_output_rows"], m["aggregate_executions"], m["aggregate_input_rows"], m["aggregate_groups"],
				m["aggregate_state_lanes"],
				m["having_executions"], m["having_input_rows"], m["having_output_rows"], calls, scanned, passing, groups, sortRows)
		}
	} else if m["filter_executions"] != 0 || m["having_executions"] != 0 || m["aggregate_executions"] != 0 {
		return fmt.Errorf("T17 raw sort unexpectedly activated filtering or aggregation")
	}
	return validateQLBT17Cleanup(m, spec)
}

func validateQLBT17Pipeline(m map[string]uint64, spec qlbT17Spec, calls, scanned, outputs uint64) error {
	if m["ql_batch_executions"] != calls || m["native_batch_executions"] != 0 || m["native_leaf_calls"] != 2*calls ||
		m["native_source_rows"] < scanned || m["native_frame_rows"] < scanned || m["intermediate_data_points"] != 0 ||
		m["final_data_points"] != outputs || m["output_rows"] != outputs || m["execution_clock_calls"] != calls ||
		m["plan_cache_hits"] != calls || m["plan_cache_compiles"] != 0 || m["plan_cache_bypass"] != 0 {
		return fmt.Errorf("T17 %s did not reuse compilation and perform the complete native QL pipeline", spec.name)
	}
	return nil
}

func validateQLBT17SortWork(m map[string]uint64, spec qlbT17Spec, calls, sortRows, outputs uint64, inputRows int) error {
	keys, k := uint64(3), uint64(15)
	kernelRows := sortRows
	if spec.composed {
		keys, k, kernelRows = 2, 2, 0
	}
	if m["sort_executions"] != calls || m["sort_input_rows"] != sortRows || m["sort_output_rows"] != outputs || m["sort_batches"] < calls ||
		m["sort_key_evaluations"] != keys*sortRows || m["sort_key_root_calls"] != keys*m["sort_batches"] ||
		m["sort_key_kernel_rows"] != kernelRows || m["sort_key_scalar_evaluations"] != 0 || m["sort_comparisons"] == 0 || m["sort_k"] != k*calls {
		return fmt.Errorf("T17 %s actual key-root/kernel/order work differs from independent fixture arithmetic", spec.name)
	}
	return validateQLBT17CandidateWork(m, spec, calls, sortRows, inputRows)
}

func validateQLBT17CandidateWork(m map[string]uint64, spec qlbT17Spec, calls, sortRows uint64, inputRows int) error {
	k := uint64(15)
	if spec.composed {
		k = 2
	}
	expectedCandidates := min(sortRows, k*calls)
	if spec.full {
		expectedCandidates = sortRows
	}
	if m["sort_candidates"] != expectedCandidates || m["sort_peak_candidates"] != expectedCandidates ||
		m["sort_candidate_capacity"] < expectedCandidates || m["sort_candidate_capacity"] > 2*max(expectedCandidates, 16*calls) ||
		m["sort_payload_capacity"] > 2*m["sort_payload_bytes"] || m["sort_accepted_rows"]-m["sort_evicted_rows"] != expectedCandidates {
		return fmt.Errorf("T17 %s did not preserve bounded candidate capacity", spec.name)
	}
	if spec.full && (m["sort_evicted_rows"] != 0 || m["sort_accepted_rows"] != sortRows ||
		m["sort_comparisons"] > uint64(4*inputRows*bits.Len(uint(inputRows-1)))*calls) {
		return fmt.Errorf("T17 full sort exceeded comparator work bound")
	}
	return nil
}

func validateQLBT17Cleanup(m map[string]uint64, spec qlbT17Spec) error {
	for _, prefix := range []string{"sort", "aggregate", "filter", "having"} {
		if m[prefix+"_reserved_bytes"] != m[prefix+"_released_bytes"] || m[prefix+"_live_bytes"] != 0 {
			return fmt.Errorf("T17 %s leaked %s state", spec.name, prefix)
		}
	}
	if m["sort_reserved_bytes"] == 0 || m["sort_peak_bytes"] == 0 || m["sort_descriptor_bytes"] == 0 ||
		m["process_peak_reserved_bytes"] == 0 || m["process_peak_reserved_bytes"] > uint64(3*spec.workers*256)*(1<<20) ||
		m["tracker_lease_charged_bytes"] == 0 || m["plan_cache_entries"] == 0 || m["plan_cache_entry_bytes"] == 0 || m["plan_cache_retained_bytes"] == 0 {
		return fmt.Errorf("T17 %s lacks bounded reservation/cache evidence", spec.name)
	}
	for _, key := range []string{
		"sort_source_pins", "post_close_live_results_delta", "post_close_reserved_bytes_delta", "post_close_owner_pins_delta", "post_close_flatten_pool_refs_delta",
		"plan_cache_inflight", "plan_cache_compile_reserved_bytes", "plan_cache_waiters", "plan_cache_uncached_bytes",
	} {
		if m[key] != 0 {
			return fmt.Errorf("T17 %s leaked %s", spec.name, key)
		}
	}
	return nil
}

func validateQLBT17Result(result Result) error {
	_, warmup, err := qlbT17Schedule(result.Cardinality)
	if err != nil || result.Scenario != ScenarioQLBT17 || result.Engine != engineMeasure || result.Mode != modeVec ||
		(result.Correctness != "pass" && result.Correctness != "partial_known_32_blocked") {
		return fmt.Errorf("invalid T17 report header")
	}
	expected := map[string]qlbT17Spec{}
	for _, spec := range qlbT17Specs() {
		expected[spec.name] = spec
	}
	var pid, base uint64
	variants := map[string]Result{}
	for _, variant := range result.Variants {
		spec, ok := expected[variant.Variant]
		if !ok {
			return fmt.Errorf("unexpected/duplicate T17 variant %s", variant.Variant)
		}
		delete(expected, variant.Variant)
		if result.Correctness == "partial_known_32_blocked" && spec.composed && spec.workers == 32 {
			if err = validateQLBT17Blocked32(variant, result.Cardinality); err != nil {
				return err
			}
			variants[spec.name] = variant
			continue
		}
		if err = validateQLBT17Variant(variant, result.Cardinality, warmup, pid, base, spec); err != nil {
			return err
		}
		pid, base = variant.BatchMetrics["benchmark_process_id"], variant.BatchMetrics["fixture_base_unix_nanos"]
		variants[spec.name] = variant
	}
	if len(expected) != 0 {
		return fmt.Errorf("T17 missing %d variants", len(expected))
	}
	full, topk := variants["fullsort_slice"], variants["topk"]
	if full.ExpectedHash != topk.ExpectedHash || full.ExpectedRows != topk.ExpectedRows {
		return fmt.Errorf("T17 sort pair returned different pages")
	}
	// Batch boundaries may differ across native scans. Compare actual row/root
	// applications and outputs; each variant separately validates its own batches.
	for _, key := range []string{"sort_input_rows", "sort_output_rows", "sort_key_evaluations", "sort_key_kernel_rows", "sort_key_scalar_evaluations", "output_rows"} {
		if full.BatchMetrics[key] != topk.BatchMetrics[key] {
			return fmt.Errorf("T17 sort pair changed %s", key)
		}
	}
	return nil
}

func validateQLBT17Variant(v Result, rows, warmup int, pid, base uint64, spec qlbT17Spec) error {
	calls := spec.calls(rows)
	if v.Scenario != ScenarioQLBT17 || v.Engine != engineMeasure || v.Mode != modeVec || v.Correctness != "pass" || v.Cardinality != rows ||
		v.QueryWorkers != spec.workers || v.QueryMemoryMiB != 256 || v.QueryIterations != calls || v.TimedSamples != calls || len(v.RawLatenciesNS) != calls ||
		v.QPS <= 0 || v.Latency.P50Ms <= 0 || v.Latency.P95Ms < v.Latency.P50Ms || v.Latency.P99Ms < v.Latency.P95Ms ||
		v.Profiles["cpu"] == "" || v.Profiles["heap"] == "" || v.Allocations.MetricSource == "" || v.Allocations.MallocsPerQuery <= 0 || v.Allocations.AllocBytesPerQuery <= 0 {
		return fmt.Errorf("T17 %s changed result/timing/profile/allocation protocol", spec.name)
	}
	for _, latency := range v.RawLatenciesNS {
		if latency <= 0 {
			return fmt.Errorf("T17 %s missing raw latency", spec.name)
		}
	}
	return validateQLBT17Evidence(v, rows, warmup, pid, base, spec)
}

func validateQLBT17Evidence(v Result, rows, warmup int, pid, base uint64, spec qlbT17Spec) error {
	calls := spec.calls(rows)
	m := v.BatchMetrics
	for _, key := range qlbT17MetricKeys() {
		if _, ok := m[key]; !ok {
			return fmt.Errorf("T17 %s missing metric %s", spec.name, key)
		}
	}
	if m["benchmark_process_id"] == 0 || pid != 0 && pid != m["benchmark_process_id"] || m["fixture_base_unix_nanos"] == 0 ||
		base != 0 && base != m["fixture_base_unix_nanos"] || m["fixture_input_rows"] != uint64(rows) ||
		m["warmup_queries"] != uint64(warmup) || m["checked_timed_samples"] != 6 || m["warmup_plan_compiles"] > 1 {
		return fmt.Errorf("T17 %s missing process/fixture/warmup evidence", spec.name)
	}
	fixtureBase := time.Unix(0, int64(m["fixture_base_unix_nanos"]))
	oracles := qlbT17Oracles(spec, rows, fixtureBase)
	if v.ExpectedRows != len(oracles[0].expected.DataPoints) || v.ResponseRows != v.ExpectedRows ||
		v.ExpectedHash != oracles[0].hash || v.ApproxResultHash != v.ExpectedHash ||
		m["bind_sets"] != uint64(len(oracles)) {
		return fmt.Errorf("T17 %s differs from independent ordered page", spec.name)
	}
	for index, oracle := range oracles {
		if m[fmt.Sprintf("bound_set_%d_queries", index)] != uint64((calls+len(oracles)-1-index)/len(oracles)) ||
			m[fmt.Sprintf("bound_set_%d_ordered_hash", index)] != oracle.hash {
			return fmt.Errorf("T17 %s missing changed-bind evidence", spec.name)
		}
	}
	for _, key := range qlbT17UnavailableMetrics() {
		if !slices.Contains(v.UnavailableMetrics, key) {
			return fmt.Errorf("T17 missing unavailable metric %s", key)
		}
		if _, ok := m[key]; ok {
			return fmt.Errorf("T17 fabricated unavailable metric %s", key)
		}
	}
	if err := validateQLBT14ResponseBytes(m); err != nil {
		return err
	}
	return validateQLBT17Metrics(m, spec, calls, oracles)
}

func validateQLBT17Matrix(results []Result) error {
	cards, processes := map[int]bool{}, map[uint64]bool{}
	for _, result := range results {
		if result.Scenario != ScenarioQLBT17 {
			continue
		}
		if cards[result.Cardinality] {
			return fmt.Errorf("T17 repeated input cardinality")
		}
		if err := validateQLBT17Result(result); err != nil {
			return err
		}
		pid := result.Variants[0].BatchMetrics["benchmark_process_id"]
		if processes[pid] {
			return fmt.Errorf("T17 reused cluster process")
		}
		cards[result.Cardinality], processes[pid] = true, true
	}
	if len(cards) != 0 && (!cards[1024] || !cards[32768] || len(cards) != 2) {
		return fmt.Errorf("T17 requires both admitted successful cardinalities")
	}
	return nil
}
