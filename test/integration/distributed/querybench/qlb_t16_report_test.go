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

func qlbT16UnavailableMetrics() []string { return qlbT15UnavailableMetrics() }

var qlbT16ExpressionMetrics = []string{
	"group_expression_executions", "group_expression_batches", "group_expression_input_rows", "group_expression_output_rows",
	"group_key_evaluations", "group_key_root_calls",
	"group_expression_vector_rows", "group_expression_scalar_evaluations", "group_expression_arm_rows", "group_expression_later_arm_rows",
	"group_expression_buffer_bytes", "group_expression_full_column_allocations", "group_expression_reserved_bytes", "group_expression_released_bytes",
	"group_expression_live_bytes",
}

var qlbT16RequiredMetrics = append(append(slices.Clone(qlbT07RequiredMetrics), qlbT16ExpressionMetrics...),
	"native_batch_executions", "fixture_input_rows", "completed_groups_per_query", "checked_response_bytes", "min_response_bytes", "max_response_bytes")

// The explicit and ordinal spellings must bind to the same work. Retention
// charges and latency can vary with native storage scheduling and are reported
// separately; these logical work counts must remain exact.
var qlbT16EquivalentWork = []string{
	"ql_batch_executions", "native_batch_executions", "native_leaf_calls", "native_source_rows", "native_frame_rows",
	"aggregate_executions", "aggregate_input_rows", "aggregate_groups", "aggregate_key_bytes", "aggregate_state_bytes", "aggregate_state_lanes",
	"group_expression_executions", "group_expression_input_rows", "group_expression_output_rows", "group_key_evaluations",
	"group_expression_vector_rows", "group_expression_scalar_evaluations", "group_expression_arm_rows", "group_expression_later_arm_rows",
	"final_data_points", "output_rows",
}

func validateQLBT16Metrics(m map[string]uint64, samples int, o *qlbT16Oracle) error {
	calls, rows, groups := uint64(samples), uint64(samples)*uint64(o.inputRows), uint64(samples)*uint64(o.groups)
	if m["ql_batch_executions"] != calls || m["native_batch_executions"] != 0 || m["native_leaf_calls"] != 2*calls ||
		m["execution_clock_calls"] != calls || m["aggregate_executions"] != calls || m["aggregate_input_rows"] != rows || m["aggregate_groups"] != groups ||
		m["native_source_rows"] < rows || m["native_frame_rows"] < rows || m["intermediate_data_points"] != 0 ||
		m["final_data_points"] != groups || m["output_rows"] != groups || m["plan_cache_hits"] != calls || m["plan_cache_compiles"] != 0 {
		return fmt.Errorf("T16 %s did not perform one complete raw scan and local aggregation", o.spec.name)
	}
	if err := validateQLBT16GroupStorage(m, calls, groups, o.spec.name); err != nil {
		return err
	}
	if !o.spec.computed {
		for _, key := range qlbT16ExpressionMetrics {
			if m[key] != 0 {
				return fmt.Errorf("T16 precomputed control evaluated %s", key)
			}
		}
		return nil
	}
	return validateQLBT16ExpressionWork(m, calls, rows, o)
}

func validateQLBT16ExpressionWork(m map[string]uint64, calls, rows uint64, o *qlbT16Oracle) error {
	if m["group_expression_executions"] != calls || m["group_expression_batches"] < calls || m["group_expression_input_rows"] != rows ||
		m["group_expression_output_rows"] != rows || m["group_key_evaluations"] != rows || m["group_expression_vector_rows"] == 0 ||
		m["group_expression_buffer_bytes"] == 0 || m["group_expression_reserved_bytes"] == 0 {
		return fmt.Errorf("T16 %s did not evaluate each unique computed key once per active row", o.spec.name)
	}
	// One row in each modulo partition cycle has key zero. CASE executes one
	// root and comparison for every row, plus addition only for other keys.
	zeroKeys := calls * uint64((o.inputRows+o.groups-1)/o.groups)
	if m["group_key_root_calls"] != m["group_expression_batches"] || m["group_expression_vector_rows"] != 3*rows-zeroKeys ||
		m["group_expression_scalar_evaluations"] != 0 || m["group_expression_arm_rows"] != rows || m["group_expression_later_arm_rows"] != zeroKeys {
		return fmt.Errorf("T16 %s actual expression work differs from independent CASE fixture arithmetic", o.spec.name)
	}
	return nil
}

func validateQLBT16GroupStorage(m map[string]uint64, calls, groups uint64, name string) error {
	if m["aggregate_group_capacity"] < groups || m["aggregate_key_bytes"] != 10*groups || m["aggregate_key_capacity"] < m["aggregate_key_bytes"] ||
		m["aggregate_state_lanes"] != 2*calls || m["aggregate_state_bytes"] != 16*groups || m["aggregate_state_capacity"] < m["aggregate_state_bytes"] {
		return fmt.Errorf("T16 %s key/state work differs", name)
	}
	if m["aggregate_reserved_bytes"] == 0 || m["aggregate_reserved_bytes"] != m["aggregate_released_bytes"] ||
		m["group_expression_reserved_bytes"] != m["group_expression_released_bytes"] || m["process_peak_reserved_bytes"] == 0 ||
		m["process_peak_reserved_bytes"] > 3*256*(1<<20) || m["tracker_lease_charged_bytes"] == 0 {
		return fmt.Errorf("T16 %s reservation bounds or release differ", name)
	}
	for _, key := range []string{
		"aggregate_live_bytes", "group_expression_live_bytes", "post_close_live_results_delta", "post_close_reserved_bytes_delta",
		"post_close_owner_pins_delta", "post_close_flatten_pool_refs_delta",
	} {
		if m[key] != 0 {
			return fmt.Errorf("T16 %s leaked %s", name, key)
		}
	}
	return nil
}

func validateQLBT16Result(result Result) error {
	iterations, warmup, err := qlbT16Schedule(result.Cardinality)
	if err != nil || result.Scenario != ScenarioQLBT16 || result.Engine != engineMeasure || result.Mode != modeVec || result.Correctness != "pass" {
		return fmt.Errorf("invalid T16 report")
	}
	expected := map[string]qlbT16Spec{}
	variants := map[string]Result{}
	for _, spec := range qlbT16Specs() {
		expected[spec.name] = spec
	}
	var pid uint64
	for _, variant := range result.Variants {
		spec, ok := expected[variant.Variant]
		if !ok {
			return fmt.Errorf("unexpected/duplicate T16 variant %s", variant.Variant)
		}
		delete(expected, variant.Variant)
		o := newQLBT16Oracle(spec, result.Cardinality)
		if err = validateQLBT16Variant(variant, result.Cardinality, iterations, warmup, pid, o); err != nil {
			return err
		}
		pid = variant.BatchMetrics["benchmark_process_id"]
		variants[variant.Variant] = variant
	}
	if len(expected) != 0 {
		return fmt.Errorf("T16 missing %d variants", len(expected))
	}
	for _, level := range []string{"low", "high"} {
		simple, explicit, ordinal := variants["simple_"+level], variants["computed_explicit_"+level], variants["computed_ordinal_"+level]
		if simple.ExpectedHash != explicit.ExpectedHash || explicit.ExpectedHash != ordinal.ExpectedHash {
			return fmt.Errorf("T16 %s equivalent forms changed result schema or values", level)
		}
		for _, key := range qlbT16EquivalentWork {
			if explicit.BatchMetrics[key] != ordinal.BatchMetrics[key] {
				return fmt.Errorf("T16 %s explicit/ordinal physical work differs: %s", level, key)
			}
		}
	}
	return nil
}

func validateQLBT16Variant(v Result, cardinality, iterations, warmup int, pid uint64, o *qlbT16Oracle) error {
	if v.Scenario != ScenarioQLBT16 || v.Engine != engineMeasure || v.Mode != modeVec || v.Correctness != "pass" || v.Cardinality != cardinality ||
		v.QueryWorkers != 1 || v.QueryMemoryMiB != 256 || v.QueryIterations != iterations || v.TimedSamples != iterations || len(v.RawLatenciesNS) != iterations ||
		v.ExpectedRows != o.groups || v.ResponseRows != o.groups || v.ExpectedHash != o.hash || v.ApproxResultHash != o.hash ||
		v.QPS <= 0 || v.Latency.P50Ms <= 0 || v.Latency.P95Ms < v.Latency.P50Ms || v.Latency.P99Ms < v.Latency.P95Ms ||
		v.Profiles["cpu"] == "" || v.Profiles["heap"] == "" || v.Allocations.MetricSource == "" || v.Allocations.AllocBytesPerQuery <= 0 || v.Allocations.MallocsPerQuery <= 0 {
		return fmt.Errorf("T16 %s changed timing/result/profile/allocation protocol", o.spec.name)
	}
	for _, latency := range v.RawLatenciesNS {
		if latency <= 0 {
			return fmt.Errorf("T16 missing raw latency")
		}
	}
	return validateQLBT16Evidence(v, warmup, pid, o)
}

func validateQLBT16Evidence(v Result, warmup int, pid uint64, o *qlbT16Oracle) error {
	m := v.BatchMetrics
	for _, key := range qlbT16RequiredMetrics {
		if _, ok := m[key]; !ok {
			return fmt.Errorf("T16 missing metric %s", key)
		}
	}
	for _, key := range qlbT16UnavailableMetrics() {
		if !slices.Contains(v.UnavailableMetrics, key) {
			return fmt.Errorf("T16 missing unavailable %s", key)
		}
		if _, ok := m[key]; ok {
			return fmt.Errorf("T16 unavailable %s measured", key)
		}
	}
	if m["benchmark_process_id"] == 0 || pid != 0 && pid != m["benchmark_process_id"] || m["warmup_queries"] != uint64(warmup) || m["checked_timed_samples"] != 6 ||
		m["fixture_input_rows"] != uint64(o.inputRows) || m["completed_groups_per_query"] != uint64(o.groups) {
		return fmt.Errorf("T16 missing process/warmup/fixture metadata")
	}
	if err := validateQLBT14ResponseBytes(m); err != nil {
		return err
	}
	return validateQLBT16Metrics(m, v.QueryIterations, o)
}

func validateQLBT16Matrix(results []Result) error {
	cards, processes := map[int]bool{}, map[uint64]bool{}
	for _, result := range results {
		if result.Scenario != ScenarioQLBT16 {
			continue
		}
		if cards[result.Cardinality] {
			return fmt.Errorf("T16 repeated input cardinality")
		}
		if err := validateQLBT16Result(result); err != nil {
			return err
		}
		pid := result.Variants[0].BatchMetrics["benchmark_process_id"]
		if processes[pid] {
			return fmt.Errorf("T16 reused cluster process")
		}
		cards[result.Cardinality], processes[pid] = true, true
	}
	if len(cards) != 0 && (!cards[1024] || !cards[32768] || len(cards) != 2) {
		return fmt.Errorf("T16 requires both focused successful cardinalities")
	}
	return nil
}
