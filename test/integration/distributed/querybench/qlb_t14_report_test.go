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
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package querybench

import (
	"fmt"
	"slices"
)

var qlbT14RequiredMetrics = []string{
	"benchmark_process_id", "warmup_queries", "checked_timed_samples", "fixture_input_rows", "completed_groups_per_query", "selected_groups_per_query",
	"ql_batch_executions", "native_batch_executions", "native_leaf_calls", "native_source_rows", "native_frame_rows",
	"intermediate_data_points", "final_data_points", "output_rows", "checked_response_bytes", "min_response_bytes", "max_response_bytes",
	"execution_clock_calls", "plan_cache_hits", "plan_cache_compiles", "process_peak_reserved_bytes", "tracker_lease_charged_bytes",
	"post_close_live_results_delta", "post_close_reserved_bytes_delta", "post_close_owner_pins_delta", "post_close_flatten_pool_refs_delta",
	"aggregate_executions", "aggregate_input_rows", "aggregate_groups", "aggregate_state_lanes", "aggregate_state_bytes", "aggregate_key_bytes",
	"aggregate_reserved_bytes", "aggregate_released_bytes", "aggregate_live_bytes",
	"merge_executions", "merge_branches", "merge_input_rows", "merge_groups", "merge_state_lanes", "merge_state_bytes", "merge_key_bytes", "merge_visits",
	"merge_reserved_bytes", "merge_released_bytes", "merge_live_bytes",
}

func qlbT14MetricKeys() []string {
	keys := append([]string(nil), qlbT14RequiredMetrics...)
	for _, prefix := range []string{"filter", "having"} {
		for _, suffix := range qlbT14FilterMetricKeys() {
			keys = append(keys, prefix+"_"+suffix)
		}
	}
	return keys
}

func validateQLBT14Result(result Result) error {
	iterations, warmup, err := qlbT14Schedule(result.Cardinality)
	if err != nil || result.Scenario != ScenarioQLBT14 || result.Engine != engineMeasure || result.Mode != modeVec || result.Correctness != "pass" {
		return fmt.Errorf("invalid T14 report header")
	}
	expected := make(map[string]qlbT14Spec)
	for _, spec := range qlbT14Specs(result.Cardinality) {
		expected[spec.name] = spec
	}
	var pid uint64
	for _, variant := range result.Variants {
		spec, exists := expected[variant.Variant]
		if !exists {
			return fmt.Errorf("unexpected/duplicate T14 variant %s", variant.Variant)
		}
		delete(expected, variant.Variant)
		oracle := newQLBT14Oracle(spec, result.Cardinality)
		if err = validateQLBT14VariantHeader(variant, result.Cardinality, iterations, oracle); err != nil {
			return err
		}
		for _, key := range qlbT14MetricKeys() {
			if _, present := variant.BatchMetrics[key]; !present {
				return fmt.Errorf("T14 missing metric %s", key)
			}
		}
		for _, key := range qlbT14UnavailableMetrics() {
			if !slices.Contains(variant.UnavailableMetrics, key) {
				return fmt.Errorf("T14 missing unavailable disclosure %s", key)
			}
			if _, measured := variant.BatchMetrics[key]; measured {
				return fmt.Errorf("T14 unavailable metric %s reported measured", key)
			}
		}
		m := variant.BatchMetrics
		if m["benchmark_process_id"] == 0 || pid != 0 && pid != m["benchmark_process_id"] || m["warmup_queries"] != uint64(warmup) ||
			m["checked_timed_samples"] != 6 || m["fixture_input_rows"] != uint64(oracle.inputRows) || m["completed_groups_per_query"] != uint64(oracle.groups) ||
			m["selected_groups_per_query"] != uint64(len(oracle.expected.DataPoints)) {
			return fmt.Errorf("T14 %s missing metadata evidence", spec.name)
		}
		if err = validateQLBT14ResponseBytes(m); err != nil {
			return err
		}
		pid = m["benchmark_process_id"]
		if err = validateQLBT14Metrics(m, iterations, oracle); err != nil {
			return err
		}
	}
	if len(expected) != 0 {
		return fmt.Errorf("T14 missing %d required variants", len(expected))
	}
	return nil
}

func validateQLBT14ResponseBytes(metrics map[string]uint64) error {
	samples := metrics["checked_timed_samples"]
	total, minimum, maximum := metrics["checked_response_bytes"], metrics["min_response_bytes"], metrics["max_response_bytes"]
	// Even an empty measure response has its BydbQL oneof tag and length.
	if samples == 0 || minimum < 2 || minimum > maximum || maximum > ^uint64(0)/samples {
		return fmt.Errorf("T14 invalid checked response byte bounds")
	}
	// Both reported extrema must occur among the checked samples.
	lower := minimum*(samples-1) + maximum
	upper := maximum*(samples-1) + minimum
	if total < lower || total > upper {
		return fmt.Errorf("T14 checked response bytes disagree with sample count and extrema")
	}
	return nil
}

func validateQLBT14Metrics(m map[string]uint64, samples int, o *qlbT14Oracle) error {
	if err := validateQLBT14Dispatch(m, samples, o); err != nil {
		return err
	}
	if err := validateQLBT14Stages(m, samples, o); err != nil {
		return err
	}
	if o.spec.mode == "native" {
		return validateQLBT14NativeWork(m, samples, o)
	}
	return validateQLBT14LocalWork(m, samples, o)
}

func validateQLBT14Dispatch(m map[string]uint64, samples int, o *qlbT14Oracle) error {
	calls, output := uint64(samples), uint64(samples*len(o.expected.DataPoints))
	branches := uint64(1)
	if o.spec.mode == "native" {
		branches = 2
	}
	if m["ql_batch_executions"] != calls*branches || m["native_batch_executions"] != 0 || m["native_leaf_calls"] != calls*branches*2 ||
		m["native_source_rows"] == 0 || m["native_frame_rows"] == 0 || m["final_data_points"] != output || m["output_rows"] != output ||
		m["execution_clock_calls"] != calls || m["plan_cache_hits"] != calls || m["plan_cache_compiles"] != 0 {
		return fmt.Errorf("T14 %s dispatch/cache/output counts differ", o.spec.name)
	}
	for _, key := range []string{
		"intermediate_data_points", "aggregate_live_bytes", "merge_live_bytes", "filter_live_bytes", "having_live_bytes", "filter_copied_bytes", "having_copied_bytes",
		"post_close_live_results_delta", "post_close_reserved_bytes_delta", "post_close_owner_pins_delta", "post_close_flatten_pool_refs_delta",
	} {
		if m[key] != 0 {
			return fmt.Errorf("T14 %s unexpected work/leak %s", o.spec.name, key)
		}
	}
	for _, prefix := range []string{"filter", "having", "aggregate", "merge"} {
		if m[prefix+"_reserved_bytes"] != m[prefix+"_released_bytes"] {
			return fmt.Errorf("T14 %s unbalanced %s reservations", o.spec.name, prefix)
		}
	}
	if m["process_peak_reserved_bytes"] == 0 || m["process_peak_reserved_bytes"] > 3*256*(1<<20) || m["tracker_lease_charged_bytes"] == 0 {
		return fmt.Errorf("T14 lacks bounded process reservation evidence")
	}
	return nil
}

func validateQLBT14Stages(m map[string]uint64, samples int, o *qlbT14Oracle) error {
	calls, groups, output := uint64(samples), uint64(samples*o.groups), uint64(samples*len(o.expected.DataPoints))
	if o.spec.percent >= 0 {
		executions := calls
		if o.groups == 0 {
			executions = 0
		}
		if m["having_executions"] != executions || m["having_input_rows"] != groups || m["having_true_rows"] != output || m["having_output_rows"] != output ||
			m["having_false_rows"] != groups-output || m["having_unknown_rows"] != 0 || m["having_reserved_bytes"] == 0 {
			return fmt.Errorf("T14 %s HAVING did not consume completed groups: %+v", o.spec.name, m)
		}
	} else if m["having_executions"] != 0 || m["having_input_rows"] != 0 || m["having_reserved_bytes"] != 0 {
		return fmt.Errorf("T14 baseline ran HAVING")
	}
	if o.spec.membership {
		input, passed := calls*uint64(o.inputRows), calls*uint64(o.aggregateRows)
		if m["filter_executions"] != calls || m["filter_input_rows"] != input || m["filter_true_rows"] != passed || m["filter_output_rows"] != passed ||
			m["filter_false_rows"] != input-passed || m["filter_unknown_rows"] != 0 || m["filter_reserved_bytes"] == 0 {
			return fmt.Errorf("T14 membership raw filter counts differ: %+v", m)
		}
	} else if m["filter_executions"] != 0 || m["filter_input_rows"] != 0 {
		return fmt.Errorf("T14 group predicate ran as a raw filter")
	}
	return nil
}

func validateQLBT14NativeWork(m map[string]uint64, samples int, o *qlbT14Oracle) error {
	calls, groups := uint64(samples), uint64(samples*o.groups)
	if m["aggregate_executions"] != 0 || m["aggregate_input_rows"] != 0 || m["aggregate_reserved_bytes"] != 0 ||
		m["merge_executions"] != calls || m["merge_branches"] != calls*2 || m["merge_groups"] != groups || m["merge_input_rows"] != groups*2 ||
		m["merge_visits"] < groups*2 || m["merge_visits"] > groups*3 || m["merge_state_lanes"] != calls*2 || m["merge_state_bytes"] != groups*18 ||
		m["merge_reserved_bytes"] == 0 {
		return fmt.Errorf("T14 %s repeated calls added native work", o.spec.name)
	}
	return nil
}

func validateQLBT14LocalWork(m map[string]uint64, samples int, o *qlbT14Oracle) error {
	calls, groups := uint64(samples), uint64(samples*o.groups)
	lanes := uint64(2)
	if o.spec.membership {
		lanes = 1
	}
	if m["merge_executions"] != 0 || m["merge_branches"] != 0 || m["merge_reserved_bytes"] != 0 || m["aggregate_executions"] != calls ||
		m["aggregate_input_rows"] != calls*uint64(o.aggregateRows) || m["aggregate_groups"] != groups || m["aggregate_state_lanes"] != calls*lanes ||
		m["aggregate_state_bytes"] != groups*lanes*8 || m["aggregate_reserved_bytes"] == 0 {
		return fmt.Errorf("T14 %s local aggregate work differs", o.spec.name)
	}
	return nil
}

func validateQLBT14Matrix(results []Result) error {
	cards := map[int]bool{}
	pids := map[uint64]bool{}
	for _, result := range results {
		if result.Scenario != ScenarioQLBT14 {
			continue
		}
		if cards[result.Cardinality] {
			return fmt.Errorf("duplicate T14 cardinality")
		}
		if err := validateQLBT14Result(result); err != nil {
			return err
		}
		pid := result.Variants[0].BatchMetrics["benchmark_process_id"]
		if pids[pid] {
			return fmt.Errorf("T14 cardinalities reused cluster process")
		}
		cards[result.Cardinality], pids[pid] = true, true
	}
	if len(cards) != 0 && (!cards[1024] || !cards[32768] || len(cards) != 2) {
		return fmt.Errorf("T14 requires complete two-cardinality matrix")
	}
	return nil
}

func validateQLBT14VariantHeader(variant Result, cardinality, iterations int, oracle *qlbT14Oracle) error {
	if variant.Scenario != ScenarioQLBT14 || variant.Engine != engineMeasure || variant.Mode != modeVec || variant.Correctness != "pass" ||
		variant.Cardinality != cardinality || variant.QueryWorkers != 1 || variant.QueryMemoryMiB != 256 || variant.QueryIterations != iterations ||
		variant.TimedSamples != iterations || len(variant.RawLatenciesNS) != iterations || variant.ExpectedRows != len(oracle.expected.DataPoints) ||
		variant.ResponseRows != variant.ExpectedRows || variant.ExpectedHash != oracle.hash || variant.ApproxResultHash != oracle.hash {
		return fmt.Errorf("T14 %s changed protocol or result", oracle.spec.name)
	}
	for _, latency := range variant.RawLatenciesNS {
		if latency <= 0 {
			return fmt.Errorf("T14 has missing latency samples")
		}
	}
	return nil
}
