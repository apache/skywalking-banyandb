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

var qlbT12RequiredMetrics = []string{
	"benchmark_process_id", "warmup_queries", "checked_timed_samples", "target_selectivity_percent", "hidden_predicate_fields",
	"ql_batch_executions", "native_batch_executions", "native_leaf_calls", "native_source_rows", "native_frame_rows",
	"intermediate_data_points", "final_data_points", "output_rows", "execution_clock_calls", "plan_cache_hits", "plan_cache_compiles",
	"filter_executions", "filter_batches", "filter_input_rows", "filter_output_rows", "filter_true_rows", "filter_false_rows", "filter_unknown_rows",
	"filter_copied_bytes", "filter_selection_bytes", "filter_reserved_bytes", "filter_released_bytes", "filter_live_bytes",
	"aggregate_executions", "aggregate_input_rows", "aggregate_groups", "aggregate_reserved_bytes", "aggregate_released_bytes", "aggregate_live_bytes",
	"merge_executions", "merge_branches", "merge_live_bytes", "process_peak_reserved_bytes", "tracker_lease_charged_bytes",
	"post_close_live_results_delta", "post_close_reserved_bytes_delta", "post_close_owner_pins_delta", "post_close_flatten_pool_refs_delta",
}

func validateQLBT12Result(result Result) error {
	iterations, warmup, err := qlbT12Schedule(result.Cardinality)
	if err != nil || result.Scenario != ScenarioQLBT12 || result.Engine != engineMeasure || result.Mode != modeVec || result.Correctness != "pass" {
		return fmt.Errorf("invalid T12 report header")
	}
	expected := make(map[string]bool, 16)
	for _, name := range qlbT12Variants() {
		expected[name] = false
	}
	var pid uint64
	for _, v := range result.Variants {
		seen, exists := expected[v.Variant]
		if !exists || seen {
			return fmt.Errorf("unexpected or repeated T12 variant %q", v.Variant)
		}
		expected[v.Variant] = true
		oracle, oracleErr := newQLBT12Oracle(v.Variant, result.Cardinality)
		if oracleErr != nil {
			return oracleErr
		}
		if err = validateQLBT12VariantHeader(v, result.Cardinality, iterations, oracle); err != nil {
			return err
		}
		current, metadataErr := validateQLBT12Metadata(v.BatchMetrics, pid, warmup, oracle)
		if metadataErr != nil {
			return metadataErr
		}
		pid = current
		if err = validateQLBT12Metrics(v.BatchMetrics, v.Variant, iterations, oracle); err != nil {
			return err
		}
	}
	for name, seen := range expected {
		if !seen {
			return fmt.Errorf("missing T12 variant %s", name)
		}
	}
	return nil
}

func validateQLBT12VariantHeader(variant Result, cardinality, iterations int, oracle *qlbT12Oracle) error {
	if variant.Scenario != ScenarioQLBT12 || variant.Engine != engineMeasure || variant.Mode != modeVec || variant.Correctness != "pass" ||
		variant.Cardinality != cardinality || variant.QueryWorkers != 1 || variant.QueryMemoryMiB != 256 || variant.QueryIterations != iterations ||
		variant.TimedSamples != iterations || len(variant.RawLatenciesNS) != iterations || variant.ExpectedRows != oracle.outputRows() ||
		variant.ResponseRows != oracle.outputRows() || variant.ExpectedHash != oracle.hash || variant.ApproxResultHash != oracle.hash {
		return fmt.Errorf("invalid T12 %s protocol or result", variant.Variant)
	}
	for _, latency := range variant.RawLatenciesNS {
		if latency <= 0 {
			return fmt.Errorf("T12 lacks timed samples")
		}
	}
	for _, key := range qlbT12RequiredMetrics {
		if _, exists := variant.BatchMetrics[key]; !exists {
			return fmt.Errorf("T12 missing metric %s", key)
		}
	}
	for _, key := range []string{"storage_physical_scanned_rows", "native_retained_frame_bytes", "total_parent_lease_charges"} {
		if !slices.Contains(variant.UnavailableMetrics, key) {
			return fmt.Errorf("T12 missing unavailable metric disclosure %s", key)
		}
		if _, measured := variant.BatchMetrics[key]; measured {
			return fmt.Errorf("T12 unavailable metric %s reported as measured", key)
		}
	}
	return nil
}

func validateQLBT12Metadata(metrics map[string]uint64, pid uint64, warmup int, oracle *qlbT12Oracle) (uint64, error) {
	hidden := uint64(1)
	if oracle.spec.wide {
		hidden = 2
	}
	current := metrics["benchmark_process_id"]
	if current == 0 || (pid != 0 && current != pid) || metrics["warmup_queries"] != uint64(warmup) || metrics["checked_timed_samples"] != 6 ||
		metrics["target_selectivity_percent"] != uint64(oracle.spec.percent) || metrics["hidden_predicate_fields"] != hidden {
		return 0, fmt.Errorf("T12 metadata evidence differs")
	}
	return current, nil
}

func validateQLBT12Metrics(m map[string]uint64, name string, samples int, oracle *qlbT12Oracle) error {
	calls := uint64(samples)
	input, passed := calls*uint64(oracle.cardinality), calls*uint64(oracle.selected)
	output := calls * uint64(oracle.outputRows())
	if m["ql_batch_executions"] != calls || m["native_batch_executions"] != 0 || m["native_leaf_calls"] != calls*2 ||
		m["native_source_rows"] == 0 || m["native_frame_rows"] == 0 || m["final_data_points"] != output || m["output_rows"] != output ||
		m["execution_clock_calls"] != calls || m["plan_cache_hits"] != calls || m["plan_cache_compiles"] != 0 {
		return fmt.Errorf("T12 %s scan/cache/output counts differ", name)
	}
	if m["filter_executions"] != calls || m["filter_batches"] < calls || m["filter_input_rows"] != input || m["filter_output_rows"] != passed ||
		m["filter_true_rows"] != passed || m["filter_false_rows"] != input-passed || m["filter_unknown_rows"] != 0 ||
		m["filter_reserved_bytes"] == 0 || m["filter_reserved_bytes"] != m["filter_released_bytes"] {
		return fmt.Errorf("T12 %s did not examine complete input or release filter state: %+v", name, m)
	}
	for _, key := range []string{
		"filter_copied_bytes", "filter_live_bytes", "aggregate_live_bytes", "merge_executions", "merge_branches", "merge_live_bytes",
		"intermediate_data_points", "post_close_live_results_delta", "post_close_reserved_bytes_delta", "post_close_owner_pins_delta", "post_close_flatten_pool_refs_delta",
	} {
		if m[key] != 0 {
			return fmt.Errorf("T12 %s unexpected work/leak %s", name, key)
		}
	}
	if oracle.spec.aggregate {
		if m["aggregate_executions"] != calls || m["aggregate_input_rows"] != passed || m["aggregate_groups"] != calls ||
			m["aggregate_reserved_bytes"] != m["aggregate_released_bytes"] {
			return fmt.Errorf("T12 aggregate did not consume exactly TRUE rows")
		}
	} else {
		for _, key := range []string{"aggregate_executions", "aggregate_input_rows", "aggregate_groups", "aggregate_reserved_bytes", "aggregate_released_bytes"} {
			if m[key] != 0 {
				return fmt.Errorf("T12 raw query unexpectedly aggregated")
			}
		}
	}
	return nil
}

func validateQLBT12Matrix(results []Result) error {
	cards, pids := map[int]bool{}, map[uint64]bool{}
	for _, result := range results {
		if result.Scenario != ScenarioQLBT12 {
			continue
		}
		if cards[result.Cardinality] {
			return fmt.Errorf("T12 repeated cardinality")
		}
		if err := validateQLBT12Result(result); err != nil {
			return err
		}
		pid := result.Variants[0].BatchMetrics["benchmark_process_id"]
		if pids[pid] {
			return fmt.Errorf("T12 requires separate cluster processes per cardinality")
		}
		pids[pid], cards[result.Cardinality] = true, true
	}
	if len(cards) > 0 && (len(cards) != 2 || !cards[1024] || !cards[16384]) {
		return fmt.Errorf("T12 incomplete cardinality matrix")
	}
	return nil
}
