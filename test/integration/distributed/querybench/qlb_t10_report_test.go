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

var qlbT10RequiredMetrics = []string{
	"benchmark_process_id", "warmup_queries", "checked_timed_samples", "ql_batch_executions", "native_batch_executions",
	"native_leaf_calls", "native_source_rows", "native_frame_rows", "intermediate_data_points", "final_data_points", "output_rows", "execution_clock_calls",
	"expression_executions", "expression_batches", "expression_input_rows", "expression_output_rows", "expression_vector_rows", "expression_scalar_evaluations",
	"expression_arm_rows", "expression_later_arm_rows", "expression_buffer_bytes", "expression_full_column_allocations", "expression_reserved_bytes",
	"expression_released_bytes", "expression_live_bytes", "aggregate_executions", "aggregate_input_rows", "aggregate_reserved_bytes", "aggregate_live_bytes",
	"merge_executions", "merge_branches", "merge_reserved_bytes", "merge_live_bytes", "process_peak_reserved_bytes", "tracker_lease_charged_bytes",
	"plan_cache_hits", "plan_cache_compiles", "post_close_live_results_delta", "post_close_reserved_bytes_delta", "post_close_owner_pins_delta",
	"post_close_flatten_pool_refs_delta",
}

func validateQLBT10Result(result Result) error {
	iterations, warmup, err := qlbT10Schedule(result.Cardinality)
	if err != nil || result.Scenario != ScenarioQLBT10 || result.Correctness != "pass" || result.Engine != engineMeasure || result.Mode != modeVec {
		return fmt.Errorf("invalid T10 result header")
	}
	expected := make(map[string]bool, len(qlbT10Variants))
	for _, name := range qlbT10Variants {
		expected[name] = false
	}
	var pid uint64
	for _, variant := range result.Variants {
		seen, exists := expected[variant.Variant]
		if !exists || seen {
			return fmt.Errorf("unexpected or repeated T10 variant %q", variant.Variant)
		}
		expected[variant.Variant] = true
		if err = validateQLBT10VariantHeader(variant, result.Cardinality, iterations); err != nil {
			return err
		}
		current := variant.BatchMetrics["benchmark_process_id"]
		if current == 0 || (pid != 0 && current != pid) || variant.BatchMetrics["warmup_queries"] != uint64(warmup) ||
			variant.BatchMetrics["checked_timed_samples"] != 6 {
			return fmt.Errorf("T10 %s lacks frozen process, warmup or correctness sampling evidence", variant.Variant)
		}
		pid = current
		if err = validateQLBT10Metrics(variant.BatchMetrics, variant.Variant, iterations, result.Cardinality); err != nil {
			return err
		}
	}
	for name, seen := range expected {
		if !seen {
			return fmt.Errorf("missing T10 variant %s", name)
		}
	}
	return nil
}

func validateQLBT10VariantHeader(variant Result, cardinality, iterations int) error {
	expectedHash, err := qlbT10ExpectedHash(variant.Variant, cardinality)
	if err != nil {
		return err
	}
	if variant.Scenario != ScenarioQLBT10 || variant.Cardinality != cardinality || variant.Correctness != "pass" || variant.QueryWorkers != 1 ||
		variant.Engine != engineMeasure || variant.Mode != modeVec || variant.QueryMemoryMiB != 256 ||
		variant.QueryIterations != iterations || variant.TimedSamples != iterations || len(variant.RawLatenciesNS) != iterations ||
		variant.ExpectedRows != cardinality || variant.ResponseRows != cardinality || variant.ExpectedHash != expectedHash || variant.ApproxResultHash != expectedHash {
		return fmt.Errorf("invalid T10 variant %s", variant.Variant)
	}
	for _, latency := range variant.RawLatenciesNS {
		if latency <= 0 {
			return fmt.Errorf("T10 %s contains an empty latency sample", variant.Variant)
		}
	}
	for _, key := range qlbT10RequiredMetrics {
		if _, exists := variant.BatchMetrics[key]; !exists {
			return fmt.Errorf("T10 %s missing %s", variant.Variant, key)
		}
	}
	for _, key := range []string{"storage_physical_scanned_rows", "native_retained_frame_bytes", "total_parent_lease_charges"} {
		if !slices.Contains(variant.UnavailableMetrics, key) {
			return fmt.Errorf("T10 %s does not disclose unavailable %s", variant.Variant, key)
		}
		if _, present := variant.BatchMetrics[key]; present {
			return fmt.Errorf("T10 %s relabels unavailable %s as a measurement", variant.Variant, key)
		}
	}
	return nil
}

func validateQLBT10Metrics(metrics map[string]uint64, name string, samples, inputRows int) error {
	total, rows := uint64(samples), uint64(samples)*uint64(inputRows)
	if metrics["ql_batch_executions"] != total || metrics["native_batch_executions"] != 0 || metrics["native_leaf_calls"] != total*2 ||
		metrics["intermediate_data_points"] != 0 || metrics["final_data_points"] != rows || metrics["output_rows"] != rows ||
		metrics["execution_clock_calls"] != total || metrics["native_source_rows"] == 0 || metrics["native_frame_rows"] == 0 ||
		metrics["plan_cache_hits"] != total || metrics["plan_cache_compiles"] != 0 {
		return fmt.Errorf("T10 %s execution or serialization counts differ from the frozen contract: %+v", name, metrics)
	}
	for _, key := range []string{
		"expression_live_bytes", "aggregate_executions", "aggregate_input_rows", "aggregate_reserved_bytes", "aggregate_live_bytes",
		"merge_executions", "merge_branches", "merge_reserved_bytes", "merge_live_bytes", "post_close_live_results_delta", "post_close_reserved_bytes_delta",
		"post_close_owner_pins_delta", "post_close_flatten_pool_refs_delta",
	} {
		if metrics[key] != 0 {
			return fmt.Errorf("T10 %s leaked or unexpectedly used %s", name, key)
		}
	}
	if name == "raw_control" {
		for _, key := range []string{"expression_executions", "expression_batches", "expression_input_rows", "expression_output_rows", "expression_reserved_bytes"} {
			if metrics[key] != 0 {
				return fmt.Errorf("T10 raw control unexpectedly used the expression evaluator")
			}
		}
		return nil
	}
	if metrics["expression_executions"] != total || metrics["expression_input_rows"] != rows || metrics["expression_output_rows"] != rows ||
		metrics["expression_batches"] < total || metrics["expression_reserved_bytes"] != metrics["expression_released_bytes"] {
		return fmt.Errorf("T10 %s did not evaluate the complete input once or release its reservations", name)
	}
	if name == "constant" && metrics["expression_full_column_allocations"] != 0 {
		return fmt.Errorf("T10 constant allocated a full-length value column")
	}
	return nil
}

func validateQLBT10Matrix(results []Result) error {
	cards, pids := make(map[int]bool, 2), make(map[uint64]bool, 2)
	for _, result := range results {
		if result.Scenario != ScenarioQLBT10 {
			continue
		}
		if cards[result.Cardinality] {
			return fmt.Errorf("T10 repeated cardinality %d", result.Cardinality)
		}
		if err := validateQLBT10Result(result); err != nil {
			return err
		}
		pid := result.Variants[0].BatchMetrics["benchmark_process_id"]
		if pids[pid] {
			return fmt.Errorf("T10 cardinalities must use independent cluster processes")
		}
		pids[pid], cards[result.Cardinality] = true, true
	}
	if len(cards) == 0 {
		return nil
	}
	if len(cards) != 2 || !cards[1024] || !cards[32768] {
		return fmt.Errorf("T10 requires 1024 and 32768 input rows")
	}
	return nil
}
