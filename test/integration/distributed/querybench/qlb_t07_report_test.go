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

import "fmt"

var qlbT07RequiredMetrics = []string{
	"benchmark_process_id", "warmup_queries", "checked_timed_samples",
	"ql_batch_executions", "native_leaf_calls", "native_source_rows", "native_frame_rows",
	"intermediate_data_points", "final_data_points", "output_rows", "execution_clock_calls",
	"aggregate_executions", "aggregate_input_rows", "aggregate_groups", "aggregate_group_capacity",
	"aggregate_key_bytes", "aggregate_key_capacity", "aggregate_state_bytes", "aggregate_state_capacity", "aggregate_state_lanes",
	"aggregate_reserved_bytes", "aggregate_released_bytes", "aggregate_live_bytes",
	"process_peak_reserved_bytes", "tracker_lease_charged_bytes", "plan_cache_hits", "plan_cache_compiles",
	"post_close_live_results_delta", "post_close_reserved_bytes_delta", "post_close_owner_pins_delta", "post_close_flatten_pool_refs_delta",
}

func validateQLBT07Result(result Result) error {
	iterations, warmup, err := qlbT07Schedule(result.Cardinality)
	if err != nil || result.Scenario != ScenarioQLBT07 || result.Correctness != "pass" {
		return fmt.Errorf("invalid T07 result header")
	}
	expected := make(map[string]bool, len(qlbT07Variants))
	for _, name := range qlbT07Variants {
		expected[name] = false
	}
	var process uint64
	for _, variant := range result.Variants {
		seen, exists := expected[variant.Variant]
		if !exists || seen {
			return fmt.Errorf("unexpected/repeated T07 variant %q", variant.Variant)
		}
		expected[variant.Variant] = true
		oracle, oracleErr := newQLBT07Oracle(variant.Variant, result.Cardinality)
		if oracleErr != nil {
			return oracleErr
		}
		if err = validateQLBT07VariantHeader(variant, result.Cardinality, iterations, oracle.rows()); err != nil {
			return err
		}
		for _, key := range qlbT07RequiredMetrics {
			if _, exists = variant.BatchMetrics[key]; !exists {
				return fmt.Errorf("%s missing T07 metric %s", variant.Variant, key)
			}
		}
		if variant.BatchMetrics["benchmark_process_id"] == 0 || (process != 0 && process != variant.BatchMetrics["benchmark_process_id"]) ||
			variant.BatchMetrics["warmup_queries"] != uint64(warmup) || variant.BatchMetrics["checked_timed_samples"] == 0 {
			return fmt.Errorf("%s has invalid process/warmup/check evidence", variant.Variant)
		}
		process = variant.BatchMetrics["benchmark_process_id"]
		if err = validateQLBT07Metrics(variant.BatchMetrics, variant.Variant, iterations, result.Cardinality, oracle.rows()); err != nil {
			return err
		}
	}
	for name, seen := range expected {
		if !seen {
			return fmt.Errorf("missing T07 variant %s", name)
		}
	}
	return nil
}

func validateQLBT07VariantHeader(variant Result, cardinality, iterations, rows int) error {
	if variant.Scenario != ScenarioQLBT07 || variant.Cardinality != cardinality || variant.Correctness != "pass" ||
		variant.QueryWorkers != 1 || variant.QueryIterations != iterations || variant.TimedSamples != iterations || len(variant.RawLatenciesNS) != iterations ||
		variant.ExpectedRows != rows || variant.ResponseRows != variant.ExpectedRows ||
		variant.ExpectedHash == 0 || variant.ApproxResultHash != variant.ExpectedHash {
		return fmt.Errorf("invalid T07 variant %s", variant.Variant)
	}
	for _, latency := range variant.RawLatenciesNS {
		if latency <= 0 {
			return fmt.Errorf("%s contains an empty latency sample", variant.Variant)
		}
	}
	return nil
}

func validateQLBT07Metrics(metrics map[string]uint64, name string, samples, inputRows, outputRows int) error {
	total, rows := uint64(samples), uint64(inputRows)
	if metrics["ql_batch_executions"] != total || metrics["native_leaf_calls"] != total*2 || metrics["aggregate_executions"] != total ||
		metrics["execution_clock_calls"] != total || metrics["aggregate_input_rows"] != total*rows ||
		metrics["aggregate_groups"] != total*qlbT07GroupCount(name, inputRows) {
		return fmt.Errorf("%s did not run one complete distributed raw scan and local aggregate per query", name)
	}
	if metrics["native_source_rows"] < total*rows || metrics["native_frame_rows"] < total*rows ||
		metrics["intermediate_data_points"] != 0 || metrics["final_data_points"] != total*uint64(outputRows) || metrics["output_rows"] != total*uint64(outputRows) {
		return fmt.Errorf("%s input/serialization counts differ from the scalar fixture", name)
	}
	lanes := uint64(2) // Nullable SUM and AVG both retain the exact non-null count.
	if name == "ql_multi_1024_groups" {
		lanes = 4
	}
	if metrics["aggregate_state_lanes"] != total*lanes || metrics["aggregate_group_capacity"] < metrics["aggregate_groups"] ||
		metrics["aggregate_state_capacity"] < metrics["aggregate_state_bytes"] || metrics["aggregate_state_bytes"] == 0 ||
		metrics["aggregate_key_capacity"] < metrics["aggregate_key_bytes"] {
		return fmt.Errorf("%s aggregate capacity/shared-state evidence is invalid", name)
	}
	if metrics["aggregate_reserved_bytes"] == 0 || metrics["aggregate_reserved_bytes"] != metrics["aggregate_released_bytes"] {
		return fmt.Errorf("%s did not release aggregate reservations", name)
	}
	for _, key := range []string{
		"aggregate_live_bytes", "post_close_live_results_delta", "post_close_reserved_bytes_delta", "post_close_owner_pins_delta", "post_close_flatten_pool_refs_delta",
	} {
		if metrics[key] != 0 {
			return fmt.Errorf("%s leaked %s=%d", name, key, metrics[key])
		}
	}
	return nil
}

func validateQLBT07Matrix(results []Result) error {
	cards := make(map[int]Result)
	processes := make(map[uint64]bool)
	for _, result := range results {
		if result.Scenario != ScenarioQLBT07 {
			continue
		}
		if _, exists := cards[result.Cardinality]; exists {
			return fmt.Errorf("T07 repeated input cardinality %d", result.Cardinality)
		}
		if err := validateQLBT07Result(result); err != nil {
			return err
		}
		pid := result.Variants[0].BatchMetrics["benchmark_process_id"]
		if processes[pid] {
			return fmt.Errorf("T07 cardinalities must use independent cluster processes")
		}
		processes[pid], cards[result.Cardinality] = true, result
	}
	if len(cards) == 0 {
		return nil
	}
	if len(cards) != 2 {
		return fmt.Errorf("T07 successful-query measurements require 1024 and 32768 input rows")
	}
	for _, name := range qlbT07Variants {
		var stateWidth uint64
		for _, cardinality := range []int{1024, 32768} {
			result, exists := cards[cardinality]
			if !exists {
				return fmt.Errorf("T07 missing input cardinality %d", cardinality)
			}
			for _, variant := range result.Variants {
				if variant.Variant != name {
					continue
				}
				stateBytes, groups := variant.BatchMetrics["aggregate_state_bytes"], variant.BatchMetrics["aggregate_groups"]
				if groups == 0 || stateBytes%groups != 0 || (stateWidth != 0 && stateWidth != stateBytes/groups) {
					return fmt.Errorf("%s fixed-width state must scale with groups, not rows", name)
				}
				stateWidth = stateBytes / groups
			}
		}
	}
	return nil
}
