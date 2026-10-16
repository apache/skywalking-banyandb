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

var qlbT08RequiredMetrics = []string{
	"benchmark_process_id", "warmup_queries", "checked_timed_samples", "ql_batch_executions", "native_batch_executions",
	"native_leaf_calls", "native_source_rows", "native_frame_rows", "intermediate_data_points", "final_data_points", "output_rows", "execution_clock_calls",
	"aggregate_executions", "aggregate_input_rows", "aggregate_groups", "aggregate_group_capacity", "aggregate_key_bytes", "aggregate_key_capacity",
	"aggregate_state_bytes", "aggregate_state_capacity", "aggregate_state_lanes", "aggregate_reserved_bytes", "aggregate_released_bytes", "aggregate_live_bytes",
	"merge_executions", "merge_branches", "merge_input_rows", "merge_groups", "merge_group_capacity", "merge_key_bytes", "merge_key_capacity",
	"merge_state_bytes", "merge_state_capacity", "merge_state_lanes", "merge_visits", "merge_reserved_bytes", "merge_released_bytes", "merge_live_bytes",
	"process_peak_reserved_bytes", "tracker_lease_charged_bytes", "plan_cache_hits", "plan_cache_compiles", "native_branches_per_query", "fallback_queries", "rpc_calls",
	"post_close_live_results_delta", "post_close_reserved_bytes_delta", "post_close_owner_pins_delta", "post_close_flatten_pool_refs_delta",
}

func validateQLBT08Result(result Result) error {
	iterations, warmup, err := qlbT08Schedule(result.Cardinality)
	if err != nil || result.Scenario != ScenarioQLBT08 || result.Correctness != "pass" {
		return fmt.Errorf("invalid T08 result header")
	}
	expected := make(map[string]bool, 11)
	for _, shape := range qlbT08Shapes {
		for _, mode := range qlbT08Modes {
			if mode != "individual" || shape.branches > 0 {
				expected[shape.name+"_"+mode] = false
			}
		}
	}
	var pid uint64
	for _, variant := range result.Variants {
		seen, exists := expected[variant.Variant]
		if !exists || seen {
			return fmt.Errorf("unexpected/repeated T08 variant %q", variant.Variant)
		}
		expected[variant.Variant] = true
		shape, mode, variantErr := qlbT08Variant(variant.Variant)
		if variantErr != nil {
			return variantErr
		}
		rows := min(16, result.Cardinality)
		if mode == "individual" {
			rows *= shape.branches
		}
		if err = validateQLBT08VariantHeader(variant, result.Cardinality, iterations, rows); err != nil {
			return err
		}
		current := variant.BatchMetrics["benchmark_process_id"]
		if current == 0 || (pid != 0 && current != pid) || variant.BatchMetrics["warmup_queries"] != uint64(warmup) || variant.BatchMetrics["checked_timed_samples"] == 0 {
			return fmt.Errorf("T08 %s lacks process/warmup/check evidence", variant.Variant)
		}
		pid = current
		if err = validateQLBT08Metrics(variant.BatchMetrics, shape, mode, iterations, result.Cardinality); err != nil {
			return err
		}
	}
	for name, seen := range expected {
		if !seen {
			return fmt.Errorf("missing T08 variant %s", name)
		}
	}
	return nil
}

func validateQLBT08VariantHeader(variant Result, cardinality, iterations, rows int) error {
	if variant.Scenario != ScenarioQLBT08 || variant.Cardinality != cardinality || variant.Correctness != "pass" || variant.QueryWorkers != 1 ||
		variant.QueryIterations != iterations || variant.TimedSamples != iterations || len(variant.RawLatenciesNS) != iterations ||
		variant.ExpectedRows != rows || variant.ResponseRows != rows || variant.ExpectedHash == 0 || variant.ApproxResultHash != variant.ExpectedHash {
		return fmt.Errorf("invalid T08 variant %s", variant.Variant)
	}
	for _, latency := range variant.RawLatenciesNS {
		if latency <= 0 {
			return fmt.Errorf("T08 %s contains an empty latency sample", variant.Variant)
		}
	}
	for _, key := range qlbT08RequiredMetrics {
		if _, exists := variant.BatchMetrics[key]; !exists {
			return fmt.Errorf("T08 %s missing %s", variant.Variant, key)
		}
	}
	return nil
}

func qlbT08Variant(name string) (qlbT08Shape, string, error) {
	for _, shape := range qlbT08Shapes {
		for _, mode := range qlbT08Modes {
			if name == shape.name+"_"+mode {
				return shape, mode, nil
			}
		}
	}
	return qlbT08Shape{}, "", fmt.Errorf("unknown T08 variant %q", name)
}

func validateQLBT08Metrics(metrics map[string]uint64, shape qlbT08Shape, mode string, samples, inputRows int) error {
	total, groups := uint64(samples), uint64(min(16, inputRows))
	branches := uint64(shape.branches)
	expectedRows := total * groups
	qlCalls, nativeCalls, leafCalls := total, uint64(0), total*2
	rpcCalls, fallback, expectedBranches := total, total, uint64(0)
	clockCalls := total
	pushdown := mode == "pushdown" && branches > 0
	if pushdown {
		qlCalls *= branches
		leafCalls *= branches
		fallback = 0
		expectedBranches = branches
	}
	if mode == "individual" {
		qlCalls, nativeCalls, leafCalls = 0, total*branches, total*branches*2
		clockCalls = 0
		expectedRows *= branches
		rpcCalls = total * branches
		fallback = 0
		expectedBranches = branches
	}
	if metrics["ql_batch_executions"] != qlCalls || metrics["native_batch_executions"] != nativeCalls || metrics["native_leaf_calls"] != leafCalls ||
		metrics["rpc_calls"] != rpcCalls || metrics["fallback_queries"] != fallback || metrics["native_branches_per_query"] != expectedBranches ||
		metrics["intermediate_data_points"] != 0 || metrics["final_data_points"] != expectedRows || metrics["output_rows"] != expectedRows ||
		metrics["execution_clock_calls"] != clockCalls {
		return fmt.Errorf("T08 %s/%s execution/serialization counts differ from its branch contract: %+v", shape.name, mode, metrics)
	}
	if metrics["native_source_rows"] == 0 || metrics["native_frame_rows"] == 0 {
		return fmt.Errorf("T08 %s/%s has no native source evidence", shape.name, mode)
	}
	if err := validateQLBT08AggregateMetrics(metrics, shape, mode, total, groups, inputRows); err != nil {
		return err
	}
	for _, key := range []string{
		"aggregate_live_bytes", "merge_live_bytes", "post_close_live_results_delta", "post_close_reserved_bytes_delta",
		"post_close_owner_pins_delta", "post_close_flatten_pool_refs_delta",
	} {
		if metrics[key] != 0 {
			return fmt.Errorf("T08 %s/%s leaked %s", shape.name, mode, key)
		}
	}
	return nil
}

func validateQLBT08AggregateMetrics(metrics map[string]uint64, shape qlbT08Shape, mode string, total, groups uint64, inputRows int) error {
	branches := uint64(shape.branches)
	local := mode != "individual" && (mode == "local" || branches == 0)
	pushdown := mode == "pushdown" && branches > 0
	if local {
		if metrics["aggregate_executions"] != total || metrics["aggregate_input_rows"] != total*uint64(inputRows) || metrics["aggregate_groups"] != total*groups ||
			metrics["aggregate_state_lanes"] != total*shape.lanes || metrics["aggregate_state_bytes"] != total*groups*shape.lanes*8 {
			return fmt.Errorf("T08 %s/%s did not use one complete local raw scan/shared state", shape.name, mode)
		}
		if err := validateQLBT08State(metrics, "aggregate"); err != nil {
			return err
		}
	} else if metrics["aggregate_executions"] != 0 || metrics["aggregate_input_rows"] != 0 || metrics["aggregate_reserved_bytes"] != 0 {
		return fmt.Errorf("T08 %s/%s unexpectedly used the local aggregate", shape.name, mode)
	}
	if pushdown {
		if metrics["merge_executions"] != total || metrics["merge_branches"] != total*branches || metrics["merge_input_rows"] != total*branches*groups ||
			metrics["merge_groups"] != total*groups || metrics["merge_state_lanes"] != total*shape.lanes ||
			metrics["merge_state_bytes"] != total*groups*(shape.lanes*8+2) || metrics["merge_visits"] == 0 ||
			metrics["merge_visits"] > metrics["merge_input_rows"]+metrics["merge_groups"] {
			return fmt.Errorf("T08 %s/%s merge branch/state/work bound differs: %+v", shape.name, mode, metrics)
		}
		if err := validateQLBT08State(metrics, "merge"); err != nil {
			return err
		}
	} else if metrics["merge_executions"] != 0 || metrics["merge_branches"] != 0 || metrics["merge_reserved_bytes"] != 0 {
		return fmt.Errorf("T08 %s/%s unexpectedly used the keyed merge", shape.name, mode)
	}
	return nil
}

func validateQLBT08State(metrics map[string]uint64, prefix string) error {
	if metrics[prefix+"_group_capacity"] < metrics[prefix+"_groups"] || metrics[prefix+"_key_capacity"] < metrics[prefix+"_key_bytes"] ||
		metrics[prefix+"_state_capacity"] < metrics[prefix+"_state_bytes"] || metrics[prefix+"_reserved_bytes"] == 0 ||
		metrics[prefix+"_reserved_bytes"] != metrics[prefix+"_released_bytes"] {
		return fmt.Errorf("T08 %s state/reservation evidence is invalid", prefix)
	}
	return nil
}

func validateQLBT08Matrix(results []Result) error {
	cards := make(map[int]Result, 2)
	pids := make(map[uint64]bool, 2)
	for _, result := range results {
		if result.Scenario != ScenarioQLBT08 {
			continue
		}
		if _, exists := cards[result.Cardinality]; exists {
			return fmt.Errorf("T08 repeated cardinality %d", result.Cardinality)
		}
		if err := validateQLBT08Result(result); err != nil {
			return err
		}
		pid := result.Variants[0].BatchMetrics["benchmark_process_id"]
		if pids[pid] {
			return fmt.Errorf("T08 cardinalities must use independent cluster processes")
		}
		pids[pid] = true
		cards[result.Cardinality] = result
	}
	if len(cards) == 0 {
		return nil
	}
	if len(cards) != 2 {
		return fmt.Errorf("T08 requires 1024 and 32768 input rows")
	}
	for _, cardinality := range []int{1024, 32768} {
		if _, exists := cards[cardinality]; !exists {
			return fmt.Errorf("T08 missing cardinality %d", cardinality)
		}
	}
	return nil
}
