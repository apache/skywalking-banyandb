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
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
)

func TestQLBT08IndividualOracleVerifiesNativeTypesAndFinalization(t *testing.T) {
	for _, shape := range qlbT08Shapes {
		if shape.branches == 0 {
			continue
		}
		oracle, err := newQLBT08Oracle(shape.name, 1024)
		if err != nil {
			t.Fatal(err)
		}
		sample := qlbT08Sample{native: make([]*measurev1.QueryResponse, len(oracle.native))}
		for index, response := range oracle.native {
			sample.native[index] = proto.Clone(response).(*measurev1.QueryResponse)
			points := sample.native[index].DataPoints
			for left, right := 0, len(points)-1; left < right; left, right = left+1, right-1 {
				points[left], points[right] = points[right], points[left]
			}
		}
		if _, err = oracle.check(sample); err != nil {
			t.Fatal(err)
		}
		sample.native[0].DataPoints = sample.native[0].DataPoints[:15]
		if _, err = oracle.check(sample); err == nil {
			t.Fatal("individual native control accepted a missing host")
		}
		if shape.name == "float_avg" || shape.name == "float_shared" {
			sample.native = append([]*measurev1.QueryResponse(nil), oracle.native...)
			sample.native[1] = proto.Clone(oracle.native[1]).(*measurev1.QueryResponse)
			sample.native[1].DataPoints[0].Fields[0].Value = intFieldValue(54)
			if _, err = oracle.check(sample); err == nil {
				t.Fatal("native float COUNT quirk must be checked before SQL int64 normalization")
			}
		}
	}
}

func TestQLBT08ReportsRejectMissingModesLeaksAndExcessBranches(t *testing.T) {
	results := []Result{qlbT08FixtureResult(t, 1024, 11), qlbT08FixtureResult(t, 32768, 22)}
	if err := validateQLBT08Matrix(results); err != nil {
		t.Fatal(err)
	}
	if validateQLBT08Matrix(results[:1]) == nil {
		t.Fatal("partial matrix accepted")
	}
	for _, change := range []func(*Result){
		func(r *Result) { r.Variants = r.Variants[:10] },
		func(r *Result) { r.Variants[0].TimedSamples-- },
		func(r *Result) { r.Variants[0].BatchMetrics["merge_branches"]++ },
		func(r *Result) { r.Variants[0].BatchMetrics["merge_visits"]++ },
		func(r *Result) { r.Variants[0].BatchMetrics["merge_live_bytes"] = 1 },
		func(r *Result) { r.Variants[0].BatchMetrics["post_close_flatten_pool_refs_delta"] = 1 },
		func(r *Result) { r.Variants[0].BatchMetrics["intermediate_data_points"] = 1 },
		func(r *Result) { r.Variants[1].BatchMetrics["aggregate_input_rows"]-- },
		func(r *Result) { delete(r.Variants[0].BatchMetrics, "merge_key_capacity") },
	} {
		result := qlbT08FixtureResult(t, 1024, 11)
		change(&result)
		if validateQLBT08Result(result) == nil {
			t.Fatal("incomplete/forged T08 measurement accepted")
		}
	}
	results[1].Variants[0].BatchMetrics["benchmark_process_id"] = 11
	if validateQLBT08Matrix(results) == nil {
		t.Fatal("shared cluster process across cardinalities accepted")
	}
}

func qlbT08FixtureResult(t *testing.T, cardinality int, pid uint64) Result {
	t.Helper()
	iterations, warmup, err := qlbT08Schedule(cardinality)
	if err != nil {
		t.Fatal(err)
	}
	result := Result{Scenario: ScenarioQLBT08, Cardinality: cardinality, Correctness: "pass"}
	for _, shape := range qlbT08Shapes {
		for _, mode := range qlbT08Modes {
			if mode == "individual" && shape.branches == 0 {
				continue
			}
			metrics := make(map[string]uint64, len(qlbT08RequiredMetrics))
			for _, key := range qlbT08RequiredMetrics {
				metrics[key] = 0
			}
			total := uint64(iterations)
			metrics["benchmark_process_id"], metrics["warmup_queries"], metrics["checked_timed_samples"] = pid, uint64(warmup), 1
			metrics["native_source_rows"], metrics["native_frame_rows"] = total*uint64(cardinality), total*uint64(cardinality)
			metrics["ql_batch_executions"], metrics["execution_clock_calls"], metrics["rpc_calls"] = total, total, total
			metrics["native_leaf_calls"], metrics["fallback_queries"] = 2*total, total
			rows := 16
			if mode == "individual" {
				rows *= shape.branches
				metrics["ql_batch_executions"], metrics["execution_clock_calls"], metrics["fallback_queries"] = 0, 0, 0
				metrics["native_batch_executions"], metrics["rpc_calls"] = total*uint64(shape.branches), total*uint64(shape.branches)
				metrics["native_leaf_calls"], metrics["native_branches_per_query"] = 2*total*uint64(shape.branches), uint64(shape.branches)
			} else {
				prefix := "aggregate"
				extraBytes := uint64(0)
				if mode == "pushdown" && shape.branches > 0 {
					prefix = "merge"
					extraBytes = 2
					metrics["fallback_queries"], metrics["native_branches_per_query"] = 0, uint64(shape.branches)
					metrics["ql_batch_executions"], metrics["native_leaf_calls"] = total*uint64(shape.branches), 2*total*uint64(shape.branches)
					metrics["merge_branches"], metrics["merge_input_rows"] = total*uint64(shape.branches), total*uint64(shape.branches)*16
					metrics["merge_visits"] = metrics["merge_input_rows"] + total*16
				} else {
					metrics["aggregate_input_rows"] = total * uint64(cardinality)
				}
				metrics[prefix+"_executions"], metrics[prefix+"_groups"], metrics[prefix+"_group_capacity"] = total, total*16, total*16
				metrics[prefix+"_state_lanes"] = total * shape.lanes
				bytes := total * 16 * (shape.lanes*8 + extraBytes)
				metrics[prefix+"_state_bytes"], metrics[prefix+"_state_capacity"] = bytes, bytes
				metrics[prefix+"_reserved_bytes"], metrics[prefix+"_released_bytes"] = bytes, bytes
			}
			metrics["final_data_points"], metrics["output_rows"] = total*uint64(rows), total*uint64(rows)
			latencies := make([]time.Duration, iterations)
			for index := range latencies {
				latencies[index] = time.Millisecond
			}
			result.Variants = append(result.Variants, Result{
				Scenario: ScenarioQLBT08, Variant: shape.name + "_" + mode, Cardinality: cardinality,
				Correctness: "pass", QueryWorkers: 1, QueryIterations: iterations, TimedSamples: iterations, RawLatenciesNS: latencies,
				ExpectedRows: rows, ResponseRows: rows, ExpectedHash: 1, ApproxResultHash: 1, BatchMetrics: metrics,
			})
		}
	}
	return result
}
