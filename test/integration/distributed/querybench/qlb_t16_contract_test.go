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
	"maps"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
)

func TestQLBT16Config(t *testing.T) {
	for _, shape := range []struct{ rows, calls, warmups int }{{1024, 100, 5}, {32768, 20, 3}} {
		cfg := Config{
			RunBench: true, InContainer: true, Engine: engineMeasure, Mode: modeVec, Scenario: Scenario("qlb_t16"), Cardinality: shape.rows,
			QueryWorkers: 1, QueryIterations: shape.calls, WarmupIterations: shape.warmups, QueryMemoryMiB: 256, Profile: true, ByIDIterations: 1000, Writers: 4,
		}
		if err := cfg.Validate(); err != nil {
			t.Fatal(err)
		}
		for _, mutate := range []func(*Config){
			func(c *Config) { c.QueryWorkers = 2 },
			func(c *Config) { c.QueryMemoryMiB = 512 },
			func(c *Config) { c.Profile = false },
			func(c *Config) { c.QueryIterations++ },
			func(c *Config) { c.WarmupIterations++ },
			func(c *Config) { c.Cardinality = 100000 },
		} {
			changed := cfg
			mutate(&changed)
			if changed.Validate() == nil {
				t.Fatal("T16 accepted a changed successful measurement protocol")
			}
		}
	}
}

func TestQLBT16OracleUsesIndependentGroupedValues(t *testing.T) {
	if len(qlbT16Specs()) != 6 {
		t.Fatal("T16 needs exactly the six low/high controls")
	}
	// Eight fixture rows have nullable values NULL,-47,-46,-45,-44,NULL,-42,-41.
	// Four high-cardinality groups contain rows (0,4),(1,5),(2,6),(3,7).
	literal := &measurev1.QueryResponse{}
	for key, total := range []int64{-44, -47, -88, -86} {
		literal.DataPoints = append(literal.DataPoints, &measurev1.DataPoint{Fields: []*measurev1.DataPoint_Field{
			{Name: "key", Value: intFieldValue(int64(key))},
			{Name: "key_again", Value: intFieldValue(int64(key))},
			{Name: "total", Value: intFieldValue(total)},
		}})
	}
	for _, spec := range qlbT16Specs() {
		if !spec.high {
			continue
		}
		oracle := newQLBT16Oracle(spec, 8)
		if err := oracle.check(literal); err != nil {
			t.Fatal(err)
		}
		if !proto.Equal(oracle.expected, literal) {
			t.Fatal("oracle differs from the worked fixture")
		}
		for _, mutate := range []func(*measurev1.QueryResponse){
			func(r *measurev1.QueryResponse) {
				r.DataPoints[1] = proto.Clone(r.DataPoints[0]).(*measurev1.DataPoint)
			},
			func(r *measurev1.QueryResponse) { r.DataPoints[0].Fields[1].Name = "hidden" },
			func(r *measurev1.QueryResponse) { r.DataPoints[0].Fields[2].Value = qlbT07Float(-44) },
			func(r *measurev1.QueryResponse) { r.DataPoints[0].Sid = 1 },
		} {
			changed := proto.Clone(literal).(*measurev1.QueryResponse)
			mutate(changed)
			if oracle.check(changed) == nil {
				t.Fatal("T16 accepted a wrong complete response")
			}
		}
	}
}

func TestQLBT16ReportRequiresCompleteEquivalentControls(t *testing.T) {
	result := qlbT16FixtureReport(1024, 1)
	if err := validateQLBT16Result(result); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*Result){
		func(r *Result) { r.Variants = r.Variants[:5] },
		func(r *Result) { r.Variants[0].RawLatenciesNS = nil },
		func(r *Result) { r.Variants[0].Profiles = nil },
		func(r *Result) { r.Variants[0].Allocations = AllocationStats{} },
		func(r *Result) { delete(r.Variants[0].BatchMetrics, "group_key_evaluations") },
		func(r *Result) { r.Variants[1].BatchMetrics["group_key_evaluations"] *= 2 },
		func(r *Result) { r.Variants[0].BatchMetrics["aggregate_key_bytes"]++ },
		func(r *Result) { r.Variants[0].BatchMetrics["aggregate_state_lanes"]++ },
		func(r *Result) { r.Variants[0].BatchMetrics["post_close_owner_pins_delta"]++ },
		func(r *Result) { r.Variants[2].BatchMetrics["group_expression_vector_rows"]++ },
		func(r *Result) { r.Variants[0].BatchMetrics["checked_response_bytes"] = 0 },
	} {
		changed := result
		changed.Variants = append([]Result(nil), result.Variants...)
		for index := range changed.Variants {
			changed.Variants[index].BatchMetrics = maps.Clone(result.Variants[index].BatchMetrics)
		}
		mutate(&changed)
		if validateQLBT16Result(changed) == nil {
			t.Fatal("T16 accepted incomplete or unequal work evidence")
		}
	}
}

func qlbT16FixtureReport(cardinality int, pid uint64) Result {
	iterations, warmup, _ := qlbT16Schedule(cardinality)
	calls := uint64(iterations)
	result := Result{Scenario: ScenarioQLBT16, Engine: engineMeasure, Mode: modeVec, Cardinality: cardinality, Correctness: "pass"}
	for _, spec := range qlbT16Specs() {
		o := newQLBT16Oracle(spec, cardinality)
		groups := calls * uint64(o.groups)
		m := map[string]uint64{}
		for _, key := range qlbT16RequiredMetrics {
			m[key] = 0
		}
		m["benchmark_process_id"], m["warmup_queries"], m["checked_timed_samples"] = pid, uint64(warmup), 6
		m["fixture_input_rows"], m["completed_groups_per_query"] = uint64(cardinality), uint64(o.groups)
		m["ql_batch_executions"], m["native_leaf_calls"], m["execution_clock_calls"], m["plan_cache_hits"] = calls, calls*2, calls, calls
		m["native_source_rows"], m["native_frame_rows"] = calls*uint64(cardinality), calls*uint64(cardinality)
		m["aggregate_executions"], m["aggregate_input_rows"], m["aggregate_groups"] = calls, calls*uint64(cardinality), groups
		m["aggregate_group_capacity"], m["aggregate_key_bytes"], m["aggregate_key_capacity"] = groups, 10*groups, 10*groups
		m["aggregate_state_lanes"], m["aggregate_state_bytes"], m["aggregate_state_capacity"] = 2*calls, 16*groups, 16*groups
		m["aggregate_reserved_bytes"], m["aggregate_released_bytes"] = 1024, 1024
		m["final_data_points"], m["output_rows"] = groups, groups
		m["process_peak_reserved_bytes"], m["tracker_lease_charged_bytes"] = 2048, 4096
		m["checked_response_bytes"], m["min_response_bytes"], m["max_response_bytes"] = 12, 2, 2
		if spec.computed {
			m["group_expression_executions"], m["group_expression_batches"], m["group_key_root_calls"] = calls, calls, calls
			m["group_expression_input_rows"], m["group_key_evaluations"] = calls*uint64(cardinality), calls*uint64(cardinality)
			m["group_expression_output_rows"] = calls * uint64(cardinality)
			zeroKeys := calls * uint64((cardinality+o.groups-1)/o.groups)
			m["group_expression_vector_rows"], m["group_expression_buffer_bytes"] = calls*uint64(cardinality)*3-zeroKeys, 1024
			m["group_expression_arm_rows"], m["group_expression_later_arm_rows"] = calls*uint64(cardinality), zeroKeys
			m["group_expression_reserved_bytes"], m["group_expression_released_bytes"] = 1024, 1024
		}
		latencies := make([]time.Duration, iterations)
		for index := range latencies {
			latencies[index] = time.Millisecond
		}
		result.Variants = append(result.Variants, Result{
			Scenario: ScenarioQLBT16, Engine: engineMeasure, Mode: modeVec, Variant: spec.name, Cardinality: cardinality, Correctness: "pass",
			QueryWorkers: 1, QueryMemoryMiB: 256, QueryIterations: iterations, TimedSamples: iterations, RawLatenciesNS: latencies,
			ExpectedRows: o.groups, ResponseRows: o.groups, ExpectedHash: o.hash, ApproxResultHash: o.hash, QPS: 1000,
			Latency: LatencyStats{P50Ms: 1, P95Ms: 1, P99Ms: 1}, Profiles: map[string]string{"cpu": "cpu.pprof", "heap": "heap.pprof"},
			Allocations:  AllocationStats{MetricSource: "runtime.MemStats", MallocsDelta: calls, TotalAllocDelta: calls * 1024, MallocsPerQuery: 1, AllocBytesPerQuery: 1024},
			BatchMetrics: m, UnavailableMetrics: qlbT16UnavailableMetrics(),
		})
	}
	return result
}

func TestQLBT16ReportRejectsMatchingIncorrectExpressionWork(t *testing.T) {
	for _, key := range []string{
		"group_key_root_calls", "group_expression_vector_rows", "group_expression_scalar_evaluations", "group_expression_arm_rows", "group_expression_later_arm_rows",
	} {
		result := qlbT16FixtureReport(1024, 1)
		for index := range result.Variants {
			if result.Variants[index].Variant == "computed_explicit_low" || result.Variants[index].Variant == "computed_ordinal_low" {
				result.Variants[index].BatchMetrics[key]++
			}
		}
		if validateQLBT16Result(result) == nil {
			t.Fatalf("equivalent forms with incorrect %s must fail the independent work gate", key)
		}
	}
}
