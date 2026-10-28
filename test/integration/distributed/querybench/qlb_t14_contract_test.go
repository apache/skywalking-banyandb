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
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/pkg/bydbql"
)

func TestQLBT14ConfigPinsSuccessfulMatrix(t *testing.T) {
	for _, rows := range []int{1024, 32768} {
		iterations, warmup := 100, 5
		if rows == 32768 {
			iterations, warmup = 20, 3
		}
		cfg := Config{
			RunBench: true, InContainer: true, Engine: engineMeasure, Mode: modeVec, Scenario: Scenario("qlb_t14"), Cardinality: rows,
			QueryWorkers: 1, QueryIterations: iterations, WarmupIterations: warmup, QueryMemoryMiB: 256, ByIDIterations: 1000, Writers: 4,
		}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("T14 complete successful protocol rejected: %v", err)
		}
		cfg.QueryIterations++
		if err := cfg.Validate(); err == nil {
			t.Fatal("changed T14 sampling protocol accepted")
		}
	}
}

func TestQLBT14ReportsRejectIncompleteAndFalseWorkEvidence(t *testing.T) {
	first, second := qlbT14FixtureReport(t, 1024, 11), qlbT14FixtureReport(t, 32768, 22)
	if err := validateQLBT14Matrix([]Result{first, second}); err != nil {
		t.Fatal(err)
	}
	if validateQLBT14Matrix([]Result{first}) == nil {
		t.Fatal("partial T14 matrix accepted")
	}
	changes := []func(*Result){
		func(r *Result) { r.Variants = r.Variants[1:] },
		func(r *Result) { r.Variants[0].BatchMetrics["merge_branches"]++ },
		func(r *Result) { r.Variants[1].BatchMetrics["having_input_rows"]++ },
		func(r *Result) { r.Variants[1].BatchMetrics["having_true_rows"]++ },
		func(r *Result) { r.Variants[1].BatchMetrics["having_released_bytes"]++ },
		func(r *Result) { r.Variants[1].BatchMetrics["having_live_bytes"] = 1 },
		func(r *Result) { r.Variants[1].BatchMetrics["post_close_owner_pins_delta"] = 1 },
		func(r *Result) { r.Variants[1].BatchMetrics["intermediate_data_points"] = 1 },
		func(r *Result) { r.Variants[0].TimedSamples-- },
		func(r *Result) { r.Variants[0].ExpectedHash++ },
		func(r *Result) { delete(r.Variants[1].BatchMetrics, "having_reserved_bytes") },
	}
	for index, change := range changes {
		report := qlbT14FixtureReport(t, 1024, 11)
		change(&report)
		if validateQLBT14Result(report) == nil {
			t.Fatalf("false T14 report mutation%d accepted", index)
		}
	}
	second.Variants[0].BatchMetrics["benchmark_process_id"] = 11
	if validateQLBT14Matrix([]Result{first, second}) == nil {
		t.Fatal("shared process across T14 cardinalities accepted")
	}
}

func TestQLBT14ReportsRequireCheckedResponseBytes(t *testing.T) {
	changes := []struct {
		change func(map[string]uint64)
		name   string
	}{
		{func(m map[string]uint64) { delete(m, "checked_response_bytes") }, "missing total"},
		{func(m map[string]uint64) { delete(m, "min_response_bytes") }, "missing minimum"},
		{func(m map[string]uint64) { delete(m, "max_response_bytes") }, "missing maximum"},
		{func(m map[string]uint64) { m["min_response_bytes"] = 1 }, "missing measure envelope"},
		{func(m map[string]uint64) { m["min_response_bytes"] = 65 }, "reversed bounds"},
		{func(m map[string]uint64) { m["checked_response_bytes"]-- }, "total too low"},
		{func(m map[string]uint64) { m["checked_response_bytes"]++ }, "total too high"},
		{func(m map[string]uint64) { m["min_response_bytes"]-- }, "minimum not observed"},
		{func(m map[string]uint64) { m["max_response_bytes"]++ }, "maximum not observed"},
		{func(m map[string]uint64) {
			m["checked_response_bytes"], m["min_response_bytes"], m["max_response_bytes"] = ^uint64(0), ^uint64(0), ^uint64(0)
		}, "overflowing bounds"},
	}
	for _, test := range changes {
		t.Run(test.name, func(t *testing.T) {
			report := qlbT14FixtureReport(t, 1024, 11)
			test.change(report.Variants[0].BatchMetrics)
			if validateQLBT14Result(report) == nil {
				t.Fatal("unmeasured or inconsistent response bytes accepted")
			}
		})
	}
}

func TestQLBT14IndependentOracleRejectsHiddenAndIncorrectValues(t *testing.T) {
	for _, spec := range qlbT14Specs(1024) {
		oracle := newQLBT14Oracle(spec, 1024)
		response := proto.Clone(oracle.expected).(*measurev1.QueryResponse)
		if err := oracle.check(response); err != nil {
			t.Fatal(err)
		}
		if len(response.DataPoints) == 0 {
			continue
		}
		if spec.hidden {
			response.DataPoints[0].Fields = append(response.DataPoints[0].Fields, &measurev1.DataPoint_Field{Name: "hidden", Value: qlbT07Float(1)})
		} else {
			response.DataPoints[0].Fields[0].Value = qlbT07Float(999)
		}
		if oracle.check(response) == nil {
			t.Fatalf("T14 %s accepted an incorrect/hidden value", spec.name)
		}
	}
}

func qlbT14FixtureReport(t *testing.T, cardinality int, pid uint64) Result {
	t.Helper()
	iterations, warmup, err := qlbT14Schedule(cardinality)
	if err != nil {
		t.Fatal(err)
	}
	result := Result{Scenario: ScenarioQLBT14, Engine: engineMeasure, Mode: modeVec, Cardinality: cardinality, Correctness: "pass"}
	for _, spec := range qlbT14Specs(cardinality) {
		oracle := newQLBT14Oracle(spec, cardinality)
		m := map[string]uint64{}
		for _, key := range qlbT14MetricKeys() {
			m[key] = 0
		}
		calls, groups, output := uint64(iterations), uint64(iterations*oracle.groups), uint64(iterations*len(oracle.expected.DataPoints))
		m["benchmark_process_id"], m["warmup_queries"], m["checked_timed_samples"] = pid, uint64(warmup), 6
		// Synthetic report fixture sizes; runtime reports size actual retained RPC responses.
		m["checked_response_bytes"], m["min_response_bytes"], m["max_response_bytes"] = 384, 64, 64
		m["fixture_input_rows"] = uint64(oracle.inputRows)
		m["completed_groups_per_query"] = uint64(oracle.groups)
		m["selected_groups_per_query"] = uint64(len(oracle.expected.DataPoints))
		m["ql_batch_executions"], m["native_leaf_calls"] = calls, 2*calls
		m["native_source_rows"], m["native_frame_rows"] = calls*uint64(cardinality), calls*uint64(cardinality)
		m["final_data_points"], m["output_rows"] = output, output
		m["execution_clock_calls"], m["plan_cache_hits"] = calls, calls
		m["process_peak_reserved_bytes"], m["tracker_lease_charged_bytes"] = 1, 1
		prefix := "aggregate"
		m["aggregate_executions"], m["aggregate_input_rows"], m["aggregate_groups"] = calls, calls*uint64(oracle.aggregateRows), groups
		m["aggregate_state_lanes"] = 2 * calls
		if spec.mode == "native" {
			prefix = "merge"
			m["aggregate_executions"], m["aggregate_input_rows"], m["aggregate_groups"], m["aggregate_state_lanes"] = 0, 0, 0, 0
			m["ql_batch_executions"], m["native_leaf_calls"] = 2*calls, 4*calls
			m["merge_executions"], m["merge_branches"], m["merge_input_rows"], m["merge_groups"] = calls, 2*calls, 2*groups, groups
			m["merge_visits"], m["merge_state_lanes"] = 3*groups, 2*calls
		}
		m[prefix+"_reserved_bytes"], m[prefix+"_released_bytes"] = calls, calls
		if spec.mode == "native" {
			m["merge_state_bytes"] = groups * 18
		} else {
			m["aggregate_state_bytes"] = groups * 16
		}
		if spec.percent >= 0 {
			if groups > 0 {
				m["having_executions"] = calls
			}
			m["having_input_rows"], m["having_true_rows"], m["having_output_rows"], m["having_false_rows"] = groups, output, output, groups-output
			m["having_reserved_bytes"], m["having_released_bytes"] = calls, calls
		}
		if spec.membership {
			m["filter_executions"] = calls
			m["filter_input_rows"] = calls * uint64(oracle.inputRows)
			m["filter_true_rows"] = calls * uint64(oracle.aggregateRows)
			m["filter_output_rows"] = calls * uint64(oracle.aggregateRows)
			m["filter_false_rows"] = m["filter_input_rows"] - m["filter_output_rows"]
			m["filter_reserved_bytes"], m["filter_released_bytes"], m["aggregate_state_lanes"] = calls, calls, calls
			m["aggregate_state_bytes"] = groups * 8
		}
		latencies := make([]time.Duration, iterations)
		for index := range latencies {
			latencies[index] = time.Millisecond
		}
		result.Variants = append(result.Variants, Result{
			Scenario: ScenarioQLBT14, Engine: engineMeasure, Mode: modeVec, Cardinality: cardinality, Variant: spec.name,
			Correctness: "pass", QueryWorkers: 1, QueryMemoryMiB: 256, QueryIterations: iterations, TimedSamples: iterations, RawLatenciesNS: latencies,
			ExpectedRows: len(oracle.expected.DataPoints), ResponseRows: len(oracle.expected.DataPoints),
			ExpectedHash: oracle.hash, ApproxResultHash: oracle.hash, BatchMetrics: m,
			UnavailableMetrics: qlbT14UnavailableMetrics(),
		})
	}
	return result
}

func TestQLBT14RequestsBindThroughPublicAPI(t *testing.T) {
	for _, spec := range qlbT14Specs(1024) {
		request := newQLBT14Oracle(spec, 1024).query(1024, time.Unix(0, 0))
		statement, err := bydbql.Prepare(request.Query)
		if err != nil {
			t.Fatalf("%s prepare: %v", spec.name, err)
		}
		if _, err = statement.Bind(request.Params); err != nil {
			t.Fatalf("%s bind: %v", spec.name, err)
		}
	}
}
