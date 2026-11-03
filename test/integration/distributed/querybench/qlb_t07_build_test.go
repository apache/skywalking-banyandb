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
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/bydbql"
)

func TestQLBT07QueriesAndIndependentOracle(t *testing.T) {
	base := time.Unix(1700000000, 0)
	for _, cardinality := range []int{1024, 32768, 100000, 1000000} {
		for _, name := range qlbT07Variants {
			request, err := qlbT07Query(name, cardinality, base)
			if err != nil {
				t.Fatal(err)
			}
			statement, err := bydbql.Prepare(request.Query)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = statement.Bind(request.Params); err != nil {
				t.Fatal(err)
			}
			oracle, err := newQLBT07Oracle(name, cardinality)
			if err != nil {
				t.Fatal(err)
			}
			if name == "ql_shared_n_groups" {
				for _, rid := range []int64{0, 1, 7, int64(cardinality - 1)} {
					response := qlbT07ExpectedRID(rid)
					if _, err = oracle.check(response); err != nil {
						t.Fatal(err)
					}
					response.DataPoints[0].Fields[1].Value = qlbT07Float(1)
					if _, err = oracle.check(response); err == nil {
						t.Fatal("COUNT must remain int64, including zero and one")
					}
				}
				continue
			}
			if _, err = oracle.check(oracle.expected); err != nil {
				t.Fatal(err)
			}
			if name == "ql_sum_scalar" && cardinality == 1024 && oracle.expected.DataPoints[0].Fields[0].Value.GetInt().Value != -908 {
				t.Fatal("1024-row scalar sum differs from independently checked -908")
			}
			if name == "ql_multi_1024_groups" {
				first, second := oracle.expected.DataPoints[0].TagFamilies[0].Tags[0], oracle.expected.DataPoints[1].TagFamilies[0].Tags[0]
				if _, null := first.Value.GetValue().(*modelv1.TagValue_Null); !null || second.Value.GetStr().GetValue() != "" || proto.Equal(first.Value, second.Value) {
					t.Fatal("NULL and empty grouping keys must remain separate")
				}
				if cardinality == 1024 && oracle.expected.DataPoints[0].Fields[0].Value.GetInt().Value != 0 {
					t.Fatal("null-only group count must be int64 zero")
				}
			}
		}
	}
}

func qlbT07ExpectedRID(rid int64) *measurev1.QueryResponse {
	_, _, value, valid := qlbT07Values(int(rid))
	fields := []*measurev1.DataPoint_Field{
		{Name: "sum_value", Value: &modelv1.FieldValue{Value: &modelv1.FieldValue_Null{}}},
		{Name: "count_value", Value: intFieldValue(0)},
		{Name: "avg_value", Value: &modelv1.FieldValue{Value: &modelv1.FieldValue_Null{}}},
	}
	if valid {
		fields[0].Value, fields[1].Value, fields[2].Value = qlbT07Float(value), intFieldValue(1), qlbT07Float(value)
	}
	return &measurev1.QueryResponse{DataPoints: []*measurev1.DataPoint{qlbT07GroupedPoint("rid",
		&modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: rid}}}, fields)}}
}

func TestQLBT07ConfigUsesFrozenFocusedSchedule(t *testing.T) {
	for _, cardinality := range []int{1024, 32768} {
		iterations, warmup, err := qlbT07Schedule(cardinality)
		if err != nil {
			t.Fatal(err)
		}
		cfg := Config{
			RunBench: true, InContainer: true, Engine: engineMeasure, Mode: modeVec, Scenario: ScenarioQLBT07,
			Cardinality: cardinality, QueryWorkers: 1, QueryIterations: iterations, WarmupIterations: warmup, ByIDIterations: 1000, Writers: 4,
		}
		if err = cfg.Validate(); err != nil {
			t.Fatal(err)
		}
		for _, change := range []func(*Config){
			func(c *Config) { c.QueryWorkers = 8 },
			func(c *Config) { c.QueryIterations++ },
			func(c *Config) { c.WarmupIterations = 0 },
			func(c *Config) { c.Cardinality = 65536 },
			func(c *Config) { c.Cardinality = 100000 },
			func(c *Config) { c.Cardinality = 1000000 },
		} {
			invalid := cfg
			change(&invalid)
			if err = invalid.Validate(); err == nil {
				t.Fatal("T07 must retain its frozen successful-query protocol")
			}
		}
	}
}

func TestQLBT07ReportsRequireMeasuredWorkAndBoundedState(t *testing.T) {
	var results []Result
	for process, cardinality := range []int{1024, 32768} {
		iterations, warmup, err := qlbT07Schedule(cardinality)
		if err != nil {
			t.Fatal(err)
		}
		result := Result{Scenario: ScenarioQLBT07, Cardinality: cardinality, Correctness: "pass"}
		for _, name := range qlbT07Variants {
			oracle, oracleErr := newQLBT07Oracle(name, cardinality)
			if oracleErr != nil {
				t.Fatal(oracleErr)
			}
			metrics := qlbT07TestMetrics(name, iterations, cardinality, oracle.rows())
			metrics["benchmark_process_id"], metrics["warmup_queries"], metrics["checked_timed_samples"] = uint64(process+1), uint64(warmup), 1
			latencies := make([]time.Duration, iterations)
			for index := range latencies {
				latencies[index] = time.Millisecond
			}
			result.Variants = append(result.Variants, Result{
				Scenario: ScenarioQLBT07, Variant: name, Cardinality: cardinality, Correctness: "pass", QueryWorkers: 1,
				QueryIterations: iterations, TimedSamples: iterations, RawLatenciesNS: latencies,
				ExpectedRows: oracle.rows(), ResponseRows: oracle.rows(), ExpectedHash: 1, ApproxResultHash: 1, BatchMetrics: metrics,
			})
		}
		results = append(results, result)
	}
	if err := validateQLBT07Matrix(results); err != nil {
		t.Fatal(err)
	}
	if validateQLBT07Matrix(results[:1]) == nil {
		t.Fatal("missing 32768-row measurement accepted")
	}
	for _, mutate := range []func(*Result){
		func(r *Result) { r.Variants = r.Variants[:3] },
		func(r *Result) { r.Variants[0].TimedSamples-- },
		func(r *Result) { r.Variants[0].BatchMetrics["aggregate_input_rows"]-- },
		func(r *Result) { r.Variants[0].BatchMetrics["aggregate_groups"]++ },
		func(r *Result) { r.Variants[0].BatchMetrics["aggregate_live_bytes"] = 1 },
		func(r *Result) { r.Variants[0].BatchMetrics["aggregate_released_bytes"] = 0 },
		func(r *Result) { delete(r.Variants[0].BatchMetrics, "aggregate_key_capacity") },
	} {
		copyResult := results[0]
		copyResult.Variants = append([]Result(nil), copyResult.Variants...)
		copyResult.Variants[0].BatchMetrics = qlbT07TestMetrics(qlbT07Variants[0], 100, 1024, 1)
		copyResult.Variants[0].BatchMetrics["benchmark_process_id"] = 1
		copyResult.Variants[0].BatchMetrics["warmup_queries"] = 5
		copyResult.Variants[0].BatchMetrics["checked_timed_samples"] = 1
		mutate(&copyResult)
		if validateQLBT07Result(copyResult) == nil {
			t.Fatal("incomplete/forged T07 measurement accepted")
		}
	}
}

func qlbT07TestMetrics(name string, samples, cardinality, rows int) map[string]uint64 {
	metrics := make(map[string]uint64)
	for _, key := range qlbT07RequiredMetrics {
		metrics[key] = 0
	}
	total := uint64(samples)
	groups := qlbT07GroupCount(name, cardinality) * total
	lanes := uint64(2)
	if name == "ql_multi_1024_groups" {
		lanes = 4
	}
	metrics["ql_batch_executions"], metrics["aggregate_executions"], metrics["execution_clock_calls"] = total, total, total
	metrics["native_leaf_calls"] = total * 2
	inputRows := total * uint64(cardinality)
	metrics["native_source_rows"], metrics["native_frame_rows"], metrics["aggregate_input_rows"] = inputRows, inputRows, inputRows
	metrics["aggregate_groups"], metrics["aggregate_group_capacity"] = groups, groups
	metrics["aggregate_state_bytes"], metrics["aggregate_state_capacity"], metrics["aggregate_state_lanes"] = groups*lanes*8, groups*lanes*8, total*lanes
	metrics["aggregate_reserved_bytes"], metrics["aggregate_released_bytes"] = groups*lanes*8, groups*lanes*8
	metrics["final_data_points"], metrics["output_rows"] = total*uint64(rows), total*uint64(rows)
	return metrics
}
