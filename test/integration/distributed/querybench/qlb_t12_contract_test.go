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

	"google.golang.org/protobuf/types/known/timestamppb"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/pkg/bydbql"
)

func TestQLBT12FrozenProtocol(t *testing.T) {
	cfg := Config{
		RunBench: true, InContainer: true, Engine: engineMeasure, Mode: modeVec, Scenario: ScenarioQLBT12,
		Cardinality: 1024, QueryWorkers: 1, QueryIterations: 100, WarmupIterations: 5, Writers: 4, ByIDIterations: 1000, QueryMemoryMiB: 256,
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	larger := cfg
	larger.Cardinality, larger.QueryIterations, larger.WarmupIterations = 16384, 20, 3
	if err := larger.Validate(); err != nil {
		t.Fatal(err)
	}
	larger.Cardinality = 32768
	if larger.Validate() == nil {
		t.Fatal("T12 accepted the cardinality blocked by response-encoding admission")
	}
	for _, change := range []func(*Config){
		func(c *Config) { c.QueryMemoryMiB = 64 },
		func(c *Config) { c.QueryWorkers = 2 },
		func(c *Config) { c.QueryIterations-- },
		func(c *Config) { c.WarmupIterations-- },
	} {
		changed := cfg
		change(&changed)
		if changed.Validate() == nil {
			t.Fatal("changed T12 benchmark protocol accepted")
		}
	}
}

func TestQLBT12OracleMatchesKnownFixtureResults(t *testing.T) {
	for _, test := range []struct {
		name       string
		selected   int
		sum, count int64
	}{
		{"aggregate_narrow_p00", 0, 0, 0},
		{"aggregate_narrow_p01", 1, 0, 0},
		{"aggregate_wide_p50", 50, -920, 40},
		{"aggregate_wide_p100", 100, -131, 80},
	} {
		oracle, err := newQLBT12Oracle(test.name, 100)
		if err != nil || oracle.selected != test.selected || oracle.sum != test.sum || oracle.count != test.count {
			t.Fatalf("%s: %+v %v", test.name, oracle, err)
		}
	}
	base := time.Unix(1700000000, 0)
	oracle, err := newQLBT12Oracle("raw_wide_p01", 200)
	if err != nil {
		t.Fatal(err)
	}
	response := &measurev1.QueryResponse{}
	for _, row := range []int{100, 0} {
		value, valueErr := qlbT10ExpectedValue("raw_control", row)
		if valueErr != nil {
			t.Fatal(valueErr)
		}
		response.DataPoints = append(response.DataPoints, &measurev1.DataPoint{
			Timestamp: timestamppb.New(base.Add(time.Duration(row) * time.Millisecond)),
			Fields:    []*measurev1.DataPoint_Field{{Name: "result", Value: value}},
		})
	}
	hash, err := oracle.check(response, base)
	if err != nil || hash != oracle.hash {
		t.Fatalf("independent raw oracle: %x %x %v", hash, oracle.hash, err)
	}
	response.DataPoints[1].Timestamp = timestamppb.New(base.Add(time.Millisecond))
	if _, err = oracle.check(response, base); err == nil {
		t.Fatal("unselected row accepted")
	}
}

func TestQLBT12QueriesKeepCompleteRawScanAndHiddenInputs(t *testing.T) {
	schema := &databasev1.Measure{Fields: []*databasev1.FieldSpec{
		{Name: "v", FieldType: databasev1.FieldType_FIELD_TYPE_INT},
		{Name: "f", FieldType: databasev1.FieldType_FIELD_TYPE_FLOAT},
		{Name: "score", FieldType: databasev1.FieldType_FIELD_TYPE_INT},
	}}
	schemas := map[bydbql.MeasureSource]*databasev1.Measure{{Group: benchGroupName, Name: qlbT12Measure}: schema}
	for _, name := range qlbT12Variants() {
		request, err := qlbT12Query(name, 1024, time.Unix(1700000000, 0))
		if err != nil {
			t.Fatal(err)
		}
		prepared, err := bydbql.Prepare(request.Query)
		if err != nil {
			t.Fatal(err)
		}
		template, err := bydbql.NewTransformer(nil).CompileMeasureWithSchemas(t.Context(), prepared, schemas)
		if err != nil {
			t.Fatal(err)
		}
		bound, err := template.BindParams(request.Params, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		if bound.Plan == nil || bound.Plan.Filter == nil || bound.Plan.NativeAggregate != nil || bound.Request.Agg != nil ||
			bound.Request.Limit != ^uint32(0) || bound.Request.Offset != 0 {
			t.Fatalf("%s lost full raw residual execution", name)
		}
	}
}

func TestQLBT12ReportsRejectChangedProtocolAndMissingEvidence(t *testing.T) {
	results := []Result{qlbT12FixtureResult(t, 1024, 11), qlbT12FixtureResult(t, 16384, 22)}
	if err := validateQLBT12Matrix(results); err != nil {
		t.Fatal(err)
	}
	if validateQLBT12Matrix(results[:1]) == nil {
		t.Fatal("T12 accepted a partial cardinality matrix")
	}
	for _, test := range []struct {
		change func(*Result)
		name   string
	}{
		{name: "missing variant", change: func(r *Result) { r.Variants = r.Variants[:15] }},
		{name: "duplicate variant", change: func(r *Result) { r.Variants[1].Variant = r.Variants[0].Variant }},
		{name: "missing counter", change: func(r *Result) { delete(r.Variants[0].BatchMetrics, "filter_selection_bytes") }},
		{name: "forged true count", change: func(r *Result) { r.Variants[0].BatchMetrics["filter_true_rows"]++ }},
		{name: "incomplete raw scan", change: func(r *Result) { r.Variants[0].BatchMetrics["filter_input_rows"]-- }},
		{name: "wrong aggregate input", change: func(r *Result) { r.Variants[2].BatchMetrics["aggregate_input_rows"]++ }},
		{name: "native aggregate leaf", change: func(r *Result) { r.Variants[2].BatchMetrics["native_batch_executions"]++ }},
		{name: "unexpected merge", change: func(r *Result) { r.Variants[2].BatchMetrics["merge_branches"]++ }},
		{name: "filter leak", change: func(r *Result) { r.Variants[0].BatchMetrics["filter_live_bytes"] = 1 }},
		{name: "owner leak", change: func(r *Result) { r.Variants[0].BatchMetrics["post_close_owner_pins_delta"] = 1 }},
		{name: "value copies", change: func(r *Result) { r.Variants[0].BatchMetrics["filter_copied_bytes"] = 1 }},
		{name: "filter byte balance", change: func(r *Result) { r.Variants[0].BatchMetrics["filter_released_bytes"]++ }},
		{name: "missing filter reservations", change: func(r *Result) {
			r.Variants[0].BatchMetrics["filter_reserved_bytes"] = 0
			r.Variants[0].BatchMetrics["filter_released_bytes"] = 0
		}},
		{name: "aggregate byte balance", change: func(r *Result) { r.Variants[2].BatchMetrics["aggregate_released_bytes"]++ }},
		{name: "raw aggregation", change: func(r *Result) { r.Variants[0].BatchMetrics["aggregate_executions"]++ }},
		{name: "worker count", change: func(r *Result) { r.Variants[0].QueryWorkers++ }},
		{name: "iterations", change: func(r *Result) { r.Variants[0].QueryIterations-- }},
		{name: "timed samples", change: func(r *Result) { r.Variants[0].TimedSamples-- }},
		{name: "missing timings", change: func(r *Result) { r.Variants[0].RawLatenciesNS = nil }},
		{name: "empty timing", change: func(r *Result) { r.Variants[0].RawLatenciesNS[0] = 0 }},
		{name: "memory budget", change: func(r *Result) { r.Variants[0].QueryMemoryMiB = 64 }},
		{name: "result hash", change: func(r *Result) { r.Variants[0].ExpectedHash++ }},
		{name: "result rows", change: func(r *Result) { r.Variants[0].ResponseRows++ }},
		{name: "warmup count", change: func(r *Result) { r.Variants[0].BatchMetrics["warmup_queries"]-- }},
		{name: "correctness sampling", change: func(r *Result) { r.Variants[0].BatchMetrics["checked_timed_samples"]-- }},
		{name: "process identity", change: func(r *Result) { r.Variants[0].BatchMetrics["benchmark_process_id"] = 0 }},
		{name: "hidden width", change: func(r *Result) { r.Variants[0].BatchMetrics["hidden_predicate_fields"]++ }},
		{name: "selectivity", change: func(r *Result) { r.Variants[0].BatchMetrics["target_selectivity_percent"]++ }},
		{name: "undisclosed unavailable metric", change: func(r *Result) { r.Variants[0].UnavailableMetrics = nil }},
		{name: "unavailable reported as measured", change: func(r *Result) { r.Variants[0].BatchMetrics["storage_physical_scanned_rows"] = 1 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			result := qlbT12FixtureResult(t, 1024, 11)
			test.change(&result)
			if validateQLBT12Result(result) == nil {
				t.Fatal("T12 accepted incomplete or forged measurement")
			}
		})
	}
	for _, variant := range results[1].Variants {
		variant.BatchMetrics["benchmark_process_id"] = 11
	}
	if validateQLBT12Matrix(results) == nil {
		t.Fatal("T12 accepted a shared cluster process across cardinalities")
	}
}

func qlbT12FixtureResult(t *testing.T, cardinality int, pid uint64) Result {
	t.Helper()
	iterations, warmup, err := qlbT12Schedule(cardinality)
	if err != nil {
		t.Fatal(err)
	}
	result := Result{Scenario: ScenarioQLBT12, Engine: engineMeasure, Mode: modeVec, Cardinality: cardinality, Correctness: "pass"}
	for _, name := range qlbT12Variants() {
		oracle, oracleErr := newQLBT12Oracle(name, cardinality)
		if oracleErr != nil {
			t.Fatal(oracleErr)
		}
		metrics := make(map[string]uint64, len(qlbT12RequiredMetrics))
		for _, key := range qlbT12RequiredMetrics {
			metrics[key] = 0
		}
		calls := uint64(iterations)
		input, passed := calls*uint64(cardinality), calls*uint64(oracle.selected)
		output := calls * uint64(oracle.outputRows())
		metrics["benchmark_process_id"], metrics["warmup_queries"], metrics["checked_timed_samples"] = pid, uint64(warmup), 6
		metrics["target_selectivity_percent"] = uint64(oracle.spec.percent)
		metrics["hidden_predicate_fields"] = 1
		if oracle.spec.wide {
			metrics["hidden_predicate_fields"] = 2
		}
		metrics["ql_batch_executions"], metrics["execution_clock_calls"], metrics["plan_cache_hits"] = calls, calls, calls
		metrics["native_leaf_calls"], metrics["native_source_rows"], metrics["native_frame_rows"] = calls*2, input*2, input*2
		metrics["final_data_points"], metrics["output_rows"] = output, output
		metrics["filter_executions"], metrics["filter_batches"], metrics["filter_input_rows"] = calls, calls, input
		metrics["filter_output_rows"], metrics["filter_true_rows"], metrics["filter_false_rows"] = passed, passed, input-passed
		metrics["filter_reserved_bytes"], metrics["filter_released_bytes"] = input*4, input*4
		if oracle.spec.aggregate {
			metrics["aggregate_executions"], metrics["aggregate_input_rows"], metrics["aggregate_groups"] = calls, passed, calls
			metrics["aggregate_reserved_bytes"], metrics["aggregate_released_bytes"] = calls*4096, calls*4096
		}
		latencies := make([]time.Duration, iterations)
		for ordinal := range latencies {
			latencies[ordinal] = time.Millisecond
		}
		result.Variants = append(result.Variants, Result{
			Scenario: ScenarioQLBT12, Variant: name, Cardinality: cardinality, Engine: engineMeasure, Mode: modeVec,
			Correctness: "pass", QueryWorkers: 1, QueryIterations: iterations, QueryMemoryMiB: 256,
			TimedSamples: iterations, RawLatenciesNS: latencies, ExpectedRows: oracle.outputRows(), ResponseRows: oracle.outputRows(),
			ExpectedHash: oracle.hash, ApproxResultHash: oracle.hash, BatchMetrics: metrics,
			UnavailableMetrics: []string{"storage_physical_scanned_rows", "native_retained_frame_bytes", "total_parent_lease_charges"},
		})
	}
	return result
}
