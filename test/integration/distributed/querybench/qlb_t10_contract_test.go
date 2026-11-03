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
	"google.golang.org/protobuf/types/known/timestamppb"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
)

func TestQLBT10OracleChecksValuesTypesNullsAndCompleteKeys(t *testing.T) {
	base := time.Unix(1700000000, 0)
	for _, name := range qlbT10Variants {
		response := &measurev1.QueryResponse{}
		for row := range 1024 {
			value, err := qlbT10ExpectedValue(name, row)
			if err != nil {
				t.Fatal(err)
			}
			response.DataPoints = append(response.DataPoints, &measurev1.DataPoint{
				Timestamp: timestamppb.New(base.Add(time.Duration(row) * time.Millisecond)),
				Fields:    []*measurev1.DataPoint_Field{{Name: "result", Value: value}},
			})
		}
		// Native row order is deliberately not an oracle requirement.
		for left, right := 0, len(response.DataPoints)-1; left < right; left, right = left+1, right-1 {
			response.DataPoints[left], response.DataPoints[right] = response.DataPoints[right], response.DataPoints[left]
		}
		expected, err := qlbT10ExpectedHash(name, 1024)
		if err != nil {
			t.Fatal(err)
		}
		checksum, err := checkQLBT10Result(response, name, 1024, base)
		if err != nil || checksum != expected {
			t.Fatalf("%s checksum: got %x expected %x: %v", name, checksum, expected, err)
		}
		for _, change := range []func(*measurev1.QueryResponse){
			func(r *measurev1.QueryResponse) { r.DataPoints = r.DataPoints[:1023] },
			func(r *measurev1.QueryResponse) { r.DataPoints[1].Timestamp = r.DataPoints[0].Timestamp },
			func(r *measurev1.QueryResponse) { r.DataPoints[0].Fields[0].Name = "wrong" },
			func(r *measurev1.QueryResponse) { r.DataPoints[0].Fields[0].Value = qlbT07Float(999.5) },
		} {
			changed := proto.Clone(response).(*measurev1.QueryResponse)
			change(changed)
			if _, err = checkQLBT10Result(changed, name, 1024, base); err == nil {
				t.Fatalf("%s oracle accepted changed values or missing keys", name)
			}
		}
	}
}

func TestQLBT10FrozenMemoryProtocol(t *testing.T) {
	cfg := Config{
		RunBench: true, InContainer: true, Engine: engineMeasure, Mode: modeVec, Scenario: ScenarioQLBT10,
		Cardinality: 1024, QueryWorkers: 1, QueryIterations: 100, WarmupIterations: 5, Writers: 4, ByIDIterations: 1000, QueryMemoryMiB: 256,
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	cfg.QueryMemoryMiB = 64
	if cfg.Validate() == nil {
		t.Fatal("T10 accepted a changed memory protocol")
	}
}

func TestQLBT10ReportsRejectChangedProtocolAndMissingEvidence(t *testing.T) {
	results := []Result{qlbT10FixtureResult(t, 1024, 11), qlbT10FixtureResult(t, 32768, 22)}
	if err := validateQLBT10Matrix(results); err != nil {
		t.Fatal(err)
	}
	if validateQLBT10Matrix(results[:1]) == nil {
		t.Fatal("T10 partial cardinality matrix accepted")
	}
	for _, change := range []func(*Result){
		func(r *Result) { r.Variants = r.Variants[:4] },
		func(r *Result) { r.Variants[1].Variant = r.Variants[0].Variant },
		func(r *Result) { r.Variants[0].QueryWorkers++ },
		func(r *Result) { r.Variants[0].QueryIterations-- },
		func(r *Result) { r.Variants[0].TimedSamples-- },
		func(r *Result) { r.Variants[0].QueryMemoryMiB = 64 },
		func(r *Result) { r.Variants[0].ExpectedHash++ },
		func(r *Result) { r.Variants[0].UnavailableMetrics = nil },
		func(r *Result) { r.Variants[0].BatchMetrics["storage_physical_scanned_rows"] = 1 },
		func(r *Result) { r.Variants[0].BatchMetrics["checked_timed_samples"]-- },
		func(r *Result) { r.Variants[0].BatchMetrics["native_leaf_calls"]++ },
		func(r *Result) { r.Variants[0].BatchMetrics["post_close_owner_pins_delta"] = 1 },
		func(r *Result) { r.Variants[0].BatchMetrics["intermediate_data_points"] = 1 },
		func(r *Result) { r.Variants[1].BatchMetrics["expression_full_column_allocations"] = 1 },
		func(r *Result) { r.Variants[2].BatchMetrics["expression_input_rows"]-- },
		func(r *Result) { r.Variants[2].BatchMetrics["expression_released_bytes"]-- },
		func(r *Result) { r.Variants[2].BatchMetrics["aggregate_executions"] = 1 },
		func(r *Result) { delete(r.Variants[2].BatchMetrics, "expression_buffer_bytes") },
	} {
		result := qlbT10FixtureResult(t, 1024, 11)
		change(&result)
		if validateQLBT10Result(result) == nil {
			t.Fatal("incomplete or forged T10 measurement accepted")
		}
	}
	for _, variant := range results[1].Variants {
		variant.BatchMetrics["benchmark_process_id"] = 11
	}
	if validateQLBT10Matrix(results) == nil {
		t.Fatal("T10 shared cluster process across cardinalities accepted")
	}
}

func qlbT10FixtureResult(t *testing.T, cardinality int, pid uint64) Result {
	t.Helper()
	iterations, warmup, err := qlbT10Schedule(cardinality)
	if err != nil {
		t.Fatal(err)
	}
	result := Result{Scenario: ScenarioQLBT10, Engine: engineMeasure, Mode: modeVec, Cardinality: cardinality, Correctness: "pass"}
	for _, name := range qlbT10Variants {
		metrics := make(map[string]uint64, len(qlbT10RequiredMetrics))
		for _, key := range qlbT10RequiredMetrics {
			metrics[key] = 0
		}
		total, rows := uint64(iterations), uint64(iterations)*uint64(cardinality)
		metrics["benchmark_process_id"], metrics["warmup_queries"], metrics["checked_timed_samples"] = pid, uint64(warmup), 6
		metrics["ql_batch_executions"], metrics["execution_clock_calls"], metrics["plan_cache_hits"] = total, total, total
		metrics["native_leaf_calls"], metrics["native_source_rows"], metrics["native_frame_rows"] = total*2, rows*2, rows*2
		metrics["final_data_points"], metrics["output_rows"] = rows, rows
		if name != "raw_control" {
			metrics["expression_executions"], metrics["expression_batches"] = total, total
			metrics["expression_input_rows"], metrics["expression_output_rows"] = rows, rows
			metrics["expression_reserved_bytes"], metrics["expression_released_bytes"] = rows*8, rows*8
		}
		expectedHash, hashErr := qlbT10ExpectedHash(name, cardinality)
		if hashErr != nil {
			t.Fatal(hashErr)
		}
		latencies := make([]time.Duration, iterations)
		for ordinal := range latencies {
			latencies[ordinal] = time.Millisecond
		}
		result.Variants = append(result.Variants, Result{
			Scenario: ScenarioQLBT10, Variant: name, Cardinality: cardinality, Engine: engineMeasure, Mode: modeVec,
			Correctness: "pass", QueryWorkers: 1, QueryIterations: iterations, QueryMemoryMiB: 256,
			TimedSamples: iterations, RawLatenciesNS: latencies, ExpectedRows: cardinality, ResponseRows: cardinality,
			ExpectedHash: expectedHash, ApproxResultHash: expectedHash, BatchMetrics: metrics,
			UnavailableMetrics: []string{"storage_physical_scanned_rows", "native_retained_frame_bytes", "total_parent_lease_charges"},
		})
	}
	return result
}
