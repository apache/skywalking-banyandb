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

	"github.com/apache/skywalking-banyandb/pkg/bydbql"
)

func TestQLBT03VariantOracleAndBinding(t *testing.T) {
	base := time.Unix(1700000000, 0)
	for _, cardinality := range []int{1024, 100000} {
		for _, name := range qlbT03Variants {
			request, expected, err := qlbT03Variant(name, cardinality, base)
			if err != nil {
				t.Fatal(err)
			}
			grammar, parseErr := bydbql.ParseQuery(request.Query)
			if parseErr != nil {
				t.Fatalf("%s: %v", name, parseErr)
			}
			if bindErr := bydbql.BindParams(grammar, request.Params); bindErr != nil {
				t.Fatalf("%s: %v", name, bindErr)
			}
			if qlbT01Hash(expected) == 0 {
				t.Fatalf("%s has no oracle hash", name)
			}
			if qlbT03NoScan(name) {
				if len(expected.DataPoints) != 0 {
					t.Fatalf("%s must be empty", name)
				}
				continue
			}
			if len(expected.DataPoints) == 0 {
				t.Fatalf("%s has no expected rows", name)
			}
			if name == "ql_repeated_projection" && len(expected.DataPoints[0].Fields) != 16 {
				t.Fatal("missing repeated projections")
			}
		}
		first, initial, err := qlbT03Variant("ql_time_exact_10pct", cardinality, base)
		if err != nil {
			t.Fatal(err)
		}
		rebound, changed, err := qlbT03Variant("ql_time_exact_10pct_rebound", cardinality, base)
		if err != nil {
			t.Fatal(err)
		}
		if first.Query != rebound.Query || qlbT01Hash(initial) == qlbT01Hash(changed) {
			t.Fatal("rebound must retain text and change bound results")
		}
		for _, selectivity := range []string{"10pct", "50pct"} {
			_, exact, buildErr := qlbT03Variant("ql_time_exact_"+selectivity, cardinality, base)
			if buildErr != nil {
				t.Fatal(buildErr)
			}
			_, legacy, buildErr := qlbT03Variant("ql_time_legacy_"+selectivity, cardinality, base)
			if buildErr != nil {
				t.Fatal(buildErr)
			}
			if !proto.Equal(exact, legacy) {
				t.Fatal("exact and legacy reference differ")
			}
		}
		_, pruned, err := qlbT03Variant("ql_time_or", cardinality, base)
		if err != nil {
			t.Fatal(err)
		}
		_, reference, err := qlbT03Variant("ql_time_or_fullscan_reference", cardinality, base)
		if err != nil {
			t.Fatal(err)
		}
		if !proto.Equal(pruned, reference) {
			t.Fatal("OR reference changed output")
		}
	}
}

func TestQLBT03ResultRejectsIncompleteAndForgedMeasurements(t *testing.T) {
	if validateQLBT03Result(Result{}) == nil {
		t.Fatal("missing variants accepted")
	}
	result := Result{Scenario: ScenarioQLBT03, Cardinality: 1024, Correctness: "pass"}
	for _, name := range qlbT03Variants {
		_, expected, err := qlbT03Variant(name, 1024, time.Unix(0, 0))
		if err != nil {
			t.Fatal(err)
		}
		result.Variants = append(result.Variants, Result{
			Scenario: ScenarioQLBT03, Variant: name, Cardinality: 1024, Correctness: "pass", QueryIterations: 1000,
			QueryWorkers: 1, TimedSamples: 1000, RawLatenciesNS: make([]time.Duration, 1000), ExpectedRows: len(expected.DataPoints), ResponseRows: len(expected.DataPoints),
			ExpectedHash: qlbT01Hash(expected), ApproxResultHash: qlbT01Hash(expected), BatchMetrics: qlbT03TestMetrics(name, 1000, len(expected.DataPoints)),
		})
	}
	for index := range result.Variants {
		for sample := range result.Variants[index].RawLatenciesNS {
			result.Variants[index].RawLatenciesNS[sample] = time.Millisecond
		}
	}
	if err := validateQLBT03Result(result); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*Result){
		func(r *Result) { r.Variants = r.Variants[1:] },
		func(r *Result) { r.Variants[0].ExpectedHash = 0; r.Variants[0].ApproxResultHash = 0 },
		func(r *Result) { r.Variants[0].TimedSamples = 0 },
		func(r *Result) { r.Variants[0].BatchMetrics = nil },
		func(r *Result) { r.Variants[0].BatchMetrics["execution_clock_calls"] = 0 },
		func(r *Result) { r.Variants[0].BatchMetrics["native_source_rows"] = 1 },
		func(r *Result) { r.Variants[0].RawLatenciesNS = r.Variants[0].RawLatenciesNS[:1] },
	} {
		copyResult := result
		copyResult.Variants = append([]Result(nil), result.Variants...)
		copyResult.Variants[0].BatchMetrics = qlbT03TestMetrics(qlbT03Variants[0], 1000, result.Variants[0].ExpectedRows)
		mutate(&copyResult)
		if validateQLBT03Result(copyResult) == nil {
			t.Fatal("incomplete measurement accepted")
		}
	}
}

func qlbT03TestMetrics(name string, samples, rows int) map[string]uint64 {
	metrics := map[string]uint64{}
	for _, key := range qlbT03RequiredMetrics {
		metrics[key] = 0
	}
	total := uint64(samples)
	metrics["execution_clock_calls"] = total
	metrics["final_data_points"] = total * uint64(rows)
	metrics["output_rows"] = total * uint64(rows)
	if !qlbT03NoScan(name) {
		sourceRows := rows
		if name == "ql_time_or" {
			sourceRows = 34
		}
		if name == "ql_time_or_fullscan_reference" {
			sourceRows = 64
		}
		metrics["ql_batch_executions"] = total
		metrics["native_leaf_calls"] = total * 2
		metrics["native_source_rows"] = total * uint64(sourceRows)
		metrics["native_frame_rows"] = total * uint64(sourceRows)
		if qlbT03Extended(name) {
			metrics["projection_calls"] = total
			metrics["limit_input_rows"] = total * uint64(rows)
			metrics["limit_output_rows"] = total * uint64(rows)
		}
		if name == "ql_time_or" || name == "ql_time_or_fullscan_reference" {
			metrics["filter_input_rows"] = total * uint64(sourceRows)
			metrics["filter_output_rows"] = total * uint64(rows)
		}
	}
	return metrics
}

func TestQLBT03MatrixRequiresBothCardinalities(t *testing.T) {
	first := Result{Scenario: ScenarioQLBT03, Cardinality: 1024}
	second := Result{Scenario: ScenarioQLBT03, Cardinality: 100000}
	if err := validateQLBT03Matrix([]Result{first, second}); err != nil {
		t.Fatal(err)
	}
	if validateQLBT03Matrix([]Result{first}) == nil {
		t.Fatal("missing cardinality accepted")
	}
	if validateQLBT03Matrix([]Result{first, first, second}) == nil {
		t.Fatal("repeated cardinality accepted")
	}
}
