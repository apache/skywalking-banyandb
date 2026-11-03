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
	"maps"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

func TestQLBT17Config(t *testing.T) {
	for _, shape := range []struct{ rows, calls, warmups int }{{1024, 100, 5}, {32768, 20, 3}} {
		cfg := Config{
			RunBench: true, InContainer: true, Engine: engineMeasure, Mode: modeVec, Scenario: Scenario("qlb_t17"), Cardinality: shape.rows,
			QueryWorkers: 1, QueryIterations: shape.calls, WarmupIterations: shape.warmups, QueryMemoryMiB: 256, Profile: true, ByIDIterations: 1000, Writers: 4,
		}
		if err := cfg.Validate(); err != nil {
			t.Fatal(err)
		}
		for _, mutate := range []func(*Config){
			func(c *Config) { c.QueryWorkers = 8 }, func(c *Config) { c.QueryMemoryMiB = 512 }, func(c *Config) { c.Profile = false },
			func(c *Config) { c.QueryIterations++ }, func(c *Config) { c.WarmupIterations++ }, func(c *Config) { c.Cardinality = 100000 },
		} {
			changed := cfg
			mutate(&changed)
			if changed.Validate() == nil {
				t.Fatal("T17 accepted a changed successful timing protocol")
			}
		}
	}
}

func TestQLBT17RawOraclePreservesOrderedPage(t *testing.T) {
	base := time.Unix(1700000000, 0)
	oracle := newQLBT17Oracle(qlbT17Spec{name: "topk"}, 16, 0, base)
	// In the literal 16-row fixture, descending first keys are
	// 3830,3353,3337,2939,2876,2755,2351,1524,1359,1179,746,737,492,171,166,NULL.
	ids := []int{8, 13, 1, 2, 4, 5, 6, 3, 15, 9}
	literal := &measurev1.QueryResponse{}
	for _, row := range ids {
		value := &modelv1.FieldValue{Value: &modelv1.FieldValue_Null{}}
		if row%5 != 0 {
			value = intFieldValue(int64(row - 48))
		}
		literal.DataPoints = append(literal.DataPoints, &measurev1.DataPoint{
			Timestamp: timestamppb.New(base.Add(time.Duration(row) * time.Millisecond)),
			Fields:    []*measurev1.DataPoint_Field{{Name: "rid", Value: intFieldValue(int64(row))}, {Name: "value", Value: value}},
		})
	}
	if !proto.Equal(oracle.expected, literal) {
		t.Fatal("T17 oracle differs from the worked 16-row page")
	}
	if err := oracle.check(literal); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*measurev1.QueryResponse){
		func(r *measurev1.QueryResponse) { r.DataPoints[0], r.DataPoints[1] = r.DataPoints[1], r.DataPoints[0] },
		func(r *measurev1.QueryResponse) { r.DataPoints[0].Fields[0].Name = "hidden" },
		func(r *measurev1.QueryResponse) { r.DataPoints[0].Fields[0].Value = qlbT07Float(-40) },
		func(r *measurev1.QueryResponse) { r.DataPoints[0].Timestamp = timestamppb.New(base) },
	} {
		changed := proto.Clone(literal).(*measurev1.QueryResponse)
		mutate(changed)
		if oracle.check(changed) == nil || qlbT17OrderedHash(changed) == oracle.hash {
			t.Fatal("T17 accepted wrong order/types/hidden fields/time")
		}
	}
}

func TestQLBT17ComposedOracleHasIndependentBoundResults(t *testing.T) {
	base := time.Unix(1700000000, 0)
	spec := qlbT17Spec{name: "composed_workers_32", workers: 32, composed: true}
	oracles := qlbT17Oracles(spec, 1024, base)
	// Literal row arithmetic grouped by row%16, with NULL every fifth row.
	for binding, want := range []struct {
		host        string
		total, peak int64
	}{{"host_01", 637, 48}, {"host_03", 622, 48}, {"host_04", 345, 46}} {
		expected := &measurev1.QueryResponse{DataPoints: []*measurev1.DataPoint{{
			TagFamilies: []*modelv1.TagFamily{{Name: benchTagFamily, Tags: []*modelv1.Tag{{Key: "node", Value: strTagValue(want.host)}}}},
			Fields:      []*measurev1.DataPoint_Field{{Name: "total", Value: intFieldValue(want.total)}, {Name: "peak", Value: intFieldValue(want.peak)}},
		}}}
		if !proto.Equal(oracles[binding].expected, expected) {
			t.Fatalf("T17 bound fixture %d differs: %s", binding, oracles[binding].expected)
		}
		if binding > 0 && oracles[binding].query(base).Query != oracles[0].query(base).Query {
			t.Fatal("bound composition changed SQL text")
		}
	}
}

func TestQLBT17ReportRejectsIncompleteOrInventedEvidence(t *testing.T) {
	result := qlbT17FixtureReport(1024, 1)
	if err := validateQLBT17Result(result); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*Result){
		func(r *Result) { r.Variants = r.Variants[:4] },
		func(r *Result) { r.Variants[0].RawLatenciesNS = nil },
		func(r *Result) { r.Variants[1].Profiles = nil },
		func(r *Result) { r.Variants[0].BatchMetrics["sort_key_root_calls"]++ },
		func(r *Result) { r.Variants[0].BatchMetrics["sort_key_kernel_rows"]++ },
		func(r *Result) { r.Variants[1].BatchMetrics["sort_peak_candidates"]++ },
		func(r *Result) { r.Variants[0].BatchMetrics["sort_comparisons"] = 1000000000 },
		func(r *Result) { r.Variants[2].BatchMetrics["filter_input_rows"]++ },
		func(r *Result) { r.Variants[4].BatchMetrics["bound_set_2_ordered_hash"]++ },
		func(r *Result) { r.Variants[3].BatchMetrics["post_close_owner_pins_delta"]++ },
		func(r *Result) { r.Variants[0].BatchMetrics["checked_response_bytes"] = 0 },
	} {
		changed := result
		changed.Variants = append([]Result(nil), result.Variants...)
		for index := range changed.Variants {
			changed.Variants[index].BatchMetrics = maps.Clone(result.Variants[index].BatchMetrics)
		}
		mutate(&changed)
		if validateQLBT17Result(changed) == nil {
			t.Fatal("T17 accepted incomplete or fabricated evidence")
		}
	}
}

func qlbT17FixtureReport(rows int, pid uint64) Result {
	_, warmup, _ := qlbT17Schedule(rows)
	base := time.Unix(1700000000, 0)
	result := Result{Scenario: ScenarioQLBT17, Engine: engineMeasure, Mode: modeVec, Cardinality: rows, Correctness: "pass"}
	for _, spec := range qlbT17Specs() {
		samples := spec.calls(rows)
		calls := uint64(samples)
		oracles := qlbT17Oracles(spec, rows, base)
		m := map[string]uint64{}
		for _, key := range qlbT17MetricKeys() {
			m[key] = 0
		}
		var scanned, passing, groups, sortRows uint64
		for index := 0; index < samples; index++ {
			o := oracles[index%len(oracles)]
			scanned += uint64(o.scanned)
			passing += uint64(o.passing)
			groups += uint64(o.groups)
			sortRows += uint64(o.sortRows)
		}
		outputs := calls * uint64(len(oracles[0].expected.DataPoints))
		m["benchmark_process_id"], m["warmup_queries"], m["checked_timed_samples"] = pid, uint64(warmup), 6
		m["fixture_input_rows"], m["fixture_base_unix_nanos"], m["bind_sets"] = uint64(rows), uint64(base.UnixNano()), uint64(len(oracles))
		m["ql_batch_executions"], m["native_leaf_calls"], m["execution_clock_calls"], m["plan_cache_hits"] = calls, 2*calls, calls, calls
		m["native_source_rows"], m["native_frame_rows"], m["final_data_points"], m["output_rows"] = scanned, scanned, outputs, outputs
		keys, k, kernelRows := uint64(3), uint64(15), sortRows
		if spec.composed {
			keys, k, kernelRows = 2, 2, 0
		}
		candidates := k * calls
		if spec.full {
			candidates = sortRows
		}
		m["sort_executions"], m["sort_input_rows"], m["sort_output_rows"], m["sort_batches"] = calls, sortRows, outputs, calls
		m["sort_key_evaluations"], m["sort_key_root_calls"], m["sort_key_kernel_rows"] = keys*sortRows, keys*calls, kernelRows
		m["sort_comparisons"], m["sort_k"] = sortRows*2, k*calls
		m["sort_candidates"], m["sort_peak_candidates"], m["sort_candidate_capacity"], m["sort_accepted_rows"] = candidates, candidates, candidates, candidates
		m["sort_descriptor_bytes"], m["sort_peak_bytes"], m["sort_reserved_bytes"], m["sort_released_bytes"] = candidates*256, candidates*256, candidates*256, candidates*256
		if spec.composed {
			m["filter_executions"], m["filter_input_rows"], m["filter_output_rows"] = calls, scanned, passing
			m["aggregate_executions"], m["aggregate_input_rows"], m["aggregate_groups"], m["aggregate_state_lanes"] = calls, passing, groups, 3*calls
			m["aggregate_state_bytes"] = 24 * groups
			m["having_executions"], m["having_input_rows"], m["having_output_rows"] = calls, groups, sortRows
		}
		m["process_peak_reserved_bytes"], m["tracker_lease_charged_bytes"] = 4096, 8192
		m["plan_cache_entries"], m["plan_cache_entry_bytes"], m["plan_cache_retained_bytes"] = 1, 1024, 1024
		m["checked_response_bytes"], m["min_response_bytes"], m["max_response_bytes"] = 12, 2, 2
		for index, o := range oracles {
			m[fmt.Sprintf("bound_set_%d_queries", index)] = uint64((samples + len(oracles) - 1 - index) / len(oracles))
			m[fmt.Sprintf("bound_set_%d_ordered_hash", index)] = o.hash
		}
		latencies := make([]time.Duration, samples)
		for index := range latencies {
			latencies[index] = time.Millisecond
		}
		result.Variants = append(result.Variants, Result{
			Scenario: ScenarioQLBT17, Engine: engineMeasure, Mode: modeVec, Cardinality: rows, Variant: spec.name, Correctness: "pass",
			QueryWorkers: spec.workers, QueryMemoryMiB: 256, QueryIterations: samples, TimedSamples: samples, RawLatenciesNS: latencies,
			ExpectedRows: len(oracles[0].expected.DataPoints), ResponseRows: len(oracles[0].expected.DataPoints), ExpectedHash: oracles[0].hash, ApproxResultHash: oracles[0].hash,
			QPS: 1000, Latency: LatencyStats{P50Ms: 1, P95Ms: 1, P99Ms: 1}, Profiles: map[string]string{"cpu": "cpu.pprof", "heap": "heap.pprof"},
			Allocations:  AllocationStats{MetricSource: "runtime.MemStats", MallocsDelta: calls, TotalAllocDelta: calls * 1024, MallocsPerQuery: 1, AllocBytesPerQuery: 1024},
			BatchMetrics: m, UnavailableMetrics: qlbT17UnavailableMetrics(),
		})
	}
	return result
}
