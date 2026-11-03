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

	"github.com/apache/skywalking-banyandb/pkg/bydbql"
)

func TestQLBT15TupleOracleAndQueries(t *testing.T) {
	specs := qlbT15Specs()
	if len(specs) != 14 {
		t.Fatalf("want 14 focused variants, got %d", len(specs))
	}
	for _, spec := range specs {
		o := newQLBT15Oracle(spec, 1024)
		want := max(1, 1024*spec.percent/100)
		if spec.empty {
			want = 0
		}
		if len(o.expected.DataPoints) != want {
			t.Fatal("incorrect unique tuple count")
		}
		if err := o.check(o.expected); err != nil {
			t.Fatal(err)
		}
		statement, err := bydbql.Prepare(o.query(1024, time.Unix(1700000000, 0)).Query)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = statement.Bind(o.query(1024, time.Unix(1700000000, 0)).Params); err != nil {
			t.Fatal(err)
		}
	}
}

func TestQLBT15OracleRejectsWrongCompleteTuples(t *testing.T) {
	spec := qlbT15Specs()[0]
	o := newQLBT15Oracle(spec, 1024)
	first, second := o.expected.DataPoints[0].TagFamilies[0].Tags, o.expected.DataPoints[1].TagFamilies[0].Tags
	if first[0].Value.GetStr().GetValue()+first[1].Value.GetStr().GetValue() != second[0].Value.GetStr().GetValue()+second[1].Value.GetStr().GetValue() {
		t.Fatal("fixture must expose ambiguous concatenation")
	}
	o.expected.DataPoints[1].TagFamilies[0].Tags[1].Value = strTagValue("wrong")
	if o.check(o.expected) == nil {
		t.Fatal("wrong tuple accepted")
	}
}

func TestQLBT15ConfigAndMetricContract(t *testing.T) {
	for _, card := range []int{1024, 8192} {
		iterations, warmup, err := qlbT15Schedule(card)
		if err != nil {
			t.Fatal(err)
		}
		cfg := Config{
			RunBench: true, InContainer: true, Engine: engineMeasure, Mode: modeVec, Scenario: ScenarioQLBT15, Cardinality: card,
			QueryWorkers: 1, QueryIterations: iterations, WarmupIterations: warmup, QueryMemoryMiB: 256, Profile: true, ByIDIterations: 1000, Writers: 4,
		}
		if err = cfg.Validate(); err != nil {
			t.Fatal(err)
		}
		cfg.Profile = false
		if cfg.validateQLBT15() == nil {
			t.Fatal("profiles must be mandatory")
		}
		o := newQLBT15Oracle(qlbT15Specs()[0], card)
		m := qlbT15TestMetrics(iterations, o)
		if err = validateQLBT15Metrics(m, iterations, o); err != nil {
			t.Fatal(err)
		}
		for _, key := range []string{"group_input_rows", "group_count", "key_bytes", "state_lanes", "post_close_owner_pins_delta"} {
			old := m[key]
			m[key]++
			if validateQLBT15Metrics(m, iterations, o) == nil {
				t.Fatalf("accepted wrong %s", key)
			}
			m[key] = old
		}
	}
}

func qlbT15TestMetrics(samples int, o *qlbT15Oracle) map[string]uint64 {
	calls := uint64(samples)
	m := map[string]uint64{
		"ql_batch_executions": calls, "native_leaf_calls": 2 * calls, "group_executions": calls,
		"group_input_rows": calls * uint64(o.inputRows), "group_count": calls * uint64(o.groups), "group_capacity": calls * uint64(o.groups),
		"final_data_points": calls * uint64(o.groups), "output_rows": calls * uint64(o.groups), "execution_clock_calls": calls,
		"plan_cache_hits": calls, "distinct_executions": calls, "group_reserved_bytes": 1, "group_released_bytes": 1,
		"process_peak_reserved_bytes": 1, "tracker_lease_charged_bytes": 1, "native_source_rows": 1, "native_frame_rows": 1,
	}
	if o.spec.reference {
		m["distinct_executions"], m["aggregate_executions"] = 0, calls
	}
	for unique := 0; unique < o.groups; unique++ {
		a, b := qlbT15Tuple(unique, o.spec.wide)
		m["key_bytes"] += uint64(20+len(a)+len(b)) * calls
	}
	m["key_capacity"] = m["key_bytes"]
	return m
}

func TestQLBT15ReportRejectsIncompleteEvidence(t *testing.T) {
	result := qlbT15FixtureReport(1024, 1)
	if err := validateQLBT15Result(result); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*Result){
		func(r *Result) { r.Variants = r.Variants[:13] },
		func(r *Result) { r.Variants[0].RawLatenciesNS = nil },
		func(r *Result) { r.Variants[0].Profiles = nil },
		func(r *Result) { r.Variants[0].UnavailableMetrics = nil },
		func(r *Result) { delete(r.Variants[0].BatchMetrics, "state_lanes") },
		func(r *Result) { r.Variants[0].BatchMetrics["checked_response_bytes"] = 0 },
		func(r *Result) { r.Variants[0].BatchMetrics["native_retained_frame_bytes"] = 0 },
	} {
		changed := result
		changed.Variants = append([]Result(nil), result.Variants...)
		changed.Variants[0].BatchMetrics = maps.Clone(result.Variants[0].BatchMetrics)
		mutate(&changed)
		if validateQLBT15Result(changed) == nil {
			t.Fatal("incomplete report accepted")
		}
	}
}

func TestQLBT15ScheduleSeparatesSuccessfulMatrixFromNativeCeiling(t *testing.T) {
	iterations, warmup, err := qlbT15Schedule(8192)
	if err != nil || iterations != 20 || warmup != 3 {
		t.Fatalf("8192-row successful protocol differs: %d/%d, %v", iterations, warmup, err)
	}
	if _, _, err = qlbT15Schedule(16384); err == nil {
		t.Fatal("16384-row wide native prerequisite rejection cannot count as successful throughput")
	}
}

func qlbT15FixtureReport(card int, pid uint64) Result {
	iterations, warmup, _ := qlbT15Schedule(card)
	result := Result{Scenario: ScenarioQLBT15, Engine: engineMeasure, Mode: modeVec, Cardinality: card, Correctness: "pass"}
	for _, spec := range qlbT15Specs() {
		o := newQLBT15Oracle(spec, card)
		m := qlbT15TestMetrics(iterations, o)
		for _, key := range qlbT15RequiredMetrics {
			if _, ok := m[key]; !ok {
				m[key] = 0
			}
		}
		m["benchmark_process_id"], m["warmup_queries"], m["checked_timed_samples"] = pid, uint64(warmup), 6
		m["fixture_input_rows"], m["completed_groups_per_query"] = uint64(o.inputRows), uint64(o.groups)
		m["min_response_bytes"], m["max_response_bytes"], m["checked_response_bytes"] = 2, 2, 12
		latencies := make([]time.Duration, iterations)
		for index := range latencies {
			latencies[index] = time.Millisecond
		}
		result.Variants = append(result.Variants, Result{
			Scenario: ScenarioQLBT15, Engine: engineMeasure, Mode: modeVec,
			Variant: spec.name, Cardinality: card, Correctness: "pass",
			QueryWorkers: 1, QueryMemoryMiB: 256, QueryIterations: iterations, TimedSamples: iterations, RawLatenciesNS: latencies,
			ExpectedRows: o.groups, ResponseRows: o.groups, ExpectedHash: o.hash, ApproxResultHash: o.hash, QPS: 1000,
			Latency: LatencyStats{P50Ms: 1, P95Ms: 1, P99Ms: 1}, Profiles: map[string]string{"cpu": "cpu.pprof", "heap": "heap.pprof"},
			BatchMetrics: m, UnavailableMetrics: qlbT15UnavailableMetrics(),
		})
	}
	return result
}
