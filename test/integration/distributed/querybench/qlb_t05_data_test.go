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
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	bydbqlv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/bydbql/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

const qlbT05Cardinality = 1024

var qlbT05Shapes = []string{"legacy_simple", "typed_wide"}

// qlbT05Query retains one text per shape while alternating service and time parameters.
func qlbT05Query(shape string, binding int, base time.Time) (*bydbqlv1.QueryRequest, *measurev1.QueryResponse, error) {
	begin := binding * 8
	end := begin + 16
	timeParam := func(offset int, adjustment time.Duration) *modelv1.TagValue {
		return &modelv1.TagValue{Value: &modelv1.TagValue_Timestamp{
			Timestamp: timestamppb.New(base.Add(time.Duration(offset)*time.Second + adjustment)),
		}}
	}
	request := &bydbqlv1.QueryRequest{}
	switch shape {
	case "legacy_simple":
		request.Query = "SELECT id, value::FIELD FROM MEASURE service_cpm_minute IN sw_metric TIME BETWEEN ? AND ? WHERE id = ? LIMIT 1024"
		request.Params = []*modelv1.TagValue{timeParam(begin, 0), timeParam(end, -time.Millisecond), strTagValue(fmt.Sprintf("svc%d", binding))}
	case "typed_wide":
		columns := []string{"id AS node", "entity_id AS entity"}
		for index := 0; index < 16; index++ {
			columns = append(columns, fmt.Sprintf("value::FIELD AS value%02d", index))
		}
		request.Query = "SELECT " + strings.Join(columns, ", ") +
			" FROM sw_metric.service_cpm_minute WHERE id = ? AND time >= ? AND time < ?" +
			" AND time >= ? AND time < ? AND time >= ? AND time < ? AND time >= ? AND time < ?"
		request.Params = []*modelv1.TagValue{strTagValue(fmt.Sprintf("svc%d", binding))}
		for margin := 0; margin < 4; margin++ {
			request.Params = append(request.Params, timeParam(begin-margin, 0), timeParam(end+margin, 0))
		}
	default:
		return nil, nil, fmt.Errorf("unknown T05 query shape %q", shape)
	}
	expected := &measurev1.QueryResponse{}
	for _, point := range qlbT01Expected(qlbT05Cardinality, binding, base).DataPoints {
		offset := int(point.Timestamp.AsTime().Sub(base) / time.Second)
		if offset < begin || offset >= end {
			continue
		}
		if shape == "legacy_simple" {
			point.TagFamilies[0].Tags = point.TagFamilies[0].Tags[:1]
			point.Fields = point.Fields[1:]
		} else {
			point.TagFamilies[0].Tags[0].Key = "node"
			point.TagFamilies[0].Tags[1].Key = "entity"
			value := point.Fields[1].Value.GetInt().GetValue()
			point.Fields = nil
			for index := 0; index < 16; index++ {
				point.Fields = append(point.Fields, &measurev1.DataPoint_Field{
					Name: fmt.Sprintf("value%02d", index), Value: intFieldValue(value),
				})
			}
		}
		expected.DataPoints = append(expected.DataPoints, point)
	}
	return request, expected, nil
}

func validateQLBT05Result(result Result) error {
	if result.Scenario != ScenarioQLBT05 || result.Cardinality != qlbT05Cardinality || result.Correctness != "pass" {
		return fmt.Errorf("invalid T05 result header")
	}
	expected := map[string]bool{
		"legacy_simple_cache_off": false, "typed_wide_cache_off": false,
		"legacy_simple_cache_on": false, "typed_wide_cache_on": false,
	}
	for _, variant := range result.Variants {
		seen, exists := expected[variant.Variant]
		if !exists || seen {
			return fmt.Errorf("unexpected/repeated T05 variant %q", variant.Variant)
		}
		expected[variant.Variant] = true
		if variant.Scenario != ScenarioQLBT05 || variant.Cardinality != qlbT05Cardinality || variant.Correctness != "pass" ||
			variant.QueryIterations != 1000 || variant.QueryWorkers != 8 || variant.TimedSamples != 1000 || len(variant.RawLatenciesNS) != 1000 ||
			variant.ExpectedRows != 32 || variant.ResponseRows != variant.ExpectedRows || variant.ExpectedHash == 0 || variant.ApproxResultHash != variant.ExpectedHash {
			return fmt.Errorf("invalid T05 variant %s", variant.Variant)
		}
		for _, latency := range variant.RawLatenciesNS {
			if latency <= 0 {
				return fmt.Errorf("%s has an empty latency sample", variant.Variant)
			}
		}
		if err := validateQLBT05Metrics(variant.BatchMetrics, strings.HasSuffix(variant.Variant, "_cache_on")); err != nil {
			return fmt.Errorf("%s: %w", variant.Variant, err)
		}
	}
	for name, seen := range expected {
		if !seen {
			return fmt.Errorf("missing T05 variant %s", name)
		}
	}
	if err := validateQLBT05MatchedInputs(result.Variants); err != nil {
		return err
	}
	return validateQLBT05ProcessIdentities(result.Variants)
}

func validateQLBT05ProcessIdentities(variants []Result) error {
	processes := make(map[bool]uint64)
	for _, variant := range variants {
		enabled := strings.HasSuffix(variant.Variant, "_cache_on")
		pid := variant.BatchMetrics["benchmark_process_id"]
		if pid == 0 || (processes[enabled] != 0 && processes[enabled] != pid) {
			return fmt.Errorf("T05 cache setting has inconsistent child process identities")
		}
		processes[enabled] = pid
	}
	if len(processes) != 2 || processes[false] == processes[true] {
		return fmt.Errorf("T05 cache settings must run in separate processes")
	}
	return nil
}

func validateQLBT05MatchedInputs(variants []Result) error {
	for _, shape := range qlbT05Shapes {
		var off, on Result
		for _, variant := range variants {
			switch variant.Variant {
			case shape + "_cache_off":
				off = variant
			case shape + "_cache_on":
				on = variant
			}
		}
		if off.ExpectedHash != on.ExpectedHash || off.BatchMetrics["rebound_expected_hash"] == 0 ||
			off.BatchMetrics["rebound_expected_hash"] == off.ExpectedHash ||
			off.BatchMetrics["rebound_expected_hash"] != on.BatchMetrics["rebound_expected_hash"] {
			return fmt.Errorf("%s cache settings did not use matching changed-bind inputs", shape)
		}
	}
	return nil
}

func validateQLBT05Metrics(metrics map[string]uint64, enabled bool) error {
	required := []string{
		"benchmark_process_id",
		"plan_cache_hits", "plan_cache_compiles", "warmup_plan_compiles", "plan_cache_entries", "plan_cache_entry_bytes",
		"plan_cache_retained_bytes", "plan_cache_retired_bytes", "plan_cache_inflight", "plan_cache_compile_reserved_bytes", "plan_cache_waiters", "plan_cache_uncached_bytes",
		"execution_clock_calls", "intermediate_data_points", "post_close_live_results_delta", "post_close_reserved_bytes_delta",
		"post_close_owner_pins_delta", "post_close_flatten_pool_refs_delta",
	}
	for _, key := range required {
		if _, exists := metrics[key]; !exists {
			return fmt.Errorf("missing T05 metric %s", key)
		}
	}
	if metrics["execution_clock_calls"] != 1000 || metrics["intermediate_data_points"] != 0 {
		return fmt.Errorf("T05 execution activation mismatch")
	}
	for _, key := range []string{
		"plan_cache_retired_bytes", "plan_cache_inflight", "plan_cache_compile_reserved_bytes", "plan_cache_waiters", "plan_cache_uncached_bytes",
		"post_close_live_results_delta", "post_close_reserved_bytes_delta", "post_close_owner_pins_delta", "post_close_flatten_pool_refs_delta",
	} {
		if metrics[key] != 0 {
			return fmt.Errorf("T05 did not release %s=%d", key, metrics[key])
		}
	}
	if enabled {
		if metrics["plan_cache_hits"] != 1000 || metrics["plan_cache_compiles"] != 0 || metrics["warmup_plan_compiles"] != 1 ||
			metrics["plan_cache_entries"] == 0 || metrics["plan_cache_entries"] > 4000 || metrics["plan_cache_retained_bytes"] > 64<<20 {
			return fmt.Errorf("T05 warm cache did not reuse one compiled plan")
		}
	} else if metrics["plan_cache_hits"] != 0 || metrics["plan_cache_compiles"] != 1000 || metrics["warmup_plan_compiles"] != 20 ||
		metrics["plan_cache_entries"] != 0 || metrics["plan_cache_retained_bytes"] != 0 {
		return fmt.Errorf("T05 disabled cache did not compile every request without retaining plans")
	}
	return nil
}
