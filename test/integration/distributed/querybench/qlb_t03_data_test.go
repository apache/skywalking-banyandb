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

var qlbT03Variants = []string{
	"ql_plain_legacy", "ql_alias", "ql_shorthand", "ql_repeated_projection",
	"ql_time_exact_10pct", "ql_time_exact_10pct_rebound", "ql_time_legacy_10pct",
	"ql_time_exact_50pct", "ql_time_legacy_50pct", "ql_time_or", "ql_time_or_fullscan_reference", "ql_empty", "ql_limit_zero",
}

func qlbT03NoScan(name string) bool { return name == "ql_empty" || name == "ql_limit_zero" }
func qlbT03Extended(name string) bool {
	return name != "ql_plain_legacy" && !strings.HasPrefix(name, "ql_time_legacy_")
}

// qlbT03Variant calculates its oracle from the deterministic write formula, never a query result.
func qlbT03Variant(name string, cardinality int, base time.Time) (*bydbqlv1.QueryRequest, *measurev1.QueryResponse, error) {
	if cardinality != 1024 && cardinality != 100000 {
		return nil, nil, fmt.Errorf("T03 cardinality must be 1024 or 100000, got %d", cardinality)
	}
	_, points := splitCardinality(cardinality)
	projection := "id, entity_id, total::FIELD, value::FIELD"
	source := fmt.Sprintf("MEASURE %s IN %s", benchMeasureName, benchGroupName)
	begin, end := 0, points
	selected := func(point int) bool { return point >= begin && point < end }
	timeParam := func(point int) *modelv1.TagValue {
		return &modelv1.TagValue{Value: &modelv1.TagValue_Timestamp{
			Timestamp: timestamppb.New(base.Add(time.Duration(point) * time.Second)),
		}}
	}
	request := &bydbqlv1.QueryRequest{}
	legacyTime := false
	switch name {
	case "ql_plain_legacy":
		projection = "id, value::FIELD"
		legacyTime = true
	case "ql_alias", "ql_shorthand", "ql_limit_zero":
		projection = "id AS node, value::FIELD AS reading"
		legacyTime = true
		if name == "ql_shorthand" {
			source = benchGroupName + "." + benchMeasureName
		}
	case "ql_repeated_projection":
		columns := []string{"id", "entity_id"}
		for field := 0; field < 16; field++ {
			columns = append(columns, fmt.Sprintf("value::FIELD AS value%02d", field))
		}
		projection = strings.Join(columns, ", ")
		legacyTime = true
	case "ql_time_exact_10pct", "ql_time_exact_10pct_rebound", "ql_time_legacy_10pct":
		end = max(1, points/10)
		if name == "ql_time_exact_10pct_rebound" {
			begin = points / 2
			end += begin
		}
		legacyTime = name == "ql_time_legacy_10pct"
	case "ql_time_exact_50pct", "ql_time_legacy_50pct":
		end = max(1, points/2)
		legacyTime = name == "ql_time_legacy_50pct"
	case "ql_time_or", "ql_time_or_fullscan_reference":
		middle := points / 2
		condition := "((time >= ? AND time < ?) OR (time >= ? AND time < ?))"
		request.Params = []*modelv1.TagValue{timeParam(0), timeParam(1), timeParam(middle), timeParam(middle + 1)}
		if name == "ql_time_or_fullscan_reference" {
			condition = "(" + condition + " OR time = ? OR time = ?)"
			request.Params = append(request.Params, timeParam(-1), timeParam(points))
		}
		request.Query = fmt.Sprintf("SELECT %s FROM %s WHERE id = 'svc0' AND %s", projection, source, condition)
		selected = func(point int) bool { return point == 0 || point == middle }
	case "ql_empty":
		begin = 1
		end = 1
	default:
		return nil, nil, fmt.Errorf("unknown T03 variant %q", name)
	}
	if request.Query == "" {
		if legacyTime {
			// Native TIME ends are inclusive; subtract one millisecond to match the SQL half-open bound.
			upper := &modelv1.TagValue{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(base.Add(time.Duration(end)*time.Second - time.Millisecond))}}
			request.Query = fmt.Sprintf("SELECT %s FROM %s TIME BETWEEN ? AND ? WHERE id = 'svc0' LIMIT %d", projection, source, cardinality)
			request.Params = []*modelv1.TagValue{timeParam(begin), upper}
		} else {
			request.Query = fmt.Sprintf("SELECT %s FROM %s WHERE id = 'svc0' AND time >= ? AND time < ?", projection, source)
			request.Params = []*modelv1.TagValue{timeParam(begin), timeParam(end)}
		}
	}
	if name == "ql_limit_zero" {
		request.Query = strings.TrimSuffix(request.Query, fmt.Sprintf("LIMIT %d", cardinality)) + "LIMIT 0"
	}
	expected := &measurev1.QueryResponse{}
	for _, point := range qlbT01Expected(cardinality, 0, base).DataPoints {
		offset := int(point.Timestamp.AsTime().Sub(base) / time.Second)
		if qlbT03NoScan(name) || !selected(offset) {
			continue
		}
		switch name {
		case "ql_plain_legacy":
			point.TagFamilies[0].Tags = point.TagFamilies[0].Tags[:1]
			point.Fields = point.Fields[1:]
		case "ql_alias", "ql_shorthand":
			point.TagFamilies[0].Tags = point.TagFamilies[0].Tags[:1]
			point.TagFamilies[0].Tags[0].Key = "node"
			point.Fields = point.Fields[1:]
			point.Fields[0].Name = "reading"
		case "ql_repeated_projection":
			value := point.Fields[1].Value.GetInt().Value
			point.Fields = nil
			for field := 0; field < 16; field++ {
				point.Fields = append(point.Fields, &measurev1.DataPoint_Field{Name: fmt.Sprintf("value%02d", field), Value: intFieldValue(value)})
			}
		}
		expected.DataPoints = append(expected.DataPoints, point)
	}
	return request, expected, nil
}

var qlbT03RequiredMetrics = []string{
	"ql_batch_executions", "native_batch_executions", "native_leaf_calls", "native_source_rows", "native_frame_rows",
	"intermediate_data_points", "final_data_points", "output_rows", "execution_clock_calls", "projection_calls", "projection_copied_value_bytes",
	"filter_input_rows", "filter_output_rows", "limit_input_rows", "limit_output_rows", "process_peak_reserved_bytes", "tracker_lease_charged_bytes",
	"post_close_live_results_delta", "post_close_reserved_bytes_delta", "post_close_owner_pins_delta", "post_close_flatten_pool_refs_delta",
}

func validateQLBT03Result(result Result) error {
	if result.Scenario != ScenarioQLBT03 || result.Correctness != "pass" || (result.Cardinality != 1024 && result.Cardinality != 100000) {
		return fmt.Errorf("invalid T03 result header")
	}
	expected := make(map[string]bool, len(qlbT03Variants))
	for _, name := range qlbT03Variants {
		expected[name] = false
	}
	for _, variant := range result.Variants {
		seen, exists := expected[variant.Variant]
		if !exists || seen {
			return fmt.Errorf("unexpected/repeated T03 variant %q", variant.Variant)
		}
		expected[variant.Variant] = true
		if variant.Scenario != ScenarioQLBT03 || variant.Cardinality != result.Cardinality || variant.QueryIterations != 1000 || variant.QueryWorkers != 1 ||
			variant.TimedSamples != 1000 || len(variant.RawLatenciesNS) != 1000 || variant.Correctness != "pass" || variant.ExpectedHash == 0 ||
			variant.ResponseRows != variant.ExpectedRows || variant.ApproxResultHash != variant.ExpectedHash {
			return fmt.Errorf("invalid T03 variant %s", variant.Variant)
		}
		for _, latency := range variant.RawLatenciesNS {
			if latency <= 0 {
				return fmt.Errorf("%s has an empty latency sample", variant.Variant)
			}
		}
		for _, key := range qlbT03RequiredMetrics {
			if _, exists = variant.BatchMetrics[key]; !exists {
				return fmt.Errorf("%s missing metric %s", variant.Variant, key)
			}
		}
		if err := validateQLBT03Metrics(variant.BatchMetrics, variant.Variant, 1000, variant.ExpectedRows); err != nil {
			return err
		}
	}
	for name, seen := range expected {
		if !seen {
			return fmt.Errorf("missing T03 variant %s", name)
		}
	}
	return validateQLBT03Pruning(result.Variants)
}

func validateQLBT03Metrics(metrics map[string]uint64, name string, samples, rows int) error {
	total := uint64(samples)
	native := total
	if qlbT03NoScan(name) {
		native = 0
	}
	if metrics["execution_clock_calls"] != total || metrics["ql_batch_executions"] != native || metrics["native_leaf_calls"] != native*2 {
		return fmt.Errorf("%s T03 activation mismatch: clock=%d QL=%d storage=%d, expected %d/%d/%d", name,
			metrics["execution_clock_calls"], metrics["ql_batch_executions"], metrics["native_leaf_calls"], total, native, native*2)
	}
	if metrics["intermediate_data_points"] != 0 || metrics["final_data_points"] != total*uint64(rows) || metrics["output_rows"] != total*uint64(rows) ||
		metrics["projection_copied_value_bytes"] != 0 {
		return fmt.Errorf("%s T03 materialization mismatch", name)
	}
	for _, key := range []string{"post_close_live_results_delta", "post_close_reserved_bytes_delta", "post_close_owner_pins_delta", "post_close_flatten_pool_refs_delta"} {
		if metrics[key] != 0 {
			return fmt.Errorf("%s leaked %s=%d", name, key, metrics[key])
		}
	}
	if !qlbT03NoScan(name) {
		if metrics["native_source_rows"] == 0 || metrics["native_frame_rows"] == 0 {
			return fmt.Errorf("%s did not observe native rows", name)
		}
		if qlbT03Extended(name) && (metrics["projection_calls"] == 0 || metrics["limit_output_rows"] != total*uint64(rows)) {
			return fmt.Errorf("%s did not activate typed projection/pagination", name)
		}
		if (name == "ql_time_or" || name == "ql_time_or_fullscan_reference") &&
			(metrics["filter_input_rows"] == 0 || metrics["filter_output_rows"] != total*uint64(rows)) {
			return fmt.Errorf("%s did not retain the complete time residual", name)
		}
	}
	if qlbT03NoScan(name) && (metrics["native_source_rows"] != 0 || metrics["native_frame_rows"] != 0 || metrics["projection_calls"] != 0) {
		return fmt.Errorf("%s scanned despite Empty/LIMIT 0", name)
	}
	return nil
}

func validateQLBT03Pruning(variants []Result) error {
	byName := make(map[string]Result, len(variants))
	for _, variant := range variants {
		byName[variant.Variant] = variant
	}
	sourceRows := func(name string) uint64 { return byName[name].BatchMetrics["native_source_rows"] }
	for _, selectivity := range []string{"10pct", "50pct"} {
		exact, legacy := byName["ql_time_exact_"+selectivity], byName["ql_time_legacy_"+selectivity]
		if exact.ExpectedRows != legacy.ExpectedRows || exact.ExpectedHash != legacy.ExpectedHash ||
			exact.BatchMetrics["native_source_rows"] != legacy.BatchMetrics["native_source_rows"] ||
			exact.BatchMetrics["native_frame_rows"] != legacy.BatchMetrics["native_frame_rows"] {
			return fmt.Errorf("T03 %s exact time lost native pruning/reference parity", selectivity)
		}
	}
	full := sourceRows("ql_plain_legacy")
	if sourceRows("ql_time_exact_10pct") >= sourceRows("ql_time_exact_50pct") || sourceRows("ql_time_exact_50pct") >= full {
		return fmt.Errorf("T03 exact time pruning did not reduce native source rows")
	}
	for _, name := range []string{"ql_alias", "ql_shorthand", "ql_repeated_projection"} {
		if sourceRows(name) != full {
			return fmt.Errorf("%s changed native scan cardinality", name)
		}
	}
	initial, rebound := byName["ql_time_exact_10pct"], byName["ql_time_exact_10pct_rebound"]
	if initial.ExpectedHash == rebound.ExpectedHash || initial.ExpectedRows != rebound.ExpectedRows ||
		sourceRows("ql_time_exact_10pct") != sourceRows("ql_time_exact_10pct_rebound") {
		return fmt.Errorf("T03 changed parameter ranges did not preserve pruning and fresh results")
	}
	pruned, reference := byName["ql_time_or"], byName["ql_time_or_fullscan_reference"]
	if pruned.ExpectedHash != reference.ExpectedHash || pruned.ExpectedRows != reference.ExpectedRows ||
		sourceRows("ql_time_or") >= sourceRows("ql_time_or_fullscan_reference") || sourceRows("ql_time_or_fullscan_reference") != full ||
		pruned.BatchMetrics["filter_input_rows"] >= reference.BatchMetrics["filter_input_rows"] {
		return fmt.Errorf("T03 residual OR envelope/reference work mismatch")
	}
	return nil
}

func validateQLBT03Matrix(results []Result) error {
	cards := map[int]bool{1024: false, 100000: false}
	active := false
	for _, result := range results {
		if result.Scenario != ScenarioQLBT03 {
			continue
		}
		active = true
		seen, exists := cards[result.Cardinality]
		if !exists || seen {
			return fmt.Errorf("unexpected/repeated T03 cardinality %d", result.Cardinality)
		}
		cards[result.Cardinality] = true
	}
	if active {
		for cardinality, seen := range cards {
			if !seen {
				return fmt.Errorf("missing T03 cardinality %d", cardinality)
			}
		}
	}
	return nil
}
