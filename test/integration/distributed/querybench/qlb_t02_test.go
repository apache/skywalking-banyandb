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
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	bydbqlv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/bydbql/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

func runQLBT02Benchmark(ctx context.Context, conn *grpc.ClientConn, cfg Config, cardinality int, mode string, base time.Time) (Result, error) {
	result := Result{Engine: engineMeasure, Mode: mode, Scenario: ScenarioQLBT02, Cardinality: cardinality, Correctness: "pass"}
	if visibilityErr := waitForQLBT02Visibility(ctx, conn, cardinality, base, false); visibilityErr != nil {
		return result, visibilityErr
	}

	nativeClient := measurev1.NewMeasureServiceClient(conn)
	qlClient := bydbqlv1.NewBydbQLServiceClient(conn)
	for _, variant := range qlbT02Variants {
		native, ql, expected, buildErr := qlbT02Variant(variant, cardinality, base)
		if buildErr != nil {
			return result, buildErr
		}
		if variant == "native_wide_hidden" {
			if visibilityErr := waitForQLBT02Visibility(ctx, conn, cardinality, base, true); visibilityErr != nil {
				return result, visibilityErr
			}
		}
		expectedHash := qlbT01Hash(expected)
		query := func() (*measurev1.QueryResponse, error) {
			queryCtx, cancel := context.WithTimeout(ctx, queryTimeout(cardinality))
			defer cancel()
			if strings.HasPrefix(variant, "native_") {
				return nativeClient.Query(queryCtx, native)
			}
			response, queryErr := qlClient.Query(queryCtx, ql)
			if queryErr != nil {
				return nil, queryErr
			}
			if response.GetMeasureResult() == nil {
				return nil, fmt.Errorf("%s returned no measure_result", variant)
			}
			return response.GetMeasureResult(), nil
		}
		check := func(response *measurev1.QueryResponse) error {
			if len(response.GetDataPoints()) != len(expected.DataPoints) || qlbT01Hash(response) != expectedHash {
				return fmt.Errorf("%s correctness mismatch: expected rows=%d hash=%x, got rows=%d hash=%x", variant,
					len(expected.DataPoints), expectedHash, len(response.GetDataPoints()), qlbT01Hash(response))
			}
			return nil
		}
		for warmup := 0; warmup < cfg.WarmupIterations; warmup++ {
			response, queryErr := query()
			if queryErr != nil {
				return result, fmt.Errorf("%s warmup: %w", variant, queryErr)
			}
			if checkErr := check(response); checkErr != nil {
				return result, checkErr
			}
		}
		metricsBefore, metricsErr := qlbT02MetricsStart()
		if metricsErr != nil {
			return result, metricsErr
		}
		profiler, profileErr := startProfile(cfg.ReportDir, Scenario(string(ScenarioQLBT02)+"_"+variant), cardinality, mode, cfg.Profile)
		if profileErr != nil {
			return result, profileErr
		}
		latencies := make([]time.Duration, 0, cfg.QueryIterations)
		samples := make([]*measurev1.QueryResponse, 0, cfg.QueryIterations/100+2)
		before := captureProcessSnapshot()
		started := time.Now()
		var runErr error
		for iteration := 0; iteration < cfg.QueryIterations; iteration++ {
			queryStarted := time.Now()
			response, queryErr := query()
			latencies = append(latencies, time.Since(queryStarted))
			if queryErr != nil {
				runErr = queryErr
				break
			}
			if len(response.GetDataPoints()) != len(expected.DataPoints) {
				runErr = fmt.Errorf("%s returned %d rows, expected %d", variant, len(response.GetDataPoints()), len(expected.DataPoints))
				break
			}
			if iteration%100 == 0 || iteration == cfg.QueryIterations-1 {
				samples = append(samples, response)
			}
		}
		elapsed := time.Since(started)
		after := captureProcessSnapshot()
		profiles, stopErr := profiler.stop()
		if runErr != nil {
			return result, runErr
		}
		if stopErr != nil {
			return result, stopErr
		}
		for _, sample := range samples {
			if checkErr := check(sample); checkErr != nil {
				return result, checkErr
			}
		}
		if len(latencies) == 0 {
			return result, fmt.Errorf("%s produced no samples", variant)
		}
		latency, qps := summarizeLatencies(latencies, elapsed)
		metrics, unavailable, metricsErr := qlbT02MetricsFinish(metricsBefore, variant, len(latencies), len(expected.DataPoints))
		if metricsErr != nil {
			return result, metricsErr
		}
		result.Variants = append(result.Variants, Result{
			Engine: engineMeasure, Mode: mode, Scenario: ScenarioQLBT02, Variant: variant,
			Cardinality: cardinality, Correctness: "pass", QueryIterations: cfg.QueryIterations, QueryWorkers: 1,
			TimedSamples: len(latencies), RawLatenciesNS: latencies, ExpectedRows: len(expected.DataPoints), ExpectedHash: expectedHash,
			ResponseRows: len(expected.DataPoints), ApproxResultHash: expectedHash, Latency: latency, QPS: qps,
			Resources: resourceDelta(before, after), Allocations: allocationDelta(before, after, len(latencies)), Profiles: profiles,
			UnavailableMetrics: unavailable, BatchMetrics: metrics,
		})
	}
	return result, nil
}

var qlbT02Variants = []string{"native_raw", "ql_raw", "native_aggregate", "ql_aggregate", "native_wide_hidden", "ql_wide_hidden"}

func validateQLBT02Result(result Result) error {
	expected := make(map[string]bool)
	for _, name := range qlbT02Variants {
		expected[name] = false
	}
	for _, variant := range result.Variants {
		seen, exists := expected[variant.Variant]
		if !exists || seen {
			return fmt.Errorf("unexpected/repeated qlb_t02 variant %q", variant.Variant)
		}
		expected[variant.Variant] = true
		if variant.QueryIterations != 1000 || variant.TimedSamples != 1000 || variant.Correctness != "pass" ||
			variant.ResponseRows != variant.ExpectedRows || variant.ApproxResultHash != variant.ExpectedHash {
			return fmt.Errorf("invalid qlb_t02 variant %s", variant.Variant)
		}
	}
	for name, seen := range expected {
		if !seen {
			return fmt.Errorf("missing qlb_t02 variant %s", name)
		}
	}
	return nil
}

func qlbT02Variant(variant string, cardinality int, base time.Time) (*measurev1.QueryRequest, *bydbqlv1.QueryRequest, *measurev1.QueryResponse, error) {
	native, err := buildScenarioQuery(ScenarioScanAll, cardinality, base)
	if err != nil {
		return nil, nil, nil, err
	}
	native.Criteria = &modelv1.Criteria{Exp: &modelv1.Criteria_Condition{Condition: &modelv1.Condition{
		Name: benchTagID, Op: modelv1.Condition_BINARY_OP_EQ, Value: strTagValue("svc0"),
	}}}
	projection := "id, entity_id, total::field, value::field"
	suffix := " WHERE id = ?"
	expected := qlbT01Expected(cardinality, 0, base)
	if strings.HasSuffix(variant, "aggregate") {
		native.Criteria = nil
		native.TagProjection = tagProjection(benchTagID)
		native.FieldProjection = &measurev1.QueryRequest_FieldProjection{Names: []string{benchFieldValue}}
		native.GroupBy = &measurev1.QueryRequest_GroupBy{TagProjection: tagProjection(benchTagID), FieldName: benchFieldValue}
		native.Agg = &measurev1.QueryRequest_Aggregation{Function: modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM, FieldName: benchFieldValue}
		projection = "id, value::field, SUM(value)"
		suffix = " GROUP BY id, value::field"
		expected = &measurev1.QueryResponse{}
		entities, points := splitCardinality(cardinality)
		for service := 0; service < benchServiceMod; service++ {
			var sum int64
			count := 0
			for entity := service; entity < entities; entity += benchServiceMod {
				for point := 0; point < points; point++ {
					sum += int64((service+1)*1000 + point%1000)
					count++
				}
			}
			if count > 0 {
				expected.DataPoints = append(expected.DataPoints, &measurev1.DataPoint{
					TagFamilies: []*modelv1.TagFamily{{Name: benchTagFamily, Tags: []*modelv1.Tag{{Key: benchTagID, Value: strTagValue(fmt.Sprintf("svc%d", service))}}}},
					Fields:      []*measurev1.DataPoint_Field{{Name: benchFieldValue, Value: intFieldValue(sum)}},
				})
			}
		}
	}
	if strings.HasSuffix(variant, "wide_hidden") {
		native.Name = qlbT02WideMeasure
		native.TagProjection = tagProjection(benchTagEntityID)
		names := qlbT02WideFields()
		native.FieldProjection = &measurev1.QueryRequest_FieldProjection{Names: names}
		projected := []string{benchTagEntityID}
		for _, name := range names {
			projected = append(projected, name+"::field")
		}
		projection = strings.Join(projected, ", ")
		entities, points := splitCardinality(cardinality)
		expected = &measurev1.QueryResponse{}
		for entity := 0; entity < entities; entity += benchServiceMod {
			for point := 0; point < points; point++ {
				dp := &measurev1.DataPoint{
					Timestamp:   timestamppb.New(base.Add(time.Duration(point) * time.Second)),
					TagFamilies: []*modelv1.TagFamily{{Name: benchTagFamily, Tags: []*modelv1.Tag{{Key: benchTagEntityID, Value: strTagValue(fmt.Sprintf("entity_%d", entity))}}}},
				}
				for field := 0; field < 16; field++ {
					dp.Fields = append(dp.Fields, &measurev1.DataPoint_Field{Name: fmt.Sprintf("wide_%02d", field), Value: intFieldValue(int64(entity*100000 + point*16 + field))})
				}
				expected.DataPoints = append(expected.DataPoints, dp)
			}
		}
	}
	params := []*modelv1.TagValue{
		{Value: &modelv1.TagValue_Timestamp{Timestamp: native.TimeRange.Begin}},
		{Value: &modelv1.TagValue_Timestamp{Timestamp: native.TimeRange.End}},
	}
	if !strings.HasSuffix(variant, "aggregate") {
		params = append(params, strTagValue("svc0"))
	}
	params = append(params, &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: int64(cardinality)}}})
	ql := &bydbqlv1.QueryRequest{
		Query:  fmt.Sprintf("SELECT %s FROM MEASURE %s IN %s TIME BETWEEN ? AND ?%s LIMIT ?", projection, native.Name, benchGroupName, suffix),
		Params: params,
	}
	return native, ql, expected, nil
}

func TestQLBT02MatrixAndExpected(t *testing.T) {
	for _, variant := range qlbT02Variants {
		native, ql, expected, err := qlbT02Variant(variant, 1024, time.Unix(0, 0))
		if err != nil {
			t.Fatal(err)
		}
		if native == nil || ql == nil || len(expected.DataPoints) == 0 {
			t.Fatalf("empty variant %s", variant)
		}
		if strings.HasSuffix(variant, "aggregate") && len(expected.DataPoints) != 16 {
			t.Fatal("aggregate must cover 16 services")
		}
		if strings.HasSuffix(variant, "wide_hidden") && (len(expected.DataPoints) != 64 || len(expected.DataPoints[0].Fields) != 16 ||
			len(expected.DataPoints[0].TagFamilies[0].Tags) != 1) {
			t.Fatal("wide hidden projection mismatch")
		}
	}
	if validateQLBT02Result(Result{}) == nil {
		t.Fatal("missing variants accepted")
	}
}
