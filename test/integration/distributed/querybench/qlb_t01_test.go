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
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	bydbqlv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/bydbql/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

func qlbT01Expected(cardinality, service int, base time.Time) *measurev1.QueryResponse {
	entities, pointsEach := splitCardinality(cardinality)
	response := &measurev1.QueryResponse{}
	for entity := service; entity < entities; entity += benchServiceMod {
		for point := 0; point < pointsEach; point++ {
			response.DataPoints = append(response.DataPoints, &measurev1.DataPoint{
				Timestamp: timestamppb.New(base.Add(time.Duration(point) * time.Second)),
				TagFamilies: []*modelv1.TagFamily{{Name: benchTagFamily, Tags: []*modelv1.Tag{
					{Key: benchTagID, Value: strTagValue(fmt.Sprintf("svc%d", service))},
					{Key: benchTagEntityID, Value: strTagValue(fmt.Sprintf("entity_%d", entity))},
				}}},
				Fields: []*measurev1.DataPoint_Field{
					{Name: benchFieldTotal, Value: intFieldValue(int64(100 + point%100))},
					{Name: benchFieldValue, Value: intFieldValue(int64((service+1)*1000 + point%1000))},
				},
			})
		}
	}
	return response
}

// qlbT01Hash ignores only storage identity, retaining timestamps and all projected values.
func qlbT01Hash(response *measurev1.QueryResponse) uint64 {
	normalized := proto.Clone(response).(*measurev1.QueryResponse)
	for _, point := range normalized.DataPoints {
		point.Sid = 0
		point.Version = 0
	}
	return hashResponse(normalized)
}

func runQLBT01Benchmark(ctx context.Context, conn *grpc.ClientConn, cfg Config, cardinality int, mode string, base time.Time) (Result, error) {
	result := Result{Engine: engineMeasure, Mode: mode, Scenario: ScenarioQLBT01, Cardinality: cardinality, Correctness: "pass"}
	native, requestErr := buildScenarioQuery(ScenarioScanAll, cardinality, base)
	if requestErr != nil {
		return result, requestErr
	}
	native.Criteria = &modelv1.Criteria{Exp: &modelv1.Criteria_Condition{Condition: &modelv1.Condition{
		Name: benchTagID, Op: modelv1.Condition_BINARY_OP_EQ, Value: strTagValue("svc0"),
	}}}
	ql := &bydbqlv1.QueryRequest{
		Query: "SELECT id, entity_id, total::field, value::field FROM MEASURE service_cpm_minute IN sw_metric TIME BETWEEN ? AND ? WHERE id = ? LIMIT ?",
		Params: []*modelv1.TagValue{
			{Value: &modelv1.TagValue_Timestamp{Timestamp: native.TimeRange.Begin}},
			{Value: &modelv1.TagValue_Timestamp{Timestamp: native.TimeRange.End}},
			strTagValue("svc0"),
			{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: int64(cardinality)}}},
		},
	}
	nativeClient := measurev1.NewMeasureServiceClient(conn)
	qlClient := bydbqlv1.NewBydbQLServiceClient(conn)
	if visibilityErr := waitForQLBT01Visibility(ctx, qlClient, ql, cardinality, base); visibilityErr != nil {
		return result, visibilityErr
	}
	for _, variant := range []string{"native", "native_rebound", "legacy_ql", "legacy_ql_rebound"} {
		service := 0
		if variant == "legacy_ql_rebound" || variant == "native_rebound" {
			service = 1
		}
		ql.Params[2] = strTagValue(fmt.Sprintf("svc%d", service))
		native.Criteria.GetCondition().Value = ql.Params[2]
		expected := qlbT01Expected(cardinality, service, base)
		expectedHash := qlbT01Hash(expected)
		query := func() (*measurev1.QueryResponse, error) {
			queryCtx, cancel := context.WithTimeout(ctx, queryTimeout(cardinality))
			defer cancel()
			if variant == "native" || variant == "native_rebound" {
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
		for warmup := 0; warmup <= cfg.WarmupIterations; warmup++ {
			response, queryErr := query()
			if queryErr != nil {
				return result, fmt.Errorf("%s warmup: %w", variant, queryErr)
			}
			if checkErr := check(response); checkErr != nil {
				return result, checkErr
			}
		}
		profiler, profileErr := startProfile(cfg.ReportDir, Scenario(string(ScenarioQLBT01)+"_"+variant), cardinality, mode, cfg.Profile)
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
		result.Variants = append(result.Variants, Result{
			Engine: engineMeasure, Mode: mode, Scenario: ScenarioQLBT01, Variant: variant,
			Cardinality: cardinality, Correctness: "pass", QueryIterations: cfg.QueryIterations, QueryWorkers: 1,
			TimedSamples: len(latencies), ExpectedRows: len(expected.DataPoints), ExpectedHash: expectedHash,
			ResponseRows: len(expected.DataPoints), ApproxResultHash: expectedHash, Latency: latency, QPS: qps,
			Resources: resourceDelta(before, after), Allocations: allocationDelta(before, after, len(latencies)), Profiles: profiles,
			UnavailableMetrics: []string{"leaf_calls", "operator_peak_retained_bytes", "lease_charges", "post_close_live_reservations", "planning_cache"},
		})
	}
	return result, nil
}

// Readiness verifies the measured service slice, avoiding an unrelated full-table
// response that can exceed the prerequisite's enforced query budget.
func waitForQLBT01Visibility(ctx context.Context, client bydbqlv1.BydbQLServiceClient, request *bydbqlv1.QueryRequest,
	cardinality int, base time.Time,
) error {
	expected := qlbT01Expected(cardinality, 0, base)
	expectedHash := qlbT01Hash(expected)
	deadline := time.NewTimer(2 * time.Minute)
	defer deadline.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var lastErr error
	for {
		queryCtx, cancel := context.WithTimeout(ctx, queryTimeout(cardinality))
		response, queryErr := client.Query(queryCtx, request)
		cancel()
		if queryErr == nil && response.GetMeasureResult() != nil && len(response.GetMeasureResult().DataPoints) == len(expected.DataPoints) &&
			qlbT01Hash(response.GetMeasureResult()) == expectedHash {
			return nil
		}
		lastErr = queryErr
		if lastErr == nil {
			lastErr = fmt.Errorf("expected %d visible rows, got %d", len(expected.DataPoints), len(response.GetMeasureResult().GetDataPoints()))
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("T01 QL fixture did not become visible: %w", lastErr)
		case <-ticker.C:
		}
	}
}

func validateQLBT01Result(result Result) error {
	expected := map[string]bool{"native": false, "native_rebound": false, "legacy_ql": false, "legacy_ql_rebound": false}
	for _, variant := range result.Variants {
		seen, exists := expected[variant.Variant]
		if !exists || seen {
			return fmt.Errorf("unexpected or repeated qlb_t01 variant %q", variant.Variant)
		}
		expected[variant.Variant] = true
		if variant.QueryIterations != 1000 || variant.TimedSamples != variant.QueryIterations || variant.Correctness != "pass" ||
			variant.ResponseRows != variant.ExpectedRows || variant.ApproxResultHash != variant.ExpectedHash {
			return fmt.Errorf("invalid qlb_t01 measurements for %s", variant.Variant)
		}
	}
	for variant, seen := range expected {
		if !seen {
			return fmt.Errorf("missing qlb_t01 variant %s", variant)
		}
	}
	return nil
}

func TestQLBT01ExpectedAndIdentity(t *testing.T) {
	base := time.Unix(0, 0)
	first := qlbT01Expected(1024, 0, base)
	rebound := qlbT01Expected(1024, 1, base)
	if len(first.DataPoints) != 64 || len(rebound.DataPoints) != 64 {
		t.Fatal("unexpected independently specified row count")
	}
	checksum := qlbT01Hash(first)
	if checksum == qlbT01Hash(rebound) {
		t.Fatal("rebound values must differ")
	}
	first.DataPoints[0].Sid = 99
	first.DataPoints[0].Version = 42
	if checksum != qlbT01Hash(first) {
		t.Fatal("storage identities must not affect checksum")
	}
	first.DataPoints[0].Fields[0].Value = intFieldValue(999)
	if checksum == qlbT01Hash(first) {
		t.Fatal("wrong values must affect checksum")
	}
}

func TestQLBT01RejectsMissingAndEmptyVariants(t *testing.T) {
	result := Result{}
	if validateQLBT01Result(result) == nil {
		t.Fatal("missing variants accepted")
	}
	for _, name := range []string{"native", "native_rebound", "legacy_ql", "legacy_ql_rebound"} {
		result.Variants = append(result.Variants, Result{
			Variant: name, Correctness: "pass", QueryIterations: 1000, TimedSamples: 1000,
			ExpectedRows: 64, ResponseRows: 64, ExpectedHash: 42, ApproxResultHash: 42,
		})
	}
	if err := validateQLBT01Result(result); err != nil {
		t.Fatal(err)
	}
	result.Variants[1].TimedSamples = 0
	if validateQLBT01Result(result) == nil {
		t.Fatal("empty variant accepted")
	}
}
