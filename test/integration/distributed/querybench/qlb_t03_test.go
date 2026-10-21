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
	"time"

	"google.golang.org/grpc"

	bydbqlv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/bydbql/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
)

func runQLBT03Benchmark(ctx context.Context, conn *grpc.ClientConn, cfg Config, cardinality int, mode string, base time.Time) (Result, error) {
	result := Result{Engine: engineMeasure, Mode: mode, Scenario: ScenarioQLBT03, Cardinality: cardinality, Correctness: "pass"}
	if err := waitForQLBT03Visibility(ctx, conn, cardinality, base); err != nil {
		return result, err
	}
	client := bydbqlv1.NewBydbQLServiceClient(conn)
	for _, name := range qlbT03Variants {
		request, expected, buildErr := qlbT03Variant(name, cardinality, base)
		if buildErr != nil {
			return result, buildErr
		}
		hash := qlbT01Hash(expected)
		query := func() (*measurev1.QueryResponse, error) {
			queryCtx, cancel := context.WithTimeout(ctx, queryTimeout(cardinality))
			defer cancel()
			response, err := client.Query(queryCtx, request)
			if err != nil {
				return nil, err
			}
			if response.GetMeasureResult() == nil {
				return nil, fmt.Errorf("%s returned no measure_result", name)
			}
			return response.GetMeasureResult(), nil
		}
		check := func(response *measurev1.QueryResponse) error {
			actual := qlbT01Hash(response)
			if len(response.GetDataPoints()) != len(expected.DataPoints) || actual != hash {
				return fmt.Errorf("%s expected rows=%d hash=%x; got rows=%d hash=%x", name, len(expected.DataPoints), hash, len(response.GetDataPoints()), actual)
			}
			return nil
		}
		for warmup := 0; warmup < cfg.WarmupIterations; warmup++ {
			response, err := query()
			if err != nil {
				return result, fmt.Errorf("%s warmup: %w", name, err)
			}
			if err = check(response); err != nil {
				return result, err
			}
		}
		metricsBefore, metricsErr := qlbT03MetricsStart()
		if metricsErr != nil {
			return result, metricsErr
		}
		profiler, profileErr := startProfile(cfg.ReportDir, Scenario(string(ScenarioQLBT03)+"_"+name), cardinality, mode, cfg.Profile)
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
			response, err := query()
			latencies = append(latencies, time.Since(queryStarted))
			if err != nil {
				runErr = fmt.Errorf("%s timed query %d: %w", name, iteration, err)
				break
			}
			if len(response.GetDataPoints()) != len(expected.DataPoints) {
				runErr = fmt.Errorf("%s returned %d rows; expected %d", name, len(response.GetDataPoints()), len(expected.DataPoints))
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
			if err := check(sample); err != nil {
				return result, err
			}
		}
		metrics, unavailable, finishErr := qlbT03MetricsFinish(metricsBefore, name, len(latencies), len(expected.DataPoints))
		if finishErr != nil {
			return result, finishErr
		}
		latency, qps := summarizeLatencies(latencies, elapsed)
		entities, points := splitCardinality(cardinality)
		result.Variants = append(result.Variants, Result{
			Engine: engineMeasure, Mode: mode, Scenario: ScenarioQLBT03, Variant: name, Cardinality: cardinality, Entities: entities, PointsEach: points,
			Correctness: "pass", QueryIterations: cfg.QueryIterations, QueryWorkers: 1, QueryMemoryMiB: cfg.QueryMemoryMiB,
			TimedSamples: len(latencies), RawLatenciesNS: latencies, ExpectedRows: len(expected.DataPoints), ExpectedHash: hash,
			ResponseRows: len(expected.DataPoints), ApproxResultHash: hash, Latency: latency, QPS: qps,
			Resources: resourceDelta(before, after), Allocations: allocationDelta(before, after, len(latencies)), Profiles: profiles,
			BatchMetrics: metrics, UnavailableMetrics: unavailable,
		})
	}
	return result, validateQLBT03Result(result)
}

func waitForQLBT03Visibility(ctx context.Context, conn *grpc.ClientConn, cardinality int, base time.Time) error {
	request, expected, err := qlbT03Variant("ql_plain_legacy", cardinality, base)
	if err != nil {
		return err
	}
	expectedHash := qlbT01Hash(expected)
	client := bydbqlv1.NewBydbQLServiceClient(conn)
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
			return fmt.Errorf("T03 QL fixture did not become visible: %w", lastErr)
		case <-ticker.C:
		}
	}
}
