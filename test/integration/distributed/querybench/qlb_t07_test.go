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
	"os"
	"time"

	"google.golang.org/grpc"

	bydbqlv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/bydbql/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
)

func runQLBT07Benchmark(ctx context.Context, conn *grpc.ClientConn, cfg Config, base time.Time) (Result, error) {
	result := Result{Engine: engineMeasure, Mode: cfg.Mode, Scenario: ScenarioQLBT07, Cardinality: cfg.Cardinality, Correctness: "pass"}
	client := bydbqlv1.NewBydbQLServiceClient(conn)
	if err := waitForQLBT07Visibility(ctx, client, cfg.Cardinality, base); err != nil {
		return result, err
	}
	for _, name := range qlbT07Variants {
		variant, err := runQLBT07Variant(ctx, client, cfg, base, name)
		if err != nil {
			return result, err
		}
		result.Variants = append(result.Variants, variant)
	}
	return result, validateQLBT07Result(result)
}

func runQLBT07Variant(ctx context.Context, client bydbqlv1.BydbQLServiceClient, cfg Config, base time.Time, name string) (Result, error) {
	request, err := qlbT07Query(name, cfg.Cardinality, base)
	if err != nil {
		return Result{}, err
	}
	oracle, err := newQLBT07Oracle(name, cfg.Cardinality)
	if err != nil {
		return Result{}, err
	}
	query := func() (*measurev1.QueryResponse, error) {
		queryCtx, cancel := context.WithTimeout(ctx, queryTimeout(cfg.Cardinality))
		defer cancel()
		response, queryErr := client.Query(queryCtx, request)
		if queryErr != nil {
			return nil, queryErr
		}
		if response.GetMeasureResult() == nil {
			return nil, fmt.Errorf("%s returned no measure result", name)
		}
		return response.GetMeasureResult(), nil
	}
	var expectedHash uint64
	for warmup := 0; warmup < cfg.WarmupIterations; warmup++ {
		response, queryErr := query()
		if queryErr != nil {
			return Result{}, fmt.Errorf("%s warmup: %w", name, queryErr)
		}
		expectedHash, err = oracle.check(response)
		if err != nil {
			return Result{}, err
		}
	}
	metricsBefore, err := qlbT07MetricsStart()
	if err != nil {
		return Result{}, err
	}
	profiler, err := startProfile(cfg.ReportDir, Scenario(string(ScenarioQLBT07)+"_"+name), cfg.Cardinality, cfg.Mode, cfg.Profile)
	if err != nil {
		return Result{}, err
	}
	latencies := make([]time.Duration, 0, cfg.QueryIterations)
	samples := make([]*measurev1.QueryResponse, 0, 12)
	before := captureProcessSnapshot()
	started := time.Now()
	var runErr error
	for iteration := 0; iteration < cfg.QueryIterations; iteration++ {
		queryStarted := time.Now()
		response, queryErr := query()
		latencies = append(latencies, time.Since(queryStarted))
		if queryErr != nil {
			runErr = fmt.Errorf("%s timed query %d: %w", name, iteration, queryErr)
			break
		}
		if len(response.GetDataPoints()) != oracle.rows() {
			runErr = fmt.Errorf("%s timed query %d returned %d rows, expected %d", name, iteration, len(response.GetDataPoints()), oracle.rows())
			break
		}
		if iteration%max(1, cfg.QueryIterations/10) == 0 || iteration == cfg.QueryIterations-1 {
			samples = append(samples, response)
		}
	}
	elapsed := time.Since(started)
	after := captureProcessSnapshot()
	profiles, stopErr := profiler.stop()
	if runErr != nil {
		return Result{}, runErr
	}
	if stopErr != nil {
		return Result{}, stopErr
	}
	for _, sample := range samples {
		if _, err = oracle.check(sample); err != nil {
			return Result{}, err
		}
	}
	// The N-group query has an unordered LIMIT 1; retain a checked sample's exact checksum.
	if len(samples) > 0 {
		expectedHash, err = oracle.check(samples[0])
		if err != nil {
			return Result{}, err
		}
	}
	metrics, unavailable, err := qlbT07MetricsFinish(metricsBefore, name, cfg.QueryIterations, cfg.Cardinality, oracle.rows())
	if err != nil {
		return Result{}, err
	}
	metrics["benchmark_process_id"] = uint64(os.Getpid())
	metrics["warmup_queries"] = uint64(cfg.WarmupIterations)
	metrics["checked_timed_samples"] = uint64(len(samples))
	latency, qps := summarizeLatencies(latencies, elapsed)
	return Result{
		Engine: engineMeasure, Mode: cfg.Mode, Scenario: ScenarioQLBT07, Variant: name, Cardinality: cfg.Cardinality,
		Correctness: "pass", QueryIterations: cfg.QueryIterations, QueryWorkers: 1, QueryMemoryMiB: cfg.QueryMemoryMiB,
		TimedSamples: len(latencies), RawLatenciesNS: latencies, ExpectedRows: oracle.rows(), ExpectedHash: expectedHash,
		ResponseRows: oracle.rows(), ApproxResultHash: expectedHash, Latency: latency, QPS: qps,
		Resources: resourceDelta(before, after), Allocations: allocationDelta(before, after, len(latencies)), Profiles: profiles,
		BatchMetrics: metrics, UnavailableMetrics: unavailable,
	}, nil
}

func waitForQLBT07Visibility(ctx context.Context, client bydbqlv1.BydbQLServiceClient, cardinality int, base time.Time) error {
	// Confirm every input chunk before checking the full aggregate once. Budget
	// rejection after visibility is complete is a permanent capability limit.
	if err := waitForQLBT07RawVisibility(ctx, client, cardinality, 8192, base); err != nil {
		return err
	}
	request, err := qlbT07Query("ql_multi_1024_groups", cardinality, base)
	if err != nil {
		return err
	}
	oracle, err := newQLBT07Oracle("ql_multi_1024_groups", cardinality)
	if err != nil {
		return err
	}
	queryCtx, cancel := context.WithTimeout(ctx, queryTimeout(cardinality))
	defer cancel()
	response, err := client.Query(queryCtx, request)
	if err != nil {
		return fmt.Errorf("T07 complete visible fixture aggregate cannot execute: %w", err)
	}
	_, err = oracle.check(response.GetMeasureResult())
	return err
}
