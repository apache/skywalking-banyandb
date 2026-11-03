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

func runQLBT10Benchmark(ctx context.Context, conn *grpc.ClientConn, cfg Config, base time.Time) (Result, error) {
	result := Result{Engine: engineMeasure, Mode: cfg.Mode, Scenario: ScenarioQLBT10, Cardinality: cfg.Cardinality, Correctness: "pass"}
	client := bydbqlv1.NewBydbQLServiceClient(conn)
	if err := waitForQLBT07RawVisibility(ctx, client, cfg.Cardinality, 8192, base); err != nil {
		return result, err
	}
	for _, name := range qlbT10Variants {
		variant, err := runQLBT10Variant(ctx, client, cfg, base, name)
		if err != nil {
			return result, err
		}
		result.Variants = append(result.Variants, variant)
	}
	return result, validateQLBT10Result(result)
}

func runQLBT10Variant(ctx context.Context, client bydbqlv1.BydbQLServiceClient, cfg Config, base time.Time, name string) (Result, error) {
	request, err := qlbT10Query(name, cfg.Cardinality, base)
	if err != nil {
		return Result{}, err
	}
	expectedHash, err := qlbT10ExpectedHash(name, cfg.Cardinality)
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
			return nil, fmt.Errorf("T10 %s returned no measure result", name)
		}
		return response.GetMeasureResult(), nil
	}
	for warmup := 0; warmup < cfg.WarmupIterations; warmup++ {
		response, queryErr := query()
		if queryErr != nil {
			return Result{}, fmt.Errorf("T10 %s warmup: %w", name, queryErr)
		}
		checksum, checkErr := checkQLBT10Result(response, name, cfg.Cardinality, base)
		if checkErr != nil {
			return Result{}, checkErr
		}
		if checksum != expectedHash {
			return Result{}, fmt.Errorf("T10 %s warmup checksum differs from independent fixture", name)
		}
	}
	metricsBefore, err := qlbT10MetricsStart()
	if err != nil {
		return Result{}, err
	}
	profiler, err := startProfile(cfg.ReportDir, Scenario(string(ScenarioQLBT10)+"_"+name), cfg.Cardinality, cfg.Mode, cfg.Profile)
	if err != nil {
		return Result{}, err
	}
	latencies := make([]time.Duration, 0, cfg.QueryIterations)
	samples := make([]*measurev1.QueryResponse, 0, 6)
	before := captureProcessSnapshot()
	started := time.Now()
	var runErr error
	for iteration := 0; iteration < cfg.QueryIterations; iteration++ {
		queryStarted := time.Now()
		response, queryErr := query()
		latencies = append(latencies, time.Since(queryStarted))
		if queryErr != nil {
			runErr = fmt.Errorf("T10 %s timed %d: %w", name, iteration, queryErr)
			break
		}
		if len(response.GetDataPoints()) != cfg.Cardinality {
			runErr = fmt.Errorf("T10 %s timed %d returned %d rows, expected %d", name, iteration, len(response.GetDataPoints()), cfg.Cardinality)
			break
		}
		if iteration%max(1, cfg.QueryIterations/5) == 0 || iteration == cfg.QueryIterations-1 {
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
		checksum, checkErr := checkQLBT10Result(sample, name, cfg.Cardinality, base)
		if checkErr != nil {
			return Result{}, checkErr
		}
		if checksum != expectedHash {
			return Result{}, fmt.Errorf("T10 %s timed checksum differs from independent fixture", name)
		}
	}
	metrics, unavailable, err := qlbT10MetricsFinish(metricsBefore, name, cfg.QueryIterations, cfg.Cardinality)
	if err != nil {
		return Result{}, err
	}
	metrics["benchmark_process_id"], metrics["warmup_queries"], metrics["checked_timed_samples"] = uint64(os.Getpid()), uint64(cfg.WarmupIterations), uint64(len(samples))
	latency, qps := summarizeLatencies(latencies, elapsed)
	return Result{
		Engine: engineMeasure, Mode: cfg.Mode, Scenario: ScenarioQLBT10, Variant: name, Cardinality: cfg.Cardinality,
		Correctness: "pass", QueryIterations: cfg.QueryIterations, QueryWorkers: 1, QueryMemoryMiB: cfg.QueryMemoryMiB,
		TimedSamples: len(latencies), RawLatenciesNS: latencies, ExpectedRows: cfg.Cardinality, ExpectedHash: expectedHash,
		ResponseRows: cfg.Cardinality, ApproxResultHash: expectedHash, Latency: latency, QPS: qps,
		Resources: resourceDelta(before, after), Allocations: allocationDelta(before, after, len(latencies)), Profiles: profiles,
		BatchMetrics: metrics, UnavailableMetrics: unavailable,
	}, nil
}
