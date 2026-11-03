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

func runQLBT12Benchmark(ctx context.Context, conn *grpc.ClientConn, cfg Config, base time.Time) (Result, error) {
	result := Result{Engine: engineMeasure, Mode: cfg.Mode, Scenario: ScenarioQLBT12, Cardinality: cfg.Cardinality, Correctness: "pass"}
	client := bydbqlv1.NewBydbQLServiceClient(conn)
	if err := waitForQLBT12Visibility(ctx, client, cfg.Cardinality, base); err != nil {
		return result, err
	}
	for _, name := range qlbT12Variants() {
		variant, err := runQLBT12Variant(ctx, client, cfg, base, name)
		if err != nil {
			return result, err
		}
		result.Variants = append(result.Variants, variant)
	}
	return result, validateQLBT12Result(result)
}

func runQLBT12Variant(ctx context.Context, client bydbqlv1.BydbQLServiceClient, cfg Config, base time.Time, name string) (Result, error) {
	request, err := qlbT12Query(name, cfg.Cardinality, base)
	if err != nil {
		return Result{}, err
	}
	oracle, err := newQLBT12Oracle(name, cfg.Cardinality)
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
			return nil, fmt.Errorf("T12 %s returned no measure result", name)
		}
		return response.GetMeasureResult(), nil
	}
	for warmup := 0; warmup < cfg.WarmupIterations; warmup++ {
		response, queryErr := query()
		if queryErr != nil {
			return Result{}, fmt.Errorf("T12 %s warmup: %w", name, queryErr)
		}
		checksum, checkErr := oracle.check(response, base)
		if checkErr != nil {
			return Result{}, checkErr
		}
		if checksum != oracle.hash {
			return Result{}, fmt.Errorf("T12 %s warmup checksum differs from independent fixture", name)
		}
	}
	metricsBefore, err := qlbT12MetricsStart()
	if err != nil {
		return Result{}, err
	}
	profiler, err := startProfile(cfg.ReportDir, Scenario(string(ScenarioQLBT12)+"_"+name), cfg.Cardinality, cfg.Mode, cfg.Profile)
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
			runErr = fmt.Errorf("T12 %s timed %d: %w", name, iteration, queryErr)
			break
		}
		if len(response.GetDataPoints()) != oracle.outputRows() {
			runErr = fmt.Errorf("T12 %s timed %d returned %d rows, expected %d", name, iteration, len(response.GetDataPoints()), oracle.outputRows())
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
		checksum, checkErr := oracle.check(sample, base)
		if checkErr != nil {
			return Result{}, checkErr
		}
		if checksum != oracle.hash {
			return Result{}, fmt.Errorf("T12 %s timed checksum differs from independent fixture", name)
		}
	}
	metrics, unavailable, err := qlbT12MetricsFinish(metricsBefore, name, cfg.QueryIterations, oracle)
	if err != nil {
		return Result{}, err
	}
	metrics["benchmark_process_id"], metrics["warmup_queries"], metrics["checked_timed_samples"] = uint64(os.Getpid()), uint64(cfg.WarmupIterations), uint64(len(samples))
	metrics["target_selectivity_percent"], metrics["hidden_predicate_fields"] = uint64(oracle.spec.percent), 1
	if oracle.spec.wide {
		metrics["hidden_predicate_fields"] = 2
	}
	latency, qps := summarizeLatencies(latencies, elapsed)
	return Result{
		Engine: engineMeasure, Mode: cfg.Mode, Scenario: ScenarioQLBT12, Variant: name, Cardinality: cfg.Cardinality,
		Correctness: "pass", QueryIterations: cfg.QueryIterations, QueryWorkers: 1, QueryMemoryMiB: cfg.QueryMemoryMiB,
		TimedSamples: len(latencies), RawLatenciesNS: latencies, ExpectedRows: oracle.outputRows(), ExpectedHash: oracle.hash,
		ResponseRows: oracle.outputRows(), ApproxResultHash: oracle.hash, Latency: latency, QPS: qps,
		Resources: resourceDelta(before, after), Allocations: allocationDelta(before, after, len(latencies)), Profiles: profiles,
		BatchMetrics: metrics, UnavailableMetrics: unavailable,
	}, nil
}
