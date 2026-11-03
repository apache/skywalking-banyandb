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

func runQLBT08Benchmark(ctx context.Context, conn *grpc.ClientConn, cfg Config, base time.Time) (Result, error) {
	result := Result{Engine: engineMeasure, Mode: cfg.Mode, Scenario: ScenarioQLBT08, Cardinality: cfg.Cardinality, Correctness: "pass"}
	client := bydbqlv1.NewBydbQLServiceClient(conn)
	if err := waitForQLBT07RawVisibility(ctx, client, cfg.Cardinality, 8192, base); err != nil {
		return result, err
	}
	for _, shape := range qlbT08Shapes {
		for _, mode := range qlbT08Modes {
			if mode == "individual" && shape.branches == 0 {
				continue
			}
			variant, err := runQLBT08Variant(ctx, conn, cfg, base, shape, mode)
			if err != nil {
				return result, err
			}
			result.Variants = append(result.Variants, variant)
		}
	}
	return result, validateQLBT08Result(result)
}

func qlbT08QueryFunc(ctx context.Context, conn *grpc.ClientConn, cfg Config, base time.Time, shape qlbT08Shape, mode string) (func() (qlbT08Sample, error), error) {
	if mode == "individual" {
		requests, err := qlbT08NativeQueries(shape.name, cfg.Cardinality, base)
		if err != nil {
			return nil, err
		}
		client := measurev1.NewMeasureServiceClient(conn)
		return func() (qlbT08Sample, error) {
			queryCtx, cancel := context.WithTimeout(ctx, queryTimeout(cfg.Cardinality))
			defer cancel()
			sample := qlbT08Sample{native: make([]*measurev1.QueryResponse, 0, len(requests))}
			for _, request := range requests {
				response, queryErr := client.Query(queryCtx, request)
				if queryErr != nil {
					return qlbT08Sample{}, queryErr
				}
				sample.native = append(sample.native, response)
			}
			return sample, nil
		}, nil
	}
	request, err := qlbT08Query(shape.name, cfg.Cardinality, base)
	if err != nil {
		return nil, err
	}
	client := bydbqlv1.NewBydbQLServiceClient(conn)
	return func() (qlbT08Sample, error) {
		queryCtx, cancel := context.WithTimeout(ctx, queryTimeout(cfg.Cardinality))
		defer cancel()
		response, queryErr := client.Query(queryCtx, request)
		if queryErr != nil {
			return qlbT08Sample{}, queryErr
		}
		if response.GetMeasureResult() == nil {
			return qlbT08Sample{}, fmt.Errorf("T08 %s returned no measure result", shape.name)
		}
		return qlbT08Sample{ql: response.GetMeasureResult()}, nil
	}, nil
}

func runQLBT08Variant(ctx context.Context, conn *grpc.ClientConn, cfg Config, base time.Time, shape qlbT08Shape, mode string) (Result, error) {
	restore, err := qlbT08SelectMode(mode)
	if err != nil {
		return Result{}, err
	}
	defer restore()
	name := shape.name + "_" + mode
	query, err := qlbT08QueryFunc(ctx, conn, cfg, base, shape, mode)
	if err != nil {
		return Result{}, err
	}
	oracle, err := newQLBT08Oracle(shape.name, cfg.Cardinality)
	if err != nil {
		return Result{}, err
	}
	rows := min(cfg.Cardinality, 16)
	if mode == "individual" {
		rows *= shape.branches
	}
	var expectedHash uint64
	for warmup := 0; warmup < cfg.WarmupIterations; warmup++ {
		response, queryErr := query()
		if queryErr != nil {
			return Result{}, fmt.Errorf("T08 %s warmup:%w", name, queryErr)
		}
		expectedHash, err = oracle.check(response)
		if err != nil {
			return Result{}, err
		}
	}
	metricsBefore, err := qlbT08MetricsStart()
	if err != nil {
		return Result{}, err
	}
	profiler, err := startProfile(cfg.ReportDir, Scenario(string(ScenarioQLBT08)+"_"+name), cfg.Cardinality, cfg.Mode, cfg.Profile)
	if err != nil {
		return Result{}, err
	}
	latencies := make([]time.Duration, 0, cfg.QueryIterations)
	samples := make([]qlbT08Sample, 0, 12)
	before := captureProcessSnapshot()
	started := time.Now()
	var runErr error
	for iteration := 0; iteration < cfg.QueryIterations; iteration++ {
		queryStarted := time.Now()
		response, queryErr := query()
		latencies = append(latencies, time.Since(queryStarted))
		if queryErr != nil {
			runErr = fmt.Errorf("T08 %s timed %d:%w", name, iteration, queryErr)
			break
		}
		if response.rows() != rows {
			runErr = fmt.Errorf("T08 %s timed %d has %d rows, expected %d", name, iteration, response.rows(), rows)
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
	metrics, unavailable, err := qlbT08MetricsFinish(metricsBefore, shape, mode, cfg.QueryIterations, cfg.Cardinality)
	if err != nil {
		return Result{}, err
	}
	metrics["benchmark_process_id"], metrics["warmup_queries"], metrics["checked_timed_samples"] = uint64(os.Getpid()), uint64(cfg.WarmupIterations), uint64(len(samples))
	latency, qps := summarizeLatencies(latencies, elapsed)
	return Result{
		Engine: engineMeasure, Mode: cfg.Mode, Scenario: ScenarioQLBT08, Variant: name, Cardinality: cfg.Cardinality,
		Correctness: "pass", QueryIterations: cfg.QueryIterations, QueryWorkers: 1, QueryMemoryMiB: cfg.QueryMemoryMiB,
		TimedSamples: len(latencies), RawLatenciesNS: latencies, ExpectedRows: rows, ExpectedHash: expectedHash, ResponseRows: rows, ApproxResultHash: expectedHash,
		Latency: latency, QPS: qps, Resources: resourceDelta(before, after), Allocations: allocationDelta(before, after, len(latencies)), Profiles: profiles,
		BatchMetrics: metrics, UnavailableMetrics: unavailable,
	}, nil
}
