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
	"google.golang.org/protobuf/proto"

	bydbqlv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/bydbql/v1"
)

func runQLBT14Benchmark(ctx context.Context, conn *grpc.ClientConn, cfg Config, base time.Time) (Result, error) {
	result := Result{Engine: engineMeasure, Mode: cfg.Mode, Scenario: ScenarioQLBT14, Cardinality: cfg.Cardinality, Correctness: "pass"}
	client := bydbqlv1.NewBydbQLServiceClient(conn)
	if err := waitForQLBT14Visibility(ctx, client, cfg.Cardinality, base); err != nil {
		return result, err
	}
	for _, spec := range qlbT14Specs(cfg.Cardinality) {
		variant, err := runQLBT14Variant(ctx, client, cfg, base, spec)
		if err != nil {
			return result, err
		}
		result.Variants = append(result.Variants, variant)
	}
	return result, validateQLBT14Result(result)
}

func runQLBT14Variant(ctx context.Context, client bydbqlv1.BydbQLServiceClient, cfg Config, base time.Time, spec qlbT14Spec) (Result, error) {
	mode := "pushdown"
	if spec.mode == "local" {
		mode = "local"
	}
	restore, err := qlbT08SelectMode(mode)
	if err != nil {
		return Result{}, err
	}
	defer restore()
	oracle := newQLBT14Oracle(spec, cfg.Cardinality)
	request := oracle.query(cfg.Cardinality, base)
	query := func() (*bydbqlv1.QueryResponse, error) {
		queryCtx, cancel := context.WithTimeout(ctx, queryTimeout(cfg.Cardinality))
		defer cancel()
		response, queryErr := client.Query(queryCtx, request)
		if queryErr != nil {
			return nil, queryErr
		}
		if response.GetMeasureResult() == nil {
			return nil, fmt.Errorf("T14 %s returned no measure result", spec.name)
		}
		return response, nil
	}
	for warmup := 0; warmup < cfg.WarmupIterations; warmup++ {
		response, queryErr := query()
		if queryErr != nil {
			return Result{}, fmt.Errorf("T14 %s warmup: %w", spec.name, queryErr)
		}
		if err = oracle.check(response.GetMeasureResult()); err != nil {
			return Result{}, err
		}
	}
	before, err := qlbT14MetricsStart()
	if err != nil {
		return Result{}, err
	}
	profiler, err := startProfile(cfg.ReportDir, Scenario(string(ScenarioQLBT14)+"_"+spec.name), cfg.Cardinality, cfg.Mode, cfg.Profile)
	if err != nil {
		return Result{}, err
	}
	latencies := make([]time.Duration, 0, cfg.QueryIterations)
	retained := make([]*bydbqlv1.QueryResponse, 0, 6)
	processBefore := captureProcessSnapshot()
	started := time.Now()
	var runErr error
	for iteration := 0; iteration < cfg.QueryIterations; iteration++ {
		queryStarted := time.Now()
		response, queryErr := query()
		latencies = append(latencies, time.Since(queryStarted))
		if queryErr != nil {
			runErr = fmt.Errorf("T14 %s timed %d: %w", spec.name, iteration, queryErr)
			break
		}
		if len(response.GetMeasureResult().DataPoints) != len(oracle.expected.DataPoints) {
			runErr = fmt.Errorf("T14 %s timed row count differs", spec.name)
			break
		}
		if iteration%max(1, cfg.QueryIterations/5) == 0 || iteration == cfg.QueryIterations-1 {
			retained = append(retained, response)
		}
	}
	elapsed := time.Since(started)
	processAfter := captureProcessSnapshot()
	profiles, stopErr := profiler.stop()
	if runErr != nil {
		return Result{}, runErr
	}
	if stopErr != nil {
		return Result{}, stopErr
	}
	var checkedBytes, minBytes, maxBytes uint64
	for _, response := range retained {
		if err = oracle.check(response.GetMeasureResult()); err != nil {
			return Result{}, err
		}
		// Size the actual complete RPC envelope after the timing/profile window.
		responseBytes := uint64(proto.Size(response))
		checkedBytes += responseBytes
		if minBytes == 0 || responseBytes < minBytes {
			minBytes = responseBytes
		}
		maxBytes = max(maxBytes, responseBytes)
	}
	metrics, unavailable, err := qlbT14MetricsFinish(before, cfg.QueryIterations, oracle)
	if err != nil {
		return Result{}, err
	}
	metrics["benchmark_process_id"], metrics["warmup_queries"], metrics["checked_timed_samples"] = uint64(os.Getpid()), uint64(cfg.WarmupIterations), uint64(len(retained))
	metrics["checked_response_bytes"], metrics["min_response_bytes"], metrics["max_response_bytes"] = checkedBytes, minBytes, maxBytes
	metrics["fixture_input_rows"] = uint64(oracle.inputRows)
	metrics["completed_groups_per_query"] = uint64(oracle.groups)
	metrics["selected_groups_per_query"] = uint64(len(oracle.expected.DataPoints))
	latency, qps := summarizeLatencies(latencies, elapsed)
	return Result{
		Engine: engineMeasure, Mode: cfg.Mode, Scenario: ScenarioQLBT14, Variant: spec.name, Cardinality: cfg.Cardinality, Correctness: "pass",
		QueryIterations: cfg.QueryIterations, QueryWorkers: 1, QueryMemoryMiB: cfg.QueryMemoryMiB, TimedSamples: len(latencies), RawLatenciesNS: latencies,
		ExpectedRows: len(oracle.expected.DataPoints), ResponseRows: len(oracle.expected.DataPoints), ExpectedHash: oracle.hash, ApproxResultHash: oracle.hash,
		Latency: latency, QPS: qps, Resources: resourceDelta(processBefore, processAfter), Allocations: allocationDelta(processBefore, processAfter, len(latencies)),
		Profiles: profiles, BatchMetrics: metrics, UnavailableMetrics: unavailable,
	}, nil
}
