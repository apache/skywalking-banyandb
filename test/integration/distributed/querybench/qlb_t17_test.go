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
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	bydbqlv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/bydbql/v1"
)

func runQLBT17Benchmark(ctx context.Context, conn *grpc.ClientConn, cfg Config, base time.Time) (Result, error) {
	result := Result{Engine: engineMeasure, Mode: cfg.Mode, Scenario: ScenarioQLBT17, Cardinality: cfg.Cardinality, Correctness: "pass"}
	var blocked32 *Result
	if path := os.Getenv("DQB_T17_BLOCKED_32_REPORT"); path != "" {
		blocked, blockedErr := newQLBT17Blocked32(cfg, path)
		if blockedErr != nil {
			return result, blockedErr
		}
		blocked32 = &blocked
	}
	client := bydbqlv1.NewBydbQLServiceClient(conn)
	if err := waitForQLBT17Visibility(ctx, client, cfg.Cardinality, base); err != nil {
		return result, err
	}
	for _, spec := range qlbT17Specs() {
		if spec.composed && spec.workers == 32 && blocked32 != nil {
			result.Correctness = "partial_known_32_blocked"
			result.Variants = append(result.Variants, *blocked32)
			continue
		}
		variant, err := runQLBT17Variant(ctx, client, cfg, base, spec)
		if err != nil {
			return result, err
		}
		result.Variants = append(result.Variants, variant)
	}
	return result, validateQLBT17Result(result)
}

func qlbT17Oracles(spec qlbT17Spec, rows int, base time.Time) []*qlbT17Oracle {
	count := 1
	if spec.composed {
		count = 3
	}
	oracles := make([]*qlbT17Oracle, count)
	for binding := range oracles {
		oracles[binding] = newQLBT17Oracle(spec, rows, binding, base)
	}
	return oracles
}

func runQLBT17Variant(ctx context.Context, client bydbqlv1.BydbQLServiceClient, cfg Config, base time.Time, spec qlbT17Spec) (Result, error) {
	restore := qlbT17SetFullSort(spec.full)
	defer restore()
	oracles := qlbT17Oracles(spec, cfg.Cardinality, base)
	requests := make([]*bydbqlv1.QueryRequest, len(oracles))
	for index, oracle := range oracles {
		requests[index] = oracle.query(base)
	}
	query := func(index int) (*bydbqlv1.QueryResponse, error) {
		queryCtx, cancel := context.WithTimeout(ctx, queryTimeout(cfg.Cardinality))
		defer cancel()
		response, err := client.Query(queryCtx, requests[index%len(requests)])
		if err != nil {
			return nil, err
		}
		if response.GetMeasureResult() == nil {
			return nil, fmt.Errorf("T17 %s returned no measure result", spec.name)
		}
		return response, nil
	}
	cacheBefore := qlbT05CacheSnapshot()
	for warmup := 0; warmup < max(cfg.WarmupIterations, len(oracles)); warmup++ {
		response, err := query(warmup)
		if err != nil {
			return Result{}, fmt.Errorf("T17 %s warmup: %w", spec.name, err)
		}
		if err = oracles[warmup%len(oracles)].check(response.GetMeasureResult()); err != nil {
			return Result{}, err
		}
	}
	warmCompiles := qlbT05CacheSnapshot().Compiles - cacheBefore.Compiles
	before, err := qlbT17MetricsStart()
	if err != nil {
		return Result{}, err
	}
	profiler, err := startProfileWithVariant(cfg.ReportDir, ScenarioQLBT17, cfg.Cardinality, cfg.Mode, spec.name, cfg.Profile)
	if err != nil {
		return Result{}, err
	}
	iterations := spec.calls(cfg.Cardinality)
	latencies := make([]time.Duration, iterations)
	retained := make([]*bydbqlv1.QueryResponse, iterations)
	processBefore := captureProcessSnapshot()
	started := time.Now()
	runErr := runQLBT17Queries(query, spec, oracles, latencies, retained)
	elapsed := time.Since(started)
	processAfter := captureProcessSnapshot()
	profiles, stopErr := profiler.stop()
	if runErr != nil {
		return Result{}, fmt.Errorf("T17 %s timed: %w", spec.name, runErr)
	}
	if stopErr != nil {
		return Result{}, stopErr
	}
	var checked, minBytes, maxBytes, checkedBytes uint64
	for index, response := range retained {
		if response == nil {
			continue
		}
		if checkErr := oracles[index%len(oracles)].check(response.GetMeasureResult()); checkErr != nil {
			return Result{}, checkErr
		}
		checked++
		responseBytes := uint64(proto.Size(response))
		checkedBytes += responseBytes
		if minBytes == 0 || responseBytes < minBytes {
			minBytes = responseBytes
		}
		maxBytes = max(maxBytes, responseBytes)
	}
	metrics, unavailable, err := qlbT17MetricsFinish(before, spec, iterations, oracles)
	if err != nil {
		return Result{}, err
	}
	metrics["benchmark_process_id"], metrics["warmup_queries"] = uint64(os.Getpid()), uint64(max(cfg.WarmupIterations, len(oracles)))
	metrics["warmup_plan_compiles"] = warmCompiles
	metrics["checked_timed_samples"], metrics["checked_response_bytes"] = checked, checkedBytes
	metrics["min_response_bytes"], metrics["max_response_bytes"] = minBytes, maxBytes
	metrics["fixture_base_unix_nanos"] = uint64(base.UnixNano())
	metrics["fixture_input_rows"], metrics["bind_sets"] = uint64(cfg.Cardinality), uint64(len(oracles))
	for index, oracle := range oracles {
		metrics[fmt.Sprintf("bound_set_%d_queries", index)] = uint64((iterations + len(oracles) - 1 - index) / len(oracles))
		metrics[fmt.Sprintf("bound_set_%d_ordered_hash", index)] = oracle.hash
	}
	latency, qps := summarizeLatencies(latencies, elapsed)
	return Result{
		Engine: engineMeasure, Mode: cfg.Mode, Scenario: ScenarioQLBT17, Variant: spec.name, Cardinality: cfg.Cardinality, Correctness: "pass",
		QueryIterations: iterations, QueryWorkers: spec.workers, QueryMemoryMiB: cfg.QueryMemoryMiB, TimedSamples: iterations, RawLatenciesNS: latencies,
		ExpectedRows: len(oracles[0].expected.DataPoints), ResponseRows: len(oracles[0].expected.DataPoints), ExpectedHash: oracles[0].hash, ApproxResultHash: oracles[0].hash,
		Latency: latency, QPS: qps, Resources: resourceDelta(processBefore, processAfter), Allocations: allocationDelta(processBefore, processAfter, iterations),
		Profiles: profiles, BatchMetrics: metrics, UnavailableMetrics: unavailable,
	}, nil
}

func runQLBT17Queries(query func(int) (*bydbqlv1.QueryResponse, error), spec qlbT17Spec, oracles []*qlbT17Oracle,
	latencies []time.Duration, retained []*bydbqlv1.QueryResponse,
) error {
	var next atomic.Int64
	var workers sync.WaitGroup
	failures := make(chan error, spec.workers)
	for worker := 0; worker < spec.workers; worker++ {
		workers.Go(func() {
			for {
				index := int(next.Add(1) - 1)
				if index >= len(latencies) {
					return
				}
				started := time.Now()
				response, err := query(index)
				latencies[index] = time.Since(started)
				if err != nil {
					failures <- err
					return
				}
				if len(response.GetMeasureResult().DataPoints) != len(oracles[index%len(oracles)].expected.DataPoints) {
					failures <- fmt.Errorf("T17 %s changed response count", spec.name)
					return
				}
				if index%max(1, len(latencies)/5) == 0 || index == len(latencies)-1 {
					retained[index] = response
				}
			}
		})
	}
	workers.Wait()
	close(failures)
	for err := range failures {
		return err
	}
	return nil
}
