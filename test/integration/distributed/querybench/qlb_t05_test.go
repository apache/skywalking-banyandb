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
	"testing"
	"time"

	"google.golang.org/grpc"

	bydbqlv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/bydbql/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
)

// Each cache setting uses a fresh process, preventing closed cluster state from crossing settings.
func runQLBT05SingleShot(t *testing.T, cfg Config) {
	t.Helper()
	if setting := os.Getenv(qlbT05ChildEnv); setting != "" {
		runQLBT05CacheChild(t, cfg, setting)
		return
	}
	result := Result{Engine: engineMeasure, Mode: cfg.Mode, Scenario: ScenarioQLBT05, Cardinality: qlbT05Cardinality, Correctness: "pass"}
	for _, setting := range []string{"off", "on"} {
		variants, runErr := runQLBT05CacheProcess(t, cfg, setting)
		if runErr != nil {
			t.Fatalf("T05 cache=%s: %v", setting, runErr)
		}
		result.Variants = append(result.Variants, variants...)
	}
	if validationErr := validateQLBT05Result(result); validationErr != nil {
		t.Fatal(validationErr)
	}
	shardPath, shardErr := writeShard(result, cfg.ReportDir)
	if shardErr != nil {
		t.Fatalf("write T05 shard: %v", shardErr)
	}
	t.Logf("T05 four matched cache variants written to %s", shardPath)
}

func runQLBT05CacheChild(t *testing.T, cfg Config, setting string) {
	t.Helper()
	cacheSize := 0
	switch setting {
	case "on":
		cacheSize = 4000
	case "off":
	default:
		t.Fatalf("invalid T05 child cache setting %q", setting)
	}
	cluster, base, clusterErr := startBenchCluster(t, cfg, fmt.Sprintf("--bydbql-plan-cache-size=%d", cacheSize))
	if clusterErr != nil {
		t.Fatalf("T05 start child cluster: %v", clusterErr)
	}
	defer cluster.close()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	if _, writeErr := writeBenchmarkData(ctx, cluster.conn, cfg, qlbT05Cardinality, base); writeErr != nil {
		t.Fatalf("write T05 child fixture: %v", writeErr)
	}
	if visibilityErr := waitForQLBT03Visibility(ctx, cluster.conn, qlbT05Cardinality, base); visibilityErr != nil {
		t.Fatal(visibilityErr)
	}
	variants, runErr := runQLBT05Variants(ctx, cluster.conn, cfg, base, setting == "on")
	if runErr != nil {
		t.Fatalf("run T05 child variants: %v", runErr)
	}
	result := Result{
		Engine: engineMeasure, Mode: cfg.Mode, Scenario: ScenarioQLBT05,
		Cardinality: qlbT05Cardinality, Correctness: "pass", Variants: variants,
	}
	if _, shardErr := writeShard(result, cfg.ReportDir); shardErr != nil {
		t.Fatalf("write T05 child shard: %v", shardErr)
	}
	t.Logf("T05 cache=%s process=%d finished two variants", setting, os.Getpid())
}

func runQLBT05Variants(ctx context.Context, conn *grpc.ClientConn, cfg Config, base time.Time, enabled bool) ([]Result, error) {
	client := bydbqlv1.NewBydbQLServiceClient(conn)
	var variants []Result
	for _, shape := range qlbT05Shapes {
		variant, err := runQLBT05Variant(ctx, client, cfg, base, shape, enabled)
		if err != nil {
			return nil, err
		}
		variants = append(variants, variant)
	}
	return variants, nil
}

type qlbT05Sample struct {
	response *measurev1.QueryResponse
	binding  int
}

func runQLBT05Variant(ctx context.Context, client bydbqlv1.BydbQLServiceClient, cfg Config, base time.Time,
	shape string, enabled bool,
) (Result, error) {
	name := shape + "_cache_off"
	if enabled {
		name = shape + "_cache_on"
	}
	requests := make([]*bydbqlv1.QueryRequest, 2)
	expected := make([]*measurev1.QueryResponse, 2)
	for binding := range requests {
		var buildErr error
		requests[binding], expected[binding], buildErr = qlbT05Query(shape, binding, base)
		if buildErr != nil {
			return Result{}, buildErr
		}
	}
	query := func(binding int) (*measurev1.QueryResponse, error) {
		queryCtx, cancel := context.WithTimeout(ctx, queryTimeout(qlbT05Cardinality))
		defer cancel()
		response, err := client.Query(queryCtx, requests[binding])
		if err != nil {
			return nil, err
		}
		if response.GetMeasureResult() == nil {
			return nil, fmt.Errorf("%s returned no measure_result", name)
		}
		return response.GetMeasureResult(), nil
	}
	check := func(sample qlbT05Sample) error {
		want := expected[sample.binding]
		if len(sample.response.GetDataPoints()) != len(want.DataPoints) || qlbT01Hash(sample.response) != qlbT01Hash(want) {
			return fmt.Errorf("%s binding=%d response differs from the independent fixture oracle", name, sample.binding)
		}
		return nil
	}
	warmBefore := qlbT05CacheSnapshot()
	for iteration := 0; iteration < cfg.WarmupIterations; iteration++ {
		binding := iteration % 2
		response, err := query(binding)
		if err != nil {
			return Result{}, fmt.Errorf("%s warmup: %w", name, err)
		}
		if err = check(qlbT05Sample{response: response, binding: binding}); err != nil {
			return Result{}, err
		}
	}
	warmCompiles := qlbT05CacheSnapshot().Compiles - warmBefore.Compiles
	metricsBefore, metricsErr := qlbT05MetricsStart()
	if metricsErr != nil {
		return Result{}, metricsErr
	}
	profiler, profileErr := startProfile(cfg.ReportDir, Scenario(string(ScenarioQLBT05)+"_"+name), qlbT05Cardinality, cfg.Mode, cfg.Profile)
	if profileErr != nil {
		return Result{}, profileErr
	}
	latencies := make([]time.Duration, cfg.QueryIterations)
	samples := make([]qlbT05Sample, cfg.QueryIterations/100+1)
	before := captureProcessSnapshot()
	started := time.Now()
	runErr := runQLBT05Queries(query, cfg, latencies, samples, len(expected[0].DataPoints))
	elapsed := time.Since(started)
	after := captureProcessSnapshot()
	profiles, stopErr := profiler.stop()
	if runErr != nil {
		return Result{}, fmt.Errorf("%s: %w", name, runErr)
	}
	if stopErr != nil {
		return Result{}, stopErr
	}
	for _, sample := range samples {
		if err := check(sample); err != nil {
			return Result{}, err
		}
	}
	metrics, unavailable, finishErr := qlbT05MetricsFinish(metricsBefore, shape, cfg.QueryIterations, len(expected[0].DataPoints))
	if finishErr != nil {
		return Result{}, finishErr
	}
	metrics["warmup_plan_compiles"] = warmCompiles
	metrics["benchmark_process_id"] = uint64(os.Getpid())
	metrics["rebound_expected_hash"] = qlbT01Hash(expected[1])
	if validationErr := validateQLBT05Metrics(metrics, enabled); validationErr != nil {
		return Result{}, fmt.Errorf("%s: %w", name, validationErr)
	}
	latency, qps := summarizeLatencies(latencies, elapsed)
	hash := qlbT01Hash(expected[0])
	return Result{
		Engine: engineMeasure, Mode: cfg.Mode, Scenario: ScenarioQLBT05, Variant: name, Cardinality: qlbT05Cardinality,
		Correctness: "pass", QueryIterations: cfg.QueryIterations, QueryWorkers: cfg.QueryWorkers, QueryMemoryMiB: cfg.QueryMemoryMiB,
		TimedSamples: len(latencies), RawLatenciesNS: latencies, ExpectedRows: len(expected[0].DataPoints), ExpectedHash: hash,
		ResponseRows: len(expected[0].DataPoints), ApproxResultHash: hash, Latency: latency, QPS: qps,
		Resources: resourceDelta(before, after), Allocations: allocationDelta(before, after, len(latencies)), Profiles: profiles,
		BatchMetrics: metrics, UnavailableMetrics: unavailable,
	}, nil
}

func runQLBT05Queries(query func(int) (*measurev1.QueryResponse, error), cfg Config, latencies []time.Duration,
	samples []qlbT05Sample, rows int,
) error {
	var next atomic.Int64
	var workers sync.WaitGroup
	failures := make(chan error, cfg.QueryWorkers)
	for worker := 0; worker < cfg.QueryWorkers; worker++ {
		workers.Go(func() {
			for {
				index := int(next.Add(1) - 1)
				if index >= cfg.QueryIterations {
					return
				}
				binding := index % 2
				started := time.Now()
				response, err := query(binding)
				latencies[index] = time.Since(started)
				if err != nil {
					failures <- fmt.Errorf("timed query %d: %w", index, err)
					return
				}
				if len(response.GetDataPoints()) != rows {
					failures <- fmt.Errorf("timed query %d returned %d rows; expected %d", index, len(response.GetDataPoints()), rows)
					return
				}
				if index%100 == 0 {
					samples[index/100] = qlbT05Sample{response: response, binding: binding}
				}
				if index == cfg.QueryIterations-1 {
					samples[len(samples)-1] = qlbT05Sample{response: response, binding: binding}
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
