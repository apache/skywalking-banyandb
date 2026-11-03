//go:build querymetrics

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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	bydbqlv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/bydbql/v1"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/pool"
	"github.com/apache/skywalking-banyandb/pkg/query/ql"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
)

func TestQLBT07ResourceBounds(t *testing.T) {
	cfg := LoadConfig()
	if !cfg.RunBench {
		t.Skip("set RUN_DISTRIBUTED_QUERY_BENCH=1 for the Docker-gated T07 resource probe")
	}
	if !cfg.InContainer || cfg.Engine != engineMeasure || (cfg.QueryMemoryMiB != 16 && cfg.QueryMemoryMiB != 256) {
		t.Fatal("T07 resource probe requires a container, DQB_ENGINE=measure and DQB_QUERY_MEMORY_MIB=16 or 256")
	}
	if !vectorized.QueryMetricsEnabled {
		t.Fatal("T07 resource probe requires a binary built with -tags=querymetrics")
	}
	if err := logger.Init(logger.Logging{Env: "dev", Level: "info"}); err != nil {
		t.Fatal(err)
	}
	gomega.RegisterTestingT(t)
	if cfg.Cardinality == 0 {
		cfg.Cardinality = 100000
	}
	if cfg.Cardinality != 100000 && cfg.Cardinality != 1000000 {
		t.Fatal("T07 resource probe requires DQB_CARDINALITY=100000 or 1000000")
	}
	cfg.Scenario, cfg.Mode = ScenarioQLBT07, modeVec
	cluster, base, err := startBenchCluster(t, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.close()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	if _, err = writeQLBT07Data(ctx, cluster.conn, cfg, base); err != nil {
		t.Fatal(err)
	}
	client := bydbqlv1.NewBydbQLServiceClient(cluster.conn)
	if err = waitForQLBT07RawVisibility(ctx, client, cfg.Cardinality, 1024, base); err != nil {
		t.Fatal(err)
	}
	request, err := qlbT07Query("ql_shared_n_groups", cfg.Cardinality, base)
	if err != nil {
		t.Fatal(err)
	}
	oracle, err := newQLBT07Oracle("ql_shared_n_groups", cfg.Cardinality)
	if err != nil {
		t.Fatal(err)
	}
	budget := int64(cfg.QueryMemoryMiB) << 20
	var successes, rejections int
	samples := make([]map[string]any, 0, 5)
	for iteration := 0; iteration < 5; iteration++ {
		nativeBefore, metricsErr := qlbT02MetricsStart()
		if metricsErr != nil {
			t.Fatal(metricsErr)
		}
		aggregateBefore := ql.AggregateExecutionStats()
		before := captureProcessSnapshot()
		started := time.Now()
		response, queryErr := client.Query(ctx, request)
		elapsed := time.Since(started)
		after := captureProcessSnapshot()
		code := status.Code(queryErr)
		expectedHash := uint64(0)
		switch code {
		case codes.OK:
			expectedHash, err = oracle.check(response.GetMeasureResult())
			if err != nil {
				t.Fatal(err)
			}
			successes++
		case codes.ResourceExhausted:
			if response.GetMeasureResult() != nil {
				t.Fatal("T07 resource rejection returned a partial measure result")
			}
			rejections++
		default:
			t.Fatalf("T07 resource query %d returned %s: %v", iteration, code, queryErr)
		}
		waitForQLBT02ResourceRelease(nativeBefore.execution.LiveResults, nativeBefore.memory.LiveReservedBytes, nativeBefore.owners, nativeBefore.flatten)
		nativeAfter := vmeasure.BatchExecutionStats()
		memory := vectorized.QueryMemoryStats()
		aggregateAfter := ql.AggregateExecutionStats()
		assertQLBT07ResourceCleanup(t, nativeBefore, aggregateBefore.LiveBytes)
		if memory.PeakReservedBytes > 3*budget {
			t.Fatalf("T07 process peak %d exceeds three per-node tracker limits (%d)", memory.PeakReservedBytes, 3*budget)
		}
		if nativeAfter.IntermediateDataPoints != nativeBefore.execution.IntermediateDataPoints {
			t.Fatal("T07 resource query constructed intermediate DataPoints")
		}
		if code == codes.OK && (aggregateAfter.InputRows-aggregateBefore.InputRows != uint64(cfg.Cardinality) ||
			aggregateAfter.Groups-aggregateBefore.Groups != uint64(cfg.Cardinality)) {
			t.Fatal("T07 resource success did not aggregate every deduplicated row and group")
		}
		samples = append(samples, map[string]any{
			"iteration": iteration, "status": code.String(), "elapsed_ns": elapsed, "expected_hash": expectedHash,
			"returned_rows": len(response.GetMeasureResult().GetDataPoints()), "input_rows": aggregateAfter.InputRows - aggregateBefore.InputRows,
			"groups":              aggregateAfter.Groups - aggregateBefore.Groups,
			"expected_input_rows": cfg.Cardinality, "expected_groups": cfg.Cardinality,
			"aggregate_executions": aggregateAfter.Executions - aggregateBefore.Executions,
			"ql_batch_executions":  nativeAfter.QLBatchExecutions - nativeBefore.execution.QLBatchExecutions,
			"native_leaf_calls":    nativeAfter.NativeLeafCalls - nativeBefore.execution.NativeLeafCalls,
			"native_source_rows":   nativeAfter.NativeInputRows - nativeBefore.execution.NativeInputRows,
			"native_frame_rows":    nativeAfter.NativeFrameRows - nativeBefore.execution.NativeFrameRows,
			"state_bytes":          aggregateAfter.StateBytes - aggregateBefore.StateBytes, "state_capacity": aggregateAfter.StateCapacity - aggregateBefore.StateCapacity,
			"key_bytes": aggregateAfter.KeyBytes - aggregateBefore.KeyBytes, "key_capacity": aggregateAfter.KeyCapacity - aggregateBefore.KeyCapacity,
			"process_peak_reserved_bytes": memory.PeakReservedBytes, "configured_per_tracker_limit_bytes": budget,
			"tracker_lease_charged_bytes":     memory.LeaseChargedBytes - nativeBefore.memory.LeaseChargedBytes,
			"aggregate_reserved_bytes":        aggregateAfter.ReservedBytes - aggregateBefore.ReservedBytes,
			"aggregate_released_bytes":        aggregateAfter.ReleasedBytes - aggregateBefore.ReleasedBytes,
			"post_close_aggregate_live_bytes": aggregateAfter.LiveBytes, "post_close_reserved_bytes": memory.LiveReservedBytes,
			"post_close_owner_pins": vectorized.OutstandingOwnerPins(), "post_close_live_results": nativeAfter.LiveResults,
			"resources": resourceDelta(before, after), "allocations": allocationDelta(before, after, 1),
		})
	}
	recoveryRequest, recoveryExpected := qlbT07RawVisibilityChunk(0, min(cfg.Cardinality, 16384), 0, base)
	gomega.Expect(waitForQLBT07RawChunk(ctx, client, recoveryRequest, recoveryExpected)).To(gomega.Succeed(), "bounded host-pruned QL recovery query")
	report := map[string]any{
		"scenario": "qlb_t07_resources", "cardinality": cfg.Cardinality, "query_memory_mib": cfg.QueryMemoryMiB,
		"successes": successes, "resource_rejections": rejections, "samples": samples,
		"recovery":            "pass",
		"peak_scope":          "process-global sum across one liaison and two independent data-node trackers",
		"unavailable_metrics": []string{"per_tracker_peak_reserved_bytes", "native_retained_frame_bytes", "total_parent_lease_charges"},
	}
	body, marshalErr := json.MarshalIndent(report, "", "  ")
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	if err = os.MkdirAll(cfg.ReportDir, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(cfg.ReportDir, fmt.Sprintf("qlb_t07_resources_%drows_%dmib.json", cfg.Cardinality, cfg.QueryMemoryMiB))
	if err = os.WriteFile(path, append(body, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Logf("T07 resource probe: success=%d ResourceExhausted=%d artifact=%s", successes, rejections, path)
}

func assertQLBT07ResourceCleanup(t *testing.T, before qlbT02MetricsSnapshot, aggregateLive int64) {
	t.Helper()
	if vmeasure.BatchExecutionStats().LiveResults != before.execution.LiveResults || vectorized.QueryMemoryStats().LiveReservedBytes != before.memory.LiveReservedBytes ||
		vectorized.OutstandingOwnerPins() != before.owners || pool.AllRefsCount()["native-query-codec-flatten"] != before.flatten ||
		ql.AggregateExecutionStats().LiveBytes != aggregateLive {
		t.Fatal("T07 resource request leaked a result, reservation, owner or native flatten buffer")
	}
}
