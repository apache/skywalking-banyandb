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
	"strings"
	"testing"
	"time"

	"github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	bydbqlv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/bydbql/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/pool"
	"github.com/apache/skywalking-banyandb/pkg/query/ql"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
)

func TestQLBT03ResourceBounds(t *testing.T) {
	cfg := LoadConfig()
	if !cfg.RunBench {
		t.Skip("set RUN_DISTRIBUTED_QUERY_BENCH=1 to run the Docker-gated T03 resource probe")
	}
	if err := validateQLBT03ResourceConfig(cfg); err != nil {
		t.Fatal(err)
	}
	if !vectorized.QueryMetricsEnabled {
		t.Fatal("T03 resource probe requires a test binary built with -tags=querymetrics")
	}
	if err := logger.Init(logger.Logging{Env: "dev", Level: "info"}); err != nil {
		t.Fatal(err)
	}
	gomega.RegisterTestingT(t)
	cfg.Engine = engineMeasure
	cfg.Mode = modeVec
	cfg.Scenario = ScenarioQLBT03
	cfg.Cardinality = 1024
	cluster, base, err := startBenchCluster(t, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if _, err = writeBenchmarkData(ctx, cluster.conn, cfg, cfg.Cardinality, base); err != nil {
		t.Fatal(err)
	}
	if err = waitForQLBT03Visibility(ctx, cluster.conn, cfg.Cardinality, base); err != nil {
		t.Fatal(err)
	}
	request, expected, err := qlbT03ResourceQuery(base)
	if err != nil {
		t.Fatal(err)
	}
	expectedHash := qlbT01Hash(expected)
	client := bydbqlv1.NewBydbQLServiceClient(cluster.conn)
	samples := make([]map[string]any, 0, 5)
	successes, rejections := 0, 0
	budget := int64(cfg.QueryMemoryMiB) << 20
	for iteration := 0; iteration < 5; iteration++ {
		qlbT03AssertResourcesReleased(t)
		if !vectorized.ResetQueryMemoryPeak() {
			t.Fatal("resource probe started with live reservations")
		}
		executionBefore := vmeasure.BatchExecutionStats()
		memoryBefore := vectorized.QueryMemoryStats()
		rawBefore := ql.RawExecutionStats()
		clockBefore := ql.ExecutionClockCalls()
		before := captureProcessSnapshot()
		started := time.Now()
		queryCtx, queryCancel := context.WithTimeout(ctx, queryTimeout(cfg.Cardinality))
		response, queryErr := client.Query(queryCtx, request)
		queryCancel()
		elapsed := time.Since(started)
		after := captureProcessSnapshot()
		code := status.Code(queryErr)
		actualRows := len(response.GetMeasureResult().GetDataPoints())
		switch code {
		case codes.OK:
			if response.GetMeasureResult() == nil || actualRows != 64 || qlbT01Hash(response.GetMeasureResult()) != expectedHash {
				t.Fatalf("resource probe %d returned an incomplete or incorrect response", iteration)
			}
			successes++
		case codes.ResourceExhausted:
			if response.GetMeasureResult() != nil {
				t.Fatalf("resource probe %d returned partial results with ResourceExhausted", iteration)
			}
			rejections++
		default:
			t.Fatalf("resource probe %d returned unexpected %s: %v", iteration, code, queryErr)
		}
		qlbT03AssertResourcesReleased(t)
		executionAfter := vmeasure.BatchExecutionStats()
		memoryAfter := vectorized.QueryMemoryStats()
		rawAfter := ql.RawExecutionStats()
		if memoryAfter.PeakReservedBytes > 3*budget {
			t.Fatalf("process-global peak %d exceeds three independent node tracker limits (%d)", memoryAfter.PeakReservedBytes, 3*budget)
		}
		if executionAfter.IntermediateDataPoints != executionBefore.IntermediateDataPoints {
			t.Fatal("resource probe materialized intermediate DataPoints")
		}
		if rawAfter.ProjectionCopiedValueBytes != rawBefore.ProjectionCopiedValueBytes {
			t.Fatal("resource probe projection copied source values")
		}
		if ql.ExecutionClockCalls()-clockBefore != 1 {
			t.Fatal("resource probe did not capture its execution clock exactly once")
		}
		if code == codes.OK && executionAfter.FinalDataPoints-executionBefore.FinalDataPoints != 64 {
			t.Fatal("successful resource probe did not serialize exactly 64 final rows")
		}
		samples = append(samples, map[string]any{
			"iteration": iteration, "status": code.String(), "returned_rows": actualRows, "elapsed_ns": elapsed,
			"resources": resourceDelta(before, after), "allocations": allocationDelta(before, after, 1),
			"process_peak_reserved_bytes": memoryAfter.PeakReservedBytes, "configured_per_tracker_limit_bytes": budget,
			"process_peak_bound_bytes": 3 * budget, "tracker_lease_charged_bytes": memoryAfter.LeaseChargedBytes - memoryBefore.LeaseChargedBytes,
			"intermediate_data_points": executionAfter.IntermediateDataPoints - executionBefore.IntermediateDataPoints,
			"final_data_points":        executionAfter.FinalDataPoints - executionBefore.FinalDataPoints,
			"post_close_live_results":  executionAfter.LiveResults, "post_close_reserved_bytes": memoryAfter.LiveReservedBytes,
			"post_close_owner_pins": vectorized.OutstandingOwnerPins(), "post_close_flatten_pool_refs": pool.AllRefsCount()["native-query-codec-flatten"],
		})
	}
	if successes+rejections != 5 {
		t.Fatal("resource probe did not execute all five requests")
	}
	report := map[string]any{
		"scenario": "qlb_t03_resources", "query_memory_mib": cfg.QueryMemoryMiB, "cardinality": 1024, "projection_columns": 2048,
		"expected_rows": 64, "expected_hash": expectedHash, "successes": successes, "resource_rejections": rejections, "samples": samples,
		"peak_scope":          "process-global sum across one liaison and two data-node trackers",
		"unavailable_metrics": []string{"per_tracker_peak_reserved_bytes", "total_parent_lease_charges", "post_close_parent_lease_reservations"},
	}
	body, marshalErr := json.MarshalIndent(report, "", "  ")
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	if err = os.MkdirAll(cfg.ReportDir, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(cfg.ReportDir, fmt.Sprintf("qlb_t03_resources_%dmib.json", cfg.QueryMemoryMiB))
	if err = os.WriteFile(path, append(body, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Logf("resource probe: successes=%d ResourceExhausted=%d artifact=%s", successes, rejections, path)
}

func validateQLBT03ResourceConfig(cfg Config) error {
	if !cfg.InContainer {
		return fmt.Errorf("T03 resource probe requires DQB_IN_CONTAINER=1")
	}
	if cfg.Engine != engineMeasure {
		return fmt.Errorf("T03 resource probe requires DQB_ENGINE=measure")
	}
	if cfg.QueryMemoryMiB != 16 && cfg.QueryMemoryMiB != 256 {
		return fmt.Errorf("T03 resource probe requires DQB_QUERY_MEMORY_MIB=16 or 256")
	}
	return nil
}

func TestQLBT03ResourceConfig(t *testing.T) {
	for _, budget := range []int{16, 256} {
		cfg := Config{InContainer: true, Engine: engineMeasure, QueryMemoryMiB: budget}
		if err := validateQLBT03ResourceConfig(cfg); err != nil {
			t.Fatal(err)
		}
		cfg.InContainer = false
		if validateQLBT03ResourceConfig(cfg) == nil {
			t.Fatal("resource probe accepted an unbounded host run")
		}
	}
	cfg := Config{InContainer: true, Engine: engineMeasure, QueryMemoryMiB: 8}
	if validateQLBT03ResourceConfig(cfg) == nil {
		t.Fatal("resource probe accepted an unplanned memory limit")
	}
}

func qlbT03ResourceQuery(base time.Time) (*bydbqlv1.QueryRequest, *measurev1.QueryResponse, error) {
	request, _, err := qlbT03Variant("ql_repeated_projection", 1024, base)
	if err != nil {
		return nil, nil, err
	}
	columns := []string{"id", "entity_id"}
	for field := 0; field < 2048; field++ {
		columns = append(columns, fmt.Sprintf("value::FIELD AS value%04d", field))
	}
	_, source, found := strings.Cut(request.Query, " FROM ")
	if !found {
		return nil, nil, fmt.Errorf("resource fixture query has no source")
	}
	request.Query = "SELECT " + strings.Join(columns, ", ") + " FROM " + source
	expected := qlbT01Expected(1024, 0, base)
	for _, point := range expected.DataPoints {
		value := point.Fields[1].Value.GetInt().Value
		point.Fields = nil
		for field := 0; field < 2048; field++ {
			point.Fields = append(point.Fields, &measurev1.DataPoint_Field{Name: fmt.Sprintf("value%04d", field), Value: intFieldValue(value)})
		}
	}
	return request, expected, nil
}

func qlbT03AssertResourcesReleased(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		execution := vmeasure.BatchExecutionStats()
		memory := vectorized.QueryMemoryStats()
		flatten, tracked := pool.AllRefsCount()["native-query-codec-flatten"]
		if tracked && execution.LiveResults == 0 && memory.LiveReservedBytes == 0 && vectorized.OutstandingOwnerPins() == 0 && flatten == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("resource probe leaked results=%d reservations=%d owners=%d flatten=%d (tracked=%v)",
				execution.LiveResults, memory.LiveReservedBytes, vectorized.OutstandingOwnerPins(), flatten, tracked)
		}
		time.Sleep(time.Millisecond)
	}
}
