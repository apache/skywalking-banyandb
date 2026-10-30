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
	"sync/atomic"
	"testing"
	"time"

	"github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	bydbqlv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/bydbql/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	liaisongrpc "github.com/apache/skywalking-banyandb/banyand/liaison/grpc"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/pool"
	"github.com/apache/skywalking-banyandb/pkg/query/ql"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
)

func TestQLBT12ResourceReadinessUsesGlobalRows(t *testing.T) {
	base := time.Unix(1700000000, 0)
	response := &measurev1.QueryResponse{}
	for _, sample := range []struct {
		row      int
		v, score int64
		f        float64
	}{{117, -28, 17, -3.25}, {101, -44, 1, 0}} {
		response.DataPoints = append(response.DataPoints, &measurev1.DataPoint{
			Timestamp: timestamppb.New(base.Add(time.Duration(sample.row) * time.Millisecond)),
			TagFamilies: []*modelv1.TagFamily{{Name: benchTagFamily, Tags: []*modelv1.Tag{{
				Key: "rid", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: int64(sample.row)}}},
			}}}},
			Fields: []*measurev1.DataPoint_Field{
				{Name: "v", Value: intFieldValue(sample.v)}, {Name: "f", Value: qlbT07Float(sample.f)}, {Name: "score", Value: intFieldValue(sample.score)},
			},
		})
	}
	if err := checkQLBT12ResourceChunk(response, 96, 128, 5, base); err != nil {
		t.Fatal(err)
	}
	response.DataPoints[1].Fields[2].Value = intFieldValue(5)
	if err := checkQLBT12ResourceChunk(response, 96, 128, 5, base); err == nil {
		t.Fatal("relative-window score accepted instead of global row%100")
	}
}

func checkQLBT12ResourceChunk(response *measurev1.QueryResponse, first, end, host int, base time.Time) error {
	start := first + (host-first%16+16)%16
	expected := 0
	if start < end {
		expected = (end - start + 15) / 16
	}
	if len(response.GetDataPoints()) != expected {
		return fmt.Errorf("T12 readiness window[%d,%d)host%d returned%d expected%d", first, end, host, len(response.GetDataPoints()), expected)
	}
	seen := make(map[int64]bool, expected)
	for _, point := range response.GetDataPoints() {
		nanos := point.GetTimestamp().AsTime().UnixNano() - base.UnixNano()
		row := nanos / int64(time.Millisecond)
		if nanos < 0 || nanos%int64(time.Millisecond) != 0 || row < int64(first) || row >= int64(end) || row%16 != int64(host) || seen[row] {
			return fmt.Errorf("T12 readiness global row identity mismatch")
		}
		seen[row] = true
		if len(point.TagFamilies) != 1 || point.TagFamilies[0].Name != benchTagFamily || len(point.TagFamilies[0].Tags) != 1 {
			return fmt.Errorf("T12 readiness tag projection mismatch")
		}
		rid := point.TagFamilies[0].Tags[0]
		if rid.Key != "rid" || rid.Value.GetInt() == nil || rid.Value.GetInt().Value != row {
			return fmt.Errorf("T12 readiness rid disagrees with global timestamp identity")
		}
		if len(point.Fields) != 3 || point.Fields[0].Name != "v" || point.Fields[1].Name != "f" || point.Fields[2].Name != "score" {
			return fmt.Errorf("T12 readiness field projection mismatch")
		}
		integer, floating := &modelv1.FieldValue{Value: &modelv1.FieldValue_Null{}}, &modelv1.FieldValue{Value: &modelv1.FieldValue_Null{}}
		if row%5 != 0 {
			integer = intFieldValue(row%97 - 48)
		}
		if row%7 != 0 {
			floating = &modelv1.FieldValue{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: float64(row%29-14) / 4}}}
		}
		for index, want := range []*modelv1.FieldValue{integer, floating, intFieldValue(row % 100)} {
			if err := checkQLBT10Value(point.Fields[index].Value, want); err != nil {
				return fmt.Errorf("T12 readiness globalrow%d field%s: %w", row, point.Fields[index].Name, err)
			}
		}
	}
	return nil
}

func TestQLBT12ResourceBounds(t *testing.T) {
	cfg := LoadConfig()
	if !cfg.RunBench {
		t.Skip("set RUN_DISTRIBUTED_QUERY_BENCH=1 for the Docker-gated T12 resource probe")
	}
	if !cfg.InContainer || cfg.Engine != engineMeasure || (cfg.QueryMemoryMiB != 16 && cfg.QueryMemoryMiB != 256) {
		t.Fatal("T12 resource probe requires a container, measure engine and 16 or 256 MiB query trackers")
	}
	if cfg.Cardinality != 100000 && cfg.Cardinality != 1000000 {
		t.Fatal("T12 resource probe requires 100000 or 1000000 rows")
	}
	if err := logger.Init(logger.Logging{Env: "dev", Level: "info"}); err != nil {
		t.Fatal(err)
	}
	gomega.RegisterTestingT(t)
	cfg.Scenario, cfg.Mode = ScenarioQLBT12, modeVec
	report := map[string]any{
		"scenario": "qlb_t12_resources", "variant": "aggregate_narrow_p50", "cardinality": cfg.Cardinality, "query_memory_mib": cfg.QueryMemoryMiB,
		"readiness": "not_started", "recovery": "not_run", "samples": []map[string]any{},
		"peak_scope":          "process-global sum across one liaison and two independent data-node trackers",
		"unavailable_metrics": []string{"per_tracker_peak_reserved_bytes", "native_retained_frame_bytes", "total_parent_lease_charges"},
	}
	defer saveQLBT12ResourceReport(t, cfg, report)
	cluster, base, err := startBenchCluster(t, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.close()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	if _, err = writeQLBT12Data(ctx, cluster.conn, cfg, base); err != nil {
		report["readiness"] = "write_failed"
		report["readiness_error"] = err.Error()
		t.Fatal(err)
	}
	client := bydbqlv1.NewBydbQLServiceClient(cluster.conn)
	verified, windows, readinessErr := verifyQLBT12ResourceFixture(ctx, client, cfg.Cardinality, base)
	report["readiness_rows"], report["readiness_queries"] = verified, windows
	if readinessErr != nil {
		report["readiness"], report["readiness_error"] = "blocked", readinessErr.Error()
		report["readiness_status"] = status.Code(readinessErr).String()
		report["recovery"] = "not_run_readiness_blocked"
		waitForQLBT02ResourceRelease(0, 0, 0, 0)
		if cleanupErr := checkQLBT12ResourceCleanup(); cleanupErr != nil {
			report["cleanup_error"] = cleanupErr.Error()
			t.Error(cleanupErr)
		}
		report["readiness_memory"] = vectorized.QueryMemoryStats()
		t.Fatalf("T12 resource readiness blocked after %d verified rows: %v", verified, readinessErr)
	}
	report["readiness"] = "pass"
	request, err := qlbT12Query("aggregate_narrow_p50", cfg.Cardinality, base)
	if err != nil {
		t.Fatal(err)
	}
	oracle, err := newQLBT12Oracle("aggregate_narrow_p50", cfg.Cardinality)
	if err != nil {
		t.Fatal(err)
	}
	var nativeAggregates atomic.Uint64
	restore := liaisongrpc.SetNativeAggregateHooksForTest(liaisongrpc.NativeAggregateTestHooks{
		BeforeBranch: func(context.Context, int, *measurev1.QueryRequest) error { nativeAggregates.Add(1); return nil },
	})
	defer restore()
	successes, rejections := 0, 0
	samples := make([]map[string]any, 0, 5)
	for iteration := 0; iteration < 5; iteration++ {
		sample, probeErr := probeQLBT12Resources(ctx, client, request, oracle, cfg, base, &nativeAggregates)
		sample["iteration"] = iteration
		samples = append(samples, sample)
		report["samples"] = samples
		if probeErr != nil {
			report["probe_error"] = probeErr.Error()
			t.Fatal(probeErr)
		}
		if sample["status"] == codes.OK.String() {
			successes++
		} else {
			rejections++
		}
	}
	report["successes"], report["resource_rejections"] = successes, rejections
	recovery := qlbT12ResourceChunk(0, min(cfg.Cardinality, 8192), 0, base)
	if err = waitQLBT12ResourceChunk(ctx, client, recovery, 0, min(cfg.Cardinality, 8192), 0, base); err != nil {
		report["recovery"], report["recovery_error"] = "failed", err.Error()
		t.Fatal(err)
	}
	waitForQLBT02ResourceRelease(0, 0, 0, 0)
	if err = checkQLBT12ResourceCleanup(); err != nil {
		t.Fatal(err)
	}
	report["recovery"] = "pass"
	t.Logf("T12 resource probe: success=%d ResourceExhausted=%d verified_rows=%d", successes, rejections, verified)
}

func verifyQLBT12ResourceFixture(ctx context.Context, client bydbqlv1.BydbQLServiceClient, cardinality int, base time.Time) (int, int, error) {
	verified, queries := 0, 0
	// Read one complete entity at a time. Each request returns at most 8192 rows;
	// this bounds response ownership without relaxing native decoder protection.
	for first := 0; first < cardinality; first += 8192 * 16 {
		end := min(first+8192*16, cardinality)
		for host := 0; host < 16; host++ {
			request := qlbT12ResourceChunk(first, end, host, base)
			if err := waitQLBT12ResourceChunk(ctx, client, request, first, end, host, base); err != nil {
				return verified, queries + 1, err
			}
			start := first + (host-first%16+16)%16
			if start < end {
				verified += (end - start + 15) / 16
			}
			queries++
		}
	}
	return verified, queries, nil
}

func qlbT12ResourceChunk(first, end, host int, base time.Time) *bydbqlv1.QueryRequest {
	return &bydbqlv1.QueryRequest{
		Query: "SELECT rid, v::FIELD, f::FIELD, score::FIELD FROM " + benchGroupName + "." + qlbT12Measure + " WHERE host = ? AND time >= ? AND time < ?",
		Params: []*modelv1.TagValue{
			strTagValue(fmt.Sprintf("host_%02d", host)),
			{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(base.Add(time.Duration(first) * time.Millisecond))}},
			{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(base.Add(time.Duration(end) * time.Millisecond))}},
		},
	}
}

func waitQLBT12ResourceChunk(ctx context.Context, client bydbqlv1.BydbQLServiceClient, request *bydbqlv1.QueryRequest, first, end, host int, base time.Time) error {
	deadline := time.Now().Add(time.Minute)
	for {
		queryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		response, queryErr := client.Query(queryCtx, request)
		cancel()
		if queryErr == nil {
			queryErr = checkQLBT12ResourceChunk(response.GetMeasureResult(), first, end, host, base)
			if queryErr == nil {
				return nil
			}
		}
		switch status.Code(queryErr) {
		case codes.InvalidArgument, codes.PermissionDenied, codes.Unauthenticated, codes.Unimplemented, codes.ResourceExhausted:
			return fmt.Errorf("T12 bounded host-pruned readiness cannot execute: %w", queryErr)
		default:
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("T12 bounded fixture visibility: %w", queryErr)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func probeQLBT12Resources(ctx context.Context, client bydbqlv1.BydbQLServiceClient, request *bydbqlv1.QueryRequest, oracle *qlbT12Oracle,
	cfg Config, base time.Time, nativeAggregates *atomic.Uint64,
) (map[string]any, error) {
	sample := map[string]any{}
	before, err := qlbT12MetricsStart()
	if err != nil {
		return sample, err
	}
	aggregateLeaves := nativeAggregates.Load()
	processBefore := captureProcessSnapshot()
	started := time.Now()
	queryCtx, cancel := context.WithTimeout(ctx, queryTimeout(cfg.Cardinality))
	response, queryErr := client.Query(queryCtx, request)
	cancel()
	sample["elapsed_ns"], sample["status"] = time.Since(started), status.Code(queryErr).String()
	processAfter := captureProcessSnapshot()
	sample["resources"], sample["allocations"] = resourceDelta(processBefore, processAfter), allocationDelta(processBefore, processAfter, 1)
	if queryErr != nil {
		sample["error"] = queryErr.Error()
	}
	waitForQLBT02ResourceRelease(0, 0, 0, 0)
	if err = checkQLBT12ResourceCleanup(); err != nil {
		return sample, err
	}
	filter, aggregate, merge := ql.FilterExecutionStats(), ql.AggregateExecutionStats(), ql.NativeAggregateExecutionStats()
	native, memory := vmeasure.BatchExecutionStats(), vectorized.QueryMemoryStats()
	sample["filter_input_rows"], sample["filter_true_rows"] = filter.InputRows-before.filter.InputRows, filter.TrueRows-before.filter.TrueRows
	sample["filter_false_rows"], sample["filter_unknown_rows"] = filter.FalseRows-before.filter.FalseRows, filter.UnknownRows-before.filter.UnknownRows
	sample["aggregate_input_rows"] = aggregate.InputRows - before.aggregate.InputRows
	sample["expected_input_rows"], sample["expected_true_rows"] = oracle.cardinality, oracle.selected
	sample["ql_batch_executions"] = native.QLBatchExecutions - before.native.execution.QLBatchExecutions
	sample["native_leaf_calls"] = native.NativeLeafCalls - before.native.execution.NativeLeafCalls
	sample["native_source_rows"] = native.NativeInputRows - before.native.execution.NativeInputRows
	sample["native_frame_rows"] = native.NativeFrameRows - before.native.execution.NativeFrameRows
	sample["native_aggregate_leaves"] = nativeAggregates.Load() - aggregateLeaves
	sample["merge_executions"], sample["merge_branches"] = merge.Executions-before.merge.Executions, merge.Branches-before.merge.Branches
	sample["filter_reserved_bytes"], sample["filter_released_bytes"] = filter.ReservedBytes-before.filter.ReservedBytes, filter.ReleasedBytes-before.filter.ReleasedBytes
	sample["aggregate_reserved_bytes"] = aggregate.ReservedBytes - before.aggregate.ReservedBytes
	sample["aggregate_released_bytes"] = aggregate.ReleasedBytes - before.aggregate.ReleasedBytes
	sample["process_peak_reserved_bytes"], sample["per_tracker_budget_bytes"] = memory.PeakReservedBytes, int64(cfg.QueryMemoryMiB)<<20
	sample["post_close_filter_live_bytes"] = filter.LiveBytes
	sample["post_close_aggregate_live_bytes"] = aggregate.LiveBytes
	sample["post_close_merge_live_bytes"] = merge.LiveBytes
	sample["post_close_native_live_results"] = native.LiveResults
	sample["post_close_reserved_bytes"] = memory.LiveReservedBytes
	sample["post_close_owner_pins"] = vectorized.OutstandingOwnerPins()
	sample["post_close_flatten_refs"] = pool.AllRefsCount()["native-query-codec-flatten"]
	if memory.PeakReservedBytes > 3*(int64(cfg.QueryMemoryMiB)<<20) {
		return sample, fmt.Errorf("T12 process query peak exceeds three independent tracker budgets")
	}
	if nativeAggregates.Load() != aggregateLeaves || merge.Executions != before.merge.Executions || merge.Branches != before.merge.Branches {
		return sample, fmt.Errorf("T12 residual unexpectedly used native aggregate leaves")
	}
	if native.IntermediateDataPoints != before.native.execution.IntermediateDataPoints {
		return sample, fmt.Errorf("T12 constructed intermediate protobuf DataPoints")
	}
	switch status.Code(queryErr) {
	case codes.OK:
		hash, checkErr := oracle.check(response.GetMeasureResult(), base)
		if checkErr != nil {
			return sample, checkErr
		}
		sample["expected_hash"], sample["actual_hash"] = oracle.hash, hash
		sample["returned_rows"] = len(response.GetMeasureResult().GetDataPoints())
		if hash != oracle.hash {
			return sample, fmt.Errorf("T12 resource result differs from independent fixture")
		}
		if filter.InputRows-before.filter.InputRows != uint64(oracle.cardinality) || filter.TrueRows-before.filter.TrueRows != uint64(oracle.selected) ||
			aggregate.InputRows-before.aggregate.InputRows != uint64(oracle.selected) {
			return sample, fmt.Errorf("T12 resource aggregation did not consume exactly complete-filter TRUE rows")
		}
	case codes.ResourceExhausted:
		if response.GetMeasureResult() != nil {
			return sample, fmt.Errorf("T12 resource rejection returned partial output")
		}
	default:
		return sample, fmt.Errorf("T12 resource probe returned unexpected status: %w", queryErr)
	}
	return sample, nil
}

func checkQLBT12ResourceCleanup() error {
	if ql.FilterExecutionStats().LiveBytes != 0 || ql.AggregateExecutionStats().LiveBytes != 0 || ql.NativeAggregateExecutionStats().LiveBytes != 0 ||
		vmeasure.BatchExecutionStats().LiveResults != 0 || vectorized.QueryMemoryStats().LiveReservedBytes != 0 || vectorized.OutstandingOwnerPins() != 0 ||
		pool.AllRefsCount()["native-query-codec-flatten"] != 0 {
		return fmt.Errorf("T12 resource request leaked filter/aggregate/native reservations, results, owners or flatten buffers")
	}
	return nil
}

func saveQLBT12ResourceReport(t *testing.T, cfg Config, report map[string]any) {
	t.Helper()
	body, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Error(err)
		return
	}
	if err = os.MkdirAll(cfg.ReportDir, 0o755); err != nil {
		t.Error(err)
		return
	}
	path := filepath.Join(cfg.ReportDir, fmt.Sprintf("qlb_t12_resources_%drows_%dmib.json", cfg.Cardinality, cfg.QueryMemoryMiB))
	if err = os.WriteFile(path, append(body, '\n'), 0o600); err != nil {
		t.Error(err)
		return
	}
	t.Logf("T12 resource artifact: %s", path)
}
