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
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package querybench

import (
	"fmt"

	liaisongrpc "github.com/apache/skywalking-banyandb/banyand/liaison/grpc"
	"github.com/apache/skywalking-banyandb/pkg/pool"
	"github.com/apache/skywalking-banyandb/pkg/query/ql"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
)

type qlbT14MetricsSnapshot struct {
	base           qlbT08MetricsSnapshot
	having, filter ql.FilterStats
}

func qlbT14MetricsStart() (qlbT14MetricsSnapshot, error) {
	base, err := qlbT08MetricsStart()
	if err != nil {
		return qlbT14MetricsSnapshot{}, err
	}
	having, filter := ql.HavingExecutionStats(), ql.FilterExecutionStats()
	if having.LiveBytes != 0 || filter.LiveBytes != 0 {
		return qlbT14MetricsSnapshot{}, fmt.Errorf("T14 starts with live predicate state")
	}
	return qlbT14MetricsSnapshot{base: base, having: having, filter: filter}, nil
}

func qlbT14MetricsFinish(before qlbT14MetricsSnapshot, samples int, oracle *qlbT14Oracle) (map[string]uint64, []string, error) {
	baseline := before.base.local.native
	waitForQLBT02ResourceRelease(baseline.execution.LiveResults, baseline.memory.LiveReservedBytes, baseline.owners, baseline.flatten)
	execution, memory := vmeasure.BatchExecutionStats(), vectorized.QueryMemoryStats()
	if execution.LiveResults != baseline.execution.LiveResults || memory.LiveReservedBytes != baseline.memory.LiveReservedBytes ||
		vectorized.OutstandingOwnerPins() != baseline.owners || pool.AllRefsCount()["native-query-codec-flatten"] != baseline.flatten {
		return nil, nil, fmt.Errorf("T14 leaked native results/reservations/owners/flatten buffers")
	}
	local, merge := ql.AggregateExecutionStats(), ql.NativeAggregateExecutionStats()
	having, filter := ql.HavingExecutionStats(), ql.FilterExecutionStats()
	if local.LiveBytes != before.base.local.aggregate.LiveBytes || merge.LiveBytes != before.base.merge.LiveBytes ||
		having.LiveBytes != before.having.LiveBytes || filter.LiveBytes != before.filter.LiveBytes {
		return nil, nil, fmt.Errorf("T14 leaked operator memory")
	}
	metrics := map[string]uint64{
		"ql_batch_executions":      execution.QLBatchExecutions - baseline.execution.QLBatchExecutions,
		"native_batch_executions":  execution.NativeBatchExecutions - baseline.execution.NativeBatchExecutions,
		"native_leaf_calls":        execution.NativeLeafCalls - baseline.execution.NativeLeafCalls,
		"native_source_rows":       execution.NativeInputRows - baseline.execution.NativeInputRows,
		"native_frame_rows":        execution.NativeFrameRows - baseline.execution.NativeFrameRows,
		"intermediate_data_points": execution.IntermediateDataPoints - baseline.execution.IntermediateDataPoints,
		"final_data_points":        execution.FinalDataPoints - baseline.execution.FinalDataPoints,
		"output_rows":              uint64(samples * len(oracle.expected.DataPoints)), "process_peak_reserved_bytes": uint64(memory.PeakReservedBytes),
		"tracker_lease_charged_bytes":   memory.LeaseChargedBytes - baseline.memory.LeaseChargedBytes,
		"post_close_live_results_delta": 0, "post_close_reserved_bytes_delta": 0, "post_close_owner_pins_delta": 0, "post_close_flatten_pool_refs_delta": 0,
		"aggregate_executions":     local.Executions - before.base.local.aggregate.Executions,
		"aggregate_input_rows":     local.InputRows - before.base.local.aggregate.InputRows,
		"aggregate_groups":         local.Groups - before.base.local.aggregate.Groups,
		"aggregate_state_lanes":    local.StateLanes - before.base.local.aggregate.StateLanes,
		"aggregate_state_bytes":    local.StateBytes - before.base.local.aggregate.StateBytes,
		"aggregate_key_bytes":      local.KeyBytes - before.base.local.aggregate.KeyBytes,
		"aggregate_reserved_bytes": local.ReservedBytes - before.base.local.aggregate.ReservedBytes,
		"aggregate_released_bytes": local.ReleasedBytes - before.base.local.aggregate.ReleasedBytes, "aggregate_live_bytes": 0,
		"merge_executions":     merge.Executions - before.base.merge.Executions,
		"merge_branches":       merge.Branches - before.base.merge.Branches,
		"merge_input_rows":     merge.InputRows - before.base.merge.InputRows,
		"merge_groups":         merge.Groups - before.base.merge.Groups,
		"merge_state_lanes":    merge.StateLanes - before.base.merge.StateLanes,
		"merge_state_bytes":    merge.StateBytes - before.base.merge.StateBytes,
		"merge_key_bytes":      merge.KeyBytes - before.base.merge.KeyBytes,
		"merge_visits":         merge.MergeVisits - before.base.merge.MergeVisits,
		"merge_reserved_bytes": merge.ReservedBytes - before.base.merge.ReservedBytes,
		"merge_released_bytes": merge.ReleasedBytes - before.base.merge.ReleasedBytes, "merge_live_bytes": 0,
	}
	qlbT14FilterMetrics(metrics, "having", before.having, having)
	qlbT14FilterMetrics(metrics, "filter", before.filter, filter)
	cache := liaisongrpc.BydbQLPlanCacheStats()
	metrics["plan_cache_hits"], metrics["plan_cache_compiles"] = cache.Hits-before.base.local.cache.Hits, cache.Compiles-before.base.local.cache.Compiles
	metrics["execution_clock_calls"] = ql.ExecutionClockCalls() - before.base.local.clock
	if err := validateQLBT14Metrics(metrics, samples, oracle); err != nil {
		return nil, nil, err
	}
	return metrics, qlbT14UnavailableMetrics(), nil
}
