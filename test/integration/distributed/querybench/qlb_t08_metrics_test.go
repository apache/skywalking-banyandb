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
	"fmt"

	liaisongrpc "github.com/apache/skywalking-banyandb/banyand/liaison/grpc"
	"github.com/apache/skywalking-banyandb/pkg/pool"
	"github.com/apache/skywalking-banyandb/pkg/query/ql"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
)

type qlbT08MetricsSnapshot struct {
	local qlbT07MetricsSnapshot
	merge ql.NativeAggregateExecutionCounters
}

func qlbT08SelectMode(mode string) (func(), error) {
	return liaisongrpc.SetNativeAggregateHooksForTest(liaisongrpc.NativeAggregateTestHooks{Disabled: mode == "local"}), nil
}

func qlbT08MetricsStart() (qlbT08MetricsSnapshot, error) {
	local, err := qlbT07MetricsStart()
	if err != nil {
		return qlbT08MetricsSnapshot{}, err
	}
	merge := ql.NativeAggregateExecutionStats()
	if merge.LiveBytes != 0 {
		return qlbT08MetricsSnapshot{}, fmt.Errorf("T08 merge starts with %d live bytes", merge.LiveBytes)
	}
	return qlbT08MetricsSnapshot{local: local, merge: merge}, nil
}

func qlbT08MetricsFinish(before qlbT08MetricsSnapshot, shape qlbT08Shape, mode string, samples, inputRows int) (map[string]uint64, []string, error) {
	baseline := before.local.native
	waitForQLBT02ResourceRelease(baseline.execution.LiveResults, baseline.memory.LiveReservedBytes, baseline.owners, baseline.flatten)
	execution, memory := vmeasure.BatchExecutionStats(), vectorized.QueryMemoryStats()
	if execution.LiveResults != baseline.execution.LiveResults || memory.LiveReservedBytes != baseline.memory.LiveReservedBytes ||
		vectorized.OutstandingOwnerPins() != baseline.owners || pool.AllRefsCount()["native-query-codec-flatten"] != baseline.flatten {
		return nil, nil, fmt.Errorf("T08 %s/%s leaked native results/reservations/owners/flatten buffers", shape.name, mode)
	}
	rows := uint64(samples * min(inputRows, 16))
	if mode == "individual" {
		rows *= uint64(shape.branches)
	}
	metrics := map[string]uint64{
		"ql_batch_executions":      execution.QLBatchExecutions - baseline.execution.QLBatchExecutions,
		"native_batch_executions":  execution.NativeBatchExecutions - baseline.execution.NativeBatchExecutions,
		"native_leaf_calls":        execution.NativeLeafCalls - baseline.execution.NativeLeafCalls,
		"native_source_rows":       execution.NativeInputRows - baseline.execution.NativeInputRows,
		"native_frame_rows":        execution.NativeFrameRows - baseline.execution.NativeFrameRows,
		"intermediate_data_points": execution.IntermediateDataPoints - baseline.execution.IntermediateDataPoints,
		"final_data_points":        execution.FinalDataPoints - baseline.execution.FinalDataPoints,
		"output_rows":              rows, "process_peak_reserved_bytes": uint64(memory.PeakReservedBytes),
		"tracker_lease_charged_bytes":   memory.LeaseChargedBytes - baseline.memory.LeaseChargedBytes,
		"post_close_live_results_delta": 0, "post_close_reserved_bytes_delta": 0, "post_close_owner_pins_delta": 0, "post_close_flatten_pool_refs_delta": 0,
	}
	local, merge := ql.AggregateExecutionStats(), ql.NativeAggregateExecutionStats()
	if local.LiveBytes != before.local.aggregate.LiveBytes || merge.LiveBytes != before.merge.LiveBytes {
		return nil, nil, fmt.Errorf("T08 %s/%s leaked local/merge aggregate memory", shape.name, mode)
	}
	metrics["execution_clock_calls"] = ql.ExecutionClockCalls() - before.local.clock
	metrics["aggregate_executions"] = local.Executions - before.local.aggregate.Executions
	metrics["aggregate_input_rows"] = local.InputRows - before.local.aggregate.InputRows
	metrics["aggregate_groups"] = local.Groups - before.local.aggregate.Groups
	metrics["aggregate_group_capacity"] = local.GroupCapacity - before.local.aggregate.GroupCapacity
	metrics["aggregate_key_bytes"] = local.KeyBytes - before.local.aggregate.KeyBytes
	metrics["aggregate_key_capacity"] = local.KeyCapacity - before.local.aggregate.KeyCapacity
	metrics["aggregate_state_bytes"] = local.StateBytes - before.local.aggregate.StateBytes
	metrics["aggregate_state_capacity"] = local.StateCapacity - before.local.aggregate.StateCapacity
	metrics["aggregate_state_lanes"] = local.StateLanes - before.local.aggregate.StateLanes
	metrics["aggregate_reserved_bytes"] = local.ReservedBytes - before.local.aggregate.ReservedBytes
	metrics["aggregate_released_bytes"] = local.ReleasedBytes - before.local.aggregate.ReleasedBytes
	metrics["aggregate_live_bytes"] = uint64(local.LiveBytes - before.local.aggregate.LiveBytes)
	metrics["merge_executions"] = merge.Executions - before.merge.Executions
	metrics["merge_branches"] = merge.Branches - before.merge.Branches
	metrics["merge_input_rows"] = merge.InputRows - before.merge.InputRows
	metrics["merge_groups"] = merge.Groups - before.merge.Groups
	metrics["merge_group_capacity"] = merge.GroupCapacity - before.merge.GroupCapacity
	metrics["merge_key_bytes"] = merge.KeyBytes - before.merge.KeyBytes
	metrics["merge_key_capacity"] = merge.KeyCapacity - before.merge.KeyCapacity
	metrics["merge_state_bytes"] = merge.StateBytes - before.merge.StateBytes
	metrics["merge_state_capacity"] = merge.StateCapacity - before.merge.StateCapacity
	metrics["merge_state_lanes"] = merge.StateLanes - before.merge.StateLanes
	metrics["merge_visits"] = merge.MergeVisits - before.merge.MergeVisits
	metrics["merge_reserved_bytes"] = merge.ReservedBytes - before.merge.ReservedBytes
	metrics["merge_released_bytes"] = merge.ReleasedBytes - before.merge.ReleasedBytes
	metrics["merge_live_bytes"] = uint64(merge.LiveBytes - before.merge.LiveBytes)
	cache := liaisongrpc.BydbQLPlanCacheStats()
	metrics["plan_cache_hits"], metrics["plan_cache_compiles"] = cache.Hits-before.local.cache.Hits, cache.Compiles-before.local.cache.Compiles
	metrics["native_branches_per_query"] = 0
	metrics["fallback_queries"] = 0
	metrics["rpc_calls"] = uint64(samples)
	switch {
	case mode == "individual":
		metrics["rpc_calls"] = uint64(samples * shape.branches)
		metrics["native_branches_per_query"] = uint64(shape.branches)
	case mode == "pushdown" && shape.branches > 0:
		metrics["native_branches_per_query"] = uint64(shape.branches)
	default:
		metrics["fallback_queries"] = uint64(samples)
	}
	if err := validateQLBT08Metrics(metrics, shape, mode, samples, inputRows); err != nil {
		return nil, nil, err
	}
	return metrics, []string{"storage_physical_scanned_rows", "native_retained_frame_bytes", "total_parent_lease_charges"}, nil
}
