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

type qlbT17MetricsSnapshot struct {
	native         qlbT02MetricsSnapshot
	sort           ql.SortExecutionCounters
	aggregate      ql.AggregateExecutionCounters
	filter, having ql.FilterStats
	cache          liaisongrpc.PlanCacheStats
	clock          uint64
}

func qlbT17SetFullSort(full bool) func() {
	if !full {
		return func() {}
	}
	return liaisongrpc.SetBydbQLPlanCacheHooksForTest(liaisongrpc.PlanCacheTestHooks{Context: ql.WithFullSort})
}

func qlbT17MetricsStart() (qlbT17MetricsSnapshot, error) {
	native, err := qlbT02MetricsStart()
	if err != nil {
		return qlbT17MetricsSnapshot{}, err
	}
	snapshot := qlbT17MetricsSnapshot{
		native: native, sort: ql.SortExecutionStats(), aggregate: ql.AggregateExecutionStats(),
		filter: ql.FilterExecutionStats(), having: ql.HavingExecutionStats(), cache: liaisongrpc.BydbQLPlanCacheStats(), clock: ql.ExecutionClockCalls(),
	}
	if snapshot.sort.LiveBytes != 0 || snapshot.aggregate.LiveBytes != 0 || snapshot.filter.LiveBytes != 0 || snapshot.having.LiveBytes != 0 {
		return snapshot, fmt.Errorf("T17 started with live QL operator state")
	}
	return snapshot, nil
}

func qlbT17MetricsFinish(before qlbT17MetricsSnapshot, spec qlbT17Spec, samples int, oracles []*qlbT17Oracle) (map[string]uint64, []string, error) {
	for _, oracle := range oracles {
		if len(oracle.expected.DataPoints) != len(oracles[0].expected.DataPoints) {
			return nil, nil, fmt.Errorf("T17 admitted fixture changed output size across bind sets")
		}
	}
	metrics, err := qlbT17NativeMetrics(before.native)
	if err != nil {
		return nil, nil, err
	}
	after := ql.SortExecutionStats()
	if after.LiveBytes != before.sort.LiveBytes {
		return nil, nil, fmt.Errorf("T17 leaked sort state")
	}
	metrics["sort_executions"] = after.Executions - before.sort.Executions
	metrics["sort_batches"] = after.Batches - before.sort.Batches
	metrics["sort_input_rows"] = after.InputRows - before.sort.InputRows
	metrics["sort_output_rows"] = after.OutputRows - before.sort.OutputRows
	metrics["sort_key_evaluations"] = after.KeyEvaluations - before.sort.KeyEvaluations
	metrics["sort_key_root_calls"] = after.KeyRootCalls - before.sort.KeyRootCalls
	metrics["sort_key_kernel_rows"] = after.KeyKernelRows - before.sort.KeyKernelRows
	metrics["sort_key_scalar_evaluations"] = after.KeyScalarEvaluations - before.sort.KeyScalarEvaluations
	metrics["sort_comparisons"] = after.Comparisons - before.sort.Comparisons
	metrics["sort_accepted_rows"] = after.AcceptedRows - before.sort.AcceptedRows
	metrics["sort_evicted_rows"] = after.EvictedRows - before.sort.EvictedRows
	metrics["sort_candidates"] = after.Candidates - before.sort.Candidates
	metrics["sort_peak_candidates"] = after.PeakCandidates - before.sort.PeakCandidates
	metrics["sort_candidate_capacity"] = after.CandidateCapacity - before.sort.CandidateCapacity
	metrics["sort_payload_bytes"] = after.PayloadBytes - before.sort.PayloadBytes
	metrics["sort_payload_capacity"] = after.PayloadCapacity - before.sort.PayloadCapacity
	metrics["sort_peak_payload_bytes"] = after.PeakPayloadBytes - before.sort.PeakPayloadBytes
	metrics["sort_descriptor_bytes"] = after.DescriptorBytes - before.sort.DescriptorBytes
	metrics["sort_peak_bytes"] = after.PeakBytes - before.sort.PeakBytes
	metrics["sort_reserved_bytes"] = after.ReservedBytes - before.sort.ReservedBytes
	metrics["sort_released_bytes"] = after.ReleasedBytes - before.sort.ReleasedBytes
	metrics["sort_source_pins"] = after.SourcePins - before.sort.SourcePins
	metrics["sort_k"] = after.K - before.sort.K
	metrics["sort_live_bytes"] = 0
	aggregate := ql.AggregateExecutionStats()
	if aggregate.LiveBytes != before.aggregate.LiveBytes {
		return nil, nil, fmt.Errorf("T17 leaked aggregate state")
	}
	metrics["aggregate_executions"] = aggregate.Executions - before.aggregate.Executions
	metrics["aggregate_input_rows"] = aggregate.InputRows - before.aggregate.InputRows
	metrics["aggregate_groups"] = aggregate.Groups - before.aggregate.Groups
	metrics["aggregate_state_lanes"] = aggregate.StateLanes - before.aggregate.StateLanes
	metrics["aggregate_state_bytes"] = aggregate.StateBytes - before.aggregate.StateBytes
	metrics["aggregate_reserved_bytes"] = aggregate.ReservedBytes - before.aggregate.ReservedBytes
	metrics["aggregate_released_bytes"] = aggregate.ReleasedBytes - before.aggregate.ReleasedBytes
	metrics["aggregate_live_bytes"] = 0
	filter, having := ql.FilterExecutionStats(), ql.HavingExecutionStats()
	if filter.LiveBytes != before.filter.LiveBytes || having.LiveBytes != before.having.LiveBytes {
		return nil, nil, fmt.Errorf("T17 leaked predicate state")
	}
	qlbT14FilterMetrics(metrics, "filter", before.filter, filter)
	qlbT14FilterMetrics(metrics, "having", before.having, having)
	cache := liaisongrpc.BydbQLPlanCacheStats()
	metrics["plan_cache_hits"], metrics["plan_cache_compiles"] = cache.Hits-before.cache.Hits, cache.Compiles-before.cache.Compiles
	metrics["plan_cache_bypass"] = cache.Bypass - before.cache.Bypass
	if cache.InFlight != 0 || cache.CompileReservedBytes != 0 || cache.Waiters != 0 || cache.UncachedBytes != 0 || cache.RetainedBytes < 0 || cache.EntryBytes < 0 {
		return nil, nil, fmt.Errorf("T17 plan cache retained per-request compilation or binding state")
	}
	metrics["plan_cache_entries"], metrics["plan_cache_entry_bytes"] = uint64(cache.Entries), uint64(cache.EntryBytes)
	metrics["plan_cache_retained_bytes"] = uint64(cache.RetainedBytes)
	metrics["plan_cache_inflight"], metrics["plan_cache_compile_reserved_bytes"], metrics["plan_cache_waiters"], metrics["plan_cache_uncached_bytes"] = 0, 0, 0, 0
	metrics["execution_clock_calls"] = ql.ExecutionClockCalls() - before.clock
	if err = validateQLBT17Metrics(metrics, spec, samples, oracles); err != nil {
		return metrics, qlbT17UnavailableMetrics(), err
	}
	return metrics, qlbT17UnavailableMetrics(), nil
}

func qlbT17NativeMetrics(before qlbT02MetricsSnapshot) (map[string]uint64, error) {
	waitForQLBT02ResourceRelease(before.execution.LiveResults, before.memory.LiveReservedBytes, before.owners, before.flatten)
	after, memory := vmeasure.BatchExecutionStats(), vectorized.QueryMemoryStats()
	if after.LiveResults != before.execution.LiveResults || memory.LiveReservedBytes != before.memory.LiveReservedBytes ||
		vectorized.OutstandingOwnerPins() != before.owners || pool.AllRefsCount()["native-query-codec-flatten"] != before.flatten {
		return nil, fmt.Errorf("T17 leaked native result/reservation/owner/flatten state")
	}
	final := after.FinalDataPoints - before.execution.FinalDataPoints
	return map[string]uint64{
		"ql_batch_executions":      after.QLBatchExecutions - before.execution.QLBatchExecutions,
		"native_batch_executions":  after.NativeBatchExecutions - before.execution.NativeBatchExecutions,
		"native_leaf_calls":        after.NativeLeafCalls - before.execution.NativeLeafCalls,
		"native_source_rows":       after.NativeInputRows - before.execution.NativeInputRows,
		"native_frame_rows":        after.NativeFrameRows - before.execution.NativeFrameRows,
		"intermediate_data_points": after.IntermediateDataPoints - before.execution.IntermediateDataPoints,
		"final_data_points":        final, "output_rows": final, "process_peak_reserved_bytes": uint64(memory.PeakReservedBytes),
		"tracker_lease_charged_bytes":   memory.LeaseChargedBytes - before.memory.LeaseChargedBytes,
		"post_close_live_results_delta": 0, "post_close_reserved_bytes_delta": 0, "post_close_owner_pins_delta": 0, "post_close_flatten_pool_refs_delta": 0,
	}, nil
}
