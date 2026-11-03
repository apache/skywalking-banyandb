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
	"github.com/apache/skywalking-banyandb/pkg/query/ql"
)

type qlbT12MetricsSnapshot struct {
	native    qlbT02MetricsSnapshot
	filter    ql.FilterStats
	aggregate ql.AggregateExecutionCounters
	merge     ql.NativeAggregateExecutionCounters
	cache     liaisongrpc.PlanCacheStats
	clock     uint64
}

func qlbT12MetricsStart() (qlbT12MetricsSnapshot, error) {
	native, err := qlbT02MetricsStart()
	if err != nil {
		return qlbT12MetricsSnapshot{}, err
	}
	filter, aggregate, merge := ql.FilterExecutionStats(), ql.AggregateExecutionStats(), ql.NativeAggregateExecutionStats()
	if filter.LiveBytes != 0 || aggregate.LiveBytes != 0 || merge.LiveBytes != 0 {
		return qlbT12MetricsSnapshot{}, fmt.Errorf("T12 starts with live operator memory")
	}
	return qlbT12MetricsSnapshot{
		native: native, filter: filter, aggregate: aggregate, merge: merge,
		cache: liaisongrpc.BydbQLPlanCacheStats(), clock: ql.ExecutionClockCalls(),
	}, nil
}

func qlbT12MetricsFinish(before qlbT12MetricsSnapshot, name string, samples int, oracle *qlbT12Oracle) (map[string]uint64, []string, error) {
	metrics, _, err := qlbT02MetricsFinish(before.native, "ql_"+name, samples, oracle.outputRows())
	if err != nil {
		return nil, nil, err
	}
	filter, aggregate, merge := ql.FilterExecutionStats(), ql.AggregateExecutionStats(), ql.NativeAggregateExecutionStats()
	if filter.LiveBytes != before.filter.LiveBytes || aggregate.LiveBytes != before.aggregate.LiveBytes || merge.LiveBytes != before.merge.LiveBytes {
		return nil, nil, fmt.Errorf("T12 operator reservation leaked")
	}
	metrics["filter_executions"] = filter.Executions - before.filter.Executions
	metrics["filter_batches"] = filter.Batches - before.filter.Batches
	metrics["filter_input_rows"] = filter.InputRows - before.filter.InputRows
	metrics["filter_output_rows"] = filter.OutputRows - before.filter.OutputRows
	metrics["filter_true_rows"] = filter.TrueRows - before.filter.TrueRows
	metrics["filter_false_rows"] = filter.FalseRows - before.filter.FalseRows
	metrics["filter_unknown_rows"] = filter.UnknownRows - before.filter.UnknownRows
	// This is a structural invariant. Unit tests and the K benchmark check column
	// identity; the RPC counter alone does not measure or prove the absence of copies.
	metrics["filter_copied_bytes"] = filter.CopiedBytes - before.filter.CopiedBytes
	metrics["filter_selection_bytes"] = filter.SelectionBytes - before.filter.SelectionBytes
	metrics["filter_reserved_bytes"] = filter.ReservedBytes - before.filter.ReservedBytes
	metrics["filter_released_bytes"] = filter.ReleasedBytes - before.filter.ReleasedBytes
	metrics["filter_live_bytes"] = 0
	metrics["aggregate_executions"] = aggregate.Executions - before.aggregate.Executions
	metrics["aggregate_input_rows"] = aggregate.InputRows - before.aggregate.InputRows
	metrics["aggregate_groups"] = aggregate.Groups - before.aggregate.Groups
	metrics["aggregate_reserved_bytes"] = aggregate.ReservedBytes - before.aggregate.ReservedBytes
	metrics["aggregate_released_bytes"] = aggregate.ReleasedBytes - before.aggregate.ReleasedBytes
	metrics["aggregate_live_bytes"] = 0
	metrics["merge_executions"] = merge.Executions - before.merge.Executions
	metrics["merge_branches"] = merge.Branches - before.merge.Branches
	metrics["merge_live_bytes"] = 0
	cache := liaisongrpc.BydbQLPlanCacheStats()
	metrics["plan_cache_hits"], metrics["plan_cache_compiles"] = cache.Hits-before.cache.Hits, cache.Compiles-before.cache.Compiles
	metrics["execution_clock_calls"] = ql.ExecutionClockCalls() - before.clock
	if err = validateQLBT12Metrics(metrics, name, samples, oracle); err != nil {
		return nil, nil, err
	}
	return metrics, []string{"storage_physical_scanned_rows", "native_retained_frame_bytes", "total_parent_lease_charges"}, nil
}
