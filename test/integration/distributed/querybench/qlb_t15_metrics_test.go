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

type qlbT15MetricsSnapshot struct {
	native    qlbT02MetricsSnapshot
	aggregate ql.AggregateExecutionCounters
	distinct  ql.DistinctExecutionCounters
	clock     uint64
	cache     liaisongrpc.PlanCacheStats
}

func qlbT15MetricsStart() (qlbT15MetricsSnapshot, error) {
	native, err := qlbT02MetricsStart()
	if err != nil {
		return qlbT15MetricsSnapshot{}, err
	}
	aggregate := ql.AggregateExecutionStats()
	distinct := ql.DistinctExecutionStats()
	if distinct.LiveBytes != 0 {
		return qlbT15MetricsSnapshot{}, fmt.Errorf("T15 started with live distinct state")
	}
	if aggregate.LiveBytes != 0 {
		return qlbT15MetricsSnapshot{}, fmt.Errorf("T15 aggregate started with %d live bytes", aggregate.LiveBytes)
	}
	return qlbT15MetricsSnapshot{native: native, aggregate: aggregate, distinct: distinct, clock: ql.ExecutionClockCalls(), cache: liaisongrpc.BydbQLPlanCacheStats()}, nil
}

func qlbT15MetricsFinish(before qlbT15MetricsSnapshot, samples int, oracle *qlbT15Oracle) (map[string]uint64, []string, error) {
	metrics, _, err := qlbT02MetricsFinish(before.native, "ql_"+oracle.spec.name, samples, oracle.groups)
	if err != nil {
		return nil, nil, err
	}
	aggregateAfter, distinctAfter := ql.AggregateExecutionStats(), ql.DistinctExecutionStats()
	if aggregateAfter.LiveBytes != before.aggregate.LiveBytes || distinctAfter.LiveBytes != before.distinct.LiveBytes {
		return nil, nil, fmt.Errorf("T15 leaked group table bytes")
	}
	metrics["execution_clock_calls"] = ql.ExecutionClockCalls() - before.clock
	metrics["aggregate_executions"] = aggregateAfter.Executions - before.aggregate.Executions
	metrics["distinct_executions"] = distinctAfter.Executions - before.distinct.Executions
	// Equal public counter layouts describe the same reused group-table algorithm.
	after, baseline := aggregateAfter, before.aggregate
	if !oracle.spec.reference {
		after = ql.AggregateExecutionCounters(distinctAfter)
		baseline = ql.AggregateExecutionCounters(before.distinct)
	}
	metrics["group_executions"] = after.Executions - baseline.Executions
	metrics["group_input_rows"] = after.InputRows - baseline.InputRows
	metrics["group_count"] = after.Groups - baseline.Groups
	metrics["group_capacity"] = after.GroupCapacity - baseline.GroupCapacity
	metrics["key_bytes"] = after.KeyBytes - baseline.KeyBytes
	metrics["key_capacity"] = after.KeyCapacity - baseline.KeyCapacity
	metrics["state_bytes"] = after.StateBytes - baseline.StateBytes
	metrics["state_capacity"] = after.StateCapacity - baseline.StateCapacity
	metrics["state_lanes"] = after.StateLanes - baseline.StateLanes
	metrics["group_reserved_bytes"] = after.ReservedBytes - baseline.ReservedBytes
	metrics["group_released_bytes"] = after.ReleasedBytes - baseline.ReleasedBytes
	metrics["group_live_bytes"] = uint64(after.LiveBytes - baseline.LiveBytes)
	cache := liaisongrpc.BydbQLPlanCacheStats()
	metrics["plan_cache_hits"], metrics["plan_cache_compiles"] = cache.Hits-before.cache.Hits, cache.Compiles-before.cache.Compiles
	if err = validateQLBT15Metrics(metrics, samples, oracle); err != nil {
		return nil, nil, err
	}
	return metrics, qlbT15UnavailableMetrics(), nil
}
