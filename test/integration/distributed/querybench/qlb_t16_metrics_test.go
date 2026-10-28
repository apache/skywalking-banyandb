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

type qlbT16MetricsSnapshot struct {
	native     qlbT02MetricsSnapshot
	aggregate  ql.AggregateExecutionCounters
	expression ql.ExpressionStats
	clock      uint64
	cache      liaisongrpc.PlanCacheStats
}

func qlbT16MetricsStart() (qlbT16MetricsSnapshot, error) {
	native, err := qlbT02MetricsStart()
	if err != nil {
		return qlbT16MetricsSnapshot{}, err
	}
	aggregate := ql.AggregateExecutionStats()
	if aggregate.LiveBytes != 0 {
		return qlbT16MetricsSnapshot{}, fmt.Errorf("T16 aggregate started with %d live bytes", aggregate.LiveBytes)
	}
	expression := ql.GroupExpressionExecutionStats()
	if expression.LiveBytes != 0 {
		return qlbT16MetricsSnapshot{}, fmt.Errorf("T16 started with live group expression state")
	}
	return qlbT16MetricsSnapshot{
		native: native, aggregate: aggregate, expression: expression, clock: ql.ExecutionClockCalls(), cache: liaisongrpc.BydbQLPlanCacheStats(),
	}, nil
}

func qlbT16MetricsFinish(before qlbT16MetricsSnapshot, samples int, oracle *qlbT16Oracle) (map[string]uint64, []string, error) {
	metrics, _, err := qlbT02MetricsFinish(before.native, "ql_"+oracle.spec.name, samples, oracle.groups)
	if err != nil {
		return nil, nil, err
	}
	after := ql.AggregateExecutionStats()
	if after.LiveBytes < 0 || after.LiveBytes != before.aggregate.LiveBytes {
		return nil, nil, fmt.Errorf("T16 aggregate leaked %d live bytes", after.LiveBytes-before.aggregate.LiveBytes)
	}
	metrics["execution_clock_calls"] = ql.ExecutionClockCalls() - before.clock
	metrics["aggregate_executions"] = after.Executions - before.aggregate.Executions
	metrics["aggregate_input_rows"] = after.InputRows - before.aggregate.InputRows
	metrics["aggregate_groups"] = after.Groups - before.aggregate.Groups
	metrics["aggregate_group_capacity"] = after.GroupCapacity - before.aggregate.GroupCapacity
	metrics["aggregate_key_bytes"] = after.KeyBytes - before.aggregate.KeyBytes
	metrics["aggregate_key_capacity"] = after.KeyCapacity - before.aggregate.KeyCapacity
	metrics["aggregate_state_bytes"] = after.StateBytes - before.aggregate.StateBytes
	metrics["aggregate_state_capacity"] = after.StateCapacity - before.aggregate.StateCapacity
	metrics["aggregate_state_lanes"] = after.StateLanes - before.aggregate.StateLanes
	metrics["aggregate_reserved_bytes"] = after.ReservedBytes - before.aggregate.ReservedBytes
	metrics["aggregate_released_bytes"] = after.ReleasedBytes - before.aggregate.ReleasedBytes
	metrics["aggregate_live_bytes"] = uint64(after.LiveBytes - before.aggregate.LiveBytes)
	cache := liaisongrpc.BydbQLPlanCacheStats()
	metrics["plan_cache_hits"], metrics["plan_cache_compiles"] = cache.Hits-before.cache.Hits, cache.Compiles-before.cache.Compiles
	qlbT16ExpressionDelta(metrics, before.expression, ql.GroupExpressionExecutionStats())
	if err = validateQLBT16Metrics(metrics, samples, oracle); err != nil {
		return nil, nil, err
	}
	return metrics, qlbT16UnavailableMetrics(), nil
}

func qlbT16ExpressionDelta(m map[string]uint64, before, after ql.ExpressionStats) {
	m["group_expression_executions"] = after.Executions - before.Executions
	m["group_expression_batches"] = after.Batches - before.Batches
	m["group_expression_input_rows"] = after.InputRows - before.InputRows
	m["group_expression_output_rows"] = after.OutputRows - before.OutputRows
	m["group_key_evaluations"] = after.KeyEvaluations - before.KeyEvaluations
	m["group_key_root_calls"] = after.KeyRootCalls - before.KeyRootCalls
	m["group_expression_vector_rows"] = after.VectorRows - before.VectorRows
	m["group_expression_scalar_evaluations"] = after.ScalarEvaluations - before.ScalarEvaluations
	m["group_expression_arm_rows"] = after.ArmRows - before.ArmRows
	m["group_expression_later_arm_rows"] = after.LaterArmRows - before.LaterArmRows
	m["group_expression_buffer_bytes"] = after.BufferBytes - before.BufferBytes
	m["group_expression_full_column_allocations"] = after.FullColumnAllocations - before.FullColumnAllocations
	m["group_expression_reserved_bytes"] = after.ReservedBytes - before.ReservedBytes
	m["group_expression_released_bytes"] = after.ReleasedBytes - before.ReleasedBytes
	m["group_expression_live_bytes"] = uint64(after.LiveBytes - before.LiveBytes)
}
