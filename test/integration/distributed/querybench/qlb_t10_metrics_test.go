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

type qlbT10MetricsSnapshot struct {
	native     qlbT02MetricsSnapshot
	expression ql.ExpressionStats
	aggregate  ql.AggregateExecutionCounters
	merge      ql.NativeAggregateExecutionCounters
	cache      liaisongrpc.PlanCacheStats
	clock      uint64
}

func qlbT10MetricsStart() (qlbT10MetricsSnapshot, error) {
	native, err := qlbT02MetricsStart()
	if err != nil {
		return qlbT10MetricsSnapshot{}, err
	}
	expression, aggregate, merge := ql.ExpressionExecutionStats(), ql.AggregateExecutionStats(), ql.NativeAggregateExecutionStats()
	if expression.LiveBytes != 0 || aggregate.LiveBytes != 0 || merge.LiveBytes != 0 {
		return qlbT10MetricsSnapshot{}, fmt.Errorf("T10 starts with live expression or aggregate reservations")
	}
	return qlbT10MetricsSnapshot{
		native: native, expression: expression, aggregate: aggregate, merge: merge,
		cache: liaisongrpc.BydbQLPlanCacheStats(), clock: ql.ExecutionClockCalls(),
	}, nil
}

func qlbT10MetricsFinish(before qlbT10MetricsSnapshot, name string, samples, inputRows int) (map[string]uint64, []string, error) {
	metrics, _, err := qlbT02MetricsFinish(before.native, "ql_"+name, samples, inputRows)
	if err != nil {
		return nil, nil, err
	}
	expression, aggregate, merge := ql.ExpressionExecutionStats(), ql.AggregateExecutionStats(), ql.NativeAggregateExecutionStats()
	if expression.LiveBytes != before.expression.LiveBytes || aggregate.LiveBytes != before.aggregate.LiveBytes || merge.LiveBytes != before.merge.LiveBytes {
		return nil, nil, fmt.Errorf("T10 %s leaked expression or aggregate state", name)
	}
	metrics["execution_clock_calls"] = ql.ExecutionClockCalls() - before.clock
	metrics["expression_executions"] = expression.Executions - before.expression.Executions
	metrics["expression_batches"] = expression.Batches - before.expression.Batches
	metrics["expression_input_rows"] = expression.InputRows - before.expression.InputRows
	metrics["expression_output_rows"] = expression.OutputRows - before.expression.OutputRows
	metrics["expression_vector_rows"] = expression.VectorRows - before.expression.VectorRows
	metrics["expression_scalar_evaluations"] = expression.ScalarEvaluations - before.expression.ScalarEvaluations
	metrics["expression_arm_rows"] = expression.ArmRows - before.expression.ArmRows
	metrics["expression_later_arm_rows"] = expression.LaterArmRows - before.expression.LaterArmRows
	metrics["expression_buffer_bytes"] = expression.BufferBytes - before.expression.BufferBytes
	metrics["expression_full_column_allocations"] = expression.FullColumnAllocations - before.expression.FullColumnAllocations
	metrics["expression_reserved_bytes"] = expression.ReservedBytes - before.expression.ReservedBytes
	metrics["expression_released_bytes"] = expression.ReleasedBytes - before.expression.ReleasedBytes
	metrics["expression_live_bytes"] = 0
	metrics["aggregate_executions"] = aggregate.Executions - before.aggregate.Executions
	metrics["aggregate_input_rows"] = aggregate.InputRows - before.aggregate.InputRows
	metrics["aggregate_reserved_bytes"] = aggregate.ReservedBytes - before.aggregate.ReservedBytes
	metrics["aggregate_live_bytes"] = 0
	metrics["merge_executions"] = merge.Executions - before.merge.Executions
	metrics["merge_branches"] = merge.Branches - before.merge.Branches
	metrics["merge_reserved_bytes"] = merge.ReservedBytes - before.merge.ReservedBytes
	metrics["merge_live_bytes"] = 0
	cache := liaisongrpc.BydbQLPlanCacheStats()
	metrics["plan_cache_hits"], metrics["plan_cache_compiles"] = cache.Hits-before.cache.Hits, cache.Compiles-before.cache.Compiles
	if err = validateQLBT10Metrics(metrics, name, samples, inputRows); err != nil {
		return nil, nil, err
	}
	return metrics, []string{"storage_physical_scanned_rows", "native_retained_frame_bytes", "total_parent_lease_charges"}, nil
}
