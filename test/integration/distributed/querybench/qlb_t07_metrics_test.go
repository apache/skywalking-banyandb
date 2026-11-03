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

type qlbT07MetricsSnapshot struct {
	native    qlbT02MetricsSnapshot
	aggregate ql.AggregateExecutionCounters
	clock     uint64
	cache     liaisongrpc.PlanCacheStats
}

func qlbT07MetricsStart() (qlbT07MetricsSnapshot, error) {
	native, err := qlbT02MetricsStart()
	if err != nil {
		return qlbT07MetricsSnapshot{}, err
	}
	aggregate := ql.AggregateExecutionStats()
	if aggregate.LiveBytes != 0 {
		return qlbT07MetricsSnapshot{}, fmt.Errorf("T07 aggregate started with %d live bytes", aggregate.LiveBytes)
	}
	return qlbT07MetricsSnapshot{native: native, aggregate: aggregate, clock: ql.ExecutionClockCalls(), cache: liaisongrpc.BydbQLPlanCacheStats()}, nil
}

func qlbT07MetricsFinish(before qlbT07MetricsSnapshot, name string, samples, inputRows, outputRows int) (map[string]uint64, []string, error) {
	metrics, _, err := qlbT02MetricsFinish(before.native, name, samples, outputRows)
	if err != nil {
		return nil, nil, err
	}
	after := ql.AggregateExecutionStats()
	if after.LiveBytes < 0 || after.LiveBytes != before.aggregate.LiveBytes {
		return nil, nil, fmt.Errorf("T07 aggregate leaked %d live bytes", after.LiveBytes-before.aggregate.LiveBytes)
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
	if err = validateQLBT07Metrics(metrics, name, samples, inputRows, outputRows); err != nil {
		return nil, nil, err
	}
	return metrics, []string{"storage_physical_scanned_rows", "native_retained_frame_bytes", "total_parent_lease_charges"}, nil
}
