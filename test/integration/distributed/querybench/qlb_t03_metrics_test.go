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

	"github.com/apache/skywalking-banyandb/pkg/pool"
	"github.com/apache/skywalking-banyandb/pkg/query/ql"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
)

type qlbT03MetricsSnapshot struct {
	native qlbT02MetricsSnapshot
	raw    ql.RawExecutionCounters
	clock  uint64
}

func qlbT03MetricsStart() (qlbT03MetricsSnapshot, error) {
	native, err := qlbT02MetricsStart()
	if err != nil {
		return qlbT03MetricsSnapshot{}, fmt.Errorf("T03 metrics: %w", err)
	}
	return qlbT03MetricsSnapshot{native: native, raw: ql.RawExecutionStats(), clock: ql.ExecutionClockCalls()}, nil
}

func qlbT03MetricsFinish(before qlbT03MetricsSnapshot, name string, samples, rows int) (map[string]uint64, []string, error) {
	baseline := before.native
	waitForQLBT02ResourceRelease(baseline.execution.LiveResults, baseline.memory.LiveReservedBytes, baseline.owners, baseline.flatten)
	after := vmeasure.BatchExecutionStats()
	memory := vectorized.QueryMemoryStats()
	raw := ql.RawExecutionStats()
	owners := vectorized.OutstandingOwnerPins()
	flatten := pool.AllRefsCount()["native-query-codec-flatten"]
	if after.LiveResults != baseline.execution.LiveResults || memory.LiveReservedBytes != baseline.memory.LiveReservedBytes ||
		owners != baseline.owners || flatten != baseline.flatten {
		return nil, nil, fmt.Errorf("%s leaked T03 resources: results=%d reservations=%d owners=%d flatten=%d", name,
			after.LiveResults-baseline.execution.LiveResults, memory.LiveReservedBytes-baseline.memory.LiveReservedBytes, owners-baseline.owners, flatten-baseline.flatten)
	}
	metrics := map[string]uint64{
		"ql_batch_executions":     after.QLBatchExecutions - baseline.execution.QLBatchExecutions,
		"native_batch_executions": after.NativeBatchExecutions - baseline.execution.NativeBatchExecutions,
		"native_leaf_calls":       after.NativeLeafCalls - baseline.execution.NativeLeafCalls,
		"native_source_rows":      after.NativeInputRows - baseline.execution.NativeInputRows, "native_frame_rows": after.NativeFrameRows - baseline.execution.NativeFrameRows,
		"intermediate_data_points": after.IntermediateDataPoints - baseline.execution.IntermediateDataPoints,
		"final_data_points":        after.FinalDataPoints - baseline.execution.FinalDataPoints, "output_rows": uint64(samples) * uint64(rows),
		"execution_clock_calls":         ql.ExecutionClockCalls() - before.clock,
		"projection_calls":              raw.ProjectionCalls - before.raw.ProjectionCalls,
		"projection_copied_value_bytes": raw.ProjectionCopiedValueBytes - before.raw.ProjectionCopiedValueBytes,
		"filter_input_rows":             raw.FilterInputRows - before.raw.FilterInputRows, "filter_output_rows": raw.FilterOutputRows - before.raw.FilterOutputRows,
		"limit_input_rows": raw.LimitInputRows - before.raw.LimitInputRows, "limit_output_rows": raw.LimitOutputRows - before.raw.LimitOutputRows,
		"process_peak_reserved_bytes": uint64(memory.PeakReservedBytes), "tracker_lease_charged_bytes": memory.LeaseChargedBytes - baseline.memory.LeaseChargedBytes,
		"post_close_live_results_delta": 0, "post_close_reserved_bytes_delta": 0, "post_close_owner_pins_delta": 0, "post_close_flatten_pool_refs_delta": 0,
	}
	if err := validateQLBT03Metrics(metrics, name, samples, rows); err != nil {
		return nil, nil, err
	}
	return metrics, []string{"storage_physical_scanned_rows", "total_parent_lease_charges", "planning_cache"}, nil
}
