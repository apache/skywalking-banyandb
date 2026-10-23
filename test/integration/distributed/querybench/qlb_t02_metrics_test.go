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
	"strings"
	"time"

	"github.com/apache/skywalking-banyandb/pkg/pool"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
)

type qlbT02MetricsSnapshot struct {
	execution vmeasure.BatchExecutionCounters
	memory    vectorized.MemoryCounters
	owners    int64
	flatten   int
}

func qlbT02MetricsStart() (qlbT02MetricsSnapshot, error) {
	// A client may receive the final message just before the server's transport
	// completion callback releases its response reservation. This wait is untimed.
	waitForQLBT02ResourceRelease(0, 0, 0, 0)
	flatten, tracked := pool.AllRefsCount()["native-query-codec-flatten"]
	if !tracked {
		return qlbT02MetricsSnapshot{}, fmt.Errorf("T02 requires native query flatten pool tracking")
	}
	if flatten != 0 {
		return qlbT02MetricsSnapshot{}, fmt.Errorf("T02 measurement started with %d checked-out native query flatten buffers", flatten)
	}
	if !vectorized.ResetQueryMemoryPeak() {
		return qlbT02MetricsSnapshot{}, fmt.Errorf("T02 measurement started with live reservations: %+v", vectorized.QueryMemoryStats())
	}
	return qlbT02MetricsSnapshot{
		execution: vmeasure.BatchExecutionStats(), memory: vectorized.QueryMemoryStats(), owners: vectorized.OutstandingOwnerPins(), flatten: flatten,
	}, nil
}

func qlbT02MetricsFinish(before qlbT02MetricsSnapshot, variant string, samples, rows int) (map[string]uint64, []string, error) {
	waitForQLBT02ResourceRelease(before.execution.LiveResults, before.memory.LiveReservedBytes, before.owners, before.flatten)
	after := vmeasure.BatchExecutionStats()
	memory := vectorized.QueryMemoryStats()
	owners := vectorized.OutstandingOwnerPins()
	flatten := pool.AllRefsCount()["native-query-codec-flatten"]
	if flatten != before.flatten {
		return nil, nil, fmt.Errorf("%s leaked native query flatten buffers: %d", variant, flatten-before.flatten)
	}
	if after.LiveResults != before.execution.LiveResults || memory.LiveReservedBytes != before.memory.LiveReservedBytes || owners != before.owners {
		return nil, nil, fmt.Errorf("%s leaked resources: results=%d reservations=%d owners=%d", variant,
			after.LiveResults-before.execution.LiveResults, memory.LiveReservedBytes-before.memory.LiveReservedBytes, owners-before.owners)
	}
	intermediate := after.IntermediateDataPoints - before.execution.IntermediateDataPoints
	final := after.FinalDataPoints - before.execution.FinalDataPoints
	if intermediate != 0 || final != uint64(samples)*uint64(rows) {
		return nil, nil, fmt.Errorf("%s serialization: intermediate=%d final=%d, expected final=%d", variant, intermediate, final, samples*rows)
	}
	qlCalls := after.QLBatchExecutions - before.execution.QLBatchExecutions
	nativeCalls := after.NativeBatchExecutions - before.execution.NativeBatchExecutions
	calls := nativeCalls
	if strings.HasPrefix(variant, "ql_") {
		calls = qlCalls
	}
	if calls != uint64(samples) {
		return nil, nil, fmt.Errorf("%s activated batch boundary %d times for %d requests", variant, calls, samples)
	}
	metrics := map[string]uint64{
		"native_batch_executions": nativeCalls, "ql_batch_executions": qlCalls,
		"native_leaf_calls":        after.NativeLeafCalls - before.execution.NativeLeafCalls,
		"native_source_rows":       after.NativeInputRows - before.execution.NativeInputRows,
		"native_frame_rows":        after.NativeFrameRows - before.execution.NativeFrameRows,
		"intermediate_data_points": intermediate, "final_data_points": final,
		"output_rows": uint64(samples) * uint64(rows), "process_peak_reserved_bytes": uint64(memory.PeakReservedBytes),
		"tracker_lease_charged_bytes":   memory.LeaseChargedBytes - before.memory.LeaseChargedBytes,
		"post_close_live_results_delta": 0, "post_close_reserved_bytes_delta": 0, "post_close_owner_pins_delta": 0,
		"post_close_flatten_pool_refs_delta": 0,
	}
	return metrics, []string{"storage_scanned_rows", "total_parent_lease_charges", "planning_cache"}, nil
}

func waitForQLBT02ResourceRelease(results, reservations, owners int64, flatten int) {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if vmeasure.BatchExecutionStats().LiveResults == results && vectorized.QueryMemoryStats().LiveReservedBytes == reservations &&
			vectorized.OutstandingOwnerPins() == owners && pool.AllRefsCount()["native-query-codec-flatten"] == flatten {
			return
		}
		time.Sleep(time.Millisecond)
	}
}
