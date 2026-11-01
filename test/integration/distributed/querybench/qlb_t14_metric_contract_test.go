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
	"slices"

	"github.com/apache/skywalking-banyandb/pkg/query/ql"
)

func qlbT14UnavailableMetrics() []string {
	return []string{"storage_physical_scanned_rows", "native_retained_frame_bytes", "total_parent_lease_charges", "operator_peak_retained_bytes"}
}

func qlbT14FilterMetricDelta(before, after ql.FilterStats) map[string]uint64 {
	return map[string]uint64{
		"executions":      after.Executions - before.Executions,
		"batches":         after.Batches - before.Batches,
		"input_rows":      after.InputRows - before.InputRows,
		"output_rows":     after.OutputRows - before.OutputRows,
		"true_rows":       after.TrueRows - before.TrueRows,
		"false_rows":      after.FalseRows - before.FalseRows,
		"unknown_rows":    after.UnknownRows - before.UnknownRows,
		"selection_bytes": after.SelectionBytes - before.SelectionBytes,
		"reserved_bytes":  after.ReservedBytes - before.ReservedBytes,
		"released_bytes":  after.ReleasedBytes - before.ReleasedBytes,
		// Structural zero; public unit/K column identity checks prove borrowing.
		"copied_bytes": after.CopiedBytes - before.CopiedBytes,
		"live_bytes":   uint64(after.LiveBytes - before.LiveBytes),
	}
}

func qlbT14FilterMetrics(metrics map[string]uint64, prefix string, before, after ql.FilterStats) {
	for suffix, delta := range qlbT14FilterMetricDelta(before, after) {
		metrics[prefix+"_"+suffix] = delta
	}
}

func qlbT14FilterMetricKeys() []string {
	deltas := qlbT14FilterMetricDelta(ql.FilterStats{}, ql.FilterStats{})
	keys := make([]string, 0, len(deltas))
	for key := range deltas {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}
