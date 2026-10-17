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

package ql

import "github.com/apache/skywalking-banyandb/pkg/query/vectorized"

// FilterStats counts examined active rows, their SQL truth values, and filter
// memory. CopiedBytes is structurally zero because filter output borrows the
// input column objects. Unit tests and kernel benchmark identity checks verify
// that invariant; it is not independently measured copy telemetry. It excludes
// charged typed decoding scratch at the protobuf input boundary.
// SelectionBytes counts allocated selection capacity. Reservations include
// compiled expression state and scratch. Process-wide counters are disabled in
// production builds.
type FilterStats struct {
	Executions     uint64
	Batches        uint64
	InputRows      uint64
	OutputRows     uint64
	TrueRows       uint64
	FalseRows      uint64
	UnknownRows    uint64
	CopiedBytes    uint64
	SelectionBytes uint64
	ReservedBytes  uint64
	ReleasedBytes  uint64
	LiveBytes      int64
}

// FilterStatistics returns filter work and memory statistics for operator.
func FilterStatistics(operator vectorized.PullOperator) (FilterStats, bool) {
	filter, ok := operator.(*filterOperator)
	if !ok {
		return FilterStats{}, false
	}
	stats := filter.stats
	stats.ReservedBytes = filter.kernel.stats.ReservedBytes
	stats.ReleasedBytes = filter.kernel.stats.ReleasedBytes
	stats.LiveBytes = filter.kernel.stats.LiveBytes
	return stats, true
}

func (o *filterOperator) record(delta FilterStats) {
	o.stats.Executions += delta.Executions
	o.stats.Batches += delta.Batches
	o.stats.InputRows += delta.InputRows
	o.stats.OutputRows += delta.OutputRows
	o.stats.TrueRows += delta.TrueRows
	o.stats.FalseRows += delta.FalseRows
	o.stats.UnknownRows += delta.UnknownRows
	recordFilterWork(delta)
}
