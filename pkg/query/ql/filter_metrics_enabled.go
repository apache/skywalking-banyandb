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
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package ql

import "sync/atomic"

var filterCounters struct {
	Executions     atomic.Uint64
	Batches        atomic.Uint64
	InputRows      atomic.Uint64
	OutputRows     atomic.Uint64
	TrueRows       atomic.Uint64
	FalseRows      atomic.Uint64
	UnknownRows    atomic.Uint64
	CopiedBytes    atomic.Uint64
	SelectionBytes atomic.Uint64
	ReservedBytes  atomic.Uint64
	ReleasedBytes  atomic.Uint64
	LiveBytes      atomic.Int64
}

// FilterExecutionStats returns process-wide filter work counters.
func FilterExecutionStats() FilterStats {
	return FilterStats{
		Executions:     filterCounters.Executions.Load(),
		Batches:        filterCounters.Batches.Load(),
		InputRows:      filterCounters.InputRows.Load(),
		OutputRows:     filterCounters.OutputRows.Load(),
		TrueRows:       filterCounters.TrueRows.Load(),
		FalseRows:      filterCounters.FalseRows.Load(),
		UnknownRows:    filterCounters.UnknownRows.Load(),
		CopiedBytes:    filterCounters.CopiedBytes.Load(),
		SelectionBytes: filterCounters.SelectionBytes.Load(),
		ReservedBytes:  filterCounters.ReservedBytes.Load(),
		ReleasedBytes:  filterCounters.ReleasedBytes.Load(),
		LiveBytes:      filterCounters.LiveBytes.Load(),
	}
}

func recordFilterWork(stats FilterStats) {
	filterCounters.Executions.Add(stats.Executions)
	filterCounters.Batches.Add(stats.Batches)
	filterCounters.InputRows.Add(stats.InputRows)
	filterCounters.OutputRows.Add(stats.OutputRows)
	filterCounters.TrueRows.Add(stats.TrueRows)
	filterCounters.FalseRows.Add(stats.FalseRows)
	filterCounters.UnknownRows.Add(stats.UnknownRows)
	filterCounters.CopiedBytes.Add(stats.CopiedBytes)
	filterCounters.SelectionBytes.Add(stats.SelectionBytes)
}

func recordFilterReserve(bytes int64) {
	filterCounters.ReservedBytes.Add(uint64(bytes))
	filterCounters.LiveBytes.Add(bytes)
}

func recordFilterRelease(bytes int64) {
	filterCounters.ReleasedBytes.Add(uint64(bytes))
	filterCounters.LiveBytes.Add(-bytes)
}
