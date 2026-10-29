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

type filterMetricCounters struct {
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

var filterCounters, havingCounters filterMetricCounters

// FilterExecutionStats returns process-wide raw WHERE filter work counters.
func FilterExecutionStats() FilterStats { return filterCounters.snapshot() }

// HavingExecutionStats returns process-wide completed-group HAVING counters.
func HavingExecutionStats() FilterStats { return havingCounters.snapshot() }

func (c *filterMetricCounters) snapshot() FilterStats {
	return FilterStats{
		Executions:     c.Executions.Load(),
		Batches:        c.Batches.Load(),
		InputRows:      c.InputRows.Load(),
		OutputRows:     c.OutputRows.Load(),
		TrueRows:       c.TrueRows.Load(),
		FalseRows:      c.FalseRows.Load(),
		UnknownRows:    c.UnknownRows.Load(),
		CopiedBytes:    c.CopiedBytes.Load(),
		SelectionBytes: c.SelectionBytes.Load(),
		ReservedBytes:  c.ReservedBytes.Load(),
		ReleasedBytes:  c.ReleasedBytes.Load(),
		LiveBytes:      c.LiveBytes.Load(),
	}
}

func recordFilterWork(stats FilterStats) { filterCounters.record(stats) }
func recordHavingWork(stats FilterStats) { havingCounters.record(stats) }
func recordFilterReserve(bytes int64)    { filterCounters.reserve(bytes) }
func recordHavingReserve(bytes int64)    { havingCounters.reserve(bytes) }
func recordFilterRelease(bytes int64)    { filterCounters.release(bytes) }
func recordHavingRelease(bytes int64)    { havingCounters.release(bytes) }

func (c *filterMetricCounters) record(stats FilterStats) {
	c.Executions.Add(stats.Executions)
	c.Batches.Add(stats.Batches)
	c.InputRows.Add(stats.InputRows)
	c.OutputRows.Add(stats.OutputRows)
	c.TrueRows.Add(stats.TrueRows)
	c.FalseRows.Add(stats.FalseRows)
	c.UnknownRows.Add(stats.UnknownRows)
	c.CopiedBytes.Add(stats.CopiedBytes)
	c.SelectionBytes.Add(stats.SelectionBytes)
}

func (c *filterMetricCounters) reserve(bytes int64) {
	c.ReservedBytes.Add(uint64(bytes))
	c.LiveBytes.Add(bytes)
}

func (c *filterMetricCounters) release(bytes int64) {
	c.ReleasedBytes.Add(uint64(bytes))
	c.LiveBytes.Add(-bytes)
}
