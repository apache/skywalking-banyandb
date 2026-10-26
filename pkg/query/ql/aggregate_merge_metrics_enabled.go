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

var nativeAggregateCounters struct {
	executions    atomic.Uint64
	branches      atomic.Uint64
	inputRows     atomic.Uint64
	groups        atomic.Uint64
	groupCapacity atomic.Uint64
	keyBytes      atomic.Uint64
	keyCapacity   atomic.Uint64
	stateBytes    atomic.Uint64
	stateCapacity atomic.Uint64
	stateLanes    atomic.Uint64
	mergeVisits   atomic.Uint64
	reserved      atomic.Uint64
	released      atomic.Uint64
	live          atomic.Int64
}

// NativeAggregateExecutionStats returns process-wide native aggregate merge counters.
func NativeAggregateExecutionStats() NativeAggregateExecutionCounters {
	c := &nativeAggregateCounters
	return NativeAggregateExecutionCounters{
		Executions: c.executions.Load(), Branches: c.branches.Load(), InputRows: c.inputRows.Load(), Groups: c.groups.Load(),
		GroupCapacity: c.groupCapacity.Load(), KeyBytes: c.keyBytes.Load(), KeyCapacity: c.keyCapacity.Load(),
		StateBytes: c.stateBytes.Load(), StateCapacity: c.stateCapacity.Load(), StateLanes: c.stateLanes.Load(), MergeVisits: c.mergeVisits.Load(),
		ReservedBytes: c.reserved.Load(), ReleasedBytes: c.released.Load(), LiveBytes: c.live.Load(),
	}
}

func recordNativeAggregateExecution(s NativeAggregateStats) {
	c := &nativeAggregateCounters
	c.executions.Add(1)
	c.branches.Add(s.Branches)
	c.inputRows.Add(s.InputRows)
	c.groups.Add(s.Groups)
	c.groupCapacity.Add(s.GroupCapacity)
	c.keyBytes.Add(s.KeyBytes)
	c.keyCapacity.Add(s.KeyCapacity)
	c.stateBytes.Add(s.StateBytes)
	c.stateCapacity.Add(s.StateCapacity)
	c.stateLanes.Add(s.StateLanes)
	c.mergeVisits.Add(s.MergeVisits)
}

func recordNativeAggregateReserve(bytes int64) {
	nativeAggregateCounters.reserved.Add(uint64(bytes))
	nativeAggregateCounters.live.Add(bytes)
}

func recordNativeAggregateRelease(bytes int64) {
	nativeAggregateCounters.released.Add(uint64(bytes))
	nativeAggregateCounters.live.Add(-bytes)
}
