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

var groupExpressionCounters struct {
	Executions            atomic.Uint64
	Batches               atomic.Uint64
	InputRows             atomic.Uint64
	KeyEvaluations        atomic.Uint64
	KeyRootCalls          atomic.Uint64
	OutputRows            atomic.Uint64
	VectorRows            atomic.Uint64
	ScalarEvaluations     atomic.Uint64
	ArmRows               atomic.Uint64
	LaterArmRows          atomic.Uint64
	BufferBytes           atomic.Uint64
	FullColumnAllocations atomic.Uint64
	ReservedBytes         atomic.Uint64
	ReleasedBytes         atomic.Uint64
	LiveBytes             atomic.Int64
}

// GroupExpressionExecutionStats returns process-wide computed grouping work counters.
func GroupExpressionExecutionStats() ExpressionStats {
	return ExpressionStats{
		Executions:            groupExpressionCounters.Executions.Load(),
		Batches:               groupExpressionCounters.Batches.Load(),
		InputRows:             groupExpressionCounters.InputRows.Load(),
		KeyEvaluations:        groupExpressionCounters.KeyEvaluations.Load(),
		KeyRootCalls:          groupExpressionCounters.KeyRootCalls.Load(),
		OutputRows:            groupExpressionCounters.OutputRows.Load(),
		VectorRows:            groupExpressionCounters.VectorRows.Load(),
		ScalarEvaluations:     groupExpressionCounters.ScalarEvaluations.Load(),
		ArmRows:               groupExpressionCounters.ArmRows.Load(),
		LaterArmRows:          groupExpressionCounters.LaterArmRows.Load(),
		BufferBytes:           groupExpressionCounters.BufferBytes.Load(),
		FullColumnAllocations: groupExpressionCounters.FullColumnAllocations.Load(),
		ReservedBytes:         groupExpressionCounters.ReservedBytes.Load(),
		ReleasedBytes:         groupExpressionCounters.ReleasedBytes.Load(),
		LiveBytes:             groupExpressionCounters.LiveBytes.Load(),
	}
}

func recordGroupExpressionWork(stats ExpressionStats) {
	groupExpressionCounters.Executions.Add(stats.Executions)
	groupExpressionCounters.Batches.Add(stats.Batches)
	groupExpressionCounters.InputRows.Add(stats.InputRows)
	groupExpressionCounters.KeyEvaluations.Add(stats.KeyEvaluations)
	groupExpressionCounters.KeyRootCalls.Add(stats.KeyRootCalls)
	groupExpressionCounters.OutputRows.Add(stats.OutputRows)
	groupExpressionCounters.VectorRows.Add(stats.VectorRows)
	groupExpressionCounters.ScalarEvaluations.Add(stats.ScalarEvaluations)
	groupExpressionCounters.ArmRows.Add(stats.ArmRows)
	groupExpressionCounters.LaterArmRows.Add(stats.LaterArmRows)
	groupExpressionCounters.BufferBytes.Add(stats.BufferBytes)
	groupExpressionCounters.FullColumnAllocations.Add(stats.FullColumnAllocations)
}

func recordGroupExpressionReserve(bytes int64) {
	groupExpressionCounters.ReservedBytes.Add(uint64(bytes))
	groupExpressionCounters.LiveBytes.Add(bytes)
}

func recordGroupExpressionRelease(bytes int64) {
	groupExpressionCounters.ReleasedBytes.Add(uint64(bytes))
	groupExpressionCounters.LiveBytes.Add(-bytes)
}
