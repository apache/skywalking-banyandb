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

var expressionCounters struct {
	Executions            atomic.Uint64
	Batches               atomic.Uint64
	InputRows             atomic.Uint64
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

// ExpressionExecutionStats returns process-wide expression work counters.
func ExpressionExecutionStats() ExpressionStats {
	return ExpressionStats{
		Executions:            expressionCounters.Executions.Load(),
		Batches:               expressionCounters.Batches.Load(),
		InputRows:             expressionCounters.InputRows.Load(),
		OutputRows:            expressionCounters.OutputRows.Load(),
		VectorRows:            expressionCounters.VectorRows.Load(),
		ScalarEvaluations:     expressionCounters.ScalarEvaluations.Load(),
		ArmRows:               expressionCounters.ArmRows.Load(),
		LaterArmRows:          expressionCounters.LaterArmRows.Load(),
		BufferBytes:           expressionCounters.BufferBytes.Load(),
		FullColumnAllocations: expressionCounters.FullColumnAllocations.Load(),
		ReservedBytes:         expressionCounters.ReservedBytes.Load(),
		ReleasedBytes:         expressionCounters.ReleasedBytes.Load(),
		LiveBytes:             expressionCounters.LiveBytes.Load(),
	}
}

func recordExpressionWork(stats ExpressionStats) {
	expressionCounters.Executions.Add(stats.Executions)
	expressionCounters.Batches.Add(stats.Batches)
	expressionCounters.InputRows.Add(stats.InputRows)
	expressionCounters.OutputRows.Add(stats.OutputRows)
	expressionCounters.VectorRows.Add(stats.VectorRows)
	expressionCounters.ScalarEvaluations.Add(stats.ScalarEvaluations)
	expressionCounters.ArmRows.Add(stats.ArmRows)
	expressionCounters.LaterArmRows.Add(stats.LaterArmRows)
	expressionCounters.BufferBytes.Add(stats.BufferBytes)
	expressionCounters.FullColumnAllocations.Add(stats.FullColumnAllocations)
}

func recordExpressionReserve(bytes int64) {
	expressionCounters.ReservedBytes.Add(uint64(bytes))
	expressionCounters.LiveBytes.Add(bytes)
}

func recordExpressionRelease(bytes int64) {
	expressionCounters.ReleasedBytes.Add(uint64(bytes))
	expressionCounters.LiveBytes.Add(-bytes)
}
