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

type expressionStage uint8

const (
	projectionStage expressionStage = iota
	whereStage
	havingStage
	groupStage
)

// operatorMetrics selects process-wide attribution once when constructing an
// expression kernel. Predicate kernels retain local expression statistics but
// report only their filter stage's process-wide work and reservations.
type operatorMetrics struct {
	expressionWork func(ExpressionStats)
	filterWork     func(FilterStats)
	reserve        func(int64)
	release        func(int64)
	stage          expressionStage
}

var (
	projectionMetrics = operatorMetrics{
		stage: projectionStage, expressionWork: recordExpressionWork,
		reserve: recordExpressionReserve, release: recordExpressionRelease,
	}
	whereMetrics = operatorMetrics{
		stage: whereStage, filterWork: recordFilterWork,
		reserve: recordFilterReserve, release: recordFilterRelease,
	}
	havingMetrics = operatorMetrics{
		stage: havingStage, filterWork: recordHavingWork,
		reserve: recordHavingReserve, release: recordHavingRelease,
	}
	groupExpressionMetrics = operatorMetrics{
		stage: groupStage, expressionWork: recordGroupExpressionWork,
		reserve: recordGroupExpressionReserve, release: recordGroupExpressionRelease,
	}
)

// ExpressionStats counts typed expression work and allocations. InputRows and
// OutputRows count active physical rows; VectorRows counts eligible rows per
// nonconstant kernel. ArmRows counts selected CASE values and eligible COALESCE
// arguments; LaterArmRows excludes each conditional's first value/argument.
// ScalarEvaluations counts non-leaf constant kernels evaluated once per operator. BufferBytes
// and FullColumnAllocations count allocated vector capacity, not variable payloads.
// Production process-wide counters are disabled; per-operator statistics remain available.
type ExpressionStats struct {
	Executions            uint64
	Batches               uint64
	InputRows             uint64
	KeyEvaluations        uint64 // Active rows requested by actual computed-key root invocations.
	KeyRootCalls          uint64 // Actual key-root calls, including cached scalar applications.
	OutputRows            uint64
	VectorRows            uint64
	ScalarEvaluations     uint64
	ArmRows               uint64
	LaterArmRows          uint64
	BufferBytes           uint64
	FullColumnAllocations uint64
	ReservedBytes         uint64
	ReleasedBytes         uint64
	LiveBytes             int64
}

// ExpressionStatistics returns expression work and memory statistics for operator.
func ExpressionStatistics(operator vectorized.PullOperator) (ExpressionStats, bool) {
	expression, ok := operator.(*expressionOperator)
	if !ok {
		return ExpressionStats{}, false
	}
	return expression.stats, true
}

func (n *expressionNode) recordArm(rows int, later bool) {
	count := uint64(rows)
	delta := ExpressionStats{ArmRows: count}
	n.op.stats.ArmRows += count
	if later {
		delta.LaterArmRows = count
		n.op.stats.LaterArmRows += count
	}
	n.op.recordWork(delta)
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
