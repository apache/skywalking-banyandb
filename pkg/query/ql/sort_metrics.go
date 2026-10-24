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

// SortStats reports actual ordering work, candidate capacity and memory ownership.
// KeyKernelRows counts eligible rows in nonconstant non-leaf expression kernels.
// PayloadBytes excludes descriptors; PayloadCapacity reserves twice its size for
// allocation slack. SourcePins counts retained source owners and stays zero for
// copied candidates. PeakBytes includes key kernels and the output buffer.
type SortStats struct {
	Executions           uint64
	Batches              uint64
	InputRows            uint64
	OutputRows           uint64
	KeyEvaluations       uint64
	KeyRootCalls         uint64
	KeyKernelRows        uint64
	KeyScalarEvaluations uint64
	Comparisons          uint64
	AcceptedRows         uint64
	EvictedRows          uint64
	Candidates           uint64
	PeakCandidates       uint64
	CandidateCapacity    uint64
	PayloadBytes         uint64
	PayloadCapacity      uint64
	PeakPayloadBytes     uint64
	DescriptorBytes      uint64
	PeakBytes            uint64
	ReservedBytes        uint64
	ReleasedBytes        uint64
	SourcePins           uint64
	K                    uint64
	LiveBytes            int64
	FullSort             bool
}

// SortExecutionCounters contains cumulative process work and completion snapshots.
// Capacity and peak fields sum successful pre-pagination completion snapshots;
// divide deltas by Executions to describe each execution. LiveBytes is a gauge.
// Production process-wide counters are disabled.
type SortExecutionCounters = SortStats

// SortStatistics returns ordering statistics through the public operator chain.
func SortStatistics(operator vectorized.PullOperator) (SortStats, bool) {
	for current := operator; current != nil; current = operatorSource(current) {
		if sorted, ok := current.(*sortOperator); ok {
			return sorted.stats, true
		}
	}
	return SortStats{}, false
}

func (o *sortOperator) recordKeyWork(stats ExpressionStats) {
	o.stats.KeyEvaluations += stats.KeyEvaluations
	o.stats.KeyRootCalls += stats.KeyRootCalls
	o.stats.KeyKernelRows += stats.VectorRows
	o.stats.KeyScalarEvaluations += stats.ScalarEvaluations
	recordSortWork(SortStats{
		KeyEvaluations: stats.KeyEvaluations, KeyRootCalls: stats.KeyRootCalls,
		KeyKernelRows: stats.VectorRows, KeyScalarEvaluations: stats.ScalarEvaluations,
	})
}

func (o *sortOperator) recordReserve(bytes int64) {
	o.stats.ReservedBytes += uint64(bytes)
	o.stats.LiveBytes += bytes
	o.stats.PeakBytes = max(o.stats.PeakBytes, uint64(o.stats.LiveBytes))
	recordSortReserve(bytes)
}

func (o *sortOperator) recordRelease(bytes int64) {
	o.stats.ReleasedBytes += uint64(bytes)
	o.stats.LiveBytes -= bytes
	recordSortRelease(bytes)
}
