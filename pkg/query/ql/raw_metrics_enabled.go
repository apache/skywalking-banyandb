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

package ql

import "sync/atomic"

var (
	rawProjectionCalls  atomic.Uint64
	rawFilterInputRows  atomic.Uint64
	rawFilterOutputRows atomic.Uint64
	rawLimitInputRows   atomic.Uint64
	rawLimitOutputRows  atomic.Uint64
)

// RawExecutionStats returns process-wide raw-operator counters for verification.
func RawExecutionStats() RawExecutionCounters {
	return RawExecutionCounters{
		ProjectionCalls: rawProjectionCalls.Load(), FilterInputRows: rawFilterInputRows.Load(), FilterOutputRows: rawFilterOutputRows.Load(),
		LimitInputRows: rawLimitInputRows.Load(), LimitOutputRows: rawLimitOutputRows.Load(),
	}
}
func recordRawProjection() { rawProjectionCalls.Add(1) }
func recordRawSelection(filterInput, filterOutput, limitInput, limitOutput uint64) {
	rawFilterInputRows.Add(filterInput)
	rawFilterOutputRows.Add(filterOutput)
	rawLimitInputRows.Add(limitInput)
	rawLimitOutputRows.Add(limitOutput)
}
