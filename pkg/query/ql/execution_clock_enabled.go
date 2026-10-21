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

import (
	"sync/atomic"
	"time"
)

type executionClock struct{ now func() time.Time }

var (
	queryClock      atomic.Pointer[executionClock]
	queryClockCalls atomic.Uint64
)

// ExecutionNow captures the clock used by one QL execution.
func ExecutionNow() time.Time {
	queryClockCalls.Add(1)
	if clock := queryClock.Load(); clock != nil {
		return clock.now()
	}
	return time.Now()
}

// SetExecutionClockForTest overrides the execution clock in instrumented tests.
// Tests must serialize overrides and call the returned restore function.
func SetExecutionClockForTest(now func() time.Time) func() {
	previous := queryClock.Swap(&executionClock{now: now})
	return func() { queryClock.Store(previous) }
}

// ExecutionClockCalls reports the number of QL execution clock captures.
func ExecutionClockCalls() uint64 { return queryClockCalls.Load() }
