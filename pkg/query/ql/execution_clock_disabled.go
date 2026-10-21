//go:build !querymetrics

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

import "time"

// ExecutionNow captures the clock used by one QL execution.
func ExecutionNow() time.Time { return time.Now() }

// SetExecutionClockForTest requires an instrumented test build.
func SetExecutionClockForTest(func() time.Time) func() {
	panic("QL execution clock overrides require -tags=querymetrics")
}

// ExecutionClockCalls is unavailable outside instrumented test builds.
func ExecutionClockCalls() uint64 { return 0 }
