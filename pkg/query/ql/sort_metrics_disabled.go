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
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package ql

import "context"

// WithFullSort leaves production execution unchanged.
func WithFullSort(ctx context.Context) context.Context { return ctx }

// SetSortExecutionHookForTest requires an instrumented test build.
func SetSortExecutionHookForTest(func(context.Context, SortStats) error) func() {
	panic("sort execution hooks require -tags=querymetrics")
}

// SortExecutionStats returns zero counters in production builds.
func SortExecutionStats() SortExecutionCounters          { return SortExecutionCounters{} }
func sortFullSort(context.Context) bool                  { return false }
func sortExecutionHook(context.Context, SortStats) error { return nil }
func recordSortWork(SortStats)                           {}
func recordSortCompletion(SortStats)                     {}
func recordSortReserve(int64)                            {}
func recordSortRelease(int64)                            {}
