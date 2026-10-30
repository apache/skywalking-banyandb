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

import (
	"context"
	"sync"
	"sync/atomic"
)

var sortCounters struct {
	stats SortStats
	mu    sync.Mutex
}

type sortFullSortKey struct{}

// WithFullSort forces reference full sorting for this instrumented request.
func WithFullSort(ctx context.Context) context.Context {
	return context.WithValue(ctx, sortFullSortKey{}, true)
}

func sortFullSort(ctx context.Context) bool {
	fullSort, _ := ctx.Value(sortFullSortKey{}).(bool)
	return fullSort
}

type sortTestHook struct {
	call func(context.Context, SortStats) error
}

var sortHook atomic.Pointer[sortTestHook]

// SetSortExecutionHookForTest observes copied candidates after each input batch.
// Tests must serialize overrides and call the returned restore function.
func SetSortExecutionHookForTest(hook func(context.Context, SortStats) error) func() {
	previous := sortHook.Swap(&sortTestHook{call: hook})
	return func() { sortHook.Store(previous) }
}

func sortExecutionHook(ctx context.Context, stats SortStats) error {
	if hook := sortHook.Load(); hook != nil && hook.call != nil {
		return hook.call(ctx, stats)
	}
	return nil
}

// SortExecutionStats returns process-wide ordering counters.
func SortExecutionStats() SortExecutionCounters {
	sortCounters.mu.Lock()
	defer sortCounters.mu.Unlock()
	return sortCounters.stats
}

func recordSortWork(delta SortStats) {
	sortCounters.mu.Lock()
	defer sortCounters.mu.Unlock()
	s := &sortCounters.stats
	s.Batches += delta.Batches
	s.InputRows += delta.InputRows
	s.OutputRows += delta.OutputRows
	s.KeyEvaluations += delta.KeyEvaluations
	s.KeyRootCalls += delta.KeyRootCalls
	s.KeyKernelRows += delta.KeyKernelRows
	s.KeyScalarEvaluations += delta.KeyScalarEvaluations
	s.Comparisons += delta.Comparisons
	s.AcceptedRows += delta.AcceptedRows
	s.EvictedRows += delta.EvictedRows
}

func recordSortCompletion(stats SortStats) {
	sortCounters.mu.Lock()
	defer sortCounters.mu.Unlock()
	s := &sortCounters.stats
	s.Executions++
	s.Candidates += stats.Candidates
	s.PeakCandidates += stats.PeakCandidates
	s.CandidateCapacity += stats.CandidateCapacity
	s.PayloadBytes += stats.PayloadBytes
	s.PayloadCapacity += stats.PayloadCapacity
	s.PeakPayloadBytes += stats.PeakPayloadBytes
	s.DescriptorBytes += stats.DescriptorBytes
	s.PeakBytes += stats.PeakBytes
	s.SourcePins += stats.SourcePins
	s.K += stats.K
}

func recordSortReserve(bytes int64) {
	sortCounters.mu.Lock()
	defer sortCounters.mu.Unlock()
	sortCounters.stats.ReservedBytes += uint64(bytes)
	sortCounters.stats.LiveBytes += bytes
}

func recordSortRelease(bytes int64) {
	sortCounters.mu.Lock()
	defer sortCounters.mu.Unlock()
	sortCounters.stats.ReleasedBytes += uint64(bytes)
	sortCounters.stats.LiveBytes -= bytes
}
