// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package tracebaseline

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	storagetrace "github.com/apache/skywalking-banyandb/banyand/trace"
)

func TestPhaseProfilerStopsAndClosesIdempotently(t *testing.T) {
	profilePath := filepath.Join(t.TempDir(), "cpu.pprof")
	profiler, startErr := startPhaseProfiler(profilePath)
	require.NoError(t, startErr)
	require.NoError(t, profiler.stop())
	require.NoError(t, profiler.stop())
	require.FileExists(t, profilePath)
}

func TestPhaseProfilerClosesFileWhenStartFails(t *testing.T) {
	firstPath := filepath.Join(t.TempDir(), "first.pprof")
	first, firstErr := startPhaseProfiler(firstPath)
	require.NoError(t, firstErr)
	secondPath := filepath.Join(t.TempDir(), "second.pprof")
	_, secondErr := startPhaseProfiler(secondPath)
	require.ErrorContains(t, secondErr, "cannot start CPU profile")
	require.NoError(t, first.stop())
}

func TestEqualLedgerChecksumsRequiresExactNonEmptySet(t *testing.T) {
	expected := map[string]string{"core": "a", "latency": "b", "start_time": "c"}
	require.True(t, equalLedgerChecksums(expected, map[string]string{"start_time": "c", "core": "a", "latency": "b"}))
	require.False(t, equalLedgerChecksums(expected, map[string]string{"core": "a", "latency": "b"}))
	require.False(t, equalLedgerChecksums(nil, nil))
}

func TestCountMergeTemperaturesCountsMixedMergeAsMature(t *testing.T) {
	events := []storagetrace.BenchmarkMergeEvent{
		{HotInputParts: 2},
		{MatureInputParts: 2},
		{HotInputParts: 1, MatureInputParts: 1},
	}

	hotMerges, matureMerges := countMergeTemperatures(events)

	require.Equal(t, 2, hotMerges)
	require.Equal(t, 2, matureMerges)
}

func TestLogicalWriteAmplificationUsesSelectedMergeBytes(t *testing.T) {
	report := storagetrace.BenchmarkMergeReport{Events: []storagetrace.BenchmarkMergeEvent{{
		InputBytes: 100, OutputBytes: 90,
		Children: []storagetrace.BenchmarkMergeChild{
			{Name: LedgerLatency, InputBytes: 50, OutputBytes: 52},
			{Name: LedgerStartTime, InputBytes: 25, OutputBytes: 24},
		},
	}}}

	require.InDelta(t, 166.0/175.0, logicalWriteAmplification(report), 0.000001)
	require.Zero(t, logicalWriteAmplification(storagetrace.BenchmarkMergeReport{}))
}
