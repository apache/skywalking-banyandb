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
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	storagetrace "github.com/apache/skywalking-banyandb/banyand/trace"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

func TestServeRejectsFinalizeWithoutSampler(t *testing.T) {
	root := t.TempDir()
	serveErr := Serve(context.Background(), ServerOptions{
		Root: root, SocketPath: filepath.Join(root, "control.sock"), OutputPath: filepath.Join(root, "report.json"),
		ProfileDir: filepath.Join(root, "profiles"), RunFinalize: true,
	})
	require.ErrorContains(t, serveErr, "finalize requires a sampler plugin")
}

func TestServeRejectsSamplerConfigWithoutPlugin(t *testing.T) {
	root := t.TempDir()
	serveErr := Serve(context.Background(), ServerOptions{
		Root: root, SocketPath: filepath.Join(root, "control.sock"), OutputPath: filepath.Join(root, "report.json"),
		ProfileDir: filepath.Join(root, "profiles"), PluginConfig: []byte(`{"healthySampleRate":"0.1"}`),
	})
	require.ErrorContains(t, serveErr, "sampler config requires a sampler plugin")
}

func TestServeRejectsUnexpectedSamplerConfigChecksum(t *testing.T) {
	root := t.TempDir()
	serveErr := Serve(context.Background(), ServerOptions{
		Root: root, SocketPath: filepath.Join(root, "control.sock"), OutputPath: filepath.Join(root, "report.json"),
		ProfileDir: filepath.Join(root, "profiles"), PluginPath: filepath.Join(root, "sampler.so"),
		PluginConfig: []byte(`{"healthySampleRate":"0.1"}`),
		ExecutionIdentity: ExecutionIdentity{
			PluginConfigSHA256: "unexpected",
		},
		SegmentTimeRange: timestamp.NewInclusiveTimeRange(time.Unix(0, 1), time.Unix(0, 2)),
	})
	require.ErrorContains(t, serveErr, "sampler config checksum")
}

func TestServeRejectsSamplingOracleWithoutPlugin(t *testing.T) {
	root := t.TempDir()
	serveErr := Serve(context.Background(), ServerOptions{
		Root: root, SocketPath: filepath.Join(root, "control.sock"), OutputPath: filepath.Join(root, "report.json"),
		ProfileDir: filepath.Join(root, "profiles"), SamplingOracle: &SamplingOracleArtifact{Version: 1},
	})
	require.ErrorContains(t, serveErr, "sampling oracle requires a sampler plugin")
}

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

func TestRetainAllFinalizeOutputRequiresExecutedLosslessFinalize(t *testing.T) {
	baseEvent := storagetrace.BenchmarkMergeEvent{
		Type: "finalize", Phase: storagetrace.BenchmarkMergePhaseCooldown,
		Sampling: storagetrace.BenchmarkMergeSamplingExecuted, PluginCalls: 2,
		TracesEvaluated: 10, TracesRetained: 10, MatureInputParts: 1,
	}
	require.True(t, retainAllFinalizeOutputCorrect([]storagetrace.BenchmarkMergeEvent{baseEvent}))
	hotBypass := storagetrace.BenchmarkMergeEvent{
		Type: "file", Phase: storagetrace.BenchmarkMergePhasePrimary,
		Sampling: storagetrace.BenchmarkMergeSamplingNotExecuted, Reason: storagetrace.BenchmarkMergeReasonGrace,
		HotInputParts: 1,
	}
	require.True(t, retainAllFinalizeOutputCorrect([]storagetrace.BenchmarkMergeEvent{hotBypass, baseEvent}))

	hotPluginExecution := hotBypass
	hotPluginExecution.Sampling = storagetrace.BenchmarkMergeSamplingExecuted
	hotPluginExecution.Reason = ""
	hotPluginExecution.PluginCalls = 1
	hotPluginExecution.TracesEvaluated = 1
	hotPluginExecution.TracesRetained = 1
	require.False(t, retainAllFinalizeOutputCorrect([]storagetrace.BenchmarkMergeEvent{hotPluginExecution, baseEvent}))

	missingFinalize := baseEvent
	missingFinalize.Type = "file"
	require.False(t, retainAllFinalizeOutputCorrect([]storagetrace.BenchmarkMergeEvent{missingFinalize}))

	droppedTrace := baseEvent
	droppedTrace.TracesRetained--
	droppedTrace.TracesDropped = 1
	require.False(t, retainAllFinalizeOutputCorrect([]storagetrace.BenchmarkMergeEvent{droppedTrace}))

	losslessRetry := baseEvent
	losslessRetry.LosslessRetry = true
	require.False(t, retainAllFinalizeOutputCorrect([]storagetrace.BenchmarkMergeEvent{losslessRetry}))

	failedFinalize := baseEvent
	failedFinalize.Error = "merge failed"
	require.False(t, retainAllFinalizeOutputCorrect([]storagetrace.BenchmarkMergeEvent{failedFinalize}))
}

func TestSamplingOracleOutputRequiresExpectedMatureDecisions(t *testing.T) {
	oracle := SamplingOracleArtifact{Evaluated: 10, Retained: 6, Dropped: 4}
	events := []storagetrace.BenchmarkMergeEvent{
		{
			Type: "file", Phase: storagetrace.BenchmarkMergePhasePrimary,
			Sampling: storagetrace.BenchmarkMergeSamplingNotExecuted, Reason: storagetrace.BenchmarkMergeReasonGrace,
			HotInputParts: 2,
		},
		{
			Type: "finalize", Phase: storagetrace.BenchmarkMergePhaseCooldown,
			Sampling: storagetrace.BenchmarkMergeSamplingExecuted, PluginCalls: 1,
			TracesEvaluated: 10, TracesRetained: 6, TracesDropped: 4, MatureInputParts: 2,
		},
	}

	require.True(t, samplingOracleOutputCorrect(events, oracle))

	wrongTotals := append([]storagetrace.BenchmarkMergeEvent(nil), events...)
	wrongTotals[1].TracesDropped--
	require.False(t, samplingOracleOutputCorrect(wrongTotals, oracle))

	hotEvaluation := append([]storagetrace.BenchmarkMergeEvent(nil), events...)
	hotEvaluation[0].Sampling = storagetrace.BenchmarkMergeSamplingExecuted
	hotEvaluation[0].PluginCalls = 1
	require.False(t, samplingOracleOutputCorrect(hotEvaluation, oracle))
}
