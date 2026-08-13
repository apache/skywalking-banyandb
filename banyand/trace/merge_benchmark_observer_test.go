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

package trace

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

func TestMergeBenchmarkCountsActualSamplerInvocations(t *testing.T) {
	first := &durationEnvelopeSampler{}
	second := &durationEnvelopeSampler{}
	chain := newMergeChain("group", "schema", []sdk.Sampler{first, second}, 1)
	t.Cleanup(chain.close)
	batch := &sdk.TraceBatch{Traces: []sdk.TraceBlock{{TraceID: "a"}, {TraceID: "b"}}}
	observation := &mergeEvaluationObservation{}
	_, executeErr := chain.executeObserved(batch, time.Second, observation)
	require.NoError(t, executeErr)
	require.Equal(t, uint64(2), observation.pluginCalls.Load())
	require.Equal(t, uint64(2), observation.evaluated.Load())

	chain.mu.Lock()
	chain.circuitOpen = true
	chain.mu.Unlock()
	bypassed := &mergeEvaluationObservation{}
	_, executeErr = chain.executeObserved(batch, time.Second, bypassed)
	require.NoError(t, executeErr)
	require.Zero(t, bypassed.pluginCalls.Load())
	require.Zero(t, bypassed.evaluated.Load())
}

func TestMergeBenchmarkObserverReportsConcurrentStagingPeak(t *testing.T) {
	observer := newMergeBenchmarkObserver(nil, mergeBenchmarkObserverOptions{})
	first := &mergeEvaluationObservation{observer: observer}
	second := &mergeEvaluationObservation{observer: observer}

	first.observeStagedBytes(100)
	second.observeStagedBytes(200)
	first.observeStagedBytes(0)
	second.observeStagedBytes(0)

	require.Equal(t, uint64(300), observer.snapshot().PeakConcurrentStagedBytes)
}

func TestBenchmarkSidxPartTotalsReportsMetadataFailure(t *testing.T) {
	_, _, totalsErr := benchmarkSidxPartTotals(fs.NewLocalFileSystem(), map[uint64]string{1: filepath.Join(t.TempDir(), "missing")})
	require.ErrorContains(t, totalsErr, "cannot read secondary-index part metadata")
}

func TestMergeBenchmarkObserverClassifiesHotAndMatureMerges(t *testing.T) {
	t.Run("hot merge bypasses sampling", func(t *testing.T) {
		event, samplerCalls, jsonLines := runObservedBenchmarkMerge(t, false)
		require.Equal(t, mergeSamplingNotExecuted, event.Sampling)
		require.Equal(t, mergeReasonGrace, event.Reason)
		require.Zero(t, event.PluginCalls)
		require.Zero(t, event.TracesEvaluated)
		require.Zero(t, samplerCalls)
		require.Equal(t, uint32(2), event.HotInputParts)
		require.Zero(t, event.MatureInputParts)
		require.Equal(t, event.InputRows, event.HotInputRows)
		require.Zero(t, event.MatureInputRows)
		require.Equal(t, uint32(0), event.InputMinDepth)
		require.Equal(t, uint32(0), event.InputMaxDepth)
		require.Equal(t, uint32(1), event.OutputDepth)
		require.Len(t, event.Children, 2)
		for childIdx := range event.Children {
			require.Equal(t, event.Sampling, event.Children[childIdx].Sampling)
			require.Equal(t, event.Reason, event.Children[childIdx].Reason)
			require.Equal(t, event.OutputPartID, event.Children[childIdx].OutputPartID)
		}
		require.Equal(t, 1, strings.Count(strings.TrimSpace(jsonLines), "\n")+1)
	})

	t.Run("mature depth one merge executes sampler", func(t *testing.T) {
		event, samplerCalls, _ := runObservedBenchmarkMerge(t, true)
		require.Equal(t, mergeSamplingExecuted, event.Sampling)
		require.Empty(t, event.Reason)
		require.Positive(t, event.PluginCalls)
		require.Equal(t, uint64(2), event.TracesEvaluated)
		require.Positive(t, samplerCalls)
		require.Equal(t, samplerCalls, int64(event.PluginCalls))
		require.Positive(t, event.EstimatedStagingBytes)
		require.Positive(t, event.StagingHardLimit)
		require.Positive(t, event.DecisionBatchLimit)
		require.LessOrEqual(t, event.DecisionBatchLimit, event.StagingHardLimit)
		require.Positive(t, event.PlannedStagingBatches)
		require.Positive(t, event.DecisionMaxTraceCount)
		require.Positive(t, event.PeakStagedBytes)
		require.Len(t, event.StagingBatches, 1)
		require.Equal(t, mergeStagingFlushEndOfMerge, event.StagingBatches[0].Reason)
		require.Equal(t, event.TracesEvaluated, event.StagingBatches[0].Traces)
		require.Positive(t, event.StagingBatches[0].Bytes)
		require.Equal(t, event.StagingBatches[0].Bytes, event.ChargedStagingBytes)
		require.Zero(t, event.HotInputParts)
		require.Equal(t, uint32(2), event.MatureInputParts)
		require.Zero(t, event.HotInputRows)
		require.Equal(t, event.InputRows, event.MatureInputRows)
		require.Equal(t, uint32(1), event.InputMinDepth)
		require.Equal(t, uint32(1), event.InputMaxDepth)
		require.Equal(t, uint32(2), event.OutputDepth)
		for childIdx := range event.Children {
			require.Equal(t, event.Sequence, event.Children[childIdx].ParentSequence)
			require.Equal(t, event.Sampling, event.Children[childIdx].Sampling)
			require.Equal(t, event.OutputPartID, event.Children[childIdx].OutputPartID)
			require.Positive(t, event.Children[childIdx].OutputBytes)
		}
	})
}

func TestMergeBenchmarkObserverReportsBudgetTriggeredBatches(t *testing.T) {
	previousBudget := testStageBudgetOverride
	testStageBudgetOverride = 1
	t.Cleanup(func() { testStageBudgetOverride = previousBudget })

	event, samplerCalls, _ := runObservedBenchmarkMerge(t, true)
	require.Equal(t, int64(2), samplerCalls)
	require.Equal(t, uint64(2), event.PluginCalls)
	require.Len(t, event.StagingBatches, 2)
	require.Equal(t, mergeStagingFlushByteAndTraceLimit, event.StagingBatches[0].Reason)
	require.Equal(t, uint64(1), event.StagingBatches[0].Traces)
	require.Equal(t, mergeStagingFlushEndOfMerge, event.StagingBatches[1].Reason)
	require.Equal(t, uint64(1), event.StagingBatches[1].Traces)
	require.GreaterOrEqual(t, event.PeakStagedBytes, event.StagingBatches[0].Bytes)
}

func TestClassifyMergeObservation(t *testing.T) {
	tests := []struct {
		name          string
		initialReason mergeSamplingReason
		wantSampling  mergeSamplingClassification
		wantReason    mergeSamplingReason
		pluginCalls   uint64
		evaluated     uint64
		oversized     uint64
		losslessRetry bool
	}{
		{name: "executed", pluginCalls: 1, evaluated: 2, wantSampling: mergeSamplingExecuted},
		{name: "pipeline disabled", initialReason: mergeReasonPipelineDisabled, wantSampling: mergeSamplingNotExecuted, wantReason: mergeReasonPipelineDisabled},
		{name: "all oversized", oversized: 2, wantSampling: mergeSamplingEnabledNoEvaluation, wantReason: mergeReasonAllOversized},
		{name: "installed without evaluation", wantSampling: mergeSamplingEnabledNoEvaluation, wantReason: mergeReasonOther},
		{
			name: "retry overrides evaluation", pluginCalls: 1, evaluated: 2, losslessRetry: true,
			wantSampling: mergeSamplingNotExecuted, wantReason: mergeReasonLosslessRetry,
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			sampling, reason := classifyMergeObservation(testCase.initialReason, testCase.pluginCalls, testCase.evaluated,
				testCase.oversized, testCase.losslessRetry)
			require.Equal(t, testCase.wantSampling, sampling)
			require.Equal(t, testCase.wantReason, reason)
		})
	}
}

func TestBuildHotMergeFilterDecisionReportsBoundedReasons(t *testing.T) {
	const group = "merge-observer-reasons"
	resetRegistries()
	t.Cleanup(resetRegistries)
	table := newImplementationGuardTable(t, group)
	table.mustAddTraces(tracesWithIDs("trace-a"), nil)
	waitForImplementationFilePartCount(t, table, 1)
	table.mustAddTraces(tracesWithIDs("trace-b"), nil)
	parts := waitForImplementationFileParts(t, table, 2)
	defer releaseImplementationParts(parts)

	table.option.nativePipelineEnabled = false
	filter, reason := table.buildHotMergeFilterDecision(parts)
	require.Nil(t, filter)
	require.Equal(t, mergeReasonPipelineDisabled, reason)
	table.option.nativePipelineEnabled = true
	filter, reason = table.buildHotMergeFilterDecision(parts)
	require.Nil(t, filter)
	require.Equal(t, mergeReasonNoSampler, reason)
	deregister := registerSampler(group, &durationEnvelopeSampler{})
	t.Cleanup(deregister)
	setMergeEventForGroup(group, false)
	filter, reason = table.buildHotMergeFilterDecision(parts)
	require.Nil(t, filter)
	require.Equal(t, mergeReasonEventDisabled, reason)
	setMergeEventForGroup(group, true)
	maxTimestamp := parts[0].p.partMetadata.MaxTimestamp
	for partIdx := 1; partIdx < len(parts); partIdx++ {
		maxTimestamp = max(maxTimestamp, parts[partIdx].p.partMetadata.MaxTimestamp)
	}
	table.setMergeNow(time.Unix(0, maxTimestamp))
	filter, reason = table.buildHotMergeFilterDecision(parts)
	require.Nil(t, filter)
	require.Equal(t, mergeReasonGrace, reason)
	table.setMergeNow(time.Unix(0, maxTimestamp+int64(2*time.Millisecond)))
	segmentRange := table.segmentTimeRange
	table.segmentTimeRange = timestamp.TimeRange{}
	filter, reason = table.buildHotMergeFilterDecision(parts)
	require.Nil(t, filter)
	require.Equal(t, mergeReasonGuardUnavailable, reason)
	table.segmentTimeRange = segmentRange
	filter, reason = table.buildHotMergeFilterDecision(parts)
	require.NotNil(t, filter)
	require.Empty(t, reason)
	filter.guard.Close()
	filter, reason = table.buildHotMergeFilterDecision(nil)
	require.Nil(t, filter)
	require.Equal(t, mergeReasonEmptyInput, reason)
}

func TestMergeBenchmarkObserverMaturesProductionSelectedHotOutputs(t *testing.T) {
	fileSystem := fs.NewLocalFileSystem()
	workspace := t.TempDir()
	root := filepath.Join(workspace, "data")
	prepared := filepath.Join(workspace, "prepared")
	for _, basePath := range []string{root, prepared} {
		for _, indexName := range []string{"latency", "start_time"} {
			require.NoError(t, os.MkdirAll(filepath.Join(basePath, sidxDirName, indexName), 0o755))
		}
	}
	partIDs := make([]uint64, 30)
	for partIdx := range partIDs {
		partIDs[partIdx] = uint64(partIdx + 1)
	}
	base := time.Date(2026, time.August, 1, 0, 0, 0, 0, time.UTC)
	buildExternalCoreParts(t, fileSystem, prepared, partIDs, base)
	for _, indexName := range []string{"latency", "start_time"} {
		buildExternalIndexParts(t, fileSystem, filepath.Join(prepared, sidxDirName, indexName), partIDs, base)
	}
	group := "merge-benchmark-hot-outputs"
	sampler := &durationEnvelopeSampler{}
	deregister := registerSampler(group, sampler)
	t.Cleanup(deregister)
	setMergeEventForGroup(group, true)
	t.Cleanup(func() { setMergeEventForGroup(group, false) })
	setMergeGraceForGroup(group, int64(2*time.Hour))
	t.Cleanup(func() { setMergeGraceForGroup(group, 0) })
	segmentRange := timestamp.NewInclusiveTimeRange(base.Add(-24*time.Hour), base.Add(24*time.Hour))
	table, tableErr := newTSTable(fileSystem, root, common.Position{Database: group}, logger.GetLogger(group), segmentRange, option{
		flushTimeout: 0, mergePolicy: newDefaultMergePolicyForTesting(), protector: protector.Nop{}, nativePipelineEnabled: true,
		mergeGraceDefault: 2 * time.Hour,
	}, nil)
	require.NoError(t, tableErr)
	t.Cleanup(func() { require.NoError(t, table.Close()) })
	for _, indexName := range []string{"latency", "start_time"} {
		table.mustGetOrCreateSidx(indexName)
	}
	table.observePartID(partIDs[len(partIDs)-1])
	table.setMergeNow(base.Add(time.Minute))
	var output bytes.Buffer
	observer := newMergeBenchmarkObserver(&output, mergeBenchmarkObserverOptions{Phase: mergePhasePrimary, Attribution: true})
	require.True(t, table.setMergeBenchmarkObserver(observer))
	require.NoError(t, table.blockMergeUntilWave())
	for _, partID := range partIDs {
		require.NoError(t, os.Rename(filepath.Join(prepared, formatExternalPartID(partID)), filepath.Join(root, formatExternalPartID(partID))))
		indexPaths := make(map[string]string, 2)
		for _, indexName := range []string{"latency", "start_time"} {
			destination := filepath.Join(root, sidxDirName, indexName, formatExternalPartID(partID))
			require.NoError(t, os.Rename(filepath.Join(prepared, sidxDirName, indexName, formatExternalPartID(partID)), destination))
			indexPaths[indexName] = destination
		}
		table.mustAddFilePart(partID, indexPaths)
	}
	waveCtx, cancelWave := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancelWave()
	require.NoError(t, table.triggerMergeWave(waveCtx))
	hotSnapshot := observer.snapshot()
	require.NotEmpty(t, hotSnapshot.Events)
	for eventIdx := range hotSnapshot.Events {
		event := &hotSnapshot.Events[eventIdx]
		require.Equal(t, mergeSamplingNotExecuted, event.Sampling)
		require.Equal(t, mergeReasonGrace, event.Reason)
		require.Equal(t, uint32(0), event.InputMinDepth)
		require.Equal(t, uint32(1), event.OutputDepth)
		require.Zero(t, event.PluginCalls)
		require.True(t, event.Resources.AttributionValid)
	}
	require.Zero(t, sampler.calls.Load())

	observer.setPhase(mergePhaseCooldown)
	table.setMergeNow(base.Add(3 * time.Hour))
	require.NoError(t, table.releaseMergeWave())
	require.NoError(t, table.triggerMerge())
	idleCtx, cancelIdle := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancelIdle()
	require.NoError(t, table.waitForMergeIdle(idleCtx))
	finalSnapshot := observer.snapshot()
	require.Greater(t, len(finalSnapshot.Events), len(hotSnapshot.Events))
	foundMatureDepthOne := false
	for eventIdx := len(hotSnapshot.Events); eventIdx < len(finalSnapshot.Events); eventIdx++ {
		event := &finalSnapshot.Events[eventIdx]
		require.True(t, event.Resources.AttributionValid)
		if event.Sampling == mergeSamplingExecuted && event.InputMinDepth >= 1 {
			foundMatureDepthOne = true
			require.Equal(t, mergePhaseCooldown, event.Phase)
			require.GreaterOrEqual(t, event.OutputDepth, uint32(2))
			require.Positive(t, event.TracesEvaluated)
		}
	}
	require.True(t, foundMatureDepthOne)
	require.Positive(t, sampler.calls.Load())
	phases := make(map[mergeBenchmarkPhase]struct{})
	for aggregateIdx := range finalSnapshot.Aggregates {
		phases[finalSnapshot.Aggregates[aggregateIdx].Phase] = struct{}{}
	}
	require.Contains(t, phases, mergePhasePrimary)
	require.Contains(t, phases, mergePhaseCooldown)
}

func runObservedBenchmarkMerge(t *testing.T, mature bool) (mergeBenchmarkEvent, int64, string) {
	t.Helper()
	fileSystem := fs.NewLocalFileSystem()
	workspace := t.TempDir()
	root := filepath.Join(workspace, "data")
	prepared := filepath.Join(workspace, "prepared")
	require.NoError(t, os.MkdirAll(root, 0o755))
	for _, indexName := range []string{"latency", "start_time"} {
		require.NoError(t, os.MkdirAll(filepath.Join(root, sidxDirName, indexName), 0o755))
		require.NoError(t, os.MkdirAll(filepath.Join(prepared, sidxDirName, indexName), 0o755))
	}
	partIDs := []uint64{1, 2}
	base := time.Date(2026, time.August, 1, 0, 0, 0, 0, time.UTC)
	buildExternalCoreParts(t, fileSystem, prepared, partIDs, base)
	for _, indexName := range []string{"latency", "start_time"} {
		buildExternalIndexParts(t, fileSystem, filepath.Join(prepared, sidxDirName, indexName), partIDs, base)
	}

	group := "merge-benchmark-observer-" + strings.ReplaceAll(t.Name(), "/", "-")
	sampler := &durationEnvelopeSampler{}
	deregister := registerSampler(group, sampler)
	t.Cleanup(deregister)
	setMergeEventForGroup(group, true)
	t.Cleanup(func() { setMergeEventForGroup(group, false) })
	setMergeGraceForGroup(group, int64(2*time.Hour))
	t.Cleanup(func() { setMergeGraceForGroup(group, 0) })

	segmentRange := timestamp.NewInclusiveTimeRange(base.Add(-24*time.Hour), base.Add(24*time.Hour))
	table, tableErr := newTSTable(fileSystem, root, common.Position{Database: group}, logger.GetLogger(group), segmentRange, option{
		flushTimeout:          0,
		mergePolicy:           newDefaultMergePolicyForTesting(),
		protector:             protector.Nop{},
		nativePipelineEnabled: true,
		mergeGraceDefault:     2 * time.Hour,
	}, nil)
	require.NoError(t, tableErr)
	t.Cleanup(func() { require.NoError(t, table.Close()) })
	for _, indexName := range []string{"latency", "start_time"} {
		table.mustGetOrCreateSidx(indexName)
	}
	table.observePartID(partIDs[len(partIDs)-1])
	for _, partID := range partIDs {
		require.NoError(t, os.Rename(filepath.Join(prepared, formatExternalPartID(partID)), filepath.Join(root, formatExternalPartID(partID))))
		for _, indexName := range []string{"latency", "start_time"} {
			require.NoError(t, os.Rename(filepath.Join(prepared, sidxDirName, indexName, formatExternalPartID(partID)),
				filepath.Join(root, sidxDirName, indexName, formatExternalPartID(partID))))
		}
		table.mustAddFilePart(partID, map[string]string{
			"latency":    filepath.Join(root, sidxDirName, "latency", formatExternalPartID(partID)),
			"start_time": filepath.Join(root, sidxDirName, "start_time", formatExternalPartID(partID)),
		})
	}
	if mature {
		snapshot := table.currentSnapshot()
		require.NotNil(t, snapshot)
		for partIdx := range snapshot.parts {
			snapshot.parts[partIdx].mergeDepth = 1
		}
		snapshot.decRef()
		table.setMergeNow(base.Add(3 * time.Hour))
	} else {
		table.setMergeNow(base.Add(time.Minute))
	}

	var jsonLines bytes.Buffer
	observer := newMergeBenchmarkObserver(&jsonLines, mergeBenchmarkObserverOptions{Phase: mergePhasePrimary, Attribution: true})
	require.True(t, table.setMergeBenchmarkObserver(observer))
	require.NoError(t, table.triggerMerge())
	waitCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	require.NoError(t, table.waitForMergeIdle(waitCtx))

	snapshot := observer.snapshot()
	require.Len(t, snapshot.Events, 1)
	require.Len(t, snapshot.Aggregates, 1)
	require.Equal(t, uint64(1), snapshot.Aggregates[0].Merges)
	require.Equal(t, snapshot.Events[0].InputBytes, snapshot.Aggregates[0].InputBytes)
	require.Equal(t, snapshot.Events[0].OutputBytes, snapshot.Aggregates[0].OutputBytes)
	require.Equal(t, uint64(2), snapshot.Aggregates[0].ChildMerges)
	require.Positive(t, snapshot.Aggregates[0].ChildOutputBytes)
	require.Positive(t, snapshot.Events[0].Resources.ElapsedNanos)
	require.Equal(t, uint32(1), snapshot.Events[0].Version)
	require.NotEmpty(t, snapshot.Events[0].SelectionSHA256)
	require.GreaterOrEqual(t, snapshot.Events[0].QueueNanos, int64(0))
	require.GreaterOrEqual(t, snapshot.Events[0].Resources.PeakHeapBytes, snapshot.Events[0].Resources.EndHeapBytes)
	require.True(t, snapshot.Events[0].Resources.AttributionValid)
	for childIdx := range snapshot.Events[0].Children {
		require.Equal(t, snapshot.Events[0].InputRows, snapshot.Events[0].Children[childIdx].InputRows)
		require.Equal(t, snapshot.Events[0].OutputRows, snapshot.Events[0].Children[childIdx].OutputRows)
	}
	var persisted mergeBenchmarkEvent
	require.NoError(t, json.Unmarshal(bytes.TrimSpace(jsonLines.Bytes()), &persisted))
	require.Equal(t, snapshot.Events[0].Sequence, persisted.Sequence)
	require.Equal(t, snapshot.Events[0].Sampling, persisted.Sampling)
	return snapshot.Events[0], sampler.calls.Load(), jsonLines.String()
}
