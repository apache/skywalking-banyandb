// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. Apache Software
// Foundation (ASF) licenses this file to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with the
// License. You may obtain a copy of the License at
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
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/filter"
)

const (
	liveGuardShardEnv                    = "BANYANDB_TRACE_GUARD_SHARD"
	liveGuardDefaultGrace                = int64(defaultTracePipelineMergeGrace)
	liveGuardLogicalNow                  = int64(1785369596171000000)
	liveGuardFixtureLargePartMinimumSize = uint64(1 << 20)
	liveGuardTraceMapHash                = "02eda3eed08d17590527d02fd5f56610fba5fe0bd7252f9575df9bbdd6c9df7f"
)

type liveGuardTraceRange struct {
	Min   int64 `json:"min"`
	Max   int64 `json:"max"`
	Count int   `json:"count"`
}

type liveGuardTracePart struct {
	Traces map[string]liveGuardTraceRange `json:"traces"`
	ID     string                         `json:"id"`
}

type liveGuardTraceTimes struct {
	Parts []liveGuardTracePart `json:"parts"`
}

type liveGuardPart struct {
	traces    map[string]liveGuardTraceRange
	id        string
	guardPart fragmentGuardPrototypePart
	metadata  partMetadata
}

type liveGuardDataset struct {
	parts      map[string]*liveGuardPart
	traceParts map[string][]string
	partIDs    []string
	logicalNow int64
}

type liveGuardSelection struct {
	name                  string
	selectedPartIDs       []string
	expectedSelectedTrace int
	expectedSplitTrace    int
	expectedSelectedBytes uint64
}

type liveGuardExpectedResult struct {
	candidatePairs int
	retained       int
	dropped        int
	falseDeferral  int
	bloomProbes    int
}

type liveGuardTrace struct {
	id      string
	idBytes []byte
	minTS   int64
	maxTS   int64
	split   bool
}

type preparedLiveGuardSelection struct {
	name                 string
	traces               []liveGuardTrace
	outsideParts         []fragmentGuardPrototypePart
	outsidePartIDs       []string
	selectedParts        []*partWrapper
	selectedMaxTimestamp int64
	selectedBytes        uint64
}

type liveGuardMetrics struct {
	retained       int
	dropped        int
	falseNegative  int
	falseDeferral  int
	bloomProbes    int
	candidateParts int
}

type liveGuardMergeResult struct {
	guardMetrics    liveGuardMetrics
	samplingApplied bool
	sampledTraces   int
	guardedDrops    int
}

var liveGuardExpectedCooledPartIDs = []string{
	"00000000000005ee",
	"0000000000000b5b",
	"0000000000001002",
	"0000000000001191",
	"000000000000165a",
	"0000000000001c36",
}

var liveGuardHotSelections = []liveGuardSelection{
	{
		name: "recent_small_parts",
		selectedPartIDs: []string{
			"0000000000002266", "000000000000226b", "0000000000002265", "0000000000002259",
			"0000000000002233", "000000000000222c", "0000000000002256", "0000000000002264",
			"0000000000002223", "0000000000002248", "000000000000223a", "00000000000021f1",
		},
		expectedSelectedTrace: 254,
		expectedSplitTrace:    18,
		expectedSelectedBytes: 301026,
	},
	{
		name: "cooled_large_plus_recent_small",
		selectedPartIDs: []string{
			"00000000000005ee", "0000000000000b5b", "0000000000001002", "0000000000001191",
			"000000000000165a", "0000000000001c36", "00000000000021f1",
		},
		expectedSelectedTrace: 31877,
		expectedSplitTrace:    34,
		expectedSelectedBytes: 34910915,
	},
	{
		name:                  "recent_large_parts",
		selectedPartIDs:       []string{"0000000000001c36", "00000000000021d4"},
		expectedSelectedTrace: 8755,
		expectedSplitTrace:    38,
		expectedSelectedBytes: 9738930,
	},
}

func loadLiveGuardDataset(testingTB testing.TB) *liveGuardDataset {
	testingTB.Helper()
	shardPath := os.Getenv(liveGuardShardEnv)
	if shardPath == "" {
		testingTB.Skipf("%s is not set", liveGuardShardEnv)
	}

	traceTimesPath := filepath.Join(filepath.Dir(shardPath), "per-trace-times.json")
	traceTimesData, readErr := os.ReadFile(traceTimesPath)
	require.NoError(testingTB, readErr)
	traceTimesHash := sha256.Sum256(traceTimesData)
	require.Equal(testingTB, liveGuardTraceMapHash, fmt.Sprintf("%x", traceTimesHash))
	var traceTimes liveGuardTraceTimes
	require.NoError(testingTB, json.Unmarshal(traceTimesData, &traceTimes))

	dataset := &liveGuardDataset{
		parts:      make(map[string]*liveGuardPart, len(traceTimes.Parts)),
		partIDs:    make([]string, 0, len(traceTimes.Parts)),
		traceParts: make(map[string][]string),
	}
	for partIdx := range traceTimes.Parts {
		tracePart := &traceTimes.Parts[partIdx]
		metadataData, metadataReadErr := os.ReadFile(filepath.Join(shardPath, tracePart.ID, metadataFilename))
		require.NoError(testingTB, metadataReadErr)
		var metadata partMetadata
		require.NoError(testingTB, json.Unmarshal(metadataData, &metadata))

		filterData, filterReadErr := os.ReadFile(filepath.Join(shardPath, tracePart.ID, traceIDFilterFilename))
		require.NoError(testingTB, filterReadErr)
		require.GreaterOrEqual(testingTB, len(filterData), 8)
		traceIDFilter := decodeBloomFilter(filterData, filter.NewBloomFilter(0))

		dataset.parts[tracePart.ID] = &liveGuardPart{
			id:       tracePart.ID,
			metadata: metadata,
			traces:   tracePart.Traces,
			guardPart: fragmentGuardPrototypePart{
				traceIDFilter: traceIDFilter,
				minTimestamp:  metadata.MinTimestamp,
				maxTimestamp:  metadata.MaxTimestamp,
			},
		}
		dataset.logicalNow = max(dataset.logicalNow, metadata.MaxTimestamp)
		dataset.partIDs = append(dataset.partIDs, tracePart.ID)
		for traceID := range tracePart.Traces {
			dataset.traceParts[traceID] = append(dataset.traceParts[traceID], tracePart.ID)
		}
	}
	sort.Strings(dataset.partIDs)
	// Replay the active-ingestion instant when the tail was new, not the later shard-download wall clock.
	require.Equal(testingTB, liveGuardLogicalNow, dataset.logicalNow)
	return dataset
}

func prepareLiveGuardSelection(testingTB testing.TB, dataset *liveGuardDataset,
	selection liveGuardSelection,
) preparedLiveGuardSelection {
	testingTB.Helper()
	selectedPartIDs := make(map[string]struct{}, len(selection.selectedPartIDs))
	selectedRanges := make(map[string]liveGuardTraceRange)
	selectedParts := make([]*partWrapper, 0, len(selection.selectedPartIDs))
	selectedMaxTimestamp := int64(math.MinInt64)
	var selectedBytes uint64
	for _, partID := range selection.selectedPartIDs {
		selectedPart := dataset.parts[partID]
		require.NotNil(testingTB, selectedPart, "part %s is missing", partID)
		selectedPartIDs[partID] = struct{}{}
		selectedMaxTimestamp = max(selectedMaxTimestamp, selectedPart.metadata.MaxTimestamp)
		selectedBytes += selectedPart.metadata.CompressedSizeBytes
		partMetadataCopy := selectedPart.metadata
		selectedParts = append(selectedParts, &partWrapper{p: &part{partMetadata: partMetadataCopy}})
		for traceID, traceRange := range selectedPart.traces {
			aggregatedRange, found := selectedRanges[traceID]
			if !found {
				selectedRanges[traceID] = traceRange
				continue
			}
			aggregatedRange.Min = min(aggregatedRange.Min, traceRange.Min)
			aggregatedRange.Max = max(aggregatedRange.Max, traceRange.Max)
			aggregatedRange.Count += traceRange.Count
			selectedRanges[traceID] = aggregatedRange
		}
	}

	prepared := preparedLiveGuardSelection{
		name:                 selection.name,
		traces:               make([]liveGuardTrace, 0, len(selectedRanges)),
		outsideParts:         make([]fragmentGuardPrototypePart, 0, len(dataset.parts)-len(selectedPartIDs)),
		outsidePartIDs:       make([]string, 0, len(dataset.parts)-len(selectedPartIDs)),
		selectedParts:        selectedParts,
		selectedMaxTimestamp: selectedMaxTimestamp,
		selectedBytes:        selectedBytes,
	}
	for traceID, traceRange := range selectedRanges {
		split := false
		for _, partID := range dataset.traceParts[traceID] {
			if _, selected := selectedPartIDs[partID]; !selected {
				split = true
				break
			}
		}
		prepared.traces = append(prepared.traces, liveGuardTrace{
			id:      traceID,
			idBytes: []byte(traceID),
			minTS:   traceRange.Min,
			maxTS:   traceRange.Max,
			split:   split,
		})
	}
	sort.Slice(prepared.traces, func(leftIdx, rightIdx int) bool {
		return prepared.traces[leftIdx].id < prepared.traces[rightIdx].id
	})

	outsideLiveParts := make([]*liveGuardPart, 0, len(dataset.parts)-len(selectedPartIDs))
	for _, partID := range dataset.partIDs {
		if _, selected := selectedPartIDs[partID]; selected {
			continue
		}
		outsideLiveParts = append(outsideLiveParts, dataset.parts[partID])
	}
	sort.Slice(outsideLiveParts, func(leftIdx, rightIdx int) bool {
		leftPart := outsideLiveParts[leftIdx]
		rightPart := outsideLiveParts[rightIdx]
		if leftPart.guardPart.minTimestamp != rightPart.guardPart.minTimestamp {
			return leftPart.guardPart.minTimestamp < rightPart.guardPart.minTimestamp
		}
		return leftPart.id < rightPart.id
	})
	for _, outsidePart := range outsideLiveParts {
		prepared.outsideParts = append(prepared.outsideParts, outsidePart.guardPart)
		prepared.outsidePartIDs = append(prepared.outsidePartIDs, outsidePart.id)
	}

	require.Len(testingTB, prepared.traces, selection.expectedSelectedTrace)
	splitCount := 0
	for traceIdx := range prepared.traces {
		if prepared.traces[traceIdx].split {
			splitCount++
		}
	}
	require.Equal(testingTB, selection.expectedSplitTrace, splitCount)
	require.Equal(testingTB, selection.expectedSelectedBytes, prepared.selectedBytes)
	return prepared
}

func runLiveAllBloomGuard(prepared *preparedLiveGuardSelection) liveGuardMetrics {
	metrics := liveGuardMetrics{}
	for traceIdx := range prepared.traces {
		traceData := &prepared.traces[traceIdx]
		canDrop := true
		for partIdx := range prepared.outsideParts {
			outsidePart := &prepared.outsideParts[partIdx]
			metrics.bloomProbes++
			if outsidePart.traceIDFilter == nil || outsidePart.traceIDFilter.MightContain(traceData.idBytes) {
				canDrop = false
				break
			}
		}
		if canDrop {
			metrics.dropped++
			if traceData.split {
				metrics.falseNegative++
			}
			continue
		}
		metrics.retained++
		if !traceData.split {
			metrics.falseDeferral++
		}
	}
	return metrics
}

func runLiveTimeOnlyGuard(prepared *preparedLiveGuardSelection, grace int64) liveGuardMetrics {
	metrics := liveGuardMetrics{}
	for traceIdx := range prepared.traces {
		traceData := &prepared.traces[traceIdx]
		guardMin := prototypeSaturatingSub(traceData.minTS, grace)
		guardMax := prototypeSaturatingAdd(traceData.maxTS, grace)
		canDrop := true
		for partIdx := range prepared.outsideParts {
			outsidePart := &prepared.outsideParts[partIdx]
			if outsidePart.maxTimestamp < guardMin || outsidePart.minTimestamp > guardMax {
				continue
			}
			metrics.candidateParts++
			canDrop = false
			break
		}
		if canDrop {
			metrics.dropped++
			if traceData.split {
				metrics.falseNegative++
			}
			continue
		}
		metrics.retained++
		if !traceData.split {
			metrics.falseDeferral++
		}
	}
	return metrics
}

func countLiveTimeCandidatePairs(prepared *preparedLiveGuardSelection, grace int64) int {
	candidatePairs := 0
	for traceIdx := range prepared.traces {
		traceData := &prepared.traces[traceIdx]
		guardMin := prototypeSaturatingSub(traceData.minTS, grace)
		guardMax := prototypeSaturatingAdd(traceData.maxTS, grace)
		for partIdx := range prepared.outsideParts {
			outsidePart := &prepared.outsideParts[partIdx]
			if outsidePart.maxTimestamp >= guardMin && outsidePart.minTimestamp <= guardMax {
				candidatePairs++
			}
		}
	}
	return candidatePairs
}

func liveGuardCooledPartIDs(dataset *liveGuardDataset, grace int64) []string {
	frontier := dataset.logicalNow - grace
	partIDs := make([]string, 0, len(dataset.partIDs))
	for _, partID := range dataset.partIDs {
		if dataset.parts[partID].metadata.MaxTimestamp <= frontier {
			partIDs = append(partIDs, partID)
		}
	}
	return partIDs
}

func liveGuardCandidatePartIDs(prepared *preparedLiveGuardSelection, grace int64) []string {
	candidatePartIDs := make(map[string]struct{})
	for traceIdx := range prepared.traces {
		traceData := &prepared.traces[traceIdx]
		guardMin := prototypeSaturatingSub(traceData.minTS, grace)
		guardMax := prototypeSaturatingAdd(traceData.maxTS, grace)
		for partIdx := range prepared.outsideParts {
			outsidePart := &prepared.outsideParts[partIdx]
			if outsidePart.maxTimestamp >= guardMin && outsidePart.minTimestamp <= guardMax {
				candidatePartIDs[prepared.outsidePartIDs[partIdx]] = struct{}{}
			}
		}
	}
	result := make([]string, 0, len(candidatePartIDs))
	for partID := range candidatePartIDs {
		result = append(result, partID)
	}
	sort.Strings(result)
	return result
}

func runLiveMaturityAwareHybrid(prepared *preparedLiveGuardSelection, now int64) liveGuardMergeResult {
	frontier := now - liveGuardDefaultGrace
	if !mergeMayContainMatureTrace(prepared.selectedParts, frontier) {
		return liveGuardMergeResult{
			guardMetrics: liveGuardMetrics{retained: len(prepared.traces)},
		}
	}
	result := liveGuardMergeResult{}
	for traceIdx := range prepared.traces {
		traceData := &prepared.traces[traceIdx]
		if traceData.maxTS > frontier {
			result.guardMetrics.retained++
			continue
		}
		result.samplingApplied = true
		result.sampledTraces++
		result.guardedDrops++
		guardResult := prototypeConfirmTraceDropBytes(
			traceData.idBytes, traceData.minTS, traceData.maxTS, liveGuardDefaultGrace, prepared.outsideParts,
		)
		result.guardMetrics.bloomProbes += guardResult.bloomProbes
		result.guardMetrics.candidateParts += guardResult.candidateParts
		if guardResult.canDrop {
			result.guardMetrics.dropped++
			if traceData.split {
				result.guardMetrics.falseNegative++
			}
			continue
		}
		result.guardMetrics.retained++
		if !traceData.split {
			result.guardMetrics.falseDeferral++
		}
	}
	return result
}

func requireLiveGuardResult(t *testing.T, expected liveGuardExpectedResult, candidatePairs int, metrics liveGuardMetrics) {
	t.Helper()
	require.Equal(t, expected.candidatePairs, candidatePairs)
	require.Equal(t, expected.retained, metrics.retained)
	require.Equal(t, expected.dropped, metrics.dropped)
	require.Equal(t, expected.falseDeferral, metrics.falseDeferral)
	require.Equal(t, expected.bloomProbes, metrics.bloomProbes)
	require.Zero(t, metrics.falseNegative)
}

func TestFragmentGuardLiveShardDefaultPickerDoesNotDispatch(t *testing.T) {
	dataset := loadLiveGuardDataset(t)
	parts := make([]*partWrapper, 0, len(dataset.partIDs))
	for _, partID := range dataset.partIDs {
		partMetadataCopy := dataset.parts[partID].metadata
		parts = append(parts, &partWrapper{p: &part{partMetadata: partMetadataCopy}})
	}

	selectedParts := newDefaultMergePolicy().getPartsToMerge(nil, parts, math.MaxUint64)

	require.Empty(t, selectedParts, "the frozen fixture is not an ordinary merge-policy dispatch")
}

func TestFragmentGuardLiveShardCooledLargeParts(t *testing.T) {
	dataset := loadLiveGuardDataset(t)
	selectedPartIDs := liveGuardCooledPartIDs(dataset, liveGuardDefaultGrace)
	require.Equal(t, liveGuardExpectedCooledPartIDs, selectedPartIDs)
	selection := liveGuardSelection{
		name:                  "cooled_large_parts",
		selectedPartIDs:       selectedPartIDs,
		expectedSelectedTrace: 31832,
		expectedSplitTrace:    31,
		expectedSelectedBytes: 34856465,
	}
	for _, partID := range selection.selectedPartIDs {
		require.GreaterOrEqual(t, dataset.parts[partID].metadata.CompressedSizeBytes, liveGuardFixtureLargePartMinimumSize)
	}
	prepared := prepareLiveGuardSelection(t, dataset, selection)
	require.Len(t, prepared.outsideParts, 20)
	require.Equal(t, []string{"00000000000021d4"}, liveGuardCandidatePartIDs(&prepared, liveGuardDefaultGrace))

	mergeResult := runLiveMaturityAwareHybrid(&prepared, dataset.logicalNow)
	timeOnlyMetrics := runLiveTimeOnlyGuard(&prepared, liveGuardDefaultGrace)
	allBloomMetrics := runLiveAllBloomGuard(&prepared)
	candidatePairs := countLiveTimeCandidatePairs(&prepared, liveGuardDefaultGrace)

	require.True(t, mergeResult.samplingApplied)
	require.Equal(t, len(prepared.traces), mergeResult.sampledTraces)
	require.Equal(t, len(prepared.traces), mergeResult.guardedDrops)
	requireLiveGuardResult(t, liveGuardExpectedResult{
		candidatePairs: 6652,
		retained:       34,
		dropped:        31798,
		falseDeferral:  3,
		bloomProbes:    6652,
	}, candidatePairs, mergeResult.guardMetrics)
	requireLiveGuardResult(t, liveGuardExpectedResult{
		candidatePairs: 6652,
		retained:       6652,
		dropped:        25180,
		falseDeferral:  6621,
	}, candidatePairs, timeOnlyMetrics)
	requireLiveGuardResult(t, liveGuardExpectedResult{
		retained:      900,
		dropped:       30932,
		falseDeferral: 869,
		bloomProbes:   629241,
	}, 0, allBloomMetrics)
	t.Logf(
		"selected_parts=%d selected_bytes=%d traces=%d outside_parts=%d hybrid_2h={pairs:%d retained:%d dropped:%d "+
			"false_deferral:%d probes:%d} time_only={retained:%d dropped:%d false_deferral:%d} "+
			"all_bloom={retained:%d dropped:%d false_deferral:%d probes:%d}",
		len(selection.selectedPartIDs), prepared.selectedBytes, len(prepared.traces), len(prepared.outsideParts),
		candidatePairs, mergeResult.guardMetrics.retained, mergeResult.guardMetrics.dropped,
		mergeResult.guardMetrics.falseDeferral, mergeResult.guardMetrics.bloomProbes,
		timeOnlyMetrics.retained, timeOnlyMetrics.dropped, timeOnlyMetrics.falseDeferral,
		allBloomMetrics.retained, allBloomMetrics.dropped, allBloomMetrics.falseDeferral, allBloomMetrics.bloomProbes,
	)
}

func TestFragmentGuardLiveShardMixedMaturitySelections(t *testing.T) {
	dataset := loadLiveGuardDataset(t)
	frontier := dataset.logicalNow - liveGuardDefaultGrace
	for selectionIdx := range liveGuardHotSelections {
		selection := liveGuardHotSelections[selectionIdx]
		t.Run(selection.name, func(t *testing.T) {
			prepared := prepareLiveGuardSelection(t, dataset, selection)
			require.Greater(t, prepared.selectedMaxTimestamp, frontier)
			matureTraces := 0
			for traceIdx := range prepared.traces {
				if prepared.traces[traceIdx].maxTS <= frontier {
					matureTraces++
				}
			}
			mergeResult := runLiveMaturityAwareHybrid(&prepared, dataset.logicalNow)
			require.Equal(t, matureTraces > 0, mergeResult.samplingApplied)
			require.Equal(t, matureTraces, mergeResult.sampledTraces)
			require.Equal(t, matureTraces, mergeResult.guardedDrops)
			require.Equal(t, len(prepared.traces), mergeResult.guardMetrics.retained+mergeResult.guardMetrics.dropped)
			if selection.name == "cooled_large_plus_recent_small" {
				require.Positive(t, matureTraces)
				require.Less(t, matureTraces, len(prepared.traces))
			}
			if selection.name == "recent_small_parts" {
				for _, partID := range selection.selectedPartIDs {
					require.Less(t, dataset.parts[partID].metadata.CompressedSizeBytes, liveGuardFixtureLargePartMinimumSize)
				}
			}
		})
	}
}

func BenchmarkFragmentGuardLiveShard(b *testing.B) {
	dataset := loadLiveGuardDataset(b)
	cooledPartIDs := liveGuardCooledPartIDs(dataset, liveGuardDefaultGrace)
	cooledSelection := liveGuardSelection{
		name:                  "cooled_large_parts",
		selectedPartIDs:       cooledPartIDs,
		expectedSelectedTrace: 31832,
		expectedSplitTrace:    31,
		expectedSelectedBytes: 34856465,
	}
	cooledPrepared := prepareLiveGuardSelection(b, dataset, cooledSelection)
	b.Run(cooledSelection.name, func(b *testing.B) {
		b.Run("hybrid_2h", func(b *testing.B) {
			benchmarkLiveGuard(b, &cooledPrepared, func(preparedSelection *preparedLiveGuardSelection) liveGuardMetrics {
				return runLiveMaturityAwareHybrid(preparedSelection, dataset.logicalNow).guardMetrics
			})
		})
		b.Run("all_bloom", func(b *testing.B) {
			benchmarkLiveGuard(b, &cooledPrepared, runLiveAllBloomGuard)
		})
		b.Run("time_only_2h", func(b *testing.B) {
			benchmarkLiveGuard(b, &cooledPrepared, func(preparedSelection *preparedLiveGuardSelection) liveGuardMetrics {
				return runLiveTimeOnlyGuard(preparedSelection, liveGuardDefaultGrace)
			})
		})
	})

	hotSelection := liveGuardHotSelections[0]
	hotPrepared := prepareLiveGuardSelection(b, dataset, hotSelection)
	b.Run("recent_small_parts/per_trace_maturity_2h", func(b *testing.B) {
		benchmarkLiveGuard(b, &hotPrepared, func(preparedSelection *preparedLiveGuardSelection) liveGuardMetrics {
			return runLiveMaturityAwareHybrid(preparedSelection, dataset.logicalNow).guardMetrics
		})
	})
}

func benchmarkLiveGuard(b *testing.B, prepared *preparedLiveGuardSelection,
	runGuard func(*preparedLiveGuardSelection) liveGuardMetrics,
) {
	b.Helper()
	expectedMetrics := runGuard(prepared)
	b.ReportAllocs()
	b.ResetTimer()
	checksum := 0
	for benchmarkIdx := 0; benchmarkIdx < b.N; benchmarkIdx++ {
		runMetrics := runGuard(prepared)
		checksum += runMetrics.retained + runMetrics.dropped
	}
	b.StopTimer()
	b.ReportMetric(float64(len(prepared.traces)), "traces/op")
	b.ReportMetric(float64(len(prepared.outsideParts)), "outside_parts/op")
	b.ReportMetric(float64(prepared.selectedBytes), "selected_bytes/op")
	b.ReportMetric(float64(expectedMetrics.candidateParts), "candidate_parts/op")
	b.ReportMetric(float64(expectedMetrics.bloomProbes), "probes/op")
	runtime.KeepAlive(checksum)
}
