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
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/filter"
)

type fragmentGuardPrototypePart struct {
	traceIDFilter *filter.BloomFilter
	minTimestamp  int64
	maxTimestamp  int64
}

type fragmentGuardPrototypeResult struct {
	candidateParts int
	bloomProbes    int
	canDrop        bool
}

func prototypeConfirmTraceDrop(traceID string, minTimestamp, maxTimestamp, grace int64,
	outsideParts []fragmentGuardPrototypePart,
) fragmentGuardPrototypeResult {
	return prototypeConfirmTraceDropBytes([]byte(traceID), minTimestamp, maxTimestamp, grace, outsideParts)
}

func prototypeConfirmTraceDropBytes(traceID []byte, minTimestamp, maxTimestamp, grace int64,
	outsideParts []fragmentGuardPrototypePart,
) fragmentGuardPrototypeResult {
	result := fragmentGuardPrototypeResult{}
	if len(traceID) == 0 || minTimestamp > maxTimestamp || grace < 0 {
		return result
	}

	guardMin := prototypeSaturatingSub(minTimestamp, grace)
	guardMax := prototypeSaturatingAdd(maxTimestamp, grace)
	for partIdx := range outsideParts {
		outsidePart := &outsideParts[partIdx]
		if outsidePart.minTimestamp > outsidePart.maxTimestamp {
			return result
		}
		if outsidePart.maxTimestamp < guardMin || outsidePart.minTimestamp > guardMax {
			continue
		}

		result.candidateParts++
		if outsidePart.traceIDFilter == nil {
			return result
		}
		result.bloomProbes++
		if outsidePart.traceIDFilter.MightContain(traceID) {
			return result
		}
	}
	result.canDrop = true
	return result
}

func prototypeApplySamplerVerdict(samplerKeeps bool, traceID string, minTimestamp, maxTimestamp, grace int64,
	outsideParts []fragmentGuardPrototypePart,
) fragmentGuardPrototypeResult {
	if samplerKeeps {
		return fragmentGuardPrototypeResult{}
	}
	return prototypeConfirmTraceDrop(traceID, minTimestamp, maxTimestamp, grace, outsideParts)
}

func prototypeSaturatingSub(value, delta int64) int64 {
	if delta > 0 && value < math.MinInt64+delta {
		return math.MinInt64
	}
	return value - delta
}

func prototypeSaturatingAdd(value, delta int64) int64 {
	if delta > 0 && value > math.MaxInt64-delta {
		return math.MaxInt64
	}
	return value + delta
}

func prototypePart(minTimestamp, maxTimestamp int64, traceIDs ...string) fragmentGuardPrototypePart {
	traceIDFilter := filter.NewBloomFilter(max(len(traceIDs), 1))
	for _, traceID := range traceIDs {
		traceIDFilter.Add([]byte(traceID))
	}
	return fragmentGuardPrototypePart{
		traceIDFilter: traceIDFilter,
		minTimestamp:  minTimestamp,
		maxTimestamp:  maxTimestamp,
	}
}

func TestFragmentGuardPrototypeDecisionTable(t *testing.T) {
	const (
		traceID = "trace-a"
		grace   = int64(10)
	)
	testCases := []struct {
		name             string
		traceID          string
		outsideParts     []fragmentGuardPrototypePart
		minTimestamp     int64
		maxTimestamp     int64
		wantCanDrop      bool
		wantCandidateCnt int
		wantProbeCnt     int
	}{
		{
			name:         "no time candidate",
			traceID:      traceID,
			minTimestamp: 100,
			maxTimestamp: 110,
			outsideParts: []fragmentGuardPrototypePart{
				prototypePart(50, 80, traceID),
				prototypePart(131, 150, traceID),
			},
			wantCanDrop: true,
		},
		{
			name:         "earlier fragment at grace boundary",
			traceID:      traceID,
			minTimestamp: 100,
			maxTimestamp: 110,
			outsideParts: []fragmentGuardPrototypePart{
				prototypePart(80, 90, traceID),
			},
			wantCandidateCnt: 1,
			wantProbeCnt:     1,
		},
		{
			name:         "later fragment at grace boundary",
			traceID:      traceID,
			minTimestamp: 100,
			maxTimestamp: 110,
			outsideParts: []fragmentGuardPrototypePart{
				prototypePart(120, 130, traceID),
			},
			wantCandidateCnt: 1,
			wantProbeCnt:     1,
		},
		{
			name:         "time candidate with negative bloom filter",
			traceID:      traceID,
			minTimestamp: 100,
			maxTimestamp: 110,
			outsideParts: []fragmentGuardPrototypePart{
				prototypePart(105, 106),
			},
			wantCanDrop:      true,
			wantCandidateCnt: 1,
			wantProbeCnt:     1,
		},
		{
			name:         "missing candidate bloom filter fails open",
			traceID:      traceID,
			minTimestamp: 100,
			maxTimestamp: 110,
			outsideParts: []fragmentGuardPrototypePart{
				{minTimestamp: 105, maxTimestamp: 106},
			},
			wantCandidateCnt: 1,
		},
		{
			name:         "invalid outside range fails open",
			traceID:      traceID,
			minTimestamp: 100,
			maxTimestamp: 110,
			outsideParts: []fragmentGuardPrototypePart{
				prototypePart(20, 10),
			},
		},
		{
			name:         "invalid selected range fails open",
			traceID:      traceID,
			minTimestamp: 110,
			maxTimestamp: 100,
		},
		{
			name:         "empty trace id fails open",
			minTimestamp: 100,
			maxTimestamp: 110,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			result := prototypeConfirmTraceDrop(
				testCase.traceID,
				testCase.minTimestamp,
				testCase.maxTimestamp,
				grace,
				testCase.outsideParts,
			)
			require.Equal(t, testCase.wantCanDrop, result.canDrop)
			require.Equal(t, testCase.wantCandidateCnt, result.candidateParts)
			require.Equal(t, testCase.wantProbeCnt, result.bloomProbes)
		})
	}
}

func TestFragmentGuardPrototypeSamplerKeepBypassesGuard(t *testing.T) {
	result := prototypeApplySamplerVerdict(true, "trace-a", 100, 110, 10, []fragmentGuardPrototypePart{
		prototypePart(100, 110, "trace-a"),
	})

	require.False(t, result.canDrop)
	require.Zero(t, result.candidateParts)
	require.Zero(t, result.bloomProbes)
}

func TestFragmentGuardPrototypeNegativeGraceFailsOpen(t *testing.T) {
	result := prototypeConfirmTraceDrop("trace-a", 100, 110, -1, []fragmentGuardPrototypePart{
		prototypePart(100, 110),
	})

	require.False(t, result.canDrop)
	require.Zero(t, result.candidateParts)
	require.Zero(t, result.bloomProbes)
}

func TestFragmentGuardPrototypeExhaustiveGapBound(t *testing.T) {
	const (
		traceID       = "trace-a"
		firstTS       = int64(100)
		fragmentGap   = int64(10)
		fragmentCount = 8
	)

	for selectedMask := 1; selectedMask < (1<<fragmentCount)-1; selectedMask++ {
		selectedMin := int64(math.MaxInt64)
		selectedMax := int64(math.MinInt64)
		outsideParts := make([]fragmentGuardPrototypePart, 0, fragmentCount)
		for fragmentIdx := 0; fragmentIdx < fragmentCount; fragmentIdx++ {
			fragmentTimestamp := firstTS + int64(fragmentIdx)*fragmentGap
			if selectedMask&(1<<fragmentIdx) != 0 {
				selectedMin = min(selectedMin, fragmentTimestamp)
				selectedMax = max(selectedMax, fragmentTimestamp)
				continue
			}
			outsideParts = append(outsideParts, prototypePart(fragmentTimestamp, fragmentTimestamp, traceID))
		}

		result := prototypeConfirmTraceDrop(traceID, selectedMin, selectedMax, fragmentGap, outsideParts)
		require.Falsef(t, result.canDrop, "selection mask %08b lost a visible trace fragment", selectedMask)
		require.Positive(t, result.candidateParts)
		require.Positive(t, result.bloomProbes)
	}
}

func TestFragmentGuardPrototypeAgainstExactOracle(t *testing.T) {
	const (
		traceID    = "trace-a"
		iterations = 1000
	)
	// A fixed pseudo-random stream makes this property regression reproducible.
	randomGenerator := rand.New(rand.NewSource(1)) //nolint:gosec
	for iteration := 0; iteration < iterations; iteration++ {
		selectedMin := int64(randomGenerator.Intn(400) - 200)
		selectedMax := selectedMin + int64(randomGenerator.Intn(50))
		grace := int64(randomGenerator.Intn(30))
		partCount := randomGenerator.Intn(20)
		outsideParts := make([]fragmentGuardPrototypePart, 0, partCount)
		exactPresence := make([]bool, 0, partCount)
		for partIdx := 0; partIdx < partCount; partIdx++ {
			partMin := int64(randomGenerator.Intn(500) - 250)
			partMax := partMin + int64(randomGenerator.Intn(50))
			containsTrace := randomGenerator.Intn(5) == 0
			partTraceIDs := []string{fmt.Sprintf("other-%d-%d", iteration, partIdx)}
			if containsTrace {
				partTraceIDs = append(partTraceIDs, traceID)
			}
			outsideParts = append(outsideParts, prototypePart(partMin, partMax, partTraceIDs...))
			exactPresence = append(exactPresence, containsTrace)
		}

		result := prototypeConfirmTraceDrop(traceID, selectedMin, selectedMax, grace, outsideParts)
		if !result.canDrop {
			continue
		}
		guardMin := prototypeSaturatingSub(selectedMin, grace)
		guardMax := prototypeSaturatingAdd(selectedMax, grace)
		for partIdx := range outsideParts {
			outsidePart := outsideParts[partIdx]
			isCandidate := outsidePart.maxTimestamp >= guardMin && outsidePart.minTimestamp <= guardMax
			require.Falsef(t, isCandidate && exactPresence[partIdx],
				"iteration %d dropped a trace present in candidate part %d", iteration, partIdx)
		}
	}
}

func TestFragmentGuardPrototypePrunesBloomProbes(t *testing.T) {
	const (
		traceID   = "trace-a"
		partCount = 100
	)
	outsideParts := make([]fragmentGuardPrototypePart, 0, partCount)
	for partIdx := 0; partIdx < partCount; partIdx++ {
		partTimestamp := int64(partIdx * 100)
		outsideParts = append(outsideParts, prototypePart(partTimestamp, partTimestamp, fmt.Sprintf("other-%d", partIdx)))
	}

	result := prototypeConfirmTraceDrop(traceID, 4_995, 5_005, 10, outsideParts)
	require.True(t, result.canDrop)
	require.Equal(t, 1, result.candidateParts)
	require.Equal(t, 1, result.bloomProbes)
	require.Less(t, result.bloomProbes, len(outsideParts))
}

func TestFragmentGuardPrototypeDocumentsUnboundedGap(t *testing.T) {
	const traceID = "trace-a"
	result := prototypeConfirmTraceDrop(traceID, 100, 100, 10, []fragmentGuardPrototypePart{
		prototypePart(111, 111, traceID),
	})

	require.True(t, result.canDrop)
	require.Zero(t, result.candidateParts)
	require.Zero(t, result.bloomProbes)
}

func TestFragmentGuardPrototypeSaturatesTimeRange(t *testing.T) {
	const traceID = "trace-a"
	result := prototypeConfirmTraceDrop(traceID, math.MinInt64+1, math.MaxInt64-1, 10, []fragmentGuardPrototypePart{
		prototypePart(math.MinInt64, math.MinInt64, traceID),
	})

	require.False(t, result.canDrop)
	require.Equal(t, 1, result.candidateParts)
	require.Equal(t, 1, result.bloomProbes)
}
