// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses
// this file to You under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License. You may
// obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package native

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func tinyCandidates(n int) []mergeCandidate {
	candidates := make([]mergeCandidate, 0, n)
	for i := 0; i < n; i++ {
		candidates = append(candidates, mergeCandidate{fullCount: 1, liveCount: 1})
	}
	return candidates
}

func TestPlanMergesNoopBelowOneSegment(t *testing.T) {
	require.Nil(t, planMerges(nil, defaultMergePlanOptions))
	require.Nil(t, planMerges(tinyCandidates(1), defaultMergePlanOptions))
}

func TestPlanMergesLeavesAlreadyBudgetedSegmentCountUntouched(t *testing.T) {
	// One segment already at the floor size is its own single-segment tier:
	// nothing should be scheduled.
	require.Empty(t, planMerges([]mergeCandidate{{liveCount: defaultMergePlanOptions.floorCount}}, defaultMergePlanOptions))
}

func TestPlanMergesAggressivelyConsolidatesTinySegments(t *testing.T) {
	// Many single-document segments (the OAP schema-registry preload
	// pattern) sit far below one tier's floor size, so the budget for that
	// little data is a single segment: all of them should land in one task
	// instead of being left to accumulate.
	tasks := planMerges(tinyCandidates(5), defaultMergePlanOptions)
	require.Len(t, tasks, 1)
	require.Len(t, tasks[0].candidates, 5)
}

func TestPlanMergesBoundsTaskSizeAndRetiresLargeSegments(t *testing.T) {
	opts := defaultMergePlanOptions
	opts.segmentsPerMergeTask = 10
	opts.maxSegmentsPerTier = 10

	candidates := tinyCandidates(10005)
	// One already-huge segment must never be proposed for merging again.
	candidates = append(candidates, mergeCandidate{fullCount: opts.maxLiveCount, liveCount: opts.maxLiveCount})

	tasks := planMerges(candidates, opts)
	require.NotEmpty(t, tasks, "a 10000+ segment backlog must be scheduled for merging")
	for _, task := range tasks {
		require.LessOrEqual(t, len(task.candidates), opts.segmentsPerMergeTask, "a single merge task must stay bounded in size")
		for _, candidate := range task.candidates {
			require.Less(t, candidate.liveCount, opts.maxLiveCount, "an already-huge segment must never be re-merged")
		}
	}
}

func TestPlanMergesSelectsSmallestSegmentsFirst(t *testing.T) {
	opts := defaultMergePlanOptions
	opts.segmentsPerMergeTask = 3
	opts.maxSegmentsPerTier = 2

	candidates := []mergeCandidate{
		{liveCount: 1000},
		{liveCount: 1},
		{liveCount: 2},
		{liveCount: 3},
		{liveCount: 4},
		{liveCount: 5},
	}
	tasks := planMerges(candidates, opts)
	require.NotEmpty(t, tasks)
	for _, candidate := range tasks[0].candidates {
		require.Less(t, candidate.liveCount, int64(1000), "the largest outlier segment should not be in the first (smallest-first) task")
	}
}

func TestCalcMergeBudgetStaysLogarithmic(t *testing.T) {
	opts := defaultMergePlanOptions
	small := calcMergeBudget(1_000, opts.floorCount, opts)
	large := calcMergeBudget(1_000_000, opts.floorCount, opts)
	require.Positive(t, small)
	require.Positive(t, large)
	// A thousand-fold increase in total size must not produce a thousand-fold
	// increase in the segment budget -- that is the entire point of tiering.
	require.Less(t, large, small*20)
}
