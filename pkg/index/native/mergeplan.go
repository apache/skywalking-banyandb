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
	"math"
	"sort"
)

// mergeCandidate is one segment as seen by the merge planner. fullCount is
// the segment's physical document count; liveCount excludes documents
// already masked by deletion.
type mergeCandidate struct {
	segment   rootSegment
	fullCount int64
	liveCount int64
}

// mergeTask names the segments one Compact call should combine into a single
// replacement segment.
type mergeTask struct {
	candidates []mergeCandidate
}

// mergePlanOptions configures planMerges. It is a native-engine adaptation of
// Lucene's TieredMergePolicy (as described in
// http://blog.mikemccandless.com/2011/02/visualizing-lucenes-segment-merges.html
// and implemented by this project's legacy engine's merge planner): segments
// are merged in bounded
// batches of comparable size instead of all at once, and a segment is
// retired from further merging once it crosses maxLiveCount. That keeps any
// single merge's cost proportional to its own tier rather than to the whole
// index, so total merge work over the life of an index that only ever grows
// is amortized O(log n) instead of O(n) per merge.
type mergePlanOptions struct {
	// maxSegmentsPerTier bounds how many segments of a given rough size may
	// coexist before they are merged down; a smaller value merges more
	// eagerly and keeps fewer live segments.
	maxSegmentsPerTier int
	// maxLiveCount is the live document count above which a segment is
	// permanently excluded from further merge consideration. Without this
	// ceiling, the largest segment would be re-merged with every batch of
	// newly admitted segments, making every merge cost proportional to the
	// entire index instead of to one tier.
	maxLiveCount int64
	// tierGrowth is the growth factor between successive tiers in the
	// logarithmic budget computed by calcMergeBudget.
	tierGrowth float64
	// segmentsPerMergeTask bounds how many segments a single mergeTask may
	// combine, which bounds the cost of any one merge.
	segmentsPerMergeTask int
	// floorCount rounds small segments up to this live count for budget and
	// tiering purposes, so that a long tail of tiny segments does not distort
	// tier placement.
	floorCount int64
}

// defaultMergePlanOptions mirrors the legacy engine's default merge plan, adjusted
// for native's typically much smaller per-document segments (a single
// Property or log entry, rather than a full trace span's worth of fields).
var defaultMergePlanOptions = mergePlanOptions{
	maxSegmentsPerTier:   10,
	maxLiveCount:         1 << 20,
	tierGrowth:           10.0,
	segmentsPerMergeTask: 10,
	floorCount:           64,
}

// planMerges computes zero or more mergeTasks for the given segments. A
// segment absent from every task should remain unmerged. The result is
// deterministic for a given input order and option set.
func planMerges(candidates []mergeCandidate, o mergePlanOptions) []mergeTask {
	if len(candidates) <= 1 {
		return nil
	}

	eligible := make([]mergeCandidate, 0, len(candidates))
	var minLive int64 = math.MaxInt64
	var eligibleLiveTotal int64
	for _, candidate := range candidates {
		if candidate.liveCount < minLive {
			minLive = candidate.liveCount
		}
		// Only small-enough segments are eligible; an already-large segment
		// is left alone so it is never re-merged once it reaches its tier
		// ceiling.
		if candidate.liveCount < o.maxLiveCount/2 {
			eligible = append(eligible, candidate)
			eligibleLiveTotal += candidate.liveCount
		}
	}
	if len(eligible) <= 1 {
		return nil
	}
	if minLive < o.floorCount {
		minLive = o.floorCount
	}

	budget := calcMergeBudget(eligibleLiveTotal, minLive, o)

	// Smaller segments merge first: sorting ascending and always taking the
	// front of the remaining eligible list groups comparably-sized segments
	// into the same task, which is what keeps individual merges cheap.
	sort.Slice(eligible, func(i, j int) bool { return eligible[i].liveCount < eligible[j].liveCount })

	var tasks []mergeTask
	for len(eligible) > 0 && len(eligible)+len(tasks) > budget {
		take := o.segmentsPerMergeTask
		if take > len(eligible) {
			take = len(eligible)
		}
		if take <= 1 {
			break
		}
		tasks = append(tasks, mergeTask{candidates: append([]mergeCandidate(nil), eligible[:take]...)})
		eligible = eligible[take:]
	}
	return tasks
}

// calcMergeBudget computes how many segments would be needed to cover
// totalSize by climbing a logarithmically growing staircase of segment
// tiers, exactly mirroring the legacy engine's merge budget. Ported rather
// than imported: pkg/index/native must not depend on the legacy
// engine (see native_dependency_test.go).
func calcMergeBudget(totalSize, firstTierSize int64, o mergePlanOptions) int {
	tierSize := firstTierSize
	if tierSize < 1 {
		tierSize = 1
	}
	maxSegmentsPerTier := o.maxSegmentsPerTier
	if maxSegmentsPerTier < 1 {
		maxSegmentsPerTier = 1
	}
	tierGrowth := o.tierGrowth
	if tierGrowth < 1 {
		tierGrowth = 1
	}

	var budget int
	for totalSize > 0 {
		segmentsInTier := float64(totalSize) / float64(tierSize)
		if segmentsInTier < float64(maxSegmentsPerTier) {
			budget += int(math.Ceil(segmentsInTier))
			break
		}
		budget += maxSegmentsPerTier
		totalSize -= int64(maxSegmentsPerTier) * tierSize
		tierSize = int64(float64(tierSize) * tierGrowth)
	}
	return budget
}
