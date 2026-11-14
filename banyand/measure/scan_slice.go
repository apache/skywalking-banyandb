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

package measure

import (
	"context"
	"math"

	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// Time-sliced merge.
//
// A time-ordered scan merges every series whose blocks overlap in time. A block cannot be
// decoded in part, so a merge over blocks that are much wider than the query window (the
// blocks of a merged part span hours) opens every series at once and keeps the in-range rows
// of all of them live. When that does not fit the query's budget, the merge unit is cut into
// consecutive time slices. Each slice merges the rows of its own time range from fresh
// cursors, releases them, then the next slice starts. A block that spans several slices is
// decoded once per slice, which is the price of not keeping its rows.
//
// The slice width is derived from block metadata and the budget, not configured: it is the
// widest slice for which the cursors it opens fit the slice budget. When even maxMergeSlices
// slices miss that budget but fit the whole allowance, the unit is cut into maxMergeSlices
// slices. A unit that fits whole, which is every narrow query, is not sliced and runs exactly
// as before.

const (
	// maxMergeSlices bounds the repeated decoding of one block.
	maxMergeSlices = 64
	// sliceBudgetShare is the part of the query's allowance one slice may keep live, the rest
	// being left to the output pages, the prefetch and the operators above the scan.
	sliceBudgetShare = 2
)

// scanSliceBudget derives, from the query's allowances, what a merge unit may keep live before it
// is cut into slices (need) and what one slice may keep live once it is (target). Both are 0 when
// there is no allowance to derive them from. A unit is sliced only when it would not fit the
// whole allowance: slicing costs repeated decodes, so a merge that fits runs as it always did.
func scanSliceBudget(ctx context.Context, tracker *vectorized.MemoryTracker, workspace int64) (need, target int64) {
	limit := tracker.Limit()
	if lease, ok := query.BudgetLeaseFromContext(ctx); ok && lease.Limit() > 0 && int64(lease.Limit()) < limit {
		limit = int64(lease.Limit())
	}
	if limit <= workspace {
		return 0, 0
	}
	return limit - workspace, (limit - workspace) / sliceBudgetShare
}

// sliceCost is the memory the cursors of unit keep live while a slice of width ns merges, in
// the worst case where every cursor overlaps it.
func sliceCost(cursors []*blockCursor, perRow uint64, width int64) uint64 {
	var total uint64
	for _, bc := range cursors {
		span := bc.bm.timestamps.max - bc.bm.timestamps.min + 1
		rows := bc.bm.count
		if width < span && span > 0 {
			rows = min(rows, uint64(math.Ceil(float64(bc.bm.count)*float64(width)/float64(span))))
		}
		cost := sharedRetainedBytes(perRow, rows, bc.bm.count, bc.bm.uncompressedSizeBytes)
		if cost < 0 || total > math.MaxUint64-uint64(cost) {
			return math.MaxUint64
		}
		total += uint64(cost)
	}
	return total
}

// sliceWidth returns the width in nanoseconds of the slices a unit over [lo, hi] is cut into,
// or 0 when it should not be sliced: it already fits, or slicing cannot make it fit. The width
// is the widest whose cost fits the slice budget; when even maxMergeSlices slices miss that
// but fit need, the whole allowance, it is the width of maxMergeSlices slices.
func sliceWidth(cursors []*blockCursor, perRow uint64, need, budget, lo, hi int64) int64 {
	span := hi - lo + 1
	if budget <= 0 || span <= 1 || len(cursors) < 2 {
		return 0
	}
	if sliceCost(cursors, perRow, span) <= uint64(need) {
		return 0
	}
	minWidth := (span + maxMergeSlices - 1) / maxMergeSlices
	if cost := sliceCost(cursors, perRow, minWidth); cost > uint64(budget) {
		if cost <= uint64(need) {
			return minWidth
		}
		return 0
	}
	good, bad := minWidth, span
	for bad-good > 1 {
		mid := good + (bad-good)/2
		if sliceCost(cursors, perRow, mid) <= uint64(budget) {
			good = mid
		} else {
			bad = mid
		}
	}
	return good
}

// cloneForSlice returns a fresh cursor over the same block that reads only [lo, hi].
func cloneForSlice(bc *blockCursor, lo, hi int64) *blockCursor {
	c := generateBlockCursor()
	c.p = bc.p
	c.bm.copyFrom(&bc.bm)
	c.minTimestamp, c.maxTimestamp = lo, hi
	c.tagProjection = bc.tagProjection
	c.fieldProjection = bc.fieldProjection
	c.schemaTagTypes = bc.schemaTagTypes
	c.shardID = bc.shardID
	return c
}

// sliceMergeUnit cuts a merge unit into time slices when it does not fit the slice budget.
// It returns the units that replace it, in visiting order, or nil to keep the unit as is.
// The cursors of the replaced unit are released.
func sliceMergeUnit(unit *scanUnit, budget *scanBudget, desc bool, stats *scanStats) []scanUnit {
	if budget == nil || budget.shared == nil || len(unit.windows) < 2 {
		return nil
	}
	var cursors []*blockCursor
	for i := range unit.windows {
		cursors = append(cursors, unit.windows[i].cursors...)
	}
	if len(cursors) < 2 {
		return nil
	}
	lo, hi := cursors[0].minTimestamp, cursors[0].maxTimestamp
	first, last := int64(math.MaxInt64), int64(math.MinInt64)
	for _, bc := range cursors {
		first, last = min(first, bc.bm.timestamps.min), max(last, bc.bm.timestamps.max)
	}
	lo, hi = max(lo, first), min(hi, last)
	width := sliceWidth(cursors, budget.perRowBytes(), budget.sliceNeed, budget.sliceBudget, lo, hi)
	if width == 0 {
		return nil
	}
	var slices [][2]int64
	for from := lo; from <= hi; {
		to := hi
		if hi-from >= width {
			to = from + width - 1
		}
		slices = append(slices, [2]int64{from, to})
		if to == hi {
			break
		}
		from = to + 1
	}
	if desc {
		for i, j := 0, len(slices)-1; i < j; i, j = i+1, j-1 {
			slices[i], slices[j] = slices[j], slices[i]
		}
	}
	units := make([]scanUnit, 0, len(slices))
	for _, sl := range slices {
		var windows []seriesWindow
		for i := range unit.windows {
			w := &unit.windows[i]
			var clones []*blockCursor
			for _, bc := range w.cursors {
				if bc.bm.timestamps.max < sl[0] || bc.bm.timestamps.min > sl[1] {
					continue
				}
				clones = append(clones, cloneForSlice(bc, sl[0], sl[1]))
			}
			if len(clones) > 0 {
				windows = append(windows, seriesWindow{
					cursors: clones, sid: w.sid,
					minTS: max(w.minTS, sl[0]), maxTS: min(w.maxTS, sl[1]),
				})
			}
		}
		if len(windows) > 0 {
			units = append(units, scanUnit{windows: windows})
		}
	}
	for i := range unit.windows {
		releaseCursors(unit.windows[i].cursors)
		unit.windows[i].cursors = nil
	}
	stats.slicedUnits++
	stats.slices += int64(len(units))
	return units
}
