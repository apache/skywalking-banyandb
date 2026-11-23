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
	"fmt"
	"math"
	"unsafe"

	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
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

// sliceCost is the memory a slice of width ns needs while it merges, in the worst case where
// every cursor overlaps it: what each cursor keeps once decoded, plus the transient expansion of
// the largest block, which a cursor's reservation holds (sharedBlockBytes) until its decode is
// trued up while the cursors decoded before it keep what they hold.
func sliceCost(cursors []*blockCursor, perRow uint64, width int64) uint64 {
	var total, largest uint64
	for _, bc := range cursors {
		largest = max(largest, bc.bm.uncompressedSizeBytes)
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
	if largest > (math.MaxUint64-total)/decodeTransientFactor {
		return math.MaxUint64
	}
	return total + largest*decodeTransientFactor
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

// The heap a slice clone takes, derived from what cloneForSlice allocates: the cursor itself, and
// the block metadata blockMetadata.copyFrom copies into fresh maps and slices (the tag types by
// family, a dataBlock per tag family, the field column metadata). The strings and projections it
// shares with the original are not counted. Maps are Go's swiss tables: a header, then groups of
// mapGroupSlots slots behind a control word, filled to at most 7/8 once a map outgrows one group.
const (
	mapHeaderBytes = 48
	mapTableBytes  = 64
	mapGroupSlots  = 8
	// slicePlanPointerBytes is the clone's entry in the slice's cursor list.
	slicePlanPointerBytes = 8
)

var (
	cursorStructBytes   = int64(unsafe.Sizeof(blockCursor{}))
	dataBlockBytes      = int64(unsafe.Sizeof(dataBlock{}))
	columnMetadataBytes = int64(unsafe.Sizeof(columnMetadata{}))
	familyTypesSlot     = int64(unsafe.Sizeof(struct {
		v map[string]pbv1.ValueType
		k string
	}{}))
	tagTypeSlot = int64(unsafe.Sizeof(struct {
		k string
		v pbv1.ValueType
	}{}))
	familyBlockSlot = int64(unsafe.Sizeof(struct {
		v *dataBlock
		k string
	}{}))
)

// allocBytes rounds an allocation of n bytes up to a size the Go allocator hands out: a multiple
// of 32 up to 512 bytes, the next power of two above, which is never below the real size class.
func allocBytes(n int64) int64 {
	if n <= 0 {
		return 0
	}
	if n <= 512 {
		return (n + 31) &^ 31
	}
	size := int64(1024)
	for size < n {
		size <<= 1
	}
	return size
}

// mapBytes is the heap of a map of n entries whose key and value take slot bytes.
func mapBytes(n int, slot int64) int64 {
	groups := int64(1)
	if n > mapGroupSlots {
		slots := int64(1)
		for slots*7 < int64(n)*8 {
			slots <<= 1
		}
		groups = slots / mapGroupSlots
	}
	total := mapHeaderBytes + allocBytes(groups*(8+mapGroupSlots*slot))
	if n > mapGroupSlots {
		total += mapTableBytes
	}
	return total
}

// sliceCloneBytes is the heap one slice clone of bc takes until it is released.
func sliceCloneBytes(bc *blockCursor) int64 {
	n := allocBytes(cursorStructBytes) + slicePlanPointerBytes
	if bc.bm.tagType != nil {
		n += mapBytes(len(bc.bm.tagType), familyTypesSlot)
		for _, tags := range bc.bm.tagType {
			n += mapBytes(len(tags), tagTypeSlot)
		}
	}
	if families := len(bc.bm.tagFamilies); families > 0 {
		n += mapBytes(families, familyBlockSlot) + int64(families)*allocBytes(dataBlockBytes)
	}
	return n + allocBytes(int64(len(bc.bm.field.columnMetadata))*columnMetadataBytes)
}

// sliceMergeUnit cuts a merge unit into time slices when it does not fit the slice budget.
// It returns the units that replace it, in visiting order, or nil to keep the unit as is.
// The cursors of the replaced unit are released.
//
// Every slice is cloned up front, so the clones of all slices are live at once: each clone is
// charged its sliceCloneBytes to the query's tracker, and the charge goes when the clone is
// released. A unit whose clones do not fit is not sliced: whole it does not fit either, so the
// query fails, as it did before time slicing, with the tracker's exhaustion error, recorded as a
// rejection. The unit then keeps its cursors.
func sliceMergeUnit(unit *scanUnit, budget *scanBudget, desc bool, stats *scanStats) ([]scanUnit, error) {
	if budget == nil || budget.shared == nil || len(unit.windows) < 2 {
		return nil, nil
	}
	var cursors []*blockCursor
	for i := range unit.windows {
		cursors = append(cursors, unit.windows[i].cursors...)
	}
	if len(cursors) < 2 {
		return nil, nil
	}
	lo, hi := cursors[0].minTimestamp, cursors[0].maxTimestamp
	first, last := int64(math.MaxInt64), int64(math.MinInt64)
	for _, bc := range cursors {
		first, last = min(first, bc.bm.timestamps.min), max(last, bc.bm.timestamps.max)
	}
	lo, hi = max(lo, first), min(hi, last)
	width := sliceWidth(cursors, budget.perRowBytes(), budget.sliceNeed, budget.sliceBudget, lo, hi)
	if width == 0 {
		return nil, nil
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
	overlaps := func(bc *blockCursor, sl [2]int64) bool {
		return bc.bm.timestamps.max >= sl[0] && bc.bm.timestamps.min <= sl[1]
	}
	cloneBytes := make(map[*blockCursor]int64, len(cursors))
	var charge, clones int64
	for _, bc := range cursors {
		size := sliceCloneBytes(bc)
		cloneBytes[bc] = size
		for _, sl := range slices {
			if overlaps(bc, sl) {
				charge += size
				clones++
			}
		}
	}
	if err := budget.shared.Reserve(charge); err != nil {
		return nil, budget.rejected(fmt.Errorf("cannot cut a merge of %d cursors into %d time slices: their %d clones need %d bytes: %w",
			len(cursors), len(slices), clones, charge, err), rejectOther)
	}
	units := make([]scanUnit, 0, len(slices))
	for _, sl := range slices {
		var windows []seriesWindow
		for i := range unit.windows {
			w := &unit.windows[i]
			var clones []*blockCursor
			for _, bc := range w.cursors {
				if !overlaps(bc, sl) {
					continue
				}
				c := cloneForSlice(bc, sl[0], sl[1])
				c.sliceTracker, c.sliceCharge = budget.shared, cloneBytes[bc]
				clones = append(clones, c)
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
	return units, nil
}
