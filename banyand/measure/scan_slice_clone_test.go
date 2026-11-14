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
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// cloneTestUnit is a merge unit of series windows of one block each, every block spanning [0, span)
// with rows rows and encoded bytes, its metadata naming one tag of one family.
func cloneTestUnit(series int, span int64, rows, encoded uint64) scanUnit {
	unit := scanUnit{windows: make([]seriesWindow, 0, series)}
	for i := 0; i < series; i++ {
		bc := generateBlockCursor()
		bc.bm.seriesID = common.SeriesID(i + 1)
		bc.bm.count = rows
		bc.bm.uncompressedSizeBytes = encoded
		bc.bm.timestamps.min = 0
		bc.bm.timestamps.max = span - 1
		bc.bm.tagType = tagType{"default": {"entity_id": pbv1.ValueTypeStr}}
		bc.bm.tagFamilies = map[string]*dataBlock{"default": {}}
		bc.minTimestamp, bc.maxTimestamp = 0, span-1
		unit.windows = append(unit.windows, seriesWindow{
			cursors: []*blockCursor{bc}, sid: common.SeriesID(i + 1), minTS: 0, maxTS: span - 1,
		})
	}
	return unit
}

func releaseUnits(units []scanUnit) {
	for i := range units {
		for j := range units[i].windows {
			releaseCursors(units[i].windows[j].cursors)
		}
	}
}

// A merge over many series whose blocks span the whole range would be cut into slices whose clones
// all live at once and need several times the query's allowance. Slicing is refused instead: the
// clones are never made, the query fails with the tracker's exhaustion, which the query reports as
// RESOURCE_EXHAUSTED, and the refusal is recorded as a rejection.
func TestSliceClonesStayWithinQueryAllowance(t *testing.T) {
	verifyNoScanLeaks(t)
	const (
		limit   = int64(32 << 20)
		series  = 2200
		span    = int64(1024) * 1_000_000_000
		rows    = 1024
		encoded = 64 << 10
	)
	ctx := vectorized.WithQueryMemoryTracker(context.Background(), limit)
	budget := newScanBudget(-1, nil)
	require.NoError(t, budget.attachShared(ctx, 3, vectorized.DefaultBatchSize, series))
	defer budget.releaseWorkspace()
	budget.rowBytes = 48
	unit := cloneTestUnit(series, span, rows, encoded)
	cursors := make([]*blockCursor, 0, series)
	for i := range unit.windows {
		cursors = append(cursors, unit.windows[i].cursors...)
	}
	width := sliceWidth(cursors, budget.perRowBytes(), budget.sliceNeed, budget.sliceBudget, 0, span-1)
	require.Positive(t, width, "precondition: the unit does not fit whole and is to be sliced")
	slices := (span + width - 1) / width
	need := int64(series) * slices * sliceCloneBytes(cursors[0])
	require.Greater(t, need, limit, "precondition: the clones of every slice exceed the allowance")

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	usedBefore := budget.shared.Used()
	var stats scanStats
	units, err := sliceUnits([]scanUnit{unit}, budget, false, &stats)
	runtime.GC()
	runtime.ReadMemStats(&after)
	defer releaseUnits(units)
	growth := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	if growth > limit {
		t.Fatalf("slicing allocated %d bytes outside the budget (tracker charge grew by %d), above the query's whole allowance of %d bytes",
			growth, budget.shared.Used()-usedBefore, limit)
	}
	require.Error(t, err, "slicing is refused")
	require.ErrorIs(t, err, vectorized.ErrMemoryBudgetExceeded)
	require.Equal(t, modelv1.Status_STATUS_RESOURCE_EXHAUSTED, protector.QueryFailure(err, "query").Status())
	require.Contains(t, err.Error(), fmt.Sprintf("into %d time slices", slices))
	require.Equal(t, usedBefore, budget.shared.Used(), "nothing stays charged")
	require.Zero(t, stats.slicedUnits)
	require.Len(t, units, 1, "the unit is kept whole")
	for i := range units[0].windows {
		require.Equal(t, cursors[i:i+1], units[0].windows[i].cursors, "with its own cursors")
		require.EqualValues(t, 1, cursors[i].refs.Load())
	}
	counts, last := budget.telemetry().rejectSummary()
	require.Equal(t, rejectSharedTracker+"=1", counts)
	require.Equal(t, err.Error(), last)
}

// A unit whose clones fit is sliced. Every clone is charged to the query's tracker, and through it
// to the lease, from the moment it is made until it is released.
func TestSliceClonesAreChargedUntilReleased(t *testing.T) {
	verifyNoScanLeaks(t)
	const (
		limit   = int64(32 << 20)
		series  = 40
		span    = int64(1024) * 1_000_000_000
		rows    = 1024
		encoded = 1 << 20
	)
	lease := &liveScanLease{scanReuseLease{limit: uint64(limit)}}
	ctx := vectorized.WithLiveLeaseAccounting(query.WithBudgetLease(t.Context(), lease))
	ctx = vectorized.WithQueryMemoryTracker(ctx, limit)
	budget := newScanBudget(-1, nil)
	require.NoError(t, budget.attachShared(ctx, 3, vectorized.DefaultBatchSize, series))
	defer budget.releaseWorkspace()
	budget.rowBytes = 48
	unit := cloneTestUnit(series, span, rows, encoded)
	per := sliceCloneBytes(unit.windows[0].cursors[0])
	usedBefore, leaseBefore := budget.shared.Used(), lease.used.Load()

	var stats scanStats
	units, err := sliceUnits([]scanUnit{unit}, budget, false, &stats)
	defer releaseUnits(units)
	require.NoError(t, err)
	require.Greater(t, len(units), 1, "precondition: the unit is sliced")
	clones := 0
	for i := range units {
		for _, w := range units[i].windows {
			for _, bc := range w.cursors {
				require.Equal(t, per, bc.sliceCharge)
				require.Same(t, budget.shared, bc.sliceTracker)
				clones++
			}
		}
	}
	require.Equal(t, series*len(units), clones, "every block overlaps every slice")
	require.Equal(t, int64(clones)*per, budget.shared.Used()-usedBefore, "the clones are charged")
	require.Equal(t, uint64(clones)*uint64(per), lease.used.Load()-leaseBefore, "and so is the lease")

	// The first slice's clones are released when its merge is done; the others stay charged.
	first := 0
	for _, w := range units[0].windows {
		first += len(w.cursors)
	}
	releaseUnits(units[:1])
	require.Equal(t, int64(clones-first)*per, budget.shared.Used()-usedBefore)
	releaseUnits(units[1:])
	require.Equal(t, usedBefore, budget.shared.Used(), "releasing the clones releases their charge")
	require.Equal(t, leaseBefore, lease.used.Load())
}

// A time-ordered scan whose merge is to be sliced but whose clones do not fit what is left of the
// allowance fails at its first batch, without slicing, with the refusal recorded on the scan.
func TestScanFailsWhenSliceClonesDoNotFit(t *testing.T) {
	var part fxPart
	for sid := common.SeriesID(1); sid <= 40; sid++ {
		part = append(part, fxRange{sid: sid, from: 1, to: 240, version: 1})
	}
	fx := newFixture(t, part)
	q := prefetchQuery(0, 400, true, true)
	result, tracker, _, closeResult := reuseTestResult(t.Context(), t, fx, q, 128<<20)
	result.budget.sliceNeed, result.budget.sliceBudget = 2<<20, 2<<20
	// Leave the scan only a few clones' worth of its allowance.
	filler := tracker.Limit() - tracker.Used() - 4<<10
	require.NoError(t, tracker.Reserve(filler))

	_, err := drainRowsErr(result)
	require.ErrorIs(t, err, vectorized.ErrMemoryBudgetExceeded)
	require.Contains(t, err.Error(), "time slices")
	require.NotNil(t, result.scan)
	require.Zero(t, result.scan.stats.slices, "the merge is not sliced")
	counts, last := result.budget.telemetry().rejectSummary()
	require.Equal(t, rejectSharedTracker+"=1", counts)
	require.Equal(t, err.Error(), last)
	closeResult()
	tracker.Release(filler)
	require.Zero(t, tracker.Used())
}

// sliceCloneBytes is derived from the structures cloneForSlice allocates. It must cover what a
// clone really takes on the heap, without overstating it so much that slicing is refused needlessly.
func TestSliceCloneBytesCoversTheHeapOfAClone(t *testing.T) {
	verifyNoScanLeaks(t)
	for _, shape := range []struct{ families, tags, fields int }{
		{1, 1, 0}, {1, 1, 1}, {1, 8, 0}, {1, 9, 0}, {2, 4, 3}, {3, 10, 5}, {1, 20, 2}, {9, 1, 0},
	} {
		t.Run(fmt.Sprintf("%d-families-%d-tags-%d-fields", shape.families, shape.tags, shape.fields), func(t *testing.T) {
			src := &blockCursor{}
			src.bm.tagType = tagType{}
			src.bm.tagFamilies = map[string]*dataBlock{}
			for f := 0; f < shape.families; f++ {
				family := fmt.Sprintf("family%d", f)
				src.bm.tagType[family] = map[string]pbv1.ValueType{}
				for g := 0; g < shape.tags; g++ {
					src.bm.tagType[family][fmt.Sprintf("tag%d", g)] = pbv1.ValueTypeStr
				}
				src.bm.tagFamilies[family] = &dataBlock{}
			}
			src.bm.field.columnMetadata = make([]columnMetadata, shape.fields)
			const n = 20000
			// The least growth of a few rounds: other allocations only add to a round.
			measured := int64(-1)
			for range 3 {
				var before, after runtime.MemStats
				// Two collections empty the cursor pool, so every clone is allocated afresh.
				runtime.GC()
				runtime.GC()
				runtime.ReadMemStats(&before)
				// A slice's cursor list keeps one pointer per clone, as this one does.
				clones := make([]*blockCursor, n)
				for i := range clones {
					clones[i] = cloneForSlice(src, 0, 1)
				}
				runtime.GC()
				runtime.ReadMemStats(&after)
				growth := (int64(after.HeapAlloc) - int64(before.HeapAlloc)) / n
				if measured < 0 || growth < measured {
					measured = growth
				}
				releaseCursors(clones)
			}
			estimate := sliceCloneBytes(src)
			t.Logf("charged %d bytes per clone, measured %d", estimate, measured)
			require.GreaterOrEqual(t, estimate, measured, "the charge covers a clone")
			require.LessOrEqual(t, estimate, measured*3/2, "the charge does not overstate a clone")
		})
	}
}
