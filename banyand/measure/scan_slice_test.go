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
	"os"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func TestTimeSlicedMergeMatchesUnslicedMerge(t *testing.T) {
	var older, newer fxPart
	for sid := common.SeriesID(1); sid <= 40; sid++ {
		older = append(older, fxRange{sid: sid, from: 1, to: 240, version: 1})
		newer = append(newer, fxRange{sid: sid, from: 200, to: 300, version: 2})
	}
	fx := newFixture(t, older, newer)
	for _, asc := range []bool{true, false} {
		for _, tc := range []struct {
			window [2]int64
			sliced bool
		}{
			{window: [2]int64{0, 400}, sliced: true},
			// Only the older blocks are in range, and their in-range rows fit: the merge is left whole.
			{window: [2]int64{50, 120}, sliced: false},
			{window: [2]int64{210, 260}, sliced: true},
		} {
			window := tc.window
			order := "desc"
			if asc {
				order = "asc"
			}
			t.Run(fmt.Sprintf("%s/%d-%d", order, window[0], window[1]), func(t *testing.T) {
				q := prefetchQuery(window[0], window[1], true, asc)

				reference, _, _, closeReference := reuseTestResult(t.Context(), t, fx, q, 64<<20)
				reference.budget.sliceNeed, reference.budget.sliceBudget = 0, 0
				want := drainRows(t, reference)
				require.NotNil(t, reference.scan)
				require.Zero(t, reference.scan.stats.slices)
				closeReference()

				// The test lease is charged every decode and never refunded, and each slice decodes the
				// blocks it overlaps again, so the sliced run needs more of it than the reference.
				sliced, tracker, _, closeSliced := reuseTestResult(t.Context(), t, fx, q, 128<<20)
				// A budget that holds a few slices' worth of cursors, so the unit is cut.
				sliced.budget.sliceNeed, sliced.budget.sliceBudget = 2<<20, 2<<20
				got := drainRows(t, sliced)
				require.NotNil(t, sliced.scan)
				require.Equal(t, tc.sliced, sliced.scan.stats.slices > 0, "slices: %d", sliced.scan.stats.slices)
				require.Equal(t, want, got)
				closeSliced()
				require.Zero(t, tracker.Used(), "slice cursors released their reservations")
			})
		}
	}
}

// sliceTestCursor is a cursor planned from metadata only: one row per nanosecond of [lo, hi] in a
// 1 MiB block.
func sliceTestCursor(lo, hi int64) *blockCursor {
	bc := &blockCursor{bm: blockMetadata{count: uint64(hi - lo + 1), uncompressedSizeBytes: 1 << 20}}
	bc.bm.timestamps.min, bc.bm.timestamps.max = lo, hi
	return bc
}

// A unit whose maxMergeSlices slices miss the slice target but fit the whole allowance is cut
// into maxMergeSlices slices rather than merged whole, which would exceed the allowance.
func TestSliceWidthFallsBackToMaxSlices(t *testing.T) {
	const perRow, lo, hi = 100, 0, 6399
	cursors := []*blockCursor{sliceTestCursor(lo, hi), sliceTestCursor(lo, hi)}
	minWidth := int64((hi - lo + 1) / maxMergeSlices)
	fits := int64(sliceCost(cursors, perRow, minWidth))
	require.Greater(t, sliceCost(cursors, perRow, hi-lo+1), uint64(fits), "the whole unit exceeds the allowance")
	require.Equal(t, minWidth, sliceWidth(cursors, perRow, fits, fits-1, lo, hi),
		"maxMergeSlices slices fit the allowance, not the slice target")
	require.Zero(t, sliceWidth(cursors, perRow, fits-1, fits-1, lo, hi), "maxMergeSlices slices fit neither")
}

// A merge reserves each cursor's pre-decode envelope, which holds the decode's transient
// expansion until the decode trues it up, while the cursors before it keep what they hold. A
// unit is left whole only when that fits; otherwise every slice it is cut into must fit.
func TestSlicedOrWholeMergeFitsItsDecodeReservations(t *testing.T) {
	verifyNoScanLeaks(t)
	const encoded = 23 << 10
	newUnit := func() scanUnit {
		var unit scanUnit
		for sid := common.SeriesID(1); sid <= 2; sid++ {
			bc := generateBlockCursor()
			bc.bm.seriesID, bc.bm.count, bc.bm.uncompressedSizeBytes = sid, 360, encoded
			bc.bm.timestamps.min, bc.bm.timestamps.max = 0, 359
			bc.minTimestamp, bc.maxTimestamp = 0, 359
			unit.windows = append(unit.windows, seriesWindow{cursors: []*blockCursor{bc}, sid: sid, minTS: 0, maxTS: 359})
		}
		return unit
	}
	b := &scanBudget{rowBytes: 48, tel: new(scanTelemetry)}
	perRow := b.perRowBytes()
	// The allowance is what the two cursors keep once decoded: no room for a decode's expansion.
	b.sliceNeed = 2 * sharedRetainedBytes(perRow, 360, 360, encoded)
	b.sliceBudget = b.sliceNeed / sliceBudgetShare
	b.shared = vectorized.NewMemoryTracker(b.sliceNeed)
	unit := newUnit()
	var stats scanStats
	units := sliceMergeUnit(&unit, b, false, &stats)
	if units == nil {
		units = []scanUnit{unit}
	}
	for _, u := range units {
		var held int64
		for _, w := range u.windows {
			for _, bc := range w.cursors {
				before := b.sharedBlockBytes(bc)
				require.NoErrorf(t, b.shared.Reserve(before), "%d slices: a cursor of [%d, %d] cannot be reserved before its decode",
					len(units), bc.minTimestamp, bc.maxTimestamp)
				kept := sharedRetainedBytes(perRow, inRangeRows(bc), bc.bm.count, bc.bm.uncompressedSizeBytes)
				b.shared.Release(before - kept)
				held += kept
			}
			releaseCursors(w.cursors)
		}
		b.shared.Release(held)
	}
}

func TestScanSliceBudget(t *testing.T) {
	const limit, workspace = 64 << 20, 4 << 20
	tracker := vectorized.NewMemoryTracker(limit)
	withLease := func(leaseLimit uint64) context.Context {
		return query.WithBudgetLease(t.Context(), &scanReuseLease{limit: leaseLimit})
	}
	for _, tt := range []struct {
		ctx              context.Context
		name             string
		workspace        int64
		wantNeed, wantTo int64
	}{
		{name: "the tracker's limit", ctx: t.Context(), workspace: workspace, wantNeed: limit - workspace, wantTo: (limit - workspace) / 2},
		{name: "a smaller lease", ctx: withLease(16 << 20), workspace: workspace, wantNeed: 12 << 20, wantTo: 6 << 20},
		{name: "a larger lease", ctx: withLease(128 << 20), workspace: workspace, wantNeed: limit - workspace, wantTo: (limit - workspace) / 2},
		{name: "a lease without a limit", ctx: withLease(0), workspace: workspace, wantNeed: limit - workspace, wantTo: (limit - workspace) / 2},
		{name: "the workspace takes it all", ctx: t.Context(), workspace: limit},
		{name: "the workspace takes the lease", ctx: withLease(2 << 20), workspace: workspace},
	} {
		need, target := scanSliceBudget(tt.ctx, tracker, tt.workspace)
		require.Equal(t, tt.wantNeed, need, tt.name)
		require.Equal(t, tt.wantTo, target, tt.name)
	}
}

func TestSliceWidth(t *testing.T) {
	const perRow, lo, hi = 100, 1000, 7399
	const span = hi - lo + 1
	cursors := []*blockCursor{sliceTestCursor(lo, hi), sliceTestCursor(lo, hi)}
	cost := func(width int64) int64 { return int64(sliceCost(cursors, perRow, width)) }
	whole := cost(span)
	require.Zero(t, sliceWidth(cursors[:1], perRow, 1, 1, lo, hi), "one cursor")
	require.Zero(t, sliceWidth(cursors, perRow, 1, 0, lo, hi), "no slice budget")
	require.Zero(t, sliceWidth(cursors, perRow, 1, 1, lo, lo), "a one-nanosecond span")
	require.Zero(t, sliceWidth(cursors, perRow, whole, whole/2, lo, hi), "the unit fits the allowance")
	minWidth := int64(span / maxMergeSlices)
	require.Zero(t, sliceWidth(cursors, perRow, cost(minWidth)-1, cost(minWidth)-1, lo, hi), "no width fits")

	for _, budget := range []int64{cost(minWidth), cost(span / 8), cost(span/2) - 1} {
		w := sliceWidth(cursors, perRow, whole-1, budget, lo, hi)
		require.GreaterOrEqual(t, w, minWidth)
		require.Less(t, w, int64(span))
		require.LessOrEqual(t, cost(w), budget, "the width fits the slice budget")
		require.Greater(t, cost(w+1), budget, "the width is the widest that fits")
	}
}

func TestSliceMergeUnit(t *testing.T) {
	verifyNoScanLeaks(t)
	const lo, hi = 0, 6399
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 64<<20)
	budget := newScanBudget(64<<20, nil)
	require.NoError(t, budget.attachShared(ctx, 6, 1024, 2))
	defer budget.releaseWorkspace()
	newUnit := func(series int) (scanUnit, []*blockCursor) {
		var unit scanUnit
		var cursors []*blockCursor
		for sid := common.SeriesID(1); sid <= common.SeriesID(series); sid++ {
			bc := generateBlockCursor()
			bc.bm.seriesID, bc.bm.count, bc.bm.uncompressedSizeBytes = sid, 6400, 1<<20
			bc.bm.timestamps.min, bc.bm.timestamps.max = lo, hi
			bc.minTimestamp, bc.maxTimestamp = lo, hi
			unit.windows = append(unit.windows, seriesWindow{cursors: []*blockCursor{bc}, sid: sid, minTS: lo, maxTS: hi})
			cursors = append(cursors, bc)
		}
		return unit, cursors
	}

	unit, cursors := newUnit(1)
	var stats scanStats
	require.Nil(t, sliceMergeUnit(&unit, budget, false, &stats), "a unit of one window is not sliced")
	require.Equal(t, cursors, unit.windows[0].cursors, "and keeps its cursors")
	require.EqualValues(t, 1, cursors[0].refs.Load())
	releaseCursors(cursors)

	for _, desc := range []bool{false, true} {
		unit, cursors = newUnit(2)
		perRow := budget.perRowBytes()
		budget.sliceNeed = int64(sliceCost(cursors, perRow, 1000))
		budget.sliceBudget = budget.sliceNeed
		width := sliceWidth(cursors, perRow, budget.sliceNeed, budget.sliceBudget, lo, hi)
		require.Positive(t, width)
		stats = scanStats{}
		units := sliceMergeUnit(&unit, budget, desc, &stats)
		require.Len(t, units, int((hi-lo+width)/width), "desc=%v", desc)
		require.EqualValues(t, 1, stats.slicedUnits)
		require.EqualValues(t, len(units), stats.slices)
		for _, bc := range cursors {
			require.Zero(t, bc.refs.Load(), "the replaced unit's cursors are released")
		}
		for _, w := range unit.windows {
			require.Nil(t, w.cursors)
		}
		// The slices cover [lo, hi] in order, without gaps or overlaps, and are visited from the
		// newest for a descending scan.
		next := int64(lo)
		for i := range units {
			u := units[i]
			if desc {
				u = units[len(units)-1-i]
			}
			require.Len(t, u.windows, 2)
			from, to := u.windows[0].minTS, u.windows[0].maxTS
			require.Equal(t, next, from, "desc=%v slice %d", desc, i)
			require.LessOrEqual(t, to-from+1, width)
			for _, w := range u.windows {
				require.Equal(t, from, w.minTS)
				require.Equal(t, to, w.maxTS)
				require.Len(t, w.cursors, 1)
				require.Equal(t, w.sid, w.cursors[0].bm.seriesID)
				require.Equal(t, from, w.cursors[0].minTimestamp)
				require.Equal(t, to, w.cursors[0].maxTimestamp)
			}
			next = to + 1
		}
		require.EqualValues(t, hi+1, next)
		for _, u := range units {
			for _, w := range u.windows {
				releaseCursors(w.cursors)
			}
		}
	}
}

// liveScanLease is a lease that takes refunds, as the lease of a data node scan does.
type liveScanLease struct{ scanReuseLease }

func (l *liveScanLease) Refund(bytes uint64) {
	for {
		current := l.used.Load()
		if l.used.CompareAndSwap(current, current-min(bytes, current)) {
			return
		}
	}
}

func (l *liveScanLease) Owner() any { return l }

// liveResult is reuseTestResult with live lease accounting.
func liveResult(t *testing.T, fx *fxFixture, q *fxQuery, limit uint64) (*queryResult, func()) {
	t.Helper()
	lease := &liveScanLease{scanReuseLease{limit: limit}}
	ctx := vectorized.WithLiveLeaseAccounting(query.WithBudgetLease(t.Context(), lease))
	ctx = vectorized.WithQueryMemoryTracker(ctx, int64(limit))
	result := fx.newResult(t, q, q.schema(t))
	result.ctx = ctx
	// The raw per-cursor quota of a data node is the protector's available bytes, not the query limit.
	result.budget = newScanBudget(1<<30, result.dp)
	require.NoError(t, result.budget.attachShared(ctx, len(result.batchSchema.Columns), 1024, len(result.data)))
	return result, sync.OnceFunc(result.Release)
}

// TestMeasureTimeSlicedMerge prints, for a time-ordered scan of many series whose blocks share one
// time range, the peak live heap, the CPU time and the number of block decodes with and without
// slicing, at the production 96 MiB query limit. It needs BYDB_SLICE_MEASURE=1.
func TestMeasureTimeSlicedMerge(t *testing.T) {
	if os.Getenv("BYDB_SLICE_MEASURE") != "1" {
		t.Skip("set BYDB_SLICE_MEASURE=1")
	}
	const rows = 240
	prev := debug.SetGCPercent(20)
	defer debug.SetGCPercent(prev)
	fmt.Printf("\n%7s %-9s %7s %7s %9s %9s %10s %9s\n", "series", "mode", "slices", "decodes", "peakHeap", "cpu", "wall", "outcome")
	for _, series := range []int{100, 500, 1000, 2000, 4000} {
		var part fxPart
		for sid := common.SeriesID(1); sid <= common.SeriesID(series); sid++ {
			part = append(part, fxRange{sid: sid, from: 1, to: rows, version: 1})
		}
		fx := newFixture(t, part)
		q := prefetchQuery(1, rows, true, true)
		for _, mode := range []string{"unsliced", "sliced", "unsl-4GiB"} {
			limit := uint64(96 << 20)
			if mode == "unsl-4GiB" {
				limit = 4 << 30
			}
			result, closeResult := liveResult(t, fx, q, limit)
			if mode != "sliced" {
				result.budget.sliceNeed, result.budget.sliceBudget = 0, 0
			}
			base := settledHeap()
			var peak atomic.Int64
			stop := make(chan struct{})
			done := make(chan struct{})
			go func() {
				defer close(done)
				var m runtime.MemStats
				for {
					select {
					case <-stop:
						return
					default:
					}
					runtime.ReadMemStats(&m)
					if h := int64(m.HeapAlloc); h > peak.Load() {
						peak.Store(h)
					}
					time.Sleep(2 * time.Millisecond)
				}
			}()
			var before, after syscall.Rusage
			_ = syscall.Getrusage(syscall.RUSAGE_SELF, &before)
			start := time.Now()
			n, err := drainRowsErr(result)
			wall := time.Since(start)
			_ = syscall.Getrusage(syscall.RUSAGE_SELF, &after)
			close(stop)
			<-done
			cpu := time.Duration(after.Utime.Nano()-before.Utime.Nano()) + time.Duration(after.Stime.Nano()-before.Stime.Nano())
			outcome := fmt.Sprintf("ok %d rows", len(n))
			if err != nil {
				outcome = "FAIL: " + err.Error()
				if len(outcome) > 110 {
					outcome = outcome[:110]
				}
			}
			var slices, decodes int64
			if result.scan != nil {
				slices, decodes = result.scan.stats.slices, result.scan.stats.mergeCursors
			}
			fmt.Printf("%7d %-9s %7d %7d %7.1fMiB %9v %10v %s\n", series, mode, slices, decodes, float64(peak.Load()-base)/1048576,
				cpu.Round(time.Millisecond), wall.Round(time.Millisecond), outcome)
			closeResult()
		}
	}
}
