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
	for _, asc := range []bool{true, false} {
		for _, window := range [][2]int64{{0, 400}, {50, 120}, {210, 260}} {
			name := "desc"
			if asc {
				name = "asc"
			}
			t.Run(name, func(t *testing.T) {
				var older, newer fxPart
				for sid := common.SeriesID(1); sid <= 40; sid++ {
					older = append(older, fxRange{sid: sid, from: 1, to: 240, version: 1})
					newer = append(newer, fxRange{sid: sid, from: 200, to: 300, version: 2})
				}
				fx := newFixture(t, older, newer)
				q := prefetchQuery(window[0], window[1], true, asc)

				reference, _, _, closeReference := reuseTestResult(t.Context(), t, fx, q, 64<<20)
				reference.budget.sliceNeed, reference.budget.sliceBudget = 0, 0
				want := drainRows(t, reference)
				require.NotNil(t, reference.scan)
				require.Zero(t, reference.scan.stats.slices)
				closeReference()

				sliced, tracker, _, closeSliced := reuseTestResult(t.Context(), t, fx, q, 64<<20)
				// A budget that holds a few slices' worth of cursors, so the unit is cut.
				sliced.budget.sliceNeed, sliced.budget.sliceBudget = 2<<20, 2<<20
				got := drainRows(t, sliced)
				require.NotNil(t, sliced.scan)
				if window[1]-window[0] > 100 {
					require.Positive(t, sliced.scan.stats.slices, "the budget should have cut the merge")
				}
				require.Equal(t, want, got)
				closeSliced()
				require.Zero(t, tracker.Used(), "slice cursors released their reservations")
			})
		}
	}
}

// sliceTestCursor is a cursor planned from metadata only: count rows over [lo, hi] in a block of
// encoded bytes.
func sliceTestCursor(lo, hi int64, count, encoded uint64) *blockCursor {
	bc := &blockCursor{bm: blockMetadata{count: count, uncompressedSizeBytes: encoded}}
	bc.bm.timestamps.min, bc.bm.timestamps.max = lo, hi
	return bc
}

// A unit whose maxMergeSlices slices miss the slice target but fit the whole allowance is cut
// into maxMergeSlices slices rather than merged whole, which would exceed the allowance.
func TestSliceWidthFallsBackToMaxSlices(t *testing.T) {
	const perRow, lo, hi = 100, 0, 6399
	cursors := []*blockCursor{sliceTestCursor(lo, hi, 6400, 1<<20), sliceTestCursor(lo, hi, 6400, 1<<20)}
	minWidth := int64((hi - lo + 1) / maxMergeSlices)
	fits := int64(sliceCost(cursors, perRow, minWidth))
	require.Greater(t, sliceCost(cursors, perRow, hi-lo+1), uint64(fits), "the whole unit exceeds the allowance")
	require.Equal(t, minWidth, sliceWidth(cursors, perRow, fits, fits-1, lo, hi),
		"maxMergeSlices slices fit the allowance, not the slice target")
	require.Zero(t, sliceWidth(cursors, perRow, fits-1, fits-1, lo, hi), "maxMergeSlices slices fit neither")
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
