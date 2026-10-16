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
	"math/rand"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
)

func metaCursor(sid common.SeriesID, minTS, maxTS int64) *blockCursor {
	bc := &blockCursor{}
	bc.bm.seriesID = sid
	bc.bm.timestamps.min = minTS
	bc.bm.timestamps.max = maxTS
	return bc
}

func segmentBounds(segments []scanSegment) [][][2]int64 {
	var out [][][2]int64
	for _, seg := range segments {
		var one [][2]int64
		for _, bc := range seg.cursors {
			one = append(one, [2]int64{bc.bm.timestamps.min, bc.bm.timestamps.max})
		}
		out = append(out, one)
	}
	return out
}

func TestPlanSegments(t *testing.T) {
	verifyNoScanLeaks(t)
	tests := []struct {
		name    string
		blocks  [][2]int64
		want    [][][2]int64
		wantRev [][][2]int64
	}{
		{
			name:   "one block",
			blocks: [][2]int64{{1, 10}},
			want:   [][][2]int64{{{1, 10}}},
		},
		{
			name:   "disjoint blocks are one segment each, whatever order they arrive in",
			blocks: [][2]int64{{21, 30}, {1, 10}, {11, 20}},
			want:   [][][2]int64{{{1, 10}}, {{11, 20}}, {{21, 30}}},
		},
		{
			name:   "overlapping blocks share a segment",
			blocks: [][2]int64{{1, 10}, {5, 15}, {20, 30}},
			want:   [][][2]int64{{{1, 10}, {5, 15}}, {{20, 30}}},
		},
		{
			name:   "an equal boundary timestamp counts as overlap",
			blocks: [][2]int64{{1, 10}, {10, 20}},
			want:   [][][2]int64{{{1, 10}, {10, 20}}},
		},
		{
			name:   "one past the boundary does not",
			blocks: [][2]int64{{1, 10}, {11, 20}},
			want:   [][][2]int64{{{1, 10}}, {{11, 20}}},
		},
		{
			name:   "a long block keeps swallowing the blocks inside it and the one it touches",
			blocks: [][2]int64{{1, 100}, {10, 20}, {30, 40}, {100, 120}, {121, 130}},
			want:   [][][2]int64{{{1, 100}, {10, 20}, {30, 40}, {100, 120}}, {{121, 130}}},
		},
		{
			name:   "chained overlap forms one segment",
			blocks: [][2]int64{{1, 10}, {8, 18}, {17, 27}},
			want:   [][][2]int64{{{1, 10}, {8, 18}, {17, 27}}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var cursors []*blockCursor
			for _, b := range tt.blocks {
				cursors = append(cursors, metaCursor(1, b[0], b[1]))
			}
			require.Equal(t, tt.want, segmentBounds(planSegments(cursors, false)))
			rev := slices.Clone(tt.want)
			slices.Reverse(rev)
			require.Equal(t, rev, segmentBounds(planSegments(cursors, true)), "a descending scan visits the latest segment first")
		})
	}
}

func TestPlanSegmentsKeepsArrivalOrderForEqualStarts(t *testing.T) {
	verifyNoScanLeaks(t)
	a, b := metaCursor(1, 5, 9), metaCursor(1, 5, 7)
	segments := planSegments([]*blockCursor{a, b}, false)
	require.Len(t, segments, 1)
	require.Same(t, a, segments[0].cursors[0])
	require.Same(t, b, segments[0].cursors[1])
}

func unitSIDs(units []scanUnit) [][]common.SeriesID {
	out := make([][]common.SeriesID, len(units))
	for i := range units {
		for j := range units[i].windows {
			out[i] = append(out[i], units[i].windows[j].sid)
		}
	}
	return out
}

func TestPlanUnits(t *testing.T) {
	verifyNoScanLeaks(t)
	cursors := func() []*blockCursor {
		return []*blockCursor{
			metaCursor(1, 1, 10), metaCursor(2, 20, 30), metaCursor(3, 40, 50), metaCursor(3, 60, 70),
		}
	}
	t.Run("without a time order every window is a unit in the requested series order", func(t *testing.T) {
		qr := &queryResult{sidToIndex: map[common.SeriesID]int{3: 0, 1: 1, 2: 2}}
		units := planUnits(qr, groupWindows(cursors()), false)
		require.Equal(t, [][]common.SeriesID{{3}, {1}, {2}}, unitSIDs(units))
		require.Len(t, units[0].windows[0].cursors, 2, "both blocks of series 3 are one window")
	})
	t.Run("a time order over disjoint series follows time", func(t *testing.T) {
		units := planUnits(&queryResult{orderByTS: true, ascTS: true}, groupWindows(cursors()), false)
		require.Equal(t, [][]common.SeriesID{{1}, {2}, {3}}, unitSIDs(units))
		units = planUnits(&queryResult{orderByTS: true}, groupWindows(cursors()), true)
		require.Equal(t, [][]common.SeriesID{{3}, {2}, {1}}, unitSIDs(units))
	})
	t.Run("series whose ranges touch or overlap are one merge unit", func(t *testing.T) {
		for _, desc := range []bool{false, true} {
			qr := &queryResult{orderByTS: true, ascTS: !desc}
			units := planUnits(qr, groupWindows([]*blockCursor{metaCursor(1, 1, 10), metaCursor(2, 10, 20)}), desc)
			require.Len(t, units, 1, "series sharing a timestamp are ordered by series id there")
			require.Len(t, units[0].windows, 2)
			units = planUnits(qr, groupWindows([]*blockCursor{metaCursor(1, 1, 15), metaCursor(2, 10, 20)}), desc)
			require.Len(t, units, 1)
		}
	})
	t.Run("only the overlapping series merge, the others stay whole units in time order", func(t *testing.T) {
		data := func() []*blockCursor {
			return []*blockCursor{
				metaCursor(1, 1, 10), metaCursor(2, 5, 20), metaCursor(3, 18, 25), metaCursor(4, 100, 110), metaCursor(5, 200, 210), metaCursor(6, 205, 300),
			}
		}
		units := planUnits(&queryResult{orderByTS: true, ascTS: true}, groupWindows(data()), false)
		require.Equal(t, [][]common.SeriesID{{1, 2, 3}, {4}, {5, 6}}, unitSIDs(units))
		units = planUnits(&queryResult{orderByTS: true}, groupWindows(data()), true)
		require.Equal(t, [][]common.SeriesID{{6, 5}, {4}, {3, 2, 1}}, unitSIDs(units))
	})
	t.Run("a single window never needs the merge", func(t *testing.T) {
		units := planUnits(&queryResult{orderByTS: true}, groupWindows([]*blockCursor{metaCursor(1, 1, 10), metaCursor(1, 5, 20)}), true)
		require.Len(t, units, 1)
		require.Len(t, units[0].windows, 1)
	})
}

var (
	allTags   = []string{fxTagSvc, fxTagURL, fxTagN, fxTagN2, fxTagArr, fxTagIAr, fxTagBin, fxTagHid, fxTagGho}
	allFields = []string{fxFldInt, fxFldFlt, fxFldNul, fxFldStr, fxFldBin}
)

// mixedFixture exercises every shape the scan distinguishes. Part order is
// oldest first; versions rise with the part.
//
//	s1: three parts, disjoint time ranges            -> three fast segments
//	s2: overlapping ranges, duplicate timestamps, rising versions
//	s3: a few rows in two parts, overlapping          -> tiny
//	s4: > 8,192 rows in one part (several blocks), another part touching its end
//	s5: parts that meet at an equal boundary timestamp, the newer one has a higher version
//	s6: one part, one block
//	s7, s8: small single-part series
func mixedFixture(t testing.TB) *fxFixture {
	return newFixture(t,
		fxPart{{1, 1, 300, 1}, {2, 1, 300, 1}, {3, 1, 3, 1}, {4, 1, mixedTailEnd, 1}, {5, 10, 20, 1}},
		fxPart{{1, 301, 600, 2}, {2, 150, 450, 2}, {3, 2, 6, 2}, {4, mixedTailEnd - 10, mixedTailEnd + 10, 5}, {6, 1, 700, 2}, {5, 20, 30, 2}},
		fxPart{{1, 601, 900, 3}, {2, 400, 700, 1}, {7, 1, 50, 3}, {8, 100, 110, 3}},
	)
}

func TestScanWindowCountsFastAndOverlapSegments(t *testing.T) {
	verifyNoScanLeaks(t)
	fx := mixedFixture(t)
	q := &fxQuery{tags: allTags, fields: allFields, minTS: 0, maxTS: 30000}
	res := fx.run(t, q, false)
	require.NotNil(t, res.scan, "the window scan must handle a query ordered by series")
	// Windows: s1..s8 (every series in the fixture).
	require.EqualValues(t, 8, res.scan.windows)
	// Overlap windows by metadata: s2 (three overlapping parts), s3, s4 (the tail), s5.
	require.EqualValues(t, 4, res.scan.windowsOverlapped)
	// s1: 3 disjoint blocks, s4: its blocks before the tail, s6, s7, s8: fast.
	require.Positive(t, res.scan.segmentsFast)
	require.EqualValues(t, 4, res.scan.segmentsOverlap, "s2, s3, s4 tail and s5 are one overlap segment each")
	// Several small segments (s3, s5, s7, s8, ...) were copied together.
	require.Positive(t, res.scan.coalesced)
}

func TestScanWindowMatchesLegacyAcrossGeneratedQueries(t *testing.T) {
	verifyNoScanLeaks(t)
	fx := mixedFixture(t)
	rng := rand.New(rand.NewSource(20261013)) //nolint:gosec // a fixed seed makes the generated queries reproducible
	hidden := map[common.SeriesID]map[string]*modelv1.TagValue{
		1: {fxTagHid: strTagValue("hidden-1")},
		4: {fxTagHid: strTagValue("hidden-4")},
		6: {fxTagHid: strTagValue("hidden-6")},
	}
	projections := []struct {
		name   string
		tags   []string
		fields []string
	}{
		{"all", allTags, allFields},
		{"numeric", []string{fxTagN2, fxTagN}, []string{fxFldInt, fxFldFlt, fxFldNul}},
		{"strings and arrays", []string{fxTagSvc, fxTagURL, fxTagArr, fxTagIAr, fxTagBin}, []string{fxFldStr, fxFldBin}},
		{"fields only", nil, []string{fxFldInt}},
		{"tags only", []string{fxTagURL}, nil},
	}
	ranges := [][2]int64{{0, 30000}, {100, 650}, {299, 301}, {mixedTailEnd - 1000, mixedTailEnd + 5}, {700, 700}, {5000, 6000}}
	limits := [][2]uint32{{0, 0}, {0, 7}, {5, 50}, {300, 2000}, {0, 100000}}
	iters := 60
	// Under the race detector the random picks of projection, range and limit are
	// replaced by a pairwise cover of their values.
	var cover [][]int
	if raceEnabled {
		var combos [][]int
		for p := range projections {
			for r := range ranges {
				for l := range limits {
					combos = append(combos, []int{p, r, l})
				}
			}
		}
		cover = coverPairs(combos, nil, nil)
		iters = len(cover)
	}
	for iter := range iters {
		order := slices.Clone(fx.sids)
		rng.Shuffle(len(order), func(i, j int) { order[i], order[j] = order[j], order[i] })
		order = order[:3+rng.Intn(len(order)-2)]
		proj := projections[rng.Intn(len(projections))]
		tr := ranges[rng.Intn(len(ranges))]
		lim := limits[rng.Intn(len(limits))]
		if raceEnabled {
			proj, tr, lim = projections[cover[iter][0]], ranges[cover[iter][1]], limits[cover[iter][2]]
		}
		q := &fxQuery{
			order: order, tags: proj.tags, fields: proj.fields, minTS: tr[0], maxTS: tr[1],
			offset: lim[0], limit: lim[1], passthrough: rng.Intn(4) == 0,
		}
		if rng.Intn(2) == 0 && len(proj.tags) > 0 {
			q.stored = hidden
		}
		what := fmt.Sprintf("iter %d order=%v proj=%s range=%v limit=%v hidden=%v passthrough=%v", iter, order, proj.name, tr, lim, q.stored != nil, q.passthrough)
		t.Run(fmt.Sprintf("%d", iter), func(t *testing.T) {
			want := fx.run(t, q, true)
			got := fx.run(t, q, false)
			requireSameResult(t, want, got, what)
		})
	}
}

func TestScanWindowMatchesLegacyForTimeOrders(t *testing.T) {
	verifyNoScanLeaks(t)
	// Series with strictly disjoint time ranges: a time-ordered query is the
	// concatenation of whole series, so the window scan serves it.
	disjoint := newFixture(t,
		fxPart{{1, 1, 400, 1}, {2, 500, 900, 1}, {3, 1001, 1010, 1}},
		fxPart{{1, 300, 450, 2}, {4, 2000, 2400, 1}, {2, 901, 1000, 2}},
	)
	// Series sharing timestamps: the legacy output interleaves them.
	interleaved := mixedFixture(t)
	fxNames := []string{"disjoint", "interleaved"}
	fixtures := map[string]*fxFixture{"disjoint": disjoint, "interleaved": interleaved}
	lims := [][2]uint32{{0, 0}, {3, 40}, {0, 5}}
	// combos are [fixture, asc (0 = true), single (0 = false), limit] indexes.
	var combos [][]int
	for f := range fxNames {
		for a := range 2 {
			for s := range 2 {
				for l := range lims {
					combos = append(combos, []int{f, a, s, l})
				}
			}
		}
	}
	if raceEnabled {
		// The interleaved fixture returned unbounded is the costly shape: pair the
		// fixtures with the limits only as far as the other dimensions require.
		combos = coverPairs(combos, [][2]int{{1, 2}, {1, 3}, {2, 3}, {0, 1}, {0, 2}}, func(c []int) int {
			return 1 + 20*c[0]*(1-min(c[3], 1))
		})
	}
	for _, c := range combos {
		name, asc, single, lim := fxNames[c[0]], c[1] == 0, c[2] == 1, lims[c[3]]
		fx := fixtures[name]
		q := &fxQuery{tags: allTags, fields: allFields, minTS: 0, maxTS: 30000, orderByTS: true, ascTS: asc, offset: lim[0], limit: lim[1]}
		if single {
			q.order = []common.SeriesID{fx.sids[len(fx.sids)/2]}
		}
		what := fmt.Sprintf("%s asc=%v single=%v limit=%v", name, asc, single, lim)
		t.Run(what, func(t *testing.T) {
			want := fx.run(t, q, true)
			got := fx.run(t, q, false)
			requireSameResult(t, want, got, what)
			require.NotNil(t, got.scan, "%s: every time order is served by the window scan", what)
			if name == "interleaved" && !single {
				require.Positive(t, got.scan.mergeRows, "interleaving series are merged")
			}
		})
	}
}

func TestScanWindowDescendingEmitsStorageOrderWithAReversedSelection(t *testing.T) {
	verifyNoScanLeaks(t)
	fx := newFixture(t, fxPart{{1, 1, 600, 1}})
	q := &fxQuery{tags: []string{fxTagURL, fxTagN2}, fields: []string{fxFldInt}, minTS: 0, maxTS: 1000, orderByTS: true}
	schema := q.schema(t)
	qr := fx.newResult(t, q, schema)
	defer qr.Release()
	b, err := qr.PullBatch(context.Background())
	require.NoError(t, err)
	require.NotNil(t, b)
	require.Equal(t, 600, b.RowCount())
	require.Len(t, b.Selection, 600)
	for i, row := range b.Selection {
		require.Equal(t, 599-i, int(row), "selection lists the rows last to first")
	}
	for i, ts := range b.Timestamps {
		require.Equal(t, int64(i+1), ts, "the columns themselves stay in storage order")
	}
	require.True(t, b.MetadataBorrowed(), "timestamps and versions are lent, not copied")
	b.Release()
	next, err := qr.PullBatch(context.Background())
	require.NoError(t, err)
	require.Nil(t, next)
}

func TestScanWindowLendsNumericColumnsAndKeepsTheCursorAlive(t *testing.T) {
	verifyNoScanLeaks(t)
	fx := newFixture(t, fxPart{{1, 1, 600, 1}})
	q := &fxQuery{tags: []string{fxTagN2, fxTagN}, fields: []string{fxFldInt, fxFldFlt, fxFldNul}, minTS: 0, maxTS: 1000, ascTS: true}
	schema := q.schema(t)
	qr := fx.newResult(t, q, schema)
	b, err := qr.PullBatch(context.Background())
	require.NoError(t, err)
	require.NotNil(t, b)
	require.Nil(t, b.Selection)
	n2, fi, ff := b.Tags[0], b.Fields[0], b.Fields[1]
	for _, c := range []interface{ Borrowed() bool }{n2.(interface{ Borrowed() bool }), fi.(interface{ Borrowed() bool }), ff.(interface{ Borrowed() bool })} {
		require.True(t, c.Borrowed(), "null-free numeric columns are lent")
	}
	// The tag with nulls is not in typed form, so it is an owned column.
	require.False(t, b.Tags[1].(interface{ Borrowed() bool }).Borrowed())
	require.Equal(t, 1, b.Owners.Len(), "the batch pins the one cursor it lends from")

	// Dropping the query's own reference must not recycle the cursor under the batch.
	before := blockCursorPool.RefsCount()
	qr.Release()
	require.Equal(t, before, blockCursorPool.RefsCount(), "the batch still holds the cursor")
	b.Release()
	require.Equal(t, before-1, blockCursorPool.RefsCount(), "the last release returns the cursor to its pool")
}

func TestScanWindowBorrowedNumericsFailVisiblyUnderPoisoningWhenReadAfterRelease(t *testing.T) {
	verifyNoScanLeaks(t)
	defer setPoisonReleasedCursor(setPoisonReleasedCursor(true))
	fx := newFixture(t, fxPart{{1, 1, 600, 1}})
	q := &fxQuery{tags: []string{fxTagN2}, fields: []string{fxFldInt, fxFldFlt}, minTS: 0, maxTS: 1000, ascTS: true}
	schema := q.schema(t)
	qr := fx.newResult(t, q, schema)
	b, err := qr.PullBatch(context.Background())
	require.NoError(t, err)
	fiView := b.Fields[0].(interface{ Borrowed() bool })
	require.True(t, fiView.Borrowed())
	tsView := b.Timestamps
	require.Equal(t, int64(1), tsView[0])
	qr.Release()
	require.Equal(t, int64(1), tsView[0], "still valid while the batch pins the cursor")
	b.Release()
	require.EqualValues(t, poisonNumeric, tsView[0], "a view read after the last release shows the poison")
}

func TestScanWindowReleasesEveryCursorOnEarlyReleaseAndOnCancel(t *testing.T) {
	verifyNoScanLeaks(t)
	fx := mixedFixture(t)
	q := &fxQuery{tags: allTags, fields: allFields, minTS: 0, maxTS: 30000}
	schema := q.schema(t)
	before := blockCursorPool.RefsCount()

	// Release after the first batch: every undecoded window and the open one must go.
	qr := fx.newResult(t, q, schema)
	b, err := qr.PullBatch(context.Background())
	require.NoError(t, err)
	require.NotNil(t, b)
	b.Release()
	qr.Release()
	require.Equal(t, before, blockCursorPool.RefsCount())

	// Cancel mid-scan: the next pull reports the interrupt and nothing leaks.
	ctx, cancel := context.WithCancel(context.Background())
	qr = fx.newResult(t, q, schema)
	qr.ctx = ctx
	b, err = qr.PullBatch(ctx)
	require.NoError(t, err)
	b.Release()
	cancel()
	_, err = qr.PullBatch(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "interrupt")
	_, again := qr.PullBatch(ctx)
	require.Error(t, again, "the error is sticky")
	qr.Release()
	require.Equal(t, before, blockCursorPool.RefsCount())

	// A full drain leaves nothing behind either.
	qr = fx.newResult(t, q, schema)
	for {
		b, err = qr.PullBatch(context.Background())
		require.NoError(t, err)
		if b == nil {
			break
		}
		b.Release()
	}
	qr.Release()
	require.Equal(t, before, blockCursorPool.RefsCount())
}

func TestScanWindowCoalescesSmallSegmentsIntoOwnedBatches(t *testing.T) {
	verifyNoScanLeaks(t)
	// Forty series of 50 rows each: every window is below model.SmallBatchRows.
	var part fxPart
	for sid := common.SeriesID(1); sid <= 40; sid++ {
		part = append(part, fxRange{sid: sid, from: 1, to: 50, version: 1})
	}
	fx := newFixture(t, part)
	q := &fxQuery{tags: []string{fxTagURL, fxTagN2}, fields: []string{fxFldInt}, minTS: 0, maxTS: 100}
	schema := q.schema(t)
	qr := fx.newResult(t, q, schema)
	defer qr.Release()
	var sizes []int
	var seriesInOrder []common.SeriesID
	for {
		b, err := qr.PullBatch(context.Background())
		require.NoError(t, err)
		if b == nil {
			break
		}
		sizes = append(sizes, b.ActiveRows())
		require.Nil(t, b.Selection)
		require.False(t, b.MetadataBorrowed(), "a coalesced batch owns its metadata")
		for _, c := range b.Fields {
			require.False(t, c.(interface{ Borrowed() bool }).Borrowed())
		}
		if len(b.SeriesBoundaries) > 0 {
			require.Equal(t, b.RowCount(), b.SeriesBoundaries[len(b.SeriesBoundaries)-1])
		}
		for _, sid := range b.SeriesIDs {
			if len(seriesInOrder) == 0 || seriesInOrder[len(seriesInOrder)-1] != sid {
				seriesInOrder = append(seriesInOrder, sid)
			}
		}
		b.Release()
	}
	require.GreaterOrEqual(t, sizes[0], coalesceTargetRows, "small windows are gathered up to the coalescing target")
	total := 0
	for _, s := range sizes {
		total += s
	}
	require.Equal(t, 2000, total)
	require.Len(t, sizes, 2, "1,050 rows reach the coalescing target, the other 950 are the tail")
	require.Len(t, seriesInOrder, 40)
	require.True(t, slices.IsSorted(seriesInOrder), "windows keep series order across the coalesced batches")
	require.EqualValues(t, 40, qr.scan.stats.windows)
	require.EqualValues(t, 0, qr.scan.stats.windowsOverlapped)
}

func TestScanWindowSkipsPlanningForTopN(t *testing.T) {
	verifyNoScanLeaks(t)
	fx := newFixture(t, fxPart{{1, 1, 50, 1}})
	q := &fxQuery{tags: []string{fxTagURL}, fields: []string{fxFldInt}, minTS: 0, maxTS: 100, ascTS: true}
	schema := q.schema(t)
	qr := fx.newResult(t, q, schema)
	defer qr.Release()
	qr.topNQueryOptions = &topNQueryOptions{}
	require.False(t, qr.useWindowScan())
	require.Nil(t, qr.scan)
}

func TestLegacySwitchDisablesTheWindowScan(t *testing.T) {
	verifyNoScanLeaks(t)
	fx := newFixture(t, fxPart{{1, 1, 50, 1}})
	q := &fxQuery{tags: []string{fxTagURL}, fields: []string{fxFldInt}, minTS: 0, maxTS: 100, ascTS: true}
	schema := q.schema(t)
	defer setLegacyBatchScan(setLegacyBatchScan(true))
	qr := fx.newResult(t, q, schema)
	defer qr.Release()
	b, err := qr.PullBatch(context.Background())
	require.NoError(t, err)
	require.NotNil(t, b)
	b.Release()
	require.Nil(t, qr.scan)
}

// expectedRows is an independent model of a query over the fixture: for every
// requested series, each timestamp in range once, at the highest version any part
// wrote, in time order; series in the requested order.
func (fx *fxFixture) expectedRows(order []common.SeriesID, minTS, maxTS int64, desc bool) [][3]int64 {
	var rows [][3]int64
	for _, sid := range order {
		newest := map[int64]int64{}
		for _, part := range fx.written {
			for _, r := range part {
				if r.sid != sid {
					continue
				}
				for ts := max(r.from, minTS); ts <= min(r.to, maxTS); ts++ {
					newest[ts] = max(newest[ts], r.version)
				}
			}
		}
		var tss []int64
		for ts := range newest {
			tss = append(tss, ts)
		}
		slices.Sort(tss)
		if desc {
			slices.Reverse(tss)
		}
		for _, ts := range tss {
			rows = append(rows, [3]int64{int64(sid), ts, newest[ts]})
		}
	}
	return rows
}

func TestScanWindowMatchesAnIndependentModel(t *testing.T) {
	verifyNoScanLeaks(t)
	fx := mixedFixture(t)
	tests := []struct {
		q    fxQuery
		desc bool
	}{
		{q: fxQuery{minTS: 0, maxTS: 30000}},
		{q: fxQuery{minTS: 120, maxTS: 700, order: []common.SeriesID{6, 2, 1, 5}}},
		{q: fxQuery{minTS: mixedTailEnd - 20, maxTS: mixedTailEnd + 5, order: []common.SeriesID{4}}},
		{q: fxQuery{minTS: 0, maxTS: 30000, order: []common.SeriesID{4}, orderByTS: true}, desc: true},
		{q: fxQuery{minTS: 0, maxTS: 30000, order: []common.SeriesID{2}, orderByTS: true}, desc: true},
		{q: fxQuery{minTS: 0, maxTS: 30000, order: []common.SeriesID{5}, orderByTS: true, ascTS: true}},
	}
	for i, tt := range tests {
		q := tt.q
		q.tags = []string{fxTagURL}
		q.fields = []string{fxFldInt}
		for _, legacy := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d legacy=%v", i, legacy), func(t *testing.T) {
				res := fx.run(t, &q, legacy)
				order := q.order
				if order == nil {
					order = fx.sids
				}
				want := fx.expectedRows(order, q.minTS, q.maxTS, tt.desc)
				require.Len(t, res.points, len(want))
				for j, dp := range res.points {
					got := [3]int64{int64(dp.GetDataPoint().GetSid()), dp.GetDataPoint().GetTimestamp().AsTime().UnixNano(), dp.GetDataPoint().GetVersion()}
					require.Equal(t, want[j], got, "row %d", j)
					url := dp.GetDataPoint().GetTagFamilies()[0].GetTags()[0].GetValue().GetStr().GetValue()
					require.Equal(t, fmt.Sprintf("/u/%d/%d/%d", want[j][0], want[j][1], want[j][2]), url, "row %d carries the newest version's values", j)
				}
			})
		}
	}
}

func TestScanWindowMatchesLegacyThroughGroupByAndAggregation(t *testing.T) {
	verifyNoScanLeaks(t)
	fx := mixedFixture(t)
	groupBy := &model.MeasureGroupBy{TagFamily: fxFamily, TagNames: []string{fxTagSvc}}
	for _, agg := range []*model.MeasureAgg{
		{FieldName: fxFldInt, Func: modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM},
		{FieldName: fxFldFlt, Func: modelv1.AggregationFunction_AGGREGATION_FUNCTION_MEAN},
		{FieldName: fxFldInt, Func: modelv1.AggregationFunction_AGGREGATION_FUNCTION_MAX},
	} {
		for _, gb := range []*model.MeasureGroupBy{nil, groupBy} {
			if gb == nil && agg.Func != modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM {
				continue
			}
			for _, tr := range [][2]int64{{0, 30000}, {250, 480}} {
				q := &fxQuery{
					tags: []string{fxTagSvc}, fields: []string{fxFldInt, fxFldFlt}, minTS: tr[0], maxTS: tr[1],
					order: []common.SeriesID{8, 6, 4, 2, 1, 3, 5, 7}, groupBy: gb, agg: agg,
				}
				what := fmt.Sprintf("agg=%v groupBy=%v range=%v", agg.Func, gb != nil, tr)
				t.Run(what, func(t *testing.T) {
					want := fx.run(t, q, true)
					got := fx.run(t, q, false)
					require.NotEmpty(t, want.points)
					requireSameResult(t, want, got, what)
				})
			}
		}
	}
}

func TestScanWindowSegmentCounters(t *testing.T) {
	verifyNoScanLeaks(t)
	tests := []struct {
		name         string
		parts        []fxPart
		fast         int64
		overlap      int64
		overlapWins  int64
		windows      int64
		wantRowCount int
	}{
		{
			name:         "a series split over disjoint parts takes the fast path once per block",
			parts:        []fxPart{{{1, 1, 300, 1}}, {{1, 301, 600, 2}}, {{1, 601, 900, 3}}},
			fast:         3,
			windows:      1,
			wantRowCount: 900,
		},
		{
			name:         "overlapping parts take the merge",
			parts:        []fxPart{{{1, 1, 400, 1}}, {{1, 300, 700, 2}}},
			overlap:      1,
			overlapWins:  1,
			windows:      1,
			wantRowCount: 700,
		},
		{
			name:         "blocks that meet at one timestamp are an overlap, the newer version wins it",
			parts:        []fxPart{{{1, 1, 300, 1}}, {{1, 300, 600, 2}}},
			overlap:      1,
			overlapWins:  1,
			windows:      1,
			wantRowCount: 600,
		},
		{
			name:         "blocks one timestamp apart are disjoint",
			parts:        []fxPart{{{1, 1, 300, 1}}, {{1, 301, 600, 2}}},
			fast:         2,
			windows:      1,
			wantRowCount: 600,
		},
		{
			name:         "an overlap in one series does not make its neighbor merge",
			parts:        []fxPart{{{1, 1, 400, 1}, {2, 1, 400, 1}}, {{1, 300, 700, 2}, {2, 401, 800, 2}}},
			fast:         2,
			overlap:      1,
			overlapWins:  1,
			windows:      2,
			wantRowCount: 700 + 800,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fx := newFixture(t, tt.parts...)
			q := &fxQuery{tags: []string{fxTagURL}, fields: []string{fxFldInt}, minTS: 0, maxTS: 10000}
			schema := q.schema(t)
			qr := fx.newResult(t, q, schema)
			defer qr.Release()
			rows := 0
			for {
				b, err := qr.PullBatch(context.Background())
				require.NoError(t, err)
				if b == nil {
					break
				}
				rows += b.ActiveRows()
				b.Release()
			}
			require.Equal(t, tt.wantRowCount, rows)
			require.NotNil(t, qr.scan)
			require.Equal(t, tt.fast, qr.scan.stats.segmentsFast, "fast segments")
			require.Equal(t, tt.overlap, qr.scan.stats.segmentsOverlap, "overlap segments")
			require.Equal(t, tt.overlapWins, qr.scan.stats.windowsOverlapped, "overlapped windows")
			require.Equal(t, tt.windows, qr.scan.stats.windows, "windows")
		})
	}
}

func TestScanWindowPassesBlockSizedBatchesThrough(t *testing.T) {
	verifyNoScanLeaks(t)
	// 20,000 rows of one series are three blocks of at most maxBlockLength+1 rows.
	fx := newFixture(t, fxPart{{1, 1, 20000, 1}})
	for _, desc := range []bool{false, true} {
		q := &fxQuery{tags: []string{fxTagURL, fxTagN2}, fields: []string{fxFldInt}, minTS: 0, maxTS: 30000, orderByTS: desc, ascTS: !desc}
		schema := q.schema(t)
		qr := fx.newResult(t, q, schema)
		pool := vectorizedBatchPool(schema)
		source := newTestBatchSource(qr, schema, pool)
		var sizes []int
		total := 0
		for {
			b, err := source.NextBatch(context.Background())
			require.NoError(t, err)
			if b == nil {
				break
			}
			require.True(t, b.Lent)
			sizes = append(sizes, b.ActiveLen())
			total += b.ActiveLen()
			pool.Put(b)
		}
		require.NoError(t, source.Close())
		require.Equal(t, 20000, total)
		require.Len(t, sizes, 3, "one batch per block, not one per 1,024 rows (desc=%v)", desc)
		for _, size := range sizes {
			require.Greater(t, size, 1024)
			require.LessOrEqual(t, size, maxBlockLength+1)
		}
	}
}

func TestScanWindowCursorsReturnToThePoolOnlyWhenEveryBatchIsReleased(t *testing.T) {
	verifyNoScanLeaks(t)
	fx := newFixture(t, fxPart{{1, 1, 20000, 1}})
	q := &fxQuery{tags: []string{fxTagURL}, fields: []string{fxFldInt}, minTS: 0, maxTS: 30000, ascTS: true}
	schema := q.schema(t)
	before := blockCursorPool.RefsCount()
	qr := fx.newResult(t, q, schema)
	var batches []*model.MeasureBatch
	for {
		b, err := qr.PullBatch(context.Background())
		require.NoError(t, err)
		if b == nil {
			break
		}
		batches = append(batches, b)
	}
	require.Len(t, batches, 3)
	qr.Release()
	require.Equal(t, before+3, blockCursorPool.RefsCount(), "each batch still pins its cursor after the query is released")
	for i, b := range batches {
		b.Release()
		require.Equal(t, before+2-i, blockCursorPool.RefsCount())
	}
}

func vectorizedBatchPool(schema *vectorized.BatchSchema) *vectorized.BatchPool {
	return vectorized.NewBatchPool(schema, 1024)
}

func newTestBatchSource(qr *queryResult, schema *vectorized.BatchSchema, pool *vectorized.BatchPool) *vmeasure.BatchSourceFromBatchResult {
	return vmeasure.NewBatchSourceFromBatchResult(qr, schema, pool, 1024)
}

func TestMixedScanRejectsRetainedResponseAt64MiB(t *testing.T) {
	verifyNoScanLeaks(t)
	fx := mixedFixture(t)
	q := &fxQuery{tags: allTags, fields: allFields, minTS: 0, maxTS: 30000}
	schema := q.schema(t)
	qr := fx.newResult(t, q, schema)
	tracker := vectorized.NewMemoryTracker(64 << 20)
	pool := vectorized.NewBatchPool(schema, 1024)
	pipeline, err := vectorized.NewPipelineBuilder().WithMemoryTracker(tracker).
		From(vmeasure.NewBatchSourceFromBatchResult(qr, schema, pool, 1024)).Build()
	require.NoError(t, err)
	require.NoError(t, pipeline.Init(context.Background()))
	iterator := vmeasure.NewIteratorFromPipeline(context.Background(), pipeline, schema, pool)
	var retained []*measurev1.InternalDataPoint
	for iterator.Next() {
		retained = append(retained, iterator.Current()...)
	}
	require.NotEmpty(t, retained)
	require.ErrorIs(t, iterator.Err(), vectorized.ErrMemoryBudgetExceeded)
	require.ErrorIs(t, iterator.Close(), vectorized.ErrMemoryBudgetExceeded)
	require.Zero(t, tracker.Used(), "failed collection must release reservations")
}
