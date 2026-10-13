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
	"container/heap"
	"context"
	"fmt"
	"math/rand"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
)

// oapFixture is the shape OAP produces: many series that all cover the same time
// range, flushed in several parts that overlap, repeat timestamps with a higher
// version, or only touch each other.
//
//	P1: every series, 1..300, v1
//	P2: even series, 250..500, v2 (overlaps P1's tail, rewrites 250..300)
//	P3: every fifth series, 100..260, v3 (overlaps both, rewrites 250..260 again)
//	P4: every series, 501..650, v4 (disjoint from everything before it)
//	P5: the first ten series, 1..50, v1 again (same version as P1: identical rows)
func oapFixture(t testing.TB, series int) *fxFixture {
	var p1, p2, p3, p4, p5 fxPart
	for sid := common.SeriesID(1); sid <= common.SeriesID(series); sid++ {
		p1 = append(p1, fxRange{sid, 1, 300, 1})
		if sid%2 == 0 {
			p2 = append(p2, fxRange{sid, 250, 500, 2})
		}
		if sid%5 == 0 {
			p3 = append(p3, fxRange{sid, 100, 260, 3})
		}
		p4 = append(p4, fxRange{sid, 501, 650, 4})
		if sid <= 10 {
			p5 = append(p5, fxRange{sid, 1, 50, 1})
		}
	}
	return newFixture(t, p1, p2, p3, p4, p5)
}

// staggeredFixture has series whose ranges start at different times and overlap
// partially, with duplicate timestamps across parts and blocks that touch at one
// timestamp.
func staggeredFixture(t testing.TB, series int) *fxFixture {
	var a, b, c fxPart
	for i := 1; i <= series; i++ {
		sid := common.SeriesID(i)
		start := int64(i * 20)
		a = append(a, fxRange{sid, start, start + 300, 1})
		if i%3 != 0 {
			b = append(b, fxRange{sid, start + 150, start + 450, 2})
		}
		if i%4 == 0 {
			c = append(c, fxRange{sid, start + 450, start + 600, 3})
		}
	}
	return newFixture(t, a, b, c)
}

func hiddenTags() map[common.SeriesID]map[string]*modelv1.TagValue {
	return map[common.SeriesID]map[string]*modelv1.TagValue{
		2:  {fxTagHid: strTagValue("hidden-2")},
		7:  {fxTagHid: strTagValue("hidden-7")},
		10: {fxTagHid: strTagValue("hidden-10")},
	}
}

func TestTimeMergeMatchesLegacyAcrossGeneratedQueries(t *testing.T) {
	verifyNoScanLeaks(t)
	fixtures := map[string]*fxFixture{
		"oap":       oapFixture(t, 100),
		"staggered": staggeredFixture(t, 24),
		"mixed":     mixedFixture(t),
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
		{"tags only", []string{fxTagURL, fxTagHid}, nil},
	}
	rng := rand.New(rand.NewSource(20261014)) //nolint:gosec // a fixed seed makes the generated queries reproducible
	ranges := [][2]int64{{0, 1 << 20}, {100, 450}, {299, 301}, {250, 260}, {501, 505}, {40, 40}}
	limits := [][2]uint32{{0, 0}, {0, 7}, {5, 50}, {300, 2000}, {0, 100000}}
	orders := []struct {
		name      string
		orderByTS bool
		ascTS     bool
	}{
		{"no order by", true, true},
		{"order by time asc", true, true},
		{"order by time desc", true, false},
		{"series order", false, true},
		{"series order desc flag", false, false},
	}
	for name, fx := range fixtures {
		for iter := range 36 {
			ord := orders[rng.Intn(len(orders))]
			proj := projections[rng.Intn(len(projections))]
			tr := ranges[rng.Intn(len(ranges))]
			lim := limits[rng.Intn(len(limits))]
			q := &fxQuery{
				tags: proj.tags, fields: proj.fields, minTS: tr[0], maxTS: tr[1],
				offset: lim[0], limit: lim[1], orderByTS: ord.orderByTS, ascTS: ord.ascTS, passthrough: rng.Intn(4) == 0,
			}
			if rng.Intn(3) != 0 {
				order := slices.Clone(fx.sids)
				rng.Shuffle(len(order), func(i, j int) { order[i], order[j] = order[j], order[i] })
				q.order = order[:max(2, rng.Intn(len(order)+1))]
			}
			if rng.Intn(2) == 0 && len(proj.tags) > 0 {
				q.stored = hiddenTags()
			}
			what := fmt.Sprintf("%s iter %d %s proj=%s range=%v limit=%v series=%d hidden=%v passthrough=%v",
				name, iter, ord.name, proj.name, tr, lim, len(q.order), q.stored != nil, q.passthrough)
			t.Run(fmt.Sprintf("%s/%d", name, iter), func(t *testing.T) {
				want := fx.run(t, q, true)
				got := fx.run(t, q, false)
				requireSameResult(t, want, got, what)
				if len(want.points) > 0 {
					require.NotNil(t, got.scan, what)
				}
			})
		}
	}
}

func TestTimeMergeMatchesLegacyThroughGroupByAggregationAndTimeBuckets(t *testing.T) {
	verifyNoScanLeaks(t)
	fixtures := map[string]*fxFixture{"oap": oapFixture(t, 60), "staggered": staggeredFixture(t, 16)}
	groupBy := &model.MeasureGroupBy{TagFamily: fxFamily, TagNames: []string{fxTagSvc}}
	bucketOnly := &model.MeasureGroupBy{TimeBucket: &model.MeasureTimeBucket{Width: "100ns", WidthNanos: 100}}
	bucketAndTag := &model.MeasureGroupBy{
		TagFamily: fxFamily, TagNames: []string{fxTagSvc}, TimeBucket: &model.MeasureTimeBucket{Width: "64ns", WidthNanos: 64},
	}
	aggs := []*model.MeasureAgg{
		{FieldName: fxFldInt, Func: modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM},
		{FieldName: fxFldFlt, Func: modelv1.AggregationFunction_AGGREGATION_FUNCTION_MEAN},
		{FieldName: fxFldInt, Func: modelv1.AggregationFunction_AGGREGATION_FUNCTION_MAX},
	}
	for name, fx := range fixtures {
		for _, gb := range []*model.MeasureGroupBy{nil, groupBy, bucketOnly, bucketAndTag} {
			for _, agg := range aggs {
				if gb == nil && agg.Func != modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM {
					continue
				}
				for _, tr := range [][2]int64{{0, 1 << 20}, {120, 480}} {
					q := &fxQuery{
						tags: []string{fxTagSvc}, fields: []string{fxFldInt, fxFldFlt}, minTS: tr[0], maxTS: tr[1],
						groupBy: gb, agg: agg, orderByTS: true, ascTS: true,
					}
					what := fmt.Sprintf("%s agg=%v groupBy=%v bucket=%v range=%v", name, agg.Func, gb != nil, gb != nil && gb.TimeBucket != nil, tr)
					t.Run(what, func(t *testing.T) {
						want := fx.run(t, q, true)
						got := fx.run(t, q, false)
						require.NotEmpty(t, want.points)
						requireSameResult(t, want, got, what)
						require.Positive(t, got.scan.mergeRows)
					})
				}
			}
		}
	}
}

func TestTimeMergeMatchesAnIndependentModelForInterleavedSeries(t *testing.T) {
	verifyNoScanLeaks(t)
	fx := oapFixture(t, 40)
	order := fx.sids
	for _, desc := range []bool{false, true} {
		q := &fxQuery{tags: []string{fxTagURL}, fields: []string{fxFldInt}, minTS: 0, maxTS: 1 << 20, orderByTS: true, ascTS: !desc}
		res := fx.run(t, q, false)
		// The model: every (series, ts) once at its newest version, ordered by time
		// (reversed for desc) and, inside one timestamp, by series id.
		type key struct{ sid, ts int64 }
		newest := map[key]int64{}
		for _, part := range fx.written {
			for _, r := range part {
				for ts := r.from; ts <= r.to; ts++ {
					k := key{int64(r.sid), ts}
					newest[k] = max(newest[k], r.version)
				}
			}
		}
		var want []key
		for k := range newest {
			want = append(want, k)
		}
		slices.SortFunc(want, func(a, b key) int {
			if a.ts != b.ts {
				if desc {
					return int(b.ts - a.ts)
				}
				return int(a.ts - b.ts)
			}
			return int(a.sid - b.sid)
		})
		require.Len(t, res.points, len(want), "desc=%v", desc)
		for i, dp := range res.points {
			d := dp.GetDataPoint()
			require.Equal(t, want[i], key{int64(d.GetSid()), d.GetTimestamp().AsTime().UnixNano()}, "row %d desc=%v", i, desc)
			require.Equal(t, newest[want[i]], d.GetVersion(), "row %d keeps the newest version", i)
		}
		require.Len(t, order, 40)
	}
}

// TestTimeMergeDedupsDuplicatesAcrossPageBoundaries pins the one place the merge
// deliberately differs from the legacy batch merge. mergeBatch stopped a page at
// 4,096 rows wherever it stood, so a timestamp duplicated across parts that fell
// on the cut was emitted twice, once per page; the row merge (queryResult.merge,
// no page cap) and the independent model emit it once.
func TestTimeMergeDedupsDuplicatesAcrossPageBoundaries(t *testing.T) {
	verifyNoScanLeaks(t)
	// Row i of the series is ts i. The legacy page ends after row 4,096: the newer
	// part's ts 4096 wins the page, the older part's ts 4096 starts the next one.
	fx := newFixture(t, fxPart{{1, 1, 9000, 1}}, fxPart{{1, 4096, 6000, 2}})
	q := &fxQuery{tags: []string{fxTagURL}, fields: []string{fxFldInt}, minTS: 0, maxTS: 1 << 20, ascTS: true, order: []common.SeriesID{1}}

	// The row path is the reference for what a query returns.
	schema := q.schema(t)
	qr := fx.newResult(t, q, schema)
	var rowPathTS []int64
	for {
		r := qr.Pull()
		if r == nil {
			break
		}
		require.NoError(t, r.Error)
		rowPathTS = append(rowPathTS, r.Timestamps...)
	}
	qr.Release()
	require.Len(t, rowPathTS, 9000)
	require.True(t, slices.IsSorted(rowPathTS))
	require.Len(t, slices.Compact(slices.Clone(rowPathTS)), 9000, "the row path never repeats a timestamp")

	legacy := fx.run(t, q, true)
	got := fx.run(t, q, false)
	require.Len(t, got.points, 9000)
	for i, dp := range got.points {
		require.EqualValues(t, rowPathTS[i], dp.GetDataPoint().GetTimestamp().AsTime().UnixNano())
	}
	require.Len(t, legacy.points, 9001, "the legacy batch merge emits the duplicate that straddles its 4,096-row page")
	var dups []int64
	for i := 1; i < len(legacy.points); i++ {
		a, b := legacy.points[i-1].GetDataPoint(), legacy.points[i].GetDataPoint()
		if a.GetTimestamp().AsTime().UnixNano() == b.GetTimestamp().AsTime().UnixNano() {
			dups = append(dups, a.GetTimestamp().AsTime().UnixNano())
			require.Greater(t, a.GetVersion(), b.GetVersion(), "the newest version came first")
		}
	}
	require.Equal(t, []int64{4096}, dups)
	// Everywhere else the two agree: drop the stray row and the results match.
	trimmed := fxResult{points: append(slices.Clone(legacy.points[:4096]), legacy.points[4097:]...)}
	for i := range got.points {
		require.True(t, proto.Equal(trimmed.points[i], got.points[i]), "row %d", i)
	}
}

func TestTimeMergeSetsSeriesBoundariesForInterleavedRuns(t *testing.T) {
	verifyNoScanLeaks(t)
	fx := oapFixture(t, 6)
	q := &fxQuery{tags: []string{fxTagURL}, fields: []string{fxFldInt}, minTS: 0, maxTS: 1 << 20, orderByTS: true, ascTS: true}
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
		rows += b.RowCount()
		require.NotEmpty(t, b.SeriesBoundaries, "rows of several series interleave in this batch")
		require.Equal(t, b.RowCount(), b.SeriesBoundaries[len(b.SeriesBoundaries)-1])
		start := 0
		for _, end := range b.SeriesBoundaries {
			require.Greater(t, end, start)
			for r := start; r < end; r++ {
				require.Equal(t, b.SeriesIDs[start], b.SeriesIDs[r], "a run is one series")
			}
			if end < b.RowCount() {
				require.NotEqual(t, b.SeriesIDs[end-1], b.SeriesIDs[end], "runs are maximal")
			}
			start = end
		}
		b.Release()
	}
	require.Positive(t, rows)
}

// TestTimeMergeDecodesCursorsAtTheFrontier is the memory bound: series that all
// cover the same time range, each flushed in many parts that are disjoint in time,
// leave one block per series live at any instant, not one per part.
func TestTimeMergeDecodesCursorsAtTheFrontier(t *testing.T) {
	verifyNoScanLeaks(t)
	const (
		series = 40
		blocks = 12
		span   = 200
	)
	parts := make([]fxPart, blocks)
	for p := range parts {
		for sid := common.SeriesID(1); sid <= series; sid++ {
			parts[p] = append(parts[p], fxRange{sid, int64(p*span + 1), int64((p + 1) * span), int64(p + 1)})
		}
	}
	fx := newFixture(t, parts...)
	q := &fxQuery{tags: []string{fxTagURL, fxTagSvc}, fields: []string{fxFldInt}, minTS: 0, maxTS: 1 << 20, orderByTS: true, ascTS: true}
	schema := q.schema(t)
	for _, desc := range []bool{false, true} {
		q.ascTS = !desc
		qr := fx.newResult(t, q, schema)
		qr.dp = nil // the test reads cursor state between pulls, which decodes ahead would race with
		all := slices.Clone(qr.data)
		require.Len(t, all, series*blocks)
		decodedAndHeld := func() (held, returned int) {
			for _, bc := range all {
				switch {
				case bc.refs.Load() > 0 && len(bc.timestamps) > 0:
					held++
				case bc.refs.Load() == 0:
					returned++
				}
			}
			return held, returned
		}
		maxHeld, rows, batches := 0, 0, 0
		returnedMidScan := 0
		for {
			b, err := qr.PullBatch(context.Background())
			require.NoError(t, err)
			if b == nil {
				break
			}
			rows += b.RowCount()
			batches++
			b.Release()
			held, returned := decodedAndHeld()
			maxHeld = max(maxHeld, held)
			if rows < series*blocks*span/2 {
				returnedMidScan = max(returnedMidScan, returned)
			}
		}
		require.Equal(t, series*blocks*span, rows)
		require.Greater(t, batches, 4)
		stats := qr.scan.stats
		require.EqualValues(t, series*blocks, stats.mergeCursors, "every block is decoded once, desc=%v", desc)
		require.LessOrEqual(t, int(stats.mergePeakLive), series+series/4+minFinishedLimit, "one live block per series at the frontier, desc=%v", desc)
		t.Logf("peak live %d held %d (series %d, blocks per series %d)", stats.mergePeakLive, maxHeld, series, blocks)
		require.GreaterOrEqual(t, int(stats.mergePeakLive), series)
		require.LessOrEqual(t, maxHeld, series+series/4+minFinishedLimit, "decoded blocks held between pulls, desc=%v", desc)
		require.Greater(t, returnedMidScan, series*(blocks/2-1), "cursors went back to the pool while the scan was still running, desc=%v", desc)
		qr.Release()
		for _, bc := range all {
			require.EqualValues(t, 0, bc.refs.Load(), "every cursor is back in the pool at the end")
		}
	}
}

// TestTimeMergeLoaderIsThePlugInPoint checks the hook step 4b needs: every decode
// goes through windowScan.load, in the order the frontier reaches the cursors.
func TestTimeMergeLoaderIsThePlugInPoint(t *testing.T) {
	verifyNoScanLeaks(t)
	fx := newFixture(t, fxPart{{1, 1, 100, 1}, {2, 50, 150, 1}}, fxPart{{1, 90, 200, 2}, {2, 140, 300, 2}})
	q := &fxQuery{tags: []string{fxTagURL}, fields: []string{fxFldInt}, minTS: 0, maxTS: 1000, orderByTS: true, ascTS: true}
	schema := q.schema(t)
	qr := fx.newResult(t, q, schema)
	defer qr.Release()
	qr.dp = nil // the order decodes happen in is what is asserted
	require.True(t, qr.useWindowScan())
	var loaded [][2]int64
	qr.scan.load = func(ctx context.Context, bc *blockCursor, desc bool) (bool, error) {
		loaded = append(loaded, [2]int64{int64(bc.bm.seriesID), bc.bm.timestamps.min})
		return decodeCursor(ctx, bc, desc)
	}
	before := len(loaded)
	b, err := qr.PullBatch(context.Background())
	require.NoError(t, err)
	b.Release()
	require.Greater(t, len(loaded), before)
	for {
		b, err = qr.PullBatch(context.Background())
		require.NoError(t, err)
		if b == nil {
			break
		}
		b.Release()
	}
	require.Equal(t, [][2]int64{{1, 1}, {2, 50}, {1, 90}, {2, 140}}, loaded, "cursors are decoded in frontier order")
}

func TestTimeMergeReleasesEverythingOnCancelAndEarlyClose(t *testing.T) {
	verifyNoScanLeaks(t)
	fx := oapFixture(t, 30)
	q := &fxQuery{tags: allTags, fields: allFields, minTS: 0, maxTS: 1 << 20, orderByTS: true, ascTS: true}
	schema := q.schema(t)
	before := blockCursorPool.RefsCount()

	qr := fx.newResult(t, q, schema)
	b, err := qr.PullBatch(context.Background())
	require.NoError(t, err)
	b.Release()
	qr.Release()
	require.Equal(t, before, blockCursorPool.RefsCount(), "an early release returns every cursor")

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
	qr.Release()
	require.Equal(t, before, blockCursorPool.RefsCount())

	// A cancel in the middle of a merge page: the page's batch is discarded.
	ctx, cancel = context.WithCancel(context.Background())
	qr = fx.newResult(t, q, schema)
	qr.ctx = ctx
	qr.dp = nil
	require.True(t, qr.useWindowScan())
	qr.scan.load = func(ctx context.Context, bc *blockCursor, desc bool) (bool, error) {
		cancel()
		return decodeCursor(ctx, bc, desc)
	}
	_, err = qr.PullBatch(ctx)
	require.Error(t, err)
	qr.Release()
	require.Equal(t, before, blockCursorPool.RefsCount())
}

// TestTimeMergeBatchesBorrowFromCursorsUnderPoisoning reads the merged string,
// bytes and array cells after the query is released and the cursors that were
// consumed are back in the pool: the batch's pins must keep exactly the memory it
// borrows alive.
func TestTimeMergeBatchesBorrowFromCursorsUnderPoisoning(t *testing.T) {
	verifyNoScanLeaks(t)
	defer setPoisonReleasedCursor(setPoisonReleasedCursor(true))
	fx := oapFixture(t, 12)
	q := &fxQuery{tags: []string{fxTagURL, fxTagArr, fxTagBin, fxTagSvc}, fields: []string{fxFldInt}, minTS: 0, maxTS: 1 << 20, orderByTS: true, ascTS: true}
	schema := q.schema(t)
	qr := fx.newResult(t, q, schema)
	var held []*model.MeasureBatch
	for {
		b, err := qr.PullBatch(context.Background())
		require.NoError(t, err)
		if b == nil {
			break
		}
		held = append(held, b)
	}
	qr.Release()
	for _, b := range held {
		require.NotZero(t, b.Owners.Len(), "string cells borrow, so the batch pins its cursors")
		for r := range b.RowCount() {
			url := b.Tags[0].(interface{ Data() []string }).Data()[r]
			want := fmt.Sprintf("/u/%d/%d/%d", b.SeriesIDs[r], b.Timestamps[r], b.Versions[r])
			require.Equal(t, want, url)
		}
		b.Release()
	}
}

// TestTimeMergeKeepsTheNewestVersionOfDuplicatesInsideOneBlock covers the
// replace branch of the duplicate rule: rows of one timestamp that sit in one
// block in ascending version order reach the merge oldest first.
func TestTimeMergeKeepsTheNewestVersionOfDuplicatesInsideOneBlock(t *testing.T) {
	verifyNoScanLeaks(t)
	fx := newFixture(t,
		fxPart{{1, 1, 40, 1}, {1, 20, 60, 2}, {1, 30, 50, 3}, {2, 1, 40, 1}, {2, 20, 60, 2}},
		fxPart{{2, 35, 70, 4}},
	)
	for _, asc := range []bool{true, false} {
		q := &fxQuery{tags: []string{fxTagURL}, fields: []string{fxFldInt}, minTS: 0, maxTS: 1000, orderByTS: true, ascTS: asc}
		want := fx.run(t, q, true)
		got := fx.run(t, q, false)
		requireSameResult(t, want, got, fmt.Sprintf("asc=%v", asc))
		newest := map[[2]int64]int64{}
		for _, part := range fx.written {
			for _, r := range part {
				for ts := r.from; ts <= r.to; ts++ {
					k := [2]int64{int64(r.sid), ts}
					newest[k] = max(newest[k], r.version)
				}
			}
		}
		require.Len(t, got.points, len(newest))
		for _, dp := range got.points {
			d := dp.GetDataPoint()
			require.Equal(t, newest[[2]int64{int64(d.GetSid()), d.GetTimestamp().AsTime().UnixNano()}], d.GetVersion())
		}
	}
}

// syntheticCursor is a decoded cursor with only keys: every projected column is
// missing, so the output is null cells, which the legacy merge fills the same way.
func syntheticCursor(sid common.SeriesID, ts, versions []int64) *blockCursor {
	bc := generateBlockCursor()
	bc.bm.seriesID = sid
	bc.bm.timestamps.min, bc.bm.timestamps.max = ts[0], ts[len(ts)-1]
	bc.minTimestamp, bc.maxTimestamp = 0, 1<<40
	bc.timestamps = append(bc.timestamps, ts...)
	bc.versions = append(bc.versions, versions...)
	return bc
}

// TestTimeMergeKeysMatchTheLegacyHeapMergeOnUnsortedVersions feeds the merge the
// streams only a hand-built cursor can: equal timestamps inside one cursor with
// versions in ascending order, so the legacy replace rule (a higher version
// overwrites the row just emitted) decides the winner.
func TestTimeMergeKeysMatchTheLegacyHeapMergeOnUnsortedVersions(t *testing.T) {
	verifyNoScanLeaks(t)
	q := &fxQuery{tags: []string{fxTagURL}, fields: []string{fxFldInt}, minTS: 0, maxTS: 1 << 40, orderByTS: true, ascTS: true}
	schema := q.schema(t)
	type spec struct {
		ts, versions []int64
		sid          common.SeriesID
	}
	specs := []spec{
		{sid: 1, ts: []int64{5, 5, 5, 7, 9}, versions: []int64{1, 3, 2, 1, 1}},
		{sid: 1, ts: []int64{5, 6, 7, 7}, versions: []int64{2, 1, 4, 2}},
		{sid: 2, ts: []int64{5, 5, 8}, versions: []int64{9, 10, 1}},
	}
	build := func() []*blockCursor {
		var out []*blockCursor
		for _, sp := range specs {
			out = append(out, syntheticCursor(sp.sid, sp.ts, sp.versions))
		}
		return out
	}
	for _, desc := range []bool{false, true} {
		cursors := build()
		if desc {
			for i, sp := range specs {
				slices.Reverse(cursors[i].timestamps)
				slices.Reverse(cursors[i].versions)
				cursors[i].idx = len(sp.ts) - 1
			}
		}
		qr := &queryResult{ctx: context.Background(), batchSchema: schema, orderByTS: true, ascTS: !desc, sidToIndex: map[common.SeriesID]int{}}
		qr.data = cursors
		wantBatch, err := qr.mergeBatchFromHeap()
		require.NoError(t, err)
		var wantTS, wantVer []int64
		var wantSID []common.SeriesID
		for wantBatch != nil {
			wantTS = append(wantTS, wantBatch.Timestamps...)
			wantVer = append(wantVer, wantBatch.Versions...)
			wantSID = append(wantSID, wantBatch.SeriesIDs...)
			wantBatch.Release()
			wantBatch, err = qr.mergeBatchFromHeap()
			require.NoError(t, err)
		}

		cursors = build()
		if desc {
			for i, sp := range specs {
				slices.Reverse(cursors[i].timestamps)
				slices.Reverse(cursors[i].versions)
				cursors[i].idx = len(sp.ts) - 1
			}
		}
		qr = &queryResult{ctx: context.Background(), batchSchema: schema, orderByTS: true, ascTS: !desc}
		var stats scanStats
		load := func(_ context.Context, bc *blockCursor, _ bool) (bool, error) { return true, nil }
		m := newTimeMerge(qr, &stats, load, newColPlan(schema), cursors, desc, nil)
		var gotTS, gotVer []int64
		var gotSID []common.SeriesID
		for {
			b, mergeErr := m.nextPage(context.Background())
			require.NoError(t, mergeErr)
			if b == nil {
				break
			}
			gotTS = append(gotTS, b.Timestamps...)
			gotVer = append(gotVer, b.Versions...)
			gotSID = append(gotSID, b.SeriesIDs...)
			b.Release()
		}
		m.close()
		require.NotEmpty(t, wantTS)
		require.Equal(t, wantTS, gotTS, "desc=%v", desc)
		require.Equal(t, wantVer, gotVer, "desc=%v", desc)
		require.Equal(t, wantSID, gotSID, "desc=%v", desc)
	}
}

// mergeBatchFromHeap runs one legacy mergeBatch page over cursors that are already
// decoded, initializing the heap on first use.
func (qr *queryResult) mergeBatchFromHeap() (*model.MeasureBatch, error) {
	if !qr.loaded {
		qr.loaded = true
		heap.Init(qr)
	}
	return qr.mergeBatch(qr.storedIndexValue, qr.batchSchema)
}
