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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
)

// withFastPaths runs fn with the ConstColumn and runs fast paths on (true) or off.
func withFastPaths(on bool, fn func()) {
	prevConst := disableConstColumns.Swap(!on)
	prevRuns := vmeasure.SetRunsDisabled(!on)
	defer func() {
		disableConstColumns.Store(prevConst)
		vmeasure.SetRunsDisabled(prevRuns)
	}()
	fn()
}

// pullAll drains qr batch by batch through PullBatch, handing each to inspect.
func pullAll(t *testing.T, qr *queryResult, inspect func(b *model.MeasureBatch)) {
	t.Helper()
	for {
		b, err := qr.PullBatch(context.Background())
		require.NoError(t, err)
		if b == nil {
			break
		}
		inspect(b)
		b.Release()
	}
	qr.Release()
}

func constOf(t *testing.T, col vectorized.Column) *vectorized.ConstColumn {
	t.Helper()
	c, ok := vectorized.AsConst(col)
	require.True(t, ok, "expected a ConstColumn, got %T", col)
	return c
}

func TestScanWindowLendsEntityTagsAsConstColumns(t *testing.T) {
	verifyNoScanLeaks(t)
	// Series 1 and 2 are large and disjoint in time (lent blocks); 3 and 4 are small
	// and disjoint from everything (coalesced into one batch of two series).
	fx := newFixture(t, fxPart{{1, 1, 3000, 1}, {2, 3001, 6000, 1}, {3, 7000, 7010, 1}, {4, 7020, 7032, 1}})
	stored := map[common.SeriesID]map[string]*modelv1.TagValue{
		1: {fxTagHid: strTagValue("hidden-1")},
		2: {fxTagHid: strTagValue("hidden-2")},
	}
	q := &fxQuery{
		tags: []string{fxTagSvc, fxTagURL, fxTagHid, fxTagN2}, fields: []string{fxFldInt}, minTS: 0, maxTS: 10000,
		stored: stored, orderByTS: true, ascTS: true,
	}
	schema := q.schema(t)
	svcIdx, urlIdx, hidIdx, n2Idx := 0, 1, 2, 3
	var lent, pages int
	withFastPaths(true, func() {
		pullAll(t, fx.newResult(t, q, schema), func(b *model.MeasureBatch) {
			require.True(t, b.SeriesKnown)
			require.Equal(t, vectorized.TimeOrderAsc, b.TimeOrder)
			require.Positive(t, b.ActiveRows())
			// Series boundaries are exact: runs of equal series ids.
			var want []int
			for i := 1; i < b.RowCount(); i++ {
				if b.SeriesIDs[i] != b.SeriesIDs[i-1] {
					want = append(want, i)
				}
			}
			if len(want) > 0 {
				want = append(want, b.RowCount())
			}
			require.Equal(t, want, append([]int(nil), b.SeriesBoundaries...))
			// A column that is not an entity tag is never constant.
			_, isConst := vectorized.AsConst(b.Tags[urlIdx])
			require.False(t, isConst)
			_, isConst = vectorized.AsConst(b.Tags[n2Idx])
			require.False(t, isConst)
			if len(want) > 0 {
				// Rows of several series stay typed: no single constant covers them.
				pages++
				_, isConst = vectorized.AsConst(b.Tags[svcIdx])
				require.False(t, isConst, "an interleaved page is not constant")
				return
			}
			lent++
			sid := b.SeriesIDs[0]
			svc := constOf(t, b.Tags[svcIdx])
			require.Equal(t, fmt.Sprintf("svc-%d", int64(sid)%4), svc.Str())
			require.Equal(t, b.RowCount(), svc.Len())
			hid := constOf(t, b.Tags[hidIdx])
			if tv, ok := stored[sid]; ok {
				require.Equal(t, tv[fxTagHid].GetStr().GetValue(), hid.Str(), "an index-substituted entity tag is constant per series")
			} else {
				require.True(t, hid.IsNullValue(), "a tag no row carries is a null constant")
			}
		})
	})
	require.Positive(t, lent)
	require.Positive(t, pages, "the two small series are coalesced into one multi-series batch")

	// With the fast paths off the same query produces no constant at all.
	withFastPaths(false, func() {
		pullAll(t, fx.newResult(t, q, schema), func(b *model.MeasureBatch) {
			for _, col := range b.Tags {
				_, isConst := vectorized.AsConst(col)
				require.False(t, isConst)
			}
		})
	})
}

func TestScanWindowCoalescedSingleSeriesBatchCarriesConstEntityTags(t *testing.T) {
	verifyNoScanLeaks(t)
	// Several small segments of one series are coalesced into one owned batch.
	fx := newFixture(t,
		fxPart{{1, 1, 40, 1}}, fxPart{{1, 100, 140, 1}}, fxPart{{1, 200, 240, 1}}, fxPart{{1, 300, 340, 1}},
	)
	q := &fxQuery{tags: []string{fxTagSvc, fxTagURL}, fields: []string{fxFldInt}, minTS: 0, maxTS: 10000, ascTS: true}
	schema := q.schema(t)
	var coalesced int
	withFastPaths(true, func() {
		qr := fx.newResult(t, q, schema)
		pullAll(t, qr, func(b *model.MeasureBatch) {
			require.True(t, b.SeriesKnown)
			require.Empty(t, b.SeriesBoundaries)
			svc := constOf(t, b.Tags[0])
			require.Equal(t, "svc-1", svc.Str())
			require.Equal(t, b.RowCount(), svc.Len())
			coalesced++
		})
	})
	require.Equal(t, 1, coalesced)
}

func TestScanWindowDescendingBatchesAreHintedDescending(t *testing.T) {
	verifyNoScanLeaks(t)
	fx := newFixture(t, fxPart{{1, 1, 3000, 1}, {2, 4000, 6000, 1}})
	q := &fxQuery{tags: []string{fxTagSvc}, fields: []string{fxFldInt}, minTS: 0, maxTS: 10000, orderByTS: true, ascTS: false}
	schema := q.schema(t)
	withFastPaths(true, func() {
		pullAll(t, fx.newResult(t, q, schema), func(b *model.MeasureBatch) {
			require.Equal(t, vectorized.TimeOrderDesc, b.TimeOrder)
			require.NotNil(t, b.Selection, "a lent descending block reads through a reversed selection")
			ts := b.Timestamps
			for i := 1; i < len(b.Selection); i++ {
				require.GreaterOrEqual(t, ts[b.Selection[i-1]], ts[b.Selection[i]])
			}
			constOf(t, b.Tags[0])
		})
	})
}

// TestScanWindowConstAndRunsMatchPerRowAndLegacy runs queries three ways: legacy scan,
// window scan with the ConstColumn and runs fast paths off, and with them on. Group-by
// on entity tags, time buckets over many series, and the OAP multi-series shapes must
// agree row for row and byte for byte.
func TestScanWindowConstAndRunsMatchPerRowAndLegacy(t *testing.T) {
	verifyNoScanLeaks(t)
	fixtures := map[string]*fxFixture{"oap": oapFixture(t, 40), "staggered": staggeredFixture(t, 12), "mixed": mixedFixture(t)}
	hidden := map[common.SeriesID]map[string]*modelv1.TagValue{}
	for sid := common.SeriesID(1); sid <= 40; sid++ {
		hidden[sid] = map[string]*modelv1.TagValue{fxTagHid: strTagValue(fmt.Sprintf("hid-%d", sid%3))}
	}
	byTag := func(names ...string) *model.MeasureGroupBy {
		return &model.MeasureGroupBy{TagFamily: fxFamily, TagNames: names}
	}
	bucket := func(width int64, names ...string) *model.MeasureGroupBy {
		gb := &model.MeasureGroupBy{TimeBucket: &model.MeasureTimeBucket{Width: fmt.Sprintf("%dns", width), WidthNanos: width}}
		if len(names) > 0 {
			gb.TagFamily, gb.TagNames = fxFamily, names
		}
		return gb
	}
	aggs := []*model.MeasureAgg{
		{FieldName: fxFldInt, Func: modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM},
		{FieldName: fxFldFlt, Func: modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM},
		{FieldName: fxFldFlt, Func: modelv1.AggregationFunction_AGGREGATION_FUNCTION_MEAN},
		{FieldName: fxFldInt, Func: modelv1.AggregationFunction_AGGREGATION_FUNCTION_MIN},
		{FieldName: fxFldNul, Func: modelv1.AggregationFunction_AGGREGATION_FUNCTION_COUNT},
	}
	groupings := map[string]*model.MeasureGroupBy{
		"scalar":                    nil,
		"entity tag":                byTag(fxTagSvc),
		"hidden entity tag":         byTag(fxTagHid),
		"both entity tags":          byTag(fxTagSvc, fxTagHid),
		"bucket":                    bucket(64),
		"bucket + entity tag":       bucket(64, fxTagSvc),
		"wide bucket + entity tags": bucket(1000, fxTagSvc, fxTagHid),
	}
	orders := map[string]func(q *fxQuery){
		"no time order": func(q *fxQuery) {},
		"time asc":      func(q *fxQuery) { q.orderByTS, q.ascTS = true, true },
		"time desc":     func(q *fxQuery) { q.orderByTS, q.ascTS = true, false },
	}
	for fxName, fx := range fixtures {
		for gName, gb := range groupings {
			for oName, order := range orders {
				for _, agg := range aggs {
					if gb == nil && agg.FieldName == fxFldNul {
						continue
					}
					if gb != nil && gb.TimeBucket != nil && oName != "time asc" {
						// A streaming time bucket needs a time-ascending input (the planner
						// does not stream any other).
						continue
					}
					q := &fxQuery{
						tags: []string{fxTagSvc, fxTagHid}, fields: []string{fxFldInt, fxFldFlt, fxFldNul}, minTS: 0, maxTS: 1 << 20,
						groupBy: gb, agg: agg, stored: hidden,
					}
					order(q)
					if gb == nil {
						q.groupBy = nil
					}
					what := fmt.Sprintf("%s/%s/%s/%s(%s)", fxName, gName, oName, agg.Func, agg.FieldName)
					t.Run(what, func(t *testing.T) {
						legacy := fx.run(t, q, true)
						var plain, fast fxResult
						withFastPaths(false, func() { plain = fx.run(t, q, false) })
						withFastPaths(true, func() { fast = fx.run(t, q, false) })
						requireSameResult(t, legacy, plain, what+" per-row vs legacy")
						requireSameResult(t, plain, fast, what+" fast vs per-row")
					})
				}
			}
		}
	}
}

// TestScanWindowRawQueriesMatchWithConstColumns covers the non-aggregating egress
// (rows and raw frames) over entity tags carried as constants.
func TestScanWindowRawQueriesMatchWithConstColumns(t *testing.T) {
	verifyNoScanLeaks(t)
	fx := mixedFixture(t)
	hidden := map[common.SeriesID]map[string]*modelv1.TagValue{1: {fxTagHid: strTagValue("hidden-1")}, 6: {fxTagHid: strTagValue("hidden-6")}}
	for _, ordered := range []bool{false, true} {
		for _, asc := range []bool{true, false} {
			for _, lim := range [][2]uint32{{0, 0}, {5, 70}} {
				q := &fxQuery{
					tags: []string{fxTagSvc, fxTagHid, fxTagURL, fxTagN}, fields: []string{fxFldInt, fxFldStr}, minTS: 0, maxTS: 30000,
					stored: hidden, orderByTS: ordered, ascTS: asc, offset: lim[0], limit: lim[1],
				}
				what := fmt.Sprintf("ordered=%v asc=%v limit=%v", ordered, asc, lim)
				t.Run(what, func(t *testing.T) {
					legacy := fx.run(t, q, true)
					var fast fxResult
					withFastPaths(true, func() { fast = fx.run(t, q, false) })
					requireSameResult(t, legacy, fast, what)
				})
			}
		}
	}
}

// TestScanWindowEntityTagFilterKeepsOrDropsWholeBatches filters a scan on an entity tag
// and compares the rows with the same filter over typed columns.
func TestScanWindowEntityTagFilterKeepsOrDropsWholeBatches(t *testing.T) {
	verifyNoScanLeaks(t)
	fx := oapFixture(t, 24)
	q := &fxQuery{tags: []string{fxTagSvc, fxTagURL}, fields: []string{fxFldInt}, minTS: 0, maxTS: 1 << 20, orderByTS: true, ascTS: true}
	schema := q.schema(t)
	keep := func(col vectorized.Column, row int) bool {
		if c, ok := vectorized.AsConst(col); ok {
			return c.Str() == "svc-1" || c.Str() == "svc-3"
		}
		v := col.(*vectorized.TypedColumn[string]).Data()[row]
		return v == "svc-1" || v == "svc-3"
	}
	run := func(on bool) (rows string, filtered, wholeDrops int) {
		withFastPaths(on, func() {
			qr := fx.newResult(t, q, schema)
			pool := vectorizedBatchPool(schema)
			source := newTestBatchSource(qr, schema, pool)
			defer source.Close()
			filter := vmeasure.NewBatchColumnFilter(schema, tagCol(schema, fxTagSvc), keep)
			for {
				b, err := source.NextBatch(context.Background())
				require.NoError(t, err)
				if b == nil {
					return
				}
				require.NoError(t, filter.Process(context.Background(), b))
				filtered++
				if b.ActiveLen() == 0 {
					wholeDrops++
				}
				for _, r := range activeRows(b) {
					rows += fmt.Sprintf("%v/%v/%v;", b.Columns[schema.TimestampIndex()].(*vectorized.TypedColumn[int64]).Data()[r],
						colString(b.Columns[tagCol(schema, fxTagSvc)], r), colString(b.Columns[tagCol(schema, fxTagURL)], r))
				}
				pool.Put(b)
			}
		})
		return
	}
	wantRows, _, _ := run(false)
	gotRows, filtered, wholeDrops := run(true)
	require.NotEmpty(t, wantRows)
	require.Equal(t, wantRows, gotRows)
	require.Positive(t, filtered)
	require.Positive(t, wholeDrops, "batches of series the filter rejects are dropped whole")
}

func tagCol(schema *vectorized.BatchSchema, name string) int {
	idx, ok := schema.TagIndex(fxFamily, name)
	if !ok {
		panic("no tag column " + name)
	}
	return idx
}

func activeRows(b *vectorized.RecordBatch) []int {
	if b.Selection == nil {
		rows := make([]int, b.Len)
		for i := range rows {
			rows[i] = i
		}
		return rows
	}
	rows := make([]int, len(b.Selection))
	for i, r := range b.Selection {
		rows[i] = int(r)
	}
	return rows
}

func colString(col vectorized.Column, row int) string {
	if c, ok := vectorized.AsConst(col); ok {
		return c.Expand().(*vectorized.TypedColumn[string]).Data()[0]
	}
	return col.(*vectorized.TypedColumn[string]).Data()[row]
}

func TestScanWindowMergePagesKeepTypedEntityColumns(t *testing.T) {
	verifyNoScanLeaks(t)
	// Two overlapping blocks of one series are merged: the page is one series but is
	// not lent from one cursor, so its entity tags stay typed (the hints still apply).
	fx := newFixture(t, fxPart{{1, 1, 600, 1}}, fxPart{{1, 300, 900, 2}})
	q := &fxQuery{tags: []string{fxTagSvc}, fields: []string{fxFldInt}, minTS: 0, maxTS: 10000, ascTS: true}
	schema := q.schema(t)
	var pages int
	withFastPaths(true, func() {
		pullAll(t, fx.newResult(t, q, schema), func(b *model.MeasureBatch) {
			pages++
			require.True(t, b.SeriesKnown)
			_, isConst := vectorized.AsConst(b.Tags[0])
			require.False(t, isConst)
		})
	})
	require.Positive(t, pages)
}

// TestScanWindowEntityTagOfAnotherStoredTypeIsAConstantNull checks the schema-type
// mismatch case: a column stored as another type than the schema declares reads as
// null, row by row on the legacy path, so a lent batch carries it as a null constant.
func TestScanWindowEntityTagOfAnotherStoredTypeIsAConstantNull(t *testing.T) {
	verifyNoScanLeaks(t)
	fx := newFixture(t, fxPart{{1, 1, 600, 1}, {2, 1, 600, 1}})
	declared := fxTagTypes[fxTagSvc]
	fxTagTypes[fxTagSvc] = pbv1.ValueTypeInt64
	defer func() { fxTagTypes[fxTagSvc] = declared }()

	q := &fxQuery{tags: []string{fxTagSvc, fxTagURL}, fields: []string{fxFldInt}, minTS: 0, maxTS: 10000, ascTS: true}
	schema := q.schema(t)
	var lent int
	withFastPaths(true, func() {
		pullAll(t, fx.newResult(t, q, schema), func(b *model.MeasureBatch) {
			lent++
			svc := constOf(t, b.Tags[0])
			require.True(t, svc.IsNullValue(), "a stored type that differs from the schema's reads as null")
			require.Equal(t, b.RowCount(), svc.Len())
		})
	})
	require.Positive(t, lent)
	legacy := fx.run(t, q, true)
	var fast fxResult
	withFastPaths(true, func() { fast = fx.run(t, q, false) })
	requireSameResult(t, legacy, fast, "stored type differs from the schema")
}
