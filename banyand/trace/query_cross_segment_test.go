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

package trace

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/sidx"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/native"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	vtrace "github.com/apache/skywalking-banyandb/pkg/query/vectorized/trace"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// crossSegmentFixture wires a real day-sharded TSDB (one shard) anchored on a
// fixed local-midnight boundary, 10 days in the past so it is comfortably
// inside the 60-day TTL opened by openCrossSegmentTSDB regardless of when the
// test runs.
//
// Day A = [boundary-24h, boundary), Day B = [boundary, boundary+24h), Day C =
// [boundary+24h, boundary+48h). A single trace has spans in all three: one
// just before the boundary (day A), one just after (day B, the straddling
// partner that the bug drops), and one deep into day C -- more than
// traceCompletionWindow past the widened query range, which must stay
// unfetched to pin the bound documented on traceCompletionWindow.
type crossSegmentFixture struct {
	boundary time.Time
	tsdb     storage.TSDB[*tsTable, option]
	cleanup  func()
	traceID  string
}

func newCrossSegmentFixture(t testing.TB, groupName string) *crossSegmentFixture {
	t.Helper()
	tmpPath, defFn := test.Space(require.New(t))

	ir := storage.IntervalRule{Unit: storage.DAY, Num: 1}
	db := openCrossSegmentTSDB(t, tmpPath, groupName, ir)

	today := time.Now().Local()
	boundary := time.Date(today.Year(), today.Month(), today.Day(), 0, 0, 0, 0, time.Local).AddDate(0, 0, -10)

	return &crossSegmentFixture{
		tsdb:     db,
		boundary: boundary,
		traceID:  "straddle-trace",
		cleanup: func() {
			db.Close()
			defFn()
		},
	}
}

// openCrossSegmentTSDB opens a real 1-shard, day-sharded TSDB for the fixture.
// It mirrors write_data_segmentref_test.go's openTestTSDBWithInterval but is
// generalized over testing.TB so both tests and benchmarks can share it.
func openCrossSegmentTSDB(t testing.TB, tmpPath, groupName string, ir storage.IntervalRule) storage.TSDB[*tsTable, option] {
	t.Helper()
	opts := storage.TSDBOpts[*tsTable, option]{
		ShardNum:        1,
		Location:        filepath.Join(tmpPath, "tab"),
		TSTableCreator:  newTSTable,
		SegmentInterval: ir,
		TTL:             storage.IntervalRule{Unit: ir.Unit, Num: 60},
		RootLeaseFactory: func(lock fs.File, root string) (storage.RootLease, error) {
			return native.NewFileRootLease(lock, root)
		},
		Option: option{protector: protector.Nop{}, mergePolicy: newDefaultMergePolicyForTesting()},
	}
	require.NoError(t, os.MkdirAll(opts.Location, storage.DirPerm))
	ctx := common.SetPosition(
		context.WithValue(context.Background(), logger.ContextKey, logger.GetLogger("test-cross-segment")),
		func(p common.Position) common.Position {
			p.Database = "test-cross-segment"
			return p
		},
	)
	db, err := storage.OpenTSDB[*tsTable, option](ctx, opts, nil, groupName)
	require.NoError(t, err)
	return db
}

// writeSpan writes one span of the fixture's trace at ts into whichever daily
// segment contains it, without any secondary index (sidx) data: Phase 2 only
// needs the core part's bloom filter to find it.
func (f *crossSegmentFixture) writeSpan(t testing.TB, ts time.Time, spanID string) {
	t.Helper()
	seg, err := f.tsdb.CreateSegmentIfNotExist(ts)
	require.NoError(t, err)
	defer seg.DecRef()
	tst, err := seg.CreateTSTableIfNotExist(common.ShardID(0))
	require.NoError(t, err)
	tst.mustAddTraces(f.singleSpanBatch(ts, spanID), nil)
}

// writeSpanWithDurationIndex writes one span exactly like writeSpan, plus a
// real on-disk sidx row on a non-timestamp-key index named indexName (its key
// is an arbitrary "duration" value, unrelated to ts), with ts stored as the
// "timestamp" tag -- mirroring write_standalone.go's processIndexRules/Rev so
// an ORDER BY query on that index can resolve the trace via Phase 1.
func (f *crossSegmentFixture) writeSpanWithDurationIndex(t testing.TB, ts time.Time, spanID string, indexName string, durationKey int64) {
	t.Helper()
	seg, err := f.tsdb.CreateSegmentIfNotExist(ts)
	require.NoError(t, err)
	defer seg.DecRef()
	tst, err := seg.CreateTSTableIfNotExist(common.ShardID(0))
	require.NoError(t, err)

	sidxInstance, err := tst.getOrCreateSidx(indexName)
	require.NoError(t, err)
	segRange := seg.GetTimeRange()
	minTS, maxTS := segRange.Start.UnixNano(), segRange.End.UnixNano()
	memPart, convertErr := sidxInstance.ConvertToMemPart([]sidx.WriteRequest{{
		Data:     encodeTraceIDForTest(f.traceID),
		SeriesID: common.SeriesID(1),
		Key:      durationKey,
		Tags: []sidx.Tag{
			{Name: "timestamp", Value: convert.Int64ToBytes(ts.UnixNano()), ValueType: pbv1.ValueTypeTimestamp},
		},
	}}, segRange.Start.UnixNano(), &minTS, &maxTS)
	require.NoError(t, convertErr)

	tst.mustAddTraces(f.singleSpanBatch(ts, spanID), map[string]*sidx.MemPart{indexName: memPart})
}

func (f *crossSegmentFixture) singleSpanBatch(ts time.Time, spanID string) *traces {
	return &traces{
		traceIDs:   []string{f.traceID},
		timestamps: []int64{ts.UnixNano()},
		tags: [][]*tagValue{{
			{tag: "strTag", valueType: pbv1.ValueTypeStr, value: []byte(spanID), valueArr: nil},
		}},
		spans:   [][]byte{[]byte(spanID)},
		spanIDs: []string{spanID},
	}
}

// narrowRange covers only day A's tail, exactly reproducing the bug report:
// a query whose range touches only one side of a segment boundary.
func (f *crossSegmentFixture) narrowRange() timestamp.TimeRange {
	return timestamp.NewTimeRange(f.boundary.Add(-60*time.Second), f.boundary, true, false)
}

const (
	spanDayA = "spanA-dayA"
	spanDayB = "spanB-dayB"
	spanDayC = "spanC-dayC-beyond-window"
)

// TestTraceIDQueryCompletesSpansAcrossSegmentBoundary is the trace-ID
// reproduction of the segment-boundary bug (GH: a trace whose spans straddle
// a segment boundary comes back incomplete when the query range covers only
// one side). Before the Phase 2 widening fix, this test fails: only spanA is
// returned. After the fix, spanA and spanB are both returned, and spanC (more
// than traceCompletionWindow past the widened range, in a third segment) is
// still excluded, pinning the documented bound.
func TestTraceIDQueryCompletesSpansAcrossSegmentBoundary(t *testing.T) {
	fixture := newCrossSegmentFixture(t, "cross-segment-traceid")
	defer fixture.cleanup()

	fixture.writeSpan(t, fixture.boundary.Add(-30*time.Second), spanDayA)
	fixture.writeSpan(t, fixture.boundary.Add(30*time.Second), spanDayB)
	fixture.writeSpan(t, fixture.boundary.Add(30*time.Hour), spanDayC)

	tr := &trace{
		pm:         protector.Nop{},
		vectorized: vtrace.VectorizedConfig{BatchSize: 10, QueryMemoryMiB: 100},
		schema: &databasev1.Trace{
			Metadata:         &commonv1.Metadata{Name: "cross-segment-traceid", Group: "cross-segment-traceid"},
			TraceIdTagName:   "trace_id",
			SpanIdTagName:    "span_id",
			TimestampTagName: "timestamp",
		},
	}
	tr.tsdb.Store(fixture.tsdb)

	timeRange := fixture.narrowRange()
	result, err := tr.Query(context.Background(), model.TraceQueryOptions{
		TraceIDs:  []string{fixture.traceID},
		TimeRange: &timeRange,
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	defer result.Release()

	got := collectResults(t, result)
	require.Len(t, got, 1, "trace-ID query must resolve exactly the one requested trace")
	require.ElementsMatch(t, []string{spanDayA, spanDayB}, got[0].SpanIDs,
		"must recover the neighboring segment's span (the fix) but not a span beyond "+
			"traceCompletionWindow in a third segment (the documented bound)")
}

// TestOrderByQueryCompletesSpansAcrossSegmentBoundary is the ORDER BY
// non-timestamp-key counterpart of the segment-boundary bug: the qualifying
// span is resolved via a real on-disk secondary index keyed on an arbitrary
// "duration"-like value (not the timestamp), exactly as an "order by duration
// desc" query would. Before the fix, Phase 2 only scans day A's segment (the
// one Phase 1's index lives in) and spanB is silently dropped.
func TestOrderByQueryCompletesSpansAcrossSegmentBoundary(t *testing.T) {
	fixture := newCrossSegmentFixture(t, "cross-segment-orderby")
	defer fixture.cleanup()

	const indexName = "duration_idx"
	fixture.writeSpanWithDurationIndex(t, fixture.boundary.Add(-30*time.Second), spanDayA, indexName, 999)
	fixture.writeSpan(t, fixture.boundary.Add(30*time.Second), spanDayB)
	fixture.writeSpan(t, fixture.boundary.Add(30*time.Hour), spanDayC)

	tr := &trace{
		pm:         protector.Nop{},
		vectorized: vtrace.VectorizedConfig{BatchSize: 10, QueryMemoryMiB: 100},
		schema: &databasev1.Trace{
			Metadata:         &commonv1.Metadata{Name: "cross-segment-orderby", Group: "cross-segment-orderby"},
			TraceIdTagName:   "trace_id",
			SpanIdTagName:    "span_id",
			TimestampTagName: "timestamp",
		},
	}
	tr.tsdb.Store(fixture.tsdb)
	durationIndexRule := &databasev1.IndexRule{
		Metadata: &commonv1.Metadata{Name: indexName},
		Tags:     []string{"duration"},
	}
	tr.OnIndexUpdate([]*databasev1.IndexRule{durationIndexRule})

	timeRange := fixture.narrowRange()
	result, err := tr.Query(context.Background(), model.TraceQueryOptions{
		Order:     &index.OrderBy{Index: durationIndexRule, Sort: modelv1.Sort_SORT_DESC},
		TimeRange: &timeRange,
		MinVal:    math.MinInt64,
		MaxVal:    math.MaxInt64,
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	defer result.Release()

	got := collectResults(t, result)
	require.Len(t, got, 1, "order-by query must resolve exactly the one qualifying trace")
	require.ElementsMatch(t, []string{spanDayA, spanDayB}, got[0].SpanIDs,
		"must recover the neighboring segment's span (the fix) but not a span beyond "+
			"traceCompletionWindow in a third segment (the documented bound)")
}

func TestRangesCover(t *testing.T) {
	day := func(d int) time.Time { return time.Date(2026, 1, d, 0, 0, 0, 0, time.Local) }
	segment := timestamp.NewSectionTimeRange
	inclusive := timestamp.NewInclusiveTimeRange
	tests := []struct {
		target timestamp.TimeRange
		name   string
		ranges []timestamp.TimeRange
		want   bool
	}{
		{
			name: "inside one segment", ranges: []timestamp.TimeRange{segment(day(1), day(2))},
			target: inclusive(day(1).Add(2*time.Hour), day(1).Add(5*time.Hour)), want: true,
		},
		{
			name: "reaches the exclusive end", ranges: []timestamp.TimeRange{segment(day(1), day(2))},
			target: inclusive(day(1).Add(2*time.Hour), day(2)), want: false,
		},
		{
			name: "starts before the first segment", ranges: []timestamp.TimeRange{segment(day(1), day(2))},
			target: inclusive(day(1).Add(-time.Minute), day(1).Add(time.Hour)), want: false,
		},
		{
			name: "two adjacent segments, unsorted", ranges: []timestamp.TimeRange{segment(day(2), day(3)), segment(day(1), day(2))},
			target: inclusive(day(1).Add(23*time.Hour), day(2).Add(time.Hour)), want: true,
		},
		{
			name: "gap between segments", ranges: []timestamp.TimeRange{segment(day(1), day(2)), segment(day(3), day(4))},
			target: inclusive(day(1).Add(23*time.Hour), day(3).Add(time.Hour)), want: false,
		},
		{name: "no segments", target: inclusive(day(1), day(1).Add(time.Hour)), want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, rangesCover(tc.ranges, tc.target))
		})
	}
}
