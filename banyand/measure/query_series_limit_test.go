// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

const (
	limitTestMeasure = "limit_measure"
	limitScoreRule   = uint32(5)
	limitGroupRule   = uint32(6)
	// limitShards spreads the series over several shards of each segment, as a live measure does.
	limitShards = 2
)

// limitTestResolver resolves the tag a criteria filters on to the engine field of its index rule.
type limitTestResolver struct{}

func (limitTestResolver) Field(tagName string) (string, string, bool) {
	if tagName == "grp" {
		return index.FieldKey{IndexRuleID: limitGroupRule}.Marshal(), "", true
	}
	return "", "", false
}

// limitSeries is one series of a normal-mode measure and the data points it holds. Every series carries a point class that
// decides where its points fall against the query window: a limited query ranks series without knowing that.
type limitSeries struct {
	inst   string
	group  string
	scores []int64 // the score the series document has in each segment
	times  []time.Time
	dups   []time.Time // points written a second time, as a newer version, in another part
	id     common.SeriesID
}

// limitFixture is a normal-mode measure with a sort-marked int index-rule tag over several day segments.
type limitFixture struct {
	db     storage.TSDB[*tsTable, option]
	m      *measure
	rule   *databasev1.IndexRule
	window timestamp.TimeRange
	series []*limitSeries
}

func newLimitSchema() (*databasev1.Measure, []*databasev1.IndexRule) {
	schema := &databasev1.Measure{
		Metadata: &commonv1.Metadata{Name: limitTestMeasure, Group: "limit_group"},
		Entity:   &databasev1.Entity{TagNames: []string{"svc", "inst"}},
		TagFamilies: []*databasev1.TagFamilySpec{{Name: "default", Tags: []*databasev1.TagSpec{
			{Name: "svc", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "inst", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "score", Type: databasev1.TagType_TAG_TYPE_INT},
			{Name: "grp", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "note", Type: databasev1.TagType_TAG_TYPE_STRING},
		}}},
		Fields: []*databasev1.FieldSpec{{
			Name: "value", FieldType: databasev1.FieldType_FIELD_TYPE_FLOAT,
			EncodingMethod:    databasev1.EncodingMethod_ENCODING_METHOD_GORILLA,
			CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
		}},
	}
	rules := []*databasev1.IndexRule{
		{Metadata: &commonv1.Metadata{Id: limitScoreRule, Name: "score"}, Tags: []string{"score"}, Type: databasev1.IndexRule_TYPE_INVERTED},
		{Metadata: &commonv1.Metadata{Id: limitGroupRule, Name: "grp"}, Tags: []string{"grp"}, Type: databasev1.IndexRule_TYPE_INVERTED, NoSort: true},
	}
	return schema, rules
}

// classBase spaces the scores of the ten point classes so that each class ties within itself.
var classBase = []int64{-40, -35, -10, 0, 5, -20, 15, 20, 25, 30}

// buildLimitFixture creates the measure, with one day segment per entry of days, and writes services of series. The point
// class of series i is i % 10, relative to the query window [window.Start, window.End]:
//
//	0 no point at all          1 only points outside the window     2 points on and just inside/outside both bounds
//	3 many points inside        4 one point inside                   5 three points, each written twice (two versions)
//	6, 8 two points near the start of the window      7, 9 two points near its end
//
// The first series of an ascending order are those with no point in the window (classes 0 and 1), then those whose points
// are all written twice (class 5), then the rest, so a count of rows instead of distinct points accepts a round too early.
// segmentScores returns the score of series i in segment d.
func buildLimitFixture(t *testing.T, days []time.Time, window timestamp.TimeRange, count int,
	segmentScores func(i, day int) int64,
) *limitFixture {
	t.Helper()
	verifyNoScanLeaks(t)
	db := openTestTSDBForRefTest(t, t.TempDir(), limitShards, nil)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	schema, rules := newLimitSchema()
	is := indexSchema{indexRules: rules}
	is.parse(schema)
	m := &measure{schema: schema, pm: protector.Nop{}, name: limitTestMeasure, group: schema.GetMetadata().GetGroup()}
	m.indexSchema.Store(is)
	m.tsdb.Store(db)
	fx := &limitFixture{m: m, db: db, window: window, rule: rules[0]}

	for i := 0; i < count; i++ {
		s := &limitSeries{inst: fmt.Sprintf("inst_%04d", i), group: "g" + strconv.Itoa(i%3)}
		series := pbv1.Series{Subject: limitTestMeasure, EntityValues: limitEntity("svc_a", s.inst)}
		require.NoError(t, series.Marshal())
		s.id = series.ID
		for d := range days {
			s.scores = append(s.scores, segmentScores(i, d))
		}
		start, end := window.Start, window.End
		switch i % 10 {
		case 0:
		case 1:
			s.times = []time.Time{start.Add(-3 * time.Hour), start.Add(-2 * time.Hour), end.Add(time.Hour)}
		case 2:
			s.times = []time.Time{start.Add(-time.Second), start, start.Add(time.Minute), end.Add(-time.Minute), end, end.Add(time.Second)}
		case 3:
			for k := 0; k < 50; k++ {
				s.times = append(s.times, start.Add(time.Duration(10+k)*time.Second))
			}
		case 4:
			s.times = []time.Time{start.Add(90 * time.Minute)}
		case 5:
			s.times = []time.Time{start.Add(time.Minute), start.Add(2 * time.Minute), start.Add(3 * time.Minute)}
			s.dups = s.times
		case 7, 9:
			s.times = []time.Time{end.Add(-5 * time.Minute), end.Add(-7 * time.Minute)}
		default:
			s.times = []time.Time{start.Add(5 * time.Minute), start.Add(7 * time.Minute)}
		}
		fx.series = append(fx.series, s)
	}
	// Series of another service share the segments, so that the entity prefix has to select.
	for i := 0; i < 20; i++ {
		other := &limitSeries{inst: fmt.Sprintf("other_%04d", i), group: "g0"}
		series := pbv1.Series{Subject: limitTestMeasure, EntityValues: limitEntity("svc_b", other.inst)}
		require.NoError(t, series.Marshal())
		other.id = series.ID
		for range days {
			other.scores = append(other.scores, -1000+int64(i))
		}
		other.times = []time.Time{window.Start.Add(time.Minute)}
		fx.series = append(fx.series, other)
	}

	for d, day := range days {
		seg, err := db.CreateSegmentIfNotExist(day)
		require.NoError(t, err)
		var docs index.Documents
		for _, s := range fx.series {
			series := pbv1.Series{Subject: limitTestMeasure, EntityValues: limitEntity(seriesService(s), s.inst)}
			require.NoError(t, series.Marshal())
			docs = append(docs, index.Document{DocID: uint64(s.id), EntityValues: series.Buffer, Fields: limitFields(s, d)})
		}
		require.NoError(t, seg.IndexDB().Insert(docs))
		lo, hi := day.Truncate(24*time.Hour), day.Truncate(24*time.Hour).Add(24*time.Hour)
		for shard := 0; shard < limitShards; shard++ {
			tst, err := seg.CreateTSTableIfNotExist(common.ShardID(shard))
			require.NoError(t, err)
			fx.write(tst, shard, lo, hi, 1, func(s *limitSeries) []time.Time { return s.times })
			fx.write(tst, shard, lo, hi, 2, func(s *limitSeries) []time.Time { return s.dups })
		}
		seg.DecRef()
	}
	return fx
}

func seriesService(s *limitSeries) string {
	if len(s.inst) > 5 && s.inst[:5] == "other" {
		return "svc_b"
	}
	return "svc_a"
}

func limitEntity(service, instance string) []*modelv1.TagValue {
	return []*modelv1.TagValue{strTagValue(service), strTagValue(instance)}
}

// limitFields writes the index fields of a series document the way the measure writer does for a normal-mode measure: the
// int tag of a sort-marked rule carries the ladder, the group tag is an unsorted rule.
func limitFields(s *limitSeries, segment int) []index.Field {
	score := index.NewBytesField(index.FieldKey{IndexRuleID: limitScoreRule}, convert.Int64ToBytes(s.scores[segment]))
	score.Store, score.Index = true, true
	score.MarkNumericTag(databasev1.TagType_TAG_TYPE_INT, int64TagValue(s.scores[segment]))
	group := index.NewBytesField(index.FieldKey{IndexRuleID: limitGroupRule}, []byte(s.group))
	group.Store, group.Index, group.NoSort = true, true, true
	return []index.Field{score, group}
}

// write adds the points pick returns of the series of one shard that fall inside [lo, hi) to tst as one memory part with the
// given version.
func (fx *limitFixture) write(tst *tsTable, shard int, lo, hi time.Time, version int64, pick func(*limitSeries) []time.Time) {
	dps := &dataPoints{}
	for _, s := range fx.series {
		if int(uint64(s.id)%limitShards) != shard {
			continue
		}
		for _, ts := range pick(s) {
			if ts.Before(lo) || !ts.Before(hi) {
				continue
			}
			dps.seriesIDs = append(dps.seriesIDs, s.id)
			dps.timestamps = append(dps.timestamps, ts.UnixNano())
			dps.versions = append(dps.versions, version)
			dps.tagFamilies = append(dps.tagFamilies, []nameValues{{
				name:   "default",
				values: []*nameValue{{name: "note", valueType: pbv1.ValueTypeStr, value: []byte("n" + s.inst)}},
			}})
			dps.fields = append(dps.fields, nameValues{
				name:   "value",
				values: []*nameValue{{name: "value", valueType: pbv1.ValueTypeFloat64, value: convert.Float64ToBytes(float64(version))}},
			})
		}
	}
	tst.mustAddDataPoints(dps)
}

type limitPoint struct {
	sid common.SeriesID
	ts  int64
}

// queryLimitPoints runs one query and returns every data point of the result in order.
func (fx *limitFixture) queryOptions(sort modelv1.Sort, limit int, criteria *modelv1.Criteria) model.MeasureQueryOptions {
	window := fx.window
	return model.MeasureQueryOptions{
		Name:      limitTestMeasure,
		TimeRange: &window,
		Entities:  [][]*modelv1.TagValue{{strTagValue("svc_a"), pbv1.AnyTagValue}},
		TagProjection: []model.TagProjection{
			{Family: "default", Names: []string{"svc", "inst", "score", "note"}},
		},
		FieldProjection: []string{"value"},
		Order:           &index.OrderBy{Type: index.OrderByTypeIndex, Sort: sort, Index: fx.rule},
		Criteria:        criteria,
		Fields:          limitTestResolver{},
		Limit:           limit,
	}
}

func (fx *limitFixture) queryLimitPoints(t *testing.T, sort modelv1.Sort, limit int, criteria *modelv1.Criteria) []limitPoint {
	t.Helper()
	points, err := fx.queryLimitPointsCtx(context.Background(), sort, limit, criteria)
	require.NoError(t, err)
	return points
}

func (fx *limitFixture) queryLimitPointsCtx(ctx context.Context, sort modelv1.Sort, limit int, criteria *modelv1.Criteria) ([]limitPoint, error) {
	result, err := fx.m.Query(ctx, fx.queryOptions(sort, limit, criteria))
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, nil
	}
	defer result.Release()
	var points []limitPoint
	batches, isBatch := result.(model.MeasureBatchResult)
	if !isBatch {
		return nil, fmt.Errorf("a normal-mode result must pull batches")
	}
	for {
		b, pullErr := batches.PullBatch(ctx)
		if pullErr != nil {
			return nil, pullErr
		}
		if b == nil {
			return points, nil
		}
		if b.Selection != nil {
			for _, row := range b.Selection {
				points = append(points, limitPoint{sid: b.SeriesIDs[row], ts: b.Timestamps[row]})
			}
		} else {
			for row := range b.Timestamps {
				points = append(points, limitPoint{sid: b.SeriesIDs[row], ts: b.Timestamps[row]})
			}
		}
		b.Release()
	}
}

func limitTestWindows() (single, double timestamp.TimeRange, day0, day1 time.Time) {
	// The test database keeps one day of data, so the segments are today's and tomorrow's.
	day0 = time.Now().UTC().Truncate(24 * time.Hour)
	day1 = day0.Add(24 * time.Hour)
	single = timestamp.NewInclusiveTimeRange(day0.Add(6*time.Hour), day0.Add(18*time.Hour))
	double = timestamp.NewInclusiveTimeRange(day0.Add(22*time.Hour), day1.Add(2*time.Hour))
	return single, double, day0, day1
}

// requireLimitedEqualsUnlimited asserts, for every limit and both directions, that the first limit points of the limited query
// are those of the unlimited query, and returns how many limits needed more than one round and how many fell back.
func requireLimitedEqualsUnlimited(t *testing.T, fx *limitFixture, criteria *modelv1.Criteria) (extraRounds, fallbacks int64) {
	t.Helper()
	for _, sort := range []modelv1.Sort{modelv1.Sort_SORT_ASC, modelv1.Sort_SORT_DESC, modelv1.Sort_SORT_UNSPECIFIED} {
		want := fx.queryLimitPoints(t, sort, 0, criteria)
		require.NotEmpty(t, want)
		for _, limit := range []int{1, 2, 3, 10, 40, 64, 100, 500, 100000} {
			roundsBefore, fallbacksBefore := seriesLimitRounds.Load(), seriesLimitFallbacks.Load()
			got := fx.queryLimitPoints(t, sort, limit, criteria)
			rounds := seriesLimitRounds.Load() - roundsBefore
			ran := rounds + seriesLimitFallbacks.Load() - fallbacksBefore
			require.GreaterOrEqual(t, ran, int64(1), "every limited query runs at least one round")
			extraRounds += ran - 1
			fallbacks += seriesLimitFallbacks.Load() - fallbacksBefore
			n := min(limit, len(want))
			require.GreaterOrEqualf(t, len(got), n, "%s limit %d returned too few points", sort, limit)
			require.Equalf(t, want[:n], got[:n], "%s limit %d", sort, limit)
		}
	}
	return extraRounds, fallbacks
}

func limitScoreOf(i int, day int) int64 {
	if day == 0 {
		return 2 * (classBase[i%10] + int64((i/10)%3)) // even scores
	}
	return 2*(classBase[9-i%10]+int64((i/10)%2)) + 1 // odd scores: no tie between segments, so the merge order is defined
}

func limitGroupCriteria(group string) *modelv1.Criteria {
	return &modelv1.Criteria{Exp: &modelv1.Criteria_Condition{Condition: &modelv1.Condition{
		Name: "grp", Op: modelv1.Condition_BINARY_OP_EQ, Value: strTagValue(group),
	}}}
}

// A normal-mode query ordered by an index rule with a limit returns the same first points as the query over every series:
// ties, series without a point in the window (they force more rounds), series whose points sit on the bounds, series with
// two versions of a point, and a criteria that keeps a third of the series.
func TestQueryLimitedSeriesSingleSegment(t *testing.T) {
	window, _, day0, _ := limitTestWindows()
	fx := buildLimitFixture(t, []time.Time{day0}, window, 200, limitScoreOf)
	for name, criteria := range map[string]*modelv1.Criteria{"none": nil, "group": limitGroupCriteria("g1")} {
		t.Run(name, func(t *testing.T) {
			extra, _ := requireLimitedEqualsUnlimited(t, fx, criteria)
			require.Positive(t, extra, "series without points in the window must force a second round")
		})
	}
}

// The same over two segments whose documents of one series carry different scores, so a series ranks by its better one.
func TestQueryLimitedSeriesTwoSegments(t *testing.T) {
	_, window, day0, day1 := limitTestWindows()
	fx := buildLimitFixture(t, []time.Time{day0, day1}, window, 200, limitScoreOf)
	for name, criteria := range map[string]*modelv1.Criteria{"none": nil, "group": limitGroupCriteria("g2")} {
		t.Run(name, func(t *testing.T) {
			extra, _ := requireLimitedEqualsUnlimited(t, fx, criteria)
			require.Positive(t, extra)
		})
	}
}

// Past the last limited round the query asks for every series, which is the unlimited query.
func TestQueryLimitedSeriesFallsBackToEverySeries(t *testing.T) {
	old := seriesRoundCap
	seriesRoundCap = 100
	t.Cleanup(func() { seriesRoundCap = old })
	window, _, day0, _ := limitTestWindows()
	fx := buildLimitFixture(t, []time.Time{day0}, window, 200, limitScoreOf)
	_, fallbacks := requireLimitedEqualsUnlimited(t, fx, nil)
	require.Positive(t, fallbacks)
}

// Only an order by an index rule limits series; a time order does not use the index.
func TestQueryLimitSeriesIgnoredForTimeOrder(t *testing.T) {
	window, _, day0, _ := limitTestWindows()
	fx := buildLimitFixture(t, []time.Time{day0}, window, 30, limitScoreOf)
	before := seriesLimitRounds.Load() + seriesLimitFallbacks.Load()
	timeWindow := fx.window
	result, err := fx.m.Query(context.Background(), model.MeasureQueryOptions{
		Name: limitTestMeasure, TimeRange: &timeWindow, Limit: 5,
		Entities:        [][]*modelv1.TagValue{{strTagValue("svc_a"), pbv1.AnyTagValue}},
		FieldProjection: []string{"value"},
		Order:           &index.OrderBy{Type: index.OrderByTypeTime, Sort: modelv1.Sort_SORT_ASC},
	})
	require.NoError(t, err)
	result.Release()
	require.Equal(t, before, seriesLimitRounds.Load()+seriesLimitFallbacks.Load())
}

// With exclusive bounds the scan still reads the bounds, so the first points are those of the unlimited query.
func TestQueryLimitedSeriesExclusiveRange(t *testing.T) {
	window, _, day0, _ := limitTestWindows()
	window = timestamp.NewTimeRange(window.Start, window.End, false, false)
	fx := buildLimitFixture(t, []time.Time{day0}, window, 200, limitScoreOf)
	extra, _ := requireLimitedEqualsUnlimited(t, fx, nil)
	require.Positive(t, extra)
}

// guaranteedPoints counts the distinct points of the scanned series strictly inside the bounds: not a row twice, and not a
// point that sits on a bound, whatever the range flags say.
func TestGuaranteedPointsCountsDistinctPointsStrictlyInside(t *testing.T) {
	window, _, day0, _ := limitTestWindows()
	fx := buildLimitFixture(t, []time.Time{day0}, window, 200, limitScoreOf)
	want := 0
	for _, s := range fx.series {
		if seriesService(s) != "svc_a" {
			continue
		}
		distinct := map[int64]struct{}{}
		for _, ts := range append(append([]time.Time(nil), s.times...), s.dups...) {
			if ts.After(window.Start) && ts.Before(window.End) {
				distinct[ts.UnixNano()] = struct{}{}
			}
		}
		want += len(distinct)
	}
	segments, err := fx.db.SelectSegments(window, true)
	require.NoError(t, err)
	defer func() {
		for i := range segments {
			segments[i].DecRef()
		}
	}()
	mqo := fx.queryOptions(modelv1.Sort_SORT_ASC, 0, nil)
	series := []*pbv1.Series{{Subject: limitTestMeasure, EntityValues: []*modelv1.TagValue{strTagValue("svc_a"), pbv1.AnyTagValue}}}
	result, _, err := fx.m.queryRound(context.Background(), mqo, series, segments, 0)
	require.NoError(t, err)
	require.NotNil(t, result)
	defer result.Release()
	start, end := window.Start.UnixNano(), window.End.UnixNano()
	counted, err := guaranteedPoints(context.Background(), result, start, end, 1<<30)
	require.NoError(t, err)
	require.Equal(t, want, counted)
	counted, err = guaranteedPoints(context.Background(), result, start, end, 5)
	require.NoError(t, err)
	require.Equal(t, 5, counted, "it stops at the limit")
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = guaranteedPoints(canceled, result, start, end, 1<<30)
	require.ErrorIs(t, err, context.Canceled)
}

// A query whose context ends between two rounds returns the error and leaves nothing behind (the fixture checks for leaks).
func TestQueryLimitedSeriesCanceledBetweenRounds(t *testing.T) {
	window, _, day0, _ := limitTestWindows()
	fx := buildLimitFixture(t, []time.Time{day0}, window, 200, limitScoreOf)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	seriesRoundObserver = func(limit int) {
		if limit > initialSeriesRound {
			cancel()
		}
	}
	t.Cleanup(func() { seriesRoundObserver = nil })
	// Limit 100 needs more than the first 64 series, so a second round starts and finds the context done.
	_, err := fx.queryLimitPointsCtx(ctx, modelv1.Sort_SORT_ASC, 100, nil)
	require.ErrorIs(t, err, context.Canceled)
}
