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
package storage

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

// seriesOrderDoc is one series document of a normal-mode measure: the entity, one string tag and the int tag the order
// rule sorts by, which a series without a value lacks.
type seriesOrderDoc struct {
	service  string
	instance string
	region   string
	score    int64
	hasScore bool
}

// seriesOrderDocs builds services x instances series. Scores come from a small range (many ties) with negative values,
// one series in 13 has none, and one in regionEvery has the region "eu" (the rest "us").
func seriesOrderDocs(services, instances, regionEvery int, seed int64) []seriesOrderDoc {
	rng := rand.New(rand.NewSource(seed)) //nolint:gosec // deterministic test data
	docs := make([]seriesOrderDoc, 0, services*instances)
	for s := 0; s < services; s++ {
		for i := 0; i < instances; i++ {
			doc := seriesOrderDoc{
				service: fmt.Sprintf("svc_%d", s), instance: fmt.Sprintf("inst_%03d", i), region: "us",
				score: int64(rng.Intn(80) - 40), hasScore: rng.Intn(13) != 0,
			}
			if regionEvery > 0 && rng.Intn(regionEvery) == 0 {
				doc.region = "eu"
			}
			docs = append(docs, doc)
		}
	}
	return docs
}

// insertSeriesOrderDocs admits docs as one native segment of si, written the way the measure writer writes a normal-mode
// series document: DocID is the series ID, no timestamp, and the sort-marked int field carries the ladder when ladder is set.
func insertSeriesOrderDocs(t testing.TB, si *seriesIndex, docs []seriesOrderDoc, ladder bool) {
	t.Helper()
	var out index.Documents
	for _, d := range docs {
		series := pbv1.Series{Subject: testSubjectSvc, EntityValues: []*modelv1.TagValue{fixtureStrTagValue(d.service), fixtureStrTagValue(d.instance)}}
		require.NoError(t, series.Marshal())
		region := index.NewStringField(index.FieldKey{TagName: "region"}, d.region)
		region.Index, region.NoSort = true, true
		fields := []index.Field{region}
		if d.hasScore {
			field := index.NewBytesField(index.FieldKey{IndexRuleID: nidx03ScoreRuleID}, convert.Int64ToBytes(d.score))
			field.Store, field.Index = true, true
			if ladder {
				field.Numeric, field.NumericKey = true, d.score
			}
			fields = append(fields, field)
		}
		out = append(out, index.Document{DocID: uint64(series.ID), EntityValues: append([]byte(nil), series.Buffer...), Fields: fields})
	}
	require.NoError(t, si.Insert(out))
}

func seriesScoreOrder(sort modelv1.Sort) *index.OrderBy {
	return &index.OrderBy{
		Type: index.OrderByTypeIndex, Sort: sort,
		Index: &databasev1.IndexRule{Metadata: &commonv1.Metadata{Id: nidx03ScoreRuleID}},
	}
}

type seriesRun struct {
	sorted [][]byte
	sd     SeriesData
}

func (r seriesRun) ids() []uint64 {
	ids := make([]uint64, len(r.sd.SeriesList))
	for i, s := range r.sd.SeriesList {
		ids[i] = uint64(s.ID)
	}
	return ids
}

func runSeriesSearch(t *testing.T, si *seriesIndex, series []*pbv1.Series, opts IndexSearchOpts) seriesRun {
	t.Helper()
	sd, sorted, err := si.Search(context.Background(), series, opts)
	require.NoError(t, err)
	return seriesRun{sd: sd, sorted: sorted}
}

// requireSeriesLimitMatchesFull asserts that a search with Limit returns exactly the first Limit series, and sort values,
// of the same search without it, and that MoreSeries says whether more followed.
func requireSeriesLimitMatchesFull(t *testing.T, si *seriesIndex, series []*pbv1.Series, opts IndexSearchOpts, limits []int) {
	t.Helper()
	full := runSeriesSearch(t, si, series, opts)
	require.False(t, full.sd.LimitApplied)
	require.False(t, full.sd.MoreSeries)
	require.Len(t, full.sorted, len(full.sd.SeriesList))
	fullIDs := full.ids()
	for _, limit := range limits {
		limited := opts
		limited.Limit = limit
		got := runSeriesSearch(t, si, series, limited)
		want := min(limit, len(fullIDs))
		require.Truef(t, got.sd.LimitApplied, "limit %d", limit)
		require.Equalf(t, len(fullIDs) > limit, got.sd.MoreSeries, "limit %d of %d series", limit, len(fullIDs))
		require.Equalf(t, fullIDs[:want], got.ids(), "limit %d", limit)
		require.Equalf(t, full.sorted[:want], got.sorted, "limit %d", limit)
		require.Len(t, got.sd.Timestamps, want)
	}
}

func TestSeriesIndex_SeriesOrderLimitMatchesFullSearch(t *testing.T) {
	docs := seriesOrderDocs(5, 130, 20, 1)
	matchers := map[string][]*pbv1.Series{
		"exact":     {fixtureSeriesExact("svc_1", "inst_007")},
		"prefix":    {fixtureSeriesPrefix("svc_2")},
		"wildcard":  {fixtureSeriesWildcard("inst_042")},
		"two":       {fixtureSeriesPrefix("svc_0"), fixtureSeriesPrefix("svc_3")},
		"all":       {{Subject: testSubjectSvc, EntityValues: []*modelv1.TagValue{pbv1.AnyTagValue, pbv1.AnyTagValue}}},
		"nothing":   {fixtureSeriesPrefix("svc_9")},
		"mixedKind": {fixtureSeriesExact("svc_4", "inst_100"), fixtureSeriesWildcard("inst_001")},
	}
	criteria := map[string]*modelv1.Criteria{
		"none":      nil,
		"selective": fixtureCondition("region", modelv1.Condition_BINARY_OP_EQ, fixtureStrTagValue("eu")),
		"common":    fixtureCondition("region", modelv1.Condition_BINARY_OP_EQ, fixtureStrTagValue("us")),
		"empty":     fixtureCondition("region", modelv1.Condition_BINARY_OP_EQ, fixtureStrTagValue("nowhere")),
	}
	limits := []int{1, 2, 5, 17, 64, 129, 130, 131, 650, 1000}
	layouts := map[string]func(t *testing.T) *seriesIndex{
		"ladder": func(t *testing.T) *seriesIndex {
			si := newEmptyOrderIndex(t)
			insertSeriesOrderDocs(t, si, docs, true)
			return si
		},
		"noLadder": func(t *testing.T) *seriesIndex {
			si := newEmptyOrderIndex(t)
			insertSeriesOrderDocs(t, si, docs, false)
			return si
		},
		"ladderAndLegacySegments": func(t *testing.T) *seriesIndex {
			si := newEmptyOrderIndex(t)
			insertSeriesOrderDocs(t, si, docs[:len(docs)/2], true)
			insertSeriesOrderDocs(t, si, docs[len(docs)/2:], false)
			return si
		},
	}
	for layoutName, build := range layouts {
		si := build(t)
		for matcherName, series := range matchers {
			for criteriaName, crit := range criteria {
				for _, sort := range []modelv1.Sort{modelv1.Sort_SORT_ASC, modelv1.Sort_SORT_DESC} {
					t.Run(fmt.Sprintf("%s/%s/%s/%s", layoutName, matcherName, criteriaName, sort), func(t *testing.T) {
						requireSeriesLimitMatchesFull(t, si, series, IndexSearchOpts{
							Order: seriesScoreOrder(sort), Criteria: crit, Fields: fixtureResolver{},
						}, limits)
					})
				}
			}
		}
	}
}

// A criteria that keeps a tiny share of the candidates makes the chunks grow past the descent's fetch cap, where the search
// ranks the whole filtered universe instead; the result must not change.
func TestSeriesIndex_SeriesOrderLimitFallsBackPastFetchCap(t *testing.T) {
	docs := seriesOrderDocs(2, topFetchCap/2+1500, 3000, 7)
	si := newEmptyOrderIndex(t)
	insertSeriesOrderDocs(t, si, docs, true)
	series := []*pbv1.Series{{Subject: testSubjectSvc, EntityValues: []*modelv1.TagValue{pbv1.AnyTagValue, pbv1.AnyTagValue}}}
	for _, sort := range []modelv1.Sort{modelv1.Sort_SORT_ASC, modelv1.Sort_SORT_DESC} {
		opts := IndexSearchOpts{
			Order: seriesScoreOrder(sort), Fields: fixtureResolver{},
			Criteria: fixtureCondition("region", modelv1.Condition_BINARY_OP_EQ, fixtureStrTagValue("eu")),
		}
		full := runSeriesSearch(t, si, series, opts)
		require.NotEmpty(t, full.sd.SeriesList)
		require.Less(t, len(full.sd.SeriesList), 30, "the criteria must be selective for the chunks to outgrow the cap")
		requireSeriesLimitMatchesFull(t, si, series, opts, []int{1, 3, len(full.sd.SeriesList), 40})
	}
}

// The limit is a count of series only for a series search ordered by an index rule that no shadowing accompanies.
func TestSeriesIndex_SeriesOrderLimitIsIgnoredOtherwise(t *testing.T) {
	si := newEmptyOrderIndex(t)
	insertSeriesOrderDocs(t, si, seriesOrderDocs(2, 50, 0, 3), true)
	series := []*pbv1.Series{fixtureSeriesPrefix("svc_0")}
	for name, opts := range map[string]IndexSearchOpts{
		"noOrder":   {Limit: 5},
		"timeOrder": {Limit: 5, Order: &index.OrderBy{Type: index.OrderByTypeTime, Sort: modelv1.Sort_SORT_ASC}},
		"shadowed": {Limit: 5, Order: seriesScoreOrder(modelv1.Sort_SORT_ASC), IsShadowed: func(context.Context, [][]byte) (map[string]struct{}, error) {
			return nil, nil
		}},
	} {
		t.Run(name, func(t *testing.T) {
			got := runSeriesSearch(t, si, series, opts)
			require.Len(t, got.sd.SeriesList, 50)
			require.False(t, got.sd.MoreSeries)
			require.False(t, got.sd.LimitApplied)
		})
	}
}
