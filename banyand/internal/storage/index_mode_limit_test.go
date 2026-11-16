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

package storage

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

type limitTestRow struct {
	service   string
	timestamp int64
}

func newIndexModeSegment(t *testing.T, rows []limitTestRow) *seriesIndex {
	t.Helper()
	dir, fn := setUp(require.New(t))
	t.Cleanup(fn)
	si, err := newSeriesIndex(context.Background(), dir, 0, 0, nil, &testRootLease{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, si.Close()) })
	var docs index.Documents
	for _, row := range rows {
		var series pbv1.Series
		series.Subject = "idx_measure"
		series.EntityValues = []*modelv1.TagValue{{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: row.service}}}}
		require.NoError(t, series.Marshal())
		subjectField := index.NewStringField(index.FieldKey{TagName: index.IndexModeName}, "idx_measure")
		subjectField.Index, subjectField.NoSort = true, true
		docs = append(docs, index.Document{
			EntityValues: append([]byte(nil), series.Buffer...),
			Fields:       []index.Field{subjectField},
			Timestamp:    row.timestamp,
			Version:      1,
		})
	}
	require.NoError(t, si.Insert(docs))
	return si
}

// searchSegments runs the index-mode search over the segments, newest first, the way the measure query does, and
// returns the services of the first want rows in time order across them. With limit zero the search returns every row and
// the caller de-duplicates; with a limit the search de-duplicates and truncates.
func searchSegments(t *testing.T, segments []*seriesIndex, order modelv1.Sort, limit, want int) []string {
	t.Helper()
	type row struct {
		service   string
		timestamp int64
	}
	seen := NewSeriesSet()
	var all []row
	for _, si := range segments {
		opts := IndexSearchOpts{IndexModeSubject: "idx_measure", Order: &index.OrderBy{Type: index.OrderByTypeTime, Sort: order}}
		if limit > 0 {
			opts.Limit, opts.SeenSeries = limit, seen
		}
		sd, _, err := si.SearchWithoutSeries(context.Background(), opts)
		require.NoError(t, err)
		require.Equal(t, limit > 0, sd.LimitApplied)
		if limit > 0 {
			require.LessOrEqual(t, len(sd.SeriesList), limit, "the search must truncate to the limit")
		}
		for i, series := range sd.SeriesList {
			if !sd.LimitApplied {
				if seen.Contains(uint64(series.ID)) {
					continue
				}
				seen.Insert(uint64(series.ID))
			}
			all = append(all, row{service: series.EntityValues[0].GetStr().GetValue(), timestamp: sd.Timestamps[i]})
		}
	}
	sort.SliceStable(all, func(i, j int) bool {
		if order == modelv1.Sort_SORT_DESC {
			return all[i].timestamp > all[j].timestamp
		}
		return all[i].timestamp < all[j].timestamp
	})
	var services []string
	for i := 0; i < len(all) && i < want; i++ {
		services = append(services, all[i].service)
	}
	return services
}

// Truncating each segment to the limit must give the rows the full search gives. A series that a newer segment holds
// shadows the same series in an older one, even when its newer row falls beyond the limit, so the older row must not
// fill a place: the series pool is shared across the segments and the segments' timestamps do not overlap, like real
// ones.
func TestSeriesIndex_IndexModeLimitMatchesFullSearchAcrossSegments(t *testing.T) {
	rng := rand.New(rand.NewSource(7)) //nolint:gosec // deterministic test data
	const segmentCount, pool, perSegment = 3, 40, 25
	var segments []*seriesIndex
	for seg := segmentCount - 1; seg >= 0; seg-- { // newest first
		picked := rng.Perm(pool)[:perSegment]
		rows := make([]limitTestRow, 0, perSegment)
		for i, p := range picked {
			rows = append(rows, limitTestRow{service: fmt.Sprintf("svc-%02d", p), timestamp: int64(seg)*1_000_000 + int64(rng.Intn(900_000)) + int64(i) + 1})
		}
		segments = append(segments, newIndexModeSegment(t, rows))
	}
	for _, order := range []modelv1.Sort{modelv1.Sort_SORT_DESC, modelv1.Sort_SORT_ASC} {
		for _, limit := range []int{1, 2, 5, 17, 40, 200} {
			want := searchSegments(t, segments, order, 0, limit)
			got := searchSegments(t, segments, order, limit, limit)
			require.Equal(t, want, got, "order %v limit %d", order, limit)
		}
	}
}

// A limit with no series seen yet still truncates before projection and orders by time.
func TestSeriesIndex_IndexModeLimitProjectsOnlyTheLimit(t *testing.T) {
	var rows []limitTestRow
	for i := 0; i < 50; i++ {
		rows = append(rows, limitTestRow{service: fmt.Sprintf("svc-%02d", i), timestamp: int64((i*7)%50+1) * 1000})
	}
	si := newIndexModeSegment(t, rows)
	sd, sortedValues, err := si.SearchWithoutSeries(context.Background(), IndexSearchOpts{
		IndexModeSubject: "idx_measure", Limit: 3, SeenSeries: NewSeriesSet(),
		Order: &index.OrderBy{Type: index.OrderByTypeTime, Sort: modelv1.Sort_SORT_DESC},
	})
	require.NoError(t, err)
	require.Equal(t, []int64{50000, 49000, 48000}, sd.Timestamps)
	require.Len(t, sortedValues, 3)
	require.Len(t, sd.SeriesList, 3)
	require.True(t, sd.LimitApplied)
}

// An index order and an unordered search ignore the limit: they return every row and leave the de-duplication to the
// caller.
func TestSeriesIndex_IndexModeLimitIgnoredWithoutTimeOrder(t *testing.T) {
	si := newIndexModeSegment(t, []limitTestRow{{"a", 1000}, {"b", 2000}, {"c", 3000}})
	seen := NewSeriesSet()
	sd, _, err := si.SearchWithoutSeries(context.Background(), IndexSearchOpts{IndexModeSubject: "idx_measure", Limit: 1, SeenSeries: seen})
	require.NoError(t, err)
	require.Len(t, sd.SeriesList, 3)
	require.False(t, sd.LimitApplied)
	require.Empty(t, seen.(seriesIDSet), "an ignored limit must not touch the seen set")
}
