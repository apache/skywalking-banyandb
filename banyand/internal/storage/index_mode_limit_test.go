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
	"bytes"
	"context"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
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

// searchSegments runs the index-mode search over the segments, newest first, the way the measure query does, and returns
// the services of the first want rows in time order across them, with their timestamps. With limit zero the search returns
// every row and the caller de-duplicates; with a limit the search drops what a newer segment holds and truncates.
func searchSegments(t *testing.T, segments []*seriesIndex, order modelv1.Sort, limit, want int, timeRange *timestamp.TimeRange) []string {
	t.Helper()
	type row struct {
		service   string
		timestamp int64
	}
	seen := make(map[uint64]struct{})
	var all []row
	for i, si := range segments {
		opts := IndexSearchOpts{
			IndexModeSubject: "idx_measure", TimeRange: timeRange,
			Order: &index.OrderBy{Type: index.OrderByTypeTime, Sort: order},
		}
		if limit > 0 {
			newer := segments[:i]
			opts.Limit = limit
			opts.IsShadowed = func(ctx context.Context, identifiers [][]byte) (map[string]struct{}, error) {
				shadowed := make(map[string]struct{})
				for _, n := range newer {
					found, err := n.ExistingIdentifiers(ctx, identifiers, IndexSearchOpts{IndexModeSubject: "idx_measure", TimeRange: timeRange})
					if err != nil {
						return nil, err
					}
					for id := range found {
						shadowed[id] = struct{}{}
					}
				}
				return shadowed, nil
			}
		}
		sd, _, err := si.SearchWithoutSeries(context.Background(), opts)
		require.NoError(t, err)
		require.Equal(t, limit > 0, sd.LimitApplied)
		if limit > 0 {
			require.LessOrEqual(t, len(sd.SeriesList), limit, "the search must truncate to the limit")
		}
		for j, series := range sd.SeriesList {
			if !sd.LimitApplied {
				if _, dup := seen[uint64(series.ID)]; dup {
					continue
				}
				seen[uint64(series.ID)] = struct{}{}
			}
			all = append(all, row{service: series.EntityValues[0].GetStr().GetValue(), timestamp: sd.Timestamps[j]})
		}
	}
	sort.SliceStable(all, func(i, j int) bool {
		if all[i].timestamp != all[j].timestamp {
			if order == modelv1.Sort_SORT_DESC {
				return all[i].timestamp > all[j].timestamp
			}
			return all[i].timestamp < all[j].timestamp
		}
		return all[i].service < all[j].service
	})
	var services []string
	for i := 0; i < len(all) && i < want; i++ {
		services = append(services, all[i].service)
	}
	return services
}

func limitTestAllTime() *timestamp.TimeRange {
	return &timestamp.TimeRange{Start: time.Unix(0, 0), End: time.Unix(0, math.MaxInt64>>1), IncludeStart: true, IncludeEnd: true}
}

// Truncating each segment to the limit must give the rows the full search gives. A series that a newer segment holds
// shadows the same series in an older one, even when its newer row falls beyond the limit, so the older row must not fill
// a place: the series pool is shared across the segments and the segments' timestamps do not overlap, like real ones. It
// runs with a time range (the trie descent) and without one (the whole filtered universe is ranked).
func TestSeriesIndex_IndexModeLimitMatchesFullSearchAcrossSegments(t *testing.T) {
	rng := rand.New(rand.NewSource(7)) //nolint:gosec // deterministic test data
	const segmentCount, pool, perSegment = 3, 1500, 600
	var segments []*seriesIndex
	for seg := segmentCount - 1; seg >= 0; seg-- { // newest first
		picked := rng.Perm(pool)[:perSegment]
		rows := make([]limitTestRow, 0, perSegment)
		for i, p := range picked {
			rows = append(rows, limitTestRow{service: fmt.Sprintf("svc-%04d", p), timestamp: int64(seg+1)*1_000_000 + int64(rng.Intn(900_000)) + int64(i) + 1})
		}
		segments = append(segments, newIndexModeSegment(t, rows))
	}
	for name, timeRange := range map[string]*timestamp.TimeRange{"descent": limitTestAllTime(), "fallback": nil} {
		for _, order := range []modelv1.Sort{modelv1.Sort_SORT_DESC, modelv1.Sort_SORT_ASC} {
			for _, limit := range []int{1, 2, 5, 17, 40, 200, 5000} {
				want := searchSegments(t, segments, order, 0, limit, timeRange)
				got := searchSegments(t, segments, order, limit, limit, timeRange)
				require.Equal(t, want, got, "%s order %v limit %d", name, order, limit)
			}
		}
	}
}

// The best-ranked rows of an older segment can all be shadowed by a newer one. The search must keep asking the index for
// more candidates until Limit rows survive, or until the index has no more.
func TestSeriesIndex_IndexModeLimitReachesPastShadowedRows(t *testing.T) {
	const hot, total = 400, 500
	var newer, older []limitTestRow
	for i := 0; i < hot; i++ { // the series the older segment ranks first are also in the newer segment
		newer = append(newer, limitTestRow{service: fmt.Sprintf("svc-%03d", i), timestamp: 2_000_000 + int64(i)})
	}
	for i := 0; i < total; i++ {
		older = append(older, limitTestRow{service: fmt.Sprintf("svc-%03d", i), timestamp: 1_000_000 + int64(total-i)}) // svc-000 is the newest
	}
	segments := []*seriesIndex{newIndexModeSegment(t, newer), newIndexModeSegment(t, older)}
	for _, order := range []modelv1.Sort{modelv1.Sort_SORT_DESC, modelv1.Sort_SORT_ASC} {
		for _, limit := range []int{5, 50, 150} {
			want := searchSegments(t, segments, order, 0, limit, limitTestAllTime())
			got := searchSegments(t, segments, order, limit, limit, limitTestAllTime())
			require.Equal(t, want, got, "order %v limit %d", order, limit)
		}
	}
}

// A limit with nothing shadowed returns the first rows in time order and reads the stored fields of only those.
func TestSeriesIndex_IndexModeLimitProjectsOnlyTheLimit(t *testing.T) {
	var rows []limitTestRow
	for i := 0; i < 2000; i++ {
		rows = append(rows, limitTestRow{service: fmt.Sprintf("svc-%04d", i), timestamp: int64((i*7)%2000+1) * 1000})
	}
	si := newIndexModeSegment(t, rows)
	sd, sortedValues, err := si.SearchWithoutSeries(context.Background(), IndexSearchOpts{
		IndexModeSubject: "idx_measure", Limit: 3, TimeRange: limitTestAllTime(),
		IsShadowed: func(context.Context, [][]byte) (map[string]struct{}, error) { return nil, nil },
		Order:      &index.OrderBy{Type: index.OrderByTypeTime, Sort: modelv1.Sort_SORT_DESC},
	})
	require.NoError(t, err)
	require.Equal(t, []int64{2000000, 1999000, 1998000}, sd.Timestamps)
	require.Len(t, sortedValues, 3)
	require.Len(t, sd.SeriesList, 3)
	require.True(t, sd.LimitApplied)
}

// A limit with no order returns any Limit rows that nothing shadows and reads the stored fields of only those.
func TestSeriesIndex_IndexModeLimitWithoutOrder(t *testing.T) {
	var rows []limitTestRow
	for i := 0; i < 2000; i++ {
		rows = append(rows, limitTestRow{service: fmt.Sprintf("svc-%04d", i), timestamp: 1000})
	}
	si := newIndexModeSegment(t, rows)
	asked := false
	sd, sortedValues, err := si.SearchWithoutSeries(context.Background(), IndexSearchOpts{
		IndexModeSubject: "idx_measure", Limit: 3, TimeRange: limitTestAllTime(),
		IsShadowed: func(context.Context, [][]byte) (map[string]struct{}, error) { asked = true; return nil, nil },
	})
	require.NoError(t, err)
	require.Len(t, sd.SeriesList, 3)
	require.Empty(t, sortedValues)
	require.True(t, sd.LimitApplied)
	require.True(t, asked)
}

// A limit with no order skips the rows a newer segment shadows, and lists more candidates while the prefix it listed
// leaves fewer than Limit rows.
func TestSeriesIndex_IndexModeLimitWithoutOrderSkipsShadowed(t *testing.T) {
	var rows []limitTestRow
	for i := 0; i < 400; i++ {
		rows = append(rows, limitTestRow{service: fmt.Sprintf("svc-%04d", i), timestamp: 1000})
	}
	si := newIndexModeSegment(t, rows)
	all, _, err := si.SearchWithoutSeries(context.Background(), IndexSearchOpts{IndexModeSubject: "idx_measure", TimeRange: limitTestAllTime()})
	require.NoError(t, err)
	require.Len(t, all.SeriesList, 400)
	// The search lists candidates in this order: shadow the first 300, so a prefix of a few rows holds no survivor.
	nameOf := func(series *pbv1.Series) string { return series.EntityValues[0].GetStr().GetValue() }
	shadowedNames := map[string]struct{}{}
	for _, series := range all.SeriesList[:300] {
		shadowedNames[nameOf(series)] = struct{}{}
	}
	shadowedBy := func(_ context.Context, identifiers [][]byte) (map[string]struct{}, error) {
		found := map[string]struct{}{}
		for _, identifier := range identifiers {
			for name := range shadowedNames {
				if bytes.Contains(identifier, []byte(name)) {
					found[string(identifier)] = struct{}{}
					break
				}
			}
		}
		return found, nil
	}
	sd, _, err := si.SearchWithoutSeries(context.Background(), IndexSearchOpts{
		IndexModeSubject: "idx_measure", Limit: 4, TimeRange: limitTestAllTime(), IsShadowed: shadowedBy,
	})
	require.NoError(t, err)
	require.Len(t, sd.SeriesList, 4)
	require.True(t, sd.LimitApplied)
	for _, series := range sd.SeriesList {
		_, isShadowed := shadowedNames[nameOf(series)]
		require.False(t, isShadowed, nameOf(series))
	}

	sd, _, err = si.SearchWithoutSeries(context.Background(), IndexSearchOpts{
		IndexModeSubject: "idx_measure", Limit: 1000, TimeRange: limitTestAllTime(), IsShadowed: shadowedBy,
	})
	require.NoError(t, err)
	require.Len(t, sd.SeriesList, 100, "fewer unshadowed rows than the limit returns them all")
}

// An index order ignores the limit: it returns every row and never asks about shadowing.
func TestSeriesIndex_IndexModeLimitIgnoredWithIndexOrderWithoutRule(t *testing.T) {
	si := newIndexModeSegment(t, []limitTestRow{{"a", 1000}, {"b", 2000}, {"c", 3000}})
	asked := false
	sd, _, err := si.SearchWithoutSeries(context.Background(), IndexSearchOpts{
		IndexModeSubject: "idx_measure", Limit: 1, Order: &index.OrderBy{Type: index.OrderByTypeIndex},
		IsShadowed: func(context.Context, [][]byte) (map[string]struct{}, error) { asked = true; return nil, nil },
	})
	require.NoError(t, err)
	require.Len(t, sd.SeriesList, 3)
	require.False(t, sd.LimitApplied)
	require.False(t, asked)
}
