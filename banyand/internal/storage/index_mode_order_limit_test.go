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
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

const orderRuleID = uint32(7)

type orderTestRow struct {
	service   string
	timestamp int64
	value     int64
	hasValue  bool
}

// newIndexModeOrderSegment writes one index-mode segment whose rows carry the int tag of an index rule, with or without the
// ladder the measure writer attaches to a scalar int index-rule tag.
func newIndexModeOrderSegment(t *testing.T, rows []orderTestRow, ladder bool) *seriesIndex {
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
		fields := []index.Field{subjectField}
		if row.hasValue {
			field := index.NewBytesField(index.FieldKey{IndexRuleID: orderRuleID}, convert.Int64ToBytes(row.value))
			field.Store, field.Index = true, true
			if ladder {
				field.Numeric, field.NumericKey = true, row.value
			}
			fields = append(fields, field)
		}
		docs = append(docs, index.Document{
			EntityValues: append([]byte(nil), series.Buffer...), Fields: fields, Timestamp: row.timestamp, Version: 1,
		})
	}
	require.NoError(t, si.Insert(docs))
	return si
}

type orderedRow struct {
	service string
	value   []byte
}

// searchOrderSegments runs the index-mode search ordered by the index rule over the segments, newest first, the way the
// measure query does. With limit zero every row comes back and the rows are de-duplicated here (the pre-existing path:
// SortHits over the whole universe); with a limit the search shadows, truncates and keeps the rank order itself.
func searchOrderSegments(t *testing.T, segments []*seriesIndex, order modelv1.Sort, limit, want int, timeRange *timestamp.TimeRange) []orderedRow {
	t.Helper()
	orderBy := &index.OrderBy{
		Type: index.OrderByTypeIndex, Sort: order,
		Index: &databasev1.IndexRule{Metadata: &commonv1.Metadata{Id: orderRuleID}},
	}
	seen := make(map[uint64]struct{})
	var all []orderedRow
	for i, si := range segments {
		opts := IndexSearchOpts{IndexModeSubject: "idx_measure", TimeRange: timeRange, Order: orderBy}
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
		sd, sortedValues, err := si.SearchWithoutSeries(context.Background(), opts)
		require.NoError(t, err)
		require.Equal(t, limit > 0, sd.LimitApplied)
		require.Len(t, sortedValues, len(sd.SeriesList), "a sorted search returns one sort value per row")
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
			all = append(all, orderedRow{service: series.EntityValues[0].GetStr().GetValue(), value: sortedValues[j]})
		}
	}
	sort.SliceStable(all, func(i, j int) bool {
		if missing := len(all[i].value) == 0; missing != (len(all[j].value) == 0) {
			return !missing // a row without the field ranks last in both directions
		}
		if c := bytes.Compare(all[i].value, all[j].value); c != 0 {
			if order == modelv1.Sort_SORT_DESC {
				return c > 0
			}
			return c < 0
		}
		return all[i].service < all[j].service
	})
	if len(all) > want {
		all = all[:want]
	}
	return all
}

// An index-rule order with a limit must give the rows the full ordered search gives, with and without the sort ladder, with
// and without a time range, with ties and with rows that lack the field (they rank last), across time-disjoint segments that
// share a series pool so that newer rows shadow older ones.
func TestSeriesIndex_IndexModeIndexOrderLimitMatchesFullSearchAcrossSegments(t *testing.T) {
	const segmentCount, pool, perSegment = 3, 1500, 600
	for _, ladder := range []bool{false, true} {
		for _, ties := range []bool{false, true} {
			t.Run(fmt.Sprintf("ladder=%v/ties=%v", ladder, ties), func(t *testing.T) {
				rng := rand.New(rand.NewSource(13)) //nolint:gosec // deterministic test data
				var segments []*seriesIndex
				for seg := segmentCount - 1; seg >= 0; seg-- { // newest first
					rows := make([]orderTestRow, 0, perSegment)
					for i, p := range rng.Perm(pool)[:perSegment] {
						value := rng.Int63n(2_000_000) - 1_000_000
						if ties {
							value = rng.Int63n(40) - 15
						}
						rows = append(rows, orderTestRow{
							service: fmt.Sprintf("svc-%04d", p), value: value, hasValue: rng.Intn(15) != 0,
							timestamp: int64(seg+1)*1_000_000 + int64(rng.Intn(900_000)) + int64(i) + 1,
						})
					}
					segments = append(segments, newIndexModeOrderSegment(t, rows, ladder))
				}
				for name, timeRange := range map[string]*timestamp.TimeRange{"range": limitTestAllTime(), "norange": nil} {
					for _, order := range []modelv1.Sort{modelv1.Sort_SORT_DESC, modelv1.Sort_SORT_ASC} {
						for _, limit := range []int{1, 2, 5, 17, 40, 200, 5000} {
							want := searchOrderSegments(t, segments, order, 0, limit, timeRange)
							got := searchOrderSegments(t, segments, order, limit, limit, timeRange)
							require.Equal(t, want, got, "%s order %v limit %d", name, order, limit)
						}
					}
				}
			})
		}
	}
}

// The best-ranked rows of an older segment can all be shadowed by a newer one: the search must keep asking for more
// candidates until Limit rows survive.
func TestSeriesIndex_IndexModeIndexOrderLimitReachesPastShadowedRows(t *testing.T) {
	const hot, total = 400, 900
	var newer, older []orderTestRow
	for i := 0; i < hot; i++ {
		newer = append(newer, orderTestRow{service: fmt.Sprintf("svc-%03d", i), timestamp: 2_000_000 + int64(i), value: int64(-i), hasValue: true})
	}
	for i := 0; i < total; i++ { // svc-000 has the largest value
		older = append(older, orderTestRow{service: fmt.Sprintf("svc-%03d", i), timestamp: 1_000_000 + int64(i), value: int64(total - i), hasValue: true})
	}
	for _, ladder := range []bool{false, true} {
		segments := []*seriesIndex{newIndexModeOrderSegment(t, newer, ladder), newIndexModeOrderSegment(t, older, ladder)}
		for _, order := range []modelv1.Sort{modelv1.Sort_SORT_DESC, modelv1.Sort_SORT_ASC} {
			for _, limit := range []int{5, 50, 150} {
				want := searchOrderSegments(t, segments, order, 0, limit, limitTestAllTime())
				got := searchOrderSegments(t, segments, order, limit, limit, limitTestAllTime())
				require.Equal(t, want, got, "ladder %v order %v limit %d", ladder, order, limit)
			}
		}
	}
}
