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
	"testing"

	"github.com/stretchr/testify/require"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

func indexModeServices(sd SeriesData) []string {
	services := make([]string, len(sd.SeriesList))
	for i, series := range sd.SeriesList {
		services[i] = series.EntityValues[0].GetStr().GetValue()
	}
	return services
}

// An explicit time order on an index-mode search orders the rows by their timestamp, which is the row's own
// timestamp, in both directions; with no order the rows keep the hit order and nothing is sorted
// (apache/skywalking#14218).
func TestSeriesIndex_IndexModeTimeOrderSortsByTimestamp(t *testing.T) {
	ctx := context.Background()
	dir, fn := setUp(require.New(t))
	defer fn()
	si, err := newSeriesIndex(ctx, dir, 0, 0, nil, &testRootLease{})
	require.NoError(t, err)
	defer func() { require.NoError(t, si.Close()) }()

	document := func(subject, service string, timestamp int64) index.Document {
		var series pbv1.Series
		series.Subject = subject
		series.EntityValues = []*modelv1.TagValue{{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: service}}}}
		require.NoError(t, series.Marshal())
		subjectField := index.NewStringField(index.FieldKey{TagName: index.IndexModeName}, subject)
		subjectField.Index, subjectField.NoSort = true, true
		return index.Document{
			EntityValues: append([]byte(nil), series.Buffer...),
			Fields:       []index.Field{subjectField},
			Timestamp:    timestamp,
			Version:      1,
		}
	}
	// The timestamps do not follow the order the services were written in, nor their identifiers.
	var docs index.Documents
	for service, timestamp := range map[string]int64{"svc-0": 3000, "svc-1": 1000, "svc-2": 5000, "svc-3": 2000, "svc-4": 4000} {
		docs = append(docs, document("idx_measure", service, timestamp))
	}
	docs = append(docs, document("other_measure", "svc-other", 9000))
	require.NoError(t, si.Insert(docs))

	search := func(order *index.OrderBy) (SeriesData, [][]byte) {
		sd, sortedValues, searchErr := si.SearchWithoutSeries(ctx, IndexSearchOpts{IndexModeSubject: "idx_measure", Order: order})
		require.NoError(t, searchErr)
		return sd, sortedValues
	}

	t.Run("no order stays unsorted", func(t *testing.T) {
		sd, sortedValues := search(nil)
		require.Nil(t, sortedValues, "without an order nothing is sorted")
		require.ElementsMatch(t, []string{"svc-0", "svc-1", "svc-2", "svc-3", "svc-4"}, indexModeServices(sd))
	})
	t.Run("descending is newest first", func(t *testing.T) {
		sd, sortedValues := search(&index.OrderBy{Type: index.OrderByTypeTime, Sort: modelv1.Sort_SORT_DESC})
		require.Equal(t, []string{"svc-2", "svc-4", "svc-0", "svc-3", "svc-1"}, indexModeServices(sd))
		require.Equal(t, []int64{5000, 4000, 3000, 2000, 1000}, sd.Timestamps)
		require.Len(t, sortedValues, 5, "the sort values are what merges the segments")
	})
	t.Run("ascending is oldest first", func(t *testing.T) {
		sd, sortedValues := search(&index.OrderBy{Type: index.OrderByTypeTime, Sort: modelv1.Sort_SORT_ASC})
		require.Equal(t, []string{"svc-1", "svc-3", "svc-0", "svc-4", "svc-2"}, indexModeServices(sd))
		require.Equal(t, []int64{1000, 2000, 3000, 4000, 5000}, sd.Timestamps)
		require.Len(t, sortedValues, 5)
	})
	t.Run("the sort values compare in time order", func(t *testing.T) {
		_, sortedValues := search(&index.OrderBy{Type: index.OrderByTypeTime, Sort: modelv1.Sort_SORT_ASC})
		for i := 1; i < len(sortedValues); i++ {
			require.LessOrEqual(t, string(sortedValues[i-1]), string(sortedValues[i]), "row %d", i)
		}
	})
}

// Rows with the same timestamp come back in a deterministic order, by identifier, in both directions.
func TestSeriesIndex_IndexModeTimeOrderTiesAreDeterministic(t *testing.T) {
	ctx := context.Background()
	dir, fn := setUp(require.New(t))
	defer fn()
	si, err := newSeriesIndex(ctx, dir, 0, 0, nil, &testRootLease{})
	require.NoError(t, err)
	defer func() { require.NoError(t, si.Close()) }()

	var docs index.Documents
	for _, row := range []struct {
		service   string
		timestamp int64
	}{{"t-b", 100}, {"t-a", 100}, {"t-c", 200}} {
		var series pbv1.Series
		series.Subject = "idx_measure"
		series.EntityValues = []*modelv1.TagValue{{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: row.service}}}}
		require.NoError(t, series.Marshal())
		subjectField := index.NewStringField(index.FieldKey{TagName: index.IndexModeName}, "idx_measure")
		subjectField.Index, subjectField.NoSort = true, true
		docs = append(docs, index.Document{
			EntityValues: append([]byte(nil), series.Buffer...), Fields: []index.Field{subjectField}, Timestamp: row.timestamp, Version: 1,
		})
	}
	require.NoError(t, si.Insert(docs))

	for _, tc := range []struct {
		want []string
		sort modelv1.Sort
	}{
		{sort: modelv1.Sort_SORT_ASC, want: []string{"t-a", "t-b", "t-c"}},
		{sort: modelv1.Sort_SORT_DESC, want: []string{"t-c", "t-a", "t-b"}},
	} {
		sd, _, searchErr := si.SearchWithoutSeries(ctx, IndexSearchOpts{
			IndexModeSubject: "idx_measure", Order: &index.OrderBy{Type: index.OrderByTypeTime, Sort: tc.sort},
		})
		require.NoError(t, searchErr)
		require.Equal(t, tc.want, indexModeServices(sd), tc.sort.String())
	}
}

// Rows written by the previous release carry the same _timestamp doc value, so the same order applies to them.
func TestSeriesIndex_IndexModeTimeOrderOnPreviousReleaseFixture(t *testing.T) {
	si := openNIDX03Fixture(t, "indexmode")
	for _, tc := range []struct {
		want []string
		sort modelv1.Sort
	}{
		{sort: modelv1.Sort_SORT_ASC, want: []string{"svcA", "svcB"}},
		{sort: modelv1.Sort_SORT_DESC, want: []string{"svcB", "svcA"}},
	} {
		t.Run(fmt.Sprint(tc.sort), func(t *testing.T) {
			sd, sortedValues, err := si.SearchWithoutSeries(context.Background(), IndexSearchOpts{
				IndexModeSubject: "idx_measure",
				Order:            &index.OrderBy{Type: index.OrderByTypeTime, Sort: tc.sort},
			})
			require.NoError(t, err)
			require.Equal(t, tc.want, indexModeServices(sd))
			require.Len(t, sortedValues, 2)
		})
	}
}
