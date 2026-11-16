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
	"testing"

	"github.com/stretchr/testify/require"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

// BenchmarkIndexModeSearchOrder measures an index-mode search that projects a stored tag over persisted segments,
// shaped like OAP's endpoint list: no order, and a time order over documents whose timestamps do not follow their
// storage order.
func BenchmarkIndexModeSearchOrder(b *testing.B) {
	for _, rows := range []int{1000, 10000, 100000} {
		b.Run(fmt.Sprintf("rows=%d", rows), func(b *testing.B) {
			ctx := context.Background()
			dir, fn := setUp(require.New(b))
			defer fn()
			si, err := newSeriesIndex(ctx, dir, 0, 0, nil, &testRootLease{})
			require.NoError(b, err)
			nameKey := index.FieldKey{TagName: "name"}
			timestamps := rand.New(rand.NewSource(1)).Perm(rows) //nolint:gosec // deterministic benchmark data
			var docs index.Documents
			for i := 0; i < rows; i++ {
				var series pbv1.Series
				series.Subject = "idx_measure"
				series.EntityValues = []*modelv1.TagValue{{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: fmt.Sprintf("svc-%07d", i)}}}}
				require.NoError(b, series.Marshal())
				subjectField := index.NewStringField(index.FieldKey{TagName: index.IndexModeName}, "idx_measure")
				subjectField.Index, subjectField.NoSort = true, true
				nameField := index.NewStringField(nameKey, fmt.Sprintf("endpoint-%07d-with-a-longer-name-to-fill-the-chunk", i))
				nameField.Store = true
				docs = append(docs, index.Document{
					EntityValues: append([]byte(nil), series.Buffer...),
					Fields:       []index.Field{subjectField, nameField},
					Timestamp:    int64(timestamps[i]+1) * 1000,
					Version:      1,
				})
			}
			require.NoError(b, si.Insert(docs))
			require.NoError(b, si.Close())
			si, err = newSeriesIndex(ctx, dir, 0, 0, nil, &testRootLease{})
			require.NoError(b, err)
			defer func() { require.NoError(b, si.Close()) }()

			timeDesc := &index.OrderBy{Type: index.OrderByTypeTime, Sort: modelv1.Sort_SORT_DESC}
			for _, tc := range []struct {
				order *index.OrderBy
				name  string
				limit int
			}{
				{name: "no-order"},
				{name: "time-desc", order: timeDesc},
				{name: "time-desc-limit-100", order: timeDesc, limit: 100},
			} {
				b.Run(tc.name, func(b *testing.B) {
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						opts := IndexSearchOpts{IndexModeSubject: "idx_measure", Order: tc.order, Projection: []index.FieldKey{nameKey}}
						want := rows
						if tc.limit > 0 {
							opts.Limit, opts.SeenSeries = tc.limit, NewSeriesSet()
							want = min(tc.limit, rows)
						}
						sd, _, searchErr := si.SearchWithoutSeries(ctx, opts)
						if searchErr != nil {
							b.Fatal(searchErr)
						}
						if len(sd.SeriesList) != want {
							b.Fatalf("got %d rows, want %d", len(sd.SeriesList), want)
						}
					}
				})
			}
		})
	}
}
