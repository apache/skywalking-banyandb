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
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

// buildSeriesOrderBenchIndex writes count series of one service in batches, as a normal-mode measure writes its series
// documents, then closes the index and reopens it from disk. Scores are heavy tailed (log-uniform over nine decades).
func buildSeriesOrderBenchIndex(b *testing.B, count int, ladder bool, regionOf func(i int) string) *seriesIndex {
	b.Helper()
	dir, fn := setUp(require.New(b))
	b.Cleanup(fn)
	ctx := context.Background()
	si, err := newSeriesIndex(ctx, dir, 0, 0, nil, &testRootLease{})
	require.NoError(b, err)
	rng := rand.New(rand.NewSource(1)) //nolint:gosec // deterministic benchmark data
	const batch = 20000
	for done := 0; done < count; done += batch {
		docs := make([]seriesOrderDoc, 0, batch)
		for i := done; i < min(done+batch, count); i++ {
			docs = append(docs, seriesOrderDoc{
				service: "svc_a", instance: fmt.Sprintf("inst_%07d", i), region: regionOf(i),
				score: int64(math.Exp(rng.Float64() * 20)), hasScore: true,
			})
		}
		insertSeriesOrderDocs(b, si, docs, ladder)
	}
	require.NoError(b, si.Close())
	si, err = newSeriesIndex(ctx, dir, 0, 0, nil, &testRootLease{})
	require.NoError(b, err)
	b.Cleanup(func() { require.NoError(b, si.Close()) })
	return si
}

// BenchmarkSeriesOrderLimit compares a series search ordered by an index rule that asks for the first 100 series (the sort
// ladder, or the exhaustive ranking when the index was written without it) with the same search without a limit, which sorts
// every series under the entity prefix.
func BenchmarkSeriesOrderLimit(b *testing.B) {
	series := []*pbv1.Series{fixtureSeriesPrefix("svc_a")}
	for _, count := range []int{100000, 500000} {
		b.Run(fmt.Sprintf("series=%d", count), func(b *testing.B) {
			for _, ladder := range []bool{true, false} {
				layout := "ladder"
				if !ladder {
					layout = "noladder"
				}
				si := buildSeriesOrderBenchIndex(b, count, ladder, func(int) string { return "us" })
				for _, sort := range []modelv1.Sort{modelv1.Sort_SORT_ASC, modelv1.Sort_SORT_DESC} {
					for _, limit := range []int{100, 0} {
						name := fmt.Sprintf("%s/%s/limit-%d", layout, sort, limit)
						if limit == 0 {
							name = fmt.Sprintf("%s/%s/full-sort", layout, sort)
						}
						b.Run(name, func(b *testing.B) {
							b.ReportAllocs()
							for i := 0; i < b.N; i++ {
								sd, _, err := si.Search(context.Background(), series, IndexSearchOpts{Order: seriesScoreOrder(sort), Limit: limit})
								if err != nil {
									b.Fatal(err)
								}
								want := count
								if limit > 0 {
									want = limit
								}
								if len(sd.SeriesList) != want {
									b.Fatalf("got %d series, want %d", len(sd.SeriesList), want)
								}
							}
						})
					}
				}
			}
		})
	}
}

// BenchmarkSeriesOrderLimitCriteria runs the ascending search of a ladder index of 100k series with a criteria that keeps
// about 50 of them, and one that keeps 1%, with a limit of 64 series and without one.
func BenchmarkSeriesOrderLimitCriteria(b *testing.B) {
	const count = 100000
	regionOf := func(i int) string {
		switch {
		case i%2083 == 0:
			return "rare" // 49 series
		case i%100 == 1:
			return "pct" // 1000 series
		default:
			return "us"
		}
	}
	si := buildSeriesOrderBenchIndex(b, count, true, regionOf)
	series := []*pbv1.Series{fixtureSeriesPrefix("svc_a")}
	matching := map[string]int{}
	for i := 0; i < count; i++ {
		matching[regionOf(i)]++
	}
	for _, tc := range []struct {
		region string
		want   int
	}{{"rare", matching["rare"]}, {"pct", matching["pct"]}} {
		for _, limit := range []int{64, 0} {
			name := fmt.Sprintf("%s/limit-%d", tc.region, limit)
			if limit == 0 {
				name = tc.region + "/unlimited"
			}
			b.Run(name, func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					sd, _, err := si.Search(context.Background(), series, IndexSearchOpts{
						Order: seriesScoreOrder(modelv1.Sort_SORT_ASC), Limit: limit, Fields: fixtureResolver{},
						Criteria: fixtureCondition("region", modelv1.Condition_BINARY_OP_EQ, fixtureStrTagValue(tc.region)),
					})
					if err != nil {
						b.Fatal(err)
					}
					want := tc.want
					if limit > 0 {
						want = min(limit, tc.want)
					}
					if len(sd.SeriesList) != want {
						b.Fatalf("got %d series, want %d", len(sd.SeriesList), want)
					}
				}
			})
		}
	}
}
