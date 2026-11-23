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

package native

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/convert"
)

// buildLatencyOwner writes rows documents, one of 100 entities each with a heavy-tailed latency (a slow-statement record
// stream), in batches of 100k, merges them into one native segment, persists and reopens the owner. With ladder the
// latency field also writes its sort ladder.
//
//nolint:contextcheck // NewOwner has no context-capable variant.
func buildLatencyOwner(b *testing.B, rows int, ladder bool) *Owner {
	b.Helper()
	options := OwnerOptions{Lease: testLease{}, Path: b.TempDir(), CompactionThreshold: -1}
	owner, err := NewOwner(options)
	require.NoError(b, err)
	rng := rand.New(rand.NewSource(21)) //nolint:gosec // deterministic benchmark data
	const chunk = 100_000
	for done := 0; done < rows; done += chunk {
		docs := make([]Document, 0, chunk)
		for i := done; i < done+chunk && i < rows; i++ {
			latency := int64(math.Exp(rng.NormFloat64()*1.2+9) * 1000) // microseconds, heavy tail
			docs = append(docs, Document{
				Identifier: []byte(fmt.Sprintf("rec-%08d", i)), Timestamp: int64(i + 1),
				Fields: []Field{
					{Name: "entity", Value: []byte(fmt.Sprintf("e%03d", i%100)), Index: true, Store: true},
					{Name: "latency", Value: convert.Int64ToBytes(latency), Index: true, Store: true, Sort: true, Ladder: ladder, LadderKey: latency},
				},
			})
		}
		require.NoError(b, owner.Batch(context.Background(), Batch{Documents: docs}))
	}
	require.NoError(b, owner.forceMergeAll(context.Background()))
	require.NoError(b, owner.Close())
	owner, err = NewOwner(options)
	require.NoError(b, err)
	b.Cleanup(func() { require.NoError(b, owner.Close()) })
	return owner
}

// BenchmarkSortCursorFirstPageLadder compares the first page of a SortCursor over a persisted, merged segment, written
// without and with the sort ladder, for an entity filter that keeps 1% of the rows and for every row.
func BenchmarkSortCursorFirstPageLadder(b *testing.B) {
	ctx := context.Background()
	const page = 100
	for _, rows := range []int{100_000, 1_000_000} {
		b.Run(fmt.Sprintf("rows=%d", rows), func(b *testing.B) {
			views := make(map[bool]*ReadView, 2)
			for _, ladder := range []bool{false, true} {
				view, err := buildLatencyOwner(b, rows, ladder).Acquire(ctx)
				require.NoError(b, err)
				b.Cleanup(func() { require.NoError(b, view.Close()) })
				require.Len(b, view.root.segments, 1)
				views[ladder] = view
			}
			scopes := map[string]TermSetRequest{
				"entity=1%": {Field: "entity", Terms: [][]byte{[]byte("e007")}, Mode: MatchAnyTerm, MaxTerms: ^uint64(0)},
				"all-rows":  {},
			}
			for _, name := range []string{"entity=1%", "all-rows"} {
				request := SortCursorRequest{Selection: scopes[name], SortField: "latency", Desc: true, PageSize: page}
				firstPage := func(view *ReadView) []SortedHit {
					cursor, err := view.NewSortCursor(ctx, request)
					require.NoError(b, err)
					defer func() { require.NoError(b, cursor.Close()) }()
					hits, err := cursor.NextPage(ctx)
					require.NoError(b, err)
					return hits
				}
				before := ladderDescents.Load()
				want := firstPage(views[false])
				require.Equal(b, before, ladderDescents.Load(), "a segment without the ladder must not descend")
				got := firstPage(views[true])
				require.Positive(b, ladderDescents.Load()-before, "the ladder segment must descend")
				require.Len(b, got, page)
				for i := range want {
					require.Equal(b, want[i].SortValue, got[i].SortValue, "row %d", i)
					require.Equal(b, want[i].Identifier, got[i].Identifier, "row %d", i)
				}
				for _, ladder := range []bool{false, true} {
					label := map[bool]string{false: "without-ladder", true: "with-ladder"}[ladder]
					b.Run(name+"/"+label, func(b *testing.B) {
						b.ReportAllocs()
						for i := 0; i < b.N; i++ {
							firstPage(views[ladder])
						}
					})
				}
			}
		})
	}
}
