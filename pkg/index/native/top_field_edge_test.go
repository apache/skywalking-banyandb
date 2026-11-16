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

// requireServedByDescent runs fn and requires that the ladder descent ran and that none of its results was replaced by the
// exhaustive fallback.
func requireServedByDescent(t *testing.T, message string, fn func()) {
	t.Helper()
	descents, fallbacks := ladderDescents.Load(), ladderFallbacks.Load()
	fn()
	require.Greater(t, ladderDescents.Load(), descents, "%s: the descent must run", message)
	require.Equal(t, fallbacks, ladderFallbacks.Load(), "%s: the descent must not fall back", message)
}

// Upserting the best-ranked documents with worse values deletes the old copies, which stay in the ladder postings. The
// descent has to subtract them before it counts, or it takes buckets of dead documents and has to fall back.
func TestTopFieldHitsAndCursorSubtractDeletedDocumentsAtTheBestRanks(t *testing.T) {
	const total = 2000
	for _, desc := range []bool{true, false} {
		for _, persisted := range []bool{false, true} {
			t.Run(fmt.Sprintf("desc=%v/persisted=%v", desc, persisted), func(t *testing.T) {
				var first, second []Document
				for i := 0; i < total; i++ {
					score := int64(i)
					first = append(first, topFieldDocument(i, &score, "ok", true))
				}
				// The top half of the ranks (the highest values for Desc, the lowest otherwise) move to worse values.
				for i := 0; i < total/2; i++ {
					id, moved := i, int64(i)+3*total
					if desc {
						id, moved = total-1-i, int64(total-1-i)-3*total
					}
					second = append(second, topFieldDocument(id, &moved, "ok", true))
				}
				owner := newTopFieldFixture(t, persisted, false, [][]Document{first, second})
				view, err := owner.Acquire(context.Background())
				require.NoError(t, err)
				defer func() { require.NoError(t, view.Close()) }()
				for _, limit := range []int{1, 20, 300} {
					request := TopFieldRequest{Candidates: topFieldCandidates(nil), Field: "score", Limit: limit, Desc: desc}
					want := exhaustiveTopField(t, view, request)
					requireServedByDescent(t, fmt.Sprintf("top field limit %d", limit), func() {
						got, topErr := view.TopFieldHits(context.Background(), request)
						require.NoError(t, topErr)
						requireSameHits(t, want, got, fmt.Sprintf("limit %d", limit))
					})
					cursorRequest := SortCursorRequest{Selection: request.Candidates, SortField: "score", Desc: desc, PageSize: limit}
					requireServedByDescent(t, fmt.Sprintf("cursor page %d", limit), func() {
						page := drainCursor(t, view, cursorRequest, 1)
						require.Len(t, page, len(want))
						for i := range want {
							require.Equal(t, want[i].Identifier, page[i].Identifier, "cursor row %d", i)
						}
					})
				}
			})
		}
	}
}

// Keys at the ends of the int64 range, around zero and at powers of two must rank like their doc values.
func TestTopFieldHitsInt64Extremes(t *testing.T) {
	specials := []int64{
		math.MinInt64, math.MinInt64 + 1, -1, 0, 1, math.MaxInt64 - 1, math.MaxInt64,
		1 << 60, -(1 << 60), 1<<60 + 1, -(1 << 60) - 1,
	}
	for _, persisted := range []bool{false, true} {
		t.Run(fmt.Sprintf("persisted=%v", persisted), func(t *testing.T) {
			rng := rand.New(rand.NewSource(23)) //nolint:gosec // deterministic test data
			batches := make([][]Document, 2)
			id := 0
			for batch := range batches {
				for i := 0; i < 1500; i++ {
					var value int64
					switch rng.Intn(3) {
					case 0:
						value = specials[rng.Intn(len(specials))] // heavy ties on the special values
					case 1:
						value = int64(rng.Uint64())
					default:
						value = rng.Int63n(20) - 10
					}
					batches[batch] = append(batches[batch], topFieldDocument(id, &value, "ok", true))
					id++
				}
			}
			owner := newTopFieldFixture(t, persisted, false, batches)
			view, err := owner.Acquire(context.Background())
			require.NoError(t, err)
			defer func() { require.NoError(t, view.Close()) }()
			for _, desc := range []bool{false, true} {
				for _, limit := range []int{1, 5, 50, 400} {
					request := TopFieldRequest{Candidates: topFieldCandidates(nil), Field: "score", Limit: limit, Desc: desc}
					want := exhaustiveTopField(t, view, request)
					requireServedByDescent(t, fmt.Sprintf("desc=%v limit=%d", desc, limit), func() {
						got, topErr := view.TopFieldHits(context.Background(), request)
						require.NoError(t, topErr)
						requireSameHits(t, want, got, fmt.Sprintf("desc=%v limit=%d", desc, limit))
					})
				}
			}
		})
	}
}

// A document that carries the field twice, as an int array does, ranks by its smallest value. The writer gives array
// elements no ladder, so a segment that mixes them with scalar documents is incomplete and must be ranked in full.
func TestTopFieldHitsMultiValuedFieldFallsBack(t *testing.T) {
	rng := rand.New(rand.NewSource(29)) //nolint:gosec // deterministic test data
	var documents []Document
	for i := 0; i < 1200; i++ {
		first := rng.Int63n(100000)
		if i%6 != 0 {
			documents = append(documents, topFieldDocument(i, &first, "ok", true))
			continue
		}
		second := rng.Int63n(100000)
		document := topFieldDocument(i, &first, "ok", false)
		document.Fields = append(document.Fields, Field{
			Name: "score", Value: convert.Int64ToBytes(second), Index: true, Store: true, Sort: true,
		})
		documents = append(documents, document)
	}
	for _, persisted := range []bool{false, true} {
		owner := newTopFieldFixture(t, persisted, false, [][]Document{documents})
		view, err := owner.Acquire(context.Background())
		require.NoError(t, err)
		for _, desc := range []bool{false, true} {
			for _, limit := range []int{1, 10, 100} {
				request := TopFieldRequest{Candidates: topFieldCandidates(nil), Field: "score", Limit: limit, Desc: desc}
				want := exhaustiveTopField(t, view, request)
				descents := ladderDescents.Load()
				got, topErr := view.TopFieldHits(context.Background(), request)
				require.NoError(t, topErr)
				requireSameHits(t, want, got, fmt.Sprintf("persisted=%v desc=%v limit=%d", persisted, desc, limit))
				require.Equal(t, descents, ladderDescents.Load(), "an incomplete ladder must force the fallback")
				cursorRequest := SortCursorRequest{Selection: request.Candidates, SortField: "score", Desc: desc, PageSize: limit}
				page := drainCursor(t, view, cursorRequest, 1)
				for i := range want {
					require.Equal(t, want[i].Identifier, page[i].Identifier, "cursor row %d", i)
				}
				require.Equal(t, descents, ladderDescents.Load(), "the cursor must not descend either")
			}
		}
		require.NoError(t, view.Close())
	}
}
