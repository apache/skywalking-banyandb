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
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

// drainCursor reads pages of pageSize until the cursor is exhausted or maxPages is reached.
func drainCursor(t *testing.T, view *ReadView, request SortCursorRequest, maxPages int) []SortedHit {
	t.Helper()
	cursor, err := view.NewSortCursor(context.Background(), request)
	require.NoError(t, err)
	defer func() { require.NoError(t, cursor.Close()) }()
	var all []SortedHit
	for page := 0; page < maxPages; page++ {
		hits, pageErr := cursor.NextPage(context.Background())
		require.NoError(t, pageErr)
		if len(hits) == 0 {
			break
		}
		all = append(all, hits...)
	}
	return all
}

func requireSameSortedHits(t *testing.T, want, got []SortedHit, message string) {
	t.Helper()
	require.Len(t, got, len(want), message)
	for i := range want {
		require.Equal(t, want[i].Identifier, got[i].Identifier, "%s row %d", message, i)
		require.Equal(t, want[i].Segment, got[i].Segment, "%s row %d", message, i)
		require.Equal(t, want[i].DocumentNumber, got[i].DocumentNumber, "%s row %d", message, i)
		require.Equal(t, want[i].SortValue, got[i].SortValue, "%s row %d", message, i)
		require.Equal(t, want[i].Missing, got[i].Missing, "%s row %d", message, i)
	}
}

func TestSortCursorFirstPageUsesTheLadderAndStaysExact(t *testing.T) {
	const span = int64(1) << 44
	shape := topFieldShape{span: span, batches: 3, perBatch: 2500, missing: 9, upserts: 12, negatives: true}
	timeRange := &TimeRange{Lower: topFieldBase + 1000*1_000_000, Upper: topFieldBase + 5000*1_000_000, IncludesLower: true, IncludesUpper: true}
	selections := map[string]TermSetRequest{
		"term":       topFieldCandidates(nil),
		"term+range": topFieldCandidates(timeRange),
		"all":        {},
		"all+range":  {Scope: QueryScope{TimeRange: timeRange}},
	}
	for _, persisted := range []bool{false, true} {
		// The same data written with and without the ladder: the plain fixture is the pre-change behavior.
		plainOwner := newTopFieldFixture(t, persisted, false, randomTopFieldBatches(rand.New(rand.NewSource(2)), shape, ladderNone)) //nolint:gosec
		ladderOwner := newTopFieldFixture(t, persisted, false, randomTopFieldBatches(rand.New(rand.NewSource(2)), shape, ladderAll)) //nolint:gosec
		plainView, err := plainOwner.Acquire(context.Background())
		require.NoError(t, err)
		ladderView, err := ladderOwner.Acquire(context.Background())
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, plainView.Close())
			require.NoError(t, ladderView.Close())
		})
		for name, selection := range selections {
			for _, desc := range []bool{false, true} {
				for _, pageSize := range []int{1, 10, 100} {
					message := fmt.Sprintf("persisted=%v %s desc=%v page=%d", persisted, name, desc, pageSize)
					request := SortCursorRequest{Selection: selection, SortField: "score", Desc: desc, PageSize: pageSize}
					before := ladderDescents.Load()
					want := drainCursor(t, plainView, request, 4)
					require.Equal(t, before, ladderDescents.Load(), "no ladder, no descent: %s", message)
					got := drainCursor(t, ladderView, request, 4)
					require.Positive(t, ladderDescents.Load()-before, "the first page must run a descent: %s", message)
					requireSameSortedHits(t, want, got, message)
				}
			}
		}
		// Reading to the end yields the complete sequence: the retained candidates were not replaced by the descent.
		request := SortCursorRequest{Selection: topFieldCandidates(nil), SortField: "score", Desc: true, PageSize: 50}
		full := drainCursor(t, ladderView, request, 1000)
		hits, err := ladderView.MatchTermsSet(context.Background(), request.Selection)
		require.NoError(t, err)
		require.Len(t, full, len(hits))
		reference, err := ladderView.SortHits(context.Background(), hits, SortRequest{Field: "score", Desc: true})
		require.NoError(t, err)
		for i := range reference {
			require.Equal(t, reference[i].Identifier, full[i].Identifier, "row %d", i)
		}
	}
}

func TestSortCursorFirstPageFallsBackWithoutACompleteLadder(t *testing.T) {
	shape := topFieldShape{span: 1 << 44, batches: 4, perBatch: 1500, missing: 5, negatives: true}
	owner := newTopFieldFixture(t, true, true, randomTopFieldBatches(rand.New(rand.NewSource(4)), shape, ladderMixed))     //nolint:gosec
	plainOwner := newTopFieldFixture(t, true, true, randomTopFieldBatches(rand.New(rand.NewSource(4)), shape, ladderNone)) //nolint:gosec
	view, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()
	plainView, err := plainOwner.Acquire(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, plainView.Close()) }()
	request := SortCursorRequest{Selection: topFieldCandidates(nil), SortField: "score", Desc: true, PageSize: 20}
	before := ladderDescents.Load()
	got := drainCursor(t, view, request, 3)
	require.Equal(t, before, ladderDescents.Load())
	requireSameSortedHits(t, drainCursor(t, plainView, request, 3), got, "merged mixed segment")
}
