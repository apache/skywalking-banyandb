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
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

const topTimeBase = int64(1_700_000_000_000_000_000)

type topTimeFixture struct {
	owner *Owner
	path  string
}

// newTopTimeFixture writes documents in several batches (one native segment per batch) and, when persisted, closes and
// reopens the owner so the segments are file-backed. upserts rewrite some identifiers with a new timestamp, which marks the
// older document deleted.
func newTopTimeFixture(t *testing.T, persisted bool, batches [][]Document) *topTimeFixture {
	t.Helper()
	fixture := &topTimeFixture{}
	options := OwnerOptions{Lease: testLease{}, CompactionThreshold: -1}
	if persisted {
		fixture.path = t.TempDir()
		options.Path = fixture.path
	}
	owner, err := NewOwner(options)
	require.NoError(t, err)
	for _, batch := range batches {
		require.NoError(t, owner.Batch(context.Background(), Batch{Documents: batch}))
	}
	if persisted {
		require.NoError(t, owner.Close())
		owner, err = NewOwner(options)
		require.NoError(t, err)
	}
	t.Cleanup(func() { require.NoError(t, owner.Close()) })
	fixture.owner = owner
	return fixture
}

func topTimeDocument(id int, timestamp int64, subject string) Document {
	return Document{
		Identifier: []byte(fmt.Sprintf("doc-%07d", id)),
		Timestamp:  timestamp,
		Fields:     []Field{{Name: "status", Value: []byte(subject), Index: true, Store: true}},
	}
}

func topTimeRequestFor(subject string, timeRange *TimeRange, limit int, desc bool) TopTimeRequest {
	return TopTimeRequest{
		Candidates: TermSetRequest{
			Field: "status", Terms: [][]byte{[]byte(subject)}, Mode: MatchAnyTerm, MaxTerms: ^uint64(0),
			Scope: QueryScope{TimeRange: timeRange},
		},
		Limit: limit, Desc: desc,
	}
}

// exhaustiveTop is the reference: every candidate through MatchTermsSet, sorted, truncated.
func exhaustiveTop(t *testing.T, view *ReadView, request TopTimeRequest) []QueryHit {
	t.Helper()
	hits, err := view.MatchTermsSet(context.Background(), request.Candidates)
	require.NoError(t, err)
	sort.SliceStable(hits, func(i, j int) bool { return topTimeLess(hits[i], hits[j], request.Desc) })
	if len(hits) > request.Limit {
		hits = hits[:request.Limit]
	}
	return hits
}

func requireSameHits(t *testing.T, want, got []QueryHit, message string) {
	t.Helper()
	require.Len(t, got, len(want), message)
	for i := range want {
		require.Equal(t, want[i].Identifier, got[i].Identifier, "%s row %d", message, i)
		require.Equal(t, want[i].Timestamp, got[i].Timestamp, "%s row %d", message, i)
		require.Equal(t, want[i].Segment, got[i].Segment, "%s row %d", message, i)
		require.Equal(t, want[i].DocumentNumber, got[i].DocumentNumber, "%s row %d", message, i)
	}
}

func randomTopTimeBatches(rng *rand.Rand, batches, perBatch int, span int64, ties bool) [][]Document {
	out := make([][]Document, batches)
	id := 0
	for b := range out {
		for i := 0; i < perBatch; i++ {
			timestamp := topTimeBase + rng.Int63n(span)
			if ties {
				timestamp = topTimeBase + rng.Int63n(40)*1_000_000_000 // many documents share a timestamp
			}
			subject := "ok"
			if rng.Intn(5) == 0 {
				subject = "other"
			}
			out[b] = append(out[b], topTimeDocument(id, timestamp, subject))
			id++
		}
	}
	return out
}

func TestTopTimeHitsMatchesExhaustiveSort(t *testing.T) {
	const span = int64(5 * 24 * 60 * 60 * 1e9)
	for _, persisted := range []bool{false, true} {
		for _, ties := range []bool{false, true} {
			t.Run(fmt.Sprintf("persisted=%v/ties=%v", persisted, ties), func(t *testing.T) {
				rng := rand.New(rand.NewSource(3)) //nolint:gosec // deterministic test data
				batches := randomTopTimeBatches(rng, 5, 2000, span, ties)
				fixture := newTopTimeFixture(t, persisted, batches)
				view, err := fixture.owner.Acquire(context.Background())
				require.NoError(t, err)
				defer func() { require.NoError(t, view.Close()) }()
				ranges := map[string]*TimeRange{
					"whole":   {Lower: topTimeBase - 1, Upper: topTimeBase + span + 1, IncludesLower: true, IncludesUpper: true},
					"partial": {Lower: topTimeBase + span/3, Upper: topTimeBase + span/2, IncludesLower: true, IncludesUpper: true},
					"narrow":  {Lower: topTimeBase + span/2, Upper: topTimeBase + span/2 + span/500, IncludesLower: true, IncludesUpper: false},
				}
				for name, timeRange := range ranges {
					for _, desc := range []bool{true, false} {
						for _, limit := range []int{1, 7, 100, 700, 20000} {
							request := topTimeRequestFor("ok", timeRange, limit, desc)
							got, topErr := view.TopTimeHits(context.Background(), request)
							require.NoError(t, topErr)
							requireSameHits(t, exhaustiveTop(t, view, request), got, fmt.Sprintf("%s desc=%v limit=%d", name, desc, limit))
						}
					}
				}
			})
		}
	}
}

// Rewriting an identifier marks its older document deleted. The trie postings still hold it, so the descent must count
// only live documents or it stops early and returns too few rows.
func TestTopTimeHitsIgnoresDeletedDocuments(t *testing.T) {
	const span = int64(24 * 60 * 60 * 1e9)
	rng := rand.New(rand.NewSource(5)) //nolint:gosec // deterministic test data
	batches := randomTopTimeBatches(rng, 3, 3000, span, false)
	// rewrite one in three of the first batch's documents into a new batch at a different time
	var rewrites []Document
	for _, doc := range batches[0] {
		if rng.Intn(3) == 0 {
			var id int
			_, err := fmt.Sscanf(string(doc.Identifier), "doc-%d", &id)
			require.NoError(t, err)
			rewrites = append(rewrites, topTimeDocument(id, topTimeBase+rng.Int63n(span), "ok"))
		}
	}
	require.NotEmpty(t, rewrites)
	batches = append(batches, rewrites)
	for _, persisted := range []bool{false, true} {
		fixture := newTopTimeFixture(t, persisted, batches)
		view, err := fixture.owner.Acquire(context.Background())
		require.NoError(t, err)
		timeRange := &TimeRange{Lower: topTimeBase - 1, Upper: topTimeBase + span + 1, IncludesLower: true, IncludesUpper: true}
		for _, desc := range []bool{true, false} {
			for _, limit := range []int{5, 50, 500} {
				request := topTimeRequestFor("ok", timeRange, limit, desc)
				got, topErr := view.TopTimeHits(context.Background(), request)
				require.NoError(t, topErr)
				requireSameHits(t, exhaustiveTop(t, view, request), got, fmt.Sprintf("persisted=%v desc=%v limit=%d", persisted, desc, limit))
			}
		}
		require.NoError(t, view.Close())
	}
}

// More documents than the limit can share the limit-th timestamp; every one of them must reach the tie-break.
func TestTopTimeHitsKeepsEveryTieOfTheLastRow(t *testing.T) {
	var docs []Document
	for i := 0; i < 3000; i++ {
		timestamp := topTimeBase + int64(i%3)*1_000_000_000
		docs = append(docs, topTimeDocument(i, timestamp, "ok"))
	}
	fixture := newTopTimeFixture(t, false, [][]Document{docs})
	view, err := fixture.owner.Acquire(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()
	timeRange := &TimeRange{Lower: topTimeBase - 1, Upper: topTimeBase + 10_000_000_000, IncludesLower: true, IncludesUpper: true}
	for _, desc := range []bool{true, false} {
		request := topTimeRequestFor("ok", timeRange, 10, desc)
		got, topErr := view.TopTimeHits(context.Background(), request)
		require.NoError(t, topErr)
		requireSameHits(t, exhaustiveTop(t, view, request), got, fmt.Sprintf("desc=%v", desc))
	}
}

func TestTopTimeHitsRejectsInvalidRequests(t *testing.T) {
	fixture := newTopTimeFixture(t, false, [][]Document{{topTimeDocument(1, topTimeBase, "ok")}})
	view, err := fixture.owner.Acquire(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()
	timeRange := &TimeRange{Lower: 0, Upper: topTimeBase * 2, IncludesLower: true, IncludesUpper: true}
	_, err = view.TopTimeHits(context.Background(), topTimeRequestFor("ok", timeRange, 0, true))
	require.ErrorIs(t, err, ErrInvalidQuery)
	_, err = view.TopTimeHits(context.Background(), topTimeRequestFor("ok", nil, 5, true))
	require.ErrorIs(t, err, ErrInvalidQuery)
	hits, err := view.TopTimeHits(context.Background(), topTimeRequestFor("missing", timeRange, 5, true))
	require.NoError(t, err)
	require.Empty(t, hits)
}

// The newest documents of a segment are the ones rewritten (moved far into the past), so the best ranks of that segment
// are deleted documents whose postings the trie still holds. Counting them would stop the descent before it reaches the
// live documents that rank below them, and no other segment can make up for it.
func TestTopTimeHitsSkipsDeletedAtTheBestRanks(t *testing.T) {
	const documents = 4000
	docs := make([]Document, 0, documents)
	for i := 0; i < documents; i++ {
		docs = append(docs, topTimeDocument(i, topTimeBase+int64(i)*1_000_000_000, "ok"))
	}
	var rewrites []Document
	for i := documents - 1500; i < documents; i++ { // the 1500 newest documents move to before every other one
		rewrites = append(rewrites, topTimeDocument(i, topTimeBase-int64(documents-i)*1_000_000_000, "ok"))
	}
	for _, persisted := range []bool{false, true} {
		fixture := newTopTimeFixture(t, persisted, [][]Document{docs, rewrites})
		view, err := fixture.owner.Acquire(context.Background())
		require.NoError(t, err)
		timeRange := &TimeRange{Lower: topTimeBase - 10_000_000_000_000, Upper: topTimeBase + 10_000_000_000_000, IncludesLower: true, IncludesUpper: true}
		for _, desc := range []bool{true, false} {
			for _, limit := range []int{5, 50, 300} {
				request := topTimeRequestFor("ok", timeRange, limit, desc)
				got, topErr := view.TopTimeHits(context.Background(), request)
				require.NoError(t, topErr)
				requireSameHits(t, exhaustiveTop(t, view, request), got, fmt.Sprintf("persisted=%v desc=%v limit=%d", persisted, desc, limit))
			}
		}
		require.NoError(t, view.Close())
	}
}
