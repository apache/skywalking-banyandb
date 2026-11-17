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

	"github.com/apache/skywalking-banyandb/pkg/convert"
)

const topFieldBase = int64(1_700_000_000_000_000_000)

type ladderMode int

const (
	ladderNone ladderMode = iota
	ladderAll
	ladderMixed
)

func (m ladderMode) String() string { return [...]string{"none", "all", "mixed"}[m] }

// topFieldDocument is one document with a scalar integer "score" sort field. A nil score leaves the field out.
func topFieldDocument(id int, score *int64, subject string, ladder bool) Document {
	document := Document{
		Identifier: []byte(fmt.Sprintf("doc-%07d", id)),
		Timestamp:  topFieldBase + int64(id)*1_000_000,
		Fields:     []Field{{Name: "status", Value: []byte(subject), Index: true, Store: true}},
	}
	if score != nil {
		document.Fields = append(document.Fields, Field{
			Name: "score", Value: convert.Int64ToBytes(*score), Index: true, Store: true, Sort: true, Ladder: ladder, LadderKey: *score,
		})
	}
	return document
}

type topFieldShape struct {
	span      int64
	batches   int
	perBatch  int
	missing   int // one in missing documents has no score; zero disables
	upserts   int // one in upserts documents rewrites an earlier identifier; zero disables
	ties      bool
	negatives bool
}

// randomTopFieldBatches builds batches whose ladder presence follows mode (mixed: every other batch has it).
func randomTopFieldBatches(rng *rand.Rand, shape topFieldShape, mode ladderMode) [][]Document {
	out := make([][]Document, shape.batches)
	next := 0
	for batch := range out {
		ladder := mode == ladderAll || (mode == ladderMixed && batch%2 == 0)
		for i := 0; i < shape.perBatch; i++ {
			id := next
			if shape.upserts > 0 && next > 0 && rng.Intn(shape.upserts) == 0 {
				id = rng.Intn(next)
			} else {
				next++
			}
			value := rng.Int63n(shape.span)
			if shape.negatives {
				value -= shape.span / 2
			}
			if shape.ties {
				value = rng.Int63n(30) - 10
			}
			var score *int64
			if shape.missing == 0 || rng.Intn(shape.missing) != 0 {
				score = &value
			}
			subject := "ok"
			if rng.Intn(5) == 0 {
				subject = "other"
			}
			out[batch] = append(out[batch], topFieldDocument(id, score, subject, ladder))
		}
	}
	return out
}

// newTopFieldFixture writes one native segment per batch, optionally persists and reopens, and optionally merges every
// segment into one first.
func newTopFieldFixture(t *testing.T, persisted, merged bool, batches [][]Document) *Owner {
	t.Helper()
	options := OwnerOptions{Lease: testLease{}, CompactionThreshold: -1}
	if persisted {
		options.Path = t.TempDir()
	}
	owner, err := NewOwner(options)
	require.NoError(t, err)
	for _, batch := range batches {
		require.NoError(t, owner.Batch(context.Background(), Batch{Documents: batch}))
	}
	if merged {
		require.NoError(t, owner.forceMergeAll(context.Background()))
	}
	if persisted {
		require.NoError(t, owner.Close())
		owner, err = NewOwner(options)
		require.NoError(t, err)
	}
	t.Cleanup(func() { require.NoError(t, owner.Close()) })
	return owner
}

func topFieldCandidates(timeRange *TimeRange) TermSetRequest {
	return TermSetRequest{
		Field: "status", Terms: [][]byte{[]byte("ok")}, Mode: MatchAnyTerm, MaxTerms: ^uint64(0),
		Scope: QueryScope{TimeRange: timeRange},
	}
}

// exhaustiveTopField is the reference: every candidate through MatchTermsSet, ranked by SortHits.
func exhaustiveTopField(t *testing.T, view *ReadView, request TopFieldRequest) []QueryHit {
	t.Helper()
	hits, err := view.MatchTermsSet(context.Background(), request.Candidates)
	require.NoError(t, err)
	sorted, err := view.SortHits(context.Background(), hits, SortRequest{Field: request.Field, Desc: request.Desc, Limit: request.Limit})
	require.NoError(t, err)
	return sorted
}

func TestLadderFieldIsWrittenAndSurvivesPersistAndMerge(t *testing.T) {
	rng := rand.New(rand.NewSource(11)) //nolint:gosec // deterministic test data
	shape := topFieldShape{span: 1 << 40, batches: 3, perBatch: 300, negatives: true}
	for _, mode := range []ladderMode{ladderNone, ladderAll} {
		for _, persisted := range []bool{false, true} {
			for _, merged := range []bool{false, true} {
				if merged && !persisted {
					continue
				}
				t.Run(fmt.Sprintf("ladder=%s/persisted=%v/merged=%v", mode, persisted, merged), func(t *testing.T) {
					owner := newTopFieldFixture(t, persisted, merged, randomTopFieldBatches(rng, shape, mode))
					view, err := owner.Acquire(context.Background())
					require.NoError(t, err)
					defer func() { require.NoError(t, view.Close()) }()
					require.NotEmpty(t, view.root.segments)
					for index := range view.root.segments {
						segment, segErr := view.segmentAt(uint64(index))
						require.NoError(t, segErr)
						fields, fieldsErr := segment.handle.reader.Fields()
						require.NoError(t, fieldsErr)
						if mode == ladderNone {
							require.NotContains(t, fields, "score"+LadderFieldSuffix)
							require.False(t, segment.handle.ladderComplete("score"))
							continue
						}
						require.Contains(t, fields, "score"+LadderFieldSuffix)
						require.True(t, segment.handle.ladderComplete("score"), "segment %d", index)
						require.Contains(t, segment.handle.indexedFields, "score"+LadderFieldSuffix)
						// The companion is not stored and the sort field's own dictionary is untouched.
						terms, termsErr := segment.handle.reader.Terms("score")
						require.NoError(t, termsErr)
						for _, term := range terms {
							require.Len(t, term, 8, "the sort field keeps only its raw keyword terms")
						}
					}
				})
			}
		}
	}
}

func TestTopFieldHitsMatchesExhaustiveSort(t *testing.T) {
	const span = int64(1) << 44
	shapes := map[string]topFieldShape{
		"wide":    {span: span, batches: 4, perBatch: 1500, missing: 7, upserts: 9, negatives: true},
		"ties":    {span: span, batches: 3, perBatch: 1500, missing: 11, upserts: 9, ties: true},
		"dense":   {span: 5000, batches: 3, perBatch: 1500, upserts: 20, negatives: true},
		"missing": {span: span, batches: 2, perBatch: 1500, missing: 2},
	}
	for shapeName, shape := range shapes {
		for _, mode := range []ladderMode{ladderNone, ladderAll, ladderMixed} {
			for _, storage := range []struct{ persisted, merged bool }{{false, false}, {true, false}, {true, true}} {
				name := fmt.Sprintf("%s/ladder=%s/persisted=%v/merged=%v", shapeName, mode, storage.persisted, storage.merged)
				t.Run(name, func(t *testing.T) {
					rng := rand.New(rand.NewSource(5)) //nolint:gosec // deterministic test data
					owner := newTopFieldFixture(t, storage.persisted, storage.merged, randomTopFieldBatches(rng, shape, mode))
					view, err := owner.Acquire(context.Background())
					require.NoError(t, err)
					defer func() { require.NoError(t, view.Close()) }()
					ranges := []*TimeRange{
						nil,
						{Lower: topFieldBase + 500*1_000_000, Upper: topFieldBase + 3000*1_000_000, IncludesLower: true, IncludesUpper: true},
					}
					for _, timeRange := range ranges {
						for _, desc := range []bool{false, true} {
							for _, limit := range []int{1, 7, 100, 700, 100000} {
								request := TopFieldRequest{Candidates: topFieldCandidates(timeRange), Field: "score", Limit: limit, Desc: desc}
								want := exhaustiveTopField(t, view, request)
								got, topErr := view.TopFieldHits(context.Background(), request)
								require.NoError(t, topErr)
								requireSameHits(t, want, got, fmt.Sprintf("range=%v desc=%v limit=%d", timeRange != nil, desc, limit))
							}
						}
					}
				})
			}
		}
	}
}

func TestTopFieldHitsUsesTheLadderOnlyWhenComplete(t *testing.T) {
	shape := topFieldShape{span: 1 << 44, batches: 3, perBatch: 1500, negatives: true}
	for _, test := range []struct {
		mode    ladderMode
		merged  bool
		descent bool
	}{
		{mode: ladderAll, descent: true},
		{mode: ladderAll, merged: true, descent: true},
		{mode: ladderNone},
		{mode: ladderMixed, merged: true}, // a merged segment that holds documents with and without the ladder is incomplete
		{mode: ladderMixed, descent: true},
	} {
		t.Run(fmt.Sprintf("ladder=%s/merged=%v", test.mode, test.merged), func(t *testing.T) {
			rng := rand.New(rand.NewSource(8)) //nolint:gosec // deterministic test data
			owner := newTopFieldFixture(t, true, test.merged, randomTopFieldBatches(rng, shape, test.mode))
			view, err := owner.Acquire(context.Background())
			require.NoError(t, err)
			defer func() { require.NoError(t, view.Close()) }()
			request := TopFieldRequest{Candidates: topFieldCandidates(nil), Field: "score", Limit: 10, Desc: true}
			before := ladderDescents.Load()
			got, err := view.TopFieldHits(context.Background(), request)
			require.NoError(t, err)
			requireSameHits(t, exhaustiveTopField(t, view, request), got, "result")
			ran := ladderDescents.Load() - before
			if test.descent {
				require.Positive(t, ran, "the ladder must serve a complete segment")
			} else {
				require.Zero(t, ran, "no descent over a segment without a complete ladder")
			}
		})
	}
}

func TestTopFieldHitsRejectsInvalidRequests(t *testing.T) {
	owner := newTopFieldFixture(t, false, false, [][]Document{{topFieldDocument(1, nil, "ok", false)}})
	view, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()
	_, err = view.TopFieldHits(context.Background(), TopFieldRequest{Candidates: topFieldCandidates(nil), Field: "score"})
	require.ErrorIs(t, err, ErrInvalidQuery)
	_, err = view.TopFieldHits(context.Background(), TopFieldRequest{Candidates: topFieldCandidates(nil), Limit: 1})
	require.ErrorIs(t, err, ErrInvalidQuery)
	hits, err := view.TopFieldHits(context.Background(), TopFieldRequest{Field: "score", Limit: 1})
	require.NoError(t, err)
	require.Empty(t, hits)
}

// TestTopFieldHitsAndCursorSkipUntimestampedDocumentsUnderARange puts the best-ranked documents outside any time range
// (they carry no timestamp), so a descent that ignored the range would fill the page with documents the range drops.
func TestTopFieldHitsAndCursorSkipUntimestampedDocumentsUnderARange(t *testing.T) {
	var documents []Document
	for i := 0; i < 600; i++ {
		score := int64(1000 + i)
		document := topFieldDocument(i, &score, "ok", true)
		if i >= 500 {
			document.Timestamp = 0
		}
		documents = append(documents, document)
	}
	owner := newTopFieldFixture(t, false, false, [][]Document{documents})
	view, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()
	timeRange := &TimeRange{Lower: topFieldBase - 1, Upper: topFieldBase + 1_000_000*1000, IncludesLower: true, IncludesUpper: true}
	request := TopFieldRequest{Candidates: topFieldCandidates(timeRange), Field: "score", Limit: 20, Desc: true}
	got, err := view.TopFieldHits(context.Background(), request)
	require.NoError(t, err)
	require.Len(t, got, 20)
	requireSameHits(t, exhaustiveTopField(t, view, request), got, "top field")
	page := drainCursor(t, view, SortCursorRequest{Selection: request.Candidates, SortField: "score", Desc: true, PageSize: 20}, 1)
	require.Len(t, page, 20)
	for i := range page {
		require.Equal(t, got[i].Identifier, page[i].Identifier, "row %d", i)
	}
}

// A page that more documents without the field than with it must fill is completed from the documents without it, which
// rank last by identifier: the descent alone would return too few rows.
func TestTopFieldHitsFillsThePageWithDocumentsMissingTheField(t *testing.T) {
	var documents []Document
	for i := 0; i < 1000; i++ {
		var score *int64
		if i%200 == 3 {
			value := int64(i)
			score = &value
		}
		documents = append(documents, topFieldDocument(i, score, "ok", true))
	}
	owner := newTopFieldFixture(t, true, false, [][]Document{documents})
	view, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()
	for _, desc := range []bool{false, true} {
		request := TopFieldRequest{Candidates: topFieldCandidates(nil), Field: "score", Limit: 10, Desc: desc}
		got, topErr := view.TopFieldHits(context.Background(), request)
		require.NoError(t, topErr)
		require.Len(t, got, 10)
		requireSameHits(t, exhaustiveTopField(t, view, request), got, fmt.Sprintf("desc=%v", desc))
	}
}
