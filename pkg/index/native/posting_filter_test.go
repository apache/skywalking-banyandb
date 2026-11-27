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
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/query"
)

func bucketTerms(values ...string) *PostingFilter {
	terms := make([][]byte, len(values))
	for i := range values {
		terms[i] = []byte(values[i])
	}
	return FilterTerms(TermSetRequest{Field: "bucket", Terms: terms, Mode: MatchAnyTerm})
}

func bucketRange(lower, upper string, includesLower, includesUpper, singleTerm bool) *PostingFilter {
	request := RangeRequest{Field: "bucket", IncludesLower: includesLower, IncludesUpper: includesUpper, MaxTerms: ^uint64(0)}
	if lower != "" {
		request.Lower = []byte(lower)
	}
	if upper != "" {
		request.Upper = []byte(upper)
	}
	if singleTerm {
		return FilterSingleTermRanges(request)
	}
	return FilterRanges(request)
}

// postingFilterCase is a filter and the plain predicate over a document's bucket that it must agree with.
type postingFilterCase struct {
	filter *PostingFilter
	keep   func(bucket string) bool
	name   string
}

func postingFilterCases() []postingFilterCase {
	in := func(values ...string) func(string) bool {
		return func(bucket string) bool {
			for _, value := range values {
				if bucket == value {
					return true
				}
			}
			return false
		}
	}
	inRange := func(bucket string) bool { return bucket >= "b10" && bucket < "b20" }
	status := FilterTerms(TermSetRequest{Field: "status", Terms: [][]byte{[]byte("ok")}, Mode: MatchAnyTerm})
	nothing := func(string) bool { return false }
	return []postingFilterCase{
		{name: "one value", filter: bucketTerms("b07"), keep: in("b07")},
		{name: "any of values", filter: bucketTerms("b07", "b41"), keep: in("b07", "b41")},
		{
			name: "all of one term", keep: in("b07"),
			filter: FilterTerms(TermSetRequest{Field: "bucket", Terms: [][]byte{[]byte("b07")}, Mode: MatchAllTerms}),
		},
		{name: "and of an exact value and the status", filter: AllOf(bucketTerms("b07", "b41"), status), keep: in("b07", "b41")},
		{name: "or", filter: AnyOf(bucketTerms("b07"), bucketTerms("b41")), keep: in("b07", "b41")},
		{name: "not", filter: NotOf(bucketTerms("b07")), keep: func(bucket string) bool { return bucket != "b07" }},
		{
			name: "not of an or", filter: NotOf(AnyOf(bucketTerms("b07"), bucketTerms("b41"))),
			keep: func(bucket string) bool { return !in("b07", "b41")(bucket) },
		},
		{
			name: "and of an exact value and a negation", keep: in("b07", "b09"),
			filter: AllOf(bucketTerms("b07", "b08", "b09"), NotOf(bucketTerms("b08"))),
		},
		{name: "range", filter: bucketRange("b10", "b20", true, false, false), keep: inRange},
		{name: "single-term range", filter: bucketRange("b10", "b20", true, false, true), keep: inRange},
		{
			name: "or of a range and a value", keep: func(bucket string) bool { return inRange(bucket) || bucket == "b41" },
			filter: AnyOf(bucketRange("b10", "b20", true, false, true), bucketTerms("b41")),
		},
		{
			name: "and of two ranges on one field", keep: inRange,
			filter: AllOf(bucketRange("b10", "", true, false, false), bucketRange("", "b20", false, false, false)),
		},
		{
			name: "and of two single-term ranges on one field", keep: inRange,
			filter: AllOf(bucketRange("b10", "", true, false, true), bucketRange("", "b20", false, false, true)),
		},
		{
			name: "and of single-term ranges meeting at one value", keep: in("b15"),
			filter: AllOf(bucketRange("b15", "", true, false, true), bucketRange("", "b15", false, true, true)),
		},
		{
			name: "and of single-term ranges that exclude each other", keep: nothing,
			filter: AllOf(bucketRange("b30", "", true, false, true), bucketRange("", "b20", false, false, true)),
		},
		{
			name: "and of a single-term range and a value on the same field", keep: in("b12"),
			filter: AllOf(bucketRange("b10", "b20", true, false, true), bucketTerms("b12", "b41")),
		},
		{name: "match all", filter: MatchAll(), keep: func(string) bool { return true }},
		{name: "match none", filter: MatchNone(), keep: nothing},
		{name: "no term", filter: FilterTerms(TermSetRequest{Field: "bucket"}), keep: nothing},
		{name: "no match", filter: bucketTerms("absent"), keep: nothing},
		{name: "empty and", filter: AllOf(), keep: func(string) bool { return true }},
		{name: "empty or", filter: AnyOf(), keep: nothing},
	}
}

func hitKeys(hits []QueryHit) []string {
	keys := make([]string, len(hits))
	for i := range hits {
		keys[i] = fmt.Sprintf("%d/%d", hits[i].Segment, hits[i].DocumentNumber)
	}
	sort.Strings(keys)
	return keys
}

// keepHits is the reference: the hits whose stored bucket the plain predicate keeps.
func keepHits(t *testing.T, view *ReadView, hits []QueryHit, keep func(bucket string) bool) []QueryHit {
	t.Helper()
	kept := make([]QueryHit, 0, len(hits))
	for _, hit := range hits {
		projected, err := view.ProjectHit(context.Background(), hit, "bucket")
		require.NoError(t, err)
		require.Len(t, projected.Fields["bucket"], 1)
		if keep(string(projected.Fields["bucket"][0])) {
			kept = append(kept, hit)
		}
	}
	return kept
}

// reservedBy returns the allocation bytes fn reserves against the query.
func reservedBy(t *testing.T, fn func(ctx context.Context)) uint64 {
	t.Helper()
	var total atomic.Uint64
	ctx, scope := query.WithAllocationScope(context.Background(), func(bytes int64) error {
		total.Add(uint64(bytes))
		return nil
	}, func(int64) {})
	defer scope.Close()
	fn(ctx)
	return total.Load()
}

func postingFilterBatches(rng *rand.Rand) [][]Document {
	const span = int64(24 * 60 * 60 * 1e9)
	out := make([][]Document, 3)
	id := 0
	for b := range out {
		for i := 0; i < 1500; i++ {
			doc := topTimeDocument(id, topTimeBase+rng.Int63n(span), "ok")
			doc.Fields = append(doc.Fields, Field{Name: "bucket", Value: []byte(fmt.Sprintf("b%02d", id%50)), Index: true, Store: true})
			out[b] = append(out[b], doc)
			id++
		}
	}
	return out
}

// A posting-level filter must select exactly the documents a plain predicate over their stored value does, in the bounded
// listing and in the time-ordered top search.
func TestPostingFiltersMatchAPlainPredicate(t *testing.T) {
	const span = int64(24 * 60 * 60 * 1e9)
	rng := rand.New(rand.NewSource(11)) //nolint:gosec // deterministic test data
	for _, persisted := range []bool{false, true} {
		fixture := newTopTimeFixture(t, persisted, postingFilterBatches(rng))
		view, err := fixture.owner.Acquire(context.Background())
		require.NoError(t, err)
		timeRange := &TimeRange{Lower: topTimeBase - 1, Upper: topTimeBase + span + 1, IncludesLower: true, IncludesUpper: true}
		candidates := topTimeRequestFor("ok", timeRange, 1, false).Candidates
		all, matchErr := view.MatchTermsSet(context.Background(), candidates)
		require.NoError(t, matchErr)
		for _, tc := range postingFilterCases() {
			t.Run(fmt.Sprintf("persisted=%v/%s", persisted, tc.name), func(t *testing.T) {
				want := keepHits(t, view, all, tc.keep)
				for _, limit := range []int{0, 1, 5, len(want), len(want) + 10} {
					got, truncated, listErr := view.MatchTermsSetPrefixFiltered(context.Background(), candidates, tc.filter, limit)
					require.NoError(t, listErr)
					if limit == 0 || limit >= len(want) {
						require.Equal(t, hitKeys(want), hitKeys(got), "limit=%d", limit)
						require.False(t, truncated, "limit=%d", limit)
						continue
					}
					require.Len(t, got, limit)
					require.True(t, truncated)
					require.Subset(t, hitKeys(want), hitKeys(got))
				}
				for _, desc := range []bool{true, false} {
					for _, limit := range []int{1, 7, 100} {
						request := topTimeRequestFor("ok", timeRange, limit, desc)
						request.Filter = tc.filter
						got, topErr := view.TopTimeHits(context.Background(), request)
						require.NoError(t, topErr)
						exhaustive := append([]QueryHit(nil), want...)
						sort.SliceStable(exhaustive, func(i, j int) bool { return topTimeLess(exhaustive[i], exhaustive[j], desc) })
						if len(exhaustive) > limit {
							exhaustive = exhaustive[:limit]
						}
						requireSameHits(t, exhaustive, got, fmt.Sprintf("desc=%v limit=%d", desc, limit))
					}
				}
			})
		}
		require.NoError(t, view.Close())
	}
}

// The point of a posting-level filter is to decode only the documents it keeps: a selective filter and a small limit must
// reserve a small share of what listing every candidate reserves. If the bitmap were narrowed after the documents were
// decoded, the hits would be the same and only this test would notice.
func TestPostingFiltersDecodeOnlyTheDocumentsTheyKeep(t *testing.T) {
	const span = int64(24 * 60 * 60 * 1e9)
	rng := rand.New(rand.NewSource(13)) //nolint:gosec // deterministic test data
	fixture := newTopTimeFixture(t, false, postingFilterBatches(rng))
	view, err := fixture.owner.Acquire(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()
	timeRange := &TimeRange{Lower: topTimeBase - 1, Upper: topTimeBase + span + 1, IncludesLower: true, IncludesUpper: true}
	candidates := topTimeRequestFor("ok", timeRange, 1, false).Candidates
	listAll := reservedBy(t, func(ctx context.Context) {
		_, _, listErr := view.MatchTermsSetPrefixFiltered(ctx, candidates, nil, 0)
		require.NoError(t, listErr)
	})
	require.NotZero(t, listAll)
	for name, tc := range map[string]struct {
		filter *PostingFilter
		limit  int
	}{
		"a selective value":          {filter: bucketTerms("b07"), limit: 0},
		"an or of two values":        {filter: AnyOf(bucketTerms("b07"), bucketTerms("b41")), limit: 0},
		"a single-term range":        {filter: bucketRange("b10", "b11", true, false, true), limit: 0},
		"no filter, a small limit":   {filter: nil, limit: 5},
		"a selective value, a limit": {filter: bucketTerms("b07"), limit: 5},
	} {
		reserved := reservedBy(t, func(ctx context.Context) {
			_, _, listErr := view.MatchTermsSetPrefixFiltered(ctx, candidates, tc.filter, tc.limit)
			require.NoError(t, listErr)
		})
		require.Less(t, reserved*10, listAll*3, "%s reserved %d bytes against %d for listing every candidate", name, reserved, listAll)
	}
}

// MatchTermsSetPrefixFiltered without a filter lists at most the limit, takes a subset of what MatchTermsSet lists, reports
// whether it left candidates out, and lists everything for a non-positive limit.
func TestMatchTermsSetPrefixLimitsTheListing(t *testing.T) {
	const span = int64(24 * 60 * 60 * 1e9)
	rng := rand.New(rand.NewSource(9)) //nolint:gosec // deterministic test data
	batches := randomTopTimeBatches(rng, 3, 1000, span, true)
	for _, persisted := range []bool{false, true} {
		fixture := newTopTimeFixture(t, persisted, batches)
		view, err := fixture.owner.Acquire(context.Background())
		require.NoError(t, err)
		timeRange := &TimeRange{Lower: topTimeBase - 1, Upper: topTimeBase + span + 1, IncludesLower: true, IncludesUpper: true}
		request := topTimeRequestFor("ok", timeRange, 1, false).Candidates
		everything, err := view.MatchTermsSet(context.Background(), request)
		require.NoError(t, err)
		require.NotEmpty(t, everything)
		known := make(map[string]struct{}, len(everything))
		for _, key := range hitKeys(everything) {
			known[key] = struct{}{}
		}
		for _, limit := range []int{1, 7, 1000, len(everything) - 1, len(everything), len(everything) + 1} {
			hits, truncated, listErr := view.MatchTermsSetPrefixFiltered(context.Background(), request, nil, limit)
			require.NoError(t, listErr)
			require.Len(t, hits, min(limit, len(everything)), "limit=%d", limit)
			require.Equal(t, limit < len(everything), truncated, "limit=%d", limit)
			for _, key := range hitKeys(hits) {
				require.Contains(t, known, key)
			}
		}
		hits, truncated, listErr := view.MatchTermsSetPrefixFiltered(context.Background(), request, nil, 0)
		require.NoError(t, listErr)
		require.Len(t, hits, len(everything))
		require.False(t, truncated)
		require.NoError(t, view.Close())
	}
}

// Ranges of one field fold only when they are siblings of one conjunction, however the criteria nested it: a conjunction of
// three conditions is built as AllOf(AllOf(a, b), c), and the bounds of a field that sit apart in it must still meet.
func TestPostingFilterFoldsRangesOfNestedConjunctions(t *testing.T) {
	const span = int64(24 * 60 * 60 * 1e9)
	rng := rand.New(rand.NewSource(11)) //nolint:gosec // deterministic test data
	fixture := newTopTimeFixture(t, false, postingFilterBatches(rng))
	view, err := fixture.owner.Acquire(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()
	timeRange := &TimeRange{Lower: topTimeBase - 1, Upper: topTimeBase + span + 1, IncludesLower: true, IncludesUpper: true}
	candidates := topTimeRequestFor("ok", timeRange, 1, false).Candidates
	status := FilterTerms(TermSetRequest{Field: "status", Terms: [][]byte{[]byte("ok")}, Mode: MatchAnyTerm})
	bounded := func(lower, upper string) *PostingFilter {
		// Each bound may expand at most 15 of the 50 terms of the field: an unfolded lower bound b10.. spans 40.
		limited := func(f *PostingFilter) *PostingFilter {
			f.rng.MaxTerms = 15
			return f
		}
		return AllOf(AllOf(limited(bucketRange(lower, "", true, false, true)), status), limited(bucketRange("", upper, false, false, true)))
	}
	hits, _, listErr := view.MatchTermsSetPrefixFiltered(context.Background(), candidates, bounded("b10", "b20"), 0)
	require.NoError(t, listErr, "the folded range [b10, b20) expands 10 terms, within the 15 each bound may expand")
	require.NotEmpty(t, hits)
	for _, hit := range keepHits(t, view, hits, func(bucket string) bool { return bucket >= "b10" && bucket < "b20" }) {
		require.NotZero(t, hit.Timestamp)
	}
	require.Len(t, keepHits(t, view, hits, func(bucket string) bool { return bucket >= "b10" && bucket < "b20" }), len(hits))
	// The conjunction is one flat list whatever the nesting.
	nested := AllOf(AllOf(bucketRange("b10", "", true, false, true), status), bucketRange("", "b20", false, false, true))
	require.Len(t, nested.children, 2, "the two ranges fold into one beside the status")
}

// A range over a field that can hold several terms must not fold: a document with the terms "apple" and "zebra" matches
// "x >= m" and "x <= c" taken one at a time, and matches no term of the folded range [m, c].
func TestPostingFilterKeepsMultiTermDocumentsUnfolded(t *testing.T) {
	owner, err := NewOwner(OwnerOptions{Lease: testLease{}})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, owner.Close()) })
	words := Field{Name: "words", Terms: []Term{{Value: []byte("apple")}, {Value: []byte("zebra")}}, Index: true}
	require.NoError(t, owner.Batch(context.Background(), Batch{Documents: []Document{
		{Identifier: []byte("both"), Fields: []Field{{Name: "kind", Value: []byte("doc"), Index: true}, words}},
	}}))
	view, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()
	candidates := TermSetRequest{Field: "kind", Terms: [][]byte{[]byte("doc")}, Mode: MatchAnyTerm}
	wordsRange := func(lower, upper string, includesLower, includesUpper, singleTerm bool) *PostingFilter {
		request := RangeRequest{Field: "words", IncludesLower: includesLower, IncludesUpper: includesUpper, MaxTerms: ^uint64(0)}
		if lower != "" {
			request.Lower = []byte(lower)
		}
		if upper != "" {
			request.Upper = []byte(upper)
		}
		if singleTerm {
			return FilterSingleTermRanges(request)
		}
		return FilterRanges(request)
	}
	// The hit-level filter keeps the document: zebra >= m, and apple <= c.
	hitLevel, err := view.MatchTermsSet(context.Background(), candidates)
	require.NoError(t, err)
	hitLevel, err = view.FilterRange(context.Background(), hitLevel, RangeRequest{Field: "words", Lower: []byte("m"), IncludesLower: true, MaxTerms: ^uint64(0)})
	require.NoError(t, err)
	hitLevel, err = view.FilterRange(context.Background(), hitLevel, RangeRequest{Field: "words", Upper: []byte("c"), IncludesUpper: true, MaxTerms: ^uint64(0)})
	require.NoError(t, err)
	require.Len(t, hitLevel, 1)

	multi := AllOf(wordsRange("m", "", true, false, false), wordsRange("", "c", false, true, false))
	require.Len(t, multi.children, 2, "ranges that are not single-term stay apart")
	got, _, err := view.MatchTermsSetPrefixFiltered(context.Background(), candidates, multi, 0)
	require.NoError(t, err)
	require.Len(t, got, 1, "the posting filter keeps the document the hit-level filter keeps")

	// A field that is single-term for every document does fold, and the folded range is empty here.
	folded := AllOf(wordsRange("m", "", true, false, true), wordsRange("", "c", false, true, true))
	require.Len(t, folded.children, 1)
}

// The leaves of a filter keep what they evaluated for a segment, so a search that evaluates the filter again (a chunk that
// grows, a fallback) expands a wildcard pattern once and spends the query's shared pattern budget once.
func TestPostingFilterEvaluatesALeafOncePerSegment(t *testing.T) {
	const span = int64(24 * 60 * 60 * 1e9)
	rng := rand.New(rand.NewSource(11)) //nolint:gosec // deterministic test data
	fixture := newTopTimeFixture(t, false, postingFilterBatches(rng))
	view, err := fixture.owner.Acquire(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()
	timeRange := &TimeRange{Lower: topTimeBase - 1, Upper: topTimeBase + span + 1, IncludesLower: true, IncludesUpper: true}
	candidates := topTimeRequestFor("ok", timeRange, 1, false).Candidates
	budget := NewWildcardBudget(WildcardLimits{})
	filter := FilterTerms(TermSetRequest{
		Field: "bucket", PatternBudget: budget, MaxTerms: ^uint64(0),
		WildcardPatterns: []WildcardPattern{{Pattern: "b1%", Syntax: WildcardSyntaxSQL}},
	})
	// Listing every candidate evaluates the filter in every segment once.
	first, _, err := view.MatchTermsSetPrefixFiltered(context.Background(), candidates, filter, 0)
	require.NoError(t, err)
	require.NotEmpty(t, first)
	spent := budget.Stats()
	require.NotZero(t, spent.Terms)
	for round := 0; round < 3; round++ {
		_, _, err = view.MatchTermsSetPrefixFiltered(context.Background(), candidates, filter, 5*(round+2))
		require.NoError(t, err)
		_, err = view.TopTimeHits(context.Background(), TopTimeRequest{Candidates: candidates, Filter: filter, Limit: 5})
		require.NoError(t, err)
	}
	require.Equal(t, spent, budget.Stats(), "evaluating the same filter again must not expand the pattern again")
}

// A TopFieldPlan with a filter must rank exactly the candidates a plain predicate leaves, with or without a ladder.
func TestTopFieldPlanFiltersMatchAPlainPredicate(t *testing.T) {
	const span = int64(1) << 40
	shape := topFieldShape{span: span, batches: 3, perBatch: 1200, missing: 9, upserts: 11, negatives: true}
	for _, mode := range []ladderMode{ladderNone, ladderAll, ladderMixed} {
		for _, storage := range []struct{ persisted, merged bool }{{false, false}, {true, false}, {true, true}} {
			t.Run(fmt.Sprintf("ladder=%s/persisted=%v/merged=%v", mode, storage.persisted, storage.merged), func(t *testing.T) {
				t.Parallel()
				rng := rand.New(rand.NewSource(17)) //nolint:gosec // deterministic test data
				batches := randomTopFieldBatches(rng, shape, mode)
				for b := range batches {
					for d := range batches[b] {
						bucket := fmt.Sprintf("b%02d", (b*shape.perBatch+d)%50)
						batches[b][d].Fields = append(batches[b][d].Fields, Field{Name: "bucket", Value: []byte(bucket), Index: true, Store: true})
					}
				}
				owner := newTopFieldFixture(t, storage.persisted, storage.merged, batches)
				view, err := owner.Acquire(context.Background())
				require.NoError(t, err)
				defer func() { require.NoError(t, view.Close()) }()
				all, matchErr := view.MatchTermsSet(context.Background(), topFieldCandidates(nil))
				require.NoError(t, matchErr)
				for _, tc := range postingFilterCases() {
					want := keepHits(t, view, all, tc.keep)
					for _, desc := range []bool{true, false} {
						for _, limit := range []int{1, 5, 40, 5000} {
							request := TopFieldRequest{Candidates: topFieldCandidates(nil), Filter: tc.filter, Field: "score", Limit: limit, Desc: desc}
							plan, planErr := view.NewTopFieldPlan(context.Background(), request)
							require.NoError(t, planErr)
							got, topErr := plan.Top(context.Background(), limit)
							require.NoError(t, topErr)
							ranked, sortErr := view.SortHits(context.Background(), want, SortRequest{Field: "score", Desc: desc, Limit: limit})
							require.NoError(t, sortErr)
							requireSameHits(t, ranked, got, fmt.Sprintf("%s desc=%v limit=%d", tc.name, desc, limit))
						}
					}
				}
			})
		}
	}
}

// Folding takes the tightest bounds of the ranges of one field, and the least term budget.
func TestIntersectRangesTakesTheTightestBounds(t *testing.T) {
	tight := intersectRanges(
		RangeRequest{Field: "f", Lower: []byte("a"), Upper: []byte("m"), IncludesLower: true, IncludesUpper: true, MaxTerms: 50},
		RangeRequest{Field: "f", Lower: []byte("c"), Upper: []byte("m"), IncludesLower: true, IncludesUpper: false, MaxTerms: 10})
	require.Equal(t, []byte("c"), tight.Lower)
	require.True(t, tight.IncludesLower)
	require.Equal(t, []byte("m"), tight.Upper)
	require.False(t, tight.IncludesUpper, "equal upper bounds are inclusive only when both are")
	require.Equal(t, uint64(10), tight.MaxTerms)
}

// Once a segment fills the limit and leaves candidates out, the prefix is complete: the segments after it must not be
// evaluated, or their candidates would spend the query's budget (here the candidate cap) for an answer already known.
func TestMatchTermsSetPrefixFilteredStopsAfterTheSegmentThatFillsTheLimit(t *testing.T) {
	const span = int64(24 * 60 * 60 * 1e9)
	rng := rand.New(rand.NewSource(17)) //nolint:gosec // deterministic test data
	batches := randomTopTimeBatches(rng, 3, 1000, span, true)
	for _, persisted := range []bool{false, true} {
		fixture := newTopTimeFixture(t, persisted, batches)
		view, err := fixture.owner.Acquire(context.Background())
		require.NoError(t, err)
		timeRange := &TimeRange{Lower: topTimeBase - 1, Upper: topTimeBase + span + 1, IncludesLower: true, IncludesUpper: true}
		request := topTimeRequestFor("ok", timeRange, 1, false).Candidates
		everything, err := view.MatchTermsSet(context.Background(), request)
		require.NoError(t, err)
		require.Greater(t, len(everything), 2)
		// Each of the three segments holds about a third of the candidates, so the cap admits the first and not the second.
		request.MaxCandidates = uint64(len(everything) / 2)
		_, _, err = view.MatchTermsSetPrefixFiltered(context.Background(), request, nil, 0)
		require.ErrorIs(t, err, ErrQueryLimit, "listing every candidate counts them all against the cap")
		hits, truncated, err := view.MatchTermsSetPrefixFiltered(context.Background(), request, nil, 1)
		require.NoError(t, err, "the first segment fills the limit, so the later segments are not counted")
		require.Len(t, hits, 1)
		require.True(t, truncated)
		require.NoError(t, view.Close())
	}
}
