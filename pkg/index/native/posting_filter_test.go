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

func bucketTerms(values ...string) *PostingFilter {
	terms := make([][]byte, len(values))
	for i := range values {
		terms[i] = []byte(values[i])
	}
	return FilterTerms(TermSetRequest{Field: "bucket", Terms: terms, Mode: MatchAnyTerm})
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
	return []postingFilterCase{
		{name: "one value", filter: bucketTerms("b07"), keep: in("b07")},
		{name: "any of values", filter: bucketTerms("b07", "b41"), keep: in("b07", "b41")},
		{
			name: "all of one term", keep: in("b07"),
			filter: FilterTerms(TermSetRequest{Field: "bucket", Terms: [][]byte{[]byte("b07")}, Mode: MatchAllTerms}),
		},
		{name: "and of an exact value and the status", keep: in("b07", "b41"), filter: AllOf(bucketTerms("b07", "b41"),
			FilterTerms(TermSetRequest{Field: "status", Terms: [][]byte{[]byte("ok")}, Mode: MatchAnyTerm}))},
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
		{
			name: "range", keep: inRange,
			filter: FilterRanges(RangeRequest{Field: "bucket", Lower: []byte("b10"), Upper: []byte("b20"), IncludesLower: true, MaxTerms: ^uint64(0)}),
		},
		{
			name: "or of a range and a value", keep: func(bucket string) bool { return inRange(bucket) || bucket == "b41" },
			filter: AnyOf(FilterRanges(RangeRequest{Field: "bucket", Lower: []byte("b10"), Upper: []byte("b20"), IncludesLower: true, MaxTerms: ^uint64(0)}), bucketTerms("b41")),
		},
		{name: "and of two ranges on one field", keep: inRange, filter: AllOf(
			FilterRanges(RangeRequest{Field: "bucket", Lower: []byte("b10"), IncludesLower: true, MaxTerms: ^uint64(0)}),
			FilterRanges(RangeRequest{Field: "bucket", Upper: []byte("b20"), MaxTerms: ^uint64(0)}))},
		{name: "and of ranges meeting at one value", keep: in("b15"), filter: AllOf(
			FilterRanges(RangeRequest{Field: "bucket", Lower: []byte("b15"), IncludesLower: true, MaxTerms: ^uint64(0)}),
			FilterRanges(RangeRequest{Field: "bucket", Upper: []byte("b15"), IncludesUpper: true, MaxTerms: ^uint64(0)}))},
		{name: "and of ranges that exclude each other", keep: func(string) bool { return false }, filter: AllOf(
			FilterRanges(RangeRequest{Field: "bucket", Lower: []byte("b30"), IncludesLower: true, MaxTerms: ^uint64(0)}),
			FilterRanges(RangeRequest{Field: "bucket", Upper: []byte("b20"), MaxTerms: ^uint64(0)}))},
		{name: "and of a range and a value on the same field", keep: in("b12"), filter: AllOf(
			FilterRanges(RangeRequest{Field: "bucket", Lower: []byte("b10"), Upper: []byte("b20"), IncludesLower: true, MaxTerms: ^uint64(0)}), bucketTerms("b12", "b41"))},
		{name: "match all", filter: MatchAll(), keep: func(string) bool { return true }},
		{name: "match none", filter: MatchNone(), keep: func(string) bool { return false }},
		{name: "no term", filter: FilterTerms(TermSetRequest{Field: "bucket"}), keep: func(string) bool { return false }},
		{name: "no match", filter: bucketTerms("absent"), keep: func(string) bool { return false }},
		{name: "empty and", filter: AllOf(), keep: func(string) bool { return true }},
		{name: "empty or", filter: AnyOf(), keep: func(string) bool { return false }},
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

func postingFilterBatches(rng *rand.Rand, span int64) [][]Document {
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
// listing, in the time-ordered top search and at hit level, while decoding only the documents it keeps.
func TestPostingFiltersMatchAPlainPredicate(t *testing.T) {
	const span = int64(24 * 60 * 60 * 1e9)
	rng := rand.New(rand.NewSource(11)) //nolint:gosec // deterministic test data
	for _, persisted := range []bool{false, true} {
		fixture := newTopTimeFixture(t, persisted, postingFilterBatches(rng, span))
		view, err := fixture.owner.Acquire(context.Background())
		require.NoError(t, err)
		timeRange := &TimeRange{Lower: topTimeBase - 1, Upper: topTimeBase + span + 1, IncludesLower: true, IncludesUpper: true}
		candidates := topTimeRequestFor("ok", timeRange, 1, false).Candidates
		all, matchErr := view.MatchTermsSet(context.Background(), candidates)
		require.NoError(t, matchErr)
		for _, tc := range postingFilterCases() {
			want := keepHits(t, view, all, tc.keep)
			label := fmt.Sprintf("persisted=%v %s", persisted, tc.name)
			byHit, filterErr := view.FilterByPosting(context.Background(), all, tc.filter)
			require.NoError(t, filterErr, label)
			require.Equal(t, hitKeys(want), hitKeys(byHit), label+" at hit level")
			for _, limit := range []int{0, 1, 5, len(want), len(want) + 10} {
				got, truncated, listErr := view.MatchTermsSetPrefixFiltered(context.Background(), candidates, tc.filter, limit)
				require.NoError(t, listErr, label)
				if limit == 0 || limit >= len(want) {
					require.Equal(t, hitKeys(want), hitKeys(got), fmt.Sprintf("%s limit=%d", label, limit))
					require.False(t, truncated, label)
					continue
				}
				require.Len(t, got, limit, label)
				require.True(t, truncated, label)
				require.Subset(t, hitKeys(want), hitKeys(got), label)
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
					requireSameHits(t, exhaustive, got, fmt.Sprintf("%s desc=%v limit=%d", label, desc, limit))
				}
			}
		}
		require.NoError(t, view.Close())
	}
}

// A TopFieldPlan with a filter must rank exactly the candidates a plain predicate leaves, with or without a ladder.
func TestTopFieldPlanFiltersMatchAPlainPredicate(t *testing.T) {
	const span = int64(1) << 40
	shape := topFieldShape{span: span, batches: 3, perBatch: 1200, missing: 9, upserts: 11, negatives: true}
	for _, mode := range []ladderMode{ladderNone, ladderAll, ladderMixed} {
		for _, storage := range []struct{ persisted, merged bool }{{false, false}, {true, false}, {true, true}} {
			t.Run(fmt.Sprintf("ladder=%s/persisted=%v/merged=%v", mode, storage.persisted, storage.merged), func(t *testing.T) {
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

// The ranges of a conjunction on one field fold into one range with the tightest bounds, so only the terms between them are
// expanded; ranges on different fields and the other children are left alone.
func TestMergeRangeChildren(t *testing.T) {
	ranged := func(field string, lower, upper string, includesLower, includesUpper bool) *PostingFilter {
		request := RangeRequest{Field: field, IncludesLower: includesLower, IncludesUpper: includesUpper, MaxTerms: 100}
		if lower != "" {
			request.Lower = []byte(lower)
		}
		if upper != "" {
			request.Upper = []byte(upper)
		}
		return FilterRanges(request)
	}
	terms := bucketTerms("b01")
	merged := mergeRangeChildren([]*PostingFilter{
		ranged("bucket", "b10", "", true, false), terms, ranged("bucket", "", "b20", false, false), ranged("other", "x", "", true, false),
	})
	require.Len(t, merged, 3, "the two bucket ranges fold into one, in the position of the first")
	require.Equal(t, filterRange, merged[0].kind)
	require.Equal(t, []byte("b10"), merged[0].rng.Lower)
	require.Equal(t, []byte("b20"), merged[0].rng.Upper)
	require.True(t, merged[0].rng.IncludesLower)
	require.False(t, merged[0].rng.IncludesUpper)
	require.Same(t, terms, merged[1])
	require.Equal(t, "other", merged[2].rng.Field)

	tight := intersectRanges(
		RangeRequest{Field: "f", Lower: []byte("a"), Upper: []byte("m"), IncludesLower: true, IncludesUpper: true, MaxTerms: 50},
		RangeRequest{Field: "f", Lower: []byte("c"), Upper: []byte("m"), IncludesLower: true, IncludesUpper: false, MaxTerms: 10})
	require.Equal(t, []byte("c"), tight.Lower)
	require.True(t, tight.IncludesLower)
	require.Equal(t, []byte("m"), tight.Upper)
	require.False(t, tight.IncludesUpper, "equal upper bounds are inclusive only when both are")
	require.Equal(t, uint64(10), tight.MaxTerms)
}
