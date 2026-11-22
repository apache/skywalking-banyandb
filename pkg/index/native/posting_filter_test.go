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

func postingFilterBatches(rng *rand.Rand, span int64) [][]Document {
	out := make([][]Document, 3)
	id := 0
	for b := range out {
		for i := 0; i < 1500; i++ {
			doc := topTimeDocument(id, topTimeBase+rng.Int63n(span), "ok")
			doc.Fields = append(doc.Fields, Field{Name: "bucket", Value: []byte(fmt.Sprintf("b%02d", id%50)), Index: true})
			out[b] = append(out[b], doc)
			id++
		}
	}
	return out
}

func hitKeys(hits []QueryHit) []string {
	keys := make([]string, len(hits))
	for i := range hits {
		keys[i] = fmt.Sprintf("%d/%d", hits[i].Segment, hits[i].DocumentNumber)
	}
	sort.Strings(keys)
	return keys
}

// Filters intersect with the candidates at posting level, so the listing and the top search must return exactly what the
// candidates narrowed by FilterTermsSet afterwards would, while decoding only the documents they keep.
func TestPostingFiltersMatchFilteringAfterwards(t *testing.T) {
	const span = int64(24 * 60 * 60 * 1e9)
	rng := rand.New(rand.NewSource(11)) //nolint:gosec // deterministic test data
	for _, persisted := range []bool{false, true} {
		fixture := newTopTimeFixture(t, persisted, postingFilterBatches(rng, span))
		view, err := fixture.owner.Acquire(context.Background())
		require.NoError(t, err)
		timeRange := &TimeRange{Lower: topTimeBase - 1, Upper: topTimeBase + span + 1, IncludesLower: true, IncludesUpper: true}
		candidates := topTimeRequestFor("ok", timeRange, 1, false).Candidates
		for name, filters := range map[string][]TermSetRequest{
			"one value":     {{Field: "bucket", Terms: [][]byte{[]byte("b07")}, Mode: MatchAnyTerm}},
			"any of values": {{Field: "bucket", Terms: [][]byte{[]byte("b07"), []byte("b41")}, Mode: MatchAnyTerm}},
			"two filters": {
				{Field: "bucket", Terms: [][]byte{[]byte("b07"), []byte("b41")}, Mode: MatchAnyTerm},
				{Field: "status", Terms: [][]byte{[]byte("ok")}, Mode: MatchAnyTerm},
			},
			"no match": {{Field: "bucket", Terms: [][]byte{[]byte("absent")}, Mode: MatchAnyTerm}},
		} {
			all, matchErr := view.MatchTermsSet(context.Background(), candidates)
			require.NoError(t, matchErr)
			want := all
			for _, filter := range filters {
				want, matchErr = view.FilterTermsSet(context.Background(), want, filter)
				require.NoError(t, matchErr)
			}
			for _, limit := range []int{0, 1, 5, len(want), len(want) + 10} {
				got, truncated, listErr := view.MatchTermsSetPrefixFiltered(context.Background(), candidates, filters, limit)
				require.NoError(t, listErr)
				label := fmt.Sprintf("persisted=%v %s limit=%d", persisted, name, limit)
				if limit == 0 || limit >= len(want) {
					require.Equal(t, hitKeys(want), hitKeys(got), label)
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
					request.Filters = filters
					got, topErr := view.TopTimeHits(context.Background(), request)
					require.NoError(t, topErr)
					exhaustive := append([]QueryHit(nil), want...)
					sort.SliceStable(exhaustive, func(i, j int) bool { return topTimeLess(exhaustive[i], exhaustive[j], desc) })
					if len(exhaustive) > limit {
						exhaustive = exhaustive[:limit]
					}
					requireSameHits(t, exhaustive, got, fmt.Sprintf("persisted=%v %s desc=%v limit=%d", persisted, name, desc, limit))
				}
			}
		}
		require.NoError(t, view.Close())
	}
}

// A TopFieldPlan with filters must rank exactly the candidates that FilterTermsSet would leave, with or without a ladder.
func TestTopFieldPlanFiltersMatchFilteringAfterwards(t *testing.T) {
	const span = int64(1) << 40
	shape := topFieldShape{span: span, batches: 3, perBatch: 1200, missing: 9, upserts: 11, negatives: true}
	for _, mode := range []ladderMode{ladderNone, ladderAll, ladderMixed} {
		for _, storage := range []struct{ persisted, merged bool }{{false, false}, {true, false}, {true, true}} {
			t.Run(fmt.Sprintf("ladder=%s/persisted=%v/merged=%v", mode, storage.persisted, storage.merged), func(t *testing.T) {
				rng := rand.New(rand.NewSource(17)) //nolint:gosec // deterministic test data
				batches := randomTopFieldBatches(rng, shape, mode)
				for b := range batches {
					for d := range batches[b] {
						bucket := fmt.Sprintf("b%02d", (b*shape.perBatch+d)%40)
						batches[b][d].Fields = append(batches[b][d].Fields, Field{Name: "bucket", Value: []byte(bucket), Index: true})
					}
				}
				owner := newTopFieldFixture(t, storage.persisted, storage.merged, batches)
				view, err := owner.Acquire(context.Background())
				require.NoError(t, err)
				defer func() { require.NoError(t, view.Close()) }()
				for name, filters := range map[string][]TermSetRequest{
					"one bucket":  {{Field: "bucket", Terms: [][]byte{[]byte("b07")}, Mode: MatchAnyTerm}},
					"two buckets": {{Field: "bucket", Terms: [][]byte{[]byte("b07"), []byte("b33")}, Mode: MatchAnyTerm}},
					"no match":    {{Field: "bucket", Terms: [][]byte{[]byte("absent")}, Mode: MatchAnyTerm}},
				} {
					for _, desc := range []bool{true, false} {
						for _, limit := range []int{1, 5, 40, 5000} {
							request := TopFieldRequest{Candidates: topFieldCandidates(nil), Filters: filters, Field: "score", Limit: limit, Desc: desc}
							plan, planErr := view.NewTopFieldPlan(context.Background(), request)
							require.NoError(t, planErr)
							got, topErr := plan.Top(context.Background(), limit)
							require.NoError(t, topErr)
							all, matchErr := view.MatchTermsSet(context.Background(), request.Candidates)
							require.NoError(t, matchErr)
							for _, filter := range filters {
								all, matchErr = view.FilterTermsSet(context.Background(), all, filter)
								require.NoError(t, matchErr)
							}
							want, sortErr := view.SortHits(context.Background(), all, SortRequest{Field: "score", Desc: desc, Limit: limit})
							require.NoError(t, sortErr)
							requireSameHits(t, want, got, fmt.Sprintf("%s desc=%v limit=%d", name, desc, limit))
						}
					}
				}
			})
		}
	}
}
