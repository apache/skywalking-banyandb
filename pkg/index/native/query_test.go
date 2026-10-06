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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/index/internal/nativeice"
)

func TestNativeQueryTermSetsPresenceRangeAndSort(t *testing.T) {
	owner, err := NewOwner(OwnerOptions{Lease: testLease{}})
	require.NoError(t, err)
	defer func() { require.NoError(t, owner.Close()) }()
	encoded := nativeice.EncodePrefixCodedInt64
	require.NoError(t, owner.Batch(context.Background(), Batch{Documents: []Document{
		{Identifier: []byte("d0"), Timestamp: 100, Fields: []Field{
			{Name: "status", Value: []byte("alpha"), Terms: []Term{{Value: []byte("alpha")}}, Store: true, Index: true},
			{Name: "series", Value: []byte("s"), Index: true},
			{Name: "latency", Terms: []Term{{Value: encoded(10)}}, Index: true},
			{Name: "sort", Value: []byte("b"), Sort: true},
		}},
		{Identifier: []byte("d1"), Timestamp: 200, Fields: []Field{
			{Name: "status", Terms: []Term{{Value: []byte("beta")}}, Index: true},
			{Name: "series", Value: []byte("s"), Index: true},
			{Name: "latency", Terms: []Term{{Value: encoded(20)}}, Index: true},
		}},
		{Identifier: []byte("d2"), Timestamp: 300, Fields: []Field{
			{Name: "status", Terms: []Term{{Value: []byte("alpha")}, {Value: []byte("beta")}}, Index: true},
			{Name: "series", Value: []byte("s"), Index: true},
			{Name: "latency", Terms: []Term{{Value: encoded(30)}}, Index: true},
			{Name: "sort", Value: []byte("c"), Sort: true},
		}},
		{Identifier: []byte("d3"), Timestamp: 400, Fields: []Field{
			{Name: "status", Terms: []Term{{Value: []byte("alpha")}}, Index: true},
			{Name: "series", Value: []byte("other"), Index: true},
		}},
	}}))
	require.NoError(t, owner.Batch(context.Background(), Batch{Deletes: [][]byte{[]byte("d3")}}))
	view, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()

	present, err := view.MatchTermsSet(context.Background(), TermSetRequest{
		Field: "status", Terms: [][]byte{[]byte("alpha"), []byte("beta")},
		Scope: QueryScope{SeriesField: "series", SeriesID: []byte("s")},
	})
	require.NoError(t, err)
	require.Equal(t, []string{"d0", "d1", "d2"}, queryIDs(present))
	all, err := view.MatchTermsSet(context.Background(), TermSetRequest{Field: "status", Terms: [][]byte{[]byte("alpha"), []byte("beta")}, Mode: MatchAllTerms})
	require.NoError(t, err)
	require.Equal(t, []string{"d2"}, queryIDs(all))
	windowed, err := view.MatchTermsSet(context.Background(), TermSetRequest{
		Field: "status", Terms: [][]byte{[]byte("alpha")}, Scope: QueryScope{TimeRange: &TimeRange{Lower: 100, Upper: 300, IncludesLower: false, IncludesUpper: true}},
	})
	require.NoError(t, err)
	require.Equal(t, []string{"d2"}, queryIDs(windowed))

	fieldPresent, err := view.MatchField(context.Background(), FieldRequest{Field: "status", MaxTerms: 2})
	require.NoError(t, err)
	require.Equal(t, []string{"d0", "d1", "d2"}, queryIDs(fieldPresent))
	ranged, err := view.MatchRange(context.Background(), RangeRequest{
		Field: "latency", Lower: encoded(10), Upper: encoded(30),
		IncludesLower: true, IncludesUpper: false, MaxTerms: 3,
	})
	require.NoError(t, err)
	require.Equal(t, []string{"d0", "d1"}, queryIDs(ranged))

	ordered, err := view.SortHits(context.Background(), present, SortRequest{Field: "sort", Desc: true, Limit: 2})
	require.NoError(t, err)
	require.Equal(t, []string{"d2", "d0"}, queryIDs(ordered))
	projected, err := view.ProjectHit(context.Background(), present[0], "status")
	require.NoError(t, err)
	require.Equal(t, []byte("d0"), projected.Identifier)
	require.Equal(t, int64(100), projected.Timestamp)
	require.Equal(t, [][]byte{[]byte("alpha")}, projected.Fields["status"])
	projected.Fields["status"][0][0] = 'x'
	projected.Identifier[0] = 'x'
	docValue, missing, err := view.ProjectSortValue(context.Background(), present[0], "sort")
	require.NoError(t, err)
	require.False(t, missing)
	require.Equal(t, []byte("b"), docValue)
	docValue[0] = 'x'
	projectedAgain, err := view.ProjectHit(context.Background(), present[0], "status")
	require.NoError(t, err)
	require.Equal(t, []byte("d0"), projectedAgain.Identifier)
	require.Equal(t, [][]byte{[]byte("alpha")}, projectedAgain.Fields["status"])
	docValueAgain, missing, err := view.ProjectSortValue(context.Background(), present[0], "sort")
	require.NoError(t, err)
	require.False(t, missing)
	require.Equal(t, []byte("b"), docValueAgain)
}

func TestNativeQueryEmptyTermRangeAndCancellation(t *testing.T) {
	owner, err := NewOwner(OwnerOptions{Lease: testLease{}})
	require.NoError(t, err)
	defer func() { require.NoError(t, owner.Close()) }()
	require.NoError(t, owner.Batch(context.Background(), Batch{Documents: []Document{
		{Identifier: []byte("empty"), Fields: []Field{{Name: "kw", Terms: []Term{{Value: []byte{}}}, Index: true}}},
	}}))
	view, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()
	present, err := view.MatchRange(context.Background(), RangeRequest{Field: "kw", Lower: []byte{}, Upper: []byte{}, IncludesLower: true, IncludesUpper: true, MaxTerms: 1})
	require.NoError(t, err)
	require.Equal(t, []string{"empty"}, queryIDs(present))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = view.MatchField(ctx, FieldRequest{Field: "kw", MaxTerms: 1})
	require.ErrorIs(t, err, context.Canceled)
}

func TestNativeSortCursorKeysetPagesAndMissingValues(t *testing.T) {
	owner, err := NewOwner(OwnerOptions{Lease: testLease{}})
	require.NoError(t, err)
	defer func() { require.NoError(t, owner.Close()) }()
	require.NoError(t, owner.Batch(context.Background(), Batch{Documents: []Document{
		{Identifier: []byte("d0"), Timestamp: 10, Fields: []Field{{Name: "sort", Value: []byte("b"), Sort: true}}},
		{Identifier: []byte("d1"), Timestamp: 20, Fields: []Field{{Name: "sort", Value: []byte("a"), Sort: true}}},
		{Identifier: []byte("d2"), Timestamp: 30, Fields: []Field{{Name: "sort", Value: []byte("b"), Sort: true}}},
		{Identifier: []byte("d3"), Timestamp: 40},
		{Identifier: []byte("d4"), Timestamp: 50, Fields: []Field{{Name: "sort", Value: []byte{}, Sort: true}}},
		{Identifier: []byte("d5"), Timestamp: 60, Fields: []Field{
			{Name: "sort", Value: []byte("z"), Sort: true},
			{Name: "sort", Value: []byte("aa"), Sort: true},
		}},
	}}))
	view, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()

	cursor, err := view.NewSortCursor(context.Background(), SortCursorRequest{SortField: "sort", PageSize: 2})
	require.NoError(t, err)
	defer func() { require.NoError(t, cursor.Close()) }()
	page, err := cursor.NextPage(context.Background())
	require.NoError(t, err)
	require.Equal(t, []string{"d4", "d1"}, sortedIDs(page))
	page, err = cursor.NextPage(context.Background())
	require.NoError(t, err)
	require.Equal(t, []string{"d5", "d0"}, sortedIDs(page))
	page, err = cursor.NextPage(context.Background())
	require.NoError(t, err)
	require.Equal(t, []string{"d2", "d3"}, sortedIDs(page))
	page, err = cursor.NextPage(context.Background())
	require.NoError(t, err)
	require.Empty(t, page)

	desc, err := view.NewSortCursor(context.Background(), SortCursorRequest{SortField: "sort", Desc: true, PageSize: 4})
	require.NoError(t, err)
	defer func() { require.NoError(t, desc.Close()) }()
	page, err = desc.NextPage(context.Background())
	require.NoError(t, err)
	require.Equal(t, []string{"d0", "d2", "d5", "d1"}, sortedIDs(page))
}

func TestNativeSortCursorPinnedViewAndCancellation(t *testing.T) {
	owner, err := NewOwner(OwnerOptions{Lease: testLease{}})
	require.NoError(t, err)
	defer func() { require.NoError(t, owner.Close()) }()
	require.NoError(t, owner.Batch(context.Background(), Batch{Documents: []Document{
		{Identifier: []byte("old"), Fields: []Field{{Name: "sort", Value: []byte("a"), Sort: true}}},
	}}))
	view, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()
	cursor, err := view.NewSortCursor(context.Background(), SortCursorRequest{SortField: "sort", PageSize: 1})
	require.NoError(t, err)
	defer func() { require.NoError(t, cursor.Close()) }()
	require.NoError(t, owner.Batch(context.Background(), Batch{Documents: []Document{
		{Identifier: []byte("new"), Fields: []Field{{Name: "sort", Value: []byte("b"), Sort: true}}},
	}}))
	page, err := cursor.NextPage(context.Background())
	require.NoError(t, err)
	require.Equal(t, []string{"old"}, sortedIDs(page))
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = cursor.NextPage(canceled)
	require.ErrorIs(t, err, context.Canceled)
}

func queryIDs(hits []QueryHit) []string {
	result := make([]string, len(hits))
	for i := range hits {
		result[i] = string(hits[i].Identifier)
	}
	return result
}

func sortedIDs(hits []SortedHit) []string {
	result := make([]string, len(hits))
	for i := range hits {
		result[i] = string(hits[i].Identifier)
	}
	return result
}

func TestNativePostingLevelConjunctionMatchesDecodedIntersection(t *testing.T) {
	owner, err := NewOwner(OwnerOptions{Lease: testLease{}})
	require.NoError(t, err)
	defer func() { require.NoError(t, owner.Close()) }()
	encoded := nativeice.EncodePrefixCodedInt64
	// Two batches so the conjunction has to hold across segments.
	for batch, ids := range [][]string{{"a0", "a1", "b0"}, {"a2", "b1", "b2"}} {
		documents := make([]Document, 0, len(ids))
		for index, id := range ids {
			documents = append(documents, Document{Identifier: []byte(id), Fields: []Field{
				{Name: "group", Value: []byte(id[:1]), Index: true},
				{Name: "kind", Value: []byte("k"), Index: true},
				{Name: "rank", Terms: []Term{{Value: encoded(int64(batch*10 + index))}}, Index: true},
			}})
		}
		require.NoError(t, owner.Batch(context.Background(), Batch{Documents: documents}))
	}
	require.NoError(t, owner.Batch(context.Background(), Batch{Deletes: [][]byte{[]byte("a1")}}))
	view, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()
	ctx := context.Background()
	identifiers := func(hits []QueryHit) []string {
		result := make([]string, 0, len(hits))
		for _, hit := range hits {
			result = append(result, string(hit.Identifier))
		}
		return result
	}
	groupA := TermSetRequest{Field: "group", Terms: [][]byte{[]byte("a")}, Mode: MatchAnyTerm}
	kind := TermSetRequest{Field: "kind", Terms: [][]byte{[]byte("k")}, Mode: MatchAnyTerm}
	pinned := TermSetRequest{Field: "_id", Terms: [][]byte{[]byte("a2"), []byte("b1")}, Mode: MatchAnyTerm}

	all, err := view.MatchAllTermSets(ctx, []TermSetRequest{kind, groupA, pinned})
	require.NoError(t, err)
	require.Equal(t, []string{"a2"}, identifiers(all))

	universe, err := view.MatchTermsSet(ctx, kind)
	require.NoError(t, err)
	require.Len(t, universe, 5, "the deleted a1 must not appear")
	filtered, err := view.FilterTermsSet(ctx, universe, groupA)
	require.NoError(t, err)
	require.Equal(t, []string{"a0", "a2"}, identifiers(filtered))

	ranged, err := view.FilterRange(ctx, universe, RangeRequest{Field: "rank", MaxTerms: ^uint64(0), Lower: encoded(2), IncludesLower: true})
	require.NoError(t, err)
	require.Equal(t, []string{"b0", "a2", "b1", "b2"}, identifiers(ranged))

	_, err = view.FilterTermsSet(ctx, universe, TermSetRequest{Field: "group", Terms: [][]byte{[]byte("a")}, Scope: QueryScope{TimeRange: &TimeRange{}}})
	require.ErrorIs(t, err, ErrInvalidQuery)
	_, err = view.FilterTermsSet(ctx, []QueryHit{{Segment: 99}}, groupA)
	require.ErrorIs(t, err, ErrInvalidQuery)
}
