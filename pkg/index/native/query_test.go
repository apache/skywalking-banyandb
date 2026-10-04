// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain a
// copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

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
			{Name: "status", Terms: []Term{{Value: []byte("alpha")}}, Index: true},
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

	any, err := view.MatchTermsSet(context.Background(), TermSetRequest{
		Field: "status", Terms: [][]byte{[]byte("alpha"), []byte("beta")},
		Scope: QueryScope{SeriesField: "series", SeriesID: []byte("s")},
	})
	require.NoError(t, err)
	require.Equal(t, []string{"d0", "d1", "d2"}, queryIDs(any))
	all, err := view.MatchTermsSet(context.Background(), TermSetRequest{Field: "status", Terms: [][]byte{[]byte("alpha"), []byte("beta")}, Mode: MatchAllTerms})
	require.NoError(t, err)
	require.Equal(t, []string{"d2"}, queryIDs(all))
	windowed, err := view.MatchTermsSet(context.Background(), TermSetRequest{
		Field: "status", Terms: [][]byte{[]byte("alpha")}, Scope: QueryScope{TimeRange: &TimeRange{Lower: 100, Upper: 300, IncludesLower: false, IncludesUpper: true}},
	})
	require.NoError(t, err)
	require.Equal(t, []string{"d2"}, queryIDs(windowed))

	present, err := view.MatchField(context.Background(), FieldRequest{Field: "status", MaxTerms: 2})
	require.NoError(t, err)
	require.Equal(t, []string{"d0", "d1", "d2"}, queryIDs(present))
	ranged, err := view.MatchRange(context.Background(), RangeRequest{
		Field: "latency", Lower: encoded(10), Upper: encoded(30),
		IncludesLower: true, IncludesUpper: false, MaxTerms: 3,
	})
	require.NoError(t, err)
	require.Equal(t, []string{"d0", "d1"}, queryIDs(ranged))

	ordered, err := view.SortHits(context.Background(), any, SortRequest{Field: "sort", Desc: true, Limit: 2})
	require.NoError(t, err)
	require.Equal(t, []string{"d2", "d0"}, queryIDs(ordered))
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
