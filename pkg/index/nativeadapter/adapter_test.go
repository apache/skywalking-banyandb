// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses
// this file to you under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License. You may
// obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.
package nativeadapter

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/numeric"
	segment "github.com/blugelabs/bluge_segment_api"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/native"
)

func TestStreamNumericEncodingParity(t *testing.T) {
	for _, value := range []float64{0, -1, 1, math.MinInt64, math.MaxInt64, 1.5} {
		got := numericPrefix(encoding.Float64ToSortableInt64(value), 0)
		want := numeric.MustNewPrefixCodedInt64(numeric.Float64ToInt64(value), 0)
		require.Equal(t, []byte(want), got)
		for shift := uint(0); shift <= 60; shift += 4 {
			require.Equal(t, []byte(numeric.MustNewPrefixCodedInt64(numeric.Float64ToInt64(value), shift)), numericPrefix(encoding.Float64ToSortableInt64(value), shift))
		}
	}
}

func TestEncodedNumericFieldMatchesLegacyOracle(t *testing.T) {
	source := index.NewIntField(index.FieldKey{IndexRuleID: 9}, -42)
	source.Store = true
	got, err := encodeField(source)
	require.NoError(t, err)
	want := bluge.NewNumericField(source.Key.Marshal(), source.GetFloat()).StoreValue().Sortable()
	want.Analyze(0)
	require.Equal(t, want.Value(), got.Value)
	expected := map[string]int{}
	want.EachTerm(func(term segment.FieldTerm) { expected[string(term.Term())]++ })
	actual := map[string]int{}
	for _, term := range got.Terms {
		actual[string(term.Value)] += int(term.Frequency)
	}
	require.Equal(t, expected, actual)
	require.True(t, got.Store)
	require.True(t, got.Sort)
}

type testLease struct{}

func (testLease) Validate() error { return nil }

func TestAdapterBatchReadbackContract(t *testing.T) {
	owner, err := native.NewOwner(native.OwnerOptions{Lease: testLease{}})
	require.NoError(t, err)
	defer owner.Close()
	adapter := &Adapter{Owner: owner}
	field := index.NewStringField(index.FieldKey{IndexRuleID: 7, SeriesID: 11, Analyzer: index.AnalyzerSimple}, "Mixed MIXED")
	field.Store = true
	require.NoError(t, adapter.Batch(context.Background(), index.Batch{Documents: []index.Document{{DocID: 42, Timestamp: -1, Fields: []index.Field{field}}}})) //nolint:lll
	view, err := adapter.Acquire(context.Background())
	require.NoError(t, err)
	defer view.Close()
	document, found, err := view.Lookup(context.Background(), convert.Uint64ToBytes(42))
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, convert.Uint64ToBytes(42), document.Identifier)
	require.Equal(t, int64(0), document.Timestamp)
	require.Len(t, document.Fields, 1)
	require.Equal(t, "\x00\x00\x00\x07", document.Fields[0].Name)
	hits, err := view.MatchTermsSet(context.Background(), native.TermSetRequest{Field: "_series_id", Terms: [][]byte{convert.Uint64ToBytes(11)}, MaxCandidates: 10})
	require.NoError(t, err)
	require.Len(t, hits, 1)
	mixedHits, err := view.MatchTermsSet(context.Background(), native.TermSetRequest{Field: "\x00\x00\x00\x07", Terms: [][]byte{[]byte("mixed")}, MaxCandidates: 10})
	require.NoError(t, err)
	require.Len(t, mixedHits, 1)
}

func TestEncodedAnalyzerEmptyTermsMatchesLegacy(t *testing.T) {
	field := index.NewStringField(index.FieldKey{IndexRuleID: 1, Analyzer: index.AnalyzerSimple}, "!!!")
	encoded, err := encodeField(field)
	require.NoError(t, err)
	require.NotNil(t, encoded.Terms)
	require.Empty(t, encoded.Terms)
}

func TestEncodedNumericAnalyzerOverrideMatchesLegacy(t *testing.T) {
	field := index.NewIntField(index.FieldKey{IndexRuleID: 2, Analyzer: index.AnalyzerKeyword}, 42)
	encoded, err := encodeField(field)
	require.NoError(t, err)
	require.NotEmpty(t, encoded.Terms)
	require.Equal(t, []byte(string(encoded.Value)), encoded.Terms[0].Value)
}

func TestAdapterTimestampSortCompatibility(t *testing.T) {
	owner, err := native.NewOwner(native.OwnerOptions{Lease: testLease{}})
	require.NoError(t, err)
	defer owner.Close()
	adapter := &Adapter{Owner: owner}
	field := index.NewStringField(index.FieldKey{IndexRuleID: 3, SeriesID: 1}, "ok")
	require.NoError(t, adapter.Batch(context.Background(), index.Batch{Documents: []index.Document{ //nolint:lll
		{DocID: 1, Timestamp: 200, Fields: []index.Field{field}},
		{DocID: 2, Timestamp: 100, Fields: []index.Field{field}},
	}}))
	view, err := adapter.Acquire(context.Background())
	require.NoError(t, err)
	defer view.Close()
	hits, err := view.MatchTermsSet(context.Background(), native.TermSetRequest{Field: field.Key.Marshal(), Terms: [][]byte{[]byte("ok")}, MaxCandidates: 10})
	require.NoError(t, err)
	sorted, err := view.SortHits(context.Background(), hits, native.SortRequest{Field: "_timestamp"})
	require.NoError(t, err)
	require.Equal(t, int64(100), sorted[0].Timestamp)
	require.Equal(t, convert.Uint64ToBytes(2), sorted[0].Identifier)
	require.Equal(t, int64(200), sorted[1].Timestamp)
}

func TestAdapterTimestampSortCursorPages(t *testing.T) {
	owner, err := native.NewOwner(native.OwnerOptions{Lease: testLease{}})
	require.NoError(t, err)
	defer owner.Close()
	adapter := &Adapter{Owner: owner}
	field := index.NewStringField(index.FieldKey{IndexRuleID: 4, SeriesID: 1}, "ok")
	require.NoError(t, adapter.Batch(context.Background(), index.Batch{Documents: []index.Document{{DocID: 1, Timestamp: 200, Fields: []index.Field{field}}, {DocID: 2, Timestamp: 100, Fields: []index.Field{field}}}})) //nolint:lll
	view, err := adapter.Acquire(context.Background())
	require.NoError(t, err)
	defer view.Close()
	cursor, err := view.NewSortCursor(context.Background(), native.SortCursorRequest{Selection: native.TermSetRequest{Field: field.Key.Marshal(), Terms: [][]byte{[]byte("ok")}, MaxCandidates: 10}, SortField: "_timestamp", PageSize: 1}) //nolint:lll
	require.NoError(t, err)
	defer cursor.Close()
	page, err := cursor.NextPage(context.Background())
	require.NoError(t, err)
	require.Len(t, page, 1)
	require.Equal(t, convert.Uint64ToBytes(2), page[0].Identifier)
	require.False(t, page[0].Missing)
	require.NotEmpty(t, page[0].SortValue)
	page, err = cursor.NextPage(context.Background())
	require.NoError(t, err)
	require.Len(t, page, 1)
	require.Equal(t, convert.Uint64ToBytes(1), page[0].Identifier)
}

func TestAdapterDurableReopenTimestampCompatibility(t *testing.T) {
	path := t.TempDir()
	owner, err := native.NewOwner(native.OwnerOptions{Lease: durableLease{}, Path: path})
	require.NoError(t, err)
	adapter := &Adapter{Owner: owner}
	field := index.NewStringField(index.FieldKey{IndexRuleID: 5, SeriesID: 1}, "ok")
	require.NoError(t, adapter.Batch(context.Background(), index.Batch{Documents: []index.Document{{DocID: 1, Timestamp: 200, Fields: []index.Field{field}}, {DocID: 2, Timestamp: 100, Fields: []index.Field{field}}}})) //nolint:lll
	require.NoError(t, owner.Close())
	legacyReader, legacyErr := bluge.OpenReader(bluge.DefaultConfig(path))
	require.NoError(t, legacyErr)
	defer legacyReader.Close()
	legacyQuery := bluge.NewDateRangeInclusiveQuery(time.Unix(0, 50), time.Unix(0, 150), true, true).SetField("_timestamp")
	legacyMatches, legacyErr := legacyReader.Search(context.Background(), bluge.NewAllMatches(legacyQuery))
	require.NoError(t, legacyErr)
	match, matchErr := legacyMatches.Next()
	require.NoError(t, matchErr)
	require.NotNil(t, match)
	next, nextErr := legacyMatches.Next()
	require.NoError(t, nextErr)
	require.Nil(t, next)
	reopenedOwner, err := native.NewOwner(native.OwnerOptions{Lease: durableLease{}, Path: path})
	require.NoError(t, err)
	defer reopenedOwner.Close()
	reopened, err := reopenedOwner.Acquire(context.Background())
	require.NoError(t, err)
	defer reopened.Close()
	hits, err := reopened.MatchTermsSet(context.Background(), native.TermSetRequest{Field: field.Key.Marshal(), Terms: [][]byte{[]byte("ok")}, MaxCandidates: 10})
	require.NoError(t, err)
	require.Len(t, hits, 2)
}

type durableLease struct{}

func (durableLease) Validate() error           { return nil }
func (durableLease) ValidatePath(string) error { return nil }

func TestAdapterBatchForwardsPersistentCallback(t *testing.T) {
	owner, err := native.NewOwner(native.OwnerOptions{Lease: testLease{}})
	require.NoError(t, err)
	defer owner.Close()
	adapter := &Adapter{Owner: owner}
	called := make(chan error, 1)
	err = adapter.Batch(context.Background(), index.Batch{PersistentCallback: func(callbackErr error) { called <- callbackErr }})
	require.ErrorIs(t, err, native.ErrPersistenceConfiguration)
	select {
	case callbackErr := <-called:
		require.ErrorIs(t, callbackErr, native.ErrPersistenceConfiguration)
	case <-time.After(time.Second):
		t.Fatal("callback not called")
	}
}
