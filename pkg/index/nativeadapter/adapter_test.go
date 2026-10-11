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
	"encoding/hex"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/native"
)

// The expected bytes below are the legacy writer's numeric prefix coding,
// captured once from it and pinned, so a change to either encoding fails here.
func TestStreamNumericEncodingParity(t *testing.T) {
	for _, testCase := range []struct {
		encoded string
		value   float64
		shift   uint
	}{
		{"2001000000000000000000", 0, 0},
		{"24080000000000000000", 0, 4},
		{"5c08", 0, 60},
		{"200040077f7f7f7f7f7f7f", -1, 0},
		{"2404003f7f7f7f7f7f7f", -1, 4},
		{"5c04", -1, 60},
		{"20013f7800000000000000", 1, 0},
		{"240b7f40000000000000", 1, 4},
		{"5c0b", 1, 60},
		{"20003c0f7f7f7f7f7f7f7f", math.MinInt64, 0},
		{"2403607f7f7f7f7f7f7f", math.MinInt64, 4},
		{"5c03", math.MinInt64, 60},
		{"2001437000000000000000", math.MaxInt64, 0},
		{"240c1f00000000000000", math.MaxInt64, 4},
		{"5c0c", math.MaxInt64, 60},
		{"20013f7c00000000000000", 1.5, 0},
		{"240b7f60000000000000", 1.5, 4},
		{"5c0b", 1.5, 60},
	} {
		require.Equal(t, testCase.encoded, hex.EncodeToString(numericPrefix(encoding.Float64ToSortableInt64(testCase.value), testCase.shift)),
			"value %v shift %d", testCase.value, testCase.shift)
	}
}

// The expected value and terms are the legacy writer's numeric field for the
// same source, captured once and pinned: the shift-0 prefix as the value, one
// prefix term per 4-bit shift, and the decimal form of the field's float.
func TestEncodedNumericFieldMatchesLegacyEncoding(t *testing.T) {
	source := index.NewIntField(index.FieldKey{IndexRuleID: 9}, -42)
	source.Store = true
	got, err := encodeField(source)
	require.NoError(t, err)
	require.Equal(t, "20007f7f7f7f7f7f7f7f56", hex.EncodeToString(got.Value))
	expected := map[string]int{"-0." + strings.Repeat("0", 321) + "203": 1}
	for _, term := range []string{
		"20007f7f7f7f7f7f7f7f56", "24077f7f7f7f7f7f7f7d", "283f7f7f7f7f7f7f7f", "2c037f7f7f7f7f7f7f",
		"301f7f7f7f7f7f7f", "34017f7f7f7f7f7f", "380f7f7f7f7f7f", "3c007f7f7f7f7f", "40077f7f7f7f",
		"443f7f7f7f", "48037f7f7f", "4c1f7f7f", "50017f7f", "540f7f", "58007f", "5c07",
	} {
		decoded, decodeErr := hex.DecodeString(term)
		require.NoError(t, decodeErr)
		expected[string(decoded)]++
	}
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
