// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses
// this file to You under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package native

import (
	"context"
	"sort"
	"testing"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
)

// BenchmarkNativeMatchTerms compares the neutral native operation with the
// existing oracle operation over the same four-document fixture. Setup,
// persistence, reopening, and parity checks are outside the timed regions.
// This is operation-level evidence only; it does not claim a production caller
// cutover or end-to-end query performance.
//
//nolint:gocyclo // fixture setup and parity checks intentionally stay beside the benchmark.
func BenchmarkNativeMatchTerms(b *testing.B) {
	ctx := context.Background()
	seriesID := common.SeriesID(7)
	statusKey := index.FieldKey{TagName: "status", SeriesID: seriesID}
	valueKey := index.FieldKey{TagName: "value", SeriesID: seriesID}
	seriesKey := index.FieldKey{TagName: "series", SeriesID: seriesID}
	nativePath := b.TempDir()
	oraclePath := b.TempDir()

	oracle, err := inverted.NewStore(inverted.StoreOpts{Path: oraclePath})
	if err != nil {
		b.Fatal(err)
	}
	oracleDocuments := []index.Document{
		{DocID: 10, Timestamp: 100, Fields: []index.Field{index.NewStringField(statusKey, "ok"), index.NewStringField(seriesKey, "series-a"), index.NewIntField(valueKey, 42)}},
		{DocID: 11, Timestamp: 300, Fields: []index.Field{
			index.NewStringField(statusKey, "bad"), index.NewStringField(seriesKey, "series-a"), index.NewIntField(valueKey, 42),
		}},
		{DocID: 12, Timestamp: 200, Fields: []index.Field{index.NewStringField(statusKey, "ok"), index.NewStringField(seriesKey, "series-a"), index.NewIntField(valueKey, 42)}},
		{DocID: 13, Timestamp: 400, Fields: []index.Field{index.NewStringField(statusKey, "ok"), index.NewStringField(seriesKey, "series-a"), index.NewIntField(valueKey, 42)}},
	}
	if batchErr := oracle.Batch(index.Batch{Documents: oracleDocuments}); batchErr != nil {
		b.Fatal(batchErr)
	}
	if deleteErr := oracle.Delete([][]byte{convert.Uint64ToBytes(13)}); deleteErr != nil {
		b.Fatal(deleteErr)
	}
	if closeErr := oracle.Close(); closeErr != nil {
		b.Fatal(closeErr)
	}
	oracle, err = inverted.NewStore(inverted.StoreOpts{Path: oraclePath})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = oracle.Close() }()

	owner, err := NewOwner(OwnerOptions{Lease: testLease{}, Path: nativePath})
	if err != nil {
		b.Fatal(err)
	}
	nativeDocuments := make([]Document, 0, len(oracleDocuments))
	for _, document := range oracleDocuments {
		nativeDocuments = append(nativeDocuments, Document{
			Identifier: convert.Uint64ToBytes(document.DocID), Timestamp: document.Timestamp,
			Fields: []Field{
				{Name: "status", Terms: []Term{{Value: []byte(map[uint64]string{10: "ok", 11: "bad", 12: "ok", 13: "ok"}[document.DocID])}}, Index: true},
				{Name: "series", Terms: []Term{{Value: convert.Uint64ToBytes(uint64(seriesID))}}, Index: true},
				{Name: "value", Terms: []Term{{Value: []byte("42")}}, Index: true},
			},
		})
	}
	waitPersist := func(batch Batch) {
		persisted := make(chan error, 1)
		batch.PersistentCallback = func(callbackErr error) { persisted <- callbackErr }
		if batchErr := owner.Batch(ctx, batch); batchErr != nil {
			b.Fatal(batchErr)
		}
		if persistErr := <-persisted; persistErr != nil {
			b.Fatal(persistErr)
		}
	}
	waitPersist(Batch{Documents: nativeDocuments})
	waitPersist(Batch{Deletes: [][]byte{convert.Uint64ToBytes(13)}})
	if closeErr := owner.Close(); closeErr != nil {
		b.Fatal(closeErr)
	}
	owner, err = NewOwner(OwnerOptions{Lease: testLease{}, Path: nativePath})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = owner.Close() }()

	nativeRequest := MatchRequest{
		Field: "status", Term: []byte("ok"), SeriesField: "series", SeriesID: convert.Uint64ToBytes(uint64(seriesID)),
		TimeRange: &TimeRange{Lower: 100, Upper: 300, IncludesLower: false, IncludesUpper: true},
	}
	oracleField := index.NewStringField(statusKey, "ok")
	lowerField := index.NewIntField(statusKey, 100)
	upperField := index.NewIntField(statusKey, 300)
	oracleField.Key.TimeRange = &index.RangeOpts{
		Lower: lowerField.GetTerm(), Upper: upperField.GetTerm(),
		IncludesLower: false, IncludesUpper: true,
	}
	nativeView, err := owner.Acquire(ctx)
	if err != nil {
		b.Fatal(err)
	}
	nativeResult, err := nativeView.MatchTerms(ctx, nativeRequest)
	numericResult, numericErr := nativeView.MatchTerms(ctx, MatchRequest{
		Field: "value", Term: []byte("42"), SeriesField: "series", SeriesID: convert.Uint64ToBytes(uint64(seriesID)),
	})
	_ = nativeView.Close()
	if err != nil {
		b.Fatal(err)
	}
	if numericErr != nil || len(numericResult.Identifiers) != 3 {
		b.Fatalf("native numeric fixture mismatch: result=%+v err=%v", numericResult, numericErr)
	}
	oracleList, oracleTimestamps, err := oracle.MatchTerms(oracleField)
	if err != nil {
		b.Fatal(err)
	}
	numericOracleList, _, numericOracleErr := oracle.MatchTerms(index.NewIntField(valueKey, 42))
	if numericOracleErr != nil || numericOracleList.Len() != 3 {
		b.Fatalf("oracle numeric fixture mismatch: list=%v err=%v", numericOracleList, numericOracleErr)
	}
	nativeIDs := make([]uint64, len(nativeResult.Identifiers))
	for i, identifier := range nativeResult.Identifiers {
		nativeIDs[i] = convert.BytesToUint64(identifier)
	}
	sort.Slice(nativeIDs, func(i, j int) bool { return nativeIDs[i] < nativeIDs[j] })
	oracleIDs := oracleList.ToSlice()
	sort.Slice(oracleIDs, func(i, j int) bool { return oracleIDs[i] < oracleIDs[j] })
	if len(nativeIDs) != len(oracleIDs) || len(nativeIDs) != 1 || nativeIDs[0] != 12 || oracleIDs[0] != 12 {
		b.Fatalf("fixture identifier parity mismatch: native=%v oracle=%v", nativeIDs, oracleIDs)
	}
	if len(nativeResult.Timestamps) != 1 || nativeResult.Timestamps[0] != 200 || oracleTimestamps.Len() != 1 || oracleTimestamps.ToSlice()[0] != 200 {
		b.Fatalf("fixture timestamp parity mismatch: native=%v oracle=%v", nativeResult.Timestamps, oracleTimestamps.ToSlice())
	}

	b.Run("native", func(b *testing.B) {
		for range b.N {
			view, acquireErr := owner.Acquire(ctx)
			if acquireErr != nil {
				b.Fatal(acquireErr)
			}
			result, matchErr := view.MatchTerms(ctx, nativeRequest)
			_ = view.Close()
			if matchErr != nil || len(result.Identifiers) != 1 || result.Timestamps[0] != 200 {
				b.Fatalf("native result mismatch: result=%+v err=%v", result, matchErr)
			}
		}
	})
	b.Run("oracle", func(b *testing.B) {
		for range b.N {
			list, timestamps, matchErr := oracle.MatchTerms(oracleField)
			if matchErr != nil || list.Len() != 1 || timestamps.Len() != 1 || timestamps.ToSlice()[0] != 200 {
				b.Fatalf("oracle result mismatch: list=%v timestamps=%v err=%v", list, timestamps, matchErr)
			}
		}
	})
}
