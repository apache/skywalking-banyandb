// Licensed to the Apache Software Foundation (ASF) under one or more contributor
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

package nativeadapter

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
)

// BenchmarkNativeMatchField compares indexed-posting presence with a legacy
// bounded all-term range over one legacy-created, closed corpus copied to the
// native path. The broad range is the corrected presence oracle; the legacy
// empty-range MatchField behavior is intentionally not used.
func BenchmarkNativeMatchField(b *testing.B) {
	store, oracle, fieldKey, cleanup := q3Fixture(b)
	defer cleanup()
	oracleRange := index.NewBytesRangeOpts(nil, nil, true, true)
	search := func() ([]uint64, []uint64) {
		searcher, err := store.NewSearcher(context.Background())
		if err != nil {
			b.Fatal(err)
		}
		ids, timestamps, err := searcher.MatchField(fieldKey)
		closeErr := searcher.Close()
		if err != nil {
			b.Fatal(err)
		}
		if closeErr != nil {
			b.Fatal(closeErr)
		}
		return ids.ToSlice(), timestamps.ToSlice()
	}
	wantIDs, wantTimestamps, err := oracle.Range(fieldKey, oracleRange)
	if err != nil {
		b.Fatal(err)
	}
	gotIDs, gotTimestamps := search()
	assertParity(b, wantIDs.ToSlice(), wantTimestamps.ToSlice(), gotIDs, gotTimestamps, []uint64{10, 11, 12}, []uint64{100, 200, 300})
	b.Run("native", func(b *testing.B) {
		for range b.N {
			ids, timestamps := search()
			if len(ids) != 3 || len(timestamps) != 3 {
				b.Fatalf("native presence mismatch: ids=%v timestamps=%v", ids, timestamps)
			}
		}
	})
	b.Run("oracle", func(b *testing.B) {
		for range b.N {
			ids, timestamps, err := oracle.Range(fieldKey, oracleRange)
			if err != nil || len(ids.ToSlice()) != 3 || len(timestamps.ToSlice()) != 3 {
				b.Fatalf("oracle presence mismatch: ids=%v timestamps=%v err=%v", ids, timestamps, err)
			}
		}
	})
}

// BenchmarkNativeRange compares numeric range traversal over the same copied
// legacy corpus. Setup, copying, reopening, and parity checks are untimed.
func BenchmarkNativeRange(b *testing.B) {
	store, oracle, fieldKey, cleanup := q3Fixture(b)
	defer cleanup()
	fieldKey.TagName = "latency"
	oracleRange := index.NewIntRangeOpts(10, 30, true, false)
	search := func() ([]uint64, []uint64) {
		searcher, err := store.NewSearcher(context.Background())
		if err != nil {
			b.Fatal(err)
		}
		ids, timestamps, err := searcher.Range(fieldKey, oracleRange)
		closeErr := searcher.Close()
		if err != nil {
			b.Fatal(err)
		}
		if closeErr != nil {
			b.Fatal(closeErr)
		}
		return ids.ToSlice(), timestamps.ToSlice()
	}
	wantIDs, wantTimestamps, err := oracle.Range(fieldKey, oracleRange)
	if err != nil {
		b.Fatal(err)
	}
	gotIDs, gotTimestamps := search()
	assertParity(b, wantIDs.ToSlice(), wantTimestamps.ToSlice(), gotIDs, gotTimestamps, []uint64{10, 11}, []uint64{100, 200})
	b.Run("native", func(b *testing.B) {
		for range b.N {
			ids, timestamps := search()
			if len(ids) != 2 || len(timestamps) != 2 {
				b.Fatalf("native range mismatch: ids=%v timestamps=%v", ids, timestamps)
			}
		}
	})
	b.Run("oracle", func(b *testing.B) {
		for range b.N {
			ids, timestamps, err := oracle.Range(fieldKey, oracleRange)
			if err != nil || len(ids.ToSlice()) != 2 || len(timestamps.ToSlice()) != 2 {
				b.Fatalf("oracle range mismatch: ids=%v timestamps=%v err=%v", ids, timestamps, err)
			}
		}
	})
}

func q3Fixture(b *testing.B) (*Store, index.SeriesStore, index.FieldKey, func()) {
	b.Helper()
	seriesID := common.SeriesID(7)
	statusKey := index.FieldKey{TagName: "status", SeriesID: seriesID}
	seriesKey := index.FieldKey{TagName: "series", SeriesID: seriesID}
	latencyKey := index.FieldKey{TagName: "latency", SeriesID: seriesID}
	documents := []index.Document{
		{DocID: 10, Timestamp: 100, Fields: []index.Field{
			index.NewStringField(statusKey, "ok"), index.NewStringField(seriesKey, "series-a"), index.NewIntField(latencyKey, 10),
		}},
		{DocID: 11, Timestamp: 200, Fields: []index.Field{
			index.NewStringField(statusKey, "bad"), index.NewStringField(seriesKey, "series-a"), index.NewIntField(latencyKey, 20),
		}},
		{DocID: 12, Timestamp: 300, Fields: []index.Field{
			index.NewStringField(statusKey, "ok"), index.NewStringField(seriesKey, "series-a"), index.NewIntField(latencyKey, 30),
		}},
		{DocID: 13, Timestamp: 400, Fields: []index.Field{
			index.NewStringField(statusKey, "ok"), index.NewStringField(seriesKey, "series-a"), index.NewIntField(latencyKey, 40),
		}},
		{DocID: 14, Timestamp: 500, Fields: []index.Field{index.NewStringField(seriesKey, "series-a")}},
	}
	oraclePath := b.TempDir()
	oracle, err := inverted.NewStore(inverted.StoreOpts{Path: oraclePath})
	if err != nil {
		b.Fatal(err)
	}
	if err = oracle.Batch(index.Batch{Documents: documents}); err != nil {
		b.Fatal(err)
	}
	if err = oracle.Delete([][]byte{convert.Uint64ToBytes(13)}); err != nil {
		b.Fatal(err)
	}
	if err = oracle.Close(); err != nil {
		b.Fatal(err)
	}
	nativePath := b.TempDir()
	if err = copyDirectory(oraclePath, nativePath); err != nil {
		b.Fatal(err)
	}
	nativeStore, err := NewStore(nativePath, benchmarkLease{}, SearcherOptions{MaxTerms: 32})
	if err != nil {
		b.Fatalf("open copied legacy corpus as native: %v", err)
	}
	oracle, err = inverted.NewStore(inverted.StoreOpts{Path: oraclePath})
	if err != nil {
		b.Fatal(err)
	}
	return nativeStore, oracle, statusKey, func() { _ = nativeStore.Close(); _ = oracle.Close() }
}

type benchmarkLease struct{}

func (benchmarkLease) Validate() error { return nil }

func (benchmarkLease) ValidatePath(string) error { return nil }

func copyDirectory(source, destination string) error {
	return filepath.Walk(source, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		target := filepath.Join(destination, relative)
		if info.IsDir() {
			return os.MkdirAll(target, info.Mode())
		}
		input, err := os.Open(path)
		if err != nil {
			return err
		}
		defer input.Close()
		output, err := os.OpenFile(target, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, info.Mode())
		if err != nil {
			return err
		}
		_, copyErr := io.Copy(output, input)
		closeErr := output.Close()
		if copyErr != nil {
			return copyErr
		}
		return closeErr
	})
}

func assertParity(b *testing.B, wantIDs, wantTimestamps, gotIDs, gotTimestamps, expectedIDs, expectedTimestamps []uint64) {
	b.Helper()
	sort.Slice(wantIDs, func(i, j int) bool { return wantIDs[i] < wantIDs[j] })
	sort.Slice(gotIDs, func(i, j int) bool { return gotIDs[i] < gotIDs[j] })
	sort.Slice(wantTimestamps, func(i, j int) bool { return wantTimestamps[i] < wantTimestamps[j] })
	sort.Slice(gotTimestamps, func(i, j int) bool { return gotTimestamps[i] < gotTimestamps[j] })
	if !equalUint64(wantIDs, gotIDs) || !equalUint64(wantTimestamps, gotTimestamps) || !equalUint64(gotIDs, expectedIDs) || !equalUint64(gotTimestamps, expectedTimestamps) {
		b.Fatalf("parity mismatch: native ids=%v timestamps=%v; oracle ids=%v timestamps=%v", gotIDs, gotTimestamps, wantIDs, wantTimestamps)
	}
}

func equalUint64(left, right []uint64) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
