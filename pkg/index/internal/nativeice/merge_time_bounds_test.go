// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. Apache Software
// Foundation (ASF) licenses this file to you under the Apache License, Version
// 2.0 (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package nativeice

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"

	roaringpkg "github.com/RoaringBitmap/roaring"
)

// timestampDocument builds a document carrying a stored _timestamp exactly the
// way the owner writes one, so a merge has the same value to fold.
func timestampDocument(identifier string, timestamp int64) EncodeDocument {
	return EncodeDocument{
		Identifier: []byte(identifier),
		Fields: []EncodeField{{
			Name:  timestampField,
			Value: EncodePrefixCodedInt64(timestamp),
			Terms: timestampTerms(timestamp), Store: true, Index: true, Sort: true,
		}},
	}
}

// timestampTerms mirrors the owner's multi-precision _timestamp term trie.
func timestampTerms(timestamp int64) []EncodeTerm {
	terms := make([]EncodeTerm, 0, 16)
	for shift := uint(0); shift <= 60; shift += 4 {
		terms = append(terms, EncodeTerm{Value: EncodePrefixCodedInt64Shift(timestamp, shift), Frequency: 1})
	}
	return terms
}

// footerBounds reads the two time slots back out of an encoded segment's
// footer, which is what a reopened reader would see.
func footerBounds(t *testing.T, payload []byte) (uint64, uint64) {
	t.Helper()
	if len(payload) < segmentFooterLength {
		t.Fatalf("payload %d bytes is shorter than a footer", len(payload))
	}
	footer := payload[len(payload)-segmentFooterLength:]
	return binary.BigEndian.Uint64(footer[36:44]), binary.BigEndian.Uint64(footer[44:52])
}

// encodeInputs writes one segment per generation and returns their readers.
func encodeInputs(t *testing.T, generations ...Generation) []*Reader {
	t.Helper()
	readers := make([]*Reader, 0, len(generations))
	for index := range generations {
		path := t.TempDir()
		if encodeErr := Encode(path, generations[index]); encodeErr != nil {
			t.Fatal(encodeErr)
		}
		reader, openErr := OpenStrict(path)
		if openErr != nil {
			t.Fatal(openErr)
		}
		t.Cleanup(func() { _ = reader.Close() })
		readers = append(readers, reader)
	}
	return readers
}

func TestMergeComputesExactTimeBounds(t *testing.T) {
	readers := encodeInputs(t,
		Generation{SegmentID: 1, SnapshotID: 1, Documents: []EncodeDocument{
			timestampDocument("a", 1_700_000_000_000_000_000),
			timestampDocument("b", 1_800_000_000_000_000_000),
		}},
		Generation{SegmentID: 2, SnapshotID: 1, Documents: []EncodeDocument{
			timestampDocument("c", 1_750_000_000_000_000_000),
		}},
	)
	inputs := []MergeInput{
		{Reader: readers[0], IndexedFields: []string{timestampField}},
		{Reader: readers[1], IndexedFields: []string{timestampField}},
	}

	var output bytes.Buffer
	stats, mergeErr := MergeSegmentsTo(context.Background(), inputs, &output, "")
	if mergeErr != nil {
		t.Fatal(mergeErr)
	}

	const wantMin = int64(1_700_000_000_000_000_000)
	const wantMax = int64(1_800_000_000_000_000_000)
	if !stats.HasTime {
		t.Fatal("merged segment reported no time bounds")
	}
	if int64(stats.TimeMin) != wantMin {
		t.Errorf("TimeMin = %d, want %d", int64(stats.TimeMin), wantMin)
	}
	if int64(stats.TimeMax) != wantMax {
		t.Errorf("TimeMax = %d, want %d", int64(stats.TimeMax), wantMax)
	}

	// The footer must carry the same values, or a reopened segment would read
	// them back as zero and lose the bounds the merge computed.
	footerMin, footerMax := footerBounds(t, output.Bytes())
	if footerMin != stats.TimeMin || footerMax != stats.TimeMax {
		t.Errorf("footer bounds = (%d, %d), want (%d, %d)", footerMin, footerMax, stats.TimeMin, stats.TimeMax)
	}
}

func TestMergeTimeBoundsExcludeDroppedDocuments(t *testing.T) {
	readers := encodeInputs(t, Generation{SegmentID: 1, SnapshotID: 1, Documents: []EncodeDocument{
		timestampDocument("keep-low", 1_000),
		timestampDocument("drop-min", 2_000),
		timestampDocument("drop-max", 9_000),
		timestampDocument("keep-high", 5_000),
	}})

	// Drop the two extreme documents. The bounds must describe only what
	// survives, otherwise a query could skip a segment that still holds an
	// in-range document.
	drop := roaringpkg.New()
	drop.Add(1)
	drop.Add(2)
	stats, mergeErr := MergeSegmentsTo(context.Background(),
		[]MergeInput{{Reader: readers[0], Drop: drop, IndexedFields: []string{timestampField}}}, &bytes.Buffer{}, "")
	if mergeErr != nil {
		t.Fatal(mergeErr)
	}
	if int64(stats.TimeMin) != 1_000 || int64(stats.TimeMax) != 5_000 {
		t.Errorf("bounds = (%d, %d), want (1000, 5000) over survivors only",
			int64(stats.TimeMin), int64(stats.TimeMax))
	}
}

func TestMergeTimeBoundsExcludeDeletedDocuments(t *testing.T) {
	deleted := timestampDocument("gone", 1)
	deleted.Deleted = true
	readers := encodeInputs(t, Generation{SegmentID: 1, SnapshotID: 1, Documents: []EncodeDocument{
		deleted,
		timestampDocument("alive", 4_200),
	}})
	stats, mergeErr := MergeSegmentsTo(context.Background(),
		[]MergeInput{{Reader: readers[0], IndexedFields: []string{timestampField}}}, &bytes.Buffer{}, "")
	if mergeErr != nil {
		t.Fatal(mergeErr)
	}
	if int64(stats.TimeMin) != 4_200 || int64(stats.TimeMax) != 4_200 {
		t.Errorf("bounds = (%d, %d), want (4200, 4200); a deleted document must not widen them",
			int64(stats.TimeMin), int64(stats.TimeMax))
	}
}

func TestMergeWithoutTimestampsReportsNoTime(t *testing.T) {
	readers := encodeInputs(t, Generation{SegmentID: 1, SnapshotID: 1, Documents: []EncodeDocument{
		{Identifier: []byte("a"), Fields: []EncodeField{
			{Name: "tag", Value: []byte("v"), Terms: []EncodeTerm{{Value: []byte("t")}}, Index: true, Store: true},
		}},
	}})
	var output bytes.Buffer
	stats, mergeErr := MergeSegmentsTo(context.Background(),
		[]MergeInput{{Reader: readers[0], IndexedFields: []string{"tag"}}}, &output, "")
	if mergeErr != nil {
		t.Fatal(mergeErr)
	}
	if stats.HasTime {
		t.Errorf("HasTime = true for a segment with no timestamps (bounds %d, %d)", stats.TimeMin, stats.TimeMax)
	}
	if stats.TimeMin != 0 || stats.TimeMax != 0 {
		t.Errorf("bounds = (%d, %d), want zero slots", stats.TimeMin, stats.TimeMax)
	}
	footerMin, footerMax := footerBounds(t, output.Bytes())
	if footerMin != 0 || footerMax != 0 {
		t.Errorf("footer bounds = (%d, %d), want zero", footerMin, footerMax)
	}
}

func TestMergeZeroTimestampIsNotTime(t *testing.T) {
	// The flush path skips a zero timestamp entirely, so a merged segment must
	// agree: treating it as a bound would claim documents a range can never
	// match.
	readers := encodeInputs(t, Generation{SegmentID: 1, SnapshotID: 1, Documents: []EncodeDocument{
		timestampDocument("zero", 0),
	}})
	stats, mergeErr := MergeSegmentsTo(context.Background(),
		[]MergeInput{{Reader: readers[0], IndexedFields: []string{timestampField}}}, &bytes.Buffer{}, "")
	if mergeErr != nil {
		t.Fatal(mergeErr)
	}
	if stats.HasTime {
		t.Errorf("HasTime = true for a lone zero timestamp (bounds %d, %d)", stats.TimeMin, stats.TimeMax)
	}
}

func TestMergeRepairsInputsWithoutBounds(t *testing.T) {
	// Inputs written before merged segments carried bounds have zero slots. A
	// merge must still produce exact bounds rather than inheriting the gap,
	// which is what makes old merged segments heal without a migration.
	readers := encodeInputs(t, Generation{SegmentID: 1, SnapshotID: 1, Documents: []EncodeDocument{
		timestampDocument("a", 10),
		timestampDocument("b", 20),
	}})
	stats, mergeErr := MergeSegmentsTo(context.Background(),
		[]MergeInput{{Reader: readers[0], IndexedFields: []string{timestampField}}}, &bytes.Buffer{}, "")
	if mergeErr != nil {
		t.Fatal(mergeErr)
	}
	if int64(stats.TimeMin) != 10 || int64(stats.TimeMax) != 20 {
		t.Errorf("bounds = (%d, %d), want (10, 20)", int64(stats.TimeMin), int64(stats.TimeMax))
	}
}

func TestMergeFormsAgreeOnTimeBounds(t *testing.T) {
	readers := encodeInputs(t,
		Generation{SegmentID: 1, SnapshotID: 1, Documents: []EncodeDocument{
			timestampDocument("a", 300), timestampDocument("b", 100),
		}},
		Generation{SegmentID: 2, SnapshotID: 1, Documents: []EncodeDocument{
			timestampDocument("c", 200),
		}},
	)
	inputs := []MergeInput{
		{Reader: readers[0], IndexedFields: []string{timestampField}},
		{Reader: readers[1], IndexedFields: []string{timestampField}},
	}
	streamed, streamErr := MergeSegmentsTo(context.Background(), inputs, &bytes.Buffer{}, "")
	if streamErr != nil {
		t.Fatal(streamErr)
	}
	buffered, bufferedErr := MergeSegments(context.Background(), inputs)
	if bufferedErr != nil {
		t.Fatal(bufferedErr)
	}
	// The buffered form returns a payload only; reopen it so both forms are
	// compared through the same footer.
	reader, openErr := OpenSegment(buffered.Payload)
	if openErr != nil {
		t.Fatal(openErr)
	}
	defer func() { _ = reader.Close() }()
	bufferedMin, bufferedMax := footerBounds(t, buffered.Payload)
	if streamed.TimeMin != bufferedMin || streamed.TimeMax != bufferedMax {
		t.Errorf("streamed bounds (%d, %d) differ from buffered (%d, %d)",
			streamed.TimeMin, streamed.TimeMax, bufferedMin, bufferedMax)
	}
}

func TestMergeTimeBoundsMatchTheFlushPathAcrossASignChange(t *testing.T) {
	// The bounds are the uint64 slots the footer holds, and the flush path
	// folds them with an unsigned comparison (owner.go). A set spanning zero
	// therefore folds to a pair whose int64 reading is inverted, that is
	// int64(min) > int64(max).
	//
	// The merge must fold them exactly the same way, because the merged bytes
	// have to stay identical to what EncodeSegment writes for the same
	// documents. It deliberately does not "fix" the ordering: a query reading
	// these bounds detects int64(min) > int64(max) and treats the segment as
	// having unknown bounds, which skips pruning rather than pruning wrongly.
	documents := []EncodeDocument{
		timestampDocument("neg", -5_000),
		timestampDocument("pos", 5_000),
	}
	readers := encodeInputs(t, Generation{SegmentID: 1, SnapshotID: 1, Documents: documents})

	var output bytes.Buffer
	stats, mergeErr := MergeSegmentsTo(context.Background(),
		[]MergeInput{{Reader: readers[0], IndexedFields: []string{timestampField}}}, &output, "")
	if mergeErr != nil {
		t.Fatal(mergeErr)
	}

	// Fold the reference way to establish what the footer must contain.
	var wantMin, wantMax uint64
	var hasTime bool
	for _, document := range documents {
		for _, field := range document.Fields {
			if field.Name != timestampField {
				continue
			}
			timestamp, decodeErr := DecodePrefixCodedInt64(field.Value)
			if decodeErr != nil {
				t.Fatal(decodeErr)
			}
			encoded := uint64(timestamp)
			if !hasTime || encoded < wantMin {
				wantMin = encoded
			}
			if !hasTime || encoded > wantMax {
				wantMax = encoded
			}
			hasTime = true
		}
	}
	if !hasTime {
		t.Fatal("test premise broken: no timestamp folded")
	}
	// The premise is that the unsigned fold really does invert here.
	if int64(wantMin) <= int64(wantMax) {
		t.Fatalf("test premise broken: bounds (%d, %d) do not invert", wantMin, wantMax)
	}
	if stats.TimeMin != wantMin || stats.TimeMax != wantMax {
		t.Errorf("merge bounds = (%d, %d), want the flush path's (%d, %d)",
			stats.TimeMin, stats.TimeMax, wantMin, wantMax)
	}
	if footerMin, footerMax := footerBounds(t, output.Bytes()); footerMin != wantMin || footerMax != wantMax {
		t.Errorf("footer bounds = (%d, %d), want (%d, %d)", footerMin, footerMax, wantMin, wantMax)
	}
}
