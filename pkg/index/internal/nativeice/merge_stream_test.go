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
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package nativeice

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	roaringpkg "github.com/RoaringBitmap/roaring"
)

// forceMergeSpill makes every staged merge section spill to a file after a
// few bytes, so tests exercise the file-backed staging path.
func forceMergeSpill(t *testing.T) {
	t.Helper()
	previous := mergeSpillThreshold
	mergeSpillThreshold = 256
	t.Cleanup(func() { mergeSpillThreshold = previous })
}

func mergeToFile(t *testing.T, inputs []MergeInput) ([]byte, MergeStats, error) {
	t.Helper()
	directory := t.TempDir()
	path := filepath.Join(directory, ".native-merge-test")
	stats, mergeErr := MergeSegmentsToFile(context.Background(), inputs, path)
	entries, readErr := os.ReadDir(directory)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if mergeErr != nil {
		if len(entries) != 0 {
			t.Fatalf("failed merge left %d files behind", len(entries))
		}
		return nil, stats, mergeErr
	}
	if len(entries) != 1 {
		t.Fatalf("merge left %d files, want only the output", len(entries))
	}
	payload, fileErr := os.ReadFile(path)
	if fileErr != nil {
		t.Fatal(fileErr)
	}
	if uint64(len(payload)) != stats.Size {
		t.Fatalf("merge stats size %d, file holds %d bytes", stats.Size, len(payload))
	}
	return payload, stats, nil
}

func TestMergeSegmentsToFileMatchesReferenceWithSpilledSections(t *testing.T) {
	forceMergeSpill(t)
	rng := rand.New(rand.NewSource(20261008)) //nolint:gosec // a fixed seed keeps failing rounds reproducible.
	for round := 0; round < 120; round++ {
		inputs := randomMergeInputs(t, rng)
		expected, expectedErr := referenceMergeSegments(context.Background(), inputs)
		actual, _, actualErr := mergeToFile(t, inputs)
		if (expectedErr == nil) != (actualErr == nil) {
			t.Fatalf("round %d: reference error %v, merge error %v", round, expectedErr, actualErr)
		}
		if expectedErr == nil && !bytes.Equal(expected.Payload, actual) {
			t.Fatalf("round %d: streamed payload differs from document re-encoding (%d vs %d bytes)", round, len(expected.Payload), len(actual))
		}
	}
}

// largeMergeGeneration exercises the shapes the small random rounds cannot
// reach: many stored and doc-value chunks, postings dense enough for bitmap
// containers and multi-chunk frequency streams, more than 1024 terms per
// field (vellum's default registry class), one-hit terms, frequencies above
// one, and terms with a single frequency-bearing document.
func largeMergeGeneration(rng *rand.Rand, prefix string, documentCount int) Generation {
	documents := make([]EncodeDocument, documentCount)
	for index := range documents {
		identifier := fmt.Sprintf("%s-%07d", prefix, index)
		document := EncodeDocument{Identifier: []byte(identifier), Deleted: rng.Intn(23) == 0}
		document.Fields = append(document.Fields,
			EncodeField{Name: "service", Value: []byte(fmt.Sprintf("svc-%d", index%3)), Index: true, Store: true},
			EncodeField{Name: "instance", Value: []byte(fmt.Sprintf("inst-%d", rng.Intn(1500))), Index: true, Sort: true},
			EncodeField{Name: "unique", Value: []byte(identifier), Index: true},
			EncodeField{Name: "blob", Value: bytes.Repeat([]byte{byte(index)}, 1+index%40), Store: true},
		)
		if index%5 == 0 {
			document.Fields = append(document.Fields, EncodeField{Name: "analyzed", Value: []byte("text"), Index: true, Terms: []EncodeTerm{
				{Value: []byte("common"), Frequency: uint64(1 + rng.Intn(3))},
				{Value: []byte(fmt.Sprintf("rare-%d", index)), Frequency: uint64(1 + index%2)},
			}})
		}
		if index%7 == 0 {
			document.Fields = append(document.Fields,
				EncodeField{Name: "sorted", Value: []byte{0xff, 0x5c, byte(index)}, Sort: true},
				EncodeField{Name: "sorted", Value: []byte(fmt.Sprintf("second-%d", index)), Sort: true, Store: true})
		}
		documents[index] = document
	}
	return Generation{Documents: documents, SegmentID: 1, SnapshotID: 1, IdentifierDocValues: prefix == "b"}
}

func TestMergeSegmentsToFileMatchesReferenceOnLargeInputs(t *testing.T) {
	for _, spill := range []bool{false, true} {
		t.Run(fmt.Sprintf("spill=%v", spill), func(t *testing.T) {
			if spill {
				forceMergeSpill(t)
			}
			rng := rand.New(rand.NewSource(7)) //nolint:gosec // deterministic fixture.
			var inputs []MergeInput
			for inputIndex, size := range []int{5000, 1, 3000, 2600} {
				directory := t.TempDir()
				if encodeErr := Encode(directory, largeMergeGeneration(rng, string(rune('a'+inputIndex)), size)); encodeErr != nil {
					t.Fatal(encodeErr)
				}
				reader, openErr := OpenStrict(directory)
				if openErr != nil {
					t.Fatal(openErr)
				}
				t.Cleanup(func() { _ = reader.Close() })
				input := MergeInput{Reader: reader, IndexedFields: []string{"declared"}}
				switch inputIndex {
				case 1:
					// Every document of this input is dropped: it contributes
					// nothing, not even its declared indexed fields.
					input.Drop = roaringpkg.BitmapOf(0)
					input.IndexedFields = []string{"only-from-empty-input"}
				case 2:
					input.Drop = roaringpkg.New()
					input.Drop.AddRange(100, 2500)
				}
				inputs = append(inputs, input)
			}
			expected, expectedErr := referenceMergeSegments(context.Background(), inputs)
			if expectedErr != nil {
				t.Fatal(expectedErr)
			}
			actual, stats, actualErr := mergeToFile(t, inputs)
			if actualErr != nil {
				t.Fatal(actualErr)
			}
			if !bytes.Equal(expected.Payload, actual) {
				t.Fatalf("streamed payload differs from document re-encoding (%d vs %d bytes)", len(expected.Payload), len(actual))
			}
			reader, openErr := OpenSegment(actual)
			if openErr != nil {
				t.Fatal(openErr)
			}
			defer func() { _ = reader.Close() }()
			if reader.DocumentCount() != stats.DocumentCount {
				t.Fatalf("merged document count %d, stats report %d", reader.DocumentCount(), stats.DocumentCount)
			}
			fields, fieldsErr := reader.Fields()
			if fieldsErr != nil {
				t.Fatal(fieldsErr)
			}
			if containsString(fields, "only-from-empty-input") || !containsString(fields, "declared") {
				t.Fatalf("merged fields = %v", fields)
			}
		})
	}
}

func TestMergeSegmentsToFileRemovesOutputOnFailure(t *testing.T) {
	forceMergeSpill(t)
	rng := rand.New(rand.NewSource(3)) //nolint:gosec // deterministic fixture.
	payload, encodeErr := EncodeSegment(largeMergeGeneration(rng, "a", 3000))
	if encodeErr != nil {
		t.Fatal(encodeErr)
	}
	reader, openErr := OpenSegmentBorrowed(payload)
	if openErr != nil {
		t.Fatal(openErr)
	}
	defer func() { _ = reader.Close() }()
	directory := t.TempDir()
	path := filepath.Join(directory, ".native-merge-canceled")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Cancel once the merge is well under way, after it has written output
	// and spilled staged sections.
	input := MergeInput{Reader: reader}
	go func() {
		for {
			if info, statErr := os.Stat(path); statErr == nil && info.Size() > 0 {
				cancel()
				return
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()
	_, mergeErr := MergeSegmentsToFile(ctx, []MergeInput{input}, path)
	if mergeErr == nil {
		t.Skip("merge finished before cancellation was observed")
	}
	if !errors.Is(mergeErr, context.Canceled) {
		t.Fatalf("merge error = %v, want context.Canceled", mergeErr)
	}
	entries, readErr := os.ReadDir(directory)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("canceled merge left %d files behind", len(entries))
	}
}

func TestMergeSegmentsToFileRejectsInvalidInputWithoutLeftovers(t *testing.T) {
	payload, encodeErr := encodeNativeSegmentUnvalidated(Generation{Documents: []EncodeDocument{
		{Identifier: []byte("x"), Fields: []EncodeField{{Name: "", Value: []byte("v"), Store: true}}},
	}})
	if encodeErr != nil {
		t.Fatal(encodeErr)
	}
	reader, openErr := OpenSegmentBorrowed(payload)
	if openErr != nil {
		t.Skipf("unencodable fixture: %v", openErr)
	}
	defer func() { _ = reader.Close() }()
	if _, _, mergeErr := mergeToFile(t, []MergeInput{{Reader: reader}}); !errors.Is(mergeErr, ErrInvalidGeneration) {
		t.Fatalf("merge error = %v, want ErrInvalidGeneration", mergeErr)
	}
}

// TestDocValueTermLengthMatchesDecoder checks the merge's copy-through
// validation accepts and rejects exactly what the doc-value decoder does, and
// that an accepted term is the canonical encoding of its decoded value.
func TestDocValueTermLengthMatchesDecoder(t *testing.T) {
	rng := rand.New(rand.NewSource(11)) //nolint:gosec // deterministic fuzzing.
	alphabet := []byte{'a', 0xff, '\\', 0x00}
	cases := [][]byte{{}, {0xff}, {'\\'}, {'\\', 'a', 0xff}, {'\\', 0xff}, {'\\', 0xff, 0xff}, {'a', '\\', '\\', 0xff, 'b'}}
	for round := 0; round < 20000; round++ {
		encoded := make([]byte, rng.Intn(8))
		for index := range encoded {
			encoded[index] = alphabet[rng.Intn(len(alphabet))]
		}
		cases = append(cases, encoded)
	}
	cases = append(cases, append(bytes.Repeat([]byte{'x'}, maxRepairSortValueLength), 0xff),
		append(bytes.Repeat([]byte{'x'}, maxRepairSortValueLength+1), 0xff))
	for _, encoded := range cases {
		value, rest, decodeErr := decodeRepairDocValueTerm(encoded)
		length, lengthErr := docValueTermLength(encoded)
		if (decodeErr == nil) != (lengthErr == nil) {
			t.Fatalf("%q: decoder error %v, length error %v", encoded, decodeErr, lengthErr)
		}
		if decodeErr != nil {
			continue
		}
		if length != len(encoded)-len(rest) || !bytes.Equal(appendNativeICEDocValueTerm(nil, value), encoded[:length]) {
			t.Fatalf("%q: length %d, decoded %q rest %q", encoded, length, value, rest)
		}
	}
}

// TestMergeSpillFilesNeverKeepAName proves a spill file is nameless while it
// holds staged bytes, so a crash cannot leave it in the index directory.
func TestMergeSpillFilesNeverKeepAName(t *testing.T) {
	forceMergeSpill(t)
	directory := t.TempDir()
	factory := spillFactory{prefix: filepath.Join(directory, ".native-merge-spill-test")}
	buffer := factory.newBuffer()
	payload := bytes.Repeat([]byte("staged"), mergeSpillThreshold)
	for round := 0; round < 3; round++ {
		if _, writeErr := buffer.Write(payload); writeErr != nil {
			t.Fatal(writeErr)
		}
	}
	if buffer.file == nil {
		t.Fatal("the buffer did not spill")
	}
	if entries, readErr := os.ReadDir(directory); readErr != nil || len(entries) != 0 {
		t.Fatalf("spilled buffer left names %v (err %v)", entries, readErr)
	}
	var copied bytes.Buffer
	if copyErr := buffer.copyTo(&copied); copyErr != nil {
		t.Fatal(copyErr)
	}
	if !bytes.Equal(copied.Bytes(), bytes.Repeat(payload, 3)) {
		t.Fatal("spilled bytes differ from the written bytes")
	}
	if resetErr := buffer.reset(); resetErr != nil {
		t.Fatal(resetErr)
	}
}
