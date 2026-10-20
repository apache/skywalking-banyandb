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
	"testing"

	roaringpkg "github.com/RoaringBitmap/roaring"
)

var differentialFieldNames = []string{"a", "b", "group", "name", "source", "sort", "zz"}

func randomMergeGeneration(rng *rand.Rand, documentCount int) Generation {
	documents := make([]EncodeDocument, documentCount)
	for index := range documents {
		document := EncodeDocument{Identifier: []byte(fmt.Sprintf("id-%d", rng.Intn(documentCount*2+3))), Deleted: rng.Intn(8) == 0}
		for fieldIndex := 0; fieldIndex < rng.Intn(7); fieldIndex++ {
			field := EncodeField{
				Name: differentialFieldNames[rng.Intn(len(differentialFieldNames))], Value: []byte(fmt.Sprintf("v%d", rng.Intn(25))),
				Index: rng.Intn(2) == 0, Store: rng.Intn(2) == 0, Sort: rng.Intn(3) == 0,
			}
			switch rng.Intn(4) {
			case 0:
				field.Terms = []EncodeTerm{}
			case 1:
				for termIndex := 0; termIndex < 1+rng.Intn(3); termIndex++ {
					field.Terms = append(field.Terms, EncodeTerm{Value: []byte(fmt.Sprintf("t%d", rng.Intn(12))), Frequency: uint64(rng.Intn(4))})
				}
			}
			document.Fields = append(document.Fields, field)
		}
		documents[index] = document
	}
	return Generation{Documents: documents, SegmentID: 1, SnapshotID: 1}
}

func randomMergeInputs(t *testing.T, rng *rand.Rand) []MergeInput {
	t.Helper()
	inputs := make([]MergeInput, 1+rng.Intn(6))
	for index := range inputs {
		generation := randomMergeGeneration(rng, 1+rng.Intn(80))
		var reader *Reader
		var openErr error
		if rng.Intn(2) == 0 {
			// From disk, so the snapshot deletion mask (Deleted) is applied.
			directory := t.TempDir()
			if encodeErr := Encode(directory, generation); encodeErr != nil {
				t.Fatal(encodeErr)
			}
			reader, openErr = OpenStrict(directory)
		} else {
			payload, encodeErr := EncodeSegment(generation)
			if encodeErr != nil {
				t.Fatal(encodeErr)
			}
			reader, openErr = OpenSegmentBorrowed(payload)
		}
		if openErr != nil {
			t.Fatal(openErr)
		}
		t.Cleanup(func() { _ = reader.Close() })
		input := MergeInput{Reader: reader}
		if rng.Intn(2) == 0 {
			input.Drop = roaringpkg.New()
			for documentNumber := range generation.Documents {
				if rng.Intn(4) == 0 {
					input.Drop.Add(uint32(documentNumber))
				}
			}
		}
		for _, name := range []string{"a", "declared-only", "zz", identifierField} {
			if rng.Intn(3) == 0 {
				input.IndexedFields = append(input.IndexedFields, name)
			}
		}
		inputs[index] = input
	}
	return inputs
}

func TestMergeSegmentsMatchesDocumentReencodingByteForByte(t *testing.T) {
	rng := rand.New(rand.NewSource(20261006)) //nolint:gosec // a fixed seed keeps failing rounds reproducible.
	for round := 0; round < 300; round++ {
		inputs := randomMergeInputs(t, rng)
		expected, expectedErr := referenceMergeSegments(context.Background(), inputs)
		actual, actualErr := MergeSegments(context.Background(), inputs)
		if (expectedErr == nil) != (actualErr == nil) {
			t.Fatalf("round %d: reference error %v, merge error %v", round, expectedErr, actualErr)
		}
		if expectedErr != nil {
			continue
		}
		if !bytes.Equal(expected.Payload, actual.Payload) {
			t.Fatalf("round %d: merged payload differs from document re-encoding (%d vs %d bytes)", round, len(expected.Payload), len(actual.Payload))
		}
	}
}

func TestMergeSegmentsMatchesReferenceOnInvalidInputs(t *testing.T) {
	cases := map[string]Generation{
		"empty field name survives": {Documents: []EncodeDocument{{Identifier: []byte("x"), Fields: []EncodeField{{Name: "", Value: []byte("v"), Store: true}}}}},
	}
	for name, generation := range cases {
		t.Run(name, func(t *testing.T) {
			payload, encodeErr := encodeNativeSegmentUnvalidated(generation)
			if encodeErr != nil {
				t.Fatal(encodeErr)
			}
			reader, openErr := OpenSegmentBorrowed(payload)
			if openErr != nil {
				t.Skipf("unencodable fixture: %v", openErr)
			}
			defer func() { _ = reader.Close() }()
			inputs := []MergeInput{{Reader: reader}}
			_, expectedErr := referenceMergeSegments(context.Background(), inputs)
			_, actualErr := MergeSegments(context.Background(), inputs)
			if !errors.Is(actualErr, ErrInvalidGeneration) || !errors.Is(expectedErr, ErrInvalidGeneration) {
				t.Fatalf("reference error %v, merge error %v; both must wrap ErrInvalidGeneration", expectedErr, actualErr)
			}
		})
	}
}

func encodeNativeSegmentUnvalidated(generation Generation) ([]byte, error) {
	payload, _, encodeErr := encodeNativeSegment(generation)
	return payload, encodeErr
}

func schemaShapedMergeInputs(b *testing.B, segments, documentsPerSegment int) []MergeInput {
	b.Helper()
	inputs := make([]MergeInput, segments)
	for segment := range inputs {
		documents := make([]EncodeDocument, documentsPerSegment)
		for index := range documents {
			name := fmt.Sprintf("service_metric_%d_%d", segment, index)
			field := func(fieldName, value string) EncodeField {
				return EncodeField{Name: fieldName, Value: []byte(value), Index: true, Store: true, Terms: []EncodeTerm{{Value: []byte(value), Frequency: 1}}}
			}
			documents[index] = EncodeDocument{Identifier: []byte("measure_sw_metricsMinute/" + name), Fields: []EncodeField{
				field("_entity_id", name), field("_group", "_schema"), field("_im_name", "measure"),
				field("group", "sw_metricsMinute"), field("name", name), field("kind", "measure"),
				field("source", fmt.Sprintf(`{"metadata":{"group":"sw_metricsMinute","name":%q},"interval":"1m","fields":[{"name":"value"}]}`, name)),
				{Name: "_source", Value: []byte(`{"id":"` + name + `"}`), Store: true},
			}}
		}
		payload, encodeErr := EncodeSegment(Generation{Documents: documents})
		if encodeErr != nil {
			b.Fatal(encodeErr)
		}
		reader, openErr := OpenSegmentBorrowed(payload)
		if openErr != nil {
			b.Fatal(openErr)
		}
		b.Cleanup(func() { _ = reader.Close() })
		inputs[segment] = MergeInput{Reader: reader}
	}
	return inputs
}

func BenchmarkMergeSegments(b *testing.B) {
	for _, shape := range []struct {
		name                          string
		segments, documentsPerSegment int
	}{{"10x1", 10, 1}, {"10x100", 10, 100}} {
		for _, implementation := range []struct {
			merge func(context.Context, []MergeInput) (MergeResult, error)
			name  string
		}{{referenceMergeSegmentsPayload, "reference"}, {materializedMergeSegments, "materialized"}, {MergeSegments, "streaming"}} {
			b.Run(shape.name+"/"+implementation.name, func(b *testing.B) {
				inputs := schemaShapedMergeInputs(b, shape.segments, shape.documentsPerSegment)
				b.ReportAllocs()
				b.ResetTimer()
				for iteration := 0; iteration < b.N; iteration++ {
					if _, mergeErr := implementation.merge(context.Background(), inputs); mergeErr != nil {
						b.Fatal(mergeErr)
					}
				}
			})
		}
	}
}

func referenceMergeSegmentsPayload(ctx context.Context, inputs []MergeInput) (MergeResult, error) {
	result, mergeErr := referenceMergeSegments(ctx, inputs)
	return MergeResult{Payload: result.Payload}, mergeErr
}
