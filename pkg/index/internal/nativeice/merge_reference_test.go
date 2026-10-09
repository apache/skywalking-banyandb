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
	"fmt"
	"math"
	"sort"
)

// referenceMergeResult is the reference merge's encoded segment and one
// old-local to new-local mapping per input; DroppedDocumentNumber marks an
// old document omitted from the output.
type referenceMergeResult struct {
	Payload  []byte
	Mappings [][]uint64
}

// referenceMergeSegments is the original MergeSegments, kept verbatim as the
// differential-test reference: it decodes every input into documents and
// re-encodes them. MergeSegments must produce byte-identical results.
//
// It merges native single-segment readers without involving a
// query engine or materializing segment bytes. Stored repeated values, analyzed
// terms and frequencies, and repeated doc values are retained. Documents are
// emitted in input order and context cancellation is checked between physical
// documents and field walks.
func referenceMergeSegments(ctx context.Context, inputs []MergeInput) (referenceMergeResult, error) {
	if err := ctx.Err(); err != nil {
		return referenceMergeResult{}, err
	}
	merged := make([]EncodeDocument, 0)
	mappings := make([][]uint64, len(inputs))
	for inputIndex, input := range inputs {
		if input.Reader == nil || input.Reader.SegmentCount() != 1 {
			return referenceMergeResult{}, fmt.Errorf("merge input %d must hold one segment: %w", inputIndex, ErrCorrupt)
		}
		segmentDocuments, segmentMappings, decodeErr := decodeMergeInput(ctx, input)
		if decodeErr != nil {
			return referenceMergeResult{}, decodeErr
		}
		outputOffset := uint64(len(merged))
		for mappingIndex, mapping := range segmentMappings {
			if mapping != DroppedDocumentNumber {
				segmentMappings[mappingIndex] = mapping + outputOffset
			}
		}
		mappings[inputIndex] = segmentMappings
		merged = append(merged, segmentDocuments...)
	}
	payload, encodeErr := EncodeSegment(Generation{Documents: merged})
	if encodeErr != nil {
		return referenceMergeResult{}, encodeErr
	}
	return referenceMergeResult{Payload: payload, Mappings: mappings}, nil
}

//nolint:govet // borrowed field collections are grouped by modality.
type mergeDocument struct {
	identifier []byte
	deleted    bool
	stored     map[string][][]byte
	terms      map[string][]EncodeTerm
	sortValues map[string][][]byte
}

//nolint:gocyclo // merge preserves independent stored, indexed, sorted and mask modalities.
func decodeMergeInput(ctx context.Context, input MergeInput) ([]EncodeDocument, []uint64, error) {
	reader := input.Reader
	physicalCount := reader.DocumentCount()
	documents := make([]mergeDocument, physicalCount)
	for documentIndex := range documents {
		documents[documentIndex] = mergeDocument{
			stored: make(map[string][][]byte), terms: make(map[string][]EncodeTerm), sortValues: make(map[string][][]byte),
		}
	}
	// VisitPhysicalDocuments is ascending by local ordinal; keep the cursor
	// local so borrowed document values never escape the callback.
	localDocument := uint64(0)
	if visitErr := reader.VisitPhysicalDocuments(ctx, func(document StoredDocument, deleted bool) error {
		if localDocument >= physicalCount {
			return fmt.Errorf("physical document walk exceeded segment count: %w", ErrCorrupt)
		}
		current := &documents[localDocument]
		current.deleted = deleted
		if visitErr := document.VisitStoredFields(func(name string, value []byte) bool {
			copied := append([]byte(nil), value...)
			if name == identifierField {
				current.identifier = copied
			} else {
				current.stored[name] = append(current.stored[name], copied)
			}
			return true
		}); visitErr != nil {
			return visitErr
		}
		localDocument++
		return nil
	}); visitErr != nil {
		return nil, nil, visitErr
	}
	if localDocument != physicalCount {
		return nil, nil, fmt.Errorf("physical document count %d differs from footer %d: %w", localDocument, physicalCount, ErrCorrupt)
	}

	fields, fieldsErr := reader.Fields()
	if fieldsErr != nil {
		return nil, nil, fieldsErr
	}
	indexed := make(map[string]struct{}, len(input.IndexedFields))
	for _, field := range input.IndexedFields {
		if field == identifierField {
			continue
		}
		indexed[field] = struct{}{}
	}
	for _, field := range fields {
		if field == identifierField {
			continue
		}
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		//nolint:contextcheck // cancellation is checked per term in the callback.
		if visitErr := reader.VisitTermPostings(
			field, nil, func(term []byte, documentsForTerm []uint64, frequencies []TermFrequency) error {
				if err := ctx.Err(); err != nil {
					return err
				}
				frequencyByDocument := make(map[uint64]uint64, len(frequencies))
				for _, frequency := range frequencies {
					if err := ctx.Err(); err != nil {
						return err
					}
					frequencyByDocument[frequency.DocumentNumber] = frequency.Frequency
				}
				for _, documentNumber := range documentsForTerm {
					if err := ctx.Err(); err != nil {
						return err
					}
					if documentNumber >= physicalCount {
						return fmt.Errorf("term %q has document %d outside segment: %w", term, documentNumber, ErrCorrupt)
					}
					frequency := frequencyByDocument[documentNumber]
					if frequency == 0 {
						frequency = 1
					}
					documents[documentNumber].terms[field] = append(documents[documentNumber].terms[field], EncodeTerm{Value: append([]byte(nil), term...), Frequency: frequency})
				}
				return nil
			}); visitErr != nil {
			return nil, nil, visitErr
		}
		if visitErr := reader.VisitFieldDocumentValues(field, func(documentNumber uint64, values [][]byte) error {
			if documentNumber >= physicalCount {
				return fmt.Errorf("doc value for field %q has document %d outside segment: %w", field, documentNumber, ErrCorrupt)
			}
			for _, value := range values {
				documents[documentNumber].sortValues[field] = append(documents[documentNumber].sortValues[field], append([]byte(nil), value...))
			}
			return ctx.Err()
		}); visitErr != nil {
			return nil, nil, visitErr
		}
	}

	mappings := make([]uint64, physicalCount)
	for documentNumber := range mappings {
		mappings[documentNumber] = DroppedDocumentNumber
	}
	result := make([]EncodeDocument, 0, physicalCount)
	for documentNumber, document := range documents {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		if document.identifier == nil {
			return nil, nil, fmt.Errorf("physical document %d has no identifier: %w", documentNumber, ErrInvalidGeneration)
		}
		if document.deleted || (input.Drop != nil && uint64(documentNumber) <= math.MaxUint32 && input.Drop.Contains(uint32(documentNumber))) {
			continue
		}
		encoded := encodeMergedDocument(document, indexed)
		mappings[documentNumber] = uint64(len(result))
		result = append(result, encoded)
	}
	return result, mappings, nil
}

func encodeMergedDocument(document mergeDocument, indexed map[string]struct{}) EncodeDocument {
	encoded := EncodeDocument{Identifier: append([]byte(nil), document.identifier...)}
	fieldNames := make(map[string]struct{}, len(document.stored)+len(document.terms)+len(document.sortValues)+len(indexed))
	for name := range document.stored {
		fieldNames[name] = struct{}{}
	}
	for name := range document.terms {
		fieldNames[name] = struct{}{}
	}
	for name := range document.sortValues {
		fieldNames[name] = struct{}{}
	}
	for name := range indexed {
		fieldNames[name] = struct{}{}
	}
	ordered := make([]string, 0, len(fieldNames))
	for name := range fieldNames {
		if name != identifierField {
			ordered = append(ordered, name)
		}
	}
	sort.Strings(ordered)
	for _, name := range ordered {
		storedValues := document.stored[name]
		terms := document.terms[name]
		sortValues := document.sortValues[name]
		_, declaredIndexed := indexed[name]
		isIndexed := declaredIndexed || len(terms) > 0
		if isIndexed && terms == nil {
			terms = make([]EncodeTerm, 0)
		}
		placeholderIndexed := len(storedValues) == 0 && len(sortValues) == 0 && len(terms) == 0 && isIndexed
		values := storedValues
		if placeholderIndexed {
			values = [][]byte{nil}
		}
		if len(values) == 0 {
			values = sortValues
		}
		if len(values) == 0 && len(terms) > 0 {
			values = [][]byte{terms[0].Value}
		}
		if len(values) == 0 {
			values = [][]byte{nil}
		}
		separateSort := len(storedValues) > 0 && len(sortValues) > 0 && !equalByteSlices(storedValues, sortValues)
		for valueIndex, value := range values {
			fieldTerms := terms
			if valueIndex > 0 {
				fieldTerms = nil
			}
			encoded.Fields = append(encoded.Fields, EncodeField{
				Name: name, Value: append([]byte(nil), value...), Terms: fieldTerms,
				Index: isIndexed && valueIndex == 0, Store: len(storedValues) > 0,
				Sort: !separateSort && valueIndex < len(sortValues),
			})
		}
		if separateSort || len(sortValues) > len(values) {
			start := 0
			if !separateSort {
				start = len(values)
			}
			for _, value := range sortValues[start:] {
				encoded.Fields = append(encoded.Fields, EncodeField{Name: name, Value: append([]byte(nil), value...), Sort: true})
			}
		}
	}
	return encoded
}

func equalByteSlices(left, right [][]byte) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if !bytes.Equal(left[index], right[index]) {
			return false
		}
	}
	return true
}
