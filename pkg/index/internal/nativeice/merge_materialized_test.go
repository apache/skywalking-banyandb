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
	"context"
	"fmt"
	"math"
)

// materializedMergeSegments is the previous in-memory MergeSegments, kept as
// the benchmark baseline for the streaming merge. It builds the whole merged
// segment in memory. It merges native single-segment readers without
// involving a query engine. Stored repeated values, analyzed
// terms and frequencies, and repeated doc values are retained. Documents are
// emitted in input order and context cancellation is checked between physical
// documents and field walks.
//
// Postings and doc values are spliced straight into the output's per-field
// structures with renumbered documents, rather than first expanding every
// input document into per-field term, stored and doc-value maps and then
// re-inverting them. The output is byte-identical to encoding the merged
// documents with EncodeSegment (see referenceMergeSegments in the tests).
func materializedMergeSegments(ctx context.Context, inputs []MergeInput) (MergeResult, error) {
	if err := ctx.Err(); err != nil {
		return MergeResult{}, err
	}
	merger := &segmentMerger{fieldsByName: make(map[string]*nativeICEField)}
	nativeICEFieldFor(merger.fieldsByName, identifierField)
	for inputIndex, input := range inputs {
		if input.Reader == nil || input.Reader.SegmentCount() != 1 {
			return MergeResult{}, fmt.Errorf("merge input %d must hold one segment: %w", inputIndex, ErrCorrupt)
		}
		if _, inputErr := merger.addInput(ctx, input); inputErr != nil {
			return MergeResult{}, inputErr
		}
	}
	documentCount := uint64(len(merger.documents))
	identifier := merger.fieldsByName[identifierField]
	for documentNumber, document := range merger.documents {
		registerNativeICETerm(identifier, document.identifier, uint64(documentNumber), 1)
	}
	fields := orderNativeICEFields(merger.fieldsByName, documentCount)
	fieldIDs := nativeICEFieldIDs(fields)
	values := make([]storedValue, 0)
	storedData, documentOffsets := encodeStoredChunks(len(merger.documents), func(documentIndex int, destination []byte) []byte {
		document := merger.documents[documentIndex]
		values = append(values[:0], storedValue{name: identifierField, value: document.identifier})
		values = append(values, document.stored...)
		return appendStoredDocument(destination, values, fieldIDs)
	})
	payload, assembleErr := assembleNativeSegment(fields, storedData, documentOffsets, documentCount, 0, 0)
	if assembleErr != nil {
		return MergeResult{}, assembleErr
	}
	return MergeResult{Payload: payload}, nil
}

type mergedDocument struct {
	identifier []byte
	stored     []storedValue
}

type segmentMerger struct {
	fieldsByName map[string]*nativeICEField
	documents    []mergedDocument
}

// field returns name's output field, rejecting the empty name the encoder's
// generation validation would reject for a surviving document.
func (m *segmentMerger) field(name string) (*nativeICEField, error) {
	if name == "" {
		return nil, fmt.Errorf("merged document field has no name: %w", ErrInvalidGeneration)
	}
	return nativeICEFieldFor(m.fieldsByName, name), nil
}

//nolint:gocyclo // stored, indexed, sorted and mask modalities are kept explicit.
func (m *segmentMerger) addInput(ctx context.Context, input MergeInput) ([]uint64, error) {
	reader := input.Reader
	physicalCount := reader.DocumentCount()
	mapping := make([]uint64, physicalCount)
	pending := make([]mergedDocument, physicalCount)
	dropped := make([]bool, physicalCount)
	hasIdentifier := make([]bool, physicalCount)
	localDocument := uint64(0)
	if visitErr := reader.VisitPhysicalDocuments(ctx, func(document StoredDocument, deleted bool) error {
		if localDocument >= physicalCount {
			return fmt.Errorf("physical document walk exceeded segment count: %w", ErrCorrupt)
		}
		current := localDocument
		dropped[current] = deleted || (input.Drop != nil && current <= math.MaxUint32 && input.Drop.Contains(uint32(current)))
		if visitErr := document.VisitStoredFields(func(name string, value []byte) bool {
			if name == identifierField {
				// The last identifier value wins, and an empty one counts as
				// missing, exactly as the document-decoding merge treated it.
				hasIdentifier[current] = len(value) > 0
				if !dropped[current] {
					pending[current].identifier = append([]byte(nil), value...)
				}
				return true
			}
			if !dropped[current] {
				pending[current].stored = append(pending[current].stored, storedValue{name: name, value: append([]byte(nil), value...)})
			}
			return true
		}); visitErr != nil {
			return visitErr
		}
		localDocument++
		return nil
	}); visitErr != nil {
		return nil, visitErr
	}
	if localDocument != physicalCount {
		return nil, fmt.Errorf("physical document count %d differs from footer %d: %w", localDocument, physicalCount, ErrCorrupt)
	}
	survivors := 0
	for documentNumber := range mapping {
		if !hasIdentifier[documentNumber] {
			return nil, fmt.Errorf("physical document %d has no identifier: %w", documentNumber, ErrInvalidGeneration)
		}
		if dropped[documentNumber] {
			mapping[documentNumber] = DroppedDocumentNumber
			continue
		}
		outputNumber := uint64(len(m.documents))
		if len(pending[documentNumber].identifier) == 0 {
			return nil, fmt.Errorf("document %d has no identifier: %w", outputNumber, ErrInvalidGeneration)
		}
		for _, value := range pending[documentNumber].stored {
			if _, fieldErr := m.field(value.name); fieldErr != nil {
				return nil, fieldErr
			}
		}
		mapping[documentNumber] = outputNumber
		m.documents = append(m.documents, pending[documentNumber])
		survivors++
	}
	if survivors > 0 {
		for _, name := range input.IndexedFields {
			if name == identifierField {
				continue
			}
			if _, fieldErr := m.field(name); fieldErr != nil {
				return nil, fieldErr
			}
		}
	}

	fields, fieldsErr := reader.Fields()
	if fieldsErr != nil {
		return nil, fieldsErr
	}
	for _, name := range fields {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var field *nativeICEField
		// "_id" terms are rebuilt fresh from the merged documents' identifiers
		// below (one term per document, trivial to regenerate), so only the
		// VisitTermPostings splice is skipped for it; its doc values -- written
		// when OwnerOptions.IdentifierDocValues is set -- still flow through
		// the same VisitFieldDocumentValues splice as any other field.
		if name != identifierField {
			//nolint:contextcheck // cancellation is checked per term in the callback.
			if visitErr := reader.VisitTermPostings(name, nil, func(term []byte, documents []uint64, frequencies []TermFrequency) error {
				if err := ctx.Err(); err != nil {
					return err
				}
				aligned := len(frequencies) == len(documents)
				var frequencyByDocument map[uint64]uint64
				termKey := ""
				for documentIndex, documentNumber := range documents {
					if documentNumber >= physicalCount {
						return fmt.Errorf("term %q has document %d outside segment: %w", term, documentNumber, ErrCorrupt)
					}
					outputNumber := mapping[documentNumber]
					if outputNumber == DroppedDocumentNumber {
						continue
					}
					var frequency uint64
					switch {
					case aligned && frequencies[documentIndex].DocumentNumber == documentNumber:
						frequency = frequencies[documentIndex].Frequency
					default:
						if frequencyByDocument == nil {
							frequencyByDocument = make(map[uint64]uint64, len(frequencies))
							for _, entry := range frequencies {
								frequencyByDocument[entry.DocumentNumber] = entry.Frequency
							}
						}
						frequency = frequencyByDocument[documentNumber]
					}
					if field == nil {
						var fieldErr error
						if field, fieldErr = m.field(name); fieldErr != nil {
							return fieldErr
						}
					}
					if termKey == "" {
						termKey = string(term)
					}
					registerNativeICETermKey(field, termKey, outputNumber, frequency)
				}
				return nil
			}); visitErr != nil {
				return nil, visitErr
			}
		}
		if visitErr := reader.VisitFieldDocumentValues(name, func(documentNumber uint64, values [][]byte) error {
			if documentNumber >= physicalCount {
				return fmt.Errorf("doc value for field %q has document %d outside segment: %w", name, documentNumber, ErrCorrupt)
			}
			outputNumber := mapping[documentNumber]
			if outputNumber == DroppedDocumentNumber || len(values) == 0 {
				return ctx.Err()
			}
			if field == nil {
				var fieldErr error
				if field, fieldErr = m.field(name); fieldErr != nil {
					return fieldErr
				}
			}
			for _, value := range values {
				field.sortValues[outputNumber] = append(field.sortValues[outputNumber], append([]byte(nil), value...))
			}
			field.documents.add(outputNumber)
			return ctx.Err()
		}); visitErr != nil {
			return nil, visitErr
		}
	}
	return mapping, nil
}
