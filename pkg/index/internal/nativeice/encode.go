// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
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
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"sync"

	roaringpkg "github.com/RoaringBitmap/roaring"
	"github.com/blevesearch/vellum"
	"github.com/klauspost/compress/s2"
)

// identifierField is the name ICE v3 reserves for the document identifier, the
// field every segment records first. Encode writes each document's Identifier
// under this name, so a walk yields it ahead of the document's other stored
// values and a selection resolves it like any other exact term.
const identifierField = "_id"

// ErrInvalidGeneration is the sentinel reported when a caller asks Encode to
// write a generation the ICE v3 grammar has no representation for: a document
// carrying no identifier, or a field carrying no name. It is deliberately
// distinct from ErrCorrupt -- nothing on disk is damaged, and nothing on disk
// is touched, because the request is rejected before the first byte is written.
// Callers classify with errors.Is.
var ErrInvalidGeneration = errors.New("nativeice: invalid generation")

// EncodeField is one value a document contributes to a generation. The same
// name may appear on several fields of one document; each contributes its own
// value, and Encode preserves them in the order the document lists them.
//
// Value is an opaque byte sequence. Encode applies no analysis, no
// normalization and no numeric coding to it, so a term selects, sorts and
// reads back as exactly the bytes the caller supplied -- which is the same
// contract Reader.VisitSelectedDocuments resolves terms under.
type EncodeField struct {
	// Name is the segment field name the value is recorded under.
	Name string
	// Value is the field's raw bytes.
	Value []byte
	// Terms, when non-nil, are the analyzed terms recorded for this value.
	// A nil slice retains the compatibility behavior of indexing Value as one
	// term; a non-nil empty slice deliberately records no terms.
	Terms []EncodeTerm
	// Index records Value as a term in Name's dictionary, so a selection on
	// Name and Value reaches the document.
	Index bool
	// Store records Value as a stored value, so a document walk yields it.
	Store bool
	// Sort records Value as a doc value, so a repair page may sort on Name.
	Sort bool
}

// EncodeTerm is one analyzed term and its occurrence frequency. Multi-hit
// postings retain the ICE frequency stream even when every frequency is one.
type EncodeTerm struct {
	Value     []byte
	Frequency uint64
}

// EncodeDocument is one physical document of a generation.
type EncodeDocument struct {
	// Fields are the document's values, in the order the document records them.
	Fields []EncodeField
	// Identifier is the document's identity. Encode records it as an indexed,
	// stored value under the reserved identifier field.
	Identifier []byte
	// Deleted marks the document as covered by the generation's deletion masks.
	// It still occupies a document number in the segment, and it is absent from
	// every count, walk, selection and repair page the generation serves.
	Deleted bool
}

// Generation is one committed ICE v3 generation: the documents it holds and the
// identifiers its segment and snapshot manifest are numbered by.
type Generation struct {
	// Documents are the generation's physical documents, in the order they take
	// document numbers in the segment.
	Documents []EncodeDocument
	// SegmentID numbers the segment file the generation writes.
	SegmentID uint64
	// SnapshotID numbers the snapshot manifest that publishes the generation,
	// and is the identifier Reader.SnapshotID reports once it is opened.
	SnapshotID uint64
	// TimeMin and TimeMax are the segment's encoded timestamp bounds.
	TimeMin uint64
	TimeMax uint64
	// IdentifierDocValues additionally writes "_id" as a doc-value column, as
	// the previous release's writer does, so a rolled-back node can still
	// read document identity back from a segment this encoder wrote.
	IdentifierDocValues bool
}

// Encode writes generation into the index directory at path as one committed
// ICE v3 generation -- a segment holding every document, and a snapshot
// manifest referencing that segment and carrying its deletion masks -- and
// leaves the directory's existing generations untouched. Open selects the
// written generation when its snapshot identifier is the directory's newest.
//
// The generation is published by its snapshot manifest, so the segment is
// complete on disk before any manifest names it. A write interrupted before
// publication therefore leaves the directory's committed state exactly as it
// was: Open still selects the newest generation that was published, or reports
// ErrNoSnapshot when none ever was.
//
// Every field a document marks Index contributes its value as a term to that
// field's dictionary, every field it marks Store contributes a stored value,
// and every field it marks Sort contributes a doc value; one field may do all
// three. Within a document, a walk yields the identifier's stored value first
// and the remaining names in ascending byte order, with the values of a
// repeated name consecutive and in the order the document lists them.
//
// The snapshot manifest carries a calculated CRC32, because the pinned
// compatibility reader validates it: that reader's default configuration turns
// snapshot CRC validation on, and it rejects a manifest whose trailing four
// bytes are not the IEEE CRC32 of everything before them. The segment footer's
// CRC32 field is a different case and stays reserved -- no reader validates it,
// so Encode writes the field without calculating it.
//
// A generation whose documents the ICE v3 grammar cannot represent is rejected
// with an error wrapping ErrInvalidGeneration before the directory is touched.
func Encode(path string, generation Generation) error {
	if validationErr := validateGeneration(generation); validationErr != nil {
		return validationErr
	}
	segmentPayload, deletionBitmap, segmentErr := encodeNativeSegment(generation)
	if segmentErr != nil {
		return segmentErr
	}
	return PublishSnapshot(path, generation.SnapshotID, []SnapshotSegmentPayload{{
		SnapshotSegment: SnapshotSegment{
			ID: generation.SegmentID, Size: uint64(len(segmentPayload)),
			DocumentCount: uint64(len(generation.Documents)), DeletionBitmap: deletionBitmap,
		},
		Payload: segmentPayload,
	}})
}

// EncodeSegment returns the native ICE segment bytes for generation without
// publishing a snapshot. Callers that own a segment lifecycle can persist
// these bytes through their own WriteTo seam and publish them separately.
func EncodeSegment(generation Generation) ([]byte, error) {
	if generationErr := validateGeneration(generation); generationErr != nil {
		return nil, generationErr
	}
	segmentPayload, _, encodeErr := encodeNativeSegment(generation)
	if encodeErr != nil {
		return nil, encodeErr
	}
	return segmentPayload, nil
}

func validateGeneration(generation Generation) error {
	for documentIndex, document := range generation.Documents {
		if len(document.Identifier) == 0 {
			return fmt.Errorf("document %d has no identifier: %w", documentIndex, ErrInvalidGeneration)
		}
		for fieldIndex, field := range document.Fields {
			if field.Name == "" {
				return fmt.Errorf("document %d field %d has no name: %w", documentIndex, fieldIndex, ErrInvalidGeneration)
			}
		}
	}
	return nil
}

const (
	nativeICEOneHitNorm  uint64 = 1
	nativeICEChunkModeV1 uint32 = 1025
)

// nativeICEField is one field's derived index state. termFrequencies[term] is
// aligned with termDocuments[term]: documents register terms in ascending
// document order, and every registration of one term by one document is
// consecutive, so a document's frequency always accumulates into the last
// entry.
type nativeICEField struct {
	sortValues      map[uint64][][]byte
	termDocuments   map[string][]uint64
	termFrequencies map[string][]uint64
	name            string
	documents       documentSet
	documentCount   uint64
	frequency       uint64
}

func encodeNativeSegment(generation Generation) ([]byte, []byte, error) {
	fields := nativeICEFields(generation)
	fieldIDs := nativeICEFieldIDs(fields)
	storedData, documentOffsets := encodeStoredDocuments(generation, fieldIDs)
	segment, assembleErr := assembleNativeSegment(fields, storedData, documentOffsets,
		uint64(len(generation.Documents)), generation.TimeMin, generation.TimeMax)
	if assembleErr != nil {
		return nil, nil, assembleErr
	}
	deletionBitmap, deletionErr := encodeDeletionBitmap(generation.Documents)
	if deletionErr != nil {
		return nil, nil, deletionErr
	}
	return segment, deletionBitmap, nil
}

func nativeICEFieldIDs(fields []nativeICEField) map[string]uint64 {
	fieldIDs := make(map[string]uint64, len(fields))
	for fieldIndex, field := range fields {
		fieldIDs[field.name] = uint64(fieldIndex)
	}
	return fieldIDs
}

// assembleNativeSegment serializes already-derived field structures and
// stored documents into segment bytes. Both EncodeSegment and MergeSegments
// end here, so a merge that derives the same fields and stored documents
// produces the same bytes as re-encoding the merged documents.
func assembleNativeSegment(
	fields []nativeICEField, storedData []byte, documentOffsets []uint64, documentCount, timeMin, timeMax uint64,
) ([]byte, error) {
	segment := make([]byte, 0, len(storedData)+len(documentOffsets)*storedDocumentOffsetByteWidth+segmentFooterLength)
	segment = append(segment, storedData...)
	storedIndexOffset := uint64(len(segment))
	for _, documentOffset := range documentOffsets {
		segment = appendNativeUint64(segment, documentOffset)
	}
	docValueStarts := make([]uint64, len(fields))
	docValueEnds := make([]uint64, len(fields))
	hasDocValues := false
	for fieldIndex, field := range fields {
		if len(field.sortValues) == 0 {
			docValueStarts[fieldIndex] = math.MaxUint64
			docValueEnds[fieldIndex] = math.MaxUint64
			continue
		}
		hasDocValues = true
		docValueStarts[fieldIndex] = uint64(len(segment))
		segment = append(segment, encodeNativeICEDocValues(documentCount, field.sortValues)...)
		docValueEnds[fieldIndex] = uint64(len(segment))
	}
	fieldOffsets := make([]uint64, len(fields))
	for fieldIndex, field := range fields {
		var dictionaryOffset uint64
		var termsErr error
		segment, dictionaryOffset, termsErr = appendNativeICETerms(segment, field, documentCount)
		if termsErr != nil {
			return nil, termsErr
		}
		fieldOffsets[fieldIndex] = uint64(len(segment))
		segment = appendNativeUvarint(segment, dictionaryOffset)
		segment = appendNativeUvarint(segment, uint64(len(field.name)))
		segment = append(segment, field.name...)
		segment = appendNativeUvarint(segment, field.documentCount)
		segment = appendNativeUvarint(segment, field.frequency)
	}
	var docValueOffset uint64 = math.MaxUint64
	if hasDocValues {
		docValueOffset = uint64(len(segment))
		for fieldIndex := range fields {
			segment = appendNativeUvarint(segment, docValueStarts[fieldIndex])
			segment = appendNativeUvarint(segment, docValueEnds[fieldIndex])
		}
	}
	fieldsIndexOffset := uint64(len(segment))
	for _, fieldOffset := range fieldOffsets {
		segment = appendNativeUint64(segment, fieldOffset)
	}
	footer := make([]byte, segmentFooterLength)
	binary.BigEndian.PutUint64(footer[0:8], documentCount)
	binary.BigEndian.PutUint64(footer[8:16], storedIndexOffset)
	binary.BigEndian.PutUint64(footer[16:24], fieldsIndexOffset)
	binary.BigEndian.PutUint64(footer[24:32], docValueOffset)
	binary.BigEndian.PutUint32(footer[32:36], nativeICEChunkModeV1)
	binary.BigEndian.PutUint64(footer[36:44], timeMin)
	binary.BigEndian.PutUint64(footer[44:52], timeMax)
	binary.BigEndian.PutUint32(footer[52:56], segmentVersion)
	return append(segment, footer...), nil
}

func encodeNativeICEDocValues(documentCount uint64, sortValues map[uint64][][]byte) []byte {
	chunkCount := (documentCount + docValueDocumentsPerChunk - 1) / docValueDocumentsPerChunk
	chunkOffsets := make([]uint64, chunkCount)
	encoded := make([]byte, 0)
	for chunkIndex := uint64(0); chunkIndex < chunkCount; chunkIndex++ {
		encoded = append(encoded, encodeNativeICEDocValueChunk(chunkIndex, documentCount, sortValues)...)
		chunkOffsets[chunkIndex] = uint64(len(encoded))
	}
	chunkTable := make([]byte, 0, len(chunkOffsets)*binary.MaxVarintLen64)
	for _, chunkOffset := range chunkOffsets {
		chunkTable = appendNativeUvarint(chunkTable, chunkOffset)
	}
	encoded = append(encoded, chunkTable...)
	encoded = appendNativeUint64(encoded, uint64(len(chunkTable)))
	return appendNativeUint64(encoded, chunkCount)
}

func encodeNativeICEDocValueChunk(chunkIndex, documentCount uint64, sortValues map[uint64][][]byte) []byte {
	firstDocument := chunkIndex * docValueDocumentsPerChunk
	lastDocument := firstDocument + docValueDocumentsPerChunk
	if lastDocument > documentCount {
		lastDocument = documentCount
	}
	type documentValues struct {
		documentNumber uint64
		valueEnd       uint64
	}
	values := make([]byte, 0)
	header := make([]documentValues, 0)
	for documentNumber := firstDocument; documentNumber < lastDocument; documentNumber++ {
		documentTerms, found := sortValues[documentNumber]
		if !found {
			continue
		}
		for _, value := range documentTerms {
			values = appendNativeICEDocValueTerm(values, value)
		}
		header = append(header, documentValues{documentNumber: documentNumber, valueEnd: uint64(len(values))})
	}
	if len(header) == 0 {
		return nil
	}
	encoded := make([]byte, 0, len(header)*2*binary.MaxVarintLen64+len(values))
	encoded = appendNativeUvarint(encoded, uint64(len(header)))
	var previousDocument, previousValueEnd uint64
	for _, value := range header {
		encoded = appendNativeUvarint(encoded, value.documentNumber-previousDocument)
		encoded = appendNativeUvarint(encoded, value.valueEnd-previousValueEnd)
		previousDocument = value.documentNumber
		previousValueEnd = value.valueEnd
	}
	return append(encoded, s2.Encode(nil, values)...)
}

func appendNativeICEDocValueTerm(destination, value []byte) []byte {
	for _, valueByte := range value {
		if valueByte == 0xff || valueByte == 0x5c {
			destination = append(destination, 0x5c)
		}
		destination = append(destination, valueByte)
	}
	return append(destination, 0xff)
}

func nativeICEFields(generation Generation) []nativeICEField {
	fieldsByName := make(map[string]*nativeICEField)
	identifier := nativeICEFieldFor(fieldsByName, identifierField)
	for documentIndex, document := range generation.Documents {
		documentNumber := uint64(documentIndex)
		registerNativeICETerm(identifier, document.Identifier, documentNumber, 1)
		if generation.IdentifierDocValues {
			identifier.sortValues[documentNumber] = append(identifier.sortValues[documentNumber], document.Identifier)
		}
		for _, field := range document.Fields {
			if !field.Store && !field.Index && !field.Sort {
				continue
			}
			nativeField := nativeICEFieldFor(fieldsByName, field.Name)
			if field.Index {
				if field.Terms == nil {
					registerNativeICETerm(nativeField, field.Value, documentNumber, 1)
				} else {
					for _, term := range field.Terms {
						registerNativeICETerm(nativeField, term.Value, documentNumber, term.Frequency)
					}
				}
			}
			if field.Sort {
				// Doc-value-only fields still need a document cardinality in
				// their field footer; otherwise readers discard their values as
				// an empty field even though the doc-value section is present.
				nativeField.documents.add(documentNumber)
				nativeField.sortValues[documentNumber] = append(nativeField.sortValues[documentNumber], field.Value)
			}
		}
	}
	return orderNativeICEFields(fieldsByName, uint64(len(generation.Documents)))
}

// orderNativeICEFields fixes field order and per-field document counts:
// the identifier first, then every other field in ascending name order.
func orderNativeICEFields(fieldsByName map[string]*nativeICEField, documentCount uint64) []nativeICEField {
	fieldNames := make([]string, 0, len(fieldsByName))
	for fieldName := range fieldsByName {
		if fieldName == identifierField {
			continue
		}
		fieldNames = append(fieldNames, fieldName)
	}
	sort.Strings(fieldNames)
	// ICE's field table reserves the first field slot for the identifier. The
	// legacy merge fast path preserves field IDs when all inputs have the same
	// field order, then reconstructs the output with _id first. Keep native
	// segments in that same canonical order or a later legacy merge can attach
	// stored values to the wrong field names.
	fields := make([]nativeICEField, 0, len(fieldNames)+1)
	identifierNativeField := fieldsByName[identifierField]
	identifierNativeField.documentCount = documentCount
	fields = append(fields, *identifierNativeField)
	for _, fieldName := range fieldNames {
		nativeField := fieldsByName[fieldName]
		nativeField.documentCount = nativeField.documents.count
		fields = append(fields, *nativeField)
	}
	return fields
}

func nativeICEFieldFor(fieldsByName map[string]*nativeICEField, name string) *nativeICEField {
	if field, found := fieldsByName[name]; found {
		return field
	}
	field := &nativeICEField{
		name:            name,
		sortValues:      make(map[uint64][][]byte),
		termDocuments:   make(map[string][]uint64),
		termFrequencies: make(map[string][]uint64),
	}
	fieldsByName[name] = field
	return field
}

func registerNativeICETerm(field *nativeICEField, value []byte, documentNumber uint64, frequency uint64) {
	registerNativeICETermKey(field, string(value), documentNumber, frequency)
}

func registerNativeICETermKey(field *nativeICEField, term string, documentNumber uint64, frequency uint64) {
	if frequency == 0 {
		frequency = 1
	}
	documents := field.termDocuments[term]
	frequencies := field.termFrequencies[term]
	if last := len(documents) - 1; last >= 0 && documents[last] == documentNumber {
		frequencies[last] += frequency
	} else {
		documents = append(documents, documentNumber)
		frequencies = append(frequencies, frequency)
		field.termDocuments[term] = documents
	}
	field.termFrequencies[term] = frequencies
	field.documents.add(documentNumber)
	field.frequency += frequency
}

// documentSet counts the distinct documents a field covers.
type documentSet struct {
	words []uint64
	count uint64
}

func (s *documentSet) add(documentNumber uint64) {
	word := documentNumber / 64
	if word >= uint64(len(s.words)) {
		grown := make([]uint64, word+1, (word+1)*2)
		copy(grown, s.words)
		s.words = grown
	}
	bit := uint64(1) << (documentNumber % 64)
	if s.words[word]&bit == 0 {
		s.words[word] |= bit
		s.count++
	}
}

func appendNativeICETerms(segment []byte, field nativeICEField, documentCount uint64) ([]byte, uint64, error) {
	if len(field.termDocuments) == 0 {
		return segment, 0, nil
	}
	terms := make([]string, 0, len(field.termDocuments))
	for term := range field.termDocuments {
		terms = append(terms, term)
	}
	sort.Strings(terms)
	values := make(map[string]uint64, len(terms))
	for _, term := range terms {
		documents := field.termDocuments[term]
		frequencies := field.termFrequencies[term]
		allFrequencyOne := true
		for _, frequency := range frequencies {
			if frequency != 1 {
				allFrequencyOne = false
				break
			}
		}
		if len(documents) == 1 && allFrequencyOne && documents[0] <= fstValueDocumentMask {
			values[term] = fstValueEncodingOneHit | (nativeICEOneHitNorm << 31) | documents[0]
			continue
		}
		var postingsOffset uint64
		var postingsErr error
		includeFrequency := len(documents) > 1 || !allFrequencyOne
		segment, postingsOffset, postingsErr = appendNativeICEPosting(segment, documents, frequencies, documentCount, includeFrequency)
		if postingsErr != nil {
			return nil, 0, postingsErr
		}
		values[term] = postingsOffset
	}
	dictionaryOffset := uint64(len(segment))
	var dictionary bytes.Buffer
	builderClass := dictionaryBuilderClass(len(terms))
	builder, builderErr := acquireDictionaryBuilder(&dictionary, builderClass)
	if builderErr != nil {
		return nil, 0, fmt.Errorf("create term dictionary for field %q: %w", field.name, builderErr)
	}
	for _, term := range terms {
		if insertErr := builder.Insert([]byte(term), values[term]); insertErr != nil {
			return nil, 0, errors.Join(fmt.Errorf("insert term into dictionary for field %q: %w", field.name, insertErr), builder.Close())
		}
	}
	if closeErr := builder.Close(); closeErr != nil {
		return nil, 0, fmt.Errorf("close term dictionary for field %q: %w", field.name, closeErr)
	}
	dictionaryBuilderClasses[builderClass].pool.Put(builder)
	segment = appendNativeUvarint(segment, uint64(dictionary.Len()))
	segment = append(segment, dictionary.Bytes()...)
	return segment, dictionaryOffset, nil
}

// dictionaryBuilderClasses size vellum's suffix-sharing registry to the
// dictionary being built. The default 10,000x2-cell table costs ~30us to clear
// per Reset (~135us to allocate per vellum.New), which dominated encoding the
// one-term dictionaries of every single-document admission and small merge.
// The registry only affects how much suffix sharing the encoder finds, never
// decodability, and above the largest class the default is kept so large
// dictionaries encode exactly as before. Builders are pooled per class and
// returned only after a successful Close.
var dictionaryBuilderClasses = []struct {
	opts     *vellum.BuilderOpts
	pool     sync.Pool
	maxTerms int
}{
	{maxTerms: 32, opts: &vellum.BuilderOpts{Encoder: 1, RegistryTableSize: 64, RegistryMRUSize: 2}},
	{maxTerms: 1024, opts: &vellum.BuilderOpts{Encoder: 1, RegistryTableSize: 2048, RegistryMRUSize: 2}},
	{maxTerms: math.MaxInt, opts: nil},
}

func dictionaryBuilderClass(termCount int) int {
	for index := range dictionaryBuilderClasses {
		if termCount <= dictionaryBuilderClasses[index].maxTerms {
			return index
		}
	}
	return len(dictionaryBuilderClasses) - 1
}

func acquireDictionaryBuilder(w io.Writer, class int) (*vellum.Builder, error) {
	if builder, ok := dictionaryBuilderClasses[class].pool.Get().(*vellum.Builder); ok {
		if resetErr := builder.Reset(w); resetErr != nil {
			return nil, resetErr
		}
		return builder, nil
	}
	return vellum.New(w, dictionaryBuilderClasses[class].opts)
}

func appendNativeICEPosting(segment []byte, documents, frequencies []uint64, documentCount uint64, includeFrequency bool) ([]byte, uint64, error) {
	postings := roaringpkg.New()
	for _, documentNumber := range documents {
		if documentNumber > math.MaxUint32 {
			return nil, 0, fmt.Errorf("document %d exceeds the posting range: %w", documentNumber, ErrInvalidGeneration)
		}
		postings.Add(uint32(documentNumber))
	}
	payload, marshalErr := postings.MarshalBinary()
	if marshalErr != nil {
		return nil, 0, fmt.Errorf("encode posting bitmap: %w", marshalErr)
	}
	frequencyOffset := uint64(0)
	postingOffset := uint64(len(segment))
	if includeFrequency {
		frequencyStream, streamErr := encodeNativeICEFrequencyStream(documents, frequencies, documentCount)
		if streamErr != nil {
			return nil, 0, streamErr
		}
		frequencyOffset = postingOffset
		segment = append(segment, frequencyStream...)
		postingOffset = uint64(len(segment))
	}
	segment = appendNativeUvarint(segment, frequencyOffset)
	segment = appendNativeUvarint(segment, 0)
	segment = appendNativeUvarint(segment, uint64(len(payload)))
	return append(segment, payload...), postingOffset, nil
}

func encodeNativeICEFrequencyStream(documents, frequencies []uint64, documentCount uint64) ([]byte, error) {
	if len(documents) == 0 || documentCount == 0 {
		return nil, fmt.Errorf("frequency stream has no documents: %w", ErrInvalidGeneration)
	}
	chunkSize, chunkErr := nativeICEChunkSize(nativeICEChunkModeV1, uint64(len(documents)), documentCount)
	if chunkErr != nil {
		return nil, chunkErr
	}
	chunkCount := (documentCount-1)/chunkSize + 1
	chunkLengths := make([]uint64, chunkCount)
	var final []byte
	var chunkData []byte
	currentChunk := uint64(0)
	for documentIndex, document := range documents {
		chunk := document / chunkSize
		if chunk >= chunkCount {
			return nil, fmt.Errorf("document %d exceeds frequency chunk range: %w", document, ErrInvalidGeneration)
		}
		if chunk != currentChunk {
			compressed := s2.EncodeBetter(nil, chunkData)
			chunkLengths[currentChunk] = uint64(len(compressed))
			final = append(final, compressed...)
			chunkData = chunkData[:0]
			currentChunk = chunk
		}
		frequency := frequencies[documentIndex]
		if frequency == 0 {
			return nil, fmt.Errorf("document %d has no frequency: %w", document, ErrInvalidGeneration)
		}
		if frequency > ^uint64(0)>>1 {
			return nil, fmt.Errorf("document %d has an unrepresentable frequency: %w", document, ErrInvalidGeneration)
		}
		chunkData = appendNativeUvarint(chunkData, frequency<<1)
		chunkData = appendNativeUvarint(chunkData, uint64(math.Float32bits(1)))
	}
	compressed := s2.EncodeBetter(nil, chunkData)
	chunkLengths[currentChunk] = uint64(len(compressed))
	final = append(final, compressed...)
	result := appendNativeUvarint(nil, chunkCount)
	var cumulative uint64
	for _, length := range chunkLengths {
		cumulative += length
		result = appendNativeUvarint(result, cumulative)
	}
	return append(result, final...), nil
}

func nativeICEChunkSize(chunkMode uint32, cardinality, maxDocuments uint64) (uint64, error) {
	switch {
	case chunkMode <= 1024:
		if chunkMode == 0 {
			return 0, fmt.Errorf("zero frequency chunk mode: %w", ErrCorrupt)
		}
		return uint64(chunkMode), nil
	case chunkMode == nativeICEChunkModeV1:
		numChunks := cardinality/1024 + 1
		chunkSize := maxDocuments / numChunks
		if chunkSize == 0 {
			return 0, fmt.Errorf("zero frequency chunk size: %w", ErrCorrupt)
		}
		return chunkSize, nil
	default:
		return 0, fmt.Errorf("unknown frequency chunk mode %d: %w", chunkMode, ErrCorrupt)
	}
}

func encodeStoredDocuments(generation Generation, fieldIDs map[string]uint64) ([]byte, []uint64) {
	return encodeStoredChunks(len(generation.Documents), func(documentIndex int, destination []byte) []byte {
		document := generation.Documents[documentIndex]
		values := make([]storedValue, 0, len(document.Fields)+1)
		values = append(values, storedValue{name: identifierField, value: document.Identifier})
		for _, field := range document.Fields {
			if field.Store {
				values = append(values, storedValue{name: field.Name, value: field.Value})
			}
		}
		return appendStoredDocument(destination, values, fieldIDs)
	})
}

// encodeStoredChunks lays documentCount stored documents, produced by
// appendDocument, into s2-compressed chunks with their offset tables.
func encodeStoredChunks(documentCount int, appendDocument func(documentIndex int, destination []byte) []byte) ([]byte, []uint64) {
	documentOffsets := make([]uint64, documentCount)
	chunkOffsets := []uint64{0}
	encoded := make([]byte, 0)
	decodedChunk := make([]byte, 0)
	for firstDocument := 0; firstDocument < documentCount; firstDocument += storedDocumentsPerChunk {
		lastDocument := firstDocument + storedDocumentsPerChunk
		if lastDocument > documentCount {
			lastDocument = documentCount
		}
		decodedChunk = decodedChunk[:0]
		for documentIndex := firstDocument; documentIndex < lastDocument; documentIndex++ {
			documentOffsets[documentIndex] = uint64(len(decodedChunk))
			decodedChunk = appendDocument(documentIndex, decodedChunk)
		}
		encoded = append(encoded, s2.Encode(nil, decodedChunk)...)
		chunkOffsets = append(chunkOffsets, uint64(len(encoded)))
	}
	chunkTable := make([]byte, 0, len(chunkOffsets)*binary.MaxVarintLen64)
	for _, chunkOffset := range chunkOffsets {
		chunkTable = appendNativeUvarint(chunkTable, chunkOffset)
	}
	encoded = append(encoded, chunkTable...)
	encoded = appendNativeUint32(encoded, uint32(len(chunkTable)))
	return appendNativeUint32(encoded, uint32(len(chunkOffsets))), documentOffsets
}

type storedValue struct {
	name  string
	value []byte
}

// appendStoredDocument appends one stored document. values[0] must be the
// identifier; the remaining values are ordered by name, stably, so repeated
// values of one name keep the order the document lists them in.
func appendStoredDocument(destination []byte, values []storedValue, fieldIDs map[string]uint64) []byte {
	sort.SliceStable(values[1:], func(leftIndex, rightIndex int) bool {
		return values[leftIndex+1].name < values[rightIndex+1].name
	})
	meta := make([]byte, 0, len(values)*3)
	var dataLength uint64
	for _, value := range values {
		meta = appendNativeUvarint(meta, fieldIDs[value.name])
		meta = appendNativeUvarint(meta, dataLength)
		meta = appendNativeUvarint(meta, uint64(len(value.value)))
		dataLength += uint64(len(value.value))
	}
	destination = appendNativeUvarint(destination, uint64(len(meta)))
	destination = appendNativeUvarint(destination, dataLength)
	destination = append(destination, meta...)
	for _, value := range values {
		destination = append(destination, value.value...)
	}
	return destination
}

func encodeDeletionBitmap(documents []EncodeDocument) ([]byte, error) {
	deleted := roaringpkg.New()
	hasDeletedDocument := false
	for documentIndex, document := range documents {
		if !document.Deleted {
			continue
		}
		if uint64(documentIndex) > math.MaxUint32 {
			return nil, fmt.Errorf("document %d exceeds the deletion-mask range: %w", documentIndex, ErrInvalidGeneration)
		}
		deleted.Add(uint32(documentIndex))
		hasDeletedDocument = true
	}
	if !hasDeletedDocument {
		return nil, nil
	}
	payload, marshalErr := deleted.MarshalBinary()
	if marshalErr != nil {
		return nil, fmt.Errorf("encode deletion bitmap: %w", marshalErr)
	}
	return payload, nil
}

func appendNativeUvarint(destination []byte, value uint64) []byte {
	var encoded [binary.MaxVarintLen64]byte
	encodedLength := binary.PutUvarint(encoded[:], value)
	return append(destination, encoded[:encodedLength]...)
}

func appendNativeUint32(destination []byte, value uint32) []byte {
	var encoded [4]byte
	binary.BigEndian.PutUint32(encoded[:], value)
	return append(destination, encoded[:]...)
}

func appendNativeUint64(destination []byte, value uint64) []byte {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	return append(destination, encoded[:]...)
}

func nativeICEFileName(identifier uint64, extension string) string {
	return fmt.Sprintf("%012x%s", identifier, extension)
}
