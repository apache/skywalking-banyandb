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
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"

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
	// Index records Value as a term in Name's dictionary, so a selection on
	// Name and Value reaches the document.
	Index bool
	// Store records Value as a stored value, so a document walk yields it.
	Store bool
	// Sort records Value as a doc value, so a repair page may sort on Name.
	Sort bool
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
	if directoryErr := os.MkdirAll(path, 0o755); directoryErr != nil {
		return fmt.Errorf("create index directory %q: %w", path, directoryErr)
	}
	segmentName := nativeICEFileName(generation.SegmentID, ".seg")
	if publishErr := publishNativeICEFile(path, segmentName, segmentPayload); publishErr != nil {
		return fmt.Errorf("publish segment %q: %w", segmentName, publishErr)
	}
	manifestPayload := encodeNativeSnapshot(generation, uint64(len(segmentPayload)), deletionBitmap)
	manifestName := nativeICEFileName(generation.SnapshotID, ".snp")
	if publishErr := publishNativeICEFile(path, manifestName, manifestPayload); publishErr != nil {
		return fmt.Errorf("publish snapshot %q: %w", manifestName, publishErr)
	}
	return nil
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

const nativeICEOneHitNorm uint64 = 1

type nativeICEField struct {
	documentNumbers map[uint64]struct{}
	sortValues      map[uint64][][]byte
	termDocuments   map[string][]uint64
	name            string
	documentCount   uint64
	frequency       uint64
}

func encodeNativeSegment(generation Generation) ([]byte, []byte, error) {
	fields := nativeICEFields(generation)
	fieldIDs := make(map[string]uint64, len(fields))
	for fieldIndex, field := range fields {
		fieldIDs[field.name] = uint64(fieldIndex)
	}
	storedData, documentOffsets := encodeStoredDocuments(generation, fieldIDs)
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
		segment = append(segment, encodeNativeICEDocValues(uint64(len(generation.Documents)), field.sortValues)...)
		docValueEnds[fieldIndex] = uint64(len(segment))
	}
	fieldOffsets := make([]uint64, len(fields))
	for fieldIndex, field := range fields {
		var dictionaryOffset uint64
		var termsErr error
		segment, dictionaryOffset, termsErr = appendNativeICETerms(segment, field)
		if termsErr != nil {
			return nil, nil, termsErr
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
	binary.BigEndian.PutUint64(footer[0:8], uint64(len(generation.Documents)))
	binary.BigEndian.PutUint64(footer[8:16], storedIndexOffset)
	binary.BigEndian.PutUint64(footer[16:24], fieldsIndexOffset)
	binary.BigEndian.PutUint64(footer[24:32], docValueOffset)
	binary.BigEndian.PutUint32(footer[32:36], 1)
	binary.BigEndian.PutUint32(footer[52:56], segmentVersion)
	segment = append(segment, footer...)
	deletionBitmap, deletionErr := encodeDeletionBitmap(generation.Documents)
	if deletionErr != nil {
		return nil, nil, deletionErr
	}
	return segment, deletionBitmap, nil
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
		registerNativeICETerm(identifier, document.Identifier, documentNumber)
		for _, field := range document.Fields {
			if !field.Store && !field.Index && !field.Sort {
				continue
			}
			nativeField := nativeICEFieldFor(fieldsByName, field.Name)
			if field.Index {
				registerNativeICETerm(nativeField, field.Value, documentNumber)
			}
			if field.Sort {
				nativeField.sortValues[documentNumber] = append(nativeField.sortValues[documentNumber], field.Value)
			}
		}
	}
	fieldNames := make([]string, 0, len(fieldsByName))
	for fieldName := range fieldsByName {
		fieldNames = append(fieldNames, fieldName)
	}
	sort.Strings(fieldNames)
	fields := make([]nativeICEField, 0, len(fieldNames))
	for _, fieldName := range fieldNames {
		nativeField := fieldsByName[fieldName]
		nativeField.documentCount = uint64(len(nativeField.documentNumbers))
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
		documentNumbers: make(map[uint64]struct{}),
		sortValues:      make(map[uint64][][]byte),
		termDocuments:   make(map[string][]uint64),
	}
	fieldsByName[name] = field
	return field
}

func registerNativeICETerm(field *nativeICEField, value []byte, documentNumber uint64) {
	term := string(value)
	documents := field.termDocuments[term]
	if len(documents) == 0 || documents[len(documents)-1] != documentNumber {
		field.termDocuments[term] = append(documents, documentNumber)
	}
	field.documentNumbers[documentNumber] = struct{}{}
	field.frequency++
}

func appendNativeICETerms(segment []byte, field nativeICEField) ([]byte, uint64, error) {
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
		if len(documents) == 1 && documents[0] <= fstValueDocumentMask {
			values[term] = fstValueEncodingOneHit | (nativeICEOneHitNorm << 31) | documents[0]
			continue
		}
		postingsOffset := uint64(len(segment))
		var postingsErr error
		segment, postingsErr = appendNativeICEPosting(segment, documents)
		if postingsErr != nil {
			return nil, 0, postingsErr
		}
		values[term] = postingsOffset
	}
	dictionaryOffset := uint64(len(segment))
	var dictionary bytes.Buffer
	builder, builderErr := vellum.New(&dictionary, nil)
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
	segment = appendNativeUvarint(segment, uint64(dictionary.Len()))
	segment = append(segment, dictionary.Bytes()...)
	return segment, dictionaryOffset, nil
}

func appendNativeICEPosting(segment []byte, documents []uint64) ([]byte, error) {
	postings := roaringpkg.New()
	for _, documentNumber := range documents {
		if documentNumber > math.MaxUint32 {
			return nil, fmt.Errorf("document %d exceeds the posting range: %w", documentNumber, ErrInvalidGeneration)
		}
		postings.Add(uint32(documentNumber))
	}
	payload, marshalErr := postings.MarshalBinary()
	if marshalErr != nil {
		return nil, fmt.Errorf("encode posting bitmap: %w", marshalErr)
	}
	segment = appendNativeUvarint(segment, 0)
	segment = appendNativeUvarint(segment, 0)
	segment = appendNativeUvarint(segment, uint64(len(payload)))
	return append(segment, payload...), nil
}

func encodeStoredDocuments(generation Generation, fieldIDs map[string]uint64) ([]byte, []uint64) {
	documentOffsets := make([]uint64, len(generation.Documents))
	chunkOffsets := []uint64{0}
	encoded := make([]byte, 0)
	for firstDocument := 0; firstDocument < len(generation.Documents); firstDocument += storedDocumentsPerChunk {
		lastDocument := firstDocument + storedDocumentsPerChunk
		if lastDocument > len(generation.Documents) {
			lastDocument = len(generation.Documents)
		}
		decodedChunk := make([]byte, 0)
		for documentIndex := firstDocument; documentIndex < lastDocument; documentIndex++ {
			documentOffsets[documentIndex] = uint64(len(decodedChunk))
			decodedChunk = append(decodedChunk, encodeStoredDocument(generation.Documents[documentIndex], fieldIDs)...)
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

func encodeStoredDocument(document EncodeDocument, fieldIDs map[string]uint64) []byte {
	type storedValue struct {
		name  string
		value []byte
	}
	values := []storedValue{{name: identifierField, value: document.Identifier}}
	for _, field := range document.Fields {
		if field.Store {
			values = append(values, storedValue{name: field.Name, value: field.Value})
		}
	}
	sort.SliceStable(values[1:], func(leftIndex, rightIndex int) bool {
		return values[leftIndex+1].name < values[rightIndex+1].name
	})
	meta := make([]byte, 0, len(values)*3)
	data := make([]byte, 0)
	for _, value := range values {
		meta = appendNativeUvarint(meta, fieldIDs[value.name])
		meta = appendNativeUvarint(meta, uint64(len(data)))
		meta = appendNativeUvarint(meta, uint64(len(value.value)))
		data = append(data, value.value...)
	}
	encoded := make([]byte, 0, len(meta)+len(data)+2*binary.MaxVarintLen64)
	encoded = appendNativeUvarint(encoded, uint64(len(meta)))
	encoded = appendNativeUvarint(encoded, uint64(len(data)))
	encoded = append(encoded, meta...)
	return append(encoded, data...)
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

func encodeNativeSnapshot(generation Generation, segmentSize uint64, deletionBitmap []byte) []byte {
	manifest := make([]byte, 0, 64+len(deletionBitmap))
	manifest = appendNativeUvarint(manifest, snapshotVersion)
	manifest = appendNativeUvarint(manifest, 1)
	manifest = appendNativeUvarint(manifest, uint64(len("ice")))
	manifest = append(manifest, "ice"...)
	manifest = appendNativeUint32(manifest, segmentVersion)
	manifest = appendNativeUvarint(manifest, generation.SegmentID)
	manifest = appendNativeUint64(manifest, segmentSize)
	manifest = appendNativeUint64(manifest, uint64(len(generation.Documents)))
	manifest = appendNativeUint64(manifest, 0)
	manifest = appendNativeUint64(manifest, 0)
	manifest = appendNativeUvarint(manifest, uint64(len(deletionBitmap)))
	manifest = append(manifest, deletionBitmap...)
	return appendNativeUint32(manifest, crc32.ChecksumIEEE(manifest))
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

func publishNativeICEFile(directory, name string, payload []byte) error {
	temporaryFile, createErr := os.CreateTemp(directory, ".nativeice-")
	if createErr != nil {
		return fmt.Errorf("create temporary file: %w", createErr)
	}
	temporaryPath := temporaryFile.Name()
	if writeErr := writeNativeICEFile(temporaryFile, payload); writeErr != nil {
		return errors.Join(fmt.Errorf("write temporary file: %w", writeErr), closeAndRemoveNativeICEFile(temporaryFile, temporaryPath))
	}
	if syncErr := temporaryFile.Sync(); syncErr != nil {
		return errors.Join(fmt.Errorf("sync temporary file: %w", syncErr), closeAndRemoveNativeICEFile(temporaryFile, temporaryPath))
	}
	if closeErr := temporaryFile.Close(); closeErr != nil {
		return errors.Join(fmt.Errorf("close temporary file: %w", closeErr), os.Remove(temporaryPath))
	}
	finalPath := filepath.Join(directory, name)
	if linkErr := os.Link(temporaryPath, finalPath); linkErr != nil {
		return errors.Join(fmt.Errorf("link temporary file as %q: %w", finalPath, linkErr), os.Remove(temporaryPath))
	}
	if removeErr := os.Remove(temporaryPath); removeErr != nil {
		return fmt.Errorf("remove temporary file %q: %w", temporaryPath, removeErr)
	}
	return syncNativeICEDirectory(directory)
}

func writeNativeICEFile(file *os.File, payload []byte) error {
	for len(payload) > 0 {
		written, writeErr := file.Write(payload)
		if writeErr != nil {
			return writeErr
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		payload = payload[written:]
	}
	return nil
}

func closeAndRemoveNativeICEFile(file *os.File, path string) error {
	return errors.Join(file.Close(), os.Remove(path))
}

func syncNativeICEDirectory(path string) error {
	directory, openErr := os.Open(path)
	if openErr != nil {
		return fmt.Errorf("open directory %q: %w", path, openErr)
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	return errors.Join(syncErr, closeErr)
}
