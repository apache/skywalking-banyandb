// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package nativeice

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"sync"

	roaringpkg "github.com/RoaringBitmap/roaring"
	"github.com/blevesearch/vellum"
	"github.com/klauspost/compress/s2"
)

const (
	maxSelectionTermCount      = 1 << 16
	maxSelectionTermLength     = 64 << 10
	maxSelectionDictionarySize = 64 << 20
	maxSelectionPostingsSize   = 64 << 20
	selectionPostingBatchSize  = 1024
	selectionDecodeReadSize    = 32 << 10
	maxFrequencyChunkCount     = 1 << 20
	maxFrequencyCompressedSize = 16 << 20
	maxFrequencyDecodedSize    = 64 << 20
	fstValueEncodingMask       = uint64(0xc000000000000000)
	fstValueEncodingOneHit     = uint64(0x8000000000000000)
	fstValueDocumentMask       = uint64(0x000000007fffffff)
)

// ErrInvalidSelection is the sentinel reported when a caller asks for a term
// selection the reader will not serve: one that names no field, that carries
// more terms than the reader's configured term-count bound, or that carries a
// term longer than its configured term-length bound.
//
// It is deliberately distinct from ErrCorrupt. Nothing on disk is damaged; the
// request itself lies outside the bounds BDB-NIDX-SPEC-001 revision 0.2
// READ-002 requires the reader to hold, so no dictionary is opened and no
// posting is decoded. Callers classify with errors.Is.
var ErrInvalidSelection = errors.New("nativeice: invalid term selection")

type termSelection struct {
	field string
	terms [][]byte
}

// VisitSelectedDocuments streams the pinned generation's live documents whose
// field records any of terms to visit, one at a time, in ascending segment and
// local document order.
//
// Terms are resolved exactly against the field's term dictionary: they are
// compared as raw bytes, with no analysis, no normalization and no range,
// prefix or wildcard expansion. A term the dictionary does not hold selects
// nothing rather than failing. The documents the terms' postings select are
// unioned, the pinned snapshot's deletion masks are removed from that union,
// and each surviving document is handed to visit exactly once however many of
// the terms selected it. An empty term set selects no document and is not an
// error.
//
// Selection precedes stored-field decoding: a document the selection does not
// hold has its stored bytes left unread, so a generation holding a damaged
// document still serves a selection that excludes it.
//
// The StoredDocument handed to visit is borrowed exactly as in
// VisitLiveDocuments: it, and every name and value it yields, stay valid only
// until visit returns. At most one document plus the reader's configured decode
// buffers, the selected field's dictionary and the unioned postings are
// resident at a time.
//
// A selection that names no field, or whose term count or term length exceeds
// the reader's configured bounds, is rejected with an error wrapping
// ErrInvalidSelection before any document is visited. The walk stops and
// returns ctx.Err() when ctx is canceled while postings are decoded or unioned,
// or between two documents. It stops and returns visit's error when visit
// fails. A dictionary, posting record or stored record that violates the ICE
// v3 grammar, or that would require decoding past a configured bound, stops the
// walk with an error wrapping ErrCorrupt.
func (r *Reader) VisitSelectedDocuments(ctx context.Context, field string, terms [][]byte, visit func(StoredDocument) error) error {
	selection := termSelection{field: field, terms: terms}
	if selectionErr := validateSelection(selection); selectionErr != nil {
		return selectionErr
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	if len(selection.terms) == 0 {
		return nil
	}
	for segmentIndex := range r.segments {
		if visitErr := walkSelectedStoredSegment(ctx, r.segments[segmentIndex], selection, visit); visitErr != nil {
			return visitErr
		}
	}
	return nil
}

func validateSelection(selection termSelection) error {
	if selection.field == "" {
		return fmt.Errorf("selection has no field: %w", ErrInvalidSelection)
	}
	if len(selection.terms) > maxSelectionTermCount {
		return fmt.Errorf("selection has %d terms, limit is %d: %w", len(selection.terms), maxSelectionTermCount, ErrInvalidSelection)
	}
	for termIndex, term := range selection.terms {
		if len(term) > maxSelectionTermLength {
			return fmt.Errorf("selection term %d has %d bytes, limit is %d: %w", termIndex, len(term), maxSelectionTermLength, ErrInvalidSelection)
		}
	}
	return nil
}

func walkSelectedStoredSegment(ctx context.Context, segment pinnedSegment, selection termSelection, visit func(StoredDocument) error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	storedReader, readerErr := newStoredSegmentReader(segment.file, segment.size, segment.record)
	if readerErr != nil {
		return readerErr
	}
	selected, selectedErr := storedReader.selectedDocuments(ctx, selection)
	if selectedErr != nil {
		return selectedErr
	}
	if selected.IsEmpty() {
		return nil
	}
	deleted, deletionErr := deletedDocuments(segment.record)
	if deletionErr != nil {
		return deletionErr
	}
	return storedReader.visitSelected(ctx, selected, deleted, visit)
}

func (s *storedSegmentReader) selectedDocuments(ctx context.Context, selection termSelection) (*roaringpkg.Bitmap, error) {
	selected := roaringpkg.New()
	dictionaryOffset, found, dictionaryErr := s.dictionaryOffset(selection.field)
	if dictionaryErr != nil {
		return nil, dictionaryErr
	}
	if !found || dictionaryOffset == 0 {
		return selected, nil
	}
	if dictionaryOffset >= s.footer.docValueOffset {
		return nil, corruptError("segment %q has a term dictionary outside its section", s.path)
	}
	dictionaryCursor := dictionaryOffset
	dictionaryLength, lengthErr := s.readUvarint(&dictionaryCursor, s.footer.docValueOffset)
	if lengthErr != nil {
		return nil, lengthErr
	}
	if dictionaryLength > maxSelectionDictionarySize || dictionaryLength > s.footer.docValueOffset-dictionaryCursor {
		return nil, corruptError("segment %q has an oversized term dictionary", s.path)
	}
	dictionaryData := make([]byte, int(dictionaryLength))
	if readErr := s.readInto(dictionaryCursor, dictionaryData); readErr != nil {
		return nil, readErr
	}
	dictionary, loadErr := loadTermDictionary(dictionaryData)
	if loadErr != nil {
		return nil, corruptError("decode term dictionary in segment %q", s.path, loadErr)
	}
	defer func() { _ = dictionary.Close() }()
	for _, term := range selection.terms {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		postingsOffset, exists, lookupErr := lookupTermPosting(dictionary, term)
		if lookupErr != nil {
			return nil, corruptError("look up term in segment %q", s.path, lookupErr)
		}
		if !exists {
			continue
		}
		if postingsErr := s.unionPostings(ctx, selected, postingsOffset); postingsErr != nil {
			return nil, postingsErr
		}
	}
	return selected, nil
}

func loadTermDictionary(data []byte) (dictionary *vellum.FST, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			dictionary = nil
			err = fmt.Errorf("term dictionary decoder panicked: %v", recovered)
		}
	}()
	return vellum.Load(data)
}

func lookupTermPosting(dictionary *vellum.FST, term []byte) (postingOffset uint64, exists bool, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			postingOffset = 0
			exists = false
			err = fmt.Errorf("term dictionary lookup panicked: %v", recovered)
		}
	}()
	return dictionary.Get(term)
}

func (s *storedSegmentReader) dictionaryOffset(field string) (uint64, bool, error) {
	for fieldID, fieldName := range s.fieldNames {
		if fieldName != field {
			continue
		}
		indexOffset := s.footer.fieldsIndexOffset + uint64(fieldID)*fieldsIndexAddressByteWidth
		var addressData [fieldsIndexAddressByteWidth]byte
		if readErr := s.readInto(indexOffset, addressData[:]); readErr != nil {
			return 0, false, readErr
		}
		offset := binary.BigEndian.Uint64(addressData[:])
		if offset >= s.footer.fieldsIndexOffset {
			return 0, false, corruptError("segment %q has a field record outside its section", s.path)
		}
		dictionaryOffset, offsetErr := s.readUvarint(&offset, s.footer.fieldsIndexOffset)
		if offsetErr != nil {
			return 0, false, offsetErr
		}
		return dictionaryOffset, true, nil
	}
	return 0, false, nil
}

func (s *storedSegmentReader) dictionary(field string) (*vellum.FST, error) {
	s.dictionaryMu.RLock()
	if dictionary := s.dictionaries[field]; dictionary != nil {
		s.dictionaryMu.RUnlock()
		return dictionary, nil
	}
	s.dictionaryMu.RUnlock()
	s.dictionaryMu.Lock()
	defer s.dictionaryMu.Unlock()
	if dictionary := s.dictionaries[field]; dictionary != nil {
		return dictionary, nil
	}
	dictionaryOffset, found, offsetErr := s.dictionaryOffset(field)
	if offsetErr != nil || !found || dictionaryOffset == 0 {
		return nil, offsetErr
	}
	if dictionaryOffset >= s.footer.docValueOffset {
		return nil, corruptError("segment %q has a term dictionary outside its section", s.path)
	}
	cursor := dictionaryOffset
	length, lengthErr := s.readUvarint(&cursor, s.footer.docValueOffset)
	if lengthErr != nil {
		return nil, lengthErr
	}
	if length > maxSelectionDictionarySize || length > s.footer.docValueOffset-cursor {
		return nil, corruptError("segment %q has an oversized term dictionary", s.path)
	}
	data, dataErr := s.readDictionaryBytes(cursor, length)
	if dataErr != nil {
		return nil, dataErr
	}
	dictionary, loadErr := loadTermDictionary(data)
	if loadErr != nil {
		return nil, corruptError("decode term dictionary in segment %q", s.path, loadErr)
	}
	if s.dictionaries == nil {
		s.dictionaries = make(map[string]*vellum.FST)
	}
	s.dictionaries[field] = dictionary
	return dictionary, nil
}

// acquireDictionaryReader returns a single-threaded vellum Reader from a
// per-field pool. Reader.Get reuses its decoder state, unlike FST.Get, while
// the pool keeps concurrent lookups independent without serializing them.
func (s *storedSegmentReader) acquireDictionaryReader(field string) (*vellum.Reader, *sync.Pool, error) {
	dictionary, dictionaryErr := s.dictionary(field)
	if dictionaryErr != nil || dictionary == nil {
		return nil, nil, dictionaryErr
	}
	readerPool := s.dictionaryReaderPool(field, dictionary)
	reader, ok := readerPool.Get().(*vellum.Reader)
	if !ok || reader == nil {
		return nil, nil, corruptError("segment %q failed to create a term dictionary reader", s.path)
	}
	return reader, readerPool, nil
}

func (s *storedSegmentReader) dictionaryReaderPool(field string, dictionary *vellum.FST) *sync.Pool {
	s.dictionaryMu.Lock()
	if s.dictionaryReaders == nil {
		s.dictionaryReaders = make(map[string]*sync.Pool)
	}
	readerPool := s.dictionaryReaders[field]
	if readerPool == nil {
		readerPool = &sync.Pool{New: func() any {
			reader, _ := dictionary.Reader()
			return reader
		}}
		s.dictionaryReaders[field] = readerPool
	}
	s.dictionaryMu.Unlock()
	return readerPool
}

func lookupTermPostingReader(reader *vellum.Reader, term []byte) (postingOffset uint64, exists bool, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			postingOffset = 0
			exists = false
			err = fmt.Errorf("term dictionary lookup panicked: %v", recovered)
		}
	}()
	return reader.Get(term)
}

// readDictionaryBytes applies the dictionary's 64 MiB bound without changing
// the smaller bound used by stored-document and posting reads.
func (s *storedSegmentReader) readDictionaryBytes(offset, length uint64) ([]byte, error) {
	if length > maxSelectionDictionarySize {
		return nil, corruptError("segment %q requested an oversized term dictionary", s.path)
	}
	if length > uint64(^uint(0)>>1) {
		return nil, corruptError("segment %q term dictionary length overflows int", s.path)
	}
	if memory, ok := s.file.(*byteSegmentFile); ok {
		if offset > uint64(len(memory.data)) || length > uint64(len(memory.data))-offset {
			return nil, corruptError("segment %q dictionary read exceeds file bounds", s.path)
		}
		return memory.data[int(offset):int(offset+length)], nil
	}
	data := make([]byte, int(length))
	if readErr := s.readInto(offset, data); readErr != nil {
		return nil, readErr
	}
	return data, nil
}

func (s *storedSegmentReader) fieldStats(field string) (uint64, uint64, bool, error) {
	s.fieldStatsMu.RLock()
	if cached, found := s.fieldStatsCache[field]; found {
		s.fieldStatsMu.RUnlock()
		return cached.documents, cached.frequency, cached.found, nil
	}
	s.fieldStatsMu.RUnlock()
	documents, frequency, found, statsErr := s.fieldStatsUncached(field)
	if statsErr != nil {
		return 0, 0, false, statsErr
	}
	s.fieldStatsMu.Lock()
	if s.fieldStatsCache == nil {
		s.fieldStatsCache = make(map[string]cachedFieldStats)
	}
	s.fieldStatsCache[field] = cachedFieldStats{documents: documents, frequency: frequency, found: found}
	s.fieldStatsMu.Unlock()
	return documents, frequency, found, nil
}

func (s *storedSegmentReader) fieldStatsUncached(field string) (uint64, uint64, bool, error) {
	for fieldID, fieldName := range s.fieldNames {
		if fieldName != field {
			continue
		}
		indexOffset := s.footer.fieldsIndexOffset + uint64(fieldID)*fieldsIndexAddressByteWidth
		var addressData [fieldsIndexAddressByteWidth]byte
		if readErr := s.readInto(indexOffset, addressData[:]); readErr != nil {
			return 0, 0, false, readErr
		}
		offset := binary.BigEndian.Uint64(addressData[:])
		if offset >= s.footer.fieldsIndexOffset {
			return 0, 0, false, corruptError("segment %q has a field record outside its section", s.path)
		}
		if _, offsetErr := s.readUvarint(&offset, s.footer.fieldsIndexOffset); offsetErr != nil {
			return 0, 0, false, offsetErr
		}
		nameLength, nameLengthErr := s.readUvarint(&offset, s.footer.fieldsIndexOffset)
		if nameLengthErr != nil || nameLength > s.footer.fieldsIndexOffset-offset {
			if nameLengthErr != nil {
				return 0, 0, false, nameLengthErr
			}
			return 0, 0, false, corruptError("segment %q has an invalid field name", s.path)
		}
		offset += nameLength
		documentCount, documentErr := s.readUvarint(&offset, s.footer.fieldsIndexOffset)
		if documentErr != nil {
			return 0, 0, false, documentErr
		}
		frequency, frequencyErr := s.readUvarint(&offset, s.footer.fieldsIndexOffset)
		if frequencyErr != nil {
			return 0, 0, false, frequencyErr
		}
		return documentCount, frequency, true, nil
	}
	return 0, 0, false, nil
}

func (s *storedSegmentReader) termFrequencies(field string, term []byte) ([]TermFrequency, error) {
	dictionary, dictionaryErr := s.dictionary(field)
	if dictionaryErr != nil || dictionary == nil {
		return nil, dictionaryErr
	}
	postingOffset, found, lookupErr := lookupTermPosting(dictionary, term)
	if lookupErr != nil || !found {
		return nil, lookupErr
	}
	if postingOffset&fstValueEncodingMask == fstValueEncodingOneHit {
		documentNumber := postingOffset & fstValueDocumentMask
		if documentNumber >= s.footer.documentCount {
			return nil, corruptError("segment %q has an out-of-range single-hit posting", s.path)
		}
		return []TermFrequency{{DocumentNumber: documentNumber, Frequency: 1}}, nil
	}
	if postingOffset&fstValueEncodingMask != 0 {
		return nil, corruptError("segment %q has an unsupported posting encoding", s.path)
	}
	if postingOffset >= s.footer.docValueOffset {
		return nil, corruptError("segment %q has a posting outside its section", s.path)
	}
	cursor := postingOffset
	frequencyOffset, frequencyErr := s.readUvarint(&cursor, s.footer.docValueOffset)
	if frequencyErr != nil {
		return nil, frequencyErr
	}
	locationOffset, locationErr := s.readUvarint(&cursor, s.footer.docValueOffset)
	if locationErr != nil {
		return nil, locationErr
	}
	if locationOffset > 0 && frequencyOffset > 0 {
		if locationOffset > ^uint64(0)-frequencyOffset {
			return nil, corruptError("segment %q has an overflowing posting detail offset", s.path)
		}
		locationOffset += frequencyOffset
	}
	if frequencyOffset > postingOffset || locationOffset > postingOffset {
		return nil, corruptError("segment %q has a posting detail offset outside its section", s.path)
	}
	postingsLength, lengthErr := s.readUvarint(&cursor, s.footer.docValueOffset)
	if lengthErr != nil || postingsLength > maxSelectionPostingsSize || postingsLength > s.footer.docValueOffset-cursor {
		if lengthErr != nil {
			return nil, lengthErr
		}
		return nil, corruptError("segment %q has an oversized posting bitmap", s.path)
	}
	postingsData, dataErr := s.readPostingBytes(cursor, postingsLength)
	if dataErr != nil {
		return nil, dataErr
	}
	postings, decodeErr := decodePostingBitmap(context.Background(), postingsData)
	if decodeErr != nil {
		return nil, corruptError("decode posting bitmap in segment %q", s.path, decodeErr)
	}
	if postings.GetCardinality() > s.footer.documentCount {
		return nil, corruptError("segment %q has a posting bitmap with too many documents", s.path)
	}
	for _, document := range postings.ToArray() {
		if uint64(document) >= s.footer.documentCount {
			return nil, corruptError("segment %q has an out-of-range posting document", s.path)
		}
	}
	result := make([]TermFrequency, 0, postings.GetCardinality())
	if frequencyOffset == 0 {
		for _, document := range postings.ToArray() {
			result = append(result, TermFrequency{DocumentNumber: uint64(document), Frequency: 1})
		}
		return result, nil
	}
	return s.decodeICEFrequencyStream(frequencyOffset, postingOffset, postings)
}

func (s *storedSegmentReader) termDocumentCounts(field string, terms [][]byte) ([]uint64, error) {
	counts := make([]uint64, len(terms))
	dictionary, dictionaryErr := s.dictionary(field)
	if dictionaryErr != nil || dictionary == nil {
		return counts, dictionaryErr
	}
	for termIndex, term := range terms {
		postingOffset, found, lookupErr := lookupTermPosting(dictionary, term)
		if lookupErr != nil {
			return nil, corruptError("look up term in segment %q", s.path, lookupErr)
		}
		if !found {
			continue
		}
		count, countErr := s.postingCount(postingOffset)
		if countErr != nil {
			return nil, countErr
		}
		counts[termIndex] = count
	}
	return counts, nil
}

func (s *storedSegmentReader) termExists(field string, term []byte) (bool, error) {
	dictionary, dictionaryErr := s.dictionary(field)
	if dictionaryErr != nil || dictionary == nil {
		return false, dictionaryErr
	}
	_, found, lookupErr := lookupTermPosting(dictionary, term)
	if lookupErr != nil {
		return false, corruptError("look up term in segment %q", s.path, lookupErr)
	}
	return found, nil
}

func (s *storedSegmentReader) termDocumentsBatch(field string, terms [][]byte) ([][]uint64, error) {
	result := make([][]uint64, len(terms))
	dictionary, dictionaryErr := s.dictionary(field)
	if dictionaryErr != nil || dictionary == nil {
		return result, dictionaryErr
	}
	for termIndex, term := range terms {
		postingOffset, found, lookupErr := lookupTermPosting(dictionary, term)
		if lookupErr != nil {
			return nil, corruptError("look up term in segment %q", s.path, lookupErr)
		}
		if found {
			postingDocuments, postingErr := s.postingDocuments(postingOffset)
			if postingErr != nil {
				return nil, postingErr
			}
			result[termIndex] = postingDocuments
		}
	}
	return result, nil
}

func (s *storedSegmentReader) postingDocuments(postingOffset uint64) ([]uint64, error) {
	switch postingOffset & fstValueEncodingMask {
	case fstValueEncodingOneHit:
		documentNumber := postingOffset & fstValueDocumentMask
		if documentNumber >= s.footer.documentCount {
			return nil, corruptError("segment %q has an out-of-range single-hit posting", s.path)
		}
		return []uint64{documentNumber}, nil
	case 0:
		break
	default:
		return nil, corruptError("segment %q has an unsupported posting encoding", s.path)
	}
	if postingOffset >= s.footer.docValueOffset {
		return nil, corruptError("segment %q has a posting outside its section", s.path)
	}
	cursor := postingOffset
	frequencyOffset, frequencyErr := s.readUvarint(&cursor, s.footer.docValueOffset)
	if frequencyErr != nil {
		return nil, frequencyErr
	}
	locationOffset, locationErr := s.readUvarint(&cursor, s.footer.docValueOffset)
	if locationErr != nil {
		return nil, locationErr
	}
	if locationOffset > 0 && frequencyOffset > 0 {
		if locationOffset > ^uint64(0)-frequencyOffset {
			return nil, corruptError("segment %q has an overflowing posting detail offset", s.path)
		}
		locationOffset += frequencyOffset
	}
	if frequencyOffset > postingOffset || locationOffset > postingOffset {
		return nil, corruptError("segment %q has a posting detail offset outside its section", s.path)
	}
	postingsLength, lengthErr := s.readUvarint(&cursor, s.footer.docValueOffset)
	if lengthErr != nil {
		return nil, lengthErr
	}
	if postingsLength > maxSelectionPostingsSize || postingsLength > s.footer.docValueOffset-cursor {
		return nil, corruptError("segment %q has an oversized posting bitmap", s.path)
	}
	if postingsLength > uint64(^uint(0)>>1) {
		return nil, corruptError("segment %q posting bitmap length overflows int", s.path)
	}
	postingsData := make([]byte, int(postingsLength))
	if readErr := s.readInto(cursor, postingsData); readErr != nil {
		return nil, readErr
	}
	postings, decodeErr := decodePostingBitmap(context.Background(), postingsData)
	if decodeErr != nil {
		return nil, corruptError("decode posting bitmap in segment %q", s.path, decodeErr)
	}
	if postings.GetCardinality() > s.footer.documentCount {
		return nil, corruptError("segment %q has a posting bitmap with too many documents", s.path)
	}
	documents := postings.ToArray()
	result := make([]uint64, len(documents))
	for documentIndex, document := range documents {
		if uint64(document) >= s.footer.documentCount {
			return nil, corruptError("segment %q has an out-of-range posting document", s.path)
		}
		result[documentIndex] = uint64(document)
	}
	return result, nil
}

func (s *storedSegmentReader) visitTermPostings(field string, terms [][]byte, visit func([]byte, []uint64, []TermFrequency) error) error {
	dictionary, dictionaryErr := s.dictionary(field)
	if dictionaryErr != nil || dictionary == nil {
		return dictionaryErr
	}

	if terms == nil {
		iterator, iteratorErr := dictionary.Iterator(nil, nil)
		if iteratorErr != nil {
			if errors.Is(iteratorErr, vellum.ErrIteratorDone) || strings.Contains(strings.ToLower(iteratorErr.Error()), "iterator") {
				return nil
			}
			return corruptError("iterate term dictionary", iteratorErr)
		}
		defer func() { _ = iterator.Close() }()
		for {
			term, value := iterator.Current()
			if term == nil && value == 0 {
				return nil
			}
			termCopy := append([]byte(nil), term...)
			termDocuments, termFrequencies, postingErr := s.postingValues(value)
			if postingErr != nil {
				return postingErr
			}
			if visitErr := visit(termCopy, termDocuments, termFrequencies); visitErr != nil {
				return visitErr
			}
			nextErr := iterator.Next()
			if nextErr != nil {
				if errors.Is(nextErr, vellum.ErrIteratorDone) || strings.Contains(strings.ToLower(nextErr.Error()), "iterator") {
					return nil
				}
				return corruptError("iterate term dictionary", nextErr)
			}
		}
	}

	for _, term := range terms {
		postingOffset, found, lookupErr := lookupTermPosting(dictionary, term)
		if lookupErr != nil {
			return corruptError("look up term in segment %q", s.path, lookupErr)
		}
		if !found {
			continue
		}
		termDocuments, termFrequencies, postingErr := s.postingValues(postingOffset)
		if postingErr != nil {
			return postingErr
		}
		if visitErr := visit(term, termDocuments, termFrequencies); visitErr != nil {
			return visitErr
		}
	}
	return nil
}

func (s *storedSegmentReader) termPosting(field string, term []byte) (TermPosting, bool, error) {
	reader, readerPool, readerErr := s.acquireDictionaryReader(field)
	if readerErr != nil || reader == nil {
		return TermPosting{}, false, readerErr
	}
	defer readerPool.Put(reader)
	return s.termPostingReader(reader, term)
}

func (s *storedSegmentReader) termPostingReader(reader *vellum.Reader, term []byte) (TermPosting, bool, error) {
	postingOffset, found, lookupErr := lookupTermPostingReader(reader, term)
	if lookupErr != nil {
		return TermPosting{}, false, corruptError("look up term in segment %q", s.path, lookupErr)
	}
	if !found {
		return TermPosting{}, false, nil
	}
	if postingOffset&fstValueEncodingMask == fstValueEncodingOneHit {
		documentNumber := postingOffset & fstValueDocumentMask
		if documentNumber >= s.footer.documentCount {
			return TermPosting{}, false, corruptError("segment %q has an out-of-range single-hit posting", s.path)
		}
		return TermPosting{OneHit: true, DocumentNumber: documentNumber}, true, nil
	}
	if postingOffset&fstValueEncodingMask != 0 {
		return TermPosting{}, false, corruptError("segment %q has an unsupported posting encoding", s.path)
	}
	documents, frequencies, bitmap, postingErr := s.postingValuesAndBitmap(postingOffset)
	if postingErr != nil {
		return TermPosting{}, false, postingErr
	}
	return TermPosting{Bitmap: bitmap, Documents: documents, Frequencies: frequencies}, true, nil
}

func (s *storedSegmentReader) postingValues(postingOffset uint64) ([]uint64, []TermFrequency, error) {
	documents, frequencies, _, postingErr := s.postingValuesAndBitmap(postingOffset)
	return documents, frequencies, postingErr
}

func (s *storedSegmentReader) postingValuesAndBitmap(postingOffset uint64) ([]uint64, []TermFrequency, *roaringpkg.Bitmap, error) {
	if postingOffset&fstValueEncodingMask == fstValueEncodingOneHit {
		documentNumber := postingOffset & fstValueDocumentMask
		if documentNumber >= s.footer.documentCount {
			return nil, nil, nil, corruptError("segment %q has an out-of-range single-hit posting", s.path)
		}
		return []uint64{documentNumber}, []TermFrequency{{DocumentNumber: documentNumber, Frequency: 1}}, nil, nil
	}
	if postingOffset&fstValueEncodingMask != 0 {
		return nil, nil, nil, corruptError("segment %q has an unsupported posting encoding", s.path)
	}
	if postingOffset >= s.footer.docValueOffset {
		return nil, nil, nil, corruptError("segment %q has a posting outside its section", s.path)
	}
	cursor := postingOffset
	frequencyOffset, frequencyErr := s.readUvarint(&cursor, s.footer.docValueOffset)
	if frequencyErr != nil {
		return nil, nil, nil, frequencyErr
	}
	locationOffset, locationErr := s.readUvarint(&cursor, s.footer.docValueOffset)
	if locationErr != nil {
		return nil, nil, nil, locationErr
	}
	if locationOffset > 0 && frequencyOffset > 0 {
		if locationOffset > ^uint64(0)-frequencyOffset {
			return nil, nil, nil, corruptError("segment %q has an overflowing posting detail offset", s.path)
		}
		locationOffset += frequencyOffset
	}
	if frequencyOffset > postingOffset || locationOffset > postingOffset {
		return nil, nil, nil, corruptError("segment %q has a posting detail offset outside its section", s.path)
	}
	postingsLength, lengthErr := s.readUvarint(&cursor, s.footer.docValueOffset)
	if lengthErr != nil || postingsLength > maxSelectionPostingsSize || postingsLength > s.footer.docValueOffset-cursor {
		if lengthErr != nil {
			return nil, nil, nil, lengthErr
		}
		return nil, nil, nil, corruptError("segment %q has an oversized posting bitmap", s.path)
	}
	postingsData, dataErr := s.readPostingBytes(cursor, postingsLength)
	if dataErr != nil {
		return nil, nil, nil, dataErr
	}
	postings, decodeErr := decodePostingBitmap(context.Background(), postingsData)
	if decodeErr != nil {
		return nil, nil, nil, corruptError("decode posting bitmap in segment %q", s.path, decodeErr)
	}
	if postings.GetCardinality() > s.footer.documentCount {
		return nil, nil, nil, corruptError("segment %q has a posting bitmap with too many documents", s.path)
	}
	documents := postings.ToArray()
	result := make([]uint64, len(documents))
	for documentIndex, document := range documents {
		if uint64(document) >= s.footer.documentCount {
			return nil, nil, nil, corruptError("segment %q has an out-of-range posting document", s.path)
		}
		result[documentIndex] = uint64(document)
	}
	if frequencyOffset == 0 {
		frequencies := make([]TermFrequency, len(documents))
		for documentIndex, document := range documents {
			frequencies[documentIndex] = TermFrequency{DocumentNumber: uint64(document), Frequency: 1}
		}
		return result, frequencies, postings, nil
	}
	frequencies, frequencyDecodeErr := s.decodeICEFrequencyStream(frequencyOffset, postingOffset, postings)
	if frequencyDecodeErr != nil {
		return nil, nil, nil, frequencyDecodeErr
	}
	return result, frequencies, postings, nil
}

func (s *storedSegmentReader) decodeICEFrequencyStream(frequencyOffset, postingOffset uint64, postings *roaringpkg.Bitmap) ([]TermFrequency, error) {
	if frequencyOffset >= postingOffset {
		return nil, corruptError("segment %q has an invalid frequency stream offset", s.path)
	}
	cursor := frequencyOffset
	chunkCount, countErr := s.readUvarint(&cursor, postingOffset)
	if countErr != nil {
		return nil, countErr
	}
	if chunkCount == 0 || chunkCount > maxFrequencyChunkCount {
		return nil, corruptError("segment %q has an invalid frequency chunk count", s.path)
	}
	chunkSize, chunkErr := nativeICEChunkSize(s.footer.chunkMode, postings.GetCardinality(), s.footer.documentCount)
	if chunkErr != nil {
		return nil, chunkErr
	}
	expectedChunks := (s.footer.documentCount-1)/chunkSize + 1
	if chunkCount != expectedChunks {
		return nil, corruptError("segment %q has %d frequency chunks, want %d", s.path, chunkCount, expectedChunks)
	}
	offsets := make([]uint64, int(chunkCount))
	var previous uint64
	for chunkIndex := range offsets {
		offset, offsetErr := s.readUvarint(&cursor, postingOffset)
		if offsetErr != nil {
			return nil, offsetErr
		}
		if offset < previous || offset > postingOffset-cursor {
			return nil, corruptError("segment %q has invalid frequency chunk offsets", s.path)
		}
		offsets[chunkIndex] = offset
		previous = offset
	}
	dataStart := cursor
	if dataStart > postingOffset || previous > postingOffset-dataStart {
		return nil, corruptError("segment %q has frequency chunk data outside its posting section", s.path)
	}
	documents := postings.ToArray()
	result := make([]TermFrequency, 0, len(documents))
	documentIndex := 0
	for chunkIndex, endOffset := range offsets {
		startOffset := uint64(0)
		if chunkIndex > 0 {
			startOffset = offsets[chunkIndex-1]
		}
		chunkDocuments := make([]uint32, 0)
		for documentIndex < len(documents) && uint64(documents[documentIndex])/chunkSize == uint64(chunkIndex) {
			chunkDocuments = append(chunkDocuments, documents[documentIndex])
			documentIndex++
		}
		if endOffset == startOffset {
			if len(chunkDocuments) != 0 {
				return nil, corruptError("segment %q has no frequency data for chunk %d", s.path, chunkIndex)
			}
			continue
		}
		compressedLength := endOffset - startOffset
		if compressedLength > maxFrequencyCompressedSize {
			return nil, corruptError("segment %q has an oversized frequency chunk", s.path)
		}
		compressed, readErr := s.readBytes(dataStart+startOffset, compressedLength)
		if readErr != nil {
			return nil, readErr
		}
		decoded, decodeErr := decodeICEFrequencyChunk(compressed)
		if decodeErr != nil {
			return nil, corruptError("decode frequency chunk in segment %q: %w", s.path, decodeErr)
		}
		if len(decoded) > maxFrequencyDecodedSize {
			return nil, corruptError("segment %q has an oversized decoded frequency chunk", s.path)
		}
		decoder := byteDecoder{payload: decoded}
		for _, document := range chunkDocuments {
			encodedFrequency, frequencyErr := decoder.uvarint()
			if frequencyErr != nil {
				return nil, frequencyErr
			}
			frequency := encodedFrequency >> 1
			if frequency == 0 {
				return nil, corruptError("segment %q has an invalid term frequency", s.path)
			}
			if _, normErr := decoder.uvarint(); normErr != nil {
				return nil, normErr
			}
			result = append(result, TermFrequency{DocumentNumber: uint64(document), Frequency: frequency})
		}
		if decoder.remaining() != 0 {
			return nil, corruptError("segment %q has trailing frequency bytes in chunk %d", s.path, chunkIndex)
		}
	}
	if documentIndex != len(documents) {
		return nil, corruptError("segment %q has postings outside its frequency chunks", s.path)
	}
	return result, nil
}

func decodeICEFrequencyChunk(compressed []byte) (decoded []byte, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("frequency chunk decoder panicked: %v", recovered)
		}
	}()
	decodedLength, lengthErr := s2.DecodedLen(compressed)
	if lengthErr != nil {
		return nil, lengthErr
	}
	if decodedLength < 0 || decodedLength > maxFrequencyDecodedSize {
		return nil, fmt.Errorf("decoded frequency chunk length %d exceeds limit", decodedLength)
	}
	decoded, decodeErr := s2.Decode(nil, compressed)
	if decodeErr != nil {
		return nil, decodeErr
	}
	if len(decoded) != decodedLength {
		return nil, fmt.Errorf("decoded frequency chunk length %d, want %d", len(decoded), decodedLength)
	}
	return decoded, nil
}

func (s *storedSegmentReader) unionPostings(ctx context.Context, selected *roaringpkg.Bitmap, postingsOffset uint64) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	switch postingsOffset & fstValueEncodingMask {
	case fstValueEncodingOneHit:
		documentNumber := postingsOffset & fstValueDocumentMask
		if documentNumber >= s.footer.documentCount {
			return corruptError("segment %q has an out-of-range single-hit posting", s.path)
		}
		selected.Add(uint32(documentNumber))
		return nil
	case 0:
		return s.unionGeneralPostings(ctx, selected, postingsOffset)
	default:
		return corruptError("segment %q has an unsupported posting encoding", s.path)
	}
}

func (s *storedSegmentReader) unionGeneralPostings(ctx context.Context, selected *roaringpkg.Bitmap, postingsOffset uint64) error {
	if postingsOffset >= s.footer.docValueOffset {
		return corruptError("segment %q has a posting outside its section", s.path)
	}
	postingsCursor := postingsOffset
	freqOffset, freqErr := s.readUvarint(&postingsCursor, s.footer.docValueOffset)
	if freqErr != nil {
		return freqErr
	}
	locationOffset, locationErr := s.readUvarint(&postingsCursor, s.footer.docValueOffset)
	if locationErr != nil {
		return locationErr
	}
	if locationOffset > 0 && freqOffset > 0 {
		if locationOffset > ^uint64(0)-freqOffset {
			return corruptError("segment %q has an overflowing posting detail offset", s.path)
		}
		locationOffset += freqOffset
	}
	if freqOffset > postingsOffset || locationOffset > postingsOffset {
		return corruptError("segment %q has a posting detail offset outside its section", s.path)
	}
	postingsLength, lengthErr := s.readUvarint(&postingsCursor, s.footer.docValueOffset)
	if lengthErr != nil {
		return lengthErr
	}
	if postingsLength > maxSelectionPostingsSize || postingsLength > s.footer.docValueOffset-postingsCursor {
		return corruptError("segment %q has an oversized posting bitmap", s.path)
	}
	postingsData := make([]byte, int(postingsLength))
	if readErr := s.readInto(postingsCursor, postingsData); readErr != nil {
		return readErr
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	postings, unmarshalErr := decodePostingBitmap(ctx, postingsData)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	if unmarshalErr != nil {
		return corruptError("decode posting bitmap in segment %q", s.path, unmarshalErr)
	}
	if postings.GetCardinality() > s.footer.documentCount {
		return corruptError("segment %q has a posting bitmap with too many documents", s.path)
	}
	return unionPostingBitmap(ctx, selected, postings, s.footer.documentCount, s.path)
}

func unionPostingBitmap(ctx context.Context, selected, postings *roaringpkg.Bitmap, documentCount uint64, path string) error {
	iterator := postings.ManyIterator()
	documentNumbers := make([]uint32, selectionPostingBatchSize)
	for {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		batchSize := iterator.NextMany(documentNumbers)
		if batchSize == 0 {
			return nil
		}
		if uint64(documentNumbers[batchSize-1]) >= documentCount {
			return corruptError("segment %q has an out-of-range posting document", path)
		}
		selected.AddMany(documentNumbers[:batchSize])
	}
}

func decodePostingBitmap(ctx context.Context, data []byte) (postings *roaringpkg.Bitmap, err error) {
	postings = roaringpkg.New()
	defer func() {
		if recovered := recover(); recovered != nil {
			postings = nil
			err = fmt.Errorf("posting bitmap decoder panicked: %v", recovered)
		}
	}()
	reader := &contextByteReader{ctx: ctx, reader: bytes.NewReader(data)}
	if _, readErr := postings.ReadFrom(reader); readErr != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, readErr
	}
	return postings, nil
}

type contextByteReader struct {
	ctx    context.Context
	reader *bytes.Reader
}

func (r *contextByteReader) Read(data []byte) (int, error) {
	if ctxErr := r.ctx.Err(); ctxErr != nil {
		return 0, ctxErr
	}
	if len(data) > selectionDecodeReadSize {
		data = data[:selectionDecodeReadSize]
	}
	return r.reader.Read(data)
}

func (s *storedSegmentReader) visitSelected(ctx context.Context, selected, deleted *roaringpkg.Bitmap, visit func(StoredDocument) error) error {
	iterator := selected.Iterator()
	var chunk []byte
	var loadedChunk uint64
	chunkLoaded := false
	for iterator.HasNext() {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		documentNumber := uint64(iterator.Next())
		if documentNumber >= s.footer.documentCount {
			return corruptError("segment %q selected an out-of-range document", s.path)
		}
		if deleted.Contains(uint32(documentNumber)) {
			continue
		}
		chunkIndex := documentNumber / storedDocumentsPerChunk
		if !chunkLoaded || loadedChunk != chunkIndex {
			loadedChunk = chunkIndex
			chunkLoaded = true
			var chunkErr error
			chunk, chunkErr = s.loadChunk(chunkIndex)
			if chunkErr != nil {
				return chunkErr
			}
		}
		document, documentErr := s.decodeDocument(documentNumber, chunk)
		if documentErr != nil {
			return documentErr
		}
		if visitErr := visit(document); visitErr != nil {
			return visitErr
		}
	}
	return nil
}
