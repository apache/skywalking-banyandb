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
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"

	roaringpkg "github.com/RoaringBitmap/roaring"
	"github.com/blevesearch/vellum"
)

const (
	maxSelectionTermCount      = 1 << 16
	maxSelectionTermLength     = 64 << 10
	maxSelectionDictionarySize = 64 << 20
	maxSelectionPostingsSize   = 64 << 20
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
// returns ctx.Err() when ctx is canceled between two documents, and stops and
// returns visit's error when visit fails. A dictionary, posting record or
// stored record that violates the ICE v3 grammar, or that would require
// decoding past a configured bound, stops the walk with an error wrapping
// ErrCorrupt.
func (r *Reader) VisitSelectedDocuments(ctx context.Context, field string, terms [][]byte, visit func(StoredDocument) error) error {
	if selectionErr := validateSelection(field, terms); selectionErr != nil {
		return selectionErr
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	if len(terms) == 0 {
		return nil
	}
	for segmentIndex := range r.segments {
		if visitErr := walkSelectedStoredSegment(ctx, r.segments[segmentIndex], field, terms, visit); visitErr != nil {
			return visitErr
		}
	}
	return nil
}

func validateSelection(field string, terms [][]byte) error {
	if field == "" {
		return fmt.Errorf("selection has no field: %w", ErrInvalidSelection)
	}
	if len(terms) > maxSelectionTermCount {
		return fmt.Errorf("selection has %d terms, limit is %d: %w", len(terms), maxSelectionTermCount, ErrInvalidSelection)
	}
	for termIndex, term := range terms {
		if len(term) > maxSelectionTermLength {
			return fmt.Errorf("selection term %d has %d bytes, limit is %d: %w", termIndex, len(term), maxSelectionTermLength, ErrInvalidSelection)
		}
	}
	return nil
}

func walkSelectedStoredSegment(ctx context.Context, record segmentRecord, field string, terms [][]byte, visit func(StoredDocument) error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	file, openErr := os.Open(record.path)
	if errors.Is(openErr, os.ErrNotExist) {
		return corruptError("open missing segment %d", record.id)
	}
	if openErr != nil {
		return corruptError("open segment %q", record.path, openErr)
	}
	defer func() {
		_ = file.Close()
	}()
	info, statErr := file.Stat()
	if statErr != nil {
		return corruptError("stat segment %q", record.path, statErr)
	}
	if !info.Mode().IsRegular() || info.Size() < segmentFooterLength {
		return corruptError("segment %q is shorter than its footer", record.path)
	}
	storedReader, readerErr := newStoredSegmentReader(file, uint64(info.Size()), record)
	if readerErr != nil {
		return readerErr
	}
	selected, selectedErr := storedReader.selectedDocuments(ctx, field, terms)
	if selectedErr != nil {
		return selectedErr
	}
	if selected.IsEmpty() {
		return nil
	}
	deleted, deletionErr := deletedDocuments(record)
	if deletionErr != nil {
		return deletionErr
	}
	return storedReader.visitSelected(ctx, selected, deleted, visit)
}

func (s *storedSegmentReader) selectedDocuments(ctx context.Context, field string, terms [][]byte) (*roaringpkg.Bitmap, error) {
	selected := roaringpkg.New()
	dictionaryOffset, found, dictionaryErr := s.dictionaryOffset(field)
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
	defer func() {
		_ = dictionary.Close()
	}()
	for _, term := range terms {
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
		if postingsErr := s.unionPostings(selected, postingsOffset); postingsErr != nil {
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

func (s *storedSegmentReader) unionPostings(selected *roaringpkg.Bitmap, postingsOffset uint64) error {
	switch postingsOffset & fstValueEncodingMask {
	case fstValueEncodingOneHit:
		documentNumber := postingsOffset & fstValueDocumentMask
		if documentNumber >= s.footer.documentCount {
			return corruptError("segment %q has an out-of-range single-hit posting", s.path)
		}
		selected.Add(uint32(documentNumber))
		return nil
	case 0:
		return s.unionGeneralPostings(selected, postingsOffset)
	default:
		return corruptError("segment %q has an unsupported posting encoding", s.path)
	}
}

func (s *storedSegmentReader) unionGeneralPostings(selected *roaringpkg.Bitmap, postingsOffset uint64) error {
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
	postings, unmarshalErr := decodePostingBitmap(postingsData)
	if unmarshalErr != nil {
		return corruptError("decode posting bitmap in segment %q", s.path, unmarshalErr)
	}
	if postings.GetCardinality() > s.footer.documentCount {
		return corruptError("segment %q has a posting bitmap with too many documents", s.path)
	}
	iterator := postings.Iterator()
	for iterator.HasNext() {
		if uint64(iterator.Next()) >= s.footer.documentCount {
			return corruptError("segment %q has an out-of-range posting document", s.path)
		}
	}
	selected.Or(postings)
	return nil
}

func decodePostingBitmap(data []byte) (postings *roaringpkg.Bitmap, err error) {
	postings = roaringpkg.New()
	defer func() {
		if recovered := recover(); recovered != nil {
			postings = nil
			err = fmt.Errorf("posting bitmap decoder panicked: %v", recovered)
		}
	}()
	if unmarshalErr := postings.UnmarshalBinary(data); unmarshalErr != nil {
		return nil, unmarshalErr
	}
	return postings, nil
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
