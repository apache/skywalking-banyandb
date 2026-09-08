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

	roaringpkg "github.com/RoaringBitmap/roaring"
	"github.com/blevesearch/vellum"
)

const (
	maxSelectionTermCount      = 1 << 16
	maxSelectionTermLength     = 64 << 10
	maxSelectionDictionarySize = 64 << 20
	maxSelectionPostingsSize   = 64 << 20
	selectionPostingBatchSize  = 1024
	selectionDecodeReadSize    = 32 << 10
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
	defer func() {
		_ = dictionary.Close()
	}()
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
