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
	"container/heap"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"sort"

	roaringpkg "github.com/RoaringBitmap/roaring"
)

const (
	repairSortFieldCount           = 4
	maxRepairPageSize              = 1 << 16
	maxRepairSortValueLength       = 64 << 10
	docValueDocumentsPerChunk      = 1024
	docValueChunkTableFooterLength = 16
	maxDocValueChunkCount          = 1 << 20
	maxDocValueCompressedChunkSize = 16 << 20
	maxDocValueDecodedChunkSize    = 64 << 20
)

// ErrInvalidRepairPage reports a bounded repair page request that the native
// reader cannot serve.
var ErrInvalidRepairPage = errors.New("nativeice: invalid repair page request")

// RepairCursor identifies a row in the pinned generation, including tuple ties.
type RepairCursor struct {
	SortValues     [][]byte
	SegmentID      uint64
	DocumentNumber uint64
}

// RepairPageRequest describes one bounded repair page.
type RepairPageRequest struct {
	After        *RepairCursor
	SortFields   [repairSortFieldCount]string
	ProjectField string
	PageSize     int
}

// RepairTupleRow is one bounded repair row returned by Reader.RepairTuplePage.
type RepairTupleRow struct {
	SortValues [][]byte
	Value      []byte
	Cursor     RepairCursor
}

// RepairTuplePage returns a bounded page from the Reader's pinned generation.
func (r *Reader) RepairTuplePage(ctx context.Context, request RepairPageRequest) ([]RepairTupleRow, error) {
	if requestErr := validateRepairPageRequest(request); requestErr != nil {
		return nil, requestErr
	}

	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, ctxErr
	}

	r.repairPageMu.Lock()
	defer r.repairPageMu.Unlock()
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, ctxErr
	}
	segmentReaders, readersErr := r.repairPageReadersFor(request.SortFields)
	if readersErr != nil {
		return nil, readersErr
	}

	candidates := make(repairTupleHeap, 0, request.PageSize)
	heap.Init(&candidates)
	for segmentIndex, segmentReader := range segmentReaders {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		documentCount := r.segments[segmentIndex].record.documentCount
		for documentNumber := uint64(0); documentNumber < documentCount; documentNumber++ {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, ctxErr
			}
			if documentNumber <= math.MaxUint32 && segmentReader.deleted.Contains(uint32(documentNumber)) {
				continue
			}
			candidate, candidateErr := segmentReader.candidate(documentNumber, r.segments[segmentIndex].record.id)
			if candidateErr != nil {
				return nil, candidateErr
			}
			if compareRepairCandidateCursor(candidate, request.After) <= 0 {
				continue
			}
			if len(candidates) == request.PageSize && compareRepairCandidates(candidate, candidates[0]) >= 0 {
				continue
			}
			stableCandidate := repairTupleCandidate{}
			if len(candidates) == request.PageSize {
				stableCandidate = heap.Pop(&candidates).(repairTupleCandidate)
			}
			copyRepairTupleCandidate(&stableCandidate, candidate)
			projectedValue, projectedErr := segmentReader.projectedValue(documentNumber, request.ProjectField, stableCandidate.value)
			if projectedErr != nil {
				return nil, projectedErr
			}
			stableCandidate.value = projectedValue
			heap.Push(&candidates, stableCandidate)
		}
	}

	sort.Slice(candidates, func(leftIndex, rightIndex int) bool {
		return compareRepairCandidates(candidates[leftIndex], candidates[rightIndex]) < 0
	})
	rows := make([]RepairTupleRow, len(candidates))
	for rowIndex, candidate := range candidates {
		sortValues := make([][]byte, repairSortFieldCount)
		copy(sortValues, candidate.sortValues[:])
		rows[rowIndex] = RepairTupleRow{
			SortValues: sortValues, Value: candidate.value,
			Cursor: RepairCursor{SortValues: sortValues, SegmentID: candidate.segmentID, DocumentNumber: candidate.documentNumber},
		}
	}
	return rows, nil
}

func (r *Reader) repairPageReadersFor(
	sortFields [repairSortFieldCount]string,
) ([]*repairSegmentPageReader, error) {
	if r.repairPageReaders != nil && r.repairPageSortFields == sortFields {
		return r.repairPageReaders, nil
	}
	readers := make([]*repairSegmentPageReader, len(r.segments))
	for segmentIndex := range r.segments {
		segmentReader, readerErr := newRepairSegmentPageReader(r.segments[segmentIndex], sortFields)
		if readerErr != nil {
			return nil, readerErr
		}
		readers[segmentIndex] = segmentReader
	}
	r.repairPageReaders = readers
	r.repairPageSortFields = sortFields
	return readers, nil
}

func validateRepairPageRequest(request RepairPageRequest) error {
	if request.PageSize <= 0 || request.PageSize > maxRepairPageSize {
		return repairPageRequestError("page size %d is outside the allowed range", request.PageSize)
	}
	if request.ProjectField == "" {
		return repairPageRequestError("projection field is empty")
	}
	if request.After != nil && len(request.After.SortValues) != repairSortFieldCount {
		return repairPageRequestError("cursor has %d components", len(request.After.SortValues))
	}
	for fieldIndex, fieldName := range request.SortFields {
		if fieldName == "" {
			return repairPageRequestError("sort field %d is empty", fieldIndex)
		}
		for previousIndex := 0; previousIndex < fieldIndex; previousIndex++ {
			if fieldName == request.SortFields[previousIndex] {
				return repairPageRequestError("sort field %q is repeated", fieldName)
			}
		}
	}
	if request.After == nil {
		return nil
	}
	for componentIndex, component := range request.After.SortValues {
		if component == nil {
			return repairPageRequestError("cursor component %d is missing", componentIndex)
		}
		if len(component) > maxRepairSortValueLength {
			return repairPageRequestError("cursor component %d exceeds %d bytes", componentIndex, maxRepairSortValueLength)
		}
	}
	return nil
}

func repairPageRequestError(format string, arguments ...any) error {
	return fmt.Errorf("nativeice: invalid repair page: "+format+": %w", append(arguments, ErrInvalidRepairPage)...)
}

type repairTupleCandidate struct {
	sortValues     [repairSortFieldCount][]byte
	value          []byte
	documentNumber uint64
	segmentID      uint64
}

func copyRepairTupleCandidate(destination *repairTupleCandidate, source repairTupleCandidate) {
	destination.documentNumber = source.documentNumber
	destination.segmentID = source.segmentID
	for valueIndex, value := range source.sortValues {
		destination.sortValues[valueIndex] = copyRepairPageValueInto(destination.sortValues[valueIndex], value)
	}
}

func compareRepairCandidates(left, right repairTupleCandidate) int {
	for valueIndex := range left.sortValues {
		if comparison := compareRepairValues(left.sortValues[valueIndex], right.sortValues[valueIndex]); comparison != 0 {
			return comparison
		}
	}
	switch {
	case left.segmentID < right.segmentID:
		return -1
	case left.segmentID > right.segmentID:
		return 1
	case left.documentNumber < right.documentNumber:
		return -1
	case left.documentNumber > right.documentNumber:
		return 1
	default:
		return 0
	}
}

func compareRepairCandidateCursor(candidate repairTupleCandidate, cursor *RepairCursor) int {
	if cursor == nil {
		return 1
	}
	cursorCandidate := repairTupleCandidate{segmentID: cursor.SegmentID, documentNumber: cursor.DocumentNumber}
	copy(cursorCandidate.sortValues[:], cursor.SortValues)
	return compareRepairCandidates(candidate, cursorCandidate)
}

func compareRepairValues(left, right []byte) int {
	switch {
	case left == nil && right == nil:
		return 0
	case left == nil:
		return 1
	case right == nil:
		return -1
	default:
		return bytes.Compare(left, right)
	}
}

type repairTupleHeap []repairTupleCandidate

func (h repairTupleHeap) Len() int {
	return len(h)
}

func (h repairTupleHeap) Less(leftIndex, rightIndex int) bool {
	return compareRepairCandidates(h[leftIndex], h[rightIndex]) > 0
}

func (h repairTupleHeap) Swap(leftIndex, rightIndex int) {
	h[leftIndex], h[rightIndex] = h[rightIndex], h[leftIndex]
}

func (h *repairTupleHeap) Push(value any) {
	*h = append(*h, value.(repairTupleCandidate))
}

func (h *repairTupleHeap) Pop() any {
	current := *h
	lastIndex := len(current) - 1
	value := current[lastIndex]
	*h = current[:lastIndex]
	return value
}

type repairSegmentPageReader struct {
	deleted           *roaringpkg.Bitmap
	sortReaders       [repairSortFieldCount]*repairDocValueReader
	stored            *storedSegmentReader
	storedChunk       []byte
	storedChunkNumber uint64
}

func newRepairSegmentPageReader(
	segment pinnedSegment,
	sortFields [repairSortFieldCount]string,
) (*repairSegmentPageReader, error) {
	storedReader, readerErr := newStoredSegmentReader(segment.file, segment.size, segment.record)
	if readerErr != nil {
		return nil, readerErr
	}
	deleted, deletionErr := deletedDocuments(segment.record)
	if deletionErr != nil {
		return nil, deletionErr
	}
	pageReader := &repairSegmentPageReader{
		deleted:           deleted,
		stored:            storedReader,
		storedChunkNumber: math.MaxUint64,
	}
	if segment.record.documentCount == 0 || storedReader.footer.docValueOffset == math.MaxUint64 {
		return pageReader, nil
	}

	locationOffset := storedReader.footer.docValueOffset
	for fieldIndex, fieldName := range storedReader.fieldNames {
		fieldStart, startErr := storedReader.readUvarint(&locationOffset, storedReader.footer.fieldsIndexOffset)
		if startErr != nil {
			return nil, startErr
		}
		fieldEnd, endErr := storedReader.readUvarint(&locationOffset, storedReader.footer.fieldsIndexOffset)
		if endErr != nil {
			return nil, endErr
		}
		if fieldStart == math.MaxUint64 || fieldEnd == math.MaxUint64 {
			if fieldStart != math.MaxUint64 || fieldEnd != math.MaxUint64 {
				return nil, corruptError("segment %q has an incomplete doc-value location for field %d", storedReader.path, fieldIndex)
			}
			continue
		}
		fieldReader, fieldErr := newRepairDocValueReader(storedReader, fieldStart, fieldEnd)
		if fieldErr != nil {
			return nil, fieldErr
		}
		for sortIndex, sortField := range sortFields {
			if sortField == fieldName {
				pageReader.sortReaders[sortIndex] = fieldReader
			}
		}
	}
	return pageReader, nil
}

func (r *repairSegmentPageReader) candidate(documentNumber, segmentID uint64) (repairTupleCandidate, error) {
	candidate := repairTupleCandidate{documentNumber: documentNumber, segmentID: segmentID}
	for sortIndex, fieldReader := range r.sortReaders {
		if fieldReader == nil {
			return repairTupleCandidate{}, corruptError("segment %d document %d is missing sort field %d", segmentID, documentNumber, sortIndex)
		}
		value, valueErr := fieldReader.smallestValue(documentNumber)
		if valueErr != nil {
			return repairTupleCandidate{}, valueErr
		}
		if value == nil {
			return repairTupleCandidate{}, corruptError("segment %d document %d is missing sort value %d", segmentID, documentNumber, sortIndex)
		}
		candidate.sortValues[sortIndex] = value
	}
	return candidate, nil
}

func (r *repairSegmentPageReader) projectedValue(documentNumber uint64, projectField string, destination []byte) ([]byte, error) {
	chunkNumber := documentNumber / storedDocumentsPerChunk
	if r.storedChunkNumber != chunkNumber {
		chunk, chunkErr := r.stored.loadChunk(chunkNumber)
		if chunkErr != nil {
			return nil, chunkErr
		}
		r.storedChunk = chunk
		r.storedChunkNumber = chunkNumber
	}
	document, documentErr := r.stored.decodeDocument(documentNumber, r.storedChunk)
	if documentErr != nil {
		return nil, documentErr
	}
	for _, field := range document.fields {
		if field.name == projectField {
			return copyRepairPageValueInto(destination, field.value), nil
		}
	}
	return nil, nil
}

type repairDocValueReader struct {
	path               string
	file               *os.File
	chunkOffsets       []uint64
	compressedBuffer   []byte
	decodedBuffer      []byte
	header             []repairDocValueMeta
	chunkNumber        uint64
	fieldDataOffset    uint64
	size               uint64
	totalDocumentCount uint64
}

type repairDocValueMeta struct {
	documentNumber uint64
	valueEnd       uint64
}

func newRepairDocValueReader(storedReader *storedSegmentReader, fieldStart, fieldEnd uint64) (*repairDocValueReader, error) {
	if storedReader.footer.documentCount > math.MaxUint64/storedDocumentOffsetByteWidth {
		return nil, corruptError("segment %q has an oversized stored document index", storedReader.path)
	}
	minimumDataOffset := storedReader.footer.storedIndexOffset + storedReader.footer.documentCount*storedDocumentOffsetByteWidth
	if minimumDataOffset < storedReader.footer.storedIndexOffset || fieldStart < minimumDataOffset ||
		fieldStart >= fieldEnd || fieldEnd > storedReader.footer.docValueOffset {
		return nil, corruptError("segment %q has invalid doc-value bounds", storedReader.path)
	}
	if fieldEnd-fieldStart <= docValueChunkTableFooterLength {
		return nil, corruptError("segment %q has a truncated doc-value chunk table", storedReader.path)
	}

	var tableFooter [docValueChunkTableFooterLength]byte
	if readErr := storedReader.readInto(fieldEnd-docValueChunkTableFooterLength, tableFooter[:]); readErr != nil {
		return nil, readErr
	}
	chunkOffsetsLength := binary.BigEndian.Uint64(tableFooter[0:8])
	chunkCount := binary.BigEndian.Uint64(tableFooter[8:16])
	expectedChunkCount := (storedReader.footer.documentCount + docValueDocumentsPerChunk - 1) / docValueDocumentsPerChunk
	if chunkCount == 0 || chunkCount != expectedChunkCount || chunkCount > maxDocValueChunkCount ||
		chunkOffsetsLength < chunkCount || chunkOffsetsLength > chunkCount*binary.MaxVarintLen64 {
		return nil, corruptError("segment %q has an invalid doc-value chunk table", storedReader.path)
	}
	tableStart := fieldEnd - docValueChunkTableFooterLength
	if chunkOffsetsLength > tableStart-fieldStart {
		return nil, corruptError("segment %q has a doc-value chunk table outside its field data", storedReader.path)
	}
	tableStart -= chunkOffsetsLength
	table, tableErr := storedReader.readBytes(tableStart, chunkOffsetsLength)
	if tableErr != nil {
		return nil, tableErr
	}
	decoder := byteDecoder{payload: table}
	offsets := make([]uint64, int(chunkCount))
	for offsetIndex := range offsets {
		offset, offsetErr := decoder.uvarint()
		if offsetErr != nil {
			return nil, offsetErr
		}
		offsets[offsetIndex] = offset
	}
	if decoder.remaining() != 0 {
		return nil, corruptError("segment %q has trailing bytes in a doc-value chunk table", storedReader.path)
	}
	fieldDataLength := tableStart - fieldStart
	for offsetIndex, offset := range offsets {
		if offset > fieldDataLength || (offsetIndex > 0 && offset < offsets[offsetIndex-1]) {
			return nil, corruptError("segment %q has invalid doc-value chunk offsets", storedReader.path)
		}
	}
	return &repairDocValueReader{
		chunkNumber:        math.MaxUint64,
		chunkOffsets:       offsets,
		fieldDataOffset:    fieldStart,
		path:               storedReader.path,
		file:               storedReader.file,
		size:               storedReader.size,
		totalDocumentCount: storedReader.footer.documentCount,
	}, nil
}

func (r *repairDocValueReader) smallestValue(documentNumber uint64) ([]byte, error) {
	chunkNumber := documentNumber / docValueDocumentsPerChunk
	if r.chunkNumber != chunkNumber {
		if chunkErr := r.loadChunk(chunkNumber); chunkErr != nil {
			return nil, chunkErr
		}
	}
	headerIndex := sort.Search(len(r.header), func(index int) bool {
		return r.header[index].documentNumber >= documentNumber
	})
	if headerIndex == len(r.header) || r.header[headerIndex].documentNumber != documentNumber {
		return nil, nil
	}
	start := uint64(0)
	if headerIndex > 0 {
		start = r.header[headerIndex-1].valueEnd
	}
	end := r.header[headerIndex].valueEnd
	if end < start || end > uint64(len(r.decodedBuffer)) {
		return nil, corruptError("segment %q has invalid doc-value bounds in a chunk", r.path)
	}
	return decodeRepairDocValueTerms(r.decodedBuffer[start:end], r.path)
}

func (r *repairDocValueReader) loadChunk(chunkNumber uint64) error {
	if chunkNumber >= uint64(len(r.chunkOffsets)) {
		return corruptError("segment %q has no doc-value chunk %d", r.path, chunkNumber)
	}
	start := uint64(0)
	if chunkNumber > 0 {
		start = r.chunkOffsets[chunkNumber-1]
	}
	end := r.chunkOffsets[chunkNumber]
	if start > end {
		return corruptError("segment %q has unordered doc-value chunk bounds", r.path)
	}
	if start == end {
		r.chunkNumber = chunkNumber
		r.header = r.header[:0]
		r.compressedBuffer = r.compressedBuffer[:0]
		r.decodedBuffer = r.decodedBuffer[:0]
		return nil
	}
	compressedLength := end - start
	if compressedLength > maxDocValueCompressedChunkSize {
		return corruptError("segment %q has an oversized doc-value chunk", r.path)
	}
	if cap(r.compressedBuffer) < int(compressedLength) {
		r.compressedBuffer = make([]byte, int(compressedLength))
	} else {
		r.compressedBuffer = r.compressedBuffer[:int(compressedLength)]
	}
	if readErr := readRepairDocValueBytes(r.file, r.size, r.fieldDataOffset+start, r.compressedBuffer, r.path); readErr != nil {
		return readErr
	}

	decoder := byteDecoder{payload: r.compressedBuffer}
	documentCount, countErr := decoder.uvarint()
	if countErr != nil {
		return countErr
	}
	if documentCount == 0 || documentCount > docValueDocumentsPerChunk {
		return corruptError("segment %q has an invalid doc-value chunk document count", r.path)
	}
	if cap(r.header) < int(documentCount) {
		r.header = make([]repairDocValueMeta, int(documentCount))
	} else {
		r.header = r.header[:int(documentCount)]
	}
	chunkFirstDocument := chunkNumber * docValueDocumentsPerChunk
	chunkLastDocument := chunkFirstDocument + docValueDocumentsPerChunk
	if chunkLastDocument > r.totalDocumentCount {
		chunkLastDocument = r.totalDocumentCount
	}
	var previousDocument, previousValueEnd uint64
	for headerIndex := range r.header {
		documentDelta, documentErr := decoder.uvarint()
		if documentErr != nil {
			return documentErr
		}
		if headerIndex > 0 && documentDelta == 0 {
			return corruptError("segment %q has repeated doc-value document numbers", r.path)
		}
		if previousDocument > math.MaxUint64-documentDelta {
			return corruptError("segment %q has overflowing doc-value document numbers", r.path)
		}
		documentNumber := previousDocument + documentDelta
		if documentNumber < chunkFirstDocument || documentNumber >= chunkLastDocument {
			return corruptError("segment %q has a doc-value document outside its chunk", r.path)
		}
		valueDelta, valueErr := decoder.uvarint()
		if valueErr != nil {
			return valueErr
		}
		if valueDelta == 0 || previousValueEnd > math.MaxUint64-valueDelta {
			return corruptError("segment %q has invalid doc-value value offsets", r.path)
		}
		valueEnd := previousValueEnd + valueDelta
		r.header[headerIndex] = repairDocValueMeta{documentNumber: documentNumber, valueEnd: valueEnd}
		previousDocument = documentNumber
		previousValueEnd = valueEnd
	}
	compressedValues := decoder.payload[decoder.offset:]
	decodedLength, lengthErr := storedChunkDecodedLength(compressedValues)
	if lengthErr != nil {
		return corruptError("decode doc-value chunk length in segment %q: %w", r.path, lengthErr)
	}
	if decodedLength < 0 || decodedLength > maxDocValueDecodedChunkSize {
		return corruptError("segment %q has an oversized decoded doc-value chunk", r.path)
	}
	if cap(r.decodedBuffer) < decodedLength {
		r.decodedBuffer = make([]byte, decodedLength)
	} else {
		r.decodedBuffer = r.decodedBuffer[:decodedLength]
	}
	decoded, decodeErr := decodeStoredChunk(r.decodedBuffer[:0], compressedValues)
	if decodeErr != nil {
		return corruptError("decode doc-value chunk in segment %q: %w", r.path, decodeErr)
	}
	if len(decoded) != decodedLength || previousValueEnd > uint64(len(decoded)) {
		return corruptError("segment %q has invalid decoded doc-value offsets", r.path)
	}
	r.decodedBuffer = decoded
	r.chunkNumber = chunkNumber
	return nil
}

func readRepairDocValueBytes(file *os.File, size, offset uint64, data []byte, path string) error {
	length := uint64(len(data))
	if offset > size || length > size-offset {
		return corruptError("segment %q reads outside its doc-value data", path)
	}
	readCount, readErr := file.ReadAt(data, int64(offset))
	if readErr != nil || readCount != len(data) {
		return corruptError("read doc-value data from segment %q", path, readErr)
	}
	return nil
}

func decodeRepairDocValueTerms(encoded []byte, path string) ([]byte, error) {
	var smallest []byte
	found := false
	for len(encoded) > 0 {
		value, remaining, valueErr := decodeRepairDocValueTerm(encoded)
		if valueErr != nil {
			return nil, corruptError("decode doc-value term in segment %q: %w", path, valueErr)
		}
		if !found || bytes.Compare(value, smallest) < 0 {
			smallest = value
			found = true
		}
		encoded = remaining
	}
	if !found {
		return nil, corruptError("segment %q has an empty doc-value entry", path)
	}
	return smallest, nil
}

func decodeRepairDocValueTerm(encoded []byte) ([]byte, []byte, error) {
	if len(encoded) == 0 {
		return nil, nil, errors.New("empty term")
	}
	var unescaped []byte
	for sourceIndex := 0; sourceIndex < len(encoded); sourceIndex++ {
		switch encoded[sourceIndex] {
		case 0xff:
			if unescaped == nil {
				if sourceIndex > maxRepairSortValueLength {
					return nil, nil, fmt.Errorf("term exceeds %d bytes", maxRepairSortValueLength)
				}
				return encoded[:sourceIndex], encoded[sourceIndex+1:], nil
			}
			return unescaped, encoded[sourceIndex+1:], nil
		case '\\':
			if sourceIndex+1 >= len(encoded) {
				return nil, nil, errors.New("truncated term escape")
			}
			if encoded[sourceIndex+1] != '\\' && encoded[sourceIndex+1] != 0xff {
				return nil, nil, errors.New("invalid term escape")
			}
			if unescaped == nil {
				if sourceIndex > maxRepairSortValueLength {
					return nil, nil, fmt.Errorf("term exceeds %d bytes", maxRepairSortValueLength)
				}
				unescaped = make([]byte, sourceIndex, maxRepairSortValueLength)
				copy(unescaped, encoded[:sourceIndex])
			}
			sourceIndex++
		}
		if unescaped != nil {
			if len(unescaped) >= maxRepairSortValueLength {
				return nil, nil, fmt.Errorf("term exceeds %d bytes", maxRepairSortValueLength)
			}
			unescaped = append(unescaped, encoded[sourceIndex])
		}
	}
	return nil, nil, errors.New("unterminated term")
}

func copyRepairPageValueInto(destination, value []byte) []byte {
	if value == nil {
		return nil
	}
	if destination == nil || cap(destination) < len(value) {
		destination = make([]byte, len(value))
	} else {
		destination = destination[:len(value)]
	}
	copy(destination, value)
	return destination
}
