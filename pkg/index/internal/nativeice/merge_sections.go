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
	"container/heap"
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	roaringpkg "github.com/RoaringBitmap/roaring"
	"github.com/blevesearch/vellum"
	"github.com/klauspost/compress/s2"
)

// mergeCancellationStride is how many documents a merge section processes
// between two context checks.
const mergeCancellationStride = 1024

// writeStored writes the stored section: s2 chunks of storedDocumentsPerChunk
// surviving documents, each written as soon as it fills, then the chunk
// table, then the per-document offset index. The offset index (8 bytes per
// document) follows the stored data but is produced alongside it, so it is
// staged in a spill buffer rather than held in memory.
//
//nolint:gocyclo // identifier validation, drop masks and chunk boundaries are kept explicit.
func (m *streamMerger) writeStored() error {
	offsets := m.stagedOffsets()
	m.chunkOffsets = append(m.chunkOffsets[:0], 0)
	sectionStart := m.output.written
	m.decodedChunk = m.decodedChunk[:0]
	documentsInChunk := 0
	var offsetBytes [8]byte
	flushChunk := func() error {
		m.encodedChunk = s2.Encode(m.encodedChunk[:cap(m.encodedChunk)], m.decodedChunk)
		if _, writeErr := m.output.Write(m.encodedChunk); writeErr != nil {
			return writeErr
		}
		m.chunkOffsets = append(m.chunkOffsets, m.output.written-sectionStart)
		m.decodedChunk = m.decodedChunk[:0]
		documentsInChunk = 0
		return nil
	}
	for inputIndex := range m.inputs {
		input := &m.inputs[inputIndex]
		walkErr := m.walkStoredDocuments(input.reader, func(local uint64, fields []storedField) error {
			dropped := input.removed.Contains(uint32(local))
			// Stored names and values reference the reader's decoded chunk,
			// which stays loaded for this whole document callback.
			var identifier []byte
			hasIdentifier := false
			m.storedValues = append(m.storedValues[:0], storedValue{name: identifierField})
			for _, field := range fields {
				if field.name == identifierField {
					// The last identifier value wins, and an empty one counts as missing.
					hasIdentifier = len(field.value) > 0
					identifier = field.value
					continue
				}
				if dropped {
					continue
				}
				if _, known := m.fieldIDs[field.name]; !known {
					return corruptError("segment %q stores unresolved field %q", input.reader.path, field.name)
				}
				m.storedValues = append(m.storedValues, storedValue(field))
			}
			if !hasIdentifier {
				return fmt.Errorf("physical document %d has no identifier: %w", local, ErrInvalidGeneration)
			}
			if dropped {
				return nil
			}
			m.storedValues[0].value = identifier
			binary.BigEndian.PutUint64(offsetBytes[:], uint64(len(m.decodedChunk)))
			if _, writeErr := offsets.Write(offsetBytes[:]); writeErr != nil {
				return writeErr
			}
			m.decodedChunk, m.storedMeta = appendStoredDocumentScratch(m.decodedChunk, m.storedValues, m.fieldIDs, m.storedMeta)
			documentsInChunk++
			if documentsInChunk == storedDocumentsPerChunk {
				return flushChunk()
			}
			return nil
		})
		if walkErr != nil {
			return walkErr
		}
	}
	if documentsInChunk > 0 {
		if flushErr := flushChunk(); flushErr != nil {
			return flushErr
		}
	}
	if offsets.Len() != m.documentCount*storedDocumentOffsetByteWidth {
		return corruptError("merged stored documents %d differ from survivors %d", offsets.Len()/storedDocumentOffsetByteWidth, m.documentCount)
	}
	table := m.docValueOut[:0]
	for _, chunkOffset := range m.chunkOffsets {
		table = appendNativeUvarint(table, chunkOffset)
	}
	table = appendNativeUint32(table, uint32(len(table)))
	table = appendNativeUint32(table, uint32(len(m.chunkOffsets)))
	m.docValueOut = table
	if _, writeErr := m.output.Write(table); writeErr != nil {
		return writeErr
	}
	m.storedIndex = m.output.written
	if copyErr := offsets.copyTo(m.output); copyErr != nil {
		return copyErr
	}
	return offsets.reset()
}

// walkStoredDocuments visits every physical document of reader in local
// order, one stored chunk at a time, decoding each document's fields into
// one reused buffer. The fields are borrowed until visit returns. Chunk loads
// share the reader's decode buffers, so the reader's walk lock is held for
// each chunk, as every stored walk does; it is released between chunks so a
// long merge does not stall lookups on its inputs.
func (m *streamMerger) walkStoredDocuments(reader *storedSegmentReader, visit func(uint64, []storedField) error) error {
	documentCount := reader.footer.documentCount
	for firstDocument := uint64(0); firstDocument < documentCount; firstDocument += storedDocumentsPerChunk {
		if err := m.ctx.Err(); err != nil {
			return err
		}
		if chunkErr := m.walkStoredChunk(reader, firstDocument, visit); chunkErr != nil {
			return chunkErr
		}
	}
	return nil
}

func (m *streamMerger) walkStoredChunk(reader *storedSegmentReader, firstDocument uint64, visit func(uint64, []storedField) error) error {
	reader.walkMu.Lock()
	defer reader.walkMu.Unlock()
	chunk, chunkErr := reader.loadChunk(firstDocument / storedDocumentsPerChunk)
	if chunkErr != nil {
		return chunkErr
	}
	lastDocument := min(firstDocument+storedDocumentsPerChunk, reader.footer.documentCount)
	for documentNumber := firstDocument; documentNumber < lastDocument; documentNumber++ {
		var decodeErr error
		if m.storedFields, decodeErr = reader.decodeDocumentInto(documentNumber, chunk, m.storedFields); decodeErr != nil {
			return decodeErr
		}
		if visitErr := visit(documentNumber, m.storedFields); visitErr != nil {
			return visitErr
		}
	}
	return nil
}

// writeDocValues writes one field's doc-value column chunk by chunk, reading
// the surviving inputs' columns in output document order. Only one output
// chunk's values and the column's chunk offset table are held.
//
//nolint:gocyclo // chunk boundaries across renumbered inputs are kept explicit.
func (m *streamMerger) writeDocValues(field *streamField) error {
	hasColumn := false
	for inputIndex := range m.inputs {
		if m.inputs[inputIndex].survivors == 0 {
			continue
		}
		if _, found := m.inputs[inputIndex].docValueRanges[field.name]; found {
			hasColumn = true
			break
		}
	}
	if !hasColumn {
		return nil
	}
	chunkCount := (m.documentCount + docValueDocumentsPerChunk - 1) / docValueDocumentsPerChunk
	m.chunkOffsets = m.chunkOffsets[:0]
	fieldStart := m.output.written
	currentChunk := uint64(0)
	m.docValueBuf = m.docValueBuf[:0]
	m.docValueHead = m.docValueHead[:0]
	documents := roaringpkg.New()
	finishChunk := func() error {
		if len(m.docValueHead) > 0 {
			encoded := appendNativeUvarint(m.docValueOut[:0], uint64(len(m.docValueHead)))
			var previousDocument, previousValueEnd uint64
			for _, header := range m.docValueHead {
				encoded = appendNativeUvarint(encoded, header.documentNumber-previousDocument)
				encoded = appendNativeUvarint(encoded, header.valueEnd-previousValueEnd)
				previousDocument = header.documentNumber
				previousValueEnd = header.valueEnd
			}
			headerLength := len(encoded)
			need := headerLength + s2.MaxEncodedLen(len(m.docValueBuf))
			if cap(encoded) < need {
				grown := make([]byte, headerLength, need)
				copy(grown, encoded)
				encoded = grown
			}
			compressed := s2.Encode(encoded[headerLength:need], m.docValueBuf)
			encoded = encoded[:headerLength+len(compressed)]
			m.docValueOut = encoded
			if _, writeErr := m.output.Write(encoded); writeErr != nil {
				return writeErr
			}
		}
		m.chunkOffsets = append(m.chunkOffsets, m.output.written-fieldStart)
		m.docValueBuf = m.docValueBuf[:0]
		m.docValueHead = m.docValueHead[:0]
		return nil
	}
	visited := 0
	for inputIndex := range m.inputs {
		input := &m.inputs[inputIndex]
		fieldRange, found := input.docValueRanges[field.name]
		if input.survivors == 0 || !found {
			continue
		}
		valueReader, readerErr := newRepairDocValueReader(input.reader, fieldRange[0], fieldRange[1])
		if readerErr != nil {
			return readerErr
		}
		visitErr := visitDocValueEntries(valueReader, func(local uint64, encoded []byte) error {
			if local >= input.physicalCount {
				return fmt.Errorf("doc value for field %q has document %d outside segment: %w", field.name, local, ErrCorrupt)
			}
			visited++
			if visited%mergeCancellationStride == 0 {
				if err := m.ctx.Err(); err != nil {
					return err
				}
			}
			outputNumber, survives := input.outputNumber(uint32(local))
			if !survives || len(encoded) == 0 {
				return nil
			}
			for outputNumber/docValueDocumentsPerChunk > currentChunk {
				if finishErr := finishChunk(); finishErr != nil {
					return finishErr
				}
				currentChunk++
			}
			for remaining := encoded; len(remaining) > 0; {
				length, valueErr := docValueTermLength(remaining)
				if valueErr != nil {
					return corruptError("decode doc-value term in segment %q: %w", input.reader.path, valueErr)
				}
				m.docValueBuf = append(m.docValueBuf, remaining[:length]...)
				remaining = remaining[length:]
			}
			m.docValueHead = append(m.docValueHead, docValueHeader{documentNumber: outputNumber, valueEnd: uint64(len(m.docValueBuf))})
			documents.Add(uint32(outputNumber))
			return nil
		})
		if visitErr != nil {
			return visitErr
		}
	}
	if documents.IsEmpty() {
		// No surviving document carries a value: the field has no column, and
		// nothing was written for it.
		return nil
	}
	for currentChunk < chunkCount {
		if finishErr := finishChunk(); finishErr != nil {
			return finishErr
		}
		currentChunk++
	}
	table := m.docValueOut[:0]
	for _, chunkOffset := range m.chunkOffsets {
		table = appendNativeUvarint(table, chunkOffset)
	}
	tableLength := uint64(len(table))
	table = appendNativeUint64(table, tableLength)
	table = appendNativeUint64(table, chunkCount)
	m.docValueOut = table
	if _, writeErr := m.output.Write(table); writeErr != nil {
		return writeErr
	}
	documents.RunOptimize()
	field.docValueDocuments = documents
	field.docValueStart = fieldStart
	field.docValueEnd = m.output.written
	return nil
}

// docValueTermLength validates the escaped doc-value term at the start of
// encoded, as decodeRepairDocValueTerm does, and returns its encoded length
// including the terminator. The grammar admits exactly one escaping of a
// value, the one appendNativeICEDocValueTerm writes, so a merge copies a
// validated term as it is instead of decoding and re-encoding it.
func docValueTermLength(encoded []byte) (int, error) {
	decodedLength := 0
	for index := 0; index < len(encoded); index++ {
		switch encoded[index] {
		case 0xff:
			return index + 1, nil
		case '\\':
			if index+1 >= len(encoded) {
				return 0, errors.New("truncated term escape")
			}
			if encoded[index+1] != '\\' && encoded[index+1] != 0xff {
				return 0, errors.New("invalid term escape")
			}
			index++
		}
		decodedLength++
		if decodedLength > maxRepairSortValueLength {
			return 0, fmt.Errorf("term exceeds %d bytes", maxRepairSortValueLength)
		}
	}
	return 0, errors.New("unterminated term")
}

// termCursor is one input's position in a field's term dictionary.
type termCursor struct {
	iterator termIterator
	term     []byte
	value    uint64
	input    int
}

func (c *termCursor) load() {
	term, value := c.iterator.Current()
	c.term = append(c.term[:0], term...)
	c.value = value
}

// termHeap orders input cursors by term and then input order, so equal terms
// pop in input order and their renumbered postings concatenate ascending.
type termHeap []*termCursor

func (h termHeap) Len() int { return len(h) }

func (h termHeap) Less(left, right int) bool {
	if order := bytes.Compare(h[left].term, h[right].term); order != 0 {
		return order < 0
	}
	return h[left].input < h[right].input
}

func (h termHeap) Swap(left, right int) { h[left], h[right] = h[right], h[left] }

func (h *termHeap) Push(value any) { *h = append(*h, value.(*termCursor)) }

func (h *termHeap) Pop() any {
	old := *h
	last := old[len(old)-1]
	old[len(old)-1] = nil
	*h = old[:len(old)-1]
	return last
}

// termDocument is one input's posting for the term being merged.
type termDocument struct {
	postings        *roaringpkg.Bitmap
	value           uint64
	frequencyOffset uint64
	survivors       uint64
	input           int
	local           uint32
	oneHit          bool
}

// fieldTermState accumulates one output field's term statistics.
type fieldTermState struct {
	dictionary *dictionaryStage
	frequency  uint64
	identifier bool
}

// writeTerms writes one field's postings, term dictionary and field record.
// The inputs' dictionaries are k-way merged in term order; each merged term's
// renumbered postings are written immediately and the term is inserted into
// the dictionary builder immediately, so no term-to-posting map is built.
//
//nolint:gocyclo // dictionary merge, posting emission and field metadata are kept explicit.
func (m *streamMerger) writeTerms(field *streamField) error {
	cursors := make(termHeap, 0, len(m.inputs))
	defer func() {
		for _, cursor := range cursors {
			_ = cursor.iterator.Close()
		}
	}()
	for inputIndex := range m.inputs {
		input := &m.inputs[inputIndex]
		if input.survivors == 0 {
			continue
		}
		dictionary, dictionaryErr := input.reader.dictionary(field.name)
		if dictionaryErr != nil {
			return dictionaryErr
		}
		if dictionary == nil {
			continue
		}
		iterator, iteratorErr := searchDictionary(dictionary, nil, nil, nil, len(m.inputs))
		if iteratorErr != nil {
			if iteratorDone(iteratorErr) {
				continue
			}
			return corruptError("iterate term dictionary", iteratorErr)
		}
		cursor := &termCursor{iterator: iterator, input: inputIndex}
		cursor.load()
		cursors = append(cursors, cursor)
	}
	heap.Init(&cursors)
	state := fieldTermState{identifier: field.name == identifierField, dictionary: m.newDictionaryStage(field.name)}
	if m.postings == nil {
		m.postings = roaringpkg.New()
	}
	if m.covered == nil {
		m.covered = roaringpkg.New()
	}
	m.covered.Clear()
	var term []byte
	for cursors.Len() > 0 {
		if err := m.ctx.Err(); err != nil {
			return err
		}
		term = append(term[:0], cursors[0].term...)
		m.termDocuments = m.termDocuments[:0]
		for cursors.Len() > 0 && bytes.Equal(cursors[0].term, term) {
			cursor := cursors[0]
			m.termDocuments = append(m.termDocuments, termDocument{input: cursor.input, value: cursor.value})
			if nextErr := cursor.iterator.Next(); nextErr != nil {
				if !iteratorDone(nextErr) {
					return corruptError("iterate term dictionary", nextErr)
				}
				_ = cursor.iterator.Close()
				heap.Pop(&cursors)
				continue
			}
			cursor.load()
			heap.Fix(&cursors, 0)
		}
		if emitErr := m.emitTerm(field.name, term, &state); emitErr != nil {
			return emitErr
		}
	}
	dictionaryOffset, dictionaryErr := state.dictionary.finish(m.output)
	if dictionaryErr != nil {
		return dictionaryErr
	}
	documentCount := m.documentCount
	if state.identifier {
		if state.frequency != m.documentCount {
			return corruptError("merged identifier postings cover %d documents, want %d", state.frequency, m.documentCount)
		}
	} else {
		if field.docValueDocuments != nil {
			m.covered.Or(field.docValueDocuments)
		}
		documentCount = m.covered.GetCardinality()
	}
	field.docValueDocuments = nil
	field.fieldOffset = m.output.written
	record := appendNativeUvarint(m.docValueOut[:0], dictionaryOffset)
	record = appendNativeUvarint(record, uint64(len(field.name)))
	record = append(record, field.name...)
	record = appendNativeUvarint(record, documentCount)
	record = appendNativeUvarint(record, state.frequency)
	m.docValueOut = record
	_, writeErr := m.output.Write(record)
	return writeErr
}

// emitTerm unions one term's input postings, renumbered into output
// documents, and writes the posting exactly as appendNativeICETerms would
// for the same documents and frequencies.
//
//nolint:gocyclo // one-hit, frequency-less and general postings follow the encoder's rules.
func (m *streamMerger) emitTerm(fieldName string, term []byte, state *fieldTermState) error {
	var total uint64
	for index := range m.termDocuments {
		entry := &m.termDocuments[index]
		input := &m.inputs[entry.input]
		if entry.value&fstValueEncodingMask == fstValueEncodingOneHit {
			local := entry.value & fstValueDocumentMask
			if local >= input.physicalCount {
				return fmt.Errorf("term %q has document %d outside segment: %w", term, local, ErrCorrupt)
			}
			entry.oneHit, entry.local = true, uint32(local)
			if !input.removed.Contains(entry.local) {
				entry.survivors = 1
			}
		} else {
			frequencyOffset, postings, decodeErr := input.reader.decodePostingAt(entry.value)
			if decodeErr != nil {
				return decodeErr
			}
			entry.postings, entry.frequencyOffset = postings, frequencyOffset
			entry.survivors = postings.GetCardinality() - postings.AndCardinality(input.removed)
		}
		total += entry.survivors
	}
	if total == 0 {
		return nil
	}
	if total == 1 {
		// Most terms of identifier-like fields have one document; resolve it
		// without building a posting bitmap or a frequency stream.
		outputNumber, frequency, singleErr := m.singleSurvivor(state.identifier)
		if singleErr != nil {
			return singleErr
		}
		if frequency == 0 || state.identifier {
			frequency = 1
		}
		if frequency == 1 && outputNumber <= fstValueDocumentMask {
			if !state.identifier {
				m.covered.Add(uint32(outputNumber))
			}
			state.frequency++
			return state.dictionary.add(term, fstValueEncodingOneHit|(nativeICEOneHitNorm<<31)|outputNumber)
		}
	}
	chunkSize, chunkErr := nativeICEChunkSize(nativeICEChunkModeV1, total, m.documentCount)
	if chunkErr != nil {
		return chunkErr
	}
	chunkCount := (m.documentCount-1)/chunkSize + 1
	m.frequencyLens = m.frequencyLens[:0]
	for len(m.frequencyLens) < int(chunkCount) {
		m.frequencyLens = append(m.frequencyLens, 0)
	}
	stage := state.dictionary.frequencies
	if resetErr := stage.reset(); resetErr != nil {
		return resetErr
	}
	m.postings.Clear()
	m.frequencyRaw = m.frequencyRaw[:0]
	currentChunk := uint64(0)
	compressChunk := func() error {
		m.frequencyOut = s2.EncodeBetter(m.frequencyOut[:cap(m.frequencyOut)], m.frequencyRaw)
		m.frequencyLens[currentChunk] = uint64(len(m.frequencyOut))
		_, writeErr := stage.Write(m.frequencyOut)
		m.frequencyRaw = m.frequencyRaw[:0]
		return writeErr
	}
	var lastDocument, lastFrequency uint64
	accept := func(outputNumber, frequency uint64) error {
		if frequency == 0 || state.identifier {
			frequency = 1
		}
		if outputNumber > math.MaxUint32 {
			return fmt.Errorf("document %d exceeds the posting range: %w", outputNumber, ErrInvalidGeneration)
		}
		if frequency > ^uint64(0)>>1 {
			return fmt.Errorf("document %d has an unrepresentable frequency: %w", outputNumber, ErrInvalidGeneration)
		}
		chunk := outputNumber / chunkSize
		if chunk >= chunkCount {
			return fmt.Errorf("document %d exceeds frequency chunk range: %w", outputNumber, ErrInvalidGeneration)
		}
		if chunk != currentChunk {
			if compressErr := compressChunk(); compressErr != nil {
				return compressErr
			}
			currentChunk = chunk
		}
		m.frequencyRaw = appendNativeUvarint(m.frequencyRaw, frequency<<1)
		m.frequencyRaw = appendNativeUvarint(m.frequencyRaw, uint64(math.Float32bits(1)))
		m.postings.Add(uint32(outputNumber))
		if !state.identifier {
			m.covered.Add(uint32(outputNumber))
		}
		state.frequency += frequency
		lastDocument, lastFrequency = outputNumber, frequency
		return nil
	}
	for index := range m.termDocuments {
		entry := &m.termDocuments[index]
		if entry.survivors == 0 {
			continue
		}
		input := &m.inputs[entry.input]
		if entry.oneHit {
			outputNumber, _ := input.outputNumber(entry.local)
			if acceptErr := accept(outputNumber, 1); acceptErr != nil {
				return acceptErr
			}
			continue
		}
		// The identifier's frequencies are always one, so its streams are
		// never decoded.
		var frequencies *frequencyCursor
		if entry.frequencyOffset != 0 && !state.identifier {
			frequencies = &m.frequencies
			if initErr := frequencies.init(input.reader, entry.frequencyOffset, entry.value, entry.postings.GetCardinality()); initErr != nil {
				return initErr
			}
		}
		iterator := entry.postings.Iterator()
		for iterator.HasNext() {
			local := iterator.Next()
			frequency := uint64(1)
			if frequencies != nil {
				var frequencyErr error
				if frequency, frequencyErr = frequencies.next(local); frequencyErr != nil {
					return frequencyErr
				}
			}
			outputNumber, survives := input.outputNumber(local)
			if !survives {
				continue
			}
			if acceptErr := accept(outputNumber, frequency); acceptErr != nil {
				return acceptErr
			}
		}
		if frequencies != nil {
			if finishErr := frequencies.finish(); finishErr != nil {
				return finishErr
			}
		}
	}
	if compressErr := compressChunk(); compressErr != nil {
		return compressErr
	}
	if total == 1 && lastFrequency == 1 && lastDocument <= fstValueDocumentMask {
		return state.dictionary.add(term, fstValueEncodingOneHit|(nativeICEOneHitNorm<<31)|lastDocument)
	}
	includeFrequency := total > 1 || lastFrequency != 1
	frequencyOffset := uint64(0)
	if includeFrequency {
		frequencyOffset = m.output.written
		header := appendNativeUvarint(m.docValueOut[:0], chunkCount)
		var cumulative uint64
		for _, length := range m.frequencyLens {
			cumulative += length
			header = appendNativeUvarint(header, cumulative)
		}
		m.docValueOut = header
		if _, writeErr := m.output.Write(header); writeErr != nil {
			return writeErr
		}
		if copyErr := stage.copyTo(m.output); copyErr != nil {
			return fmt.Errorf("write term %q frequencies for field %q: %w", term, fieldName, copyErr)
		}
	}
	postingOffset := m.output.written
	header := appendNativeUvarint(m.docValueOut[:0], frequencyOffset)
	header = appendNativeUvarint(header, 0)
	header = appendNativeUvarint(header, m.postings.GetSerializedSizeInBytes())
	m.docValueOut = header
	if _, writeErr := m.output.Write(header); writeErr != nil {
		return writeErr
	}
	if _, writeErr := m.postings.WriteTo(m.output); writeErr != nil {
		return fmt.Errorf("encode posting bitmap: %w", writeErr)
	}
	return state.dictionary.add(term, postingOffset)
}

// singleSurvivor returns the output document and frequency of the merged
// term's only surviving document.
func (m *streamMerger) singleSurvivor(identifier bool) (uint64, uint64, error) {
	for index := range m.termDocuments {
		entry := &m.termDocuments[index]
		if entry.survivors == 0 {
			continue
		}
		input := &m.inputs[entry.input]
		if entry.oneHit {
			outputNumber, _ := input.outputNumber(entry.local)
			return outputNumber, 1, nil
		}
		var frequencies *frequencyCursor
		if entry.frequencyOffset != 0 && !identifier {
			frequencies = &m.frequencies
			if initErr := frequencies.init(input.reader, entry.frequencyOffset, entry.value, entry.postings.GetCardinality()); initErr != nil {
				return 0, 0, initErr
			}
		}
		iterator := entry.postings.Iterator()
		for iterator.HasNext() {
			local := iterator.Next()
			frequency := uint64(1)
			if frequencies != nil {
				var frequencyErr error
				if frequency, frequencyErr = frequencies.next(local); frequencyErr != nil {
					return 0, 0, frequencyErr
				}
			}
			if outputNumber, survives := input.outputNumber(local); survives {
				return outputNumber, frequency, nil
			}
		}
	}
	return 0, 0, corruptError("merged term lost its surviving document")
}

// frequencyCursor streams one input posting's ICE frequency stream, decoding
// one chunk at a time as the posting's documents advance.
type frequencyCursor struct {
	reader        *storedSegmentReader
	offsets       []uint64
	compressed    []byte
	decoded       []byte
	decoder       byteDecoder
	chunkSize     uint64
	dataStart     uint64
	currentChunk  uint64
	postingOffset uint64
	loaded        bool
}

func (c *frequencyCursor) init(reader *storedSegmentReader, frequencyOffset, postingOffset, cardinality uint64) error {
	c.reader, c.postingOffset, c.loaded = reader, postingOffset, false
	if frequencyOffset >= postingOffset {
		return corruptError("segment %q has an invalid frequency stream offset", reader.path)
	}
	cursor := frequencyOffset
	chunkCount, countErr := reader.readUvarint(&cursor, postingOffset)
	if countErr != nil {
		return countErr
	}
	if chunkCount == 0 || chunkCount > maxFrequencyChunkCount {
		return corruptError("segment %q has an invalid frequency chunk count", reader.path)
	}
	chunkSize, chunkErr := nativeICEChunkSize(reader.footer.chunkMode, cardinality, reader.footer.documentCount)
	if chunkErr != nil {
		return chunkErr
	}
	if expected := (reader.footer.documentCount-1)/chunkSize + 1; chunkCount != expected {
		return corruptError("segment %q has %d frequency chunks, want %d", reader.path, chunkCount, expected)
	}
	c.chunkSize = chunkSize
	c.offsets = c.offsets[:0]
	var previous uint64
	for index := uint64(0); index < chunkCount; index++ {
		offset, offsetErr := reader.readUvarint(&cursor, postingOffset)
		if offsetErr != nil {
			return offsetErr
		}
		if offset < previous || offset > postingOffset-cursor {
			return corruptError("segment %q has invalid frequency chunk offsets", reader.path)
		}
		c.offsets = append(c.offsets, offset)
		previous = offset
	}
	c.dataStart = cursor
	if c.dataStart > postingOffset || previous > postingOffset-c.dataStart {
		return corruptError("segment %q has frequency chunk data outside its posting section", reader.path)
	}
	return nil
}

func (c *frequencyCursor) next(document uint32) (uint64, error) {
	chunk := uint64(document) / c.chunkSize
	if !c.loaded || chunk != c.currentChunk {
		if finishErr := c.finish(); finishErr != nil {
			return 0, finishErr
		}
		if loadErr := c.load(chunk); loadErr != nil {
			return 0, loadErr
		}
	}
	encodedFrequency, frequencyErr := c.decoder.uvarint()
	if frequencyErr != nil {
		return 0, frequencyErr
	}
	frequency := encodedFrequency >> 1
	if frequency == 0 {
		return 0, corruptError("segment %q has an invalid term frequency", c.reader.path)
	}
	if _, normErr := c.decoder.uvarint(); normErr != nil {
		return 0, normErr
	}
	return frequency, nil
}

func (c *frequencyCursor) load(chunk uint64) error {
	if chunk >= uint64(len(c.offsets)) {
		return corruptError("segment %q has postings outside its frequency chunks", c.reader.path)
	}
	start := uint64(0)
	if chunk > 0 {
		start = c.offsets[chunk-1]
	}
	end := c.offsets[chunk]
	if end == start {
		return corruptError("segment %q has no frequency data for chunk %d", c.reader.path, chunk)
	}
	length := end - start
	if length > maxFrequencyCompressedSize {
		return corruptError("segment %q has an oversized frequency chunk", c.reader.path)
	}
	if uint64(cap(c.compressed)) < length {
		c.compressed = make([]byte, length)
	}
	c.compressed = c.compressed[:length]
	if readErr := c.reader.readInto(c.dataStart+start, c.compressed); readErr != nil {
		return readErr
	}
	decoded, decodeErr := decodeICEFrequencyChunkInto(c.decoded, c.compressed)
	if decodeErr != nil {
		return corruptError("decode frequency chunk in segment %q: %w", c.reader.path, decodeErr)
	}
	c.decoded = decoded
	c.decoder = byteDecoder{payload: decoded}
	c.currentChunk, c.loaded = chunk, true
	return nil
}

// finish verifies the loaded chunk was consumed exactly.
func (c *frequencyCursor) finish() error {
	if c.loaded && c.decoder.remaining() != 0 {
		return corruptError("segment %q has trailing frequency bytes in chunk %d", c.reader.path, c.currentChunk)
	}
	c.loaded = false
	return nil
}

func decodeICEFrequencyChunkInto(destination, compressed []byte) (decoded []byte, err error) {
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
	if cap(destination) < decodedLength {
		destination = make([]byte, decodedLength)
	}
	decoded, decodeErr := s2.Decode(destination[:decodedLength], compressed)
	if decodeErr != nil {
		return nil, decodeErr
	}
	if len(decoded) != decodedLength {
		return nil, fmt.Errorf("decoded frequency chunk length %d, want %d", len(decoded), decodedLength)
	}
	return decoded, nil
}

// dictionaryStage builds one field's term dictionary as terms arrive in
// order. vellum's registry size depends on the dictionary's term count, so
// the first terms are held until the count crosses the largest sized class;
// beyond it the default class is fixed and terms stream straight into the
// builder. The builder's output is staged in a spill buffer because the
// dictionary's length precedes its bytes.
type dictionaryStage struct {
	builder     *vellum.Builder
	output      *spillBuffer
	frequencies *spillBuffer
	field       string
	arena       []byte
	pending     []pendingTerm
	class       int
	terms       int
}

type pendingTerm struct {
	value uint64
	end   int
}

func (m *streamMerger) newDictionaryStage(field string) *dictionaryStage {
	if m.dictionary == nil {
		m.dictionary = &dictionaryStage{output: m.newSpillBuffer(), frequencies: m.newSpillBuffer()}
	}
	stage := m.dictionary
	stage.field, stage.builder, stage.terms = field, nil, 0
	stage.arena, stage.pending = stage.arena[:0], stage.pending[:0]
	return stage
}

func (d *dictionaryStage) add(term []byte, value uint64) error {
	d.terms++
	if d.builder == nil {
		largestSized := dictionaryBuilderClasses[len(dictionaryBuilderClasses)-2].maxTerms
		if d.terms <= largestSized {
			d.arena = append(d.arena, term...)
			d.pending = append(d.pending, pendingTerm{end: len(d.arena), value: value})
			return nil
		}
		if startErr := d.start(len(dictionaryBuilderClasses) - 1); startErr != nil {
			return startErr
		}
	}
	return d.insert(term, value)
}

func (d *dictionaryStage) start(class int) error {
	if resetErr := d.output.reset(); resetErr != nil {
		return resetErr
	}
	builder, builderErr := acquireDictionaryBuilder(d.output, class)
	if builderErr != nil {
		return fmt.Errorf("create term dictionary for field %q: %w", d.field, builderErr)
	}
	d.builder, d.class = builder, class
	start := 0
	for _, pending := range d.pending {
		if insertErr := d.insert(d.arena[start:pending.end], pending.value); insertErr != nil {
			return insertErr
		}
		start = pending.end
	}
	d.arena, d.pending = d.arena[:0], d.pending[:0]
	return nil
}

func (d *dictionaryStage) insert(term []byte, value uint64) error {
	if insertErr := d.builder.Insert(term, value); insertErr != nil {
		closeErr := d.builder.Close()
		d.builder = nil
		return errors.Join(fmt.Errorf("insert term into dictionary for field %q: %w", d.field, insertErr), closeErr)
	}
	return nil
}

// finish closes the dictionary and writes it as uvarint(length) followed by
// the FST bytes, returning its offset, or zero for a field without terms.
func (d *dictionaryStage) finish(output *countingWriter) (uint64, error) {
	if d.terms == 0 {
		return 0, nil
	}
	if d.builder == nil {
		if startErr := d.start(dictionaryBuilderClass(d.terms)); startErr != nil {
			return 0, startErr
		}
	}
	builder := d.builder
	d.builder = nil
	if closeErr := builder.Close(); closeErr != nil {
		return 0, fmt.Errorf("close term dictionary for field %q: %w", d.field, closeErr)
	}
	dictionaryBuilderClasses[d.class].pool.Put(builder)
	dictionaryOffset := output.written
	if writeErr := output.writeUvarint(d.output.Len()); writeErr != nil {
		return 0, writeErr
	}
	if copyErr := d.output.copyTo(output); copyErr != nil {
		return 0, fmt.Errorf("write term dictionary for field %q: %w", d.field, copyErr)
	}
	return dictionaryOffset, d.output.reset()
}
