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
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"

	roaringpkg "github.com/RoaringBitmap/roaring"
)

// errFieldResolved stops an early-exit walk once a field's presence is known.
var errFieldResolved = errors.New("nativeice: merge field resolved")

// streamInput is one merge input's renumbering and read state. removed is
// the union of the input's snapshot deletion mask and its Drop mask, limited
// to the input's physical documents; a surviving document's output number is
// base plus its local number minus the removed documents before it.
type streamInput struct {
	reader         *storedSegmentReader
	removed        *roaringpkg.Bitmap
	docValueRanges map[string][2]uint64
	indexedFields  []string
	physicalCount  uint64
	survivors      uint64
	base           uint64
}

// outputNumber maps a surviving local document to its output number.
func (in *streamInput) outputNumber(local uint32) (uint64, bool) {
	if in.removed.IsEmpty() {
		return in.base + uint64(local), true
	}
	if in.removed.Contains(local) {
		return 0, false
	}
	return in.base + uint64(local) - in.removed.Rank(local), true
}

// streamField is one output field's metadata. Only per-field scalars and a
// compressed bitmap of documents carrying doc values are retained across
// sections.
type streamField struct {
	docValueDocuments *roaringpkg.Bitmap
	name              string
	docValueStart     uint64
	docValueEnd       uint64
	fieldOffset       uint64
}

//nolint:govet // merge state is grouped by section.
type streamMerger struct {
	ctx           context.Context
	output        *countingWriter
	spill         spillFactory
	inputs        []streamInput
	fields        []streamField
	fieldIDs      map[string]uint64
	documentCount uint64
	storedIndex   uint64

	// Time bounds over the surviving timestamped documents, in the same
	// uint64(int64) form the flush path writes into the footer. hasTime is
	// false when no surviving document carried a timestamp, which leaves the
	// footer slots at zero and marks the segment as having no time bounds.
	timeMin uint64
	timeMax uint64
	hasTime bool

	// Reusable section buffers: the merge keeps one steady working set
	// instead of allocating per document or per term.
	storedValues  []storedValue
	storedFields  []storedField
	storedMeta    []byte
	decodedChunk  []byte
	encodedChunk  []byte
	chunkOffsets  []uint64
	docValueBuf   []byte
	docValueHead  []docValueHeader
	docValueOut   []byte
	frequencyRaw  []byte
	frequencyOut  []byte
	frequencyLens []uint64
	postings      *roaringpkg.Bitmap
	covered       *roaringpkg.Bitmap
	termDocuments []termDocument
	staged        []*spillBuffer
	offsets       *spillBuffer
	dictionary    *dictionaryStage
	frequencies   frequencyCursor
}

type docValueHeader struct {
	documentNumber uint64
	valueEnd       uint64
}

func (m *streamMerger) newSpillBuffer() *spillBuffer {
	buffer := m.spill.newBuffer()
	m.staged = append(m.staged, buffer)
	return buffer
}

// stagedOffsets returns the merger's reusable buffer for the stored offset
// index.
func (m *streamMerger) stagedOffsets() *spillBuffer {
	if m.offsets == nil {
		m.offsets = m.newSpillBuffer()
	}
	return m.offsets
}

// prepare validates the inputs, computes every input's renumbering and the
// output field set. The field set must be complete before the first stored
// document is written, because stored values record their field's position
// in the output field table.
func (m *streamMerger) prepare(inputs []MergeInput) error {
	m.inputs = make([]streamInput, len(inputs))
	names := map[string]struct{}{identifierField: {}}
	addField := func(name string) error {
		if name == "" {
			return fmt.Errorf("merged document field has no name: %w", ErrInvalidGeneration)
		}
		names[name] = struct{}{}
		return nil
	}
	for inputIndex, input := range inputs {
		if input.Reader == nil || input.Reader.SegmentCount() != 1 {
			return fmt.Errorf("merge input %d must hold one segment: %w", inputIndex, ErrCorrupt)
		}
		state, stateErr := newStreamInput(input, m.documentCount)
		if stateErr != nil {
			return stateErr
		}
		m.inputs[inputIndex] = state
		m.documentCount += state.survivors
		if state.survivors == 0 {
			continue
		}
		for _, name := range input.IndexedFields {
			if name == identifierField {
				continue
			}
			if fieldErr := addField(name); fieldErr != nil {
				return fieldErr
			}
		}
		if resolveErr := m.resolveInputFields(&m.inputs[inputIndex], names, addField); resolveErr != nil {
			return resolveErr
		}
	}
	if m.documentCount > math.MaxUint32+1 {
		return fmt.Errorf("merged segment of %d documents exceeds the posting range: %w", m.documentCount, ErrInvalidGeneration)
	}
	ordered := make([]string, 0, len(names))
	for name := range names {
		if name != identifierField {
			ordered = append(ordered, name)
		}
	}
	sort.Strings(ordered)
	// The identifier keeps the first field slot, as orderNativeICEFields does.
	ordered = append([]string{identifierField}, ordered...)
	m.fields = make([]streamField, len(ordered))
	m.fieldIDs = make(map[string]uint64, len(ordered))
	for fieldIndex, name := range ordered {
		m.fields[fieldIndex] = streamField{name: name, docValueStart: math.MaxUint64, docValueEnd: math.MaxUint64}
		m.fieldIDs[name] = uint64(fieldIndex)
	}
	return nil
}

func newStreamInput(input MergeInput, base uint64) (streamInput, error) {
	reader, readerErr := input.Reader.storedReader(0)
	if readerErr != nil {
		return streamInput{}, readerErr
	}
	physicalCount := reader.footer.documentCount
	removed, deletionErr := deletedDocuments(input.Reader.segments[0].record)
	if deletionErr != nil {
		return streamInput{}, deletionErr
	}
	if input.Drop != nil {
		removed.Or(input.Drop)
	}
	if physicalCount <= math.MaxUint32 {
		removed.RemoveRange(physicalCount, math.MaxUint32+1)
	}
	removedCount := removed.GetCardinality()
	if removedCount > physicalCount {
		return streamInput{}, corruptError("segment %q removes more documents than it holds", reader.path)
	}
	ranges, rangesErr := docValueRanges(reader)
	if rangesErr != nil {
		return streamInput{}, rangesErr
	}
	return streamInput{
		reader: reader, removed: removed, docValueRanges: ranges, indexedFields: input.IndexedFields,
		physicalCount: physicalCount, survivors: physicalCount - removedCount, base: base,
	}, nil
}

// docValueRanges reads an input's per-field doc-value section bounds once.
func docValueRanges(reader *storedSegmentReader) (map[string][2]uint64, error) {
	if reader.footer.documentCount == 0 || reader.footer.docValueOffset == math.MaxUint64 {
		return nil, nil
	}
	ranges := make(map[string][2]uint64)
	locationOffset := reader.footer.docValueOffset
	for fieldIndex, fieldName := range reader.fieldNames {
		fieldStart, startErr := reader.readUvarint(&locationOffset, reader.footer.fieldsIndexOffset)
		if startErr != nil {
			return nil, startErr
		}
		fieldEnd, endErr := reader.readUvarint(&locationOffset, reader.footer.fieldsIndexOffset)
		if endErr != nil {
			return nil, endErr
		}
		if fieldStart == math.MaxUint64 || fieldEnd == math.MaxUint64 {
			if fieldStart != math.MaxUint64 || fieldEnd != math.MaxUint64 {
				return nil, corruptError("segment %q has an incomplete doc-value location for field %d", reader.path, fieldIndex)
			}
			continue
		}
		ranges[fieldName] = [2]uint64{fieldStart, fieldEnd}
	}
	return ranges, nil
}

// resolveInputFields adds every field of a surviving input that the merged
// segment carries: a field with a surviving term, a surviving doc value, or a
// surviving stored value. Cheap checks run first and each stops at the first
// surviving hit, so the stored walk runs only for fields no other modality
// already resolved, and stops as soon as all are resolved.
func (m *streamMerger) resolveInputFields(input *streamInput, names map[string]struct{}, addField func(string) error) error {
	inputFields := input.reader.fieldNames
	unresolved := make(map[string]struct{})
	for _, name := range inputFields {
		if name == identifierField {
			continue
		}
		if _, known := names[name]; !known {
			unresolved[name] = struct{}{}
		}
	}
	for name := range unresolved {
		if err := m.ctx.Err(); err != nil {
			return err
		}
		found, termErr := input.hasSurvivingTerm(name)
		if termErr != nil {
			return termErr
		}
		if !found {
			found, termErr = input.hasSurvivingDocValue(name)
			if termErr != nil {
				return termErr
			}
		}
		if found {
			if fieldErr := addField(name); fieldErr != nil {
				return fieldErr
			}
			delete(unresolved, name)
		}
	}
	if len(unresolved) == 0 {
		return nil
	}
	walkErr := m.walkStoredDocuments(input.reader, func(local uint64, fields []storedField) error {
		if input.removed.Contains(uint32(local)) {
			return nil
		}
		for _, field := range fields {
			if _, pending := unresolved[field.name]; pending {
				if fieldErr := addField(field.name); fieldErr != nil {
					return fieldErr
				}
				delete(unresolved, field.name)
			}
		}
		if len(unresolved) == 0 {
			return errFieldResolved
		}
		return nil
	})
	if errors.Is(walkErr, errFieldResolved) {
		return nil
	}
	return walkErr
}

func (in *streamInput) hasSurvivingTerm(field string) (bool, error) {
	dictionary, dictionaryErr := in.reader.dictionary(field)
	if dictionaryErr != nil || dictionary == nil {
		return false, dictionaryErr
	}
	iterator, iteratorErr := dictionary.Iterator(nil, nil)
	if iteratorErr != nil {
		if iteratorDone(iteratorErr) {
			return false, nil
		}
		return false, corruptError("iterate term dictionary", iteratorErr)
	}
	defer func() { _ = iterator.Close() }()
	for {
		_, value := iterator.Current()
		survives, surviveErr := in.postingSurvives(value)
		if surviveErr != nil || survives {
			return survives, surviveErr
		}
		if nextErr := iterator.Next(); nextErr != nil {
			if iteratorDone(nextErr) {
				return false, nil
			}
			return false, corruptError("iterate term dictionary", nextErr)
		}
	}
}

func (in *streamInput) postingSurvives(value uint64) (bool, error) {
	if value&fstValueEncodingMask == fstValueEncodingOneHit {
		local := value & fstValueDocumentMask
		if local >= in.physicalCount {
			return false, corruptError("segment %q has an out-of-range single-hit posting", in.reader.path)
		}
		return !in.removed.Contains(uint32(local)), nil
	}
	_, postings, decodeErr := in.reader.decodePostingAt(value)
	if decodeErr != nil {
		return false, decodeErr
	}
	return postings.GetCardinality() > postings.AndCardinality(in.removed), nil
}

func (in *streamInput) hasSurvivingDocValue(field string) (bool, error) {
	fieldRange, found := in.docValueRanges[field]
	if !found {
		return false, nil
	}
	valueReader, readerErr := newRepairDocValueReader(in.reader, fieldRange[0], fieldRange[1])
	if readerErr != nil {
		return false, readerErr
	}
	survives := false
	visitErr := visitDocValueEntries(valueReader, func(local uint64, encoded []byte) error {
		if len(encoded) > 0 && !in.removed.Contains(uint32(local)) {
			survives = true
			return errFieldResolved
		}
		return nil
	})
	if errors.Is(visitErr, errFieldResolved) {
		return survives, nil
	}
	return survives, visitErr
}

// visitDocValueEntries walks a doc-value column chunk by chunk, handing visit
// each document's encoded value run. The run is borrowed from the reader's
// chunk buffer until visit returns.
func visitDocValueEntries(valueReader *repairDocValueReader, visit func(uint64, []byte) error) error {
	for chunkNumber := uint64(0); chunkNumber < uint64(len(valueReader.chunkOffsets)); chunkNumber++ {
		if loadErr := valueReader.loadChunk(chunkNumber); loadErr != nil {
			return loadErr
		}
		start := uint64(0)
		for _, entry := range valueReader.header {
			if entry.valueEnd < start || entry.valueEnd > uint64(len(valueReader.decodedBuffer)) {
				return corruptError("segment %q has invalid doc-value bounds in a chunk", valueReader.path)
			}
			if visitErr := visit(entry.documentNumber, valueReader.decodedBuffer[start:entry.valueEnd]); visitErr != nil {
				return visitErr
			}
			start = entry.valueEnd
		}
	}
	return nil
}

func (m *streamMerger) write() error {
	if storedErr := m.writeStored(); storedErr != nil {
		return storedErr
	}
	hasDocValues := false
	for fieldIndex := range m.fields {
		if docValueErr := m.writeDocValues(&m.fields[fieldIndex]); docValueErr != nil {
			return docValueErr
		}
		if m.fields[fieldIndex].docValueStart != math.MaxUint64 {
			hasDocValues = true
		}
	}
	for fieldIndex := range m.fields {
		if termsErr := m.writeTerms(&m.fields[fieldIndex]); termsErr != nil {
			return termsErr
		}
	}
	docValueOffset := uint64(math.MaxUint64)
	if hasDocValues {
		docValueOffset = m.output.written
		for _, field := range m.fields {
			if writeErr := m.output.writeUvarint(field.docValueStart); writeErr != nil {
				return writeErr
			}
			if writeErr := m.output.writeUvarint(field.docValueEnd); writeErr != nil {
				return writeErr
			}
		}
	}
	fieldsIndexOffset := m.output.written
	for _, field := range m.fields {
		if writeErr := m.output.writeUint64(field.fieldOffset); writeErr != nil {
			return writeErr
		}
	}
	footer := make([]byte, segmentFooterLength)
	binary.BigEndian.PutUint64(footer[0:8], m.documentCount)
	binary.BigEndian.PutUint64(footer[8:16], m.storedIndex)
	binary.BigEndian.PutUint64(footer[16:24], fieldsIndexOffset)
	binary.BigEndian.PutUint64(footer[24:32], docValueOffset)
	binary.BigEndian.PutUint32(footer[32:36], nativeICEChunkModeV1)
	// The time bounds a flush segment already writes into these slots. Writing
	// them here as well is what lets a query skip or narrow a merged segment
	// instead of decoding every candidate it holds; leaving them at zero is what
	// previously made every merged segment report "no timestamps".
	binary.BigEndian.PutUint64(footer[36:44], m.timeMin)
	binary.BigEndian.PutUint64(footer[44:52], m.timeMax)
	binary.BigEndian.PutUint32(footer[52:56], segmentVersion)
	_, writeErr := m.output.Write(footer)
	return writeErr
}
