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
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package inverted

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"sync"

	roaringpkg "github.com/RoaringBitmap/roaring"

	"github.com/apache/skywalking-banyandb/pkg/index/internal/nativeice"
)

type nativePluginField struct {
	terms     map[string][]uint64
	termFreqs map[string]map[uint64]uint64
	docCount  uint64
	frequency uint64
}

type nativePluginDocument struct {
	terms     map[string][][]byte
	termFreqs map[string]map[string]uint64
	docValues map[string][][]byte
	modes     map[string]nativePluginMode
	fields    []nativeice.DecodedField
}
type nativePluginMode struct{ index, store, sort bool }

// nativeMergeTerm is the compact per-document representation used while
// rewriting a reader-backed segment. Keeping the frequency beside its term
// avoids the string-keyed maps materialize used to retain for every document.
type nativeMergeTerm struct {
	value     []byte
	frequency uint64
}

type nativeMergeField struct {
	name       string
	values     [][]byte
	sortValues [][]byte
	terms      []nativeMergeTerm
	mode       nativePluginMode
}

type nativeMergeDocument struct {
	fields []nativeMergeField
}

func nativeMergeValuesEqual(left, right [][]byte) bool {
	if len(left) != len(right) {
		return false
	}
	for valueIndex := range left {
		if !bytes.Equal(left[valueIndex], right[valueIndex]) {
			return false
		}
	}
	return true
}

func (d *nativeMergeDocument) field(name string) *nativeMergeField {
	for fieldIndex := range d.fields {
		if d.fields[fieldIndex].name == name {
			return &d.fields[fieldIndex]
		}
	}
	d.fields = append(d.fields, nativeMergeField{name: name})
	return &d.fields[len(d.fields)-1]
}

//nolint:govet // field order keeps the segment state grouped by lifecycle role.
type nativePluginSegment struct {
	fields             map[string]*nativePluginField
	frequencies        map[string]uint64
	emptyIndexedFields map[string]struct{}
	documents          []nativePluginDocument
	payload            []byte
	data               *segmentBytes
	reader             *nativeice.Reader
	readerDictionaries map[string]*nativeice.Dictionary
	dictionaryWrappers map[string]segmentDictionary
	fieldNames         []string
	lazyMu             sync.Mutex
	materializeMu      sync.Mutex
	dictionaryMu       sync.Mutex
	materialized       bool
	mergeDocuments     []nativeMergeDocument
	mergePrepared      bool
	timeMin            int64
	timeMax            int64
	decoded            bool
}

func nativeSegmentPluginNew(results []segmentDocument, normCalc func(string, int) float32) (segmentValue, uint64, error) {
	_ = normCalc
	generation := nativeice.Generation{SegmentID: 1, SnapshotID: 1}
	for _, result := range results {
		if result == nil {
			return nil, 0, errors.New("inverted: nil segment document")
		}
		encoded := nativeice.EncodeDocument{}
		result.EachField(func(field segmentField) {
			name := field.Name()
			value := append([]byte(nil), field.Value()...)
			if name == docIDField && field.Store() {
				encoded.Identifier = append([]byte(nil), value...)
			}
			var analyzedTerms []nativeice.EncodeTerm
			if field.Index() {
				analyzedTerms = make([]nativeice.EncodeTerm, 0, field.Length())
				field.EachTerm(func(term segmentFieldTerm) {
					termValue := append([]byte(nil), term.Term()...)
					analyzedTerms = append(analyzedTerms, nativeice.EncodeTerm{Value: termValue, Frequency: uint64(term.Frequency())})
				})
			}
			if name != docIDField {
				encoded.Fields = append(encoded.Fields, nativeice.EncodeField{
					Name: name, Value: value, Terms: analyzedTerms, Index: field.Index(), Store: field.Store(), Sort: field.IndexDocValues(),
				})
			}
		})
		if len(encoded.Identifier) == 0 {
			return nil, 0, errors.New("inverted: every segment document needs a stored _id")
		}
		generation.Documents = append(generation.Documents, encoded)
		// ICE v3 segment footers reserve timestamp bounds; the native encoder
		// deliberately writes zero because the lifecycle contract does not carry
		// document time bounds.
	}
	if len(generation.Documents) != len(results) {
		return nil, 0, errors.New("inverted: every segment document needs a stored _id")
	}
	payload, encodeErr := nativeice.EncodeSegment(generation)
	if encodeErr != nil {
		return nil, 0, encodeErr
	}
	reader, openErr := nativeice.OpenSegmentBorrowed(payload)
	if openErr != nil {
		return nil, 0, openErr
	}
	fieldNames, fieldsErr := reader.Fields()
	if fieldsErr != nil {
		_ = reader.Close()
		return nil, 0, fieldsErr
	}
	timeMin, timeMax := reader.TimeBounds()
	return &nativePluginSegment{
		payload: payload, reader: reader, fields: make(map[string]*nativePluginField),
		readerDictionaries: make(map[string]*nativeice.Dictionary), dictionaryWrappers: make(map[string]segmentDictionary),
		frequencies: make(map[string]uint64), emptyIndexedFields: make(map[string]struct{}),
		fieldNames: fieldNames, timeMin: timeMin, timeMax: timeMax, decoded: true,
	}, uint64(len(payload)), nil
}

// nativeSegmentPluginLoad validates the segment framing and keeps the native
// reader as the source of truth. Stored fields, dictionaries, postings, and
// doc values are decoded when the segment API asks for them rather than when
// an index writer reopens a generation.
func nativeSegmentPluginLoad(data *segmentBytes) (segmentValue, error) {
	if data == nil || data.Len() == 0 {
		return nil, errors.New("inverted: empty segment")
	}
	payload, readErr := data.Read(0, data.Len())
	if readErr != nil {
		return nil, readErr
	}
	reader, openErr := nativeice.OpenSegmentBorrowed(payload)
	if openErr != nil {
		return nil, openErr
	}
	fieldNames, fieldsErr := reader.Fields()
	if fieldsErr != nil {
		return nil, fieldsErr
	}
	timeMin, timeMax := reader.TimeBounds()
	return &nativePluginSegment{
		data: data, reader: reader, fields: make(map[string]*nativePluginField),
		readerDictionaries: make(map[string]*nativeice.Dictionary), dictionaryWrappers: make(map[string]segmentDictionary),
		frequencies: make(map[string]uint64), emptyIndexedFields: make(map[string]struct{}),
		documents: nil, fieldNames: fieldNames, timeMin: timeMin, timeMax: timeMax, decoded: true,
	}, nil
}

// materialize decodes a lazily loaded segment only when a merge needs to
// rewrite every field. Query and scan paths remain payload-backed.
//
//nolint:gocyclo // materialization preserves every native segment modality.
func (s *nativePluginSegment) materialize(closeCh chan struct{}) error {
	s.materializeMu.Lock()
	defer s.materializeMu.Unlock()
	if s.reader == nil || s.materialized {
		return nil
	}
	if storedErr := s.ensureStoredDocuments(closeCh); storedErr != nil {
		return storedErr
	}
	fieldNames, fieldsErr := s.reader.Fields()
	if fieldsErr != nil {
		return fieldsErr
	}
	for _, fieldName := range fieldNames {
		if mergeCloseRequested(closeCh) {
			return errors.New("inverted: merge canceled")
		}
		postingsErr := s.reader.VisitTermPostings(fieldName, nil, func(term []byte, memberships []uint64, frequencies []nativeice.TermFrequency) error {
			if mergeCloseRequested(closeCh) {
				return errors.New("inverted: merge canceled")
			}
			for _, number := range memberships {
				if int(number) < len(s.documents) {
					if s.documents[number].terms == nil {
						s.documents[number].terms = make(map[string][][]byte)
					}
					s.documents[number].terms[fieldName] = append(s.documents[number].terms[fieldName], append([]byte(nil), term...))
				}
			}
			for _, frequency := range frequencies {
				if frequency.DocumentNumber < uint64(len(s.documents)) {
					if s.documents[frequency.DocumentNumber].termFreqs[fieldName] == nil {
						s.documents[frequency.DocumentNumber].termFreqs[fieldName] = make(map[string]uint64)
					}
					s.documents[frequency.DocumentNumber].termFreqs[fieldName][string(term)] = frequency.Frequency
				}
			}
			return nil
		})
		if postingsErr != nil {
			return postingsErr
		}
		valuesErr := s.reader.VisitFieldDocumentValues(fieldName, func(documentNumber uint64, documentValues [][]byte) error {
			if mergeCloseRequested(closeCh) {
				return errors.New("inverted: merge canceled")
			}
			if documentNumber >= uint64(len(s.documents)) || len(documentValues) == 0 {
				return nil
			}
			if s.documents[documentNumber].docValues == nil {
				s.documents[documentNumber].docValues = make(map[string][][]byte)
			}
			for _, value := range documentValues {
				s.documents[documentNumber].docValues[fieldName] = append(s.documents[documentNumber].docValues[fieldName], append([]byte(nil), value...))
			}
			return nil
		})
		if valuesErr != nil {
			return valuesErr
		}
	}
	for documentIndex := range s.documents {
		s.documents[documentIndex].modes = make(map[string]nativePluginMode)
		for name := range s.documents[documentIndex].terms {
			s.documents[documentIndex].modes[name] = nativePluginMode{index: true}
		}
		for name := range s.documents[documentIndex].docValues {
			mode := s.documents[documentIndex].modes[name]
			mode.sort = true
			s.documents[documentIndex].modes[name] = mode
		}
		for _, field := range s.documents[documentIndex].fields {
			mode := s.documents[documentIndex].modes[field.Name]
			mode.store = true
			if field.Name == docIDField {
				mode.index = true
			}
			s.documents[documentIndex].modes[field.Name] = mode
		}
	}
	for _, fieldName := range fieldNames {
		represented := false
		for documentIndex := range s.documents {
			if len(s.documents[documentIndex].terms[fieldName]) > 0 || len(s.documents[documentIndex].docValues[fieldName]) > 0 {
				represented = true
				break
			}
			for _, field := range s.documents[documentIndex].fields {
				if field.Name == fieldName {
					represented = true
					break
				}
			}
			if represented {
				break
			}
		}
		if !represented {
			s.emptyIndexedFields[fieldName] = struct{}{}
		}
		_, frequency, statsErr := s.reader.FieldStats(fieldName)
		if statsErr != nil {
			return statsErr
		}
		s.frequencies[fieldName] = frequency
	}
	// Keep the reader alive after materialization. Writer introductions can
	// query a segment concurrently with a background merge; clearing this
	// pointer would let a racing DocsMatchingTerms observe it as non-nil and
	// then dereference nil. The reader is immutable and remains the query
	// source, while the materialized maps serve merge serialization.
	s.materialized = true
	return nil
}

// prepareMergeDocuments streams a reader-backed segment into compact field
// slots. Unlike materialize, it does not retain one string-keyed terms,
// frequency and doc-value map per document. The resulting slots are owned by
// the merge and are discarded with the segment after EncodeSegment returns.
// The reader remains immutable and live, so queries can continue concurrently.
// Plugin readers are opened from raw segment bytes and therefore have no
// snapshot deletion mask; merger drops are the sole deletion source here and
// retain the segment's physical document numbering.
//
//nolint:gocyclo // each stream contributes one independent field modality.
func (s *nativePluginSegment) prepareMergeDocuments(closeCh chan struct{}) error {
	s.materializeMu.Lock()
	defer s.materializeMu.Unlock()
	if s.reader == nil || s.mergePrepared {
		return nil
	}
	visibleCount, countErr := s.reader.VisibleDocCount()
	if countErr != nil {
		return countErr
	}
	documents := make([]nativeMergeDocument, 0, visibleCount)
	visitContext, stopContext := mergeContext(closeCh)
	visitErr := s.reader.VisitLiveDocuments(visitContext, func(document nativeice.StoredDocument) error {
		if mergeCloseRequested(closeCh) {
			return errors.New("inverted: merge canceled")
		}
		decoded := nativeMergeDocument{}
		if fieldsErr := document.VisitStoredFields(func(name string, value []byte) bool {
			field := decoded.field(name)
			field.values = append(field.values, append([]byte(nil), value...))
			field.mode.store = true
			if name == docIDField {
				field.mode.index = true
			}
			return true
		}); fieldsErr != nil {
			return fieldsErr
		}
		documents = append(documents, decoded)
		return nil
	})
	stopContext()
	if errors.Is(visitErr, context.Canceled) && mergeCloseRequested(closeCh) {
		return errors.New("inverted: merge canceled")
	}
	if visitErr != nil {
		return visitErr
	}
	for _, fieldName := range s.fieldNames {
		if mergeCloseRequested(closeCh) {
			return errors.New("inverted: merge canceled")
		}
		postingsErr := s.reader.VisitTermPostings(fieldName, nil, func(term []byte, memberships []uint64, frequencies []nativeice.TermFrequency) error {
			if mergeCloseRequested(closeCh) {
				return errors.New("inverted: merge canceled")
			}
			for membershipIndex, number := range memberships {
				if number >= uint64(len(documents)) {
					continue
				}
				frequency := uint64(1)
				if membershipIndex < len(frequencies) && frequencies[membershipIndex].DocumentNumber == number {
					frequency = frequencies[membershipIndex].Frequency
				}
				field := documents[number].field(fieldName)
				field.mode.index = true
				field.terms = append(field.terms, nativeMergeTerm{value: append([]byte(nil), term...), frequency: frequency})
			}
			return nil
		})
		if postingsErr != nil {
			return postingsErr
		}
		valuesErr := s.reader.VisitFieldDocumentValues(fieldName, func(documentNumber uint64, values [][]byte) error {
			if mergeCloseRequested(closeCh) {
				return errors.New("inverted: merge canceled")
			}
			if documentNumber >= uint64(len(documents)) || len(values) == 0 {
				return nil
			}
			field := documents[documentNumber].field(fieldName)
			field.mode.sort = true
			for _, value := range values {
				field.sortValues = append(field.sortValues, append([]byte(nil), value...))
			}
			return nil
		})
		if valuesErr != nil {
			return valuesErr
		}
		represented := false
		for documentIndex := range documents {
			for fieldIndex := range documents[documentIndex].fields {
				if documents[documentIndex].fields[fieldIndex].name == fieldName {
					represented = true
					break
				}
			}
			if represented {
				break
			}
		}
		if !represented {
			s.emptyIndexedFields[fieldName] = struct{}{}
		}
	}
	s.mergeDocuments = documents
	s.mergePrepared = true
	return nil
}

func (s *nativePluginSegment) releaseMergeDocuments() {
	s.materializeMu.Lock()
	s.mergeDocuments = nil
	s.mergePrepared = false
	s.materializeMu.Unlock()
}

func (s *nativePluginSegment) ensureStoredDocuments(closeCh chan struct{}) error {
	s.lazyMu.Lock()
	defer s.lazyMu.Unlock()
	if s.reader == nil {
		return nil
	}
	visibleCount, countErr := s.reader.VisibleDocCount()
	if countErr != nil {
		return countErr
	}
	if visibleCount == int64(len(s.documents)) {
		return nil
	}
	s.documents = make([]nativePluginDocument, 0, visibleCount)
	visitContext, stopContext := mergeContext(closeCh)
	defer stopContext()
	visitErr := s.reader.VisitLiveDocuments(visitContext, func(document nativeice.StoredDocument) error {
		if mergeCloseRequested(closeCh) {
			return errors.New("inverted: merge canceled")
		}
		decoded := nativePluginDocument{termFreqs: make(map[string]map[string]uint64)}
		if fieldsErr := document.VisitStoredFields(func(name string, value []byte) bool {
			decoded.fields = append(decoded.fields, nativeice.DecodedField{Name: name, Value: append([]byte(nil), value...)})
			return true
		}); fieldsErr != nil {
			return fieldsErr
		}
		s.documents = append(s.documents, decoded)
		return nil
	})
	if errors.Is(visitErr, context.Canceled) && mergeCloseRequested(closeCh) {
		return errors.New("inverted: merge canceled")
	}
	return visitErr
}

func mergeContext(closeCh chan struct{}) (context.Context, func()) {
	if closeCh == nil {
		return context.Background(), func() {}
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		select {
		case <-closeCh:
			cancel()
		case <-done:
		}
	}()
	return ctx, func() {
		close(done)
		cancel()
	}
}

func nativeSegmentPluginMerge(segments []segmentValue, drops []*roaringpkg.Bitmap, mergeBufferSize int) segmentMergerValue {
	return &nativeSegmentMerger{segments: segments, drops: drops, mergeBufferSize: mergeBufferSize}
}

func (s *nativePluginSegment) encodeMergeDocument(document nativeMergeDocument, emptyIndexedFields map[string]struct{}) nativeice.EncodeDocument {
	encoded := nativeice.EncodeDocument{}
	for fieldIndex := range document.fields {
		field := &document.fields[fieldIndex]
		if field.name == docIDField && len(field.values) > 0 {
			encoded.Identifier = append([]byte(nil), field.values[len(field.values)-1]...)
		}
	}
	for _, name := range s.fieldNames {
		if name == docIDField {
			continue
		}
		var field *nativeMergeField
		for fieldIndex := range document.fields {
			if document.fields[fieldIndex].name == name {
				field = &document.fields[fieldIndex]
				break
			}
		}
		mode := nativePluginMode{}
		var values, sortValues [][]byte
		var terms []nativeMergeTerm
		if field != nil {
			mode = field.mode
			values, sortValues, terms = field.values, field.sortValues, field.terms
		}
		_, emptyIndexed := emptyIndexedFields[name]
		if field == nil && !emptyIndexed {
			continue
		}
		encodedTerms := make([]nativeice.EncodeTerm, 0, len(terms))
		for _, term := range terms {
			encodedTerms = append(encodedTerms, nativeice.EncodeTerm{Value: term.value, Frequency: term.frequency})
		}
		separateSort := mode.sort && len(values) > 0 && len(sortValues) > 0 && !nativeMergeValuesEqual(values, sortValues)
		if len(values) == 0 {
			if len(sortValues) > 0 {
				values = sortValues
			} else {
				values = [][]byte{nil}
				if len(terms) > 0 {
					values[0] = terms[0].value
				}
			}
		}
		for valueIndex, value := range values {
			fieldTerms := encodedTerms
			if valueIndex > 0 {
				fieldTerms = nil
			}
			sortThisValue := mode.sort && !separateSort && valueIndex < len(sortValues)
			encoded.Fields = append(encoded.Fields, nativeice.EncodeField{
				Name: name, Value: value, Terms: fieldTerms, Index: (mode.index || emptyIndexed) && valueIndex == 0,
				Store: mode.store, Sort: sortThisValue,
			})
		}
		if separateSort || len(sortValues) > len(values) {
			start := 0
			if !separateSort {
				start = len(values)
			}
			for _, sortValue := range sortValues[start:] {
				encoded.Fields = append(encoded.Fields, nativeice.EncodeField{Name: name, Value: sortValue, Sort: true})
			}
		}
	}
	return encoded
}

type nativeSegmentMerger struct {
	segments           []segmentValue
	drops              []*roaringpkg.Bitmap
	newDocumentNumbers [][]uint64
	mergeBufferSize    int
}

//nolint:gocyclo // Merge must preserve all segment modalities while applying drops.
func (m *nativeSegmentMerger) WriteTo(writer io.Writer, closeCh chan struct{}) (int64, error) {
	if writer == nil {
		return 0, errors.New("inverted: nil merge writer")
	}
	if m.closeRequested(closeCh) {
		return 0, errors.New("inverted: merge canceled")
	}
	generation := nativeice.Generation{SegmentID: 1, SnapshotID: 1}
	frequencyWritten := make(map[string]bool)
	emptyIndexedFields := make(map[string]struct{})
	m.newDocumentNumbers = make([][]uint64, len(m.segments))
	preparedSegments := make([]*nativePluginSegment, 0, len(m.segments))
	defer func() {
		for _, segment := range preparedSegments {
			segment.releaseMergeDocuments()
		}
	}()
	for segmentIndex, value := range m.segments {
		current, ok := value.(*nativePluginSegment)
		if !ok {
			return 0, errors.New("inverted: unsupported segment implementation")
		}
		readerBacked := current.reader != nil
		if readerBacked {
			if prepareErr := current.prepareMergeDocuments(closeCh); prepareErr != nil {
				return 0, prepareErr
			}
			preparedSegments = append(preparedSegments, current)
		} else if materializeErr := current.materialize(closeCh); materializeErr != nil {
			return 0, materializeErr
		}
		for name := range current.emptyIndexedFields {
			emptyIndexedFields[name] = struct{}{}
		}
		documentCount := len(current.documents)
		if readerBacked {
			documentCount = len(current.mergeDocuments)
		}
		mapping := make([]uint64, documentCount)
		for idx := range mapping {
			mapping[idx] = math.MaxInt64
		}
		m.newDocumentNumbers[segmentIndex] = mapping
		drop := (*roaringpkg.Bitmap)(nil)
		if segmentIndex < len(m.drops) {
			drop = m.drops[segmentIndex]
		}
		for documentIndex := 0; documentIndex < documentCount; documentIndex++ {
			if drop != nil && drop.Contains(uint32(documentIndex)) {
				continue
			}
			if m.closeRequested(closeCh) {
				return 0, errors.New("inverted: merge canceled")
			}
			if readerBacked {
				encoded := current.encodeMergeDocument(current.mergeDocuments[documentIndex], emptyIndexedFields)
				generation.Documents = append(generation.Documents, encoded)
				// EncodeDocument now owns the byte slices through its field and
				// term views. Drop the compact slot headers as soon as this
				// document is transferred so the generation does not retain two
				// copies of the per-document slice metadata during encoding.
				current.mergeDocuments[documentIndex].fields = nil
				mapping[documentIndex] = uint64(len(generation.Documents) - 1)
				continue
			}
			document := current.documents[documentIndex]
			encoded := nativeice.EncodeDocument{}
			values := make(map[string][][]byte)
			for _, field := range document.fields {
				if field.Name == docIDField {
					encoded.Identifier = append([]byte(nil), field.Value...)
					continue
				}
				values[field.Name] = append(values[field.Name], append([]byte(nil), field.Value...))
			}
			names := make(map[string]struct{})
			for name := range values {
				names[name] = struct{}{}
			}
			for name := range document.terms {
				names[name] = struct{}{}
			}
			for name := range document.docValues {
				names[name] = struct{}{}
			}
			for name, mode := range document.modes {
				if mode.index || mode.store || mode.sort {
					names[name] = struct{}{}
				}
			}
			for name := range current.emptyIndexedFields {
				names[name] = struct{}{}
			}
			orderedNames := make([]string, 0, len(names))
			for name := range names {
				if name != docIDField {
					orderedNames = append(orderedNames, name)
				}
			}
			sort.Strings(orderedNames)
			for _, name := range orderedNames {
				mode := document.modes[name]
				_, emptyIndexed := emptyIndexedFields[name]
				terms := make([]nativeice.EncodeTerm, 0, len(document.terms[name]))
				for _, term := range document.terms[name] {
					frequency := uint64(1)
					if document.termFreqs[name] != nil {
						frequency = document.termFreqs[name][string(term)]
						if frequency == 0 {
							frequency = 1
						}
					} else if current.decoded && !frequencyWritten[name] {
						frequency = current.frequencies[name]
						if frequency == 0 {
							frequency = 1
						}
						frequencyWritten[name] = true
					}
					terms = append(terms, nativeice.EncodeTerm{Value: append([]byte(nil), term...), Frequency: frequency})
				}
				fieldValues := values[name]
				sortValues := document.docValues[name]
				separateSort := mode.sort && len(fieldValues) > 0 && len(sortValues) > 0 && !bytes.Equal(fieldValues[0], sortValues[0])
				if len(fieldValues) == 0 {
					if len(document.docValues[name]) > 0 {
						fieldValues = document.docValues[name]
					} else {
						fieldValues = [][]byte{nil}
						if len(document.terms[name]) > 0 {
							fieldValues[0] = append([]byte(nil), document.terms[name][0]...)
						}
					}
				}
				for valueIndex, value := range fieldValues {
					fieldTerms := terms
					if valueIndex > 0 {
						fieldTerms = nil
					}
					sortThisValue := mode.sort && !separateSort && valueIndex < len(sortValues)
					encoded.Fields = append(encoded.Fields, nativeice.EncodeField{
						Name: name, Value: value, Terms: fieldTerms, Index: (mode.index || emptyIndexed) && valueIndex == 0,
						Store: mode.store, Sort: sortThisValue,
					})
				}
				if separateSort || len(sortValues) > len(fieldValues) {
					start := 0
					if !separateSort {
						start = len(fieldValues)
					}
					for _, sortValue := range sortValues[start:] {
						encoded.Fields = append(encoded.Fields, nativeice.EncodeField{Name: name, Value: sortValue, Sort: true})
					}
				}
			}
			generation.Documents = append(generation.Documents, encoded)
			mapping[documentIndex] = uint64(len(generation.Documents) - 1)
		}
	}
	payload, encodeErr := nativeice.EncodeSegment(generation)
	if encodeErr != nil {
		return 0, encodeErr
	}
	return writeNativePayload(writer, payload, m.mergeBufferSize, closeCh)
}

func writeNativePayload(writer io.Writer, payload []byte, chunkSize int, closeCh chan struct{}) (int64, error) {
	if chunkSize <= 0 || chunkSize >= len(payload) {
		written, writeErr := writer.Write(payload)
		if writeErr != nil {
			return int64(written), writeErr
		}
		if written != len(payload) {
			return int64(written), io.ErrShortWrite
		}
		return int64(written), nil
	}
	var total int
	for offset := 0; offset < len(payload); offset += chunkSize {
		if closeCh != nil {
			select {
			case <-closeCh:
				return int64(total), errors.New("inverted: merge canceled")
			default:
			}
		}
		end := offset + chunkSize
		if end > len(payload) {
			end = len(payload)
		}
		written, writeErr := writer.Write(payload[offset:end])
		total += written
		if writeErr != nil {
			return int64(total), writeErr
		}
		if written != end-offset {
			return int64(total), io.ErrShortWrite
		}
	}
	return int64(total), nil
}

func (m *nativeSegmentMerger) closeRequested(closeCh chan struct{}) bool {
	return mergeCloseRequested(closeCh)
}

func mergeCloseRequested(closeCh chan struct{}) bool {
	if closeCh == nil {
		return false
	}
	select {
	case <-closeCh:
		return true
	default:
		return false
	}
}
func (m *nativeSegmentMerger) DocumentNumbers() [][]uint64 { return m.newDocumentNumbers }

func (s *nativePluginSegment) Dictionary(field string) (segmentDictionary, error) {
	if s.reader != nil {
		s.dictionaryMu.Lock()
		defer s.dictionaryMu.Unlock()
		if s.dictionaryWrappers == nil {
			s.dictionaryWrappers = make(map[string]segmentDictionary)
		}
		if wrapper := s.dictionaryWrappers[field]; wrapper != nil {
			return wrapper, nil
		}
		if dictionary := s.readerDictionaries[field]; dictionary != nil {
			wrapper := &nativePluginReaderDictionary{dictionary: dictionary}
			s.dictionaryWrappers[field] = wrapper
			return wrapper, nil
		}
		dictionaryValue, dictionaryErr := s.reader.DictionaryValue(field)
		if dictionaryErr != nil {
			return nil, dictionaryErr
		}
		dictionary := &dictionaryValue
		s.readerDictionaries[field] = dictionary
		wrapper := &nativePluginReaderDictionary{dictionary: dictionary}
		s.dictionaryWrappers[field] = wrapper
		return wrapper, nil
	}
	s.dictionaryMu.Lock()
	defer s.dictionaryMu.Unlock()
	if s.dictionaryWrappers == nil {
		s.dictionaryWrappers = make(map[string]segmentDictionary)
	}
	if wrapper := s.dictionaryWrappers[field]; wrapper != nil {
		return wrapper, nil
	}
	wrapper := &nativePluginDictionary{field: s.fields[field]}
	s.dictionaryWrappers[field] = wrapper
	return wrapper, nil
}

func (s *nativePluginSegment) VisitStoredFields(number uint64, visit segmentStoredVisitor) error {
	if s.reader != nil {
		return s.reader.VisitDocument(number, func(document nativeice.StoredDocument) error {
			return document.VisitStoredFields(func(name string, value []byte) bool { return visit(name, value) })
		})
	}
	if number >= uint64(len(s.documents)) {
		return fmt.Errorf("inverted: document %d out of range", number)
	}
	for _, field := range s.documents[number].fields {
		if !visit(field.Name, field.Value) {
			break
		}
	}
	return nil
}

func (s *nativePluginSegment) Count() uint64 {
	if s.reader != nil {
		visibleCount, countErr := s.reader.VisibleDocCount()
		if countErr != nil || visibleCount < 0 {
			return 0
		}
		return uint64(visibleCount)
	}
	return uint64(len(s.documents))
}

func (s *nativePluginSegment) DocsMatchingTerms(terms []segmentTerm) (*roaringpkg.Bitmap, error) {
	result := roaringpkg.New()
	if s.reader != nil {
		termsByField := make(map[string][][]byte)
		for _, term := range terms {
			termsByField[term.Field()] = append(termsByField[term.Field()], term.Term())
		}
		for field, fieldTerms := range termsByField {
			memberships, membershipErr := s.reader.TermDocumentsBatch(field, fieldTerms)
			if membershipErr != nil {
				return nil, membershipErr
			}
			for _, documents := range memberships {
				for _, number := range documents {
					result.Add(uint32(number))
				}
			}
		}
		return result, nil
	}
	for _, term := range terms {
		if field := s.fields[term.Field()]; field != nil {
			for _, number := range field.terms[string(term.Term())] {
				result.Add(uint32(number))
			}
		}
	}
	return result, nil
}

func (s *nativePluginSegment) Fields() []string {
	if s.reader != nil {
		return append([]string(nil), s.fieldNames...)
	}
	result := make([]string, 0, len(s.fields))
	for name := range s.fields {
		result = append(result, name)
	}
	sort.Strings(result)
	return result
}

func (s *nativePluginSegment) CollectionStats(field string) (segmentStats, error) {
	if s.reader != nil {
		documents, frequency, statsErr := s.reader.FieldStats(field)
		if statsErr != nil {
			return nil, statsErr
		}
		return &nativePluginStats{total: s.Count(), documents: documents, frequency: frequency}, nil
	}
	entry := s.fields[field]
	if entry == nil {
		entry = &nativePluginField{}
	}
	return &nativePluginStats{total: s.Count(), documents: entry.docCount, frequency: entry.frequency}, nil
}

func (s *nativePluginSegment) Size() int {
	if s.data != nil {
		return s.data.Len()
	}
	return len(s.payload)
}

func (s *nativePluginSegment) DocumentValueReader(fields []string) (segmentDocValues, error) {
	if s.reader != nil {
		return nativePluginDocValues{segment: s, fields: fields}, nil
	}
	return nativePluginDocValues{segment: s, fields: fields}, nil
}

func (s *nativePluginSegment) WriteTo(writer io.Writer, closeCh chan struct{}) (int64, error) {
	if writer == nil {
		return 0, errors.New("inverted: nil segment writer")
	}
	if closeCh != nil {
		select {
		case <-closeCh:
			return 0, errors.New("inverted: segment write canceled")
		default:
		}
	}
	if s.data != nil {
		return s.data.WriteTo(writer)
	}
	written, err := writer.Write(s.payload)
	if err != nil {
		return int64(written), err
	}
	if written != len(s.payload) {
		return int64(written), io.ErrShortWrite
	}
	return int64(written), nil
}
func (s *nativePluginSegment) Type() string              { return "ice" }
func (s *nativePluginSegment) Version() uint32           { return 3 }
func (s *nativePluginSegment) Timestamp() (int64, int64) { return s.timeMin, s.timeMax }

type nativePluginStats struct{ total, documents, frequency uint64 }

func (s *nativePluginStats) TotalDocumentCount() uint64    { return s.total }
func (s *nativePluginStats) DocumentCount() uint64         { return s.documents }
func (s *nativePluginStats) SumTotalTermFrequency() uint64 { return s.frequency }
func (s *nativePluginStats) Merge(other segmentStats) {
	s.total += other.TotalDocumentCount()
	s.documents += other.DocumentCount()
	s.frequency += other.SumTotalTermFrequency()
}

type nativePluginDictionary struct {
	field *nativePluginField
}

type nativePluginReaderDictionary struct {
	dictionary *nativeice.Dictionary
}

func nativePluginContains(dictionary *nativeice.Dictionary, field *nativePluginField, term []byte) (bool, error) {
	if dictionary != nil {
		return dictionary.TermExists(term)
	}
	if field == nil {
		return false, nil
	}
	_, ok := field.terms[string(term)]
	return ok, nil
}

func nativePluginContainsClose(dictionary *nativeice.Dictionary) error {
	if dictionary != nil {
		return dictionary.Close()
	}
	return nil
}

func (d *nativePluginDictionary) Contains(term []byte) (bool, error) {
	return nativePluginContains(nil, d.field, term)
}
func (d *nativePluginDictionary) Close() error { return nativePluginContainsClose(nil) }

func (d *nativePluginReaderDictionary) Contains(term []byte) (bool, error) {
	return nativePluginContains(d.dictionary, nil, term)
}

func (d *nativePluginReaderDictionary) Close() error {
	return nativePluginContainsClose(d.dictionary)
}

func nativePluginPostingsList(
	dictionary *nativeice.Dictionary,
	field *nativePluginField,
	term []byte,
	except *roaringpkg.Bitmap,
	prealloc segmentPostingsList,
) (segmentPostingsList, error) {
	if dictionary != nil {
		posting, found, postingsErr := dictionary.TermPosting(term)
		if postingsErr != nil {
			return nil, postingsErr
		}
		if !found {
			return emptyNativePluginPostings, nil
		}
		if posting.OneHit {
			if except != nil && except.Contains(uint32(posting.DocumentNumber)) {
				return emptyNativePluginPostings, nil
			}
			postings := nativePluginPostingsFromPrealloc(prealloc)
			postings.oneHit, postings.oneHitNumber = true, posting.DocumentNumber
			return postings, nil
		}
		values := posting.Documents
		actualBitmap := posting.Bitmap
		frequencies := make([]int, len(posting.Frequencies))
		for frequencyIndex, frequency := range posting.Frequencies {
			if frequencyIndex >= len(frequencies) {
				break
			}
			frequencies[frequencyIndex] = int(frequency.Frequency)
		}
		if except != nil && len(values) > 0 {
			if actualBitmap != nil {
				actualBitmap.AndNot(except)
			}
			filteredValues := make([]uint64, 0, len(values))
			filteredFrequencies := make([]int, 0, len(values))
			for valueIndex, value := range values {
				if !except.Contains(uint32(value)) {
					filteredValues = append(filteredValues, value)
					frequency := 0
					if valueIndex < len(frequencies) {
						frequency = frequencies[valueIndex]
					}
					filteredFrequencies = append(filteredFrequencies, frequency)
				}
			}
			values, frequencies = filteredValues, filteredFrequencies
		}
		if len(values) == 0 {
			return emptyNativePluginPostings, nil
		}
		postings := nativePluginPostingsFromPrealloc(prealloc)
		postings.values, postings.frequencies, postings.actualBitmap = values, frequencies, actualBitmap
		return postings, nil
	}
	var values []uint64
	var frequencyMap map[uint64]uint64
	var actualBitmap *roaringpkg.Bitmap
	if field != nil {
		values = field.terms[string(term)]
		frequencyMap = field.termFreqs[string(term)]
	}
	if except != nil && len(values) > 0 {
		filtered := make([]uint64, 0, len(values))
		for _, value := range values {
			if !except.Contains(uint32(value)) {
				filtered = append(filtered, value)
			}
		}
		values = filtered
	}
	if len(values) == 0 {
		return emptyNativePluginPostings, nil
	}
	postings := nativePluginPostingsFromPrealloc(prealloc)
	if len(values) == 1 && (frequencyMap == nil || frequencyMap[values[0]] <= 1) {
		postings.oneHit, postings.oneHitNumber = true, values[0]
		return postings, nil
	}
	if len(values) > 0 {
		actualBitmap = roaringpkg.New()
		for _, value := range values {
			actualBitmap.Add(uint32(value))
		}
	}
	postings.values, postings.frequencyMap, postings.actualBitmap = values, frequencyMap, actualBitmap
	return postings, nil
}

func (d *nativePluginDictionary) PostingsList(term []byte, except *roaringpkg.Bitmap, prealloc segmentPostingsList) (segmentPostingsList, error) {
	return nativePluginPostingsList(nil, d.field, term, except, prealloc)
}

func (d *nativePluginReaderDictionary) PostingsList(term []byte, except *roaringpkg.Bitmap, prealloc segmentPostingsList) (segmentPostingsList, error) {
	return nativePluginPostingsList(d.dictionary, nil, term, except, prealloc)
}

func nativePluginPostingsFromPrealloc(prealloc segmentPostingsList) *nativePluginPostings {
	postings, reusable := prealloc.(*nativePluginPostings)
	if !reusable || postings == nil || postings == emptyNativePluginPostings {
		return &nativePluginPostings{}
	}
	*postings = nativePluginPostings{}
	return postings
}

func nativePluginDictionaryIteratorFor(
	dictionary *nativeice.Dictionary,
	field *nativePluginField,
	automaton segmentAutomaton,
	start, end []byte,
) segmentDictionaryIterator {
	terms := []string{}
	if dictionary != nil && dictionary.Bound() {
		iterator, iteratorErr := dictionary.NewDictionaryTermIterator(automaton, start, end)
		return &nativePluginDictionaryIterator{nativeIterator: iterator, termsErr: iteratorErr, index: -1}
	}
	if field != nil {
		for term := range field.terms {
			if (len(start) == 0 || term >= string(start)) && (len(end) == 0 || term < string(end)) {
				if automaton != nil {
					state := automaton.Start()
					for _, character := range []byte(term) {
						state = automaton.Accept(state, character)
					}
					if !automaton.IsMatch(state) {
						continue
					}
				}
				terms = append(terms, term)
			}
		}
	}
	sort.Strings(terms)
	return &nativePluginDictionaryIterator{terms: terms, field: field, index: -1}
}

func (d *nativePluginDictionary) Iterator(automaton segmentAutomaton, start, end []byte) segmentDictionaryIterator {
	return nativePluginDictionaryIteratorFor(nil, d.field, automaton, start, end)
}

func (d *nativePluginReaderDictionary) Iterator(automaton segmentAutomaton, start, end []byte) segmentDictionaryIterator {
	return nativePluginDictionaryIteratorFor(d.dictionary, nil, automaton, start, end)
}

//nolint:govet // direct document and frequency slices avoid per-lookup bitmap copies.
type nativePluginPostings struct {
	values       []uint64
	frequencies  []int
	frequencyMap map[uint64]uint64
	actualBitmap *roaringpkg.Bitmap
	oneHit       bool
	oneHitNumber uint64
}

var emptyNativePluginPostings = &nativePluginPostings{}

func (p *nativePluginPostings) Iterator(_, _, _ bool, prealloc segmentPostingsIter) (segmentPostingsIter, error) {
	iterator, ok := prealloc.(*nativePluginPostingsIterator)
	if !ok || iterator == nil {
		iterator = &nativePluginPostingsIterator{}
	}
	iterator.values = p.values
	iterator.frequencies = p.frequencies
	iterator.frequencyMap = p.frequencyMap
	iterator.actualBitmap = p.actualBitmap
	iterator.bitmapMode = false
	if iterator.actualBitmap != nil {
		iterator.actual = iterator.actualBitmap.Iterator()
	} else {
		iterator.actual = nil
	}
	iterator.oneHit = p.oneHit
	iterator.oneHitNumber = p.oneHitNumber
	iterator.index = -1
	return iterator, nil
}

func (p *nativePluginPostings) Size() int {
	if p.oneHit {
		return 8
	}
	return len(p.values) * 8
}

func (p *nativePluginPostings) Count() uint64 {
	if p.oneHit {
		return 1
	}
	return uint64(len(p.values))
}

type nativePluginPostingsIterator struct {
	actual       roaringpkg.IntPeekable
	frequencyMap map[uint64]uint64
	actualBitmap *roaringpkg.Bitmap
	frequencies  []int
	values       []uint64
	posting      nativePluginPosting
	oneHitNumber uint64
	index        int
	bitmapMode   bool
	oneHit       bool
}

func (p *nativePluginPostingsIterator) Next() (segmentPosting, error) {
	if p.oneHit {
		if p.index >= 0 {
			return nil, nil
		}
		p.index = 0
		p.posting.number = p.oneHitNumber
		p.posting.frequency = 1
		return &p.posting, nil
	}
	if p.bitmapMode {
		if p.actual == nil {
			return nil, nil
		}
		if !p.actual.HasNext() {
			return nil, nil
		}
		p.posting.number = uint64(p.actual.Next())
		p.posting.frequency = p.frequencyFor(p.posting.number)
		return &p.posting, nil
	}
	p.index++
	if p.index >= len(p.values) {
		return nil, nil
	}
	number := p.values[p.index]
	frequency := 0
	if p.index < len(p.frequencies) {
		frequency = p.frequencies[p.index]
	} else {
		frequency = int(p.frequencyMap[number])
	}
	if frequency == 0 {
		frequency = 1
	}
	p.posting.number = number
	p.posting.frequency = frequency
	return &p.posting, nil
}

func (p *nativePluginPostingsIterator) Advance(number uint64) (segmentPosting, error) {
	if p.oneHit {
		if p.index >= 0 || p.oneHitNumber < number {
			p.index = 0
			return nil, nil
		}
		return p.Next()
	}
	if p.bitmapMode {
		if p.actual == nil {
			return nil, nil
		}
		p.actual.AdvanceIfNeeded(uint32(number))
		return p.Next()
	}
	for p.index+1 < len(p.values) && p.values[p.index+1] < number {
		p.index++
	}
	return p.Next()
}

func (p *nativePluginPostingsIterator) Size() int {
	if p.oneHit {
		return 1
	}
	if p.bitmapMode {
		if p.actualBitmap == nil {
			return 0
		}
		return int(p.actualBitmap.GetCardinality())
	}
	return len(p.values)
}

func (p *nativePluginPostingsIterator) Empty() bool {
	if p.bitmapMode {
		return !p.oneHit && (p.actualBitmap == nil || p.actualBitmap.IsEmpty())
	}
	return !p.oneHit && len(p.values) == 0
}

func (p *nativePluginPostingsIterator) Count() uint64 {
	if p.oneHit {
		return 1
	}
	if p.bitmapMode {
		if p.actualBitmap == nil {
			return 0
		}
		return p.actualBitmap.GetCardinality()
	}
	return uint64(len(p.values))
}
func (p *nativePluginPostingsIterator) Close() error { return nil }

func (p *nativePluginPostingsIterator) ActualBitmap() *roaringpkg.Bitmap {
	return p.actualBitmap
}

func (p *nativePluginPostingsIterator) DocNum1Hit() (uint64, bool) {
	return p.oneHitNumber, p.oneHit
}

func (p *nativePluginPostingsIterator) ReplaceActual(actual *roaringpkg.Bitmap) {
	p.index = -1
	p.bitmapMode = true
	p.oneHit = false
	p.oneHitNumber = 0
	p.actualBitmap = actual
	if actual == nil {
		p.actual = nil
		return
	}
	p.actual = actual.Iterator()
}

func (p *nativePluginPostingsIterator) frequencyFor(documentNumber uint64) int {
	if p.frequencyMap != nil {
		frequency := int(p.frequencyMap[documentNumber])
		if frequency > 0 {
			return frequency
		}
		return 1
	}
	index := sort.Search(len(p.values), func(index int) bool { return p.values[index] >= documentNumber })
	if index < len(p.values) && p.values[index] == documentNumber && index < len(p.frequencies) && p.frequencies[index] > 0 {
		return p.frequencies[index]
	}
	return 1
}

type nativePluginPosting struct {
	number    uint64
	frequency int
}

func (p *nativePluginPosting) Number() uint64               { return p.number }
func (p *nativePluginPosting) SetNumber(n uint64)           { p.number = n }
func (p *nativePluginPosting) Frequency() int               { return p.frequency }
func (p *nativePluginPosting) Norm() float64                { return 1 }
func (p *nativePluginPosting) Locations() []segmentLocation { return nil }
func (p *nativePluginPosting) Size() int                    { return 8 }

type nativePluginDocValues struct {
	segment *nativePluginSegment
	fields  []string
}

func (d nativePluginDocValues) VisitDocumentValues(number uint64, visit segmentDocumentValueVisitor) error {
	if d.segment.reader != nil {
		for _, wanted := range d.fields {
			values, valuesErr := d.segment.reader.DocumentValues(wanted, number)
			if valuesErr != nil {
				return valuesErr
			}
			for _, value := range values {
				visit(wanted, value)
			}
		}
		return nil
	}
	if number >= uint64(len(d.segment.documents)) {
		return fmt.Errorf("inverted: document %d out of range", number)
	}
	document := d.segment.documents[number]
	for _, wanted := range d.fields {
		for _, value := range document.docValues[wanted] {
			visit(wanted, value)
		}
	}
	return nil
}

//nolint:govet // iterator state is grouped by its two segment representations.
type nativePluginDictionaryIterator struct {
	field          *nativePluginField
	nativeIterator *nativeice.DictionaryTermIterator
	terms          []string
	index          int
	termsErr       error
	entry          nativePluginDictionaryEntry
}

type nativePluginDictionaryEntry struct {
	term  string
	count uint64
}

func (e nativePluginDictionaryEntry) Term() string  { return e.term }
func (e nativePluginDictionaryEntry) Count() uint64 { return e.count }
func (i *nativePluginDictionaryIterator) Next() (segmentDictionaryEntry, error) {
	if i.termsErr != nil {
		return nil, i.termsErr
	}
	if i.nativeIterator != nil {
		term, count, iteratorErr := i.nativeIterator.NextString()
		if iteratorErr != nil || (term == "" && count == 0) {
			return nil, iteratorErr
		}
		i.entry.term, i.entry.count = term, count
		return &i.entry, nil
	}
	i.index++
	if i.index >= len(i.terms) {
		return nil, nil
	}
	term := i.terms[i.index]
	i.entry.term, i.entry.count = term, uint64(len(i.field.terms[term]))
	return &i.entry, nil
}

func (i *nativePluginDictionaryIterator) Close() error {
	if i.nativeIterator != nil {
		return i.nativeIterator.Close()
	}
	return nil
}

var (
	_ segmentValue       = (*nativePluginSegment)(nil)
	_ segmentMergerValue = (*nativeSegmentMerger)(nil)
	_                    = bytes.Compare
)
