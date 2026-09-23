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
	"errors"
	"fmt"
	"io"
	"math"
	"sort"

	roaringpkg "github.com/RoaringBitmap/roaring"

	"github.com/apache/skywalking-banyandb/pkg/index/inverted/internal/nativeice"
)

type nativePluginField struct {
	name      string
	stored    [][]byte
	terms     map[string][]uint64
	termFreqs map[string]map[uint64]uint64
	docValues [][]byte
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

type nativePluginSegment struct {
	fields      map[string]*nativePluginField
	frequencies map[string]uint64
	documents   []nativePluginDocument
	payload     []byte
	timeMin     int64
	timeMax     int64
	decoded     bool
}

func nativeSegmentPluginNew(results []segmentDocument, normCalc func(string, int) float32) (segmentValue, uint64, error) {
	_ = normCalc
	segment := &nativePluginSegment{fields: make(map[string]*nativePluginField), frequencies: make(map[string]uint64)}
	generation := nativeice.Generation{SegmentID: 1, SnapshotID: 1}
	for _, result := range results {
		if result == nil {
			return nil, 0, errors.New("inverted: nil segment document")
		}
		document := nativePluginDocument{
			terms: make(map[string][][]byte), termFreqs: make(map[string]map[string]uint64),
			docValues: make(map[string][][]byte), modes: make(map[string]nativePluginMode),
		}
		encoded := nativeice.EncodeDocument{}
		result.EachField(func(field segmentField) {
			name := field.Name()
			mode := document.modes[name]
			mode.index = mode.index || field.Index()
			mode.store = mode.store || field.Store()
			mode.sort = mode.sort || field.IndexDocValues()
			document.modes[name] = mode
			value := append([]byte(nil), field.Value()...)
			if field.Store() {
				document.fields = append(document.fields, nativeice.DecodedField{Name: name, Value: value})
			}
			if name == docIDField && field.Store() {
				encoded.Identifier = append([]byte(nil), value...)
			}
			var analyzedTerms []nativeice.EncodeTerm
			if field.Index() {
				analyzedTerms = make([]nativeice.EncodeTerm, 0)
				field.EachTerm(func(term segmentFieldTerm) {
					termValue := append([]byte(nil), term.Term()...)
					document.terms[name] = append(document.terms[name], termValue)
					if document.termFreqs[name] == nil {
						document.termFreqs[name] = make(map[string]uint64)
					}
					document.termFreqs[name][string(termValue)] = uint64(term.Frequency())
					analyzedTerms = append(analyzedTerms, nativeice.EncodeTerm{Value: termValue, Frequency: uint64(term.Frequency())})
				})
			}
			if field.IndexDocValues() {
				document.docValues[name] = append(document.docValues[name], append([]byte(nil), value...))
			}
			if name != docIDField {
				encoded.Fields = append(encoded.Fields, nativeice.EncodeField{
					Name: name, Value: value, Terms: analyzedTerms, Index: field.Index(), Store: field.Store(), Sort: field.IndexDocValues(),
				})
			}
		})
		sort.SliceStable(document.fields, func(leftIndex, rightIndex int) bool {
			leftName, rightName := document.fields[leftIndex].Name, document.fields[rightIndex].Name
			if leftName == docIDField {
				return true
			}
			if rightName == docIDField {
				return false
			}
			return leftName < rightName
		})
		if len(encoded.Identifier) == 0 {
			return nil, 0, errors.New("inverted: every segment document needs a stored _id")
		}
		document.terms[docIDField] = [][]byte{append([]byte(nil), encoded.Identifier...)}
		generation.Documents = append(generation.Documents, encoded)
		segment.documents = append(segment.documents, document)
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
	segment.payload = payload
	segment.rebuild()
	for _, document := range segment.documents {
		for name, mode := range document.modes {
			if mode.index && segment.fields[name] == nil {
				segment.fields[name] = &nativePluginField{name: name, terms: make(map[string][]uint64), termFreqs: make(map[string]map[uint64]uint64)}
			}
		}
	}
	for name, field := range segment.fields {
		segment.frequencies[name] = field.frequency
	}
	return segment, uint64(len(segment.documents)), nil
}

func (s *nativePluginSegment) rebuild() {
	s.fields = make(map[string]*nativePluginField)
	for documentNumber, document := range s.documents {
		seen := make(map[string]bool)
		for _, field := range document.fields {
			entry := s.fields[field.Name]
			if entry == nil {
				entry = &nativePluginField{name: field.Name, terms: make(map[string][]uint64), termFreqs: make(map[string]map[uint64]uint64)}
				s.fields[field.Name] = entry
			}
			entry.stored = append(entry.stored, append([]byte(nil), field.Value...))
			if document.terms == nil {
				if !seen[field.Name] {
					entry.docCount++
					seen[field.Name] = true
				}
				entry.terms[string(field.Value)] = append(entry.terms[string(field.Value)], uint64(documentNumber))
				entry.frequency++
			}
		}
		for name, terms := range document.terms {
			entry := s.fields[name]
			if entry == nil {
				entry = &nativePluginField{name: name, terms: make(map[string][]uint64), termFreqs: make(map[string]map[uint64]uint64)}
				s.fields[name] = entry
			}
			if len(terms) > 0 && !seen[name] {
				entry.docCount++
				seen[name] = true
			}
			for _, term := range terms {
				entry.terms[string(term)] = append(entry.terms[string(term)], uint64(documentNumber))
				frequency := document.termFreqs[name][string(term)]
				if frequency == 0 {
					frequency = 1
				}
				if entry.termFreqs[string(term)] == nil {
					entry.termFreqs[string(term)] = make(map[uint64]uint64)
				}
				entry.termFreqs[string(term)][uint64(documentNumber)] = frequency
				entry.frequency += frequency
			}
		}
		for name, mode := range document.modes {
			if !mode.index {
				continue
			}
			entry := s.fields[name]
			if entry == nil {
				entry = &nativePluginField{name: name, terms: make(map[string][]uint64), termFreqs: make(map[string]map[uint64]uint64)}
				s.fields[name] = entry
			}
			if !seen[name] {
				entry.docCount++
				seen[name] = true
			}
		}
		for name, values := range document.docValues {
			entry := s.fields[name]
			if entry == nil {
				entry = &nativePluginField{name: name, terms: make(map[string][]uint64), termFreqs: make(map[string]map[uint64]uint64)}
				s.fields[name] = entry
			}
			entry.docValues = append(entry.docValues, values...)
		}
	}
}

//nolint:gocyclo // Loading reconstructs all native segment modalities in one pass.
func nativeSegmentPluginLoad(data *segmentBytes) (segmentValue, error) {
	if data == nil || data.Len() == 0 {
		return nil, errors.New("inverted: empty segment")
	}
	payload, readErr := data.Read(0, data.Len())
	if readErr != nil {
		return nil, readErr
	}
	documents, decodeErr := nativeice.DecodeStoredSegment(payload)
	if decodeErr != nil {
		return nil, decodeErr
	}
	segment := &nativePluginSegment{
		payload: append([]byte(nil), payload...), fields: make(map[string]*nativePluginField),
		frequencies: make(map[string]uint64), decoded: true,
	}
	for _, document := range documents {
		segment.documents = append(segment.documents, nativePluginDocument{fields: document.Fields, termFreqs: make(map[string]map[string]uint64)})
	}
	reader, openErr := nativeice.OpenSegment(payload)
	if openErr != nil {
		return nil, openErr
	}
	segment.timeMin, segment.timeMax = reader.TimeBounds()
	fieldNames, fieldsErr := reader.Fields()
	if fieldsErr != nil {
		return nil, fieldsErr
	}
	for _, fieldName := range fieldNames {
		terms, termsErr := reader.Terms(fieldName)
		if termsErr != nil {
			return nil, termsErr
		}
		for _, term := range terms {
			memberships, membershipErr := reader.TermDocuments(fieldName, term)
			if membershipErr != nil {
				return nil, membershipErr
			}
			for _, membership := range memberships {
				for _, number := range membership.DocumentNumber {
					if int(number) < len(segment.documents) {
						if segment.documents[number].terms == nil {
							segment.documents[number].terms = make(map[string][][]byte)
						}
						segment.documents[number].terms[fieldName] = append(segment.documents[number].terms[fieldName], append([]byte(nil), term...))
						if segment.documents[number].termFreqs == nil {
							segment.documents[number].termFreqs = make(map[string]map[string]uint64)
						}
					}
				}
			}
			frequencies, frequencyErr := reader.TermFrequencies(fieldName, term)
			if frequencyErr != nil {
				return nil, frequencyErr
			}
			for _, frequencySet := range frequencies {
				for _, frequency := range frequencySet.Values {
					if frequency.DocumentNumber < uint64(len(segment.documents)) {
						if segment.documents[frequency.DocumentNumber].termFreqs[fieldName] == nil {
							segment.documents[frequency.DocumentNumber].termFreqs[fieldName] = make(map[string]uint64)
						}
						segment.documents[frequency.DocumentNumber].termFreqs[fieldName][string(term)] = frequency.Frequency
					}
				}
			}
		}
		docValues, valuesErr := reader.DocValues(fieldName)
		if valuesErr != nil {
			return nil, valuesErr
		}
		for _, values := range docValues {
			for documentNumber, documentValues := range values.Values {
				if documentNumber >= len(segment.documents) {
					continue
				}
				if segment.documents[documentNumber].docValues == nil {
					segment.documents[documentNumber].docValues = make(map[string][][]byte)
				}
				for _, value := range documentValues {
					segment.documents[documentNumber].docValues[fieldName] = append(segment.documents[documentNumber].docValues[fieldName], append([]byte(nil), value...))
				}
			}
		}
	}
	for documentIndex := range segment.documents {
		segment.documents[documentIndex].modes = make(map[string]nativePluginMode)
		for name := range segment.documents[documentIndex].terms {
			segment.documents[documentIndex].modes[name] = nativePluginMode{index: true}
		}
		for name := range segment.documents[documentIndex].docValues {
			mode := segment.documents[documentIndex].modes[name]
			mode.sort = true
			segment.documents[documentIndex].modes[name] = mode
		}
		for _, field := range segment.documents[documentIndex].fields {
			mode := segment.documents[documentIndex].modes[field.Name]
			mode.store = true
			if field.Name == docIDField {
				mode.index = true
			}
			segment.documents[documentIndex].modes[field.Name] = mode
		}
	}
	segment.rebuild()
	for _, fieldName := range fieldNames {
		if segment.fields[fieldName] == nil {
			segment.fields[fieldName] = &nativePluginField{name: fieldName, terms: make(map[string][]uint64), termFreqs: make(map[string]map[uint64]uint64)}
		}
		documents, frequency, statsErr := reader.FieldStats(fieldName)
		if statsErr != nil {
			return nil, statsErr
		}
		if field := segment.fields[fieldName]; field != nil {
			field.docCount = documents
			field.frequency = frequency
			segment.frequencies[fieldName] = frequency
		}
	}
	return segment, nil
}

func nativeSegmentPluginMerge(segments []segmentValue, drops []*roaringpkg.Bitmap, mergeBufferSize int) segmentMergerValue {
	return &nativeSegmentMerger{segments: segments, drops: drops, mergeBufferSize: mergeBufferSize}
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
	m.newDocumentNumbers = make([][]uint64, len(m.segments))
	for segmentIndex, value := range m.segments {
		current, ok := value.(*nativePluginSegment)
		if !ok {
			return 0, errors.New("inverted: unsupported segment implementation")
		}
		mapping := make([]uint64, len(current.documents))
		for idx := range mapping {
			mapping[idx] = math.MaxInt64
		}
		m.newDocumentNumbers[segmentIndex] = mapping
		drop := (*roaringpkg.Bitmap)(nil)
		if segmentIndex < len(m.drops) {
			drop = m.drops[segmentIndex]
		}
		for documentIndex, document := range current.documents {
			if drop != nil && drop.Contains(uint32(documentIndex)) {
				continue
			}
			if m.closeRequested(closeCh) {
				return 0, errors.New("inverted: merge canceled")
			}
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
			orderedNames := make([]string, 0, len(names))
			for name := range names {
				if name != docIDField {
					orderedNames = append(orderedNames, name)
				}
			}
			sort.Strings(orderedNames)
			for _, name := range orderedNames {
				mode := document.modes[name]
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
					fieldValues = document.docValues[name]
					if len(fieldValues) == 0 {
						fieldValues = [][]byte{nil}
					}
					if len(fieldValues) == 1 && fieldValues[0] == nil && len(document.terms[name]) > 0 {
						fieldValues[0] = append([]byte(nil), document.terms[name][0]...)
					}
				}
				for valueIndex, value := range fieldValues {
					fieldTerms := terms
					if valueIndex > 0 {
						fieldTerms = nil
					}
					sortThisValue := mode.sort && !separateSort && valueIndex < len(sortValues)
					encoded.Fields = append(encoded.Fields, nativeice.EncodeField{
						Name: name, Value: value, Terms: fieldTerms, Index: mode.index && valueIndex == 0,
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
	return &nativePluginDictionary{field: s.fields[field]}, nil
}

func (s *nativePluginSegment) VisitStoredFields(number uint64, visit segmentStoredVisitor) error {
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
func (s *nativePluginSegment) Count() uint64 { return uint64(len(s.documents)) }
func (s *nativePluginSegment) DocsMatchingTerms(terms []segmentTerm) (*roaringpkg.Bitmap, error) {
	result := roaringpkg.New()
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
	result := make([]string, 0, len(s.fields))
	for name := range s.fields {
		result = append(result, name)
	}
	sort.Strings(result)
	return result
}

func (s *nativePluginSegment) CollectionStats(field string) (segmentStats, error) {
	entry := s.fields[field]
	if entry == nil {
		entry = &nativePluginField{}
	}
	return &nativePluginStats{total: s.Count(), documents: entry.docCount, frequency: entry.frequency}, nil
}
func (s *nativePluginSegment) Size() int { return len(s.payload) }
func (s *nativePluginSegment) DocumentValueReader(fields []string) (segmentDocValues, error) {
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

type nativePluginDictionary struct{ field *nativePluginField }

func (d *nativePluginDictionary) Contains(term []byte) (bool, error) {
	if d.field == nil {
		return false, nil
	}
	_, ok := d.field.terms[string(term)]
	return ok, nil
}
func (d *nativePluginDictionary) Close() error { return nil }
func (d *nativePluginDictionary) PostingsList(term []byte, except *roaringpkg.Bitmap, _ segmentPostingsList) (segmentPostingsList, error) {
	result := roaringpkg.New()
	if d.field != nil {
		for _, n := range d.field.terms[string(term)] {
			result.Add(uint32(n))
		}
	}
	if except != nil {
		result.AndNot(except)
	}
	frequencies := make(map[uint64]int)
	if d.field != nil {
		for number, frequency := range d.field.termFreqs[string(term)] {
			frequencies[number] = int(frequency)
		}
	}
	return &nativePluginPostings{bitmap: result, frequencies: frequencies}, nil
}

func (d *nativePluginDictionary) Iterator(automaton segmentAutomaton, start, end []byte) segmentDictionaryIterator {
	terms := []string{}
	if d.field != nil {
		for term := range d.field.terms {
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
	return &nativePluginDictionaryIterator{terms: terms, field: d.field, index: -1}
}

type nativePluginPostings struct {
	bitmap      *roaringpkg.Bitmap
	frequencies map[uint64]int
}

func (p *nativePluginPostings) Iterator(_, _, _ bool, _ segmentPostingsIter) (segmentPostingsIter, error) {
	return &nativePluginPostingsIterator{values: p.bitmap.ToArray(), frequencies: p.frequencies, index: -1}, nil
}
func (p *nativePluginPostings) Size() int     { return int(p.bitmap.GetSizeInBytes()) }
func (p *nativePluginPostings) Count() uint64 { return p.bitmap.GetCardinality() }

type nativePluginPostingsIterator struct {
	frequencies map[uint64]int
	values      []uint32
	index       int
}

func (p *nativePluginPostingsIterator) Next() (segmentPosting, error) {
	p.index++
	if p.index >= len(p.values) {
		return nil, nil
	}
	number := uint64(p.values[p.index])
	frequency := p.frequencies[number]
	if frequency == 0 {
		frequency = 1
	}
	return &nativePluginPosting{number: number, frequency: frequency}, nil
}

func (p *nativePluginPostingsIterator) Advance(number uint64) (segmentPosting, error) {
	for p.index+1 < len(p.values) && uint64(p.values[p.index+1]) < number {
		p.index++
	}
	return p.Next()
}
func (p *nativePluginPostingsIterator) Size() int     { return len(p.values) }
func (p *nativePluginPostingsIterator) Empty() bool   { return len(p.values) == 0 }
func (p *nativePluginPostingsIterator) Count() uint64 { return uint64(len(p.values)) }
func (p *nativePluginPostingsIterator) Close() error  { return nil }

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

type nativePluginDictionaryIterator struct {
	field *nativePluginField
	terms []string
	index int
}

type nativePluginDictionaryEntry struct {
	term  string
	count uint64
}

func (e nativePluginDictionaryEntry) Term() string  { return e.term }
func (e nativePluginDictionaryEntry) Count() uint64 { return e.count }
func (i *nativePluginDictionaryIterator) Next() (segmentDictionaryEntry, error) {
	i.index++
	if i.index >= len(i.terms) {
		return nil, nil
	}
	term := i.terms[i.index]
	return nativePluginDictionaryEntry{term: term, count: uint64(len(i.field.terms[term]))}, nil
}
func (*nativePluginDictionaryIterator) Close() error { return nil }

var (
	_ segmentValue       = (*nativePluginSegment)(nil)
	_ segmentMergerValue = (*nativeSegmentMerger)(nil)
	_                    = bytes.Compare
)
