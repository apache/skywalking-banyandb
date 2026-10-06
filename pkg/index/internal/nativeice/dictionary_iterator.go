// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package nativeice

import (
	"context"
	"errors"

	"github.com/blevesearch/vellum"
)

// DictionaryAutomaton is the byte automaton used to prune dictionary walks.
// Implementations are evaluated by the cached FST before a posting is read.
type DictionaryAutomaton interface {
	Start() int
	IsMatch(int) bool
	CanMatch(int) bool
	WillAlwaysMatch(int) bool
	Accept(int, byte) int
}

// VisitTerms walks one field's exact dictionary terms in each pinned segment.
// Terms are copied before the callback and are not globally ordered across
// segments. No posting bitmap or frequency stream is decoded. Returning false
// stops the walk without error.
func (r *Reader) VisitTerms(ctx context.Context, field string, visit func([]byte) bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	for segmentIndex := range r.segments {
		storedReader, readerErr := r.storedReader(segmentIndex)
		if readerErr != nil {
			return readerErr
		}
		fst, dictionaryErr := storedReader.dictionary(field)
		if dictionaryErr != nil {
			return dictionaryErr
		}
		if fst == nil {
			continue
		}
		iterator, iteratorErr := fst.Search(nil, nil, nil)
		if iteratorErr != nil {
			if errors.Is(iteratorErr, vellum.ErrIteratorDone) {
				continue
			}
			return corruptError("search term dictionary", iteratorErr)
		}
		for {
			if err := ctx.Err(); err != nil {
				_ = iterator.Close()
				return err
			}
			term, postingOffset := iterator.Current()
			// Vellum represents a valid empty key as a nil byte slice. The
			// non-zero posting value distinguishes it from iterator exhaustion.
			if term == nil && postingOffset == 0 {
				break
			}
			if !visit(append([]byte(nil), term...)) {
				_ = iterator.Close()
				return nil
			}
			if nextErr := iterator.Next(); nextErr != nil {
				if errors.Is(nextErr, vellum.ErrIteratorDone) {
					break
				}
				_ = iterator.Close()
				return corruptError("advance term dictionary", nextErr)
			}
		}
		if closeErr := iterator.Close(); closeErr != nil {
			return closeErr
		}
	}
	return nil
}

type vellumDictionaryAutomaton struct{ automaton DictionaryAutomaton }

func (a vellumDictionaryAutomaton) Start() int              { return a.automaton.Start() }
func (a vellumDictionaryAutomaton) IsMatch(state int) bool  { return a.automaton.IsMatch(state) }
func (a vellumDictionaryAutomaton) CanMatch(state int) bool { return a.automaton.CanMatch(state) }
func (a vellumDictionaryAutomaton) WillAlwaysMatch(state int) bool {
	return a.automaton.WillAlwaysMatch(state)
}

func (a vellumDictionaryAutomaton) Accept(state int, character byte) int {
	return a.automaton.Accept(state, character)
}

// DictionaryTermIterator walks one field's terms in lexical order. Terms are
// selected by the FST before their posting bitmap is decoded; Count returns
// only that bitmap's cardinality and never decodes the frequency stream.
// Terms returned by Next are owned by the caller.
type DictionaryTermIterator struct {
	iterator *vellum.FSTIterator
	reader   *storedSegmentReader
	field    string
	closed   bool
}

// NewDictionaryTermIterator opens a bounded dictionary iterator over one
// segment. The end key is exclusive, and nil or empty bounds retain vellum's
// unbounded semantics. Close is safe to call more than once.
func (r *Reader) NewDictionaryTermIterator(field string, automaton DictionaryAutomaton, start, end []byte) (*DictionaryTermIterator, error) {
	if len(r.segments) != 1 {
		return nil, errors.New("nativeice: dictionary iterator requires one segment")
	}
	storedReader, readerErr := r.storedReader(0)
	if readerErr != nil {
		return nil, readerErr
	}
	dictionary, dictionaryErr := storedReader.dictionary(field)
	if dictionaryErr != nil {
		return nil, dictionaryErr
	}
	return newDictionaryTermIterator(storedReader, field, dictionary, automaton, start, end)
}

// NewDictionaryTermIterator opens an iterator using this dictionary handle's
// already-loaded FST, avoiding repeated field-cache lookups.
func (d *Dictionary) NewDictionaryTermIterator(automaton DictionaryAutomaton, start, end []byte) (*DictionaryTermIterator, error) {
	return newDictionaryTermIterator(d.owner, d.field, d.fst, automaton, start, end)
}

func newDictionaryTermIterator(
	storedReader *storedSegmentReader,
	field string,
	dictionary *vellum.FST,
	automaton DictionaryAutomaton,
	start, end []byte,
) (*DictionaryTermIterator, error) {
	if dictionary == nil {
		return &DictionaryTermIterator{reader: storedReader, field: field, closed: true}, nil
	}
	var vellumAutomaton vellum.Automaton
	if automaton != nil {
		vellumAutomaton = vellumDictionaryAutomaton{automaton: automaton}
	}
	// The segment API uses an empty bound for "unbounded". Vellum treats a
	// non-nil empty end as an actual upper bound, which would incorrectly hide
	// every non-empty term.
	if len(start) == 0 {
		start = nil
	}
	if len(end) == 0 {
		end = nil
	}
	// vellum retains both bounds for the iterator lifetime. Own copies here so
	// callers may reuse or mutate their query buffers after this method returns.
	if start != nil {
		start = append([]byte(nil), start...)
	}
	if end != nil {
		end = append([]byte(nil), end...)
	}
	iterator, iteratorErr := dictionary.Search(vellumAutomaton, start, end)
	if iteratorErr != nil {
		if errors.Is(iteratorErr, vellum.ErrIteratorDone) {
			return &DictionaryTermIterator{reader: storedReader, field: field, closed: true}, nil
		}
		return nil, corruptError("search term dictionary", iteratorErr)
	}
	return &DictionaryTermIterator{iterator: iterator, reader: storedReader, field: field}, nil
}

// Next returns the next matching term and its document count. A nil term and
// nil error indicate exhaustion. The empty term is valid and is distinguished
// from exhaustion by its non-zero posting value.
func (i *DictionaryTermIterator) Next() ([]byte, uint64, error) {
	term, _, count, done, nextErr := i.nextEntry(false)
	if nextErr != nil || done {
		return nil, 0, nextErr
	}
	return term, count, nil
}

// NextTerm returns the next dictionary term without decoding its posting bitmap.
// A nil term and nil error indicate exhaustion.
func (i *DictionaryTermIterator) NextTerm() ([]byte, error) {
	if i.closed || i.iterator == nil {
		return nil, nil
	}
	term, postingOffset := i.iterator.Current()
	if term == nil && postingOffset == 0 {
		return nil, i.closeIterator()
	}
	// A non-nil empty slice keeps a valid empty key distinct from exhaustion.
	owned := append([]byte{}, term...)
	if nextErr := i.iterator.Next(); nextErr != nil {
		if errors.Is(nextErr, vellum.ErrIteratorDone) {
			if closeErr := i.closeIterator(); closeErr != nil {
				return nil, closeErr
			}
		} else {
			return nil, errors.Join(corruptError("advance term dictionary", nextErr), i.closeIterator())
		}
	}
	return owned, nil
}

// NextKey advances to the next matching dictionary key without decoding its
// posting cardinality. The present flag distinguishes a valid empty key from
// exhaustion, so range scans do not pay a second posting decode.
func (i *DictionaryTermIterator) NextKey() ([]byte, bool, error) {
	if i.closed || i.iterator == nil {
		return nil, false, nil
	}
	term, postingOffset := i.iterator.Current()
	if term == nil && postingOffset == 0 {
		return nil, false, i.closeIterator()
	}
	owned := append([]byte(nil), term...)
	if nextErr := i.iterator.Next(); nextErr != nil {
		if errors.Is(nextErr, vellum.ErrIteratorDone) {
			if closeErr := i.closeIterator(); closeErr != nil {
				return nil, false, closeErr
			}
		} else {
			return nil, false, errors.Join(corruptError("advance term dictionary", nextErr), i.closeIterator())
		}
	}
	return owned, true, nil
}

// NextString returns the next matching term without the intermediate byte
// slice allocation used by Next. The returned string is owned by the caller.
func (i *DictionaryTermIterator) NextString() (string, uint64, error) {
	_, term, count, done, nextErr := i.nextEntry(true)
	if nextErr != nil || done {
		return "", 0, nextErr
	}
	return term, count, nil
}

func (i *DictionaryTermIterator) nextEntry(asString bool) ([]byte, string, uint64, bool, error) {
	if i.closed || i.iterator == nil {
		return nil, "", 0, true, nil
	}
	term, postingOffset := i.iterator.Current()
	if term == nil && postingOffset == 0 {
		return nil, "", 0, true, i.closeIterator()
	}
	count, countErr := i.reader.postingCount(postingOffset)
	if countErr != nil {
		return nil, "", 0, false, errors.Join(countErr, i.closeIterator())
	}
	var termBytes []byte
	var termString string
	if asString {
		termString = string(term)
	} else {
		termBytes = append([]byte{}, term...)
	}
	if nextErr := i.iterator.Next(); nextErr != nil {
		if errors.Is(nextErr, vellum.ErrIteratorDone) {
			if closeErr := i.closeIterator(); closeErr != nil {
				return nil, "", 0, false, closeErr
			}
		} else {
			advanceErr := corruptError("advance term dictionary", nextErr)
			return nil, "", 0, false, errors.Join(advanceErr, i.closeIterator())
		}
	}
	return termBytes, termString, count, false, nil
}

// Close stops the dictionary walk and releases its FST iterator.
func (i *DictionaryTermIterator) Close() error {
	return i.closeIterator()
}

func (i *DictionaryTermIterator) closeIterator() error {
	i.closed = true
	if i.iterator == nil {
		return nil
	}
	closeErr := i.iterator.Close()
	i.iterator = nil
	return closeErr
}

func (s *storedSegmentReader) postingCount(postingOffset uint64) (uint64, error) {
	switch postingOffset & fstValueEncodingMask {
	case fstValueEncodingOneHit:
		documentNumber := postingOffset & fstValueDocumentMask
		if documentNumber >= s.footer.documentCount {
			return 0, corruptError("segment %q has an out-of-range single-hit posting", s.path)
		}
		return 1, nil
	case 0:
		break
	default:
		return 0, corruptError("segment %q has an unsupported posting encoding", s.path)
	}
	if postingOffset >= s.footer.docValueOffset {
		return 0, corruptError("segment %q has a posting outside its section", s.path)
	}
	cursor := postingOffset
	frequencyOffset, frequencyErr := s.readUvarint(&cursor, s.footer.docValueOffset)
	if frequencyErr != nil {
		return 0, frequencyErr
	}
	locationOffset, locationErr := s.readUvarint(&cursor, s.footer.docValueOffset)
	if locationErr != nil {
		return 0, locationErr
	}
	if locationOffset > 0 && frequencyOffset > 0 {
		if locationOffset > ^uint64(0)-frequencyOffset {
			return 0, corruptError("segment %q has an overflowing posting detail offset", s.path)
		}
		locationOffset += frequencyOffset
	}
	if frequencyOffset > postingOffset || locationOffset > postingOffset {
		return 0, corruptError("segment %q has a posting detail offset outside its section", s.path)
	}
	postingsLength, lengthErr := s.readUvarint(&cursor, s.footer.docValueOffset)
	if lengthErr != nil {
		return 0, lengthErr
	}
	if postingsLength > maxSelectionPostingsSize || postingsLength > s.footer.docValueOffset-cursor {
		return 0, corruptError("segment %q has an oversized posting bitmap", s.path)
	}
	postingsData, dataErr := s.readPostingBytes(cursor, postingsLength)
	if dataErr != nil {
		return 0, dataErr
	}
	postings, decodeErr := decodePostingBitmap(context.Background(), postingsData)
	if decodeErr != nil {
		return 0, corruptError("decode posting bitmap in segment %q", s.path, decodeErr)
	}
	if postings.GetCardinality() > s.footer.documentCount {
		return 0, corruptError("segment %q has a posting bitmap with too many documents", s.path)
	}
	documents := postings.Iterator()
	for documents.HasNext() {
		if uint64(documents.Next()) >= s.footer.documentCount {
			return 0, corruptError("segment %q has an out-of-range posting document", s.path)
		}
	}
	return postings.GetCardinality(), nil
}

// NewDictionaryTermIterators opens one bounded dictionary cursor per pinned
// segment. Callers own and must close every returned cursor.
func (r *Reader) NewDictionaryTermIterators(ctx context.Context, field string) ([]*DictionaryTermIterator, error) {
	iterators := make([]*DictionaryTermIterator, 0, len(r.segments))
	for segmentIndex := range r.segments {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		storedReader, err := r.storedReader(segmentIndex)
		if err != nil {
			for _, iterator := range iterators {
				_ = iterator.Close()
			}
			return nil, err
		}
		dictionary, err := storedReader.dictionary(field)
		if err != nil {
			for _, iterator := range iterators {
				_ = iterator.Close()
			}
			return nil, err
		}
		iterator, err := newDictionaryTermIterator(storedReader, field, dictionary, nil, nil, nil)
		if err != nil {
			for _, existing := range iterators {
				_ = existing.Close()
			}
			return nil, err
		}
		iterators = append(iterators, iterator)
	}
	return iterators, nil
}
