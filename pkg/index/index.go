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

// Package index implements the index system for searching data.
package index

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index/posting"
	"github.com/apache/skywalking-banyandb/pkg/iter/sort"
)

var errMalformed = errors.New("the data is malformed")

// FieldKey is the key of field in a document.
type FieldKey struct {
	SeriesID    common.SeriesID
	IndexRuleID uint32
	Analyzer    databasev1.IndexRule_Analyzer
}

// MarshalIndexRule encodes the index rule id to string representation.
func (f FieldKey) MarshalIndexRule() string {
	return string(convert.Uint32ToBytes(f.IndexRuleID))
}

// Marshal encodes f to bytes.
func (f FieldKey) Marshal() []byte {
	var s []byte
	if f.HasSeriesID() {
		s = f.SeriesID.Marshal()
	}
	i := []byte(f.MarshalIndexRule())
	b := make([]byte, len(s)+len(i))
	copy(b, s)
	copy(b[len(s):], i)
	return b
}

// HasSeriesID reports whether f has a series id.
func (f FieldKey) HasSeriesID() bool {
	return f.SeriesID > 0
}

// MarshalToStr encodes f to string.
func (f FieldKey) MarshalToStr() string {
	return string(f.Marshal())
}

// Unmarshal decodes bytes to f.
func (f *FieldKey) Unmarshal(raw []byte) error {
	f.SeriesID = common.SeriesID(convert.BytesToUint64(raw[0:8]))
	f.IndexRuleID = convert.BytesToUint32(raw[8:])
	return nil
}

// Equal reports whether f and other have the same series id and index rule id.
func (f FieldKey) Equal(other FieldKey) bool {
	return f.SeriesID == other.SeriesID && f.IndexRuleID == other.IndexRuleID
}

// Field is a indexed item in a document.
type Field struct {
	Term   []byte
	Key    FieldKey
	NoSort bool
}

// Marshal encodes f to bytes.
func (f Field) Marshal() []byte {
	s := f.Key.SeriesID.Marshal()
	i := []byte(f.Key.MarshalIndexRule())
	b := make([]byte, len(s)+len(i)+len(f.Term))
	bp := copy(b, s)
	bp += copy(b[bp:], i)
	copy(b[bp:], f.Term)
	return b
}

// FieldStr return a string represent of Field which is composed by key and term.
func FieldStr(key FieldKey, term []byte) string {
	f := Field{Key: key, Term: term}
	return string(f.Marshal())
}

// Unmarshal decodes bytes to f.
func (f *Field) Unmarshal(raw []byte) error {
	if len(raw) < 13 {
		return errors.WithMessagef(errMalformed, "malformed field: expected more than 12, got %d", len(raw))
	}
	fk := &f.Key
	err := fk.Unmarshal(raw[:12])
	if err != nil {
		return errors.Wrap(err, "unmarshal a field")
	}
	f.Term = UnmarshalTerm(raw)
	return nil
}

// UnmarshalTerm decodes term from a encoded field.
func UnmarshalTerm(raw []byte) []byte {
	term := raw[12:]
	result := make([]byte, len(term))
	copy(result, term)
	return result
}

// RangeOpts contains options to performance a continuous scan.
type RangeOpts struct {
	Upper         []byte
	Lower         []byte
	IncludesUpper bool
	IncludesLower bool
}

// Between reports whether value is in the range.
func (r RangeOpts) Between(value []byte) int {
	if r.Upper != nil {
		var in bool
		if r.IncludesUpper {
			in = bytes.Compare(r.Upper, value) >= 0
		} else {
			in = bytes.Compare(r.Upper, value) > 0
		}
		if !in {
			return 1
		}
	}
	if r.Lower != nil {
		var in bool
		if r.IncludesLower {
			in = bytes.Compare(r.Lower, value) <= 0
		} else {
			in = bytes.Compare(r.Lower, value) < 0
		}
		if !in {
			return -1
		}
	}
	return 0
}

type ItemRef struct {
	SeriesID common.SeriesID
	DocID    uint64
	Term     []byte
}

func (ir ItemRef) SortedField() []byte {
	return ir.Term
}

// FieldIterator allows iterating over a field's posting values.
type FieldIterator[T sort.Comparable] interface {
	Next() bool
	Val() T
	Close() error
}

// DummyFieldIterator never iterates.
var DummyFieldIterator = &dummyIterator{}

type dummyIterator struct{}

func (i *dummyIterator) Next() bool {
	return false
}

func (i *dummyIterator) Val() *ItemRef {
	return &ItemRef{}
}

func (i *dummyIterator) Close() error {
	return nil
}

// Document represents a document in a index.
type Document struct {
	Fields       []Field
	EntityValues []byte
	DocID        uint64
}

// Documents is a collection of documents.
type Documents []Document

// Batch is a collection of documents.
type Batch struct {
	Applied   chan struct{}
	Documents Documents
}

// Writer allows writing fields and docID in a document to a index.
type Writer interface {
	Write(fields []Field, docID uint64) error
	Batch(batch Batch) error
}

// FieldIterable allows building a FieldIterator.
type FieldIterable interface {
	Iterator(fieldKey FieldKey, termRange RangeOpts, order modelv1.Sort, preLoadSize int) (iter FieldIterator[*ItemRef], err error)
	Sort(sids []common.SeriesID, fieldKey FieldKey, order modelv1.Sort, preLoadSize int) (FieldIterator[*ItemRef], error)
}

// Searcher allows searching a field either by its key or by its key and term.
type Searcher interface {
	FieldIterable
	Match(fieldKey FieldKey, match []string) (list posting.List, err error)
	MatchField(fieldKey FieldKey) (list posting.List, err error)
	MatchTerms(field Field) (list posting.List, err error)
	Range(fieldKey FieldKey, opts RangeOpts) (list posting.List, err error)
}

// Store is an abstract of a index repository.
type Store interface {
	io.Closer
	Writer
	Searcher
	SizeOnDisk() int64
}

// Series represents a series in a index.
type Series struct {
	EntityValues []byte
	ID           common.SeriesID
}

func (s Series) String() string {
	return fmt.Sprintf("%s:%d", s.EntityValues, s.ID)
}

// SeriesStore is an abstract of a series repository.
type SeriesStore interface {
	Store
	// Search returns a list of series that match the given matchers.
	Search(context.Context, []SeriesMatcher) ([]Series, error)
}

// SeriesMatcherType represents the type of series matcher.
type SeriesMatcherType int

const (
	// SeriesMatcherTypeExact represents an exact matcher.
	SeriesMatcherTypeExact SeriesMatcherType = iota
	// SeriesMatcherTypePrefix represents a prefix matcher.
	SeriesMatcherTypePrefix
	// SeriesMatcherTypeWildcard represents a wildcard matcher.
	SeriesMatcherTypeWildcard
)

// SeriesMatcher represents a series matcher.
type SeriesMatcher struct {
	Match []byte
	Type  SeriesMatcherType
}

// String returns a string representation of the series matcher.
func (s SeriesMatcher) String() string {
	switch s.Type {
	case SeriesMatcherTypeExact:
		return fmt.Sprintf("exact:%s", s.Match)
	case SeriesMatcherTypePrefix:
		return fmt.Sprintf("prefix:%s", s.Match)
	case SeriesMatcherTypeWildcard:
		return fmt.Sprintf("wildcard:%s", s.Match)
	default:
		return fmt.Sprintf("unknown:%s", s.Match)
	}
}

// GetSearcher returns a searcher associated with input index rule type.
type GetSearcher func(location databasev1.IndexRule_Type) (Searcher, error)

// Filter is a node in the filter tree.
type Filter interface {
	fmt.Stringer
	Execute(getSearcher GetSearcher, seriesID common.SeriesID) (posting.List, error)
}
