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
	"fmt"
	"io"

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index/posting"
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
	return bytes.Join([][]byte{
		f.SeriesID.Marshal(),
		convert.Uint32ToBytes(f.IndexRuleID),
	}, nil)
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
	Term []byte
	Key  FieldKey
}

// Marshal encodes f to bytes.
func (f Field) Marshal() ([]byte, error) {
	return bytes.Join([][]byte{f.Key.Marshal(), f.Term}, nil), nil
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
	term := raw[12:]
	f.Term = make([]byte, len(term))
	copy(f.Term, term)
	return nil
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

// FieldIterator allows iterating over a field's posting values.
type FieldIterator interface {
	Next() bool
	Val() *PostingValue
	Close() error
}

// DummyFieldIterator never iterates.
var DummyFieldIterator = &dummyIterator{}

type dummyIterator struct{}

func (i *dummyIterator) Next() bool {
	return false
}

func (i *dummyIterator) Val() *PostingValue {
	return nil
}

func (i *dummyIterator) Close() error {
	return nil
}

// PostingValue is the collection of a field's values.
type PostingValue struct {
	Value posting.List
	Term  []byte
}

// Writer allows writing fields and itemID in a document to a index.
type Writer interface {
	Write(fields []Field, itemID common.ItemID) error
}

// FieldIterable allows building a FieldIterator.
type FieldIterable interface {
	Iterator(fieldKey FieldKey, termRange RangeOpts, order modelv1.Sort) (iter FieldIterator, err error)
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
	observability.Observable
	io.Closer
	Writer
	Searcher
}

// GetSearcher returns a searcher associated with input index rule type.
type GetSearcher func(location databasev1.IndexRule_Type) (Searcher, error)

// Filter is a node in the filter tree.
type Filter interface {
	fmt.Stringer
	Execute(getSearcher GetSearcher, seriesID common.SeriesID) (posting.List, error)
}
