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

	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index/posting"
	"github.com/apache/skywalking-banyandb/pkg/iter/sort"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

const (
	// AnalyzerUnspecified represents an unspecified analyzer.
	AnalyzerUnspecified = ""
	// AnalyzerKeyword is a “noop” analyzer which returns the entire input string as a single token.
	AnalyzerKeyword = "keyword"
	// AnalyzerSimple breaks text into tokens at any non-letter character.
	AnalyzerSimple = "simple"
	// AnalyzerStandard provides grammar based tokenization.
	AnalyzerStandard = "standard"
	// AnalyzerURL breaks test into tokens at any non-letter and non-digit character.
	AnalyzerURL = "url"
)

// FieldKey is the key of field in a document.
type FieldKey struct {
	Analyzer    string
	SeriesID    common.SeriesID
	IndexRuleID uint32
}

// Marshal encodes f to string.
func (f FieldKey) Marshal() string {
	return string(convert.Uint32ToBytes(f.IndexRuleID))
}

// HasSeriesID reports whether f has a series id.
func (f FieldKey) HasSeriesID() bool {
	return f.SeriesID > 0
}

// Field is a indexed item in a document.
type Field struct {
	Term   []byte
	Key    FieldKey
	NoSort bool
	Store  bool
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

// DocumentResult represents a document in an index.
type DocumentResult struct {
	EntityValues []byte
	Values       map[string][]byte
	SortedValue  []byte
	SeriesID     common.SeriesID
	DocID        uint64
	Timestamp    int64
}

// SortedField returns the value of the sorted field.
func (ir DocumentResult) SortedField() []byte {
	return ir.SortedValue
}

// FieldIterator allows iterating over a field's posting values.
type FieldIterator[T sort.Comparable] interface {
	Next() bool
	Val() T
	Close() error
	Query() Query
}

// DummyFieldIterator never iterates.
var DummyFieldIterator = &dummyIterator{}

type dummyIterator struct{}

func (i *dummyIterator) Next() bool {
	return false
}

func (i *dummyIterator) Val() *DocumentResult {
	return &DocumentResult{}
}

func (i *dummyIterator) Close() error {
	return nil
}

func (i *dummyIterator) Query() Query {
	return nil
}

// Document represents a document in an index.
type Document struct {
	Fields       []Field
	EntityValues []byte
	Timestamp    int64
	DocID        uint64
}

// Documents is a collection of documents.
type Documents []Document

// Batch is a collection of documents.
type Batch struct {
	Documents Documents
}

// Writer allows writing fields and docID in a document to an index.
type Writer interface {
	Batch(batch Batch) error
}

// FieldIterable allows building a FieldIterator.
type FieldIterable interface {
	BuildQuery(seriesMatchers []SeriesMatcher, secondaryQuery Query) (Query, error)
	Iterator(ctx context.Context, fieldKey FieldKey, termRange RangeOpts, order modelv1.Sort,
		preLoadSize int, query Query, fieldKeys []FieldKey) (iter FieldIterator[*DocumentResult], err error)
	Sort(ctx context.Context, sids []common.SeriesID, fieldKey FieldKey,
		order modelv1.Sort, timeRange *timestamp.TimeRange, preLoadSize int) (FieldIterator[*DocumentResult], error)
}

// Searcher allows searching a field either by its key or by its key and term.
type Searcher interface {
	FieldIterable
	Match(fieldKey FieldKey, match []string, opts *modelv1.Condition_MatchOption) (list posting.List, err error)
	MatchField(fieldKey FieldKey) (list posting.List, err error)
	MatchTerms(field Field) (list posting.List, err error)
	Range(fieldKey FieldKey, opts RangeOpts) (list posting.List, err error)
}

// Query is an abstract of an index query.
type Query interface {
	fmt.Stringer
}

// Store is an abstract of an index repository.
type Store interface {
	io.Closer
	Writer
	Searcher
	CollectMetrics(...string)
}

// Series represents a series in an index.
type Series struct {
	EntityValues []byte
	ID           common.SeriesID
}

func (s Series) String() string {
	return fmt.Sprintf("%s:%d", s.EntityValues, s.ID)
}

// SortedField returns the value of the sorted field.
func (s Series) SortedField() []byte {
	return s.EntityValues
}

// SeriesDocument represents a series document in an index.
type SeriesDocument struct {
	Fields map[string][]byte
	Key    Series
}

// SeriesStore is an abstract of a series repository.
type SeriesStore interface {
	Store
	// Search returns a list of series that match the given matchers.
	Search(context.Context, []FieldKey, Query) ([]SeriesDocument, error)
	SeriesIterator(context.Context) (FieldIterator[Series], error)
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
