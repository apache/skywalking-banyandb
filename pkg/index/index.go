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
	"math"
	"strconv"

	"github.com/blugelabs/bluge/numeric"

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
	// IndexModeName is the name in the index mode.
	IndexModeName = "_im_name"
	// IndexModeEntityTagPrefix is the entity tag prefix in the index mode.
	IndexModeEntityTagPrefix = "_im_entity_tag_"
)

var (
	defaultUpper = convert.Uint64ToBytes(math.MaxUint64)
	defaultLower = convert.Uint64ToBytes(0)
)

// FieldKey is the key of field in a document.
type FieldKey struct {
	TimeRange   *RangeOpts
	Analyzer    string
	TagName     string
	SeriesID    common.SeriesID
	IndexRuleID uint32
}

// Marshal encodes f to string.
func (f FieldKey) Marshal() string {
	if len(f.TagName) > 0 {
		return f.TagName
	}
	return string(convert.Uint32ToBytes(f.IndexRuleID))
}

// NewStringField creates a new string field.
func NewStringField(key FieldKey, value string) Field {
	return Field{
		term: &BytesTermValue{Value: convert.StringToBytes(value)},
		Key:  key,
	}
}

// NewIntField creates a new int field.
func NewIntField(key FieldKey, value int64) Field {
	return Field{
		term: &FloatTermValue{Value: numeric.Int64ToFloat64(value)},
		Key:  key,
	}
}

// NewBytesField creates a new bytes field.
func NewBytesField(key FieldKey, value []byte) Field {
	return Field{
		term: &BytesTermValue{Value: bytes.Clone(value)},
		Key:  key,
	}
}

// Field is a indexed item in a document.
type Field struct {
	term   IsTermValue
	Key    FieldKey
	NoSort bool
	Store  bool
	Index  bool
}

// GetTerm returns the term value of the field.
func (f *Field) GetTerm() IsTermValue {
	return f.term
}

// GetBytes returns the byte value of the field.
func (f *Field) GetBytes() []byte {
	if bv, ok := f.GetTerm().(*BytesTermValue); ok {
		return bv.Value
	}
	panic("field is not bytes")
}

// GetFloat returns the float value of the field.
func (f *Field) GetFloat() float64 {
	if fv, ok := f.GetTerm().(*FloatTermValue); ok {
		return fv.Value
	}
	panic("field is not float")
}

// String returns a string representation of the field.
func (f *Field) String() string {
	return fmt.Sprintf("{\"key\": \"%s\", \"term\": %s}", f.Key.Marshal(), f.term)
}

// MarshalJSON encodes f to JSON.
func (f *Field) MarshalJSON() ([]byte, error) {
	return []byte(f.String()), nil
}

// IsTermValue is the interface for term value.
type IsTermValue interface {
	isTermValue()
	String() string
}

// BytesTermValue represents a byte term value.
type BytesTermValue struct {
	Value []byte
}

func (BytesTermValue) isTermValue() {}

func (b BytesTermValue) String() string {
	return convert.BytesToString(b.Value)
}

// FloatTermValue represents a float term value.
type FloatTermValue struct {
	Value float64
}

func (FloatTermValue) isTermValue() {}

func (f FloatTermValue) String() string {
	return strconv.FormatInt(numeric.Float64ToInt64(f.Value), 10)
}

// RangeOpts contains options to performance a continuous scan.
type RangeOpts struct {
	Upper         IsTermValue
	Lower         IsTermValue
	IncludesUpper bool
	IncludesLower bool
}

// IsEmpty returns true if the range is empty.
func (r RangeOpts) IsEmpty() bool {
	return r.Upper == nil && r.Lower == nil
}

// Valid returns true if the range is valid.
func (r RangeOpts) Valid() bool {
	if r.Upper == nil || r.Lower == nil {
		return false
	}
	switch upper := r.Upper.(type) {
	case *BytesTermValue:
		if bytes.Compare(r.Lower.(*BytesTermValue).Value, upper.Value) > 0 {
			return false
		}
	case *FloatTermValue:
		if r.Lower.(*FloatTermValue).Value > upper.Value {
			return false
		}
	default:
		return false
	}
	return true
}

// NewStringRangeOpts creates a new string range option.
func NewStringRangeOpts(lower, upper string, includesLower, includesUpper bool) RangeOpts {
	var upperBytes, lowerBytes []byte
	if len(upper) == 0 {
		upperBytes = defaultUpper
	} else {
		upperBytes = convert.StringToBytes(upper)
	}
	if len(lower) == 0 {
		lowerBytes = defaultLower
	} else {
		lowerBytes = convert.StringToBytes(lower)
	}
	return RangeOpts{
		Lower:         &BytesTermValue{Value: upperBytes},
		Upper:         &BytesTermValue{Value: lowerBytes},
		IncludesLower: includesLower,
		IncludesUpper: includesUpper,
	}
}

// NewIntRangeOpts creates a new int range option.
func NewIntRangeOpts(lower, upper int64, includesLower, includesUpper bool) RangeOpts {
	return RangeOpts{
		Lower:         &FloatTermValue{Value: numeric.Int64ToFloat64(lower)},
		Upper:         &FloatTermValue{Value: numeric.Int64ToFloat64(upper)},
		IncludesLower: includesLower,
		IncludesUpper: includesUpper,
	}
}

// NewBytesRangeOpts creates a new bytes range option.
func NewBytesRangeOpts(lower, upper []byte, includesLower, includesUpper bool) RangeOpts {
	if len(upper) == 0 {
		upper = defaultUpper
	}
	if len(lower) == 0 {
		lower = defaultLower
	}
	return RangeOpts{
		Lower:         &BytesTermValue{Value: bytes.Clone(lower)},
		Upper:         &BytesTermValue{Value: bytes.Clone(upper)},
		IncludesLower: includesLower,
		IncludesUpper: includesUpper,
	}
}

// DocumentResult represents a document in an index.
type DocumentResult struct {
	EntityValues []byte
	Values       map[string][]byte
	SortedValue  []byte
	SeriesID     common.SeriesID
	DocID        uint64
	Timestamp    int64
	Version      int64
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
	Version      int64
}

// Documents is a collection of documents.
type Documents []Document

// Batch is a collection of documents.
type Batch struct {
	PersistentCallback func(error)
	Documents          Documents
}

// Writer allows writing fields and docID in a document to an index.
type Writer interface {
	Batch(batch Batch) error
	InsertSeriesBatch(batch Batch) error
	UpdateSeriesBatch(batch Batch) error
	Delete(docID [][]byte) error
}

// FieldIterable allows building a FieldIterator.
type FieldIterable interface {
	BuildQuery(seriesMatchers []SeriesMatcher, secondaryQuery Query, timeRange *timestamp.TimeRange) (Query, error)
	Iterator(ctx context.Context, fieldKey FieldKey, termRange RangeOpts, order modelv1.Sort,
		preLoadSize int) (iter FieldIterator[*DocumentResult], err error)
	Sort(ctx context.Context, sids []common.SeriesID, fieldKey FieldKey,
		order modelv1.Sort, timeRange *timestamp.TimeRange, preLoadSize int) (FieldIterator[*DocumentResult], error)
}

// Searcher allows searching a field either by its key or by its key and term.
type Searcher interface {
	FieldIterable
	Match(fieldKey FieldKey, match []string, opts *modelv1.Condition_MatchOption) (list posting.List, timestamps posting.List, err error)
	MatchField(fieldKey FieldKey) (list posting.List, timestamps posting.List, err error)
	MatchTerms(field Field) (list posting.List, timestamps posting.List, err error)
	Range(fieldKey FieldKey, opts RangeOpts) (list posting.List, timestamps posting.List, err error)
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
	Reset()
	TakeFileSnapshot(dst string) error
}

// Series represents a series in an index.
type Series struct {
	EntityValues []byte
}

func (s Series) String() string {
	return convert.BytesToString(s.EntityValues)
}

// SortedField returns the value of the sorted field.
func (s Series) SortedField() []byte {
	return s.EntityValues
}

// SeriesDocument represents a series document in an index.
type SeriesDocument struct {
	Fields    map[string][]byte
	Key       Series
	Timestamp int64
	Version   int64
}

// OrderByType is the type of order by.
type OrderByType int

const (
	// OrderByTypeTime is the order by time.
	OrderByTypeTime OrderByType = iota
	// OrderByTypeIndex is the order by index.
	OrderByTypeIndex
	// OrderByTypeSeries is the order by series.
	OrderByTypeSeries
)

// OrderBy is the order by rule.
type OrderBy struct {
	Index *databasev1.IndexRule
	Sort  modelv1.Sort
	Type  OrderByType
}

// SeriesStore is an abstract of a series repository.
type SeriesStore interface {
	Store
	// Search returns a list of series that match the given matchers.
	Search(context.Context, []FieldKey, Query, int) ([]SeriesDocument, error)
	SeriesIterator(context.Context) (FieldIterator[Series], error)
	SeriesSort(ctx context.Context, indexQuery Query, orderBy *OrderBy,
		preLoadSize int, fieldKeys []FieldKey) (iter FieldIterator[*DocumentResult], err error)
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
	Execute(getSearcher GetSearcher, seriesID common.SeriesID, timeRange *RangeOpts) (posting.List, posting.List, error)
	ShouldSkip(tagFamilyFilters FilterOp) (bool, error)
}

// FilterOp is an interface for filtering operations based on skipping index.
type FilterOp interface {
	Eq(tagName string, tagValue string) bool
	Range(tagName string, rangeOpts RangeOpts) (bool, error)
}
