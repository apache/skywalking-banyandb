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
	"github.com/apache/skywalking-banyandb/pkg/encoding"
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
func (fk FieldKey) Marshal() string {
	if len(fk.TagName) > 0 {
		return fk.TagName
	}
	return string(convert.Uint32ToBytes(fk.IndexRuleID))
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
	Marshal() ([]byte, error)
	Unmarshal(data []byte) error
}

// BytesTermValue represents a byte term value.
type BytesTermValue struct {
	Value []byte
}

func (BytesTermValue) isTermValue() {}

func (bv BytesTermValue) String() string {
	return convert.BytesToString(bv.Value)
}

// Marshal encodes BytesTermValue to bytes.
func (bv BytesTermValue) Marshal() ([]byte, error) {
	var result []byte
	result = append(result, 0) // type: bytes
	result = encoding.EncodeBytes(result, bv.Value)
	return result, nil
}

// Unmarshal decodes BytesTermValue from bytes.
func (bv *BytesTermValue) Unmarshal(data []byte) error {
	if len(data) < 1 {
		return fmt.Errorf("insufficient data for term value type")
	}

	if data[0] != 0 {
		return fmt.Errorf("invalid term value type, expected bytes (0), got %d", data[0])
	}

	var valueBytes []byte
	var err error
	_, valueBytes, err = encoding.DecodeBytes(data[1:])
	if err != nil {
		return fmt.Errorf("failed to decode bytes value: %w", err)
	}

	bv.Value = valueBytes
	return nil
}

// FloatTermValue represents a float term value.
type FloatTermValue struct {
	Value float64
}

func (FloatTermValue) isTermValue() {}

func (fv FloatTermValue) String() string {
	return strconv.FormatInt(numeric.Float64ToInt64(fv.Value), 10)
}

// Marshal encodes FloatTermValue to bytes.
func (fv FloatTermValue) Marshal() ([]byte, error) {
	var result []byte
	result = append(result, 1) // type: float
	result = encoding.EncodeBytes(result, convert.Float64ToBytes(fv.Value))
	return result, nil
}

// Unmarshal decodes FloatTermValue from bytes.
func (fv *FloatTermValue) Unmarshal(data []byte) error {
	if len(data) < 1 {
		return fmt.Errorf("insufficient data for term value type")
	}

	if data[0] != 1 {
		return fmt.Errorf("invalid term value type, expected float (1), got %d", data[0])
	}

	var valueBytes []byte
	var err error
	_, valueBytes, err = encoding.DecodeBytes(data[1:])
	if err != nil {
		return fmt.Errorf("failed to decode float value: %w", err)
	}

	if len(valueBytes) != 8 {
		return fmt.Errorf("invalid float value length: %d", len(valueBytes))
	}

	fv.Value = convert.BytesToFloat64(valueBytes)
	return nil
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

// Marshal encodes RangeOpts to bytes.
func (r RangeOpts) Marshal() ([]byte, error) {
	var result []byte

	// Marshal Upper
	if r.Upper != nil {
		result = append(result, 1) // has upper
		upperBytes, err := r.Upper.Marshal()
		if err != nil {
			return nil, fmt.Errorf("failed to marshal upper: %w", err)
		}
		result = encoding.EncodeBytes(result, upperBytes)
	} else {
		result = append(result, 0) // no upper
	}

	// Marshal Lower
	if r.Lower != nil {
		result = append(result, 1) // has lower
		lowerBytes, err := r.Lower.Marshal()
		if err != nil {
			return nil, fmt.Errorf("failed to marshal lower: %w", err)
		}
		result = encoding.EncodeBytes(result, lowerBytes)
	} else {
		result = append(result, 0) // no lower
	}

	// Marshal boolean flags
	flags := byte(0)
	if r.IncludesUpper {
		flags |= 1
	}
	if r.IncludesLower {
		flags |= 2
	}
	result = append(result, flags)

	return result, nil
}

// Unmarshal decodes RangeOpts from bytes.
func (r *RangeOpts) Unmarshal(data []byte) error {
	if len(data) < 3 {
		return fmt.Errorf("insufficient data for range options")
	}

	// Unmarshal Upper
	hasUpper := data[0] == 1
	tail := data[1:]

	if hasUpper {
		var upperBytes []byte
		var err error
		tail, upperBytes, err = encoding.DecodeBytes(tail)
		if err != nil {
			return fmt.Errorf("failed to decode upper: %w", err)
		}
		// Create the appropriate type based on the first byte
		if len(upperBytes) > 0 {
			switch upperBytes[0] {
			case 0:
				r.Upper = &BytesTermValue{}
			case 1:
				r.Upper = &FloatTermValue{}
			default:
				return fmt.Errorf("unknown term value type for upper: %d", upperBytes[0])
			}
		} else {
			return fmt.Errorf("empty upperBytes for upper term")
		}
		if err := r.Upper.Unmarshal(upperBytes); err != nil {
			return fmt.Errorf("failed to unmarshal upper: %w", err)
		}
	} else {
		r.Upper = nil
	}

	// Unmarshal Lower
	hasLower := tail[0] == 1
	tail = tail[1:]

	if hasLower {
		var lowerBytes []byte
		var err error
		tail, lowerBytes, err = encoding.DecodeBytes(tail)
		if err != nil {
			return fmt.Errorf("failed to decode lower: %w", err)
		}
		// Create the appropriate type based on the first byte
		if len(lowerBytes) > 0 {
			switch lowerBytes[0] {
			case 0:
				r.Lower = &BytesTermValue{}
			case 1:
				r.Lower = &FloatTermValue{}
			default:
				return fmt.Errorf("unknown term value type for lower: %d", lowerBytes[0])
			}
		} else {
			return fmt.Errorf("empty lowerBytes for lower term")
		}
		if err := r.Lower.Unmarshal(lowerBytes); err != nil {
			return fmt.Errorf("failed to unmarshal lower: %w", err)
		}
	} else {
		r.Lower = nil
	}

	// Unmarshal boolean flags
	flags := tail[0]
	r.IncludesUpper = (flags & 1) != 0
	r.IncludesLower = (flags & 2) != 0

	return nil
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
		Lower:         &BytesTermValue{Value: lowerBytes},
		Upper:         &BytesTermValue{Value: upperBytes},
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

// Marshal encodes Documents to bytes.
func (docs Documents) Marshal() ([]byte, error) {
	var result []byte

	// Write the number of documents
	result = encoding.VarUint64ToBytes(result, uint64(len(docs)))

	// Marshal each document
	for _, doc := range docs {
		docBytes, err := doc.Marshal()
		if err != nil {
			return nil, fmt.Errorf("failed to marshal document: %w", err)
		}
		result = encoding.EncodeBytes(result, docBytes)
	}

	return result, nil
}

// Unmarshal decodes Documents from bytes.
func (docs *Documents) Unmarshal(data []byte) error {
	if len(data) == 0 {
		*docs = nil
		return nil
	}

	// Read the number of documents
	tail, docCount := encoding.BytesToVarUint64(data)
	if uint64(len(tail)) < docCount {
		return fmt.Errorf("insufficient data for documents: need %d documents, have %d bytes", docCount, len(tail))
	}

	// Initialize the documents slice
	*docs = make(Documents, 0, docCount)

	// Unmarshal each document
	for i := uint64(0); i < docCount; i++ {
		var docBytes []byte
		var err error
		tail, docBytes, err = encoding.DecodeBytes(tail)
		if err != nil {
			return fmt.Errorf("failed to decode document %d: %w", i, err)
		}

		var doc Document
		if err := doc.Unmarshal(docBytes); err != nil {
			return fmt.Errorf("failed to unmarshal document %d: %w", i, err)
		}

		*docs = append(*docs, doc)
	}

	return nil
}

// Marshal encodes Document to bytes.
func (doc Document) Marshal() ([]byte, error) {
	var result []byte

	// Marshal EntityValues
	result = encoding.EncodeBytes(result, doc.EntityValues)

	// Marshal Timestamp
	result = encoding.VarInt64ToBytes(result, doc.Timestamp)

	// Marshal DocID
	result = encoding.VarUint64ToBytes(result, doc.DocID)

	// Marshal Version
	result = encoding.VarInt64ToBytes(result, doc.Version)

	// Marshal Fields
	result = encoding.VarUint64ToBytes(result, uint64(len(doc.Fields)))
	for _, field := range doc.Fields {
		fieldBytes, err := field.Marshal()
		if err != nil {
			return nil, fmt.Errorf("failed to marshal field: %w", err)
		}
		result = encoding.EncodeBytes(result, fieldBytes)
	}

	return result, nil
}

// Unmarshal decodes Document from bytes.
func (doc *Document) Unmarshal(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("empty data for document")
	}
	var (
		tail         []byte
		entityValues []byte
		err          error
	)
	tail, entityValues, err = encoding.DecodeBytes(data)
	if err != nil {
		return fmt.Errorf("failed to decode entity values: %w", err)
	}
	doc.EntityValues = entityValues
	var timestamp int64
	tail, timestamp, err = encoding.BytesToVarInt64(tail)
	if err != nil {
		return fmt.Errorf("failed to decode timestamp: %w", err)
	}
	doc.Timestamp = timestamp
	tail, doc.DocID = encoding.BytesToVarUint64(tail)
	var version int64
	tail, version, err = encoding.BytesToVarInt64(tail)
	if err != nil {
		return fmt.Errorf("failed to decode version: %w", err)
	}
	doc.Version = version
	tail, fieldCount := encoding.BytesToVarUint64(tail)
	doc.Fields = make([]Field, 0, fieldCount)
	for i := uint64(0); i < fieldCount; i++ {
		var fieldBytes []byte
		tail, fieldBytes, err = encoding.DecodeBytes(tail)
		if err != nil {
			return fmt.Errorf("failed to decode field %d: %w", i, err)
		}
		var field Field
		if err := field.Unmarshal(fieldBytes); err != nil {
			return fmt.Errorf("failed to unmarshal field %d: %w", i, err)
		}
		doc.Fields = append(doc.Fields, field)
	}
	return nil
}

// Marshal encodes Field to bytes.
func (f Field) Marshal() ([]byte, error) {
	var result []byte
	// Marshal Key
	keyBytes, err := f.Key.MarshalAll()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal field key: %w", err)
	}
	result = encoding.EncodeBytes(result, keyBytes)
	// Marshal term
	termBytes, err := f.term.Marshal()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal field term: %w", err)
	}
	result = encoding.EncodeBytes(result, termBytes)
	// Marshal boolean flags
	flags := byte(0)
	if f.NoSort {
		flags |= 1
	}
	if f.Store {
		flags |= 2
	}
	if f.Index {
		flags |= 4
	}
	result = append(result, flags)
	return result, nil
}

// Unmarshal decodes Field from bytes.
func (f *Field) Unmarshal(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("empty data for field")
	}
	var (
		tail     []byte
		keyBytes []byte
		err      error
	)
	tail, keyBytes, err = encoding.DecodeBytes(data)
	if err != nil {
		return fmt.Errorf("failed to decode field key: %w", err)
	}
	if unmarshalErr := f.Key.UnmarshalAll(keyBytes); unmarshalErr != nil {
		return fmt.Errorf("failed to unmarshal field key: %w", unmarshalErr)
	}
	var termBytes []byte
	tail, termBytes, err = encoding.DecodeBytes(tail)
	if err != nil {
		return fmt.Errorf("failed to decode field term: %w", err)
	}
	if f.term == nil {
		// Try to detect type from first byte
		if len(termBytes) > 0 {
			switch termBytes[0] {
			case 0:
				f.term = &BytesTermValue{}
			case 1:
				f.term = &FloatTermValue{}
			default:
				return fmt.Errorf("unknown term value type: %d", termBytes[0])
			}
		} else {
			return fmt.Errorf("empty termBytes for field term")
		}
	}
	if err := f.term.Unmarshal(termBytes); err != nil {
		return fmt.Errorf("failed to unmarshal field term: %w", err)
	}
	if len(tail) < 1 {
		return fmt.Errorf("insufficient data for field flags")
	}
	flags := tail[0]
	f.NoSort = (flags & 1) != 0
	f.Store = (flags & 2) != 0
	f.Index = (flags & 4) != 0
	return nil
}

// MarshalAll encodes FieldKey to bytes.
func (fk FieldKey) MarshalAll() ([]byte, error) {
	var result []byte

	// Marshal TimeRange
	if fk.TimeRange != nil {
		result = append(result, 1) // has time range
		timeRangeBytes, err := fk.TimeRange.Marshal()
		if err != nil {
			return nil, fmt.Errorf("failed to marshal time range: %w", err)
		}
		result = encoding.EncodeBytes(result, timeRangeBytes)
	} else {
		result = append(result, 0) // no time range
	}

	// Marshal Analyzer
	result = encoding.EncodeBytes(result, convert.StringToBytes(fk.Analyzer))

	// Marshal TagName
	result = encoding.EncodeBytes(result, convert.StringToBytes(fk.TagName))

	// Marshal SeriesID
	result = fk.SeriesID.AppendToBytes(result)

	// Marshal IndexRuleID
	result = encoding.Uint32ToBytes(result, fk.IndexRuleID)

	return result, nil
}

// UnmarshalAll decodes FieldKey from bytes.
func (fk *FieldKey) UnmarshalAll(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("empty data for field key")
	}
	// Unmarshal TimeRange
	if len(data) < 1 {
		return fmt.Errorf("insufficient data for time range flag")
	}
	hasTimeRange := data[0] == 1
	tail := data[1:]
	if hasTimeRange {
		var timeRangeBytes []byte
		var err error
		tail, timeRangeBytes, err = encoding.DecodeBytes(tail)
		if err != nil {
			return fmt.Errorf("failed to decode time range: %w", err)
		}
		fk.TimeRange = &RangeOpts{}
		if err := fk.TimeRange.Unmarshal(timeRangeBytes); err != nil {
			return fmt.Errorf("failed to unmarshal time range: %w", err)
		}
	} else {
		fk.TimeRange = nil
	}
	// Unmarshal Analyzer
	var analyzerBytes []byte
	var err error
	tail, analyzerBytes, err = encoding.DecodeBytes(tail)
	if err != nil {
		return fmt.Errorf("failed to decode analyzer: %w", err)
	}
	fk.Analyzer = convert.BytesToString(analyzerBytes)
	// Unmarshal TagName
	var tagNameBytes []byte
	tail, tagNameBytes, err = encoding.DecodeBytes(tail)
	if err != nil {
		return fmt.Errorf("failed to decode tag name: %w", err)
	}
	fk.TagName = convert.BytesToString(tagNameBytes)
	// Unmarshal SeriesID
	if len(tail) < 8 {
		return fmt.Errorf("insufficient data for series ID")
	}
	fk.SeriesID = common.SeriesID(encoding.BytesToUint64(tail))
	tail = tail[8:]
	// Unmarshal IndexRuleID (fixed 4 bytes)
	if len(tail) < 4 {
		return fmt.Errorf("insufficient data for IndexRuleID")
	}
	fk.IndexRuleID = encoding.BytesToUint32(tail)
	// No need to advance tail further as this is the last field
	return nil
}

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
	EnableExternalSegments() (ExternalSegmentStreamer, error)
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

// ExternalSegmentStreamer provides methods for streaming external segments into an index.
type ExternalSegmentStreamer interface {
	// StartSegment begins streaming a new segment.
	StartSegment() error
	// WriteChunk writes a chunk of segment data.
	WriteChunk(data []byte) error
	// CompleteSegment signals completion of segment streaming.
	CompleteSegment() error
	// Status returns the current status of the segment streaming.
	Status() string
	// BytesReceived returns the number of bytes received so far.
	BytesReceived() uint64
}

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
