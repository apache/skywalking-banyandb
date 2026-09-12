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

// Package model defines the structures and interfaces for query options and results.
package model

import (
	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// Tag is a tag name and its values.
type Tag struct {
	Name   string
	Values []*modelv1.TagValue
}

// TagFamily is a tag family name and its tags.
type TagFamily struct {
	Name string
	Tags []Tag
}

// Field is a field name and its values.
type Field struct {
	Name   string
	Values []*modelv1.FieldValue
}

// TagProjection is the projection of a tag family and its tags.
type TagProjection struct {
	Family string
	Names  []string
}

// MeasureGroupBy describes a GroupBy clause for a measure query. v1 supports
// a single tag family; each entry in TagNames is a key column. An empty
// TagNames slice means the query carries no GroupBy clause.
type MeasureGroupBy struct {
	TagFamily string
	TagNames  []string
}

// MeasureAgg describes a single aggregation for a measure query. v1 supports
// one aggregation per query — matches the singular QueryRequest.agg proto
// field. FieldName must reference a field in MeasureQueryOptions.FieldProjection.
type MeasureAgg struct {
	FieldName string
	Func      modelv1.AggregationFunction
}

// MeasureQueryOptions is the options of a measure query.
type MeasureQueryOptions struct {
	Query           index.Query
	TimeRange       *timestamp.TimeRange
	Order           *index.OrderBy
	GroupBy         *MeasureGroupBy
	Agg             *MeasureAgg
	Name            string
	Entities        [][]*modelv1.TagValue
	TagProjection   []TagProjection
	FieldProjection []string
	Sort            modelv1.Sort
	Number          int32
	TopNFieldType   databasev1.FieldType
}

// MeasureResult is the result of a query.
type MeasureResult struct {
	Error       error
	Timestamps  []int64
	Versions    []int64
	ShardIDs    []common.ShardID
	TagFamilies []TagFamily
	Fields      []Field
	SID         common.SeriesID
}

// MeasureQueryResult is the result of a measure query.
type MeasureQueryResult interface {
	Pull() *MeasureResult
	Release()
}

// StreamQueryOptions is the options of a stream query.
type StreamQueryOptions struct {
	Name           string
	TimeRange      *timestamp.TimeRange
	Entities       [][]*modelv1.TagValue
	InvertedFilter index.Filter
	SkippingFilter index.Filter
	Order          *index.OrderBy
	TagProjection  []TagProjection
	MaxElementSize int
}

// Reset resets the StreamQueryOptions.
func (s *StreamQueryOptions) Reset() {
	s.Name = ""
	s.TimeRange = nil
	s.Entities = nil
	s.InvertedFilter = nil
	s.SkippingFilter = nil
	s.Order = nil
	s.TagProjection = nil
	s.MaxElementSize = 0
}

// CopyFrom copies the StreamQueryOptions from other to s.
func (s *StreamQueryOptions) CopyFrom(other *StreamQueryOptions) {
	s.Name = other.Name
	s.TimeRange = other.TimeRange

	// Deep copy for Entities if it's a slice
	if other.Entities != nil {
		s.Entities = make([][]*modelv1.TagValue, len(other.Entities))
		copy(s.Entities, other.Entities)
	} else {
		s.Entities = nil
	}

	s.InvertedFilter = other.InvertedFilter
	s.SkippingFilter = other.SkippingFilter
	s.Order = other.Order

	// Deep copy if TagProjection is a slice
	if other.TagProjection != nil {
		s.TagProjection = make([]TagProjection, len(other.TagProjection))
		copy(s.TagProjection, other.TagProjection)
	} else {
		s.TagProjection = nil
	}

	s.MaxElementSize = other.MaxElementSize
}

// TraceQueryOptions is the options of a trace query.
type TraceQueryOptions struct {
	SkippingFilter index.Filter
	TagFilter      TagFilterMatcher
	TimeRange      *timestamp.TimeRange
	Order          *index.OrderBy
	TagProjection  *TagProjection
	Name           string
	Entities       [][]*modelv1.TagValue
	TraceIDs       []string
	MaxTraceSize   int
	MinVal         int64
	MaxVal         int64
}

// Reset resets the TraceQueryOptions.
func (t *TraceQueryOptions) Reset() {
	t.Name = ""
	t.TimeRange = nil
	t.SkippingFilter = nil
	t.TagFilter = nil
	t.Order = nil
	t.TagProjection = nil
	t.Entities = nil
	t.TraceIDs = nil
	t.MaxTraceSize = 0
}

// CopyFrom copies the TraceQueryOptions from other to t.
func (t *TraceQueryOptions) CopyFrom(other *TraceQueryOptions) {
	t.Name = other.Name
	t.TimeRange = other.TimeRange
	t.SkippingFilter = other.SkippingFilter
	t.TagFilter = other.TagFilter
	t.Order = other.Order
	t.TagProjection = other.TagProjection
	t.Entities = other.Entities
	t.MaxTraceSize = other.MaxTraceSize
}

// TraceResult is the result of a query.
type TraceResult struct {
	Error      error
	TID        string
	Spans      [][]byte
	SpanIDs    []string
	Tags       []Tag
	Key        int64
	GroupIndex int
}

// TraceQueryResult is the result of a trace query.
type TraceQueryResult interface {
	Pull() *TraceResult
	Release()
}
