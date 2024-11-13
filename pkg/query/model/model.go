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
	"context"

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

// OrderBy is the order by rule.
type OrderBy struct {
	Index *databasev1.IndexRule
	Sort  modelv1.Sort
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

// MeasureQueryOptions is the options of a measure query.
type MeasureQueryOptions struct {
	Query           index.Query
	TimeRange       *timestamp.TimeRange
	Order           *OrderBy
	Name            string
	Entities        [][]*modelv1.TagValue
	TagProjection   []TagProjection
	FieldProjection []string
	OrderByType     OrderByType
}

// MeasureResult is the result of a query.
type MeasureResult struct {
	Error       error
	Timestamps  []int64
	Versions    []int64
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
	Filter         index.Filter
	Order          *OrderBy
	TagProjection  []TagProjection
	MaxElementSize int
}

// StreamResult is the result of a query.
type StreamResult struct {
	Error       error
	Timestamps  []int64
	ElementIDs  []uint64
	TagFamilies []TagFamily
	SIDs        []common.SeriesID
}

// StreamQueryResult is the result of a stream query.
type StreamQueryResult interface {
	Pull(context.Context) *StreamResult
	Release()
}
