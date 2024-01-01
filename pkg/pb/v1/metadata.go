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

// Package v1 implements helpers to access data defined by API v1.
package v1

import (
	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// FindTagByName finds TagSpec in several tag families by its name.
// The tag name should be unique in these families.
func FindTagByName(families []*databasev1.TagFamilySpec, tagName string) (int, int, *databasev1.TagSpec) {
	for fi, family := range families {
		for ti, tag := range family.Tags {
			if tagName == tag.GetName() {
				return fi, ti, tag
			}
		}
	}
	return 0, 0, nil
}

func tagValueTypeConv(tagValue *modelv1.TagValue) (tagType databasev1.TagType, isNull bool) {
	switch tagValue.GetValue().(type) {
	case *modelv1.TagValue_Int:
		return databasev1.TagType_TAG_TYPE_INT, false
	case *modelv1.TagValue_Str:
		return databasev1.TagType_TAG_TYPE_STRING, false
	case *modelv1.TagValue_IntArray:
		return databasev1.TagType_TAG_TYPE_INT_ARRAY, false
	case *modelv1.TagValue_StrArray:
		return databasev1.TagType_TAG_TYPE_STRING_ARRAY, false
	case *modelv1.TagValue_BinaryData:
		return databasev1.TagType_TAG_TYPE_DATA_BINARY, false
	case *modelv1.TagValue_Null:
		return databasev1.TagType_TAG_TYPE_UNSPECIFIED, true
	}
	return databasev1.TagType_TAG_TYPE_UNSPECIFIED, false
}

// FieldValueTypeConv recognizes the field type from its value.
func FieldValueTypeConv(fieldValue *modelv1.FieldValue) (tagType databasev1.FieldType, isNull bool) {
	switch fieldValue.GetValue().(type) {
	case *modelv1.FieldValue_Int:
		return databasev1.FieldType_FIELD_TYPE_INT, false
	case *modelv1.FieldValue_Float:
		return databasev1.FieldType_FIELD_TYPE_FLOAT, false
	case *modelv1.FieldValue_Str:
		return databasev1.FieldType_FIELD_TYPE_STRING, false
	case *modelv1.FieldValue_BinaryData:
		return databasev1.FieldType_FIELD_TYPE_DATA_BINARY, false
	case *modelv1.FieldValue_Null:
		return databasev1.FieldType_FIELD_TYPE_UNSPECIFIED, true
	}
	return databasev1.FieldType_FIELD_TYPE_UNSPECIFIED, false
}

// OrderBy is the order by rule.
type OrderBy struct {
	Index *databasev1.IndexRule
	Sort  modelv1.Sort
}

// AnyTagValue is the `*` for a regular expression. It could match "any" Entry in an Entity.
var AnyTagValue = &modelv1.TagValue{Value: &modelv1.TagValue_Null{}}

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

// Result is the result of a query.
type Result struct {
	Timestamps  []int64
	ElementIDs  []string
	TagFamilies []TagFamily
	Fields      []Field
	SID         common.SeriesID
}

// TagProjection is the projection of a tag family and its tags.
type TagProjection struct {
	Family string
	Names  []string
}

// StreamQueryOptions is the options of a stream query.
type StreamQueryOptions struct {
	Name          string
	TimeRange     *timestamp.TimeRange
	Entity        []*modelv1.TagValue
	Filter        index.Filter
	Order         *OrderBy
	TagProjection []TagProjection
}

// StreamSortOptions is the options of a stream sort.
type StreamSortOptions struct {
	Name          string
	TimeRange     *timestamp.TimeRange
	Entities      [][]*modelv1.TagValue
	Filter        index.Filter
	Order         *OrderBy
	TagProjection []TagProjection
}

// StreamQueryResult is the result of a stream query.
type StreamQueryResult interface {
	Pull() *Result
	Release()
}

// MeasureQueryOptions is the options of a measure query.
type MeasureQueryOptions struct {
	Name            string
	TimeRange       *timestamp.TimeRange
	Entity          []*modelv1.TagValue
	Filter          index.Filter
	Order           *OrderBy
	TagProjection   []TagProjection
	FieldProjection []string
}

// MeasureQueryResult is the result of a measure query.
type MeasureQueryResult interface {
	Pull() *Result
	Release()
}

var (
	// NullFieldValue represents a null field value in the model.
	NullFieldValue = &modelv1.FieldValue{Value: &modelv1.FieldValue_Null{}}

	// EmptyStrFieldValue represents an empty string field value in the model.
	EmptyStrFieldValue = &modelv1.FieldValue{Value: &modelv1.FieldValue_Str{Str: &modelv1.Str{Value: ""}}}

	// EmptyBinaryFieldValue represents an empty binary field value in the model.
	EmptyBinaryFieldValue = &modelv1.FieldValue{Value: &modelv1.FieldValue_BinaryData{BinaryData: []byte{}}}

	// NullTagFamily represents a null tag family in the model.
	NullTagFamily = &modelv1.TagFamilyForWrite{}

	// NullTagValue represents a null tag value in the model.
	NullTagValue = &modelv1.TagValue{Value: &modelv1.TagValue_Null{}}

	// EmptyStrTagValue represents an empty string tag value in the model.
	EmptyStrTagValue = &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: ""}}}

	// EmptyStrArrTagValue represents an empty string array tag value in the model.
	EmptyStrArrTagValue = &modelv1.TagValue{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: []string{}}}}

	// EmptyIntArrTagValue represents an empty integer array tag value in the model.
	EmptyIntArrTagValue = &modelv1.TagValue{Value: &modelv1.TagValue_IntArray{IntArray: &modelv1.IntArray{Value: []int64{}}}}

	// EmptyBinaryTagValue represents an empty binary tag value in the model.
	EmptyBinaryTagValue = &modelv1.TagValue{Value: &modelv1.TagValue_BinaryData{BinaryData: []byte{}}}
)
