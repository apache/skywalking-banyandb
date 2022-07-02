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

package v1

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
)

var binaryOpsMap = map[string]modelv1.Condition_BinaryOp{
	"=":          modelv1.Condition_BINARY_OP_EQ,
	"!=":         modelv1.Condition_BINARY_OP_NE,
	">":          modelv1.Condition_BINARY_OP_GT,
	">=":         modelv1.Condition_BINARY_OP_GE,
	"<":          modelv1.Condition_BINARY_OP_LT,
	"<=":         modelv1.Condition_BINARY_OP_LE,
	"having":     modelv1.Condition_BINARY_OP_HAVING,
	"not having": modelv1.Condition_BINARY_OP_NOT_HAVING,
}

type StreamQueryRequestBuilder struct {
	ec *streamv1.QueryRequest
}

func NewStreamQueryRequestBuilder() *StreamQueryRequestBuilder {
	return &StreamQueryRequestBuilder{
		ec: &streamv1.QueryRequest{
			Projection: &modelv1.TagProjection{},
		},
	}
}

func (b *StreamQueryRequestBuilder) Metadata(group, name string) *StreamQueryRequestBuilder {
	b.ec.Metadata = &commonv1.Metadata{
		Group: group,
		Name:  name,
	}
	return b
}

func (b *StreamQueryRequestBuilder) Limit(limit uint32) *StreamQueryRequestBuilder {
	b.ec.Limit = limit
	return b
}

func (b *StreamQueryRequestBuilder) Offset(offset uint32) *StreamQueryRequestBuilder {
	b.ec.Offset = offset
	return b
}

type TagTypeID string

func (b *StreamQueryRequestBuilder) TagsInTagFamily(tagFamilyName string, items ...interface{}) *StreamQueryRequestBuilder {
	if len(items)%3 != 0 {
		panic("expect 3 to be a factor of the length of items")
	}

	criteriaConditions := make([]*modelv1.Condition, len(items)/3)
	for i := 0; i < len(items)/3; i++ {
		key, op, values := items[i*3+0], items[i*3+1], items[i*3+2]
		criteriaConditions[i] = &modelv1.Condition{
			Name:  key.(string),
			Op:    binaryOpsMap[op.(string)],
			Value: buildTagValue(values),
		}
	}

	b.ec.Criteria = append(b.ec.Criteria, &modelv1.Criteria{
		TagFamilyName: tagFamilyName,
		Conditions:    criteriaConditions,
	})

	return b
}

func buildTagValue(value interface{}) *modelv1.TagValue {
	switch v := value.(type) {
	case int:
		return &modelv1.TagValue{
			Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: int64(v)}},
		}
	case []int:
		return &modelv1.TagValue{
			Value: &modelv1.TagValue_IntArray{IntArray: &modelv1.IntArray{Value: convert.IntToInt64(v...)}},
		}
	case int8:
		return &modelv1.TagValue{
			Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: int64(v)}},
		}
	case []int8:
		return &modelv1.TagValue{
			Value: &modelv1.TagValue_IntArray{IntArray: &modelv1.IntArray{Value: convert.Int8ToInt64(v...)}},
		}
	case int16:
		return &modelv1.TagValue{
			Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: int64(v)}},
		}
	case []int16:
		return &modelv1.TagValue{
			Value: &modelv1.TagValue_IntArray{IntArray: &modelv1.IntArray{Value: convert.Int16ToInt64(v...)}},
		}
	case int32:
		return &modelv1.TagValue{
			Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: int64(v)}},
		}
	case []int32:
		return &modelv1.TagValue{
			Value: &modelv1.TagValue_IntArray{IntArray: &modelv1.IntArray{Value: convert.Int32ToInt64(v...)}},
		}
	case int64:
		return &modelv1.TagValue{
			Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: v}},
		}
	case []int64:
		return &modelv1.TagValue{
			Value: &modelv1.TagValue_IntArray{IntArray: &modelv1.IntArray{Value: v}},
		}
	case string:
		return &modelv1.TagValue{
			Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: v}},
		}
	case []string:
		return &modelv1.TagValue{
			Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: v}},
		}
	case TagTypeID:
		return &modelv1.TagValue{
			Value: &modelv1.TagValue_Id{Id: &modelv1.ID{Value: string(v)}},
		}
	}
	panic("not supported")
}

func (b *StreamQueryRequestBuilder) Projection(tagFamily string, projections ...string) *StreamQueryRequestBuilder {
	b.ec.Projection.TagFamilies = append(b.ec.Projection.GetTagFamilies(), &modelv1.TagProjection_TagFamily{
		Name: tagFamily,
		Tags: projections,
	})
	return b
}

func (b *StreamQueryRequestBuilder) OrderBy(indexRuleName string, sort modelv1.Sort) *StreamQueryRequestBuilder {
	b.ec.OrderBy = &modelv1.QueryOrder{
		IndexRuleName: indexRuleName,
		Sort:          sort,
	}
	return b
}

func (b *StreamQueryRequestBuilder) TimeRange(sT, eT time.Time) *StreamQueryRequestBuilder {
	b.ec.TimeRange = &modelv1.TimeRange{
		Begin: timestamppb.New(sT),
		End:   timestamppb.New(eT),
	}
	return b
}

func (b *StreamQueryRequestBuilder) Build() *streamv1.QueryRequest {
	return b.ec
}

type QueryResponseElementBuilder struct {
	elem *streamv1.Element
}

func NewQueryEntityBuilder() *QueryResponseElementBuilder {
	return &QueryResponseElementBuilder{elem: &streamv1.Element{}}
}

func (qeb *QueryResponseElementBuilder) EntityID(elementID string) *QueryResponseElementBuilder {
	qeb.elem.ElementId = elementID
	return qeb
}

func (qeb *QueryResponseElementBuilder) Timestamp(t time.Time) *QueryResponseElementBuilder {
	qeb.elem.Timestamp = timestamppb.New(t)
	return qeb
}

func (qeb *QueryResponseElementBuilder) FieldsInTagFamily(tagFamily string, items ...interface{}) *QueryResponseElementBuilder {
	if len(items)%2 != 0 {
		panic("invalid fields list")
	}

	l := len(items) / 2
	tags := make([]*modelv1.Tag, l)
	for i := 0; i < l; i++ {
		key, values := items[i*2+0], items[i*2+1]
		tags[i] = &modelv1.Tag{
			Key:   key.(string),
			Value: buildTagValue(values),
		}
	}

	qeb.elem.TagFamilies = append(qeb.elem.GetTagFamilies(), &modelv1.TagFamily{
		Name: tagFamily,
		Tags: tags,
	})

	return qeb
}

func (qeb *QueryResponseElementBuilder) Build() *streamv1.Element {
	return qeb.elem
}

type MeasureQueryRequestBuilder struct {
	ec *measurev1.QueryRequest
}

func NewMeasureQueryRequestBuilder() *MeasureQueryRequestBuilder {
	return &MeasureQueryRequestBuilder{
		ec: &measurev1.QueryRequest{
			TagProjection:   &modelv1.TagProjection{},
			FieldProjection: &measurev1.QueryRequest_FieldProjection{},
		},
	}
}

func (b *MeasureQueryRequestBuilder) Metadata(group, name string) *MeasureQueryRequestBuilder {
	b.ec.Metadata = &commonv1.Metadata{
		Group: group,
		Name:  name,
	}
	return b
}

func (b *MeasureQueryRequestBuilder) TagsInTagFamily(tagFamilyName string, items ...interface{}) *MeasureQueryRequestBuilder {
	if len(items)%3 != 0 {
		panic("expect 3 to be a factor of the length of items")
	}

	criteriaConditions := make([]*modelv1.Condition, len(items)/3)
	for i := 0; i < len(items)/3; i++ {
		key, op, values := items[i*3+0], items[i*3+1], items[i*3+2]
		criteriaConditions[i] = &modelv1.Condition{
			Name:  key.(string),
			Op:    binaryOpsMap[op.(string)],
			Value: buildTagValue(values),
		}
	}

	b.ec.Criteria = append(b.ec.Criteria, &modelv1.Criteria{
		TagFamilyName: tagFamilyName,
		Conditions:    criteriaConditions,
	})

	return b
}

func (b *MeasureQueryRequestBuilder) TagProjection(tagFamily string, projections ...string) *MeasureQueryRequestBuilder {
	b.ec.TagProjection.TagFamilies = append(b.ec.TagProjection.GetTagFamilies(), &modelv1.TagProjection_TagFamily{
		Name: tagFamily,
		Tags: projections,
	})
	return b
}

func (b *MeasureQueryRequestBuilder) FieldProjection(projections ...string) *MeasureQueryRequestBuilder {
	b.ec.FieldProjection.Names = append(b.ec.FieldProjection.Names, projections...)
	return b
}

func (b *MeasureQueryRequestBuilder) GroupBy(tagFamily string, projections ...string) *GroupByBuilder {
	b.ec.GroupBy = &measurev1.QueryRequest_GroupBy{
		TagProjection: &modelv1.TagProjection{
			TagFamilies: []*modelv1.TagProjection_TagFamily{
				{
					Name: tagFamily,
					Tags: projections,
				},
			},
		},
	}
	return &GroupByBuilder{
		builder: b,
	}
}

func (b *MeasureQueryRequestBuilder) TimeRange(sT, eT time.Time) *MeasureQueryRequestBuilder {
	b.ec.TimeRange = &modelv1.TimeRange{
		Begin: timestamppb.New(sT),
		End:   timestamppb.New(eT),
	}
	return b
}

func (b *MeasureQueryRequestBuilder) Top(n int32, fieldName string, sort modelv1.Sort) *MeasureQueryRequestBuilder {
	b.ec.Top = &measurev1.QueryRequest_Top{
		Number:         n,
		FieldName:      fieldName,
		FieldValueSort: sort,
	}
	return b
}

func (b *MeasureQueryRequestBuilder) OrderBy(indexRuleName string, sort modelv1.Sort) *MeasureQueryRequestBuilder {
	b.ec.OrderBy = &modelv1.QueryOrder{
		IndexRuleName: indexRuleName,
		Sort:          sort,
	}
	return b
}

func (b *MeasureQueryRequestBuilder) Limit(offset, limit uint32) *MeasureQueryRequestBuilder {
	b.ec.Offset = offset
	b.ec.Limit = limit
	return b
}

func (b *MeasureQueryRequestBuilder) Build() *measurev1.QueryRequest {
	return b.ec
}

type GroupByBuilder struct {
	builder *MeasureQueryRequestBuilder
}

func (b *GroupByBuilder) Max(fieldName string) *MeasureQueryRequestBuilder {
	return b.aggregate(fieldName, modelv1.AggregationFunction_AGGREGATION_FUNCTION_MAX)
}

func (b *GroupByBuilder) Min(fieldName string) *MeasureQueryRequestBuilder {
	return b.aggregate(fieldName, modelv1.AggregationFunction_AGGREGATION_FUNCTION_MIN)
}

func (b *GroupByBuilder) Mean(fieldName string) *MeasureQueryRequestBuilder {
	return b.aggregate(fieldName, modelv1.AggregationFunction_AGGREGATION_FUNCTION_MEAN)
}

func (b *GroupByBuilder) Count(fieldName string) *MeasureQueryRequestBuilder {
	return b.aggregate(fieldName, modelv1.AggregationFunction_AGGREGATION_FUNCTION_COUNT)
}

func (b *GroupByBuilder) Sum(fieldName string) *MeasureQueryRequestBuilder {
	return b.aggregate(fieldName, modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM)
}

func (b *GroupByBuilder) aggregate(fieldName string, aggregationFunc modelv1.AggregationFunction) *MeasureQueryRequestBuilder {
	b.builder.ec.GroupBy.FieldName = fieldName
	b.builder.ec.Agg = &measurev1.QueryRequest_Aggregation{
		FieldName: fieldName,
		Function:  aggregationFunc,
	}
	return b.builder
}

func Eq(key string, val interface{}) *modelv1.Condition {
	return &modelv1.Condition{
		Name:  key,
		Op:    modelv1.Condition_BINARY_OP_EQ,
		Value: buildTagValue(val),
	}
}

func Ne(key string, val interface{}) *modelv1.Condition {
	return &modelv1.Condition{
		Name:  key,
		Op:    modelv1.Condition_BINARY_OP_NE,
		Value: buildTagValue(val),
	}
}

func Le(key string, val interface{}) *modelv1.Condition {
	return &modelv1.Condition{
		Name:  key,
		Op:    modelv1.Condition_BINARY_OP_LE,
		Value: buildTagValue(val),
	}
}

func Lt(key string, val interface{}) *modelv1.Condition {
	return &modelv1.Condition{
		Name:  key,
		Op:    modelv1.Condition_BINARY_OP_LT,
		Value: buildTagValue(val),
	}
}

func Ge(key string, val interface{}) *modelv1.Condition {
	return &modelv1.Condition{
		Name:  key,
		Op:    modelv1.Condition_BINARY_OP_GE,
		Value: buildTagValue(val),
	}
}

func Gt(key string, val interface{}) *modelv1.Condition {
	return &modelv1.Condition{
		Name:  key,
		Op:    modelv1.Condition_BINARY_OP_GT,
		Value: buildTagValue(val),
	}
}

func And(tagFamilyName string, conditions ...*modelv1.Condition) *modelv1.Criteria {
	return &modelv1.Criteria{
		TagFamilyName: tagFamilyName,
		Conditions:    conditions,
	}
}
