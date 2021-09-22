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

package v2

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	commonv2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v2"
	modelv2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v2"
	streamv2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v2"
	"github.com/apache/skywalking-banyandb/pkg/convert"
)

var (
	binaryOpsMap = map[string]modelv2.Condition_BinaryOp{
		"=":          modelv2.Condition_BINARY_OP_EQ,
		"!=":         modelv2.Condition_BINARY_OP_NE,
		">":          modelv2.Condition_BINARY_OP_GT,
		">=":         modelv2.Condition_BINARY_OP_GE,
		"<":          modelv2.Condition_BINARY_OP_LT,
		"<=":         modelv2.Condition_BINARY_OP_LE,
		"having":     modelv2.Condition_BINARY_OP_HAVING,
		"not having": modelv2.Condition_BINARY_OP_NOT_HAVING,
	}
)

type QueryRequestBuilder struct {
	ec *streamv2.QueryRequest
}

func NewQueryRequestBuilder() *QueryRequestBuilder {
	return &QueryRequestBuilder{
		ec: &streamv2.QueryRequest{
			Projection: &modelv2.Projection{},
		},
	}
}

func (b *QueryRequestBuilder) Metadata(group, name string) *QueryRequestBuilder {
	b.ec.Metadata = &commonv2.Metadata{
		Group: group,
		Name:  name,
	}
	return b
}

func (b *QueryRequestBuilder) Limit(limit uint32) *QueryRequestBuilder {
	b.ec.Limit = limit
	return b
}

func (b *QueryRequestBuilder) Offset(offset uint32) *QueryRequestBuilder {
	b.ec.Offset = offset
	return b
}

func (b *QueryRequestBuilder) FieldsInTagFamily(tagFamilyName string, items ...interface{}) *QueryRequestBuilder {
	if len(items)%3 != 0 {
		panic("expect 3 to be a factor of the length of items")
	}

	criteriaConditions := make([]*modelv2.Condition, len(items)/3)
	for i := 0; i < len(items)/3; i++ {
		key, op, values := items[i*3+0], items[i*3+1], items[i*3+2]
		criteriaConditions[i] = &modelv2.Condition{
			Name:  key.(string),
			Op:    binaryOpsMap[op.(string)],
			Value: buildTagValue(values),
		}
	}

	b.ec.Criteria = append(b.ec.Criteria, &streamv2.QueryRequest_Criteria{
		TagFamilyName: tagFamilyName,
		Conditions:    criteriaConditions,
	})

	return b
}

func buildTagValue(value interface{}) *modelv2.TagValue {
	switch v := value.(type) {
	case int:
		return &modelv2.TagValue{
			Value: &modelv2.TagValue_Int{Int: &modelv2.Int{Value: int64(v)}},
		}
	case []int:
		return &modelv2.TagValue{
			Value: &modelv2.TagValue_IntArray{IntArray: &modelv2.IntArray{Value: convert.IntToInt64(v...)}},
		}
	case int8:
		return &modelv2.TagValue{
			Value: &modelv2.TagValue_Int{Int: &modelv2.Int{Value: int64(v)}},
		}
	case []int8:
		return &modelv2.TagValue{
			Value: &modelv2.TagValue_IntArray{IntArray: &modelv2.IntArray{Value: convert.Int8ToInt64(v...)}},
		}
	case int16:
		return &modelv2.TagValue{
			Value: &modelv2.TagValue_Int{Int: &modelv2.Int{Value: int64(v)}},
		}
	case []int16:
		return &modelv2.TagValue{
			Value: &modelv2.TagValue_IntArray{IntArray: &modelv2.IntArray{Value: convert.Int16ToInt64(v...)}},
		}
	case int32:
		return &modelv2.TagValue{
			Value: &modelv2.TagValue_Int{Int: &modelv2.Int{Value: int64(v)}},
		}
	case []int32:
		return &modelv2.TagValue{
			Value: &modelv2.TagValue_IntArray{IntArray: &modelv2.IntArray{Value: convert.Int32ToInt64(v...)}},
		}
	case int64:
		return &modelv2.TagValue{
			Value: &modelv2.TagValue_Int{Int: &modelv2.Int{Value: v}},
		}
	case []int64:
		return &modelv2.TagValue{
			Value: &modelv2.TagValue_IntArray{IntArray: &modelv2.IntArray{Value: v}},
		}
	case string:
		return &modelv2.TagValue{
			Value: &modelv2.TagValue_Str{Str: &modelv2.Str{Value: v}},
		}
	case []string:
		return &modelv2.TagValue{
			Value: &modelv2.TagValue_StrArray{StrArray: &modelv2.StrArray{Value: v}},
		}
	}
	panic("not supported")
}

func (b *QueryRequestBuilder) Projection(tagFamily string, projections ...string) *QueryRequestBuilder {
	b.ec.Projection.TagFamilies = append(b.ec.Projection.GetTagFamilies(), &modelv2.Projection_TagFamily{
		Name: tagFamily,
		Tags: projections,
	})
	return b
}

func (b *QueryRequestBuilder) OrderBy(indexRuleName string, sort modelv2.QueryOrder_Sort) *QueryRequestBuilder {
	b.ec.OrderBy = &modelv2.QueryOrder{
		IndexRuleName: indexRuleName,
		Sort:          sort,
	}
	return b
}

func (b *QueryRequestBuilder) TimeRange(sT, eT time.Time) *QueryRequestBuilder {
	b.ec.TimeRange = &modelv2.TimeRange{
		Begin: timestamppb.New(sT),
		End:   timestamppb.New(eT),
	}
	return b
}

func (b *QueryRequestBuilder) Build() *streamv2.QueryRequest {
	return b.ec
}

type QueryResponseElementBuilder struct {
	elem *streamv2.Element
}

func NewQueryEntityBuilder() *QueryResponseElementBuilder {
	return &QueryResponseElementBuilder{elem: &streamv2.Element{}}
}

func (qeb *QueryResponseElementBuilder) EntityID(elementId string) *QueryResponseElementBuilder {
	qeb.elem.ElementId = elementId
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
	tags := make([]*modelv2.Tag, l)
	for i := 0; i < l; i++ {
		key, values := items[i*2+0], items[i*2+1]
		tags[i] = &modelv2.Tag{
			Key:   key.(string),
			Value: buildTagValue(values),
		}
	}

	qeb.elem.TagFamilies = append(qeb.elem.GetTagFamilies(), &modelv2.TagFamily{
		Name: tagFamily,
		Tags: tags,
	})

	return qeb
}

func (qeb *QueryResponseElementBuilder) Build() *streamv2.Element {
	return qeb.elem
}
