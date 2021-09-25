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
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
)

type WriteRequestBuilder struct {
	we *tracev1.WriteRequest
}

func NewWriteEntityBuilder() *WriteRequestBuilder {
	return &WriteRequestBuilder{we: &tracev1.WriteRequest{}}
}

func (web *WriteRequestBuilder) Metadata(group, name string) *WriteRequestBuilder {
	web.we.Metadata = &commonv1.Metadata{
		Group: group,
		Name:  name,
	}
	return web
}

func (web *WriteRequestBuilder) EntityValue(ev *tracev1.EntityValue) *WriteRequestBuilder {
	web.we.Entity = ev
	return web
}

func (web *WriteRequestBuilder) Build() *tracev1.WriteRequest {
	return web.we
}

type EntityValueBuilder struct {
	ev *tracev1.EntityValue
}

func NewEntityValueBuilder() *EntityValueBuilder {
	return &EntityValueBuilder{ev: &tracev1.EntityValue{}}
}

func (evb *EntityValueBuilder) EntityID(entityID string) *EntityValueBuilder {
	evb.ev.EntityId = entityID
	return evb
}

func (evb *EntityValueBuilder) Timestamp(time time.Time) *EntityValueBuilder {
	evb.ev.Timestamp = timestamppb.New(time)
	return evb
}

func (evb *EntityValueBuilder) DataBinary(data []byte) *EntityValueBuilder {
	evb.ev.DataBinary = data
	return evb
}

func (evb *EntityValueBuilder) Fields(items ...interface{}) *EntityValueBuilder {
	evb.ev.Fields = make([]*modelv1.Field, len(items))
	for idx, item := range items {
		evb.ev.Fields[idx] = buildField(item)
	}
	return evb
}

func buildField(value interface{}) *modelv1.Field {
	if value == nil {
		return &modelv1.Field{ValueType: &modelv1.Field_Null{}}
	}
	switch v := value.(type) {
	case string:
		return &modelv1.Field{
			ValueType: &modelv1.Field_Str{
				Str: &modelv1.Str{
					Value: v,
				},
			},
		}
	case []string:
		return &modelv1.Field{
			ValueType: &modelv1.Field_StrArray{
				StrArray: &modelv1.StrArray{
					Value: v,
				},
			},
		}
	case int:
		return &modelv1.Field{
			ValueType: &modelv1.Field_Int{
				Int: &modelv1.Int{
					Value: int64(v),
				},
			},
		}
	case []int:
		return &modelv1.Field{
			ValueType: &modelv1.Field_IntArray{
				IntArray: &modelv1.IntArray{
					Value: convert.IntToInt64(v...),
				},
			},
		}
	case int64:
		return &modelv1.Field{
			ValueType: &modelv1.Field_Int{
				Int: &modelv1.Int{
					Value: v,
				},
			},
		}
	case []int64:
		return &modelv1.Field{
			ValueType: &modelv1.Field_IntArray{
				IntArray: &modelv1.IntArray{
					Value: v,
				},
			},
		}
	default:
		panic("type not supported")
	}
}

func (evb *EntityValueBuilder) Build() *tracev1.EntityValue {
	return evb.ev
}
