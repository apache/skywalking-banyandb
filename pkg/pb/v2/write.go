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
	"bytes"
	"strings"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v2"
	modelv2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v2"
	streamv2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v2"
	"github.com/apache/skywalking-banyandb/pkg/convert"
)

const strDelimiter = "\n"

var ErrUnsupportedTagForIndexField = errors.New("the tag type(for example, null) can not be as the index field value")

func MarshalIndexFieldValue(tagValue *modelv2.TagValue) ([]byte, error) {
	switch x := tagValue.GetValue().(type) {
	case *modelv2.TagValue_Str:
		return []byte(x.Str.GetValue()), nil
	case *modelv2.TagValue_Int:
		return convert.Int64ToBytes(x.Int.GetValue()), nil
	case *modelv2.TagValue_StrArray:
		return []byte(strings.Join(x.StrArray.GetValue(), strDelimiter)), nil
	case *modelv2.TagValue_IntArray:
		buf := bytes.NewBuffer(nil)
		for _, i := range x.IntArray.GetValue() {
			buf.Write(convert.Int64ToBytes(i))
		}
		return buf.Bytes(), nil
	case *modelv2.TagValue_BinaryData:
		return x.BinaryData, nil
	}
	return nil, ErrUnsupportedTagForIndexField
}

type StreamWriteRequestBuilder struct {
	ec *streamv2.WriteRequest
}

func NewStreamWriteRequestBuilder() *StreamWriteRequestBuilder {
	return &StreamWriteRequestBuilder{
		ec: &streamv2.WriteRequest{
			Element: &streamv2.ElementValue{
				TagFamilies: make([]*modelv2.TagFamilyForWrite, 0),
			},
		},
	}
}

func (b *StreamWriteRequestBuilder) Metadata(group, name string) *StreamWriteRequestBuilder {
	b.ec.Metadata = &commonv2.Metadata{
		Group: group,
		Name:  name,
	}
	return b
}

func (b *StreamWriteRequestBuilder) ID(id string) *StreamWriteRequestBuilder {
	b.ec.Element.ElementId = id
	return b
}

func (b *StreamWriteRequestBuilder) Timestamp(t time.Time) *StreamWriteRequestBuilder {
	b.ec.Element.Timestamp = timestamppb.New(t)
	return b
}

func (b *StreamWriteRequestBuilder) TagFamily(tags ...interface{}) *StreamWriteRequestBuilder {
	tagFamily := &modelv2.TagFamilyForWrite{}
	for _, tag := range tags {
		tagFamily.Tags = append(tagFamily.Tags, getTag(tag))
	}
	b.ec.Element.TagFamilies = append(b.ec.Element.TagFamilies, tagFamily)
	return b
}

func (b *StreamWriteRequestBuilder) Build() *streamv2.WriteRequest {
	return b.ec
}

func getTag(tag interface{}) *modelv2.TagValue {
	if tag == nil {
		return &modelv2.TagValue{
			Value: &modelv2.TagValue_Null{},
		}
	}
	switch t := tag.(type) {
	case int:
		return &modelv2.TagValue{
			Value: &modelv2.TagValue_Int{
				Int: &modelv2.Int{
					Value: int64(t),
				},
			},
		}
	case string:
		return &modelv2.TagValue{
			Value: &modelv2.TagValue_Str{
				Str: &modelv2.Str{
					Value: t,
				},
			},
		}
	case []byte:
		return &modelv2.TagValue{
			Value: &modelv2.TagValue_BinaryData{
				BinaryData: t,
			},
		}
	}
	return nil
}
