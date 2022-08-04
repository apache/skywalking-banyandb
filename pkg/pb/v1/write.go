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
	"bytes"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
)

type ID string

var strDelimiter = []byte("\n")

var (
	ErrUnsupportedTagForIndexField = errors.New("the tag type(for example, null) can not be as the index field value")
	ErrNullValue                   = errors.New("the tag value is null")
)

func MarshalIndexFieldValue(tagValue *modelv1.TagValue) ([]byte, error) {
	fv, err := ParseIndexFieldValue(tagValue)
	if err != nil {
		return nil, err
	}
	val := fv.GetValue()
	if val != nil {
		return val, nil
	}
	return fv.marshalArr(), nil
}

type FieldValue struct {
	value    []byte
	arr      [][]byte
	splitter []byte
}

func newValue(value []byte) FieldValue {
	return FieldValue{
		value: value,
	}
}

func newValueWithSplitter(splitter []byte) *FieldValue {
	return &FieldValue{
		splitter: splitter,
	}
}

func appendValue(fv *FieldValue, value []byte) *FieldValue {
	if fv == nil {
		fv = &FieldValue{}
	}
	fv.arr = append(fv.arr, value)
	return fv
}

func (fv FieldValue) GetValue() []byte {
	if len(fv.value) < 1 {
		return nil
	}
	return fv.value
}

func (fv FieldValue) GetArr() [][]byte {
	if len(fv.arr) < 1 {
		return nil
	}
	return fv.arr
}

func (fv *FieldValue) marshalArr() []byte {
	switch len(fv.arr) {
	case 0:
		return nil
	case 1:
		return fv.arr[0]
	}
	n := len(fv.splitter) * (len(fv.arr) - 1)
	for i := 0; i < len(fv.arr); i++ {
		n += len(fv.arr[i])
	}
	buf := bytes.NewBuffer(nil)
	buf.Grow(n)
	buf.Write(fv.arr[0])
	for _, v := range fv.arr[1:] {
		if fv.splitter != nil {
			buf.Write(fv.splitter)
		}
		buf.Write(v)
	}
	return buf.Bytes()
}

func ParseIndexFieldValue(tagValue *modelv1.TagValue) (FieldValue, error) {
	switch x := tagValue.GetValue().(type) {
	case *modelv1.TagValue_Null:
		return FieldValue{}, ErrNullValue
	case *modelv1.TagValue_Str:
		return newValue([]byte(x.Str.GetValue())), nil
	case *modelv1.TagValue_Int:
		return newValue(convert.Int64ToBytes(x.Int.GetValue())), nil
	case *modelv1.TagValue_StrArray:
		fv := newValueWithSplitter(strDelimiter)
		for _, v := range x.StrArray.GetValue() {
			fv = appendValue(fv, []byte(v))
		}
		return *fv, nil
	case *modelv1.TagValue_IntArray:
		var fv *FieldValue
		for _, i := range x.IntArray.GetValue() {
			fv = appendValue(fv, convert.Int64ToBytes(i))
		}
		return *fv, nil
	case *modelv1.TagValue_BinaryData:
		return newValue(x.BinaryData), nil
	case *modelv1.TagValue_Id:
		return newValue([]byte(x.Id.GetValue())), nil
	}
	return FieldValue{}, ErrUnsupportedTagForIndexField
}

type StreamWriteRequestBuilder struct {
	ec *streamv1.WriteRequest
}

func NewStreamWriteRequestBuilder() *StreamWriteRequestBuilder {
	return &StreamWriteRequestBuilder{
		ec: &streamv1.WriteRequest{
			Element: &streamv1.ElementValue{
				TagFamilies: make([]*modelv1.TagFamilyForWrite, 0),
			},
		},
	}
}

func (b *StreamWriteRequestBuilder) Metadata(group, name string) *StreamWriteRequestBuilder {
	b.ec.Metadata = &commonv1.Metadata{
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
	tagFamily := &modelv1.TagFamilyForWrite{}
	for _, tag := range tags {
		tagFamily.Tags = append(tagFamily.Tags, getTag(tag))
	}
	b.ec.Element.TagFamilies = append(b.ec.Element.TagFamilies, tagFamily)
	return b
}

func (b *StreamWriteRequestBuilder) Build() *streamv1.WriteRequest {
	return b.ec
}

func getTag(tag interface{}) *modelv1.TagValue {
	if tag == nil {
		return &modelv1.TagValue{
			Value: &modelv1.TagValue_Null{},
		}
	}
	switch t := tag.(type) {
	case int:
		return &modelv1.TagValue{
			Value: &modelv1.TagValue_Int{
				Int: &modelv1.Int{
					Value: int64(t),
				},
			},
		}
	case string:
		return &modelv1.TagValue{
			Value: &modelv1.TagValue_Str{
				Str: &modelv1.Str{
					Value: t,
				},
			},
		}
	case []byte:
		return &modelv1.TagValue{
			Value: &modelv1.TagValue_BinaryData{
				BinaryData: t,
			},
		}
	case ID:
		return &modelv1.TagValue{
			Value: &modelv1.TagValue_Id{
				Id: &modelv1.ID{
					Value: string(t),
				},
			},
		}
	}
	return nil
}

type MeasureWriteRequestBuilder struct {
	ec *measurev1.WriteRequest
}

func NewMeasureWriteRequestBuilder() *MeasureWriteRequestBuilder {
	return &MeasureWriteRequestBuilder{
		ec: &measurev1.WriteRequest{
			DataPoint: &measurev1.DataPointValue{
				TagFamilies: make([]*modelv1.TagFamilyForWrite, 0),
				Fields:      make([]*modelv1.FieldValue, 0),
			},
		},
	}
}

func (b *MeasureWriteRequestBuilder) Metadata(group, name string) *MeasureWriteRequestBuilder {
	b.ec.Metadata = &commonv1.Metadata{
		Group: group,
		Name:  name,
	}
	return b
}

func (b *MeasureWriteRequestBuilder) TagFamily(tags ...interface{}) *MeasureWriteRequestBuilder {
	tagFamily := &modelv1.TagFamilyForWrite{}
	for _, tag := range tags {
		tagFamily.Tags = append(tagFamily.Tags, getTag(tag))
	}
	b.ec.DataPoint.TagFamilies = append(b.ec.DataPoint.TagFamilies, tagFamily)
	return b
}

func (b *MeasureWriteRequestBuilder) Fields(fields ...interface{}) *MeasureWriteRequestBuilder {
	fieldValues := make([]*modelv1.FieldValue, 0)
	for _, field := range fields {
		fieldValues = append(fieldValues, getField(field))
	}
	b.ec.DataPoint.Fields = append(b.ec.DataPoint.Fields, fieldValues...)
	return b
}

func (b *MeasureWriteRequestBuilder) Timestamp(t time.Time) *MeasureWriteRequestBuilder {
	b.ec.DataPoint.Timestamp = timestamppb.New(t)
	return b
}

func (b *MeasureWriteRequestBuilder) Build() *measurev1.WriteRequest {
	return b.ec
}

func getField(field interface{}) *modelv1.FieldValue {
	if field == nil {
		return &modelv1.FieldValue{
			Value: &modelv1.FieldValue_Null{},
		}
	}
	switch t := field.(type) {
	case int8:
		return &modelv1.FieldValue{
			Value: &modelv1.FieldValue_Int{
				Int: &modelv1.Int{
					Value: int64(t),
				},
			},
		}
	case int32:
		return &modelv1.FieldValue{
			Value: &modelv1.FieldValue_Int{
				Int: &modelv1.Int{
					Value: int64(t),
				},
			},
		}
	case int64:
		return &modelv1.FieldValue{
			Value: &modelv1.FieldValue_Int{
				Int: &modelv1.Int{
					Value: t,
				},
			},
		}
	case int:
		return &modelv1.FieldValue{
			Value: &modelv1.FieldValue_Int{
				Int: &modelv1.Int{
					Value: int64(t),
				},
			},
		}
	case string:
		return &modelv1.FieldValue{
			Value: &modelv1.FieldValue_Str{
				Str: &modelv1.Str{
					Value: t,
				},
			},
		}
	case []byte:
		return &modelv1.FieldValue{
			Value: &modelv1.FieldValue_BinaryData{
				BinaryData: t,
			},
		}
	}
	return nil
}
