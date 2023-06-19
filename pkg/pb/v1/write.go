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
	"encoding/hex"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
)

const fieldFlagLength = 9

var zeroFieldValue = &modelv1.FieldValue{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 0}}}

var (
	// TagFlag is a flag suffix to identify the encoding method.
	TagFlag = make([]byte, fieldFlagLength)

	strDelimiter = []byte("\n")
	nullTag      = &modelv1.TagValue{Value: &modelv1.TagValue_Null{}}
	nullTagValue = TagValue{}

	errUnsupportedTagForIndexField = errors.New("the tag type(for example, null) can not be as the index field value")
	errMalformedElement            = errors.New("element is malformed")
	errMalformedField              = errors.New("field is malformed")
)

// MarshalTagValue encodes modelv1.TagValue to bytes.
func MarshalTagValue(tagValue *modelv1.TagValue) ([]byte, error) {
	fv, err := ParseTagValue(tagValue)
	if err != nil {
		return nil, err
	}
	val := fv.GetValue()
	if val != nil {
		return val, nil
	}
	return fv.marshalArr(), nil
}

// TagValue seels single value and array value.
type TagValue struct {
	value    []byte
	arr      [][]byte
	splitter []byte
}

// GetValue returns the single value.
func (fv TagValue) GetValue() []byte {
	if len(fv.value) < 1 {
		return nil
	}
	return fv.value
}

// GetArr returns the array value.
func (fv TagValue) GetArr() [][]byte {
	if len(fv.arr) < 1 {
		return nil
	}
	return fv.arr
}

func newValue(value []byte) TagValue {
	return TagValue{
		value: value,
	}
}

func newValueWithSplitter(splitter []byte) *TagValue {
	return &TagValue{
		splitter: splitter,
	}
}

func appendValue(fv *TagValue, value []byte) *TagValue {
	if fv == nil {
		fv = &TagValue{}
	}
	fv.arr = append(fv.arr, value)
	return fv
}

func (fv *TagValue) marshalArr() []byte {
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

// ParseTagValue decodes modelv1.TagValue to TagValue.
func ParseTagValue(tagValue *modelv1.TagValue) (TagValue, error) {
	switch x := tagValue.GetValue().(type) {
	case *modelv1.TagValue_Null:
		return nullTagValue, nil
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
		var fv *TagValue
		for _, i := range x.IntArray.GetValue() {
			fv = appendValue(fv, convert.Int64ToBytes(i))
		}
		return *fv, nil
	case *modelv1.TagValue_BinaryData:
		return newValue(bytes.Clone(x.BinaryData)), nil
	}
	return TagValue{}, errUnsupportedTagForIndexField
}

// EncodeFamily encodes a tag family to bytes by referring to its specification.
func EncodeFamily(familySpec *databasev1.TagFamilySpec, family *modelv1.TagFamilyForWrite) ([]byte, error) {
	if len(family.GetTags()) > len(familySpec.GetTags()) {
		return nil, errors.Wrap(errMalformedElement, "tag number is more than expected")
	}
	data := &modelv1.TagFamilyForWrite{}
	for ti, tag := range family.GetTags() {
		tagSpec := familySpec.GetTags()[ti]
		tType, isNull := tagValueTypeConv(tag)
		if !isNull && tType != tagSpec.GetType() {
			return nil, errors.Wrapf(errMalformedElement, "tag %s type is unexpected", tagSpec.GetName())
		}
		if tagSpec.IndexedOnly {
			data.Tags = append(data.Tags, nullTag)
		} else {
			data.Tags = append(data.Tags, tag)
		}
	}
	return proto.Marshal(data)
}

// DecodeFieldValue decodes bytes to field value based on its specification.
func DecodeFieldValue(fieldValue []byte, fieldSpec *databasev1.FieldSpec) (*modelv1.FieldValue, error) {
	switch fieldSpec.GetFieldType() {
	case databasev1.FieldType_FIELD_TYPE_STRING:
		return &modelv1.FieldValue{Value: &modelv1.FieldValue_Str{Str: &modelv1.Str{Value: string(fieldValue)}}}, nil
	case databasev1.FieldType_FIELD_TYPE_INT:
		if len(fieldValue) == 0 {
			return zeroFieldValue, nil
		}
		if len(fieldValue) != 8 {
			return nil, errors.WithMessagef(errMalformedField, "the length of encoded field value(int64) %s is %d, less than 8",
				hex.EncodeToString(fieldValue), len(fieldValue))
		}
		return &modelv1.FieldValue{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: convert.BytesToInt64(fieldValue)}}}, nil
	case databasev1.FieldType_FIELD_TYPE_FLOAT:
		if len(fieldValue) == 0 {
			return zeroFieldValue, nil
		}
		if len(fieldValue) != 8 {
			return nil, errors.WithMessagef(errMalformedField, "the length of encoded field value(float64) %s is %d, less than 8",
				hex.EncodeToString(fieldValue), len(fieldValue))
		}
		return &modelv1.FieldValue{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: convert.BytesToFloat64(fieldValue)}}}, nil
	case databasev1.FieldType_FIELD_TYPE_DATA_BINARY:
		return &modelv1.FieldValue{Value: &modelv1.FieldValue_BinaryData{BinaryData: fieldValue}}, nil
	}
	return &modelv1.FieldValue{Value: &modelv1.FieldValue_Null{}}, nil
}

// EncoderFieldFlag encodes the encoding method, compression method, and interval into bytes.
func EncoderFieldFlag(fieldSpec *databasev1.FieldSpec, interval time.Duration) []byte {
	encodingMethod := byte(fieldSpec.GetEncodingMethod().Number())
	compressionMethod := byte(fieldSpec.GetCompressionMethod().Number())
	bb := make([]byte, fieldFlagLength)
	bb[0] = encodingMethod<<4 | compressionMethod
	copy(bb[1:], convert.Int64ToBytes(int64(interval)))
	return bb
}

// DecodeFieldFlag decodes the encoding method, compression method, and interval from bytes.
func DecodeFieldFlag(key []byte) (*databasev1.FieldSpec, time.Duration, error) {
	if len(key) < fieldFlagLength {
		return nil, 0, errors.WithMessagef(errMalformedField, "flag %s is invalid", hex.EncodeToString(key))
	}
	b := key[len(key)-9:]
	return &databasev1.FieldSpec{
		EncodingMethod:    databasev1.EncodingMethod(int32(b[0]) >> 4),
		CompressionMethod: databasev1.CompressionMethod(int32(b[0] & 0x0F)),
	}, time.Duration(convert.BytesToInt64(b[1:])), nil
}
