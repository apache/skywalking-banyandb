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
	"fmt"
	"strconv"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/types/known/timestamppb"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// ValueType is the type of the tag and field value.
type ValueType byte

// ValueType constants.
const (
	ValueTypeUnknown ValueType = iota
	ValueTypeStr
	ValueTypeInt64
	ValueTypeFloat64
	ValueTypeBinaryData
	ValueTypeStrArr
	ValueTypeInt64Arr
	ValueTypeTimestamp
)

// MustTagValueToValueType converts modelv1.TagValue to ValueType.
func MustTagValueToValueType(tag *modelv1.TagValue) ValueType {
	switch tag.Value.(type) {
	case *modelv1.TagValue_Null:
		return ValueTypeUnknown
	case *modelv1.TagValue_Str:
		return ValueTypeStr
	case *modelv1.TagValue_Int:
		return ValueTypeInt64
	case *modelv1.TagValue_BinaryData:
		return ValueTypeBinaryData
	case *modelv1.TagValue_StrArray:
		return ValueTypeStrArr
	case *modelv1.TagValue_IntArray:
		return ValueTypeInt64Arr
	case *modelv1.TagValue_Timestamp:
		return ValueTypeTimestamp
	default:
		panic("unknown tag value type")
	}
}

// MustTagValueSpecToValueType converts databasev1.TagType to ValueType.
func MustTagValueSpecToValueType(tag databasev1.TagType) ValueType {
	switch tag {
	case databasev1.TagType_TAG_TYPE_STRING:
		return ValueTypeStr
	case databasev1.TagType_TAG_TYPE_INT:
		return ValueTypeInt64
	case databasev1.TagType_TAG_TYPE_DATA_BINARY:
		return ValueTypeBinaryData
	case databasev1.TagType_TAG_TYPE_STRING_ARRAY:
		return ValueTypeStrArr
	case databasev1.TagType_TAG_TYPE_INT_ARRAY:
		return ValueTypeInt64Arr
	case databasev1.TagType_TAG_TYPE_TIMESTAMP:
		return ValueTypeTimestamp
	default:
		panic("unknown tag value type")
	}
}

// MustTagValueToStr converts modelv1.TagValue to string.
func MustTagValueToStr(tag *modelv1.TagValue) string {
	switch tag.Value.(type) {
	case *modelv1.TagValue_Str:
		return `"` + tag.GetStr().Value + `"`
	case *modelv1.TagValue_Int:
		return strconv.FormatInt(tag.GetInt().Value, 10)
	case *modelv1.TagValue_BinaryData:
		return fmt.Sprintf("%x", tag.GetBinaryData())
	case *modelv1.TagValue_Timestamp:
		return tag.GetTimestamp().String()
	default:
		panic("unknown tag value type")
	}
}

// MarshalTagValues marshals tag values.
func MarshalTagValues(dest []byte, tags []*modelv1.TagValue) ([]byte, error) {
	var err error
	for _, tag := range tags {
		dest, err = marshalTagValue(dest, tag)
		if err != nil {
			return nil, err
		}
	}
	return dest, nil
}

// UnmarshalTagValues unmarshals tag values.
func UnmarshalTagValues(dest []byte, destTags []*modelv1.TagValue, src []byte) ([]byte, []*modelv1.TagValue, error) {
	var err error
	var tag *modelv1.TagValue
	for len(src) > 0 {
		dest = dest[:0]
		dest, src, tag, err = unmarshalTagValue(dest, src)
		if err != nil {
			return nil, nil, err
		}
		destTags = append(destTags, tag)
	}
	return dest, destTags, nil
}

func marshalTagValue(dest []byte, tv *modelv1.TagValue) ([]byte, error) {
	dest = append(dest, byte(MustTagValueToValueType(tv)))
	switch tv.Value.(type) {
	case *modelv1.TagValue_Null:
		dest = marshalEntityValue(dest, nil)
	case *modelv1.TagValue_Str:
		dest = marshalEntityValue(dest, []byte(tv.GetStr().Value))
	case *modelv1.TagValue_Int:
		dest = marshalEntityValue(dest, encoding.Int64ToBytes(nil, tv.GetInt().Value))
	case *modelv1.TagValue_BinaryData:
		dest = marshalEntityValue(dest, tv.GetBinaryData())
	case *modelv1.TagValue_Timestamp:
		// Convert timestamp to 64-bit nanoseconds since epoch for efficient storage
		ts := tv.GetTimestamp()
		epochNanos := ts.Seconds*1e9 + int64(ts.Nanos)
		tsBytes := encoding.Int64ToBytes(nil, epochNanos)
		dest = marshalEntityValue(dest, tsBytes)
	default:
		return nil, errors.New("unsupported tag value type: " + tv.String())
	}
	return dest, nil
}

func marshalTagValueWithWildcard(dest []byte, tv *modelv1.TagValue) ([]byte, error) {
	if tv == AnyTagValue {
		dest = marshalEntityValue(dest, anyWildcard)
		return dest, nil
	}
	return marshalTagValue(dest, tv)
}

func unmarshalTagValue(dest []byte, src []byte) ([]byte, []byte, *modelv1.TagValue, error) {
	if len(src) == 0 {
		return nil, nil, nil, errors.New("empty tag value")
	}
	var err error
	vt := ValueType(src[0])
	switch vt {
	case ValueTypeUnknown:
		// skip ValueType and entityDelimiter
		return dest, src[2:], NullTagValue, nil
	case ValueTypeStr:
		if dest, src, err = unmarshalEntityValue(dest, src[1:]); err != nil {
			return nil, nil, nil, errors.WithMessage(err, "unmarshal string tag value")
		}
		if len(dest) == 0 {
			return dest, src, NullTagValue, nil
		}
		return dest, src, &modelv1.TagValue{
			Value: &modelv1.TagValue_Str{
				Str: &modelv1.Str{
					Value: string(dest),
				},
			},
		}, nil
	case ValueTypeInt64:
		if dest, src, err = unmarshalEntityValue(dest, src[1:]); err != nil {
			return nil, nil, nil, errors.WithMessage(err, "unmarshal int tag value")
		}
		return dest, src, &modelv1.TagValue{
			Value: &modelv1.TagValue_Int{
				Int: &modelv1.Int{
					Value: encoding.BytesToInt64(dest),
				},
			},
		}, nil
	case ValueTypeBinaryData:
		if dest, src, err = unmarshalEntityValue(dest, src[1:]); err != nil {
			return nil, nil, nil, errors.WithMessage(err, "unmarshal binary tag value")
		}
		if len(dest) == 0 {
			return dest, src, NullTagValue, nil
		}
		data := make([]byte, len(dest))
		copy(data, dest)
		return dest, src, &modelv1.TagValue{
			Value: &modelv1.TagValue_BinaryData{
				BinaryData: data,
			},
		}, nil
	case ValueTypeTimestamp:
		if dest, src, err = unmarshalEntityValue(dest, src[1:]); err != nil {
			return nil, nil, nil, errors.WithMessage(err, "unmarshal timestamp tag value")
		}
		// Unmarshal 64-bit epoch nanoseconds and convert back to seconds + nanos
		if len(dest) < 8 { // Need at least 8 bytes for the 64-bit value
			return nil, src, nil, errors.New("insufficient bytes for timestamp")
		}
		epochNanos := encoding.BytesToInt64(dest)
		seconds := epochNanos / 1e9
		nanos := int32(epochNanos % 1e9)
		return dest, src, &modelv1.TagValue{
			Value: &modelv1.TagValue_Timestamp{
				Timestamp: &timestamppb.Timestamp{
					Seconds: seconds,
					Nanos:   nanos,
				},
			},
		}, nil
	default:
		return nil, src, nil, fmt.Errorf("unsupported tag value type %d, tag value: %s", vt, src)
	}
}

const (
	entityDelimiter = '|'
	escape          = '\\'
)

var anyWildcard = []byte{'*'}

func marshalEntityValue(dest, src []byte) []byte {
	if src == nil {
		dest = append(dest, entityDelimiter)
		return dest
	}
	if bytes.IndexByte(src, entityDelimiter) < 0 && bytes.IndexByte(src, escape) < 0 {
		dest = append(dest, src...)
		dest = append(dest, entityDelimiter)
		return dest
	}
	for _, b := range src {
		if b == entityDelimiter || b == escape {
			dest = append(dest, escape)
		}
		dest = append(dest, b)
	}
	dest = append(dest, entityDelimiter)
	return dest
}

func unmarshalEntityValue(dest, src []byte) ([]byte, []byte, error) {
	if len(src) == 0 {
		return nil, nil, errors.New("empty entity value")
	}
	if src[0] == entityDelimiter {
		return dest, src[1:], nil
	}
	for len(src) > 0 {
		switch {
		case src[0] == escape:
			if len(src) < 2 {
				return nil, nil, errors.New("invalid escape character")
			}
			src = src[1:]
			dest = append(dest, src[0])
		case src[0] == entityDelimiter:
			return dest, src[1:], nil
		default:
			dest = append(dest, src[0])
		}
		src = src[1:]
	}
	return nil, nil, errors.New("invalid entity value")
}

// MustCompareTagValue compares two tag values.
// It returns 0 if tv1 == tv2, -1 if tv1 < tv2, 1 if tv1 > tv2.
// It panics if the tag value type is inconsistent.
func MustCompareTagValue(tv1, tv2 *modelv1.TagValue) int {
	if tv1 == nil && tv2 == nil {
		return 0
	}
	if tv1 == nil {
		return -1
	}
	if tv2 == nil {
		return 1
	}
	if tv1 == AnyTagValue {
		return 1
	}
	if tv2 == AnyTagValue {
		return -1
	}
	vt1 := MustTagValueToValueType(tv1)
	vt2 := MustTagValueToValueType(tv2)
	if vt1 != vt2 {
		logger.Panicf("inconsistent tag value type: %v vs %v", vt1, vt2)
	}
	switch vt1 {
	case ValueTypeStr:
		return bytes.Compare(convert.StringToBytes(tv1.GetStr().Value), convert.StringToBytes(tv2.GetStr().Value))
	case ValueTypeInt64:
		return int(tv1.GetInt().Value - tv2.GetInt().Value)
	case ValueTypeBinaryData:
		return bytes.Compare(tv1.GetBinaryData(), tv2.GetBinaryData())
	case ValueTypeTimestamp:
		ts1 := tv1.GetTimestamp()
		ts2 := tv2.GetTimestamp()
		if ts1.Seconds < ts2.Seconds {
			return -1
		} else if ts1.Seconds > ts2.Seconds {
			return 1
		}
		return 0
	default:
		logger.Panicf("unsupported tag value type: %v", vt1)
		return 0
	}
}
