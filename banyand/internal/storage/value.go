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

package storage

import (
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/pkg/errors"
)

type ValueType byte

const (
	ValueTypeUnknown ValueType = iota
	ValueTypeStr
	ValueTypeInt64
	ValueTypeFloat64
	ValueTypeBinaryData
	ValueTypeStrArr
	ValueTypeInt64Arr
)

func MustTagValueToValueType(tag *modelv1.TagValue) ValueType {
	switch tag.Value.(type) {
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
	default:
		panic("unknown tag value type")
	}
}

func marshalTagValue(dest []byte, tv *modelv1.TagValue) ([]byte, error) {
	if tv.Value == AnyEntry {
		dest = marshalEntityValue(dest, anyWildcard)
		return dest, nil
	}
	dest = append(dest, byte(MustTagValueToValueType(tv)))
	switch tv.Value.(type) {
	case *modelv1.TagValue_Str:
		dest = marshalEntityValue(dest, convert.StringToBytes(tv.GetStr().Value))
	case *modelv1.TagValue_Int:
		dest = encoding.Int64ToBytes(dest, tv.GetInt().Value)
		dest = marshalEntityValue(dest, nil)
	case *modelv1.TagValue_BinaryData:
		dest = marshalEntityValue(dest, tv.GetBinaryData())
	default:
		return nil, errors.New("unsupported tag value type: " + tv.String())
	}
	return dest, nil
}

func unmarshalTagValue(dest []byte, src []byte) ([]byte, []byte, *modelv1.TagValue, error) {
	if len(src) == 0 {
		return nil, nil, nil, errors.New("empty tag value")
	}
	var err error
	vt := ValueType(src[0])
	switch vt {
	case ValueTypeStr:
		if dest, src, err = unmarshalEntityValue(dest, src[1:]); err != nil {
			return nil, nil, nil, errors.WithMessage(err, "unmarshal string tag value")
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
					Value: convert.BytesToInt64(dest),
				},
			},
		}, nil
	case ValueTypeBinaryData:
		if dest, src, err = unmarshalEntityValue(dest, src[1:]); err != nil {
			return nil, nil, nil, errors.WithMessage(err, "unmarshal binary tag value")
		}
		data := make([]byte, len(dest))
		copy(data, dest)
		return dest, src, &modelv1.TagValue{
			Value: &modelv1.TagValue_BinaryData{
				BinaryData: data,
			},
		}, nil
	}
	return nil, nil, nil, errors.New("unsupported tag value type")
}
