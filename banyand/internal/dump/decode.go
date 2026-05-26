// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package dump

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

// DecodeTagValue converts a byte-encoded tag value back to a typed
// modelv1.TagValue. valueType selects the decoding; the returned value is
// self-contained (string / binary contents are copied), so it may be retained
// past the next Iterator.Next()/Close().
func DecodeTagValue(valueType pbv1.ValueType, value []byte, valueArr [][]byte) *modelv1.TagValue {
	if value == nil && valueArr == nil {
		return pbv1.NullTagValue
	}

	switch valueType {
	case pbv1.ValueTypeStr:
		if value == nil {
			return pbv1.NullTagValue
		}
		return &modelv1.TagValue{
			Value: &modelv1.TagValue_Str{
				Str: &modelv1.Str{
					Value: string(value),
				},
			},
		}
	case pbv1.ValueTypeInt64:
		if value == nil {
			return pbv1.NullTagValue
		}
		return &modelv1.TagValue{
			Value: &modelv1.TagValue_Int{
				Int: &modelv1.Int{
					Value: convert.BytesToInt64(value),
				},
			},
		}
	case pbv1.ValueTypeTimestamp:
		if value == nil {
			return pbv1.NullTagValue
		}
		return &modelv1.TagValue{
			Value: &modelv1.TagValue_Timestamp{
				Timestamp: timestamppb.New(time.Unix(0, convert.BytesToInt64(value))),
			},
		}
	case pbv1.ValueTypeStrArr:
		var values []string
		if valueArr != nil {
			values = make([]string, 0, len(valueArr))
			for _, item := range valueArr {
				values = append(values, string(item))
			}
		} else {
			values = decodePackedStrArray(value)
		}
		return &modelv1.TagValue{
			Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: values}},
		}
	case pbv1.ValueTypeInt64Arr:
		var values []int64
		if valueArr != nil {
			values = make([]int64, 0, len(valueArr))
			for _, item := range valueArr {
				values = append(values, convert.BytesToInt64(item))
			}
		} else {
			values = decodePackedInt64Array(value)
		}
		return &modelv1.TagValue{
			Value: &modelv1.TagValue_IntArray{IntArray: &modelv1.IntArray{Value: values}},
		}
	case pbv1.ValueTypeBinaryData:
		if value == nil {
			return pbv1.NullTagValue
		}
		b := make([]byte, len(value))
		copy(b, value)
		return &modelv1.TagValue{
			Value: &modelv1.TagValue_BinaryData{BinaryData: b},
		}
	default:
		if value != nil {
			return &modelv1.TagValue{
				Value: &modelv1.TagValue_Str{
					Str: &modelv1.Str{
						Value: string(value),
					},
				},
			}
		}
		return pbv1.NullTagValue
	}
}

// decodePackedStrArray splits a column-stored string array (elements separated by
// encoding.EntityDelimiter with encoding.Escape escaping) into its elements.
func decodePackedStrArray(value []byte) []string {
	if len(value) == 0 {
		return nil
	}
	// UnmarshalVarArray decodes in place; copy first so the caller's value is kept intact.
	buf := append([]byte(nil), value...)
	var out []string
	for idx := 0; idx < len(buf); {
		end, next, err := encoding.UnmarshalVarArray(buf, idx)
		if err != nil {
			break
		}
		out = append(out, string(buf[idx:end]))
		idx = next
	}
	return out
}

// decodePackedInt64Array splits a column-stored int64 array (consecutive 8-byte
// big-endian int64s) into its elements.
func decodePackedInt64Array(value []byte) []int64 {
	out := make([]int64, 0, len(value)/8)
	for i := 0; i+8 <= len(value); i += 8 {
		out = append(out, convert.BytesToInt64(value[i:i+8]))
	}
	return out
}
