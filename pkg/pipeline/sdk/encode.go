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

package sdk

import (
	"fmt"

	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/encoding/vararray"
	"github.com/apache/skywalking-banyandb/pkg/pb/v1/valuetype"
)

// int64Width is the byte width of one encoded int64 (matches convert.Int64ToBytes).
const int64Width = 8

// EncodeTagValue marshals a raw Go value into the native tag-value byte
// layout that DecodeTagValue decodes — the inverse of DecodeTagValue. It
// deliberately takes a plain Go value (any), not a Value: Value's fields are
// unexported and this package exports no constructor for it, so an encoder
// built around Value would be unconstructible from outside this package
// (e.g. from pkg/pipeline/sdk/sdktest, a different package, or a plugin
// author's own test) — defeating the purpose of a shared, reusable encoder.
//
// Only a genuinely nil v (an untyped nil `any`) encodes to a nil raw value —
// the native "tag absent on this row" representation. A present-but-empty
// collection ([]byte{}, []string{}, []int64{}) encodes to a NON-nil empty
// raw value, so it decodes back as an empty value rather than as null,
// consistently across all three collection types.
//
// Supported (vt, Go type) pairs:
//   - ValueTypeStr:        string
//   - ValueTypeInt64:      int64
//   - ValueTypeTimestamp:  int64 (unix nanoseconds; same 8-byte wire layout as Int64)
//   - ValueTypeFloat64:    float64
//   - ValueTypeBinaryData: []byte
//   - ValueTypeStrArr:     []string
//   - ValueTypeInt64Arr:   []int64
//
// A mismatch between vt and v's concrete Go type, or an unsupported vt,
// returns an error.
func EncodeTagValue(vt valuetype.ValueType, v any) ([]byte, error) {
	if v == nil {
		return nil, nil
	}
	switch vt {
	case valuetype.ValueTypeStr:
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("EncodeTagValue: ValueTypeStr requires a string, got %T", v)
		}
		// Allocated copy — NOT convert.StringToBytes, whose unsafe zero-copy
		// slice aliases the string's read-only backing memory and must not be
		// handed out as a mutable []byte. An empty string is a value (not null),
		// so it encodes to a non-nil empty slice; only a nil `v` (handled above)
		// encodes to a nil raw, which DecodeTagValue reads back as null.
		if len(s) == 0 {
			return []byte{}, nil
		}
		return []byte(s), nil
	case valuetype.ValueTypeInt64, valuetype.ValueTypeTimestamp:
		i, ok := v.(int64)
		if !ok {
			return nil, fmt.Errorf("EncodeTagValue: value type %d requires an int64, got %T", vt, v)
		}
		return convert.Int64ToBytes(i), nil
	case valuetype.ValueTypeFloat64:
		f, ok := v.(float64)
		if !ok {
			return nil, fmt.Errorf("EncodeTagValue: ValueTypeFloat64 requires a float64, got %T", v)
		}
		return convert.Float64ToBytes(f), nil
	case valuetype.ValueTypeBinaryData:
		b, ok := v.([]byte)
		if !ok {
			return nil, fmt.Errorf("EncodeTagValue: ValueTypeBinaryData requires a []byte, got %T", v)
		}
		// make (not append to a nil slice) so a present-but-empty []byte
		// yields a non-nil empty result rather than nil (which decodes as null).
		out := make([]byte, len(b))
		copy(out, b)
		return out, nil
	case valuetype.ValueTypeStrArr:
		arr, ok := v.([]string)
		if !ok {
			return nil, fmt.Errorf("EncodeTagValue: ValueTypeStrArr requires a []string, got %T", v)
		}
		// Start from a non-nil empty slice so an empty []string encodes to a
		// non-nil empty result (present, not null), consistent with []int64.
		dst := []byte{}
		for _, s := range arr {
			dst = vararray.MarshalVarArray(dst, convert.StringToBytes(s))
		}
		return dst, nil
	case valuetype.ValueTypeInt64Arr:
		arr, ok := v.([]int64)
		if !ok {
			return nil, fmt.Errorf("EncodeTagValue: ValueTypeInt64Arr requires a []int64, got %T", v)
		}
		dst := make([]byte, 0, len(arr)*int64Width)
		for _, i := range arr {
			dst = append(dst, convert.Int64ToBytes(i)...)
		}
		return dst, nil
	default:
		return nil, fmt.Errorf("EncodeTagValue: unsupported value type: %d", vt)
	}
}
