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

// Value is a single decoded tag value. The accessor matching ValueType returns
// the decoded datum; the others return their zero value. A nil raw value
// decodes to a null Value (IsNull reports true).
type Value struct {
	str       string
	bytes     []byte
	strArr    []string
	intArr    []int64
	int64Val  int64
	floatVal  float64
	valueType valuetype.ValueType
	null      bool
}

// IsNull reports whether the tag was absent on this row.
func (v Value) IsNull() bool { return v.null }

// ValueType returns the type tag of the decoded value.
func (v Value) ValueType() valuetype.ValueType { return v.valueType }

// Str returns the string value (valid for ValueTypeStr).
func (v Value) Str() string { return v.str }

// Int64 returns the integer value (valid for ValueTypeInt64 and, as unix
// nanoseconds, ValueTypeTimestamp).
func (v Value) Int64() int64 { return v.int64Val }

// Float64 returns the float value (valid for ValueTypeFloat64).
func (v Value) Float64() float64 { return v.floatVal }

// Bytes returns the raw value (valid for ValueTypeBinaryData).
func (v Value) Bytes() []byte { return v.bytes }

// StrArr returns the string-array value (valid for ValueTypeStrArr).
func (v Value) StrArr() []string { return v.strArr }

// Int64Arr returns the integer-array value (valid for ValueTypeInt64Arr).
func (v Value) Int64Arr() []int64 { return v.intArr }

// At decodes the value at the given span row. A nil element decodes to a null
// Value. It returns an error if row is out of range.
func (c *TagColumn) At(row int) (Value, error) {
	if row < 0 || row >= len(c.Values) {
		return Value{}, fmt.Errorf("tag %q: row %d out of range [0,%d)", c.Name, row, len(c.Values))
	}
	return DecodeTagValue(c.ValueType, c.Values[row])
}

// DecodeTagValue decodes one marshaled tag value, as stored in the native trace
// block, into a typed Value. It mirrors the engine's own per-row decode so a
// plugin never needs to import banyand/trace internals. A nil raw value yields
// a null Value.
func DecodeTagValue(valueType valuetype.ValueType, raw []byte) (Value, error) {
	if raw == nil {
		return Value{valueType: valueType, null: true}, nil
	}
	switch valueType {
	case valuetype.ValueTypeStr:
		return Value{valueType: valueType, str: string(raw)}, nil
	case valuetype.ValueTypeInt64:
		if len(raw) != 8 {
			return Value{}, fmt.Errorf("int64: expected 8 bytes, got %d", len(raw))
		}
		return Value{valueType: valueType, int64Val: convert.BytesToInt64(raw)}, nil
	case valuetype.ValueTypeFloat64:
		if len(raw) != 8 {
			return Value{}, fmt.Errorf("float64: expected 8 bytes, got %d", len(raw))
		}
		return Value{valueType: valueType, floatVal: convert.BytesToFloat64(raw)}, nil
	case valuetype.ValueTypeBinaryData:
		return Value{valueType: valueType, bytes: raw}, nil
	case valuetype.ValueTypeTimestamp:
		if len(raw) != 8 {
			return Value{}, fmt.Errorf("timestamp: expected 8 bytes, got %d", len(raw))
		}
		return Value{valueType: valueType, int64Val: convert.BytesToInt64(raw)}, nil
	case valuetype.ValueTypeInt64Arr:
		if len(raw)%8 != 0 {
			return Value{}, fmt.Errorf("int64 array: length %d is not a multiple of 8", len(raw))
		}
		values := make([]int64, 0, len(raw)/8)
		for i := 0; i < len(raw); i += 8 {
			values = append(values, convert.BytesToInt64(raw[i:i+8]))
		}
		return Value{valueType: valueType, intArr: values}, nil
	case valuetype.ValueTypeStrArr:
		var values []string
		for idx := 0; idx < len(raw); {
			end, next, err := vararray.UnmarshalVarArray(raw, idx)
			if err != nil {
				return Value{}, fmt.Errorf("str array: %w", err)
			}
			values = append(values, string(raw[idx:end]))
			idx = next
		}
		return Value{valueType: valueType, strArr: values}, nil
	case valuetype.ValueTypeUnknown:
		return Value{valueType: valueType, null: true}, nil
	default:
		return Value{}, fmt.Errorf("unsupported value type: %d", valueType)
	}
}
