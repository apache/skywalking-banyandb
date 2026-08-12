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

// AtInto decodes the value at the given span row into dst. Callers may reuse
// dst across rows to avoid copying the Value and reallocating array storage.
func (c *TagColumn) AtInto(row int, dst *Value) error {
	if dst == nil {
		return fmt.Errorf("tag %q: destination is nil", c.Name)
	}
	if row < 0 || row >= len(c.Values) {
		return fmt.Errorf("tag %q: row %d out of range [0,%d)", c.Name, row, len(c.Values))
	}
	return DecodeTagValueInto(dst, c.ValueType, c.Values[row])
}

// DecodeTagValue decodes one marshaled tag value, as stored in the native trace
// block, into a typed Value. It mirrors the engine's own per-row decode so a
// plugin never needs to import banyand/trace internals. A nil raw value yields
// a null Value.
func DecodeTagValue(valueType valuetype.ValueType, raw []byte) (Value, error) {
	var value Value
	if err := DecodeTagValueInto(&value, valueType, raw); err != nil {
		return Value{}, err
	}
	return value, nil
}

// DecodeTagValueInto decodes one marshaled tag value into dst. Existing array
// capacity in dst is reused; all fields from a preceding value are reset.
func DecodeTagValueInto(dst *Value, valueType valuetype.ValueType, raw []byte) error {
	if dst == nil {
		return fmt.Errorf("destination is nil")
	}
	strArr := dst.strArr[:0]
	intArr := dst.intArr[:0]
	*dst = Value{valueType: valueType, strArr: strArr, intArr: intArr}
	if raw == nil {
		dst.null = true
		return nil
	}
	switch valueType {
	case valuetype.ValueTypeStr:
		dst.str = string(raw)
	case valuetype.ValueTypeInt64:
		if len(raw) != 8 {
			return fmt.Errorf("int64: expected 8 bytes, got %d", len(raw))
		}
		dst.int64Val = convert.BytesToInt64(raw)
	case valuetype.ValueTypeFloat64:
		if len(raw) != 8 {
			return fmt.Errorf("float64: expected 8 bytes, got %d", len(raw))
		}
		dst.floatVal = convert.BytesToFloat64(raw)
	case valuetype.ValueTypeBinaryData:
		dst.bytes = raw
	case valuetype.ValueTypeTimestamp:
		if len(raw) != 8 {
			return fmt.Errorf("timestamp: expected 8 bytes, got %d", len(raw))
		}
		dst.int64Val = convert.BytesToInt64(raw)
	case valuetype.ValueTypeInt64Arr:
		if len(raw)%8 != 0 {
			return fmt.Errorf("int64 array: length %d is not a multiple of 8", len(raw))
		}
		for offset := 0; offset < len(raw); offset += 8 {
			dst.intArr = append(dst.intArr, convert.BytesToInt64(raw[offset:offset+8]))
		}
	case valuetype.ValueTypeStrArr:
		for idx := 0; idx < len(raw); {
			end, next, err := vararray.UnmarshalVarArray(raw, idx)
			if err != nil {
				return fmt.Errorf("str array: %w", err)
			}
			dst.strArr = append(dst.strArr, string(raw[idx:end]))
			idx = next
		}
	case valuetype.ValueTypeUnknown:
		dst.null = true
	default:
		return fmt.Errorf("unsupported value type: %d", valueType)
	}
	return nil
}
