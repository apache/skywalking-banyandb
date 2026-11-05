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

package csvfmt

import (
	"fmt"
	"time"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

// CheckRow verifies that the row's resource (column 0) is in the layout and that every
// tag or field column the resource does not own is null.
func (l *Layout) CheckRow(row []Value) error {
	if len(row) != len(l.Columns) {
		return fmt.Errorf("row has %d cells, the layout has %d columns", len(row), len(l.Columns))
	}
	if row[0].Null {
		return fmt.Errorf("column %q is null", l.Columns[0].Header)
	}
	resource := row[0].Str
	owned, ok := l.owned[resource]
	if !ok {
		return fmt.Errorf("resource %q is not in the layout", resource)
	}
	for i := range l.Columns {
		c := &l.Columns[i]
		if row[i].Null || (c.Kind != KindTag && c.Kind != KindField) || owned[i] {
			continue
		}
		return fmt.Errorf("column %q is not null, but resource %q does not own it", c.Header, resource)
	}
	return nil
}

// TagValue converts a cell of a column of type t to a tag value. Null and a type a tag
// cannot hold (float) yield the null tag value.
func (v Value) TagValue(t ValueType) *modelv1.TagValue {
	if !v.Null {
		switch t {
		case ValueString:
			return &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: v.Str}}}
		case ValueInt:
			return &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: v.Int}}}
		case ValueBinary:
			return &modelv1.TagValue{Value: &modelv1.TagValue_BinaryData{BinaryData: v.Bytes}}
		case ValueStringArray:
			return &modelv1.TagValue{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: v.Strs}}}
		case ValueIntArray:
			return &modelv1.TagValue{Value: &modelv1.TagValue_IntArray{IntArray: &modelv1.IntArray{Value: v.Ints}}}
		case ValueTimestamp:
			return &modelv1.TagValue{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(time.Unix(0, v.Int))}}
		default:
		}
	}
	return &modelv1.TagValue{Value: &modelv1.TagValue_Null{}}
}

// Encode renders a cell of a column of type t in its canonical CSV encoding; it is the
// inverse of decoding. Null and an unknown type yield NullCell.
func (v Value) Encode(t ValueType) string {
	if v.Null {
		return NullCell
	}
	switch t {
	case ValueString:
		return EncodeString(v.Str)
	case ValueInt:
		return EncodeInt(v.Int)
	case ValueTimestamp:
		return EncodeTimestamp(v.Int)
	case ValueFloat:
		return EncodeFloat(v.Float)
	case ValueBinary:
		return EncodeBinary(v.Bytes)
	case ValueStringArray:
		return EncodeStringArray(v.Strs)
	case ValueIntArray:
		return EncodeIntArray(v.Ints)
	default:
		return NullCell
	}
}

// FieldValue converts a cell of a column of type t to a field value. Null and a type a
// field cannot hold yield the null field value.
func (v Value) FieldValue(t ValueType) *modelv1.FieldValue {
	if !v.Null {
		switch t {
		case ValueString:
			return &modelv1.FieldValue{Value: &modelv1.FieldValue_Str{Str: &modelv1.Str{Value: v.Str}}}
		case ValueInt:
			return &modelv1.FieldValue{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: v.Int}}}
		case ValueFloat:
			return &modelv1.FieldValue{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: v.Float}}}
		case ValueBinary:
			return &modelv1.FieldValue{Value: &modelv1.FieldValue_BinaryData{BinaryData: v.Bytes}}
		default:
		}
	}
	return &modelv1.FieldValue{Value: &modelv1.FieldValue_Null{}}
}

// RowSize returns the bytes msg adds to a batch as an element of a repeated field, apart
// from the field tag: its length prefix plus its encoded size.
func RowSize(msg proto.Message) int {
	return protowire.SizeBytes(proto.Size(msg))
}
