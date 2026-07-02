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

package measure

import (
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// G5b — byte-to-column decoders. These mirror mustDecodeTagValue and
// mustDecodeFieldValue from query.go but emit directly into typed
// vectorized.Column instances, bypassing the *modelv1.TagValue intermediate
// for the batch path.
//
// Used by blockCursor.copyAllToBatch / copyToBatch when populating a
// *model.MeasureBatch's typed tag/field columns. For passthrough column
// types (ColumnTypeTagValue / ColumnTypeFieldValue), the caller falls back
// to mustDecodeTagValue + Append the *modelv1.TagValue pointer.

// appendDecodedTagBytesAsTyped decodes a single stored cell into a typed
// tag column. The column must match valueType — a programmer-error
// mismatch panics rather than silently corrupting data, the same contract
// mustDecodeTagValue holds. A nil raw value yields AppendNull.
func appendDecodedTagBytesAsTyped(col vectorized.Column, valueType pbv1.ValueType, raw []byte) {
	if raw == nil {
		col.AppendNull()
		return
	}
	switch valueType {
	case pbv1.ValueTypeInt64:
		c, ok := col.(*vectorized.TypedColumn[int64])
		if !ok {
			logger.Panicf("appendDecodedTagBytesAsTyped: column type %s mismatched with valueType int64", col.Type())
		}
		c.Append(convert.BytesToInt64(raw))
	case pbv1.ValueTypeStr:
		c, ok := col.(*vectorized.TypedColumn[string])
		if !ok {
			logger.Panicf("appendDecodedTagBytesAsTyped: column type %s mismatched with valueType str", col.Type())
		}
		c.Append(string(raw))
	case pbv1.ValueTypeBinaryData:
		c, ok := col.(*vectorized.TypedColumn[[]byte])
		if !ok {
			logger.Panicf("appendDecodedTagBytesAsTyped: column type %s mismatched with valueType bytes", col.Type())
		}
		buf := make([]byte, len(raw))
		copy(buf, raw)
		c.Append(buf)
	case pbv1.ValueTypeInt64Arr:
		c, ok := col.(*vectorized.TypedColumn[[]int64])
		if !ok {
			logger.Panicf("appendDecodedTagBytesAsTyped: column type %s mismatched with valueType int64[]", col.Type())
		}
		var values []int64
		for i := 0; i < len(raw); i += 8 {
			values = append(values, convert.BytesToInt64(raw[i:i+8]))
		}
		c.Append(values)
	case pbv1.ValueTypeStrArr:
		c, ok := col.(*vectorized.TypedColumn[[]string])
		if !ok {
			logger.Panicf("appendDecodedTagBytesAsTyped: column type %s mismatched with valueType string[]", col.Type())
		}
		bb := bigValuePool.Generate()
		defer bigValuePool.Release(bb)
		var values []string
		buf := raw
		var err error
		for len(buf) > 0 {
			bb.Buf, buf, err = unmarshalVarArray(bb.Buf[:0], buf)
			if err != nil {
				logger.Panicf("unmarshalVarArray failed: %v", err)
			}
			values = append(values, string(bb.Buf))
		}
		c.Append(values)
	case pbv1.ValueTypeUnknown:
		logger.Panicf("appendDecodedTagBytesAsTyped: unknown value type")
	default:
		logger.Panicf("appendDecodedTagBytesAsTyped: unsupported value type %v", valueType)
	}
}

// appendDecodedFieldBytesAsTyped is the field-side counterpart. Mirrors
// mustDecodeFieldValue's null-vs-empty distinction: for nil raw input,
// pbv1.ValueTypeStr / ValueTypeBinaryData yield "valid empty" cells (so
// downstream serializer reproduces pbv1.EmptyStrFieldValue /
// EmptyBinaryFieldValue); other types yield AppendNull.
func appendDecodedFieldBytesAsTyped(col vectorized.Column, valueType pbv1.ValueType, raw []byte) {
	if raw == nil {
		switch valueType {
		case pbv1.ValueTypeStr:
			c, ok := col.(*vectorized.TypedColumn[string])
			if !ok {
				logger.Panicf("appendDecodedFieldBytesAsTyped: column type %s mismatched with valueType str", col.Type())
			}
			c.Append("")
			return
		case pbv1.ValueTypeBinaryData:
			c, ok := col.(*vectorized.TypedColumn[[]byte])
			if !ok {
				logger.Panicf("appendDecodedFieldBytesAsTyped: column type %s mismatched with valueType bytes", col.Type())
			}
			c.Append([]byte{})
			return
		default:
			col.AppendNull()
			return
		}
	}
	switch valueType {
	case pbv1.ValueTypeInt64:
		c, ok := col.(*vectorized.TypedColumn[int64])
		if !ok {
			logger.Panicf("appendDecodedFieldBytesAsTyped: column type %s mismatched with valueType int64", col.Type())
		}
		c.Append(convert.BytesToInt64(raw))
	case pbv1.ValueTypeFloat64:
		c, ok := col.(*vectorized.TypedColumn[float64])
		if !ok {
			logger.Panicf("appendDecodedFieldBytesAsTyped: column type %s mismatched with valueType float64", col.Type())
		}
		c.Append(convert.BytesToFloat64(raw))
	case pbv1.ValueTypeStr:
		c, ok := col.(*vectorized.TypedColumn[string])
		if !ok {
			logger.Panicf("appendDecodedFieldBytesAsTyped: column type %s mismatched with valueType str", col.Type())
		}
		c.Append(string(raw))
	case pbv1.ValueTypeBinaryData:
		c, ok := col.(*vectorized.TypedColumn[[]byte])
		if !ok {
			logger.Panicf("appendDecodedFieldBytesAsTyped: column type %s mismatched with valueType bytes", col.Type())
		}
		buf := make([]byte, len(raw))
		copy(buf, raw)
		c.Append(buf)
	case pbv1.ValueTypeUnknown, pbv1.ValueTypeInt64Arr, pbv1.ValueTypeStrArr:
		logger.Panicf("appendDecodedFieldBytesAsTyped: unsupported value type %v", valueType)
	default:
		logger.Panicf("appendDecodedFieldBytesAsTyped: unsupported value type %v", valueType)
	}
}

// appendTagValueAsTyped projects a pre-decoded *modelv1.TagValue onto a
// typed tag column. Used by the indexValue substitution path in
// copyAllToBatch where the storage layer has already produced a
// *modelv1.TagValue (from a hidden index lookup) and we need to project
// its inner primitive into the typed column slot.
//
// On a oneof / column-type mismatch the column receives an AppendNull
// rather than panicking — degrades gracefully on heterogeneous
// projections, matching copyAllTo's null-substitution pattern.
func appendTagValueAsTyped(col vectorized.Column, v *modelv1.TagValue) {
	if v == nil {
		col.AppendNull()
		return
	}
	switch x := v.Value.(type) {
	case *modelv1.TagValue_Null:
		col.AppendNull()
	case *modelv1.TagValue_Int:
		if c, ok := col.(*vectorized.TypedColumn[int64]); ok {
			c.Append(x.Int.GetValue())
			return
		}
		col.AppendNull()
	case *modelv1.TagValue_Str:
		if c, ok := col.(*vectorized.TypedColumn[string]); ok {
			c.Append(x.Str.GetValue())
			return
		}
		col.AppendNull()
	case *modelv1.TagValue_BinaryData:
		if c, ok := col.(*vectorized.TypedColumn[[]byte]); ok {
			buf := make([]byte, len(x.BinaryData))
			copy(buf, x.BinaryData)
			c.Append(buf)
			return
		}
		col.AppendNull()
	case *modelv1.TagValue_IntArray:
		if c, ok := col.(*vectorized.TypedColumn[[]int64]); ok {
			out := make([]int64, len(x.IntArray.GetValue()))
			copy(out, x.IntArray.GetValue())
			c.Append(out)
			return
		}
		col.AppendNull()
	case *modelv1.TagValue_StrArray:
		if c, ok := col.(*vectorized.TypedColumn[[]string]); ok {
			out := make([]string, len(x.StrArray.GetValue()))
			copy(out, x.StrArray.GetValue())
			c.Append(out)
			return
		}
		col.AppendNull()
	default:
		col.AppendNull()
	}
}
