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
	"fmt"
	"slices"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// extractTagRow writes one already-decoded *modelv1.TagValue into row rowIdx
// of col. The row must already exist in the column (length >= rowIdx+1);
// extract does not grow the column.
//
// Slice-typed values (BinaryData, IntArray, StrArray) are defensively copied
// because the storage layer may reuse the protobuf objects between Pull()
// calls; storing slices by reference would alias prior batches into later ones.
//
// An unknown TagValue oneof variant returns an error — silent null fallback
// would mask schema-evolution bugs.
func extractTagRow(col vectorized.Column, rowIdx int, tv *modelv1.TagValue) error {
	if tv == nil {
		return fmt.Errorf("vectorized.measure: nil TagValue at row %d", rowIdx)
	}
	switch v := tv.Value.(type) {
	case *modelv1.TagValue_Null:
		col.MarkNullAt(rowIdx)
		return nil
	case *modelv1.TagValue_Int:
		c, ok := col.(*vectorized.TypedColumn[int64])
		if !ok {
			return columnTypeMismatch(col, "int64", rowIdx)
		}
		c.Data()[rowIdx] = v.Int.GetValue()
		return nil
	case *modelv1.TagValue_Str:
		c, ok := col.(*vectorized.TypedColumn[string])
		if !ok {
			return columnTypeMismatch(col, "string", rowIdx)
		}
		c.Data()[rowIdx] = v.Str.GetValue()
		return nil
	case *modelv1.TagValue_BinaryData:
		c, ok := col.(*vectorized.TypedColumn[[]byte])
		if !ok {
			return columnTypeMismatch(col, "bytes", rowIdx)
		}
		buf := make([]byte, len(v.BinaryData))
		copy(buf, v.BinaryData)
		c.Data()[rowIdx] = buf
		return nil
	case *modelv1.TagValue_IntArray:
		c, ok := col.(*vectorized.TypedColumn[[]int64])
		if !ok {
			return columnTypeMismatch(col, "int64[]", rowIdx)
		}
		c.Data()[rowIdx] = slices.Clone(v.IntArray.GetValue())
		return nil
	case *modelv1.TagValue_StrArray:
		c, ok := col.(*vectorized.TypedColumn[[]string])
		if !ok {
			return columnTypeMismatch(col, "string[]", rowIdx)
		}
		c.Data()[rowIdx] = slices.Clone(v.StrArray.GetValue())
		return nil
	case *modelv1.TagValue_Timestamp:
		c, ok := col.(*vectorized.TypedColumn[int64])
		if !ok {
			return columnTypeMismatch(col, "int64", rowIdx)
		}
		c.Data()[rowIdx] = v.Timestamp.AsTime().UnixNano()
		return nil
	default:
		return fmt.Errorf("vectorized.measure: unsupported TagValue variant %T at row %d", tv.Value, rowIdx)
	}
}

// extractFieldRow is the field-side counterpart of extractTagRow. Same rules
// for defensive copies and unknown variants.
func extractFieldRow(col vectorized.Column, rowIdx int, fv *modelv1.FieldValue) error {
	if fv == nil {
		return fmt.Errorf("vectorized.measure: nil FieldValue at row %d", rowIdx)
	}
	switch v := fv.Value.(type) {
	case *modelv1.FieldValue_Null:
		col.MarkNullAt(rowIdx)
		return nil
	case *modelv1.FieldValue_Int:
		c, ok := col.(*vectorized.TypedColumn[int64])
		if !ok {
			return columnTypeMismatch(col, "int64", rowIdx)
		}
		c.Data()[rowIdx] = v.Int.GetValue()
		return nil
	case *modelv1.FieldValue_Float:
		c, ok := col.(*vectorized.TypedColumn[float64])
		if !ok {
			return columnTypeMismatch(col, "float64", rowIdx)
		}
		c.Data()[rowIdx] = v.Float.GetValue()
		return nil
	case *modelv1.FieldValue_Str:
		c, ok := col.(*vectorized.TypedColumn[string])
		if !ok {
			return columnTypeMismatch(col, "string", rowIdx)
		}
		c.Data()[rowIdx] = v.Str.GetValue()
		return nil
	case *modelv1.FieldValue_BinaryData:
		c, ok := col.(*vectorized.TypedColumn[[]byte])
		if !ok {
			return columnTypeMismatch(col, "bytes", rowIdx)
		}
		buf := make([]byte, len(v.BinaryData))
		copy(buf, v.BinaryData)
		c.Data()[rowIdx] = buf
		return nil
	default:
		return fmt.Errorf("vectorized.measure: unsupported FieldValue variant %T at row %d", fv.Value, rowIdx)
	}
}

// extractTagBulk extracts n tag values starting at src[0] into col[offset..offset+n).
// The v1 implementation is a tight per-row loop; future SIMD-friendly fast paths
// can specialize this signature without changing callers.
func extractTagBulk(col vectorized.Column, offset int, src []*modelv1.TagValue, n int) error {
	if n > len(src) {
		return fmt.Errorf("vectorized.measure: extractTagBulk n=%d > len(src)=%d", n, len(src))
	}
	for i := range n {
		if extractErr := extractTagRow(col, offset+i, src[i]); extractErr != nil {
			return extractErr
		}
	}
	return nil
}

// extractFieldBulk is the field-side counterpart of extractTagBulk.
func extractFieldBulk(col vectorized.Column, offset int, src []*modelv1.FieldValue, n int) error {
	if n > len(src) {
		return fmt.Errorf("vectorized.measure: extractFieldBulk n=%d > len(src)=%d", n, len(src))
	}
	for i := range n {
		if extractErr := extractFieldRow(col, offset+i, src[i]); extractErr != nil {
			return extractErr
		}
	}
	return nil
}

func columnTypeMismatch(col vectorized.Column, want string, row int) error {
	return fmt.Errorf("vectorized.measure: column type %s, value type %s, row %d",
		col.Type(), want, row)
}
