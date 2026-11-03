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
	"strings"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// AppendColumnRangeCoerced copies n rows starting at srcPos from src into dst.
// When dst and src share the same underlying type it delegates to
// AppendColumnRange for the fast path. When dst is
// ColumnTypeTagValue and src is a native column type, each source cell is
// promoted to a *modelv1.TagValue wrapper so the merged schema remains
// type-consistent even across groups whose schemas diverged on this column.
// The symmetric ColumnTypeFieldValue coercion is handled similarly. Any other
// type mismatch delegates to AppendColumnRange so the existing error
// path is preserved.
//
// Directional invariant: BuildMultiGroupBatchSchema only ever promotes
// (native → TagValue/FieldValue) on type divergence; it never demotes the
// other direction. So this helper handles "src is native, dst is passthrough"
// — never the inverse. If a future phase introduces a passthrough-to-native
// reshape it will surface here via the delegate's "dst <type> vs src
// tagvalue" error, which is the correct loud-failure outcome.
func AppendColumnRangeCoerced(dst, src vectorized.Column, srcPos, n int) error {
	// The coercions below read the source cell by cell; a constant column is
	// expanded once, which only a batch that did not come off the wire can carry.
	src = vectorized.Materialize(src)
	dTagCol, dIsTag := dst.(*vectorized.TypedColumn[*modelv1.TagValue])
	if dIsTag {
		if _, sameType := src.(*vectorized.TypedColumn[*modelv1.TagValue]); sameType {
			return AppendColumnRange(dst, src, srcPos, n)
		}
		for k := range n {
			rowIdx := srcPos + k
			if src.IsNull(rowIdx) {
				dTagCol.Append(nil)
				dTagCol.MarkNullAt(dTagCol.Len() - 1)
				continue
			}
			var tv *modelv1.TagValue
			switch sc := src.(type) {
			case *vectorized.TypedColumn[int64]:
				tv = &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: sc.Data()[rowIdx]}}}
			case *vectorized.TypedColumn[string]:
				// The cell may borrow a received frame, and the passthrough value travels on to the response.
				tv = &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: strings.Clone(sc.Data()[rowIdx])}}}
			case *vectorized.TypedColumn[[]byte]:
				raw := sc.Data()[rowIdx]
				buf := make([]byte, len(raw))
				copy(buf, raw)
				tv = &modelv1.TagValue{Value: &modelv1.TagValue_BinaryData{BinaryData: buf}}
			case *vectorized.TypedColumn[[]int64]:
				tv = &modelv1.TagValue{Value: &modelv1.TagValue_IntArray{IntArray: &modelv1.IntArray{Value: slices.Clone(sc.Data()[rowIdx])}}}
			case *vectorized.TypedColumn[[]string]:
				tv = &modelv1.TagValue{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: cloneStrings(sc.Data()[rowIdx])}}}
			default:
				return fmt.Errorf("AppendColumnRangeCoerced: cannot coerce %s to tagvalue", src.Type())
			}
			dTagCol.Append(tv)
		}
		return nil
	}
	dFieldCol, dIsField := dst.(*vectorized.TypedColumn[*modelv1.FieldValue])
	if dIsField {
		if _, sameType := src.(*vectorized.TypedColumn[*modelv1.FieldValue]); sameType {
			return AppendColumnRange(dst, src, srcPos, n)
		}
		for k := range n {
			rowIdx := srcPos + k
			if src.IsNull(rowIdx) {
				dFieldCol.Append(nil)
				dFieldCol.MarkNullAt(dFieldCol.Len() - 1)
				continue
			}
			var fv *modelv1.FieldValue
			switch sc := src.(type) {
			case *vectorized.TypedColumn[int64]:
				fv = &modelv1.FieldValue{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: sc.Data()[rowIdx]}}}
			case *vectorized.TypedColumn[float64]:
				fv = &modelv1.FieldValue{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: sc.Data()[rowIdx]}}}
			case *vectorized.TypedColumn[string]:
				fv = &modelv1.FieldValue{Value: &modelv1.FieldValue_Str{Str: &modelv1.Str{Value: strings.Clone(sc.Data()[rowIdx])}}}
			case *vectorized.TypedColumn[[]byte]:
				raw := sc.Data()[rowIdx]
				buf := make([]byte, len(raw))
				copy(buf, raw)
				fv = &modelv1.FieldValue{Value: &modelv1.FieldValue_BinaryData{BinaryData: buf}}
			default:
				return fmt.Errorf("AppendColumnRangeCoerced: cannot coerce %s to fieldvalue", src.Type())
			}
			dFieldCol.Append(fv)
		}
		return nil
	}
	return AppendColumnRange(dst, src, srcPos, n)
}

// appendTagValuesAsNative copies n TagValue cells starting at srcPos into a
// native tag column, keeping nulls. A cell whose variant doesn't match dst's
// type is an error, so a partial can't change a tag's planned type.
func appendTagValuesAsNative(dst, src vectorized.Column, srcPos, n int) error {
	tc, ok := vectorized.Materialize(src).(*vectorized.TypedColumn[*modelv1.TagValue])
	if !ok {
		return fmt.Errorf("appendTagValuesAsNative: source is %s, not tagvalue", src.Type())
	}
	for k := range n {
		rowIdx := srcPos + k
		v := tc.Data()[rowIdx]
		if tc.IsNull(rowIdx) || v == nil {
			dst.AppendNull()
			continue
		}
		matched := false
		switch payload := v.GetValue().(type) {
		case nil, *modelv1.TagValue_Null:
			dst.AppendNull()
			matched = true
		case *modelv1.TagValue_Str:
			if d, isStr := dst.(*vectorized.TypedColumn[string]); isStr {
				// The cell may borrow a received frame.
				d.Append(strings.Clone(payload.Str.GetValue()))
				matched = true
			}
		case *modelv1.TagValue_Int:
			if d, isInt := dst.(*vectorized.TypedColumn[int64]); isInt {
				d.Append(payload.Int.GetValue())
				matched = true
			}
		case *modelv1.TagValue_BinaryData:
			if d, isBytes := dst.(*vectorized.TypedColumn[[]byte]); isBytes {
				d.Append(slices.Clone(payload.BinaryData))
				matched = true
			}
		case *modelv1.TagValue_IntArray:
			if d, isInts := dst.(*vectorized.TypedColumn[[]int64]); isInts {
				d.Append(slices.Clone(payload.IntArray.GetValue()))
				matched = true
			}
		case *modelv1.TagValue_StrArray:
			if d, isStrs := dst.(*vectorized.TypedColumn[[]string]); isStrs {
				d.Append(cloneStrings(payload.StrArray.GetValue()))
				matched = true
			}
		}
		if !matched {
			return fmt.Errorf("cell %d is %T, want %s", rowIdx, v.GetValue(), dst.Type())
		}
	}
	return nil
}
