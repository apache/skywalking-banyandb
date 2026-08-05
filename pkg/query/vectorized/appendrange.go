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

package vectorized

import (
	"fmt"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

// AppendActiveRows appends every active row of src (its Selection when present,
// else all [0,Len) rows) onto dst, column by column, advancing dst.Len. dst and
// src must share the same schema/column layout. It is the columnar analog of a
// row-wise copy used to flatten a Selection-narrowed batch into a dense one (frame
// emit) or to concatenate per-node batches (liaison merge).
func AppendActiveRows(dst, src *RecordBatch) error {
	if src == nil || src.ActiveLen() == 0 {
		return nil
	}
	if len(dst.Columns) != len(src.Columns) {
		return fmt.Errorf("AppendActiveRows: column count mismatch dst=%d src=%d", len(dst.Columns), len(src.Columns))
	}
	if src.Selection == nil {
		for colIdx := range src.Columns {
			if appendErr := AppendColumnRange(dst.Columns[colIdx], src.Columns[colIdx], 0, src.Len); appendErr != nil {
				return appendErr
			}
		}
		dst.Len += src.Len
		return nil
	}
	for _, row := range src.Selection {
		for colIdx := range src.Columns {
			if appendErr := AppendColumnRange(dst.Columns[colIdx], src.Columns[colIdx], int(row), 1); appendErr != nil {
				return appendErr
			}
		}
	}
	dst.Len += len(src.Selection)
	return nil
}

// AppendColumnRange copies n rows starting at srcPos from src into dst.
// Both columns must share the same TypedColumn[T] type. Validity bits are
// propagated cell-by-cell via dst.MarkNullAt when src.IsNull reports null at
// the corresponding row.
//
// Slice-typed cell values ([]byte / []int64 / []string) are not deep-copied
// here. Storage decoders already produce owned cell slices, and avoiding a
// second copy keeps egress paths allocation-stable.
func AppendColumnRange(dst, src Column, srcPos, n int) error {
	startLen := dst.Len()
	switch d := dst.(type) {
	case *TypedColumn[int64]:
		sCol, ok := src.(*TypedColumn[int64])
		if !ok {
			return fmt.Errorf("AppendColumnRange: dst int64 vs src %s", src.Type())
		}
		sData := sCol.Data()
		for k := range n {
			d.Append(sData[srcPos+k])
		}
	case *TypedColumn[float64]:
		sCol, ok := src.(*TypedColumn[float64])
		if !ok {
			return fmt.Errorf("AppendColumnRange: dst float64 vs src %s", src.Type())
		}
		sData := sCol.Data()
		for k := range n {
			d.Append(sData[srcPos+k])
		}
	case *TypedColumn[string]:
		sCol, ok := src.(*TypedColumn[string])
		if !ok {
			return fmt.Errorf("AppendColumnRange: dst string vs src %s", src.Type())
		}
		sData := sCol.Data()
		for k := range n {
			d.Append(sData[srcPos+k])
		}
	case *TypedColumn[[]byte]:
		sCol, ok := src.(*TypedColumn[[]byte])
		if !ok {
			return fmt.Errorf("AppendColumnRange: dst bytes vs src %s", src.Type())
		}
		sData := sCol.Data()
		for k := range n {
			d.Append(sData[srcPos+k])
		}
	case *TypedColumn[[]int64]:
		sCol, ok := src.(*TypedColumn[[]int64])
		if !ok {
			return fmt.Errorf("AppendColumnRange: dst int64[] vs src %s", src.Type())
		}
		sData := sCol.Data()
		for k := range n {
			d.Append(sData[srcPos+k])
		}
	case *TypedColumn[[]string]:
		sCol, ok := src.(*TypedColumn[[]string])
		if !ok {
			return fmt.Errorf("AppendColumnRange: dst string[] vs src %s", src.Type())
		}
		sData := sCol.Data()
		for k := range n {
			d.Append(sData[srcPos+k])
		}
	case *TypedColumn[*modelv1.TagValue]:
		sCol, ok := src.(*TypedColumn[*modelv1.TagValue])
		if !ok {
			return fmt.Errorf("AppendColumnRange: dst tagvalue vs src %s", src.Type())
		}
		sData := sCol.Data()
		for k := range n {
			d.Append(sData[srcPos+k])
		}
	case *TypedColumn[*modelv1.FieldValue]:
		sCol, ok := src.(*TypedColumn[*modelv1.FieldValue])
		if !ok {
			return fmt.Errorf("AppendColumnRange: dst fieldvalue vs src %s", src.Type())
		}
		sData := sCol.Data()
		for k := range n {
			d.Append(sData[srcPos+k])
		}
	default:
		return fmt.Errorf("AppendColumnRange: unsupported dst type %s", dst.Type())
	}
	for k := range n {
		if src.IsNull(srcPos + k) {
			dst.MarkNullAt(startLen + k)
		}
	}
	return nil
}
