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
