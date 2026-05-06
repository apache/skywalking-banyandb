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
	"slices"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// serializeBatchToProto converts the active rows of b into measurev1.InternalDataPoint
// messages, appending to dst. Pass dst=nil to allocate; pass dst[:0] to reuse capacity.
//
// This is the focal point of differential parity testing: it is the only place
// where the vectorized output shape diverges from the row-based output shape.
// Row order matches batch row order (respecting Selection); the row-based path
// produces messages in the same order from MeasureResult iteration.
func serializeBatchToProto(b *vectorized.RecordBatch, dst []*measurev1.InternalDataPoint) []*measurev1.InternalDataPoint {
	if dst == nil {
		dst = make([]*measurev1.InternalDataPoint, 0, b.ActiveLen())
	}
	schema := b.Schema
	active := activeIndices(b)
	for _, rowIdx := range active {
		dp := buildDataPoint(b, schema, int(rowIdx))
		idp := &measurev1.InternalDataPoint{DataPoint: dp}
		if i := schema.ShardIDIndex(); i >= 0 {
			idp.ShardId = uint32(b.Columns[i].(*vectorized.TypedColumn[int64]).Data()[rowIdx])
		}
		dst = append(dst, idp)
	}
	return dst
}

// buildDataPoint materializes one DataPoint from row rowIdx of b. Tags are
// emitted family-by-family using the schema's pre-computed TagFamilyGroups
// layout — no per-row map allocation. Field columns become DataPoint_Field
// entries in schema order.
func buildDataPoint(b *vectorized.RecordBatch, schema *vectorized.BatchSchema, rowIdx int) *measurev1.DataPoint {
	dp := &measurev1.DataPoint{}
	if i := schema.TimestampIndex(); i >= 0 {
		ns := b.Columns[i].(*vectorized.TypedColumn[int64]).Data()[rowIdx]
		dp.Timestamp = timestamppb.New(time.Unix(0, ns))
	}
	if i := schema.VersionIndex(); i >= 0 {
		dp.Version = b.Columns[i].(*vectorized.TypedColumn[int64]).Data()[rowIdx]
	}
	if i := schema.SeriesIDIndex(); i >= 0 {
		dp.Sid = uint64(b.Columns[i].(*vectorized.TypedColumn[int64]).Data()[rowIdx])
	}
	if len(schema.TagFamilyGroups) > 0 {
		dp.TagFamilies = make([]*modelv1.TagFamily, 0, len(schema.TagFamilyGroups))
		for _, group := range schema.TagFamilyGroups {
			tf := &modelv1.TagFamily{
				Name: group.Family,
				Tags: make([]*modelv1.Tag, 0, len(group.Columns)),
			}
			for _, colIdx := range group.Columns {
				tf.Tags = append(tf.Tags, &modelv1.Tag{
					Key:   schema.Columns[colIdx].Name,
					Value: columnValueToTagValue(b.Columns[colIdx], rowIdx),
				})
			}
			dp.TagFamilies = append(dp.TagFamilies, tf)
		}
	}
	if len(schema.FieldColumns) > 0 {
		dp.Fields = make([]*measurev1.DataPoint_Field, 0, len(schema.FieldColumns))
		for _, colIdx := range schema.FieldColumns {
			dp.Fields = append(dp.Fields, &measurev1.DataPoint_Field{
				Name:  schema.Columns[colIdx].Name,
				Value: columnValueToFieldValue(b.Columns[colIdx], rowIdx),
			})
		}
	}
	return dp
}

// columnValueToTagValue materializes a *modelv1.TagValue from one row of col.
//
// Passthrough columns (TypedColumn[*modelv1.TagValue]) take a fast path:
// the original protobuf pointer from the scan source is returned directly
// — zero allocation, byte-identical to what the row path emits.
//
// Typed columns reconstruct the protobuf wrapper. Slice-typed values
// (BinaryData, IntArray, StrArray) are defensively copied so the produced
// TagValue does not alias the column's backing slice across pool reuse.
func columnValueToTagValue(col vectorized.Column, rowIdx int) *modelv1.TagValue {
	if pc, ok := col.(*vectorized.TypedColumn[*modelv1.TagValue]); ok {
		v := pc.Data()[rowIdx]
		if v == nil {
			return pbv1NullTagValueRef
		}
		return v
	}
	if col.IsNull(rowIdx) {
		return &modelv1.TagValue{Value: &modelv1.TagValue_Null{}}
	}
	switch c := col.(type) {
	case *vectorized.TypedColumn[int64]:
		return &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: c.Data()[rowIdx]}}}
	case *vectorized.TypedColumn[string]:
		return &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: c.Data()[rowIdx]}}}
	case *vectorized.TypedColumn[[]byte]:
		src := c.Data()[rowIdx]
		buf := make([]byte, len(src))
		copy(buf, src)
		return &modelv1.TagValue{Value: &modelv1.TagValue_BinaryData{BinaryData: buf}}
	case *vectorized.TypedColumn[[]int64]:
		return &modelv1.TagValue{Value: &modelv1.TagValue_IntArray{IntArray: &modelv1.IntArray{Value: slices.Clone(c.Data()[rowIdx])}}}
	case *vectorized.TypedColumn[[]string]:
		return &modelv1.TagValue{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: slices.Clone(c.Data()[rowIdx])}}}
	}
	return &modelv1.TagValue{Value: &modelv1.TagValue_Null{}}
}

// columnValueToFieldValue is the field-side counterpart. Passthrough columns
// for FieldValue return the source pointer directly; typed columns
// reconstruct the protobuf wrapper with the same defensive-copy rule for
// BinaryData.
func columnValueToFieldValue(col vectorized.Column, rowIdx int) *modelv1.FieldValue {
	if pc, ok := col.(*vectorized.TypedColumn[*modelv1.FieldValue]); ok {
		v := pc.Data()[rowIdx]
		if v == nil {
			return pbv1NullFieldValueRef
		}
		return v
	}
	if col.IsNull(rowIdx) {
		return &modelv1.FieldValue{Value: &modelv1.FieldValue_Null{}}
	}
	switch c := col.(type) {
	case *vectorized.TypedColumn[int64]:
		return &modelv1.FieldValue{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: c.Data()[rowIdx]}}}
	case *vectorized.TypedColumn[float64]:
		return &modelv1.FieldValue{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: c.Data()[rowIdx]}}}
	case *vectorized.TypedColumn[string]:
		return &modelv1.FieldValue{Value: &modelv1.FieldValue_Str{Str: &modelv1.Str{Value: c.Data()[rowIdx]}}}
	case *vectorized.TypedColumn[[]byte]:
		src := c.Data()[rowIdx]
		buf := make([]byte, len(src))
		copy(buf, src)
		return &modelv1.FieldValue{Value: &modelv1.FieldValue_BinaryData{BinaryData: buf}}
	}
	return &modelv1.FieldValue{Value: &modelv1.FieldValue_Null{}}
}
