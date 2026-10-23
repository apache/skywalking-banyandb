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
	"strings"
	"sync/atomic"
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
	serializedRows.Add(int64(b.ActiveLen()))
	if dst == nil {
		dst = make([]*measurev1.InternalDataPoint, 0, b.ActiveLen())
	}
	schema := b.Schema
	shardIdx := schema.ShardIDIndex()
	// Fast path: BatchScan / BatchSourceFromBatchResult emit
	// nil-Selection batches on the scan hot path. Iterating Len directly
	// avoids the per-batch []uint16 materialization activeIndices would
	// otherwise allocate.
	if b.Selection == nil {
		for rowIdx := 0; rowIdx < b.Len; rowIdx++ {
			dst = append(dst, makeInternalDataPoint(b, schema, rowIdx, shardIdx))
		}
		return dst
	}
	for _, rowIdx := range b.Selection {
		dst = append(dst, makeInternalDataPoint(b, schema, int(rowIdx), shardIdx))
	}
	return dst
}

func makeInternalDataPoint(b *vectorized.RecordBatch, schema *vectorized.BatchSchema,
	rowIdx, shardIdx int,
) *measurev1.InternalDataPoint {
	idp := &measurev1.InternalDataPoint{DataPoint: buildDataPoint(b, schema, rowIdx)}
	if shardIdx >= 0 {
		idp.ShardId = uint32(b.Columns[shardIdx].(*vectorized.TypedColumn[int64]).Data()[rowIdx])
	}
	return idp
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
		// One slab per row for the families and tags keeps the per-cell
		// allocation count down: the Tag / TagFamily structs and their pointer
		// slices are carved from three shared backing arrays.
		var totalTags int
		for _, group := range schema.TagFamilyGroups {
			totalTags += len(group.Columns)
		}
		famSlab := make([]modelv1.TagFamily, len(schema.TagFamilyGroups))
		tagSlab := make([]modelv1.Tag, totalTags)
		tagPtrs := make([]*modelv1.Tag, totalTags)
		dp.TagFamilies = make([]*modelv1.TagFamily, len(schema.TagFamilyGroups))
		tagPos := 0
		for g, group := range schema.TagFamilyGroups {
			tf := &famSlab[g]
			tf.Name = group.Family
			tf.Tags = tagPtrs[tagPos : tagPos : tagPos+len(group.Columns)]
			for _, colIdx := range group.Columns {
				tag := &tagSlab[tagPos]
				tag.Key = schema.Columns[colIdx].Name
				tag.Value = columnValueToTagValue(b.Columns[colIdx], rowIdx)
				tf.Tags = append(tf.Tags, tag)
				tagPos++
			}
			dp.TagFamilies[g] = tf
		}
	}
	if len(schema.FieldColumns) > 0 {
		fieldSlab := make([]measurev1.DataPoint_Field, len(schema.FieldColumns))
		dp.Fields = make([]*measurev1.DataPoint_Field, len(schema.FieldColumns))
		for i, colIdx := range schema.FieldColumns {
			f := &fieldSlab[i]
			f.Name = schema.Columns[colIdx].Name
			f.Value = columnValueToFieldValue(b.Columns[colIdx], rowIdx)
			dp.Fields[i] = f
		}
	}
	return dp
}

// Native cells are built as one struct per cell so the TagValue / FieldValue,
// its oneof wrapper and the inner scalar message share a single allocation.
type (
	intTagCell struct {
		tv modelv1.TagValue
		w  modelv1.TagValue_Int
		v  modelv1.Int
	}
	strTagCell struct {
		tv modelv1.TagValue
		w  modelv1.TagValue_Str
		v  modelv1.Str
	}
	binTagCell struct {
		w  modelv1.TagValue_BinaryData
		tv modelv1.TagValue
	}
	intArrTagCell struct {
		tv modelv1.TagValue
		w  modelv1.TagValue_IntArray
		v  modelv1.IntArray
	}
	strArrTagCell struct {
		tv modelv1.TagValue
		w  modelv1.TagValue_StrArray
		v  modelv1.StrArray
	}
	intFieldCell struct {
		fv modelv1.FieldValue
		w  modelv1.FieldValue_Int
		v  modelv1.Int
	}
	floatFieldCell struct {
		fv modelv1.FieldValue
		w  modelv1.FieldValue_Float
		v  modelv1.Float
	}
	strFieldCell struct {
		fv modelv1.FieldValue
		w  modelv1.FieldValue_Str
		v  modelv1.Str
	}
	binFieldCell struct {
		w  modelv1.FieldValue_BinaryData
		fv modelv1.FieldValue
	}
)

// columnValueToTagValue materializes a *modelv1.TagValue from one row of col.
//
// Passthrough columns (TypedColumn[*modelv1.TagValue]) take a fast path:
// the original protobuf pointer from the scan source is returned directly
// — zero allocation, byte-identical to what the row path emits.
//
// Typed columns reconstruct the protobuf wrapper. Slice-typed values
// (BinaryData, IntArray, StrArray) are defensively copied so the produced
// TagValue does not alias the column's backing slice across pool reuse, and
// string payloads are cloned because a string cell may borrow storage memory
// that is recycled when the batch's owners are released, while the TagValue is
// marshaled only later.
func columnValueToTagValue(col vectorized.Column, rowIdx int) *modelv1.TagValue {
	if pc, ok := col.(*vectorized.TypedColumn[*modelv1.TagValue]); ok {
		v := pc.Data()[rowIdx]
		if v == nil {
			return pbv1NullTagValueRef
		}
		return v
	}
	if col.IsNull(rowIdx) {
		return pbv1NullTagValueRef
	}
	switch c := col.(type) {
	case *vectorized.ConstColumn:
		return constToTagValue(c)
	case *vectorized.TypedColumn[int64]:
		cell := &intTagCell{}
		cell.v.Value = c.Data()[rowIdx]
		cell.w.Int = &cell.v
		cell.tv.Value = &cell.w
		return &cell.tv
	case *vectorized.TypedColumn[string]:
		cell := &strTagCell{}
		cell.v.Value = strings.Clone(c.Data()[rowIdx])
		cell.w.Str = &cell.v
		cell.tv.Value = &cell.w
		return &cell.tv
	case *vectorized.TypedColumn[[]byte]:
		src := c.Data()[rowIdx]
		buf := make([]byte, len(src))
		copy(buf, src)
		cell := &binTagCell{}
		cell.w.BinaryData = buf
		cell.tv.Value = &cell.w
		return &cell.tv
	case *vectorized.TypedColumn[[]int64]:
		cell := &intArrTagCell{}
		cell.v.Value = slices.Clone(c.Data()[rowIdx])
		cell.w.IntArray = &cell.v
		cell.tv.Value = &cell.w
		return &cell.tv
	case *vectorized.TypedColumn[[]string]:
		cell := &strArrTagCell{}
		cell.v.Value = cloneStrings(c.Data()[rowIdx])
		cell.w.StrArray = &cell.v
		cell.tv.Value = &cell.w
		return &cell.tv
	}
	return pbv1NullTagValueRef
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
		return pbv1NullFieldValueRef
	}
	switch c := col.(type) {
	case *vectorized.ConstColumn:
		return constToFieldValue(c)
	case *vectorized.TypedColumn[int64]:
		cell := &intFieldCell{}
		cell.v.Value = c.Data()[rowIdx]
		cell.w.Int = &cell.v
		cell.fv.Value = &cell.w
		return &cell.fv
	case *vectorized.TypedColumn[float64]:
		cell := &floatFieldCell{}
		cell.v.Value = c.Data()[rowIdx]
		cell.w.Float = &cell.v
		cell.fv.Value = &cell.w
		return &cell.fv
	case *vectorized.TypedColumn[string]:
		cell := &strFieldCell{}
		cell.v.Value = strings.Clone(c.Data()[rowIdx])
		cell.w.Str = &cell.v
		cell.fv.Value = &cell.w
		return &cell.fv
	case *vectorized.TypedColumn[[]byte]:
		src := c.Data()[rowIdx]
		buf := make([]byte, len(src))
		copy(buf, src)
		cell := &binFieldCell{}
		cell.w.BinaryData = buf
		cell.fv.Value = &cell.w
		return &cell.fv
	}
	return pbv1NullFieldValueRef
}

// constToTagValue builds the TagValue of a non-null ConstColumn row, with the same
// ownership rules as the typed cases of columnValueToTagValue.
func constToTagValue(c *vectorized.ConstColumn) *modelv1.TagValue {
	switch c.Type() { //nolint:exhaustive // a ConstColumn only stands for these four types
	case vectorized.ColumnTypeInt64:
		cell := &intTagCell{}
		cell.v.Value = c.Int64()
		cell.w.Int = &cell.v
		cell.tv.Value = &cell.w
		return &cell.tv
	case vectorized.ColumnTypeString:
		cell := &strTagCell{}
		cell.v.Value = strings.Clone(c.Str())
		cell.w.Str = &cell.v
		cell.tv.Value = &cell.w
		return &cell.tv
	case vectorized.ColumnTypeBytes:
		cell := &binTagCell{}
		cell.w.BinaryData = copyBytes(c.Bytes())
		cell.tv.Value = &cell.w
		return &cell.tv
	default:
		return pbv1NullTagValueRef
	}
}

// constToFieldValue is constToTagValue for a field.
func constToFieldValue(c *vectorized.ConstColumn) *modelv1.FieldValue {
	switch c.Type() { //nolint:exhaustive // a ConstColumn only stands for these four types
	case vectorized.ColumnTypeInt64:
		cell := &intFieldCell{}
		cell.v.Value = c.Int64()
		cell.w.Int = &cell.v
		cell.fv.Value = &cell.w
		return &cell.fv
	case vectorized.ColumnTypeFloat64:
		cell := &floatFieldCell{}
		cell.v.Value = c.Float64()
		cell.w.Float = &cell.v
		cell.fv.Value = &cell.w
		return &cell.fv
	case vectorized.ColumnTypeString:
		cell := &strFieldCell{}
		cell.v.Value = strings.Clone(c.Str())
		cell.w.Str = &cell.v
		cell.fv.Value = &cell.w
		return &cell.fv
	case vectorized.ColumnTypeBytes:
		cell := &binFieldCell{}
		cell.w.BinaryData = copyBytes(c.Bytes())
		cell.fv.Value = &cell.w
		return &cell.fv
	default:
		return pbv1NullFieldValueRef
	}
}

// copyBytes returns a non-nil copy of src, as the typed bytes cases build it.
func copyBytes(src []byte) []byte {
	buf := make([]byte, len(src))
	copy(buf, src)
	return buf
}

// serializedRows counts the rows serialized into row-form InternalDataPoints by this process.
var serializedRows atomic.Int64

// SerializedRowCount returns how many rows this process has materialized as InternalDataPoints.
// Tests use its delta to check that a result is converted to rows only once, at egress.
func SerializedRowCount() int64 { return serializedRows.Load() }
