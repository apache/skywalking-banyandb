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
// grouped into TagFamilies by their TagFamily name; field columns become
// DataPoint_Field entries in schema order.
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
	tagFamilies := map[string]*modelv1.TagFamily{}
	for colIdx, def := range schema.Columns {
		switch def.Role {
		case vectorized.RoleTag:
			tf, exists := tagFamilies[def.TagFamily]
			if !exists {
				tf = &modelv1.TagFamily{Name: def.TagFamily}
				tagFamilies[def.TagFamily] = tf
				dp.TagFamilies = append(dp.TagFamilies, tf)
			}
			tf.Tags = append(tf.Tags, &modelv1.Tag{
				Key:   def.Name,
				Value: columnValueToTagValue(b.Columns[colIdx], rowIdx),
			})
		case vectorized.RoleField:
			dp.Fields = append(dp.Fields, &measurev1.DataPoint_Field{
				Name:  def.Name,
				Value: columnValueToFieldValue(b.Columns[colIdx], rowIdx),
			})
		case vectorized.RoleTimestamp, vectorized.RoleVersion,
			vectorized.RoleSeriesID, vectorized.RoleShardID:
			// Metadata roles are handled before the loop (Timestamp/Version/Sid)
			// or via the InternalDataPoint wrapper (ShardId). Skip here.
		}
	}
	return dp
}

func columnValueToTagValue(col vectorized.Column, rowIdx int) *modelv1.TagValue {
	if col.IsNull(rowIdx) {
		return &modelv1.TagValue{Value: &modelv1.TagValue_Null{}}
	}
	switch c := col.(type) {
	case *vectorized.TypedColumn[int64]:
		return &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: c.Data()[rowIdx]}}}
	case *vectorized.TypedColumn[string]:
		return &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: c.Data()[rowIdx]}}}
	case *vectorized.TypedColumn[[]byte]:
		return &modelv1.TagValue{Value: &modelv1.TagValue_BinaryData{BinaryData: c.Data()[rowIdx]}}
	case *vectorized.TypedColumn[[]int64]:
		return &modelv1.TagValue{Value: &modelv1.TagValue_IntArray{IntArray: &modelv1.IntArray{Value: c.Data()[rowIdx]}}}
	case *vectorized.TypedColumn[[]string]:
		return &modelv1.TagValue{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: c.Data()[rowIdx]}}}
	}
	return &modelv1.TagValue{Value: &modelv1.TagValue_Null{}}
}

func columnValueToFieldValue(col vectorized.Column, rowIdx int) *modelv1.FieldValue {
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
		return &modelv1.FieldValue{Value: &modelv1.FieldValue_BinaryData{BinaryData: c.Data()[rowIdx]}}
	}
	return &modelv1.FieldValue{Value: &modelv1.FieldValue_Null{}}
}
