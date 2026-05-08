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

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// BuildMeasureBatchFromResult converts a *model.MeasureResult produced by a
// row-shaped MeasureQueryResult.Pull() call into a *model.MeasureBatch whose
// columns match the supplied BatchSchema.
//
// This is the G5b "dual-emit" wrapper helper: the storage layer can
// implement MeasureBatchResult.PullBatch by calling its existing Pull() and
// passing the result through this converter. It preserves the existing
// row-path decode pipeline; the architectural decode-elimination is left to
// G5c/G5d (block_cursor native column emit).
//
// Schema-declared tag/field columns missing from the result are null-filled
// using pbv1.Null{Tag,Field}Value singletons — matching the multi-group
// projection behavior in fillTags / fillFields when one group's schema
// lacks a tag the other has.
//
// Returns (nil, nil) when r is nil. Returns (nil, err) when a length
// invariant is violated (a value slice is shorter than the timestamp count).
func BuildMeasureBatchFromResult(r *model.MeasureResult, schema *vectorized.BatchSchema) (*model.MeasureBatch, error) {
	if r == nil {
		return nil, nil
	}
	if schema == nil {
		return nil, fmt.Errorf("BuildMeasureBatchFromResult: nil schema")
	}
	n := len(r.Timestamps)

	timestamps := make([]int64, n)
	copy(timestamps, r.Timestamps)
	versions := make([]int64, n)
	if len(r.Versions) >= n {
		copy(versions, r.Versions[:n])
	}
	shardIDs := make([]common.ShardID, n)
	if len(r.ShardIDs) >= n {
		copy(shardIDs, r.ShardIDs[:n])
	}
	seriesIDs := make([]common.SeriesID, n)
	for i := range seriesIDs {
		seriesIDs[i] = r.SID
	}

	resultTags := make(map[string]*model.Tag)
	for i := range r.TagFamilies {
		tf := &r.TagFamilies[i]
		for j := range tf.Tags {
			tag := &tf.Tags[j]
			resultTags[tf.Name+"\x00"+tag.Name] = tag
		}
	}
	resultFields := make(map[string]*model.Field, len(r.Fields))
	for i := range r.Fields {
		resultFields[r.Fields[i].Name] = &r.Fields[i]
	}

	tagCols := make([]vectorized.Column, 0, len(schema.Columns))
	fieldCols := make([]vectorized.Column, 0, len(schema.Columns))
	for _, def := range schema.Columns {
		switch def.Role {
		case vectorized.RoleTag:
			tag := resultTags[def.TagFamily+"\x00"+def.Name]
			col, fillErr := buildTagColumn(def, tag, n)
			if fillErr != nil {
				return nil, fillErr
			}
			tagCols = append(tagCols, col)
		case vectorized.RoleField:
			fld := resultFields[def.Name]
			col, fillErr := buildFieldColumn(def, fld, n)
			if fillErr != nil {
				return nil, fillErr
			}
			fieldCols = append(fieldCols, col)
		case vectorized.RoleTimestamp, vectorized.RoleVersion,
			vectorized.RoleSeriesID, vectorized.RoleShardID:
			// Metadata roles are populated via the parallel slices on the
			// MeasureBatch itself; no per-column entry is needed.
		}
	}

	return &model.MeasureBatch{
		Schema:     schema,
		Timestamps: timestamps,
		Versions:   versions,
		ShardIDs:   shardIDs,
		SeriesIDs:  seriesIDs,
		Tags:       tagCols,
		Fields:     fieldCols,
	}, nil
}

// buildTagColumn allocates the typed Column corresponding to def and fills
// it from tag (or null-fills it when tag is nil — the multi-group "schema
// declares but result lacks" case). Dispatches on def.Type so the same
// helper handles passthrough columns (G5a, ColumnTypeTagValue) and native
// typed columns (G5c+, Int64 / String / Bytes / Int64Array / StrArray).
//
// Length-invariant violations surface as errors rather than truncation;
// callers see a clean failure path during dual-emit.
func buildTagColumn(def vectorized.ColumnDef, tag *model.Tag, n int) (vectorized.Column, error) {
	if def.Type == vectorized.ColumnTypeTagValue {
		col := vectorized.NewTagValueColumn(n)
		if tag == nil {
			for range n {
				col.Append(pbv1.NullTagValue)
			}
			return col, nil
		}
		if len(tag.Values) < n {
			return nil, fmt.Errorf("BuildMeasureBatchFromResult: tag %s.%s has %d values, expected %d",
				def.TagFamily, def.Name, len(tag.Values), n)
		}
		for k := range n {
			col.Append(tag.Values[k])
		}
		return col, nil
	}
	col := vectorized.NewColumnForType(def.Type, n)
	if tag == nil {
		for range n {
			appendTagValueAsTyped(col, pbv1.NullTagValue)
		}
		return col, nil
	}
	if len(tag.Values) < n {
		return nil, fmt.Errorf("BuildMeasureBatchFromResult: tag %s.%s has %d values, expected %d",
			def.TagFamily, def.Name, len(tag.Values), n)
	}
	for k := range n {
		appendTagValueAsTyped(col, tag.Values[k])
	}
	return col, nil
}

// buildFieldColumn is the field-side counterpart of buildTagColumn.
func buildFieldColumn(def vectorized.ColumnDef, fld *model.Field, n int) (vectorized.Column, error) {
	if def.Type == vectorized.ColumnTypeFieldValue {
		col := vectorized.NewFieldValueColumn(n)
		if fld == nil {
			for range n {
				col.Append(pbv1.NullFieldValue)
			}
			return col, nil
		}
		if len(fld.Values) < n {
			return nil, fmt.Errorf("BuildMeasureBatchFromResult: field %s has %d values, expected %d",
				def.Name, len(fld.Values), n)
		}
		for k := range n {
			col.Append(fld.Values[k])
		}
		return col, nil
	}
	col := vectorized.NewColumnForType(def.Type, n)
	if fld == nil {
		for range n {
			appendFieldValueAsTyped(col, pbv1.NullFieldValue)
		}
		return col, nil
	}
	if len(fld.Values) < n {
		return nil, fmt.Errorf("BuildMeasureBatchFromResult: field %s has %d values, expected %d",
			def.Name, len(fld.Values), n)
	}
	for k := range n {
		appendFieldValueAsTyped(col, fld.Values[k])
	}
	return col, nil
}

// appendTagValueAsTyped projects a pre-decoded *modelv1.TagValue onto a
// native typed column. On a oneof / column-type mismatch the column
// receives an AppendNull rather than panicking, matching the existing
// copyAllTo behavior of substituting NullTagValue for unsatisfied
// projections in heterogeneous multi-group results.
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

// appendFieldValueAsTyped is the field-side counterpart of
// appendTagValueAsTyped. FieldValue has fewer variants (no Int64Array /
// StrArray) — the rest mirror.
func appendFieldValueAsTyped(col vectorized.Column, v *modelv1.FieldValue) {
	if v == nil {
		col.AppendNull()
		return
	}
	switch x := v.Value.(type) {
	case *modelv1.FieldValue_Null:
		col.AppendNull()
	case *modelv1.FieldValue_Int:
		if c, ok := col.(*vectorized.TypedColumn[int64]); ok {
			c.Append(x.Int.GetValue())
			return
		}
		col.AppendNull()
	case *modelv1.FieldValue_Float:
		if c, ok := col.(*vectorized.TypedColumn[float64]); ok {
			c.Append(x.Float.GetValue())
			return
		}
		col.AppendNull()
	case *modelv1.FieldValue_Str:
		if c, ok := col.(*vectorized.TypedColumn[string]); ok {
			c.Append(x.Str.GetValue())
			return
		}
		col.AppendNull()
	case *modelv1.FieldValue_BinaryData:
		if c, ok := col.(*vectorized.TypedColumn[[]byte]); ok {
			buf := make([]byte, len(x.BinaryData))
			copy(buf, x.BinaryData)
			c.Append(buf)
			return
		}
		col.AppendNull()
	default:
		col.AppendNull()
	}
}
