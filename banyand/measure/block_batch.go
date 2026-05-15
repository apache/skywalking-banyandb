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
	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// G5b/G5c — typed-column emit path on blockCursor.
//
// copyAllToBatch and copyToBatch mirror copyAllTo / copyTo (block.go) but
// emit directly into typed vectorized.Column instances on a *MeasureBatch,
// bypassing the *modelv1.TagValue / *modelv1.FieldValue intermediate.
//
// copyAllTo remains untouched — the row path keeps producing
// *MeasureResult bytes-identically for legacy consumers (gRPC parity is
// the C1 invariant). The two paths exist side by side until G5e flips the
// default and the row path can be retired.
//
// Schema indexing contract: MeasureBatch.Tags[i] corresponds to the i-th
// RoleTag entry in schema.Columns (in declaration order); Fields[i]
// likewise. copyAllToBatch maintains a tagIdx / fieldIdx pair walking the
// schema in order. The schema and batch must agree on row count: every
// column in Tags and Fields must already be allocated (e.g. via
// newMeasureBatchForSchema) before the first copyAllToBatch / copyToBatch
// call so this function only appends rows.

// newMeasureBatchForSchema allocates an empty *MeasureBatch with all
// per-column TypedColumn instances pre-created at the requested capacity.
// Tag/Field column types are taken from the schema's ColumnDef entries —
// callers can mix passthrough and native types within a single batch.
func newMeasureBatchForSchema(schema *vectorized.BatchSchema, capacity int) *model.MeasureBatch {
	b := &model.MeasureBatch{Schema: schema}
	if capacity > 0 {
		b.Timestamps = make([]int64, 0, capacity)
		b.Versions = make([]int64, 0, capacity)
		b.ShardIDs = make([]common.ShardID, 0, capacity)
		b.SeriesIDs = make([]common.SeriesID, 0, capacity)
	}
	for _, def := range schema.Columns {
		switch def.Role {
		case vectorized.RoleTag:
			b.Tags = append(b.Tags, vectorized.NewColumnForType(def.Type, capacity))
		case vectorized.RoleField:
			b.Fields = append(b.Fields, vectorized.NewColumnForType(def.Type, capacity))
		case vectorized.RoleTimestamp, vectorized.RoleVersion,
			vectorized.RoleSeriesID, vectorized.RoleShardID:
			// Metadata roles use the parallel slices on the batch.
		}
	}
	return b
}

// copyAllToBatch is the multi-row counterpart of copyAllTo. It appends every
// active row from the cursor (idx..len for ascending; 0..idx+1 for desc)
// into b's parallel slices and typed columns.
//
// Decoding strategy depends on the column type declared by the schema:
//   - RoleTag with ColumnTypeTagValue: passthrough — decode each cell once
//     via mustDecodeTagValue and Append the *modelv1.TagValue pointer.
//     Functionally identical to copyAllTo's row-path output.
//   - RoleTag with native types (Int64 / String / Bytes / Int64Array /
//     StrArray): decode bytes directly via appendDecodedTagBytesAsTyped,
//     skipping the protobuf wrapper. Stored-index substitutions (hidden
//     tag projection) project the *modelv1.TagValue onto the typed cell
//     via appendTagValueAsTyped.
//   - RoleField follows the same pattern with the field-side decoders.
//
// Missing tag families and missing tags are null-filled the same way
// copyAllTo emits pbv1.NullTagValue: AppendNull on a typed column or
// Append(pbv1.NullTagValue) on a passthrough column.
func (bc *blockCursor) copyAllToBatch(b *model.MeasureBatch, schema *vectorized.BatchSchema,
	storedIndexValue map[common.SeriesID]map[string]*modelv1.TagValue, desc bool,
) {
	var idx, offset int
	if desc {
		idx = 0
		offset = bc.idx + 1
	} else {
		idx = bc.idx
		offset = len(bc.timestamps)
	}
	if offset <= idx {
		return
	}
	size := offset - idx

	sid := bc.bm.seriesID

	// Append metadata. desc reverses the slice via append-in-reverse.
	if desc {
		for i := offset - 1; i >= idx; i-- {
			b.Timestamps = append(b.Timestamps, bc.timestamps[i])
			b.Versions = append(b.Versions, bc.versions[i])
			b.ShardIDs = append(b.ShardIDs, bc.shardID)
			b.SeriesIDs = append(b.SeriesIDs, sid)
		}
	} else {
		b.Timestamps = append(b.Timestamps, bc.timestamps[idx:offset]...)
		b.Versions = append(b.Versions, bc.versions[idx:offset]...)
		for range size {
			b.ShardIDs = append(b.ShardIDs, bc.shardID)
			b.SeriesIDs = append(b.SeriesIDs, sid)
		}
	}

	var indexValue map[string]*modelv1.TagValue
	if storedIndexValue != nil {
		indexValue = storedIndexValue[sid]
	}

	tagIdx, fieldIdx := 0, 0
	for _, def := range schema.Columns {
		switch def.Role {
		case vectorized.RoleTag:
			col := b.Tags[tagIdx]
			tagIdx++
			bc.fillTagColumnAllRows(col, def, idx, offset, indexValue, desc)
		case vectorized.RoleField:
			col := b.Fields[fieldIdx]
			fieldIdx++
			bc.fillFieldColumnAllRows(col, def, idx, offset, desc)
		case vectorized.RoleTimestamp, vectorized.RoleVersion,
			vectorized.RoleSeriesID, vectorized.RoleShardID:
			// Metadata handled above.
		}
	}
}

// fillTagColumnAllRows appends rows [idx, offset) of the tag described by
// def onto col. desc reverses the iteration direction.
//
// Resolution order matches copyAllTo:
//  1. If indexValue has the tag, project that pre-decoded value into col
//     for every row. (Hidden tag projection — the index lookup already
//     produced a *modelv1.TagValue.)
//  2. Find the columnFamily for def.TagFamily in bc.tagFamilies; if not
//     found, null-fill.
//  3. Within the family, find the column for def.Name; if not found,
//     null-fill.
//  4. Validate the column's stored valueType matches the projection's
//     schemaType. On mismatch, null-fill (same defensive behavior as
//     copyAllTo's hasSchemaType check).
//  5. Decode each stored byte slice into the typed column.
func (bc *blockCursor) fillTagColumnAllRows(col vectorized.Column, def vectorized.ColumnDef,
	idx, offset int, indexValue map[string]*modelv1.TagValue, desc bool,
) {
	size := offset - idx
	// Hidden tag substitution.
	if indexValue != nil && indexValue[def.Name] != nil {
		v := indexValue[def.Name]
		for range size {
			appendTagValueAt(col, def, v)
		}
		return
	}
	cf := bc.findTagFamily(def.TagFamily)
	if cf == nil {
		appendNullTagN(col, def, size)
		return
	}
	column := cf.findColumn(def.Name)
	if column == nil {
		appendNullTagN(col, def, size)
		return
	}
	schemaType, hasSchemaType := bc.schemaTagTypes[def.Name]
	if !hasSchemaType || column.valueType != schemaType {
		appendNullTagN(col, def, size)
		return
	}
	if desc {
		for i := offset - 1; i >= idx; i-- {
			fillTagCell(col, def, column.valueType, column.values[i])
		}
	} else {
		for i := idx; i < offset; i++ {
			fillTagCell(col, def, column.valueType, column.values[i])
		}
	}
}

// fillFieldColumnAllRows mirrors fillTagColumnAllRows but on the field side.
// There is no indexValue path for fields and no schemaType check (fields
// don't have hidden-tag substitutions).
func (bc *blockCursor) fillFieldColumnAllRows(col vectorized.Column, def vectorized.ColumnDef,
	idx, offset int, desc bool,
) {
	size := offset - idx
	column := bc.findFieldColumn(def.Name)
	if column == nil {
		appendNullFieldN(col, def, size)
		return
	}
	if desc {
		for i := offset - 1; i >= idx; i-- {
			fillFieldCell(col, def, column.valueType, column.values[i])
		}
	} else {
		for i := idx; i < offset; i++ {
			fillFieldCell(col, def, column.valueType, column.values[i])
		}
	}
}

// fillTagCell decodes one stored cell of valueType into col, dispatching on
// def.Type so passthrough columns (ColumnTypeTagValue) keep using
// mustDecodeTagValue while native columns go through the byte-to-typed
// fast path.
func fillTagCell(col vectorized.Column, def vectorized.ColumnDef, valueType pbv1.ValueType, raw []byte) {
	if def.Type == vectorized.ColumnTypeTagValue {
		c := col.(*vectorized.TypedColumn[*modelv1.TagValue])
		c.Append(mustDecodeTagValue(valueType, raw))
		return
	}
	appendDecodedTagBytesAsTyped(col, valueType, raw)
}

// fillFieldCell is the field-side counterpart.
func fillFieldCell(col vectorized.Column, def vectorized.ColumnDef, valueType pbv1.ValueType, raw []byte) {
	if def.Type == vectorized.ColumnTypeFieldValue {
		c := col.(*vectorized.TypedColumn[*modelv1.FieldValue])
		c.Append(mustDecodeFieldValue(valueType, raw))
		return
	}
	appendDecodedFieldBytesAsTyped(col, valueType, raw)
}

// copyToBatch appends the single row at bc.idx into b's parallel slices and
// typed columns. It is the batch-path mirror of blockCursor.copyTo (block.go)
// and is used by mergeBatch when emitting a fresh (non-duplicate) row.
//
// Schema/column indexing contract matches copyAllToBatch: Tags[tagIdx]
// aligns with the tagIdx-th RoleTag entry in schema.Columns, Fields similarly.
func (bc *blockCursor) copyToBatch(b *model.MeasureBatch, schema *vectorized.BatchSchema,
	storedIndexValue map[common.SeriesID]map[string]*modelv1.TagValue,
) {
	sid := bc.bm.seriesID
	b.Timestamps = append(b.Timestamps, bc.timestamps[bc.idx])
	b.Versions = append(b.Versions, bc.versions[bc.idx])
	b.ShardIDs = append(b.ShardIDs, bc.shardID)
	b.SeriesIDs = append(b.SeriesIDs, sid)

	var indexValue map[string]*modelv1.TagValue
	if storedIndexValue != nil {
		indexValue = storedIndexValue[sid]
	}

	tagIdx, fieldIdx := 0, 0
	for _, def := range schema.Columns {
		switch def.Role {
		case vectorized.RoleTag:
			col := b.Tags[tagIdx]
			tagIdx++
			bc.fillTagCell(col, def, bc.idx, indexValue)
		case vectorized.RoleField:
			col := b.Fields[fieldIdx]
			fieldIdx++
			bc.fillFieldCell(col, def, bc.idx)
		case vectorized.RoleTimestamp, vectorized.RoleVersion,
			vectorized.RoleSeriesID, vectorized.RoleShardID:
			// Metadata handled via the parallel slices above.
		}
	}
}

// fillTagCell appends a single tag cell at row rowIdx from bc into col.
func (bc *blockCursor) fillTagCell(col vectorized.Column, def vectorized.ColumnDef,
	rowIdx int, indexValue map[string]*modelv1.TagValue,
) {
	if indexValue != nil && indexValue[def.Name] != nil {
		appendTagValueAt(col, def, indexValue[def.Name])
		return
	}
	cf := bc.findTagFamily(def.TagFamily)
	if cf == nil {
		appendNullTagN(col, def, 1)
		return
	}
	column := cf.findColumn(def.Name)
	if column == nil {
		appendNullTagN(col, def, 1)
		return
	}
	schemaType, hasSchemaType := bc.schemaTagTypes[def.Name]
	if !hasSchemaType || column.valueType != schemaType {
		appendNullTagN(col, def, 1)
		return
	}
	fillTagCell(col, def, column.valueType, column.values[rowIdx])
}

// fillFieldCell appends a single field cell at row rowIdx from bc into col.
func (bc *blockCursor) fillFieldCell(col vectorized.Column, def vectorized.ColumnDef, rowIdx int) {
	column := bc.findFieldColumn(def.Name)
	if column == nil {
		appendNullFieldN(col, def, 1)
		return
	}
	fillFieldCell(col, def, column.valueType, column.values[rowIdx])
}

// replaceInBatch overwrites the LAST row of b (at index lastRow) with the
// data from bc.idx. It is the batch-path mirror of blockCursor.replace
// (block.go) and is called by mergeBatch when a duplicate timestamp with a
// newer version is encountered.
//
// The parallel metadata slices are updated in-place; typed column cells are
// overwritten via setTagCellAt / setFieldCellAt.
func (bc *blockCursor) replaceInBatch(b *model.MeasureBatch, schema *vectorized.BatchSchema,
	storedIndexValue map[common.SeriesID]map[string]*modelv1.TagValue,
) {
	sid := bc.bm.seriesID
	lastRow := len(b.Timestamps) - 1
	b.Versions[lastRow] = bc.versions[bc.idx]

	var indexValue map[string]*modelv1.TagValue
	if storedIndexValue != nil {
		indexValue = storedIndexValue[sid]
	}

	tagIdx, fieldIdx := 0, 0
	for _, def := range schema.Columns {
		switch def.Role {
		case vectorized.RoleTag:
			col := b.Tags[tagIdx]
			tagIdx++
			bc.setTagCellAt(col, def, bc.idx, lastRow, indexValue)
		case vectorized.RoleField:
			col := b.Fields[fieldIdx]
			fieldIdx++
			bc.setFieldCellAt(col, def, bc.idx, lastRow)
		case vectorized.RoleTimestamp, vectorized.RoleVersion,
			vectorized.RoleSeriesID, vectorized.RoleShardID:
			// Metadata handled above.
		}
	}
}

// setTagCellAt overwrites column position destRow with the tag value decoded
// from bc at srcRow.
func (bc *blockCursor) setTagCellAt(col vectorized.Column, def vectorized.ColumnDef,
	srcRow, destRow int, indexValue map[string]*modelv1.TagValue,
) {
	if indexValue != nil && indexValue[def.Name] != nil {
		setTagValueAt(col, def, destRow, indexValue[def.Name])
		return
	}
	cf := bc.findTagFamily(def.TagFamily)
	if cf == nil {
		setNullTagAt(col, def, destRow)
		return
	}
	column := cf.findColumn(def.Name)
	if column == nil {
		setNullTagAt(col, def, destRow)
		return
	}
	schemaType, hasSchemaType := bc.schemaTagTypes[def.Name]
	if !hasSchemaType || column.valueType != schemaType {
		setNullTagAt(col, def, destRow)
		return
	}
	setTagCellAt(col, def, column.valueType, column.values[srcRow], destRow)
}

// setFieldCellAt overwrites column position destRow with the field value
// decoded from bc at srcRow.
func (bc *blockCursor) setFieldCellAt(col vectorized.Column, def vectorized.ColumnDef, srcRow, destRow int) {
	column := bc.findFieldColumn(def.Name)
	if column == nil {
		setNullFieldAt(col, def, destRow)
		return
	}
	setFieldCellAt(col, def, column.valueType, column.values[srcRow], destRow)
}

// setTagCellAt overwrites one cell in col at destRow. Mirrors fillTagCell but
// uses SetAt instead of Append.
func setTagCellAt(col vectorized.Column, def vectorized.ColumnDef, valueType pbv1.ValueType, raw []byte, destRow int) {
	if def.Type == vectorized.ColumnTypeTagValue {
		c := col.(*vectorized.TypedColumn[*modelv1.TagValue])
		c.SetAt(destRow, mustDecodeTagValue(valueType, raw))
		return
	}
	setDecodedTagBytesAt(col, valueType, raw, destRow)
}

// setFieldCellAt overwrites one field cell in col at destRow.
func setFieldCellAt(col vectorized.Column, def vectorized.ColumnDef, valueType pbv1.ValueType, raw []byte, destRow int) {
	if def.Type == vectorized.ColumnTypeFieldValue {
		c := col.(*vectorized.TypedColumn[*modelv1.FieldValue])
		c.SetAt(destRow, mustDecodeFieldValue(valueType, raw))
		return
	}
	setDecodedFieldBytesAt(col, valueType, raw, destRow)
}

// setTagValueAt overwrites one passthrough/native tag cell with a pre-decoded
// *modelv1.TagValue, dispatching on def.Type.
func setTagValueAt(col vectorized.Column, def vectorized.ColumnDef, destRow int, v *modelv1.TagValue) {
	if def.Type == vectorized.ColumnTypeTagValue {
		c := col.(*vectorized.TypedColumn[*modelv1.TagValue])
		c.SetAt(destRow, v)
		return
	}
	setTagValueTypedAt(col, v, destRow)
}

// setNullTagAt marks destRow null in a tag column.
func setNullTagAt(col vectorized.Column, def vectorized.ColumnDef, destRow int) {
	if def.Type == vectorized.ColumnTypeTagValue {
		c := col.(*vectorized.TypedColumn[*modelv1.TagValue])
		c.SetAt(destRow, pbv1.NullTagValue)
		return
	}
	col.MarkNullAt(destRow)
}

// setNullFieldAt marks destRow null in a field column.
func setNullFieldAt(col vectorized.Column, def vectorized.ColumnDef, destRow int) {
	if def.Type == vectorized.ColumnTypeFieldValue {
		c := col.(*vectorized.TypedColumn[*modelv1.FieldValue])
		c.SetAt(destRow, pbv1.NullFieldValue)
		return
	}
	col.MarkNullAt(destRow)
}

// setDecodedTagBytesAt overwrites a native typed tag column at destRow by
// decoding raw bytes of the given valueType.
func setDecodedTagBytesAt(col vectorized.Column, valueType pbv1.ValueType, raw []byte, destRow int) {
	if raw == nil {
		col.MarkNullAt(destRow)
		return
	}
	switch valueType {
	case pbv1.ValueTypeInt64:
		col.(*vectorized.TypedColumn[int64]).SetAt(destRow, convert.BytesToInt64(raw))
	case pbv1.ValueTypeStr:
		col.(*vectorized.TypedColumn[string]).SetAt(destRow, string(raw))
	case pbv1.ValueTypeBinaryData:
		buf := make([]byte, len(raw))
		copy(buf, raw)
		col.(*vectorized.TypedColumn[[]byte]).SetAt(destRow, buf)
	case pbv1.ValueTypeInt64Arr:
		var values []int64
		for i := 0; i < len(raw); i += 8 {
			values = append(values, convert.BytesToInt64(raw[i:i+8]))
		}
		col.(*vectorized.TypedColumn[[]int64]).SetAt(destRow, values)
	case pbv1.ValueTypeStrArr:
		bb := bigValuePool.Generate()
		var values []string
		buf := raw
		var unmarshalErr error
		for len(buf) > 0 {
			bb.Buf, buf, unmarshalErr = unmarshalVarArray(bb.Buf[:0], buf)
			if unmarshalErr != nil {
				logger.Panicf("setDecodedTagBytesAt unmarshalVarArray failed: %v", unmarshalErr)
			}
			values = append(values, string(bb.Buf))
		}
		col.(*vectorized.TypedColumn[[]string]).SetAt(destRow, values)
	default:
		col.MarkNullAt(destRow)
	}
}

// setDecodedFieldBytesAt overwrites a native typed field column at destRow.
func setDecodedFieldBytesAt(col vectorized.Column, valueType pbv1.ValueType, raw []byte, destRow int) {
	if raw == nil {
		switch valueType {
		case pbv1.ValueTypeStr:
			col.(*vectorized.TypedColumn[string]).SetAt(destRow, "")
		case pbv1.ValueTypeBinaryData:
			col.(*vectorized.TypedColumn[[]byte]).SetAt(destRow, []byte{})
		default:
			col.MarkNullAt(destRow)
		}
		return
	}
	switch valueType {
	case pbv1.ValueTypeInt64:
		col.(*vectorized.TypedColumn[int64]).SetAt(destRow, convert.BytesToInt64(raw))
	case pbv1.ValueTypeFloat64:
		col.(*vectorized.TypedColumn[float64]).SetAt(destRow, convert.BytesToFloat64(raw))
	case pbv1.ValueTypeStr:
		col.(*vectorized.TypedColumn[string]).SetAt(destRow, string(raw))
	case pbv1.ValueTypeBinaryData:
		buf := make([]byte, len(raw))
		copy(buf, raw)
		col.(*vectorized.TypedColumn[[]byte]).SetAt(destRow, buf)
	default:
		col.MarkNullAt(destRow)
	}
}

// setTagValueTypedAt projects a pre-decoded *modelv1.TagValue onto a native
// typed column at destRow.
func setTagValueTypedAt(col vectorized.Column, v *modelv1.TagValue, destRow int) {
	if v == nil {
		col.MarkNullAt(destRow)
		return
	}
	switch x := v.Value.(type) {
	case *modelv1.TagValue_Null:
		col.MarkNullAt(destRow)
	case *modelv1.TagValue_Int:
		if c, ok := col.(*vectorized.TypedColumn[int64]); ok {
			c.SetAt(destRow, x.Int.GetValue())
			return
		}
		col.MarkNullAt(destRow)
	case *modelv1.TagValue_Str:
		if c, ok := col.(*vectorized.TypedColumn[string]); ok {
			c.SetAt(destRow, x.Str.GetValue())
			return
		}
		col.MarkNullAt(destRow)
	case *modelv1.TagValue_BinaryData:
		if c, ok := col.(*vectorized.TypedColumn[[]byte]); ok {
			buf := make([]byte, len(x.BinaryData))
			copy(buf, x.BinaryData)
			c.SetAt(destRow, buf)
			return
		}
		col.MarkNullAt(destRow)
	case *modelv1.TagValue_IntArray:
		if c, ok := col.(*vectorized.TypedColumn[[]int64]); ok {
			out := make([]int64, len(x.IntArray.GetValue()))
			copy(out, x.IntArray.GetValue())
			c.SetAt(destRow, out)
			return
		}
		col.MarkNullAt(destRow)
	case *modelv1.TagValue_StrArray:
		if c, ok := col.(*vectorized.TypedColumn[[]string]); ok {
			out := make([]string, len(x.StrArray.GetValue()))
			copy(out, x.StrArray.GetValue())
			c.SetAt(destRow, out)
			return
		}
		col.MarkNullAt(destRow)
	default:
		col.MarkNullAt(destRow)
	}
}

// appendTagValueAt projects a pre-decoded *modelv1.TagValue onto col,
// dispatching on def.Type.
func appendTagValueAt(col vectorized.Column, def vectorized.ColumnDef, v *modelv1.TagValue) {
	if def.Type == vectorized.ColumnTypeTagValue {
		c := col.(*vectorized.TypedColumn[*modelv1.TagValue])
		c.Append(v)
		return
	}
	appendTagValueAsTyped(col, v)
}

// appendNullTagN appends n null cells to a tag column. Passthrough columns
// store the pbv1.NullTagValue singleton; native columns use AppendNull
// (which marks the validity bit and stores the zero value).
func appendNullTagN(col vectorized.Column, def vectorized.ColumnDef, n int) {
	if def.Type == vectorized.ColumnTypeTagValue {
		c := col.(*vectorized.TypedColumn[*modelv1.TagValue])
		for range n {
			c.Append(pbv1.NullTagValue)
		}
		return
	}
	for range n {
		col.AppendNull()
	}
}

// appendNullFieldN is the field-side counterpart.
func appendNullFieldN(col vectorized.Column, def vectorized.ColumnDef, n int) {
	if def.Type == vectorized.ColumnTypeFieldValue {
		c := col.(*vectorized.TypedColumn[*modelv1.FieldValue])
		for range n {
			c.Append(pbv1.NullFieldValue)
		}
		return
	}
	for range n {
		col.AppendNull()
	}
}

// findTagFamily / findColumn are tiny helpers extracted so the row path
// (copyAllTo / copyTo) and the batch path share the same search logic.
// columnFamily and column are unexported types in this package — these
// methods live alongside copyAllToBatch and may be inlined in the future
// if profiling shows the call overhead matters.
func (bc *blockCursor) findTagFamily(family string) *columnFamily {
	for i := range bc.tagFamilies {
		if bc.tagFamilies[i].name == family {
			return &bc.tagFamilies[i]
		}
	}
	return nil
}

func (cf *columnFamily) findColumn(name string) *column {
	for i := range cf.columns {
		if cf.columns[i].name == name {
			return &cf.columns[i]
		}
	}
	return nil
}

func (bc *blockCursor) findFieldColumn(name string) *column {
	for i := range bc.fields.columns {
		if bc.fields.columns[i].name == name {
			return &bc.fields.columns[i]
		}
	}
	return nil
}
