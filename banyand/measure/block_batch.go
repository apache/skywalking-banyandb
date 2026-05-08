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
