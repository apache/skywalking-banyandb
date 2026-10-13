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

// newMeasureBatchForSchema returns a *MeasureBatch wired to schema with
// per-column TypedColumn instances pre-acquired at the requested capacity.
// Tag/Field column types are taken from the schema's ColumnDef entries —
// callers can mix passthrough and native types within a single batch.
// The batch and its columns come from the model.MeasureBatch pool; the
// consumer must call MeasureBatch.Release once finished with it.
func newMeasureBatchForSchema(schema *vectorized.BatchSchema, capacity int) *model.MeasureBatch {
	return model.AcquireMeasureBatch(schema, capacity)
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
			bc.fillTagColumnAllRows(b, col, def, idx, offset, indexValue, desc, false)
		case vectorized.RoleField:
			col := b.Fields[fieldIdx]
			fieldIdx++
			bc.fillFieldColumnAllRows(b, col, def, idx, offset, desc, false)
		case vectorized.RoleTimestamp, vectorized.RoleVersion,
			vectorized.RoleSeriesID, vectorized.RoleShardID,
			vectorized.RoleElementID, vectorized.RoleOrderKey:
			// Metadata handled above.
		}
	}
}

// lendBatch builds a batch that lends the whole decoded block instead of copying
// it: Timestamps, Versions and typed numeric columns are views of the cursor's
// slices, string, bytes and string-array cells borrow its decoded bytes, and the
// batch pins the cursor until it is released. Rows stay in storage order; a
// descending scan sets Selection to [n-1, ..., 0] rather than reversing columns.
// Hidden (index-substituted) tags, missing columns and columns with nulls are
// filled into owned columns row by row, as copyAllToBatch does.
//
// The caller keeps its own reference to bc and must not hand it a block of more
// than 65,536 rows when desc is set (a uint16 selection cannot address them).
func (bc *blockCursor) lendBatch(schema *vectorized.BatchSchema,
	storedIndexValue map[common.SeriesID]map[string]*modelv1.TagValue, desc bool,
) *model.MeasureBatch {
	size := len(bc.timestamps)
	b := model.AcquireBorrowingMeasureBatch(schema, size)
	b.BorrowMetadata(bc.timestamps, bc.versions)
	b.Owners.Pin(bc)
	sid := bc.bm.seriesID
	for range size {
		b.ShardIDs = append(b.ShardIDs, bc.shardID)
		b.SeriesIDs = append(b.SeriesIDs, sid)
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
			bc.fillTagColumnAllRows(b, col, def, 0, size, indexValue, false, true)
		case vectorized.RoleField:
			col := b.Fields[fieldIdx]
			fieldIdx++
			bc.fillFieldColumnAllRows(b, col, def, 0, size, false, true)
		case vectorized.RoleTimestamp, vectorized.RoleVersion,
			vectorized.RoleSeriesID, vectorized.RoleShardID,
			vectorized.RoleElementID, vectorized.RoleOrderKey:
			// Metadata handled above.
		}
	}
	if desc {
		b.Selection = make([]uint16, size)
		for i := range b.Selection {
			b.Selection[i] = uint16(size - 1 - i)
		}
	}
	return b
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
//  5. Decode each stored byte slice into the typed column. String, bytes and
//     string-array columns borrow the cursor's decoded bytes instead of copying
//     them (borrowTagRows); b then pins the cursor. When lend is set, a typed
//     numeric column is lent as a zero-copy view of the cursor's slice too
//     (lendNumericRows).
func (bc *blockCursor) fillTagColumnAllRows(b *model.MeasureBatch, col vectorized.Column, def vectorized.ColumnDef,
	idx, offset int, indexValue map[string]*modelv1.TagValue, desc, lend bool,
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
	if bc.borrowTagRows(b, col, def, column, idx, offset, desc) {
		return
	}
	if lend && !desc && bc.lendNumericRows(b, col, def, column, idx, offset) {
		return
	}
	fillTagRows(col, def, column, idx, offset, desc)
}

// fillFieldColumnAllRows mirrors fillTagColumnAllRows but on the field side.
// There is no indexValue path for fields and no schemaType check (fields
// don't have hidden-tag substitutions).
func (bc *blockCursor) fillFieldColumnAllRows(b *model.MeasureBatch, col vectorized.Column, def vectorized.ColumnDef,
	idx, offset int, desc, lend bool,
) {
	size := offset - idx
	column := bc.findFieldColumn(def.Name)
	if column == nil {
		appendNullFieldN(col, def, size)
		return
	}
	if lend && !desc && bc.lendNumericRows(b, col, def, column, idx, offset) {
		return
	}
	fillFieldRows(col, def, column, idx, offset, desc)
}

// lendNumericRows makes col a zero-copy view of rows [idx, offset) of a typed
// int64 or float64 column and pins bc on b so the slice stays valid until b is
// released. It returns false, having changed nothing, when it does not apply:
// the column is passthrough, not in typed form (it has nulls or was decoded to
// bytes) or of another value type, so the caller takes the copying path.
//
// The view is capacity-limited and col is marked borrowed (TypedColumn.Borrow),
// so neither an append nor a Reset can write into the cursor's slice.
func (bc *blockCursor) lendNumericRows(b *model.MeasureBatch, col vectorized.Column, def vectorized.ColumnDef,
	column *column, idx, offset int,
) bool {
	switch column.form {
	case numericFormInt64:
		if def.Type != vectorized.ColumnTypeInt64 {
			return false
		}
		c, ok := col.(*vectorized.TypedColumn[int64])
		if !ok || c.Len() != 0 {
			return false
		}
		c.Borrow(column.int64s[idx:offset])
	case numericFormFloat64:
		if def.Type != vectorized.ColumnTypeFloat64 {
			return false
		}
		c, ok := col.(*vectorized.TypedColumn[float64])
		if !ok || c.Len() != 0 {
			return false
		}
		c.Borrow(column.float64s[idx:offset])
	case numericFormBytes:
		return false
	}
	b.Owners.Pin(bc)
	return true
}

// fillTagRows appends rows [idx, offset) of column onto col, reversed when desc.
// A typed int64 column is appended in bulk; anything else goes cell by cell.
func fillTagRows(col vectorized.Column, def vectorized.ColumnDef, column *column, idx, offset int, desc bool) {
	if column.form == numericFormInt64 &&
		appendNumericRows(col, def, vectorized.ColumnTypeTagValue, column.int64s[idx:offset], desc, int64TagValue) {
		return
	}
	if desc {
		for i := offset - 1; i >= idx; i-- {
			fillTagCell(col, def, column.valueType, column.rowBytes(i))
		}
	} else {
		for i := idx; i < offset; i++ {
			fillTagCell(col, def, column.valueType, column.rowBytes(i))
		}
	}
}

// fillFieldRows is the field-side counterpart of fillTagRows.
func fillFieldRows(col vectorized.Column, def vectorized.ColumnDef, column *column, idx, offset int, desc bool) {
	switch column.form {
	case numericFormInt64:
		if appendNumericRows(col, def, vectorized.ColumnTypeFieldValue, column.int64s[idx:offset], desc, int64FieldValue) {
			return
		}
	case numericFormFloat64:
		if appendNumericRows(col, def, vectorized.ColumnTypeFieldValue, column.float64s[idx:offset], desc, float64FieldValue) {
			return
		}
	case numericFormBytes:
	}
	if desc {
		for i := offset - 1; i >= idx; i-- {
			fillFieldCell(col, def, column.valueType, column.rowBytes(i))
		}
	} else {
		for i := idx; i < offset; i++ {
			fillFieldCell(col, def, column.valueType, column.rowBytes(i))
		}
	}
}

// appendNumericRows copies src onto col, reversed when desc. A passthrough
// column of def.Type == passthroughType receives wrap(v) per row, matching
// mustDecodeTagValue/mustDecodeFieldValue; a native TypedColumn[T] receives the
// values themselves. It returns false, having appended nothing, when col is
// neither, so the caller can take the generic path that reports the mismatch.
func appendNumericRows[T int64 | float64, P any](col vectorized.Column, def vectorized.ColumnDef,
	passthroughType vectorized.ColumnType, src []T, desc bool, wrap func(T) P,
) bool {
	if def.Type == passthroughType {
		c, ok := col.(*vectorized.TypedColumn[P])
		if !ok {
			return false
		}
		if desc {
			for i := len(src) - 1; i >= 0; i-- {
				c.Append(wrap(src[i]))
			}
		} else {
			for _, v := range src {
				c.Append(wrap(v))
			}
		}
		return true
	}
	c, ok := col.(*vectorized.TypedColumn[T])
	if !ok {
		return false
	}
	if desc {
		for i := len(src) - 1; i >= 0; i-- {
			c.Append(src[i])
		}
	} else {
		c.AppendSlice(src)
	}
	return true
}

// borrowTagRows appends rows [idx, offset) of a string, bytes or string-array tag
// as zero-copy views over the cursor's decoded bytes, reversed when desc, and pins
// bc on b so the bytes stay valid until b is released. It returns false, having
// appended nothing, when it does not apply: the column is passthrough, typed
// numeric, or of another value type, so the caller takes the copying path.
//
// The views are valid because every value of a bytes-form column points into
// memory only this cursor writes and only its last release recycles: the decoder
// buffer for plain encoding, or a block-sized allocation the dictionary decoder
// makes for dictionary encoding (the pooled Dictionary holds just the headers).
// Bytes cells are capacity-limited so an append cannot write into a neighbor.
func (bc *blockCursor) borrowTagRows(b *model.MeasureBatch, col vectorized.Column, def vectorized.ColumnDef,
	column *column, idx, offset int, desc bool,
) bool {
	if def.Type == vectorized.ColumnTypeTagValue || column.form != numericFormBytes {
		return false
	}
	var borrowed bool
	switch column.valueType {
	case pbv1.ValueTypeStr:
		c, ok := col.(*vectorized.TypedColumn[string])
		if !ok {
			return false
		}
		forEachRow(idx, offset, desc, func(i int) {
			raw := column.values[i]
			if raw == nil {
				c.AppendNull()
				return
			}
			borrowed = borrowed || len(raw) > 0
			c.Append(convert.BytesToString(raw))
		})
	case pbv1.ValueTypeBinaryData:
		c, ok := col.(*vectorized.TypedColumn[[]byte])
		if !ok {
			return false
		}
		forEachRow(idx, offset, desc, func(i int) {
			raw := column.values[i]
			if raw == nil {
				c.AppendNull()
				return
			}
			borrowed = borrowed || len(raw) > 0
			c.Append(raw[:len(raw):len(raw)])
		})
	case pbv1.ValueTypeStrArr:
		c, ok := col.(*vectorized.TypedColumn[[]string])
		if !ok {
			return false
		}
		borrowed = borrowStrArrRows(c, column, idx, offset, desc)
	default:
		return false
	}
	if borrowed {
		b.Owners.Pin(bc)
	}
	return true
}

// forEachRow calls fn for each row in [idx, offset), last to first when desc.
func forEachRow(idx, offset int, desc bool, fn func(i int)) {
	if desc {
		for i := offset - 1; i >= idx; i-- {
			fn(i)
		}
		return
	}
	for i := idx; i < offset; i++ {
		fn(i)
	}
}

// borrowStrArrRows appends the string-array cells of rows [idx, offset) to c. An
// element without escape sequences is a view over the stored bytes; one with
// escapes is unescaped into a fresh string, since its decoded form is not stored.
// All cells slice one growing slab instead of allocating a slice per row. It
// reports whether any element borrows.
func borrowStrArrRows(c *vectorized.TypedColumn[[]string], column *column, idx, offset int, desc bool) bool {
	bb := bigValuePool.Generate()
	defer bigValuePool.Release(bb)
	var slab []string
	var borrowed bool
	forEachRow(idx, offset, desc, func(i int) {
		raw := column.values[i]
		if raw == nil {
			c.AppendNull()
			return
		}
		start := len(slab)
		for buf := raw; len(buf) > 0; {
			var rest []byte
			var err error
			bb.Buf, rest, err = unmarshalVarArray(bb.Buf[:0], buf)
			if err != nil {
				logger.Panicf("unmarshalVarArray failed: %v", err)
			}
			// consumed includes the trailing delimiter; unescaping only ever shrinks
			// the element, so equal lengths mean the stored bytes are the element.
			consumed := len(buf) - len(rest)
			if len(bb.Buf) == consumed-1 {
				slab = append(slab, convert.BytesToString(buf[:len(bb.Buf)]))
				borrowed = borrowed || len(bb.Buf) > 0
			} else {
				slab = append(slab, string(bb.Buf))
			}
			buf = rest
		}
		if len(slab) == start {
			c.Append(nil)
			return
		}
		c.Append(slab[start:len(slab):len(slab)])
	})
	return borrowed
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
			bc.fillTagCell(b, col, def, bc.idx, indexValue)
		case vectorized.RoleField:
			col := b.Fields[fieldIdx]
			fieldIdx++
			bc.fillFieldCell(col, def, bc.idx)
		case vectorized.RoleTimestamp, vectorized.RoleVersion,
			vectorized.RoleSeriesID, vectorized.RoleShardID,
			vectorized.RoleElementID, vectorized.RoleOrderKey:
			// Metadata handled via the parallel slices above.
		}
	}
}

// fillTagCell appends a single tag cell at row rowIdx from bc into col.
func (bc *blockCursor) fillTagCell(b *model.MeasureBatch, col vectorized.Column, def vectorized.ColumnDef,
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
	if bc.borrowTagRows(b, col, def, column, rowIdx, rowIdx+1, false) {
		return
	}
	fillTagRows(col, def, column, rowIdx, rowIdx+1, false)
}

// fillFieldCell appends a single field cell at row rowIdx from bc into col.
func (bc *blockCursor) fillFieldCell(col vectorized.Column, def vectorized.ColumnDef, rowIdx int) {
	column := bc.findFieldColumn(def.Name)
	if column == nil {
		appendNullFieldN(col, def, 1)
		return
	}
	fillFieldRows(col, def, column, rowIdx, rowIdx+1, false)
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
			vectorized.RoleSeriesID, vectorized.RoleShardID,
			vectorized.RoleElementID, vectorized.RoleOrderKey:
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
	setTagCellAt(col, def, column.valueType, column.rowBytes(srcRow), destRow)
}

// setFieldCellAt overwrites column position destRow with the field value
// decoded from bc at srcRow.
func (bc *blockCursor) setFieldCellAt(col vectorized.Column, def vectorized.ColumnDef, srcRow, destRow int) {
	column := bc.findFieldColumn(def.Name)
	if column == nil {
		setNullFieldAt(col, def, destRow)
		return
	}
	setFieldCellAt(col, def, column.valueType, column.rowBytes(srcRow), destRow)
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
		defer bigValuePool.Release(bb)
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
