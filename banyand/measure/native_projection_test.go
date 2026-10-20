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
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

const nativeProjRows = 48

// nativeProjTag / nativeProjField describe one stored column of the fixture.
type nativeProjTag struct {
	name string
	vt   pbv1.ValueType
	typ  vectorized.ColumnType
}

var nativeProjTags = []nativeProjTag{
	{"t_int", pbv1.ValueTypeInt64, vectorized.ColumnTypeInt64},
	{"t_str", pbv1.ValueTypeStr, vectorized.ColumnTypeString},
	{"t_bin", pbv1.ValueTypeBinaryData, vectorized.ColumnTypeBytes},
	{"t_iarr", pbv1.ValueTypeInt64Arr, vectorized.ColumnTypeInt64Array},
	{"t_sarr", pbv1.ValueTypeStrArr, vectorized.ColumnTypeStrArray},
}

var nativeProjFields = []nativeProjTag{
	{"f_int", pbv1.ValueTypeInt64, vectorized.ColumnTypeInt64},
	{"f_float", pbv1.ValueTypeFloat64, vectorized.ColumnTypeFloat64},
	{"f_str", pbv1.ValueTypeStr, vectorized.ColumnTypeString},
	{"f_bin", pbv1.ValueTypeBinaryData, vectorized.ColumnTypeBytes},
}

// nativeProjRaw returns the stored bytes of one fixture cell. Every column has
// null rows (i%7==3) and the string / binary columns also have empty rows.
func nativeProjRaw(vt pbv1.ValueType, i int) []byte {
	if i%7 == 3 {
		return nil
	}
	switch vt {
	case pbv1.ValueTypeInt64:
		return convert.Int64ToBytes(int64(i*3 - 20))
	case pbv1.ValueTypeFloat64:
		return convert.Float64ToBytes(float64(i) * 1.5)
	case pbv1.ValueTypeStr:
		if i%5 == 0 {
			return []byte{}
		}
		return []byte(fmt.Sprintf("value-%d", i))
	case pbv1.ValueTypeBinaryData:
		if i%5 == 0 {
			return []byte{}
		}
		return []byte{byte(i), 0, 0xff}
	case pbv1.ValueTypeInt64Arr:
		if i%4 == 0 {
			return []byte{}
		}
		var out []byte
		for k := range i % 3 {
			out = append(out, convert.Int64ToBytes(int64(i+k))...)
		}
		return append(out, convert.Int64ToBytes(int64(-i))...)
	case pbv1.ValueTypeStrArr:
		var out []byte
		switch i % 4 {
		case 0:
			return []byte{}
		case 1:
			out = marshalVarArray(out, []byte("a|b"))
		default:
			out = marshalVarArray(out, []byte("x"))
			out = marshalVarArray(out, []byte(fmt.Sprintf("y-%d", i)))
		}
		return out
	default:
	}
	return nil
}

func buildNativeProjCursor(t *testing.T, typedNumeric bool) *blockCursor {
	t.Helper()
	// The cursor comes from the pool and its creator reference is dropped when the
	// test ends, after the batches that pin it were released: releasing a cursor the
	// pool never handed out would unbalance the pool's count.
	bc := generateBlockCursor()
	t.Cleanup(func() { releaseBlockCursor(bc) })
	bc.bm = blockMetadata{seriesID: common.SeriesID(5)}
	bc.shardID = 1
	bc.schemaTagTypes = map[string]pbv1.ValueType{}
	for i := range nativeProjRows {
		bc.timestamps = append(bc.timestamps, int64(i+1))
		bc.versions = append(bc.versions, 1)
	}
	load := func(name string, vt pbv1.ValueType) column {
		values := make([][]byte, nativeProjRows)
		for i := range values {
			values[i] = nativeProjRaw(vt, i)
		}
		src := &column{name: name, valueType: vt, values: values}
		cm := &columnMetadata{}
		buf := &bytes.Buffer{}
		w := &writer{}
		w.init(buf)
		src.mustWriteTo(cm, w)
		dst := column{}
		dst.mustReadValues(&bc.columnValuesDecoder, buf, *cm, uint64(nativeProjRows), typedNumeric)
		return dst
	}
	var tagCols []column
	for _, tg := range nativeProjTags {
		tagCols = append(tagCols, load(tg.name, tg.vt))
		bc.schemaTagTypes[tg.name] = tg.vt
	}
	bc.tagFamilies = []columnFamily{{name: "default", columns: tagCols}}
	for _, f := range nativeProjFields {
		bc.fields.columns = append(bc.fields.columns, load(f.name, f.vt))
	}
	return bc
}

// nativeProjSchema declares every fixture column, native or passthrough, plus a
// tag the cursor does not store and a family it does not have.
func nativeProjSchema(native bool) *vectorized.BatchSchema {
	cols := []vectorized.ColumnDef{
		{Role: vectorized.RoleTimestamp, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleVersion, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleSeriesID, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleShardID, Type: vectorized.ColumnTypeInt64},
	}
	for _, tg := range nativeProjTags {
		typ := vectorized.ColumnTypeTagValue
		if native {
			typ = tg.typ
		}
		cols = append(cols, vectorized.ColumnDef{Role: vectorized.RoleTag, TagFamily: "default", Name: tg.name, Type: typ})
	}
	nativeStr, nativeFieldStr := vectorized.ColumnTypeTagValue, vectorized.ColumnTypeFieldValue
	if native {
		nativeStr, nativeFieldStr = vectorized.ColumnTypeString, vectorized.ColumnTypeString
	}
	cols = append(cols,
		vectorized.ColumnDef{Role: vectorized.RoleTag, TagFamily: "default", Name: "t_missing", Type: nativeStr},
		vectorized.ColumnDef{Role: vectorized.RoleTag, TagFamily: "other", Name: "t_other", Type: nativeStr},
	)
	for _, f := range nativeProjFields {
		typ := vectorized.ColumnTypeFieldValue
		if native {
			typ = f.typ
		}
		cols = append(cols, vectorized.ColumnDef{Role: vectorized.RoleField, Name: f.name, Type: typ})
	}
	cols = append(cols, vectorized.ColumnDef{Role: vectorized.RoleField, Name: "f_missing", Type: nativeFieldStr})
	return vectorized.NewBatchSchema(cols)
}

// nativeCellAsTagValue is what the egress serializer produces for a cell of
// either column shape. Native null cells become pbv1.NullTagValue, as at egress.
func nativeCellAsTagValue(col vectorized.Column, row int) *modelv1.TagValue {
	if pc, ok := col.(*vectorized.TypedColumn[*modelv1.TagValue]); ok {
		if v := pc.Data()[row]; v != nil {
			return v
		}
		return pbv1.NullTagValue
	}
	if col.IsNull(row) {
		return pbv1.NullTagValue
	}
	switch c := col.(type) {
	case *vectorized.TypedColumn[int64]:
		return &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: c.Data()[row]}}}
	case *vectorized.TypedColumn[string]:
		return &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: c.Data()[row]}}}
	case *vectorized.TypedColumn[[]byte]:
		return &modelv1.TagValue{Value: &modelv1.TagValue_BinaryData{BinaryData: c.Data()[row]}}
	case *vectorized.TypedColumn[[]int64]:
		return &modelv1.TagValue{Value: &modelv1.TagValue_IntArray{IntArray: &modelv1.IntArray{Value: c.Data()[row]}}}
	case *vectorized.TypedColumn[[]string]:
		return &modelv1.TagValue{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: c.Data()[row]}}}
	}
	panic(fmt.Sprintf("unexpected tag column %T", col))
}

func nativeCellAsFieldValue(col vectorized.Column, row int) *modelv1.FieldValue {
	if pc, ok := col.(*vectorized.TypedColumn[*modelv1.FieldValue]); ok {
		if v := pc.Data()[row]; v != nil {
			return v
		}
		return pbv1.NullFieldValue
	}
	if col.IsNull(row) {
		return pbv1.NullFieldValue
	}
	switch c := col.(type) {
	case *vectorized.TypedColumn[int64]:
		return &modelv1.FieldValue{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: c.Data()[row]}}}
	case *vectorized.TypedColumn[float64]:
		return &modelv1.FieldValue{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: c.Data()[row]}}}
	case *vectorized.TypedColumn[string]:
		return &modelv1.FieldValue{Value: &modelv1.FieldValue_Str{Str: &modelv1.Str{Value: c.Data()[row]}}}
	case *vectorized.TypedColumn[[]byte]:
		return &modelv1.FieldValue{Value: &modelv1.FieldValue_BinaryData{BinaryData: c.Data()[row]}}
	}
	panic(fmt.Sprintf("unexpected field column %T", col))
}

// requireNativeMatchesPassthrough checks every cell of the native batch against
// the passthrough batch after egress-style materialization. Marshaled bytes are
// compared as well so empty-versus-nil payloads cannot hide a wire difference.
func requireNativeMatchesPassthrough(t *testing.T, want, got *model.MeasureBatch) {
	t.Helper()
	require.Equal(t, want.Timestamps, got.Timestamps)
	require.Equal(t, want.Versions, got.Versions)
	require.Equal(t, want.SeriesIDs, got.SeriesIDs)
	require.Equal(t, want.ShardIDs, got.ShardIDs)
	require.Len(t, got.Tags, len(want.Tags))
	require.Len(t, got.Fields, len(want.Fields))
	for c := range want.Tags {
		require.Equal(t, want.Tags[c].Len(), got.Tags[c].Len(), "tag column %d", c)
		for i := 0; i < want.Tags[c].Len(); i++ {
			w, g := nativeCellAsTagValue(want.Tags[c], i), nativeCellAsTagValue(got.Tags[c], i)
			require.True(t, proto.Equal(w, g), "tag column %d row %d: want %v got %v", c, i, w, g)
			wb, _ := proto.Marshal(w)
			gb, _ := proto.Marshal(g)
			require.Equal(t, wb, gb, "tag column %d row %d wire", c, i)
		}
	}
	for c := range want.Fields {
		require.Equal(t, want.Fields[c].Len(), got.Fields[c].Len(), "field column %d", c)
		for i := 0; i < want.Fields[c].Len(); i++ {
			w, g := nativeCellAsFieldValue(want.Fields[c], i), nativeCellAsFieldValue(got.Fields[c], i)
			require.True(t, proto.Equal(w, g), "field column %d row %d: want %v got %v", c, i, w, g)
			wb, _ := proto.Marshal(w)
			gb, _ := proto.Marshal(g)
			require.Equal(t, wb, gb, "field column %d row %d wire", c, i)
		}
	}
}

// TestNativeProjectionMatchesPassthrough decodes the same stored cells into a
// passthrough schema and the all-native schema BuildBatchSchema now emits, over
// every tag/field type, null and empty cells, absent columns, hidden-tag
// substitution, a schema/stored type mismatch, both directions and the
// single-row / replace paths.
func TestNativeProjectionMatchesPassthrough(t *testing.T) {
	verifyNoScanLeaks(t)
	hiddenIndex := map[common.SeriesID]map[string]*modelv1.TagValue{
		5: {
			"t_str":  {Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "from-index"}}},
			"t_iarr": {Value: &modelv1.TagValue_IntArray{IntArray: &modelv1.IntArray{Value: []int64{1, 2}}}},
		},
	}
	for _, typedNumeric := range []bool{false, true} {
		for _, desc := range []bool{false, true} {
			for _, mode := range []string{"plain", "hidden", "schemaMismatch"} {
				t.Run(fmt.Sprintf("typed=%v/desc=%v/%s", typedNumeric, desc, mode), func(t *testing.T) {
					var index map[common.SeriesID]map[string]*modelv1.TagValue
					build := func() *blockCursor {
						bc := buildNativeProjCursor(t, typedNumeric)
						bc.idx = 3
						switch mode {
						case "hidden":
							index = hiddenIndex
						case "schemaMismatch":
							bc.schemaTagTypes["t_sarr"] = pbv1.ValueTypeStr
							bc.schemaTagTypes["t_int"] = pbv1.ValueTypeBinaryData
						}
						return bc
					}
					passSchema, natSchema := nativeProjSchema(false), nativeProjSchema(true)
					want := newMeasureBatchForSchema(passSchema, nativeProjRows)
					defer want.Release()
					build().copyAllToBatch(want, passSchema, index, desc)
					got := newMeasureBatchForSchema(natSchema, nativeProjRows)
					defer got.Release()
					nativeCursor := build()
					nativeCursor.copyAllToBatch(got, natSchema, index, desc)
					require.Positive(t, got.RowCount())
					requireNativeMatchesPassthrough(t, want, got)
				})
			}
		}
	}
}

func TestNativeProjectionSingleRowAndReplaceMatchPassthrough(t *testing.T) {
	verifyNoScanLeaks(t)
	hiddenIndex := map[common.SeriesID]map[string]*modelv1.TagValue{
		5: {"t_sarr": {Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: []string{"h"}}}}},
	}
	for _, typedNumeric := range []bool{false, true} {
		for _, useHidden := range []bool{false, true} {
			t.Run(fmt.Sprintf("typed=%v/hidden=%v", typedNumeric, useHidden), func(t *testing.T) {
				var index map[common.SeriesID]map[string]*modelv1.TagValue
				if useHidden {
					index = hiddenIndex
				}
				run := func(schema *vectorized.BatchSchema) *model.MeasureBatch {
					bc := buildNativeProjCursor(t, typedNumeric)
					b := newMeasureBatchForSchema(schema, nativeProjRows)
					for _, row := range []int{0, 3, 4, 10, nativeProjRows - 1} {
						bc.idx = row
						bc.copyToBatch(b, schema, index)
					}
					for _, row := range []int{3, 17} {
						bc.idx = row
						bc.replaceInBatch(b, schema, index)
					}
					return b
				}
				want, got := run(nativeProjSchema(false)), run(nativeProjSchema(true))
				requireNativeMatchesPassthrough(t, want, got)
				want.Release()
				got.Release()
			})
		}
	}
}
