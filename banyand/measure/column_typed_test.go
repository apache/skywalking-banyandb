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
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

const typedTestRows = 64

// genTypedValues returns encoded rows of a counter (monotonic) or gauge
// (noisy) series. withNull replaces one row with nil so the column falls back
// to the bytes encoding.
func genTypedValues(vt pbv1.ValueType, kind string, rows int, withNull bool) [][]byte {
	r := rand.New(rand.NewSource(11)) //nolint:gosec // deterministic test data.
	values := make([][]byte, rows)
	cur := int64(1_000_000)
	for i := range values {
		switch kind {
		case "counter":
			cur += int64(r.Intn(50))
		default:
			cur = int64(r.NormFloat64()*5000) + 500_000
		}
		if vt == pbv1.ValueTypeFloat64 {
			values[i] = convert.Float64ToBytes(math.Round(float64(cur)*7) / 100)
		} else {
			values[i] = convert.Int64ToBytes(cur)
		}
	}
	if withNull {
		values[rows/2] = nil
	}
	return values
}

// readBack encodes c with mustWriteTo and decodes it into dst, the way a part
// round trip does.
func readBack(t *testing.T, c *column, dst *column, typedNumeric bool) {
	t.Helper()
	cm := &columnMetadata{}
	buf := &bytes.Buffer{}
	w := &writer{}
	w.init(buf)
	c.mustWriteTo(cm, w)
	dst.mustReadValues(&encoding.BytesBlockDecoder{}, buf, *cm, uint64(len(c.values)), typedNumeric)
}

func TestColumnTypedDecode(t *testing.T) {
	for _, vt := range []pbv1.ValueType{pbv1.ValueTypeInt64, pbv1.ValueTypeFloat64} {
		for _, kind := range []string{"counter", "gauge"} {
			for _, withNull := range []bool{false, true} {
				t.Run(fmt.Sprintf("%v/%s/null=%v", vt, kind, withNull), func(t *testing.T) {
					src := &column{name: "c", valueType: vt, values: genTypedValues(vt, kind, 4096, withNull)}
					bytesCol, typedCol := &column{}, &column{}
					readBack(t, src, bytesCol, false)
					readBack(t, src, typedCol, true)

					require.Equal(t, numericFormBytes, bytesCol.form)
					require.Equal(t, src.values, bytesCol.values)
					require.Equal(t, len(src.values), typedCol.rowCount())
					if withNull {
						// Nulls force the plain encoding, which has no typed form.
						require.Equal(t, numericFormBytes, typedCol.form)
						require.Equal(t, bytesCol.values, typedCol.values)
						return
					}
					require.Empty(t, typedCol.values)
					for i := range src.values {
						require.Equal(t, src.values[i], typedCol.rowBytes(i), "row %d", i)
					}
					if vt == pbv1.ValueTypeInt64 {
						require.Equal(t, numericFormInt64, typedCol.form)
						require.Empty(t, typedCol.float64s)
					} else {
						require.Equal(t, numericFormFloat64, typedCol.form)
						require.Empty(t, typedCol.int64s)
						for i, v := range typedCol.float64s {
							require.Equal(t, math.Float64bits(convert.BytesToFloat64(src.values[i])), math.Float64bits(v))
						}
					}
				})
			}
		}
	}
}

func TestColumnTypedReuseDoesNotLeak(t *testing.T) {
	for _, vt := range []pbv1.ValueType{pbv1.ValueTypeInt64, pbv1.ValueTypeFloat64} {
		t.Run(fmt.Sprint(vt), func(t *testing.T) {
			clean := &column{name: "c", valueType: vt, values: genTypedValues(vt, "gauge", 32, false)}
			nulls := &column{name: "c", valueType: vt, values: genTypedValues(vt, "counter", 16, true)}
			reused := &column{}

			readBack(t, clean, reused, true)
			require.NotEqual(t, numericFormBytes, reused.form)
			reused.reset()
			require.Equal(t, numericFormBytes, reused.form)
			require.Zero(t, reused.rowCount())
			require.Empty(t, reused.int64s)
			require.Empty(t, reused.float64s)

			// typed block, then bytes-form block into the same column.
			readBack(t, clean, reused, true)
			readBack(t, nulls, reused, true)
			require.Equal(t, numericFormBytes, reused.form)
			require.Empty(t, reused.int64s)
			require.Empty(t, reused.float64s)
			require.Equal(t, nulls.values, reused.values)

			// bytes block, then typed block into the same column.
			readBack(t, clean, reused, true)
			require.NotEqual(t, numericFormBytes, reused.form)
			require.Empty(t, reused.values)
			require.Equal(t, len(clean.values), reused.rowCount())
			for i := range clean.values {
				require.Equal(t, clean.values[i], reused.rowBytes(i))
			}

			// A sequential (merge) read must never produce the typed form.
			reused.reset()
			readBack(t, clean, reused, false)
			require.Equal(t, numericFormBytes, reused.form)
			require.Equal(t, clean.values, reused.values)
		})
	}
}

// TestColumnTypedReuseKeepsBuffer proves the typed slice is owned by the column
// and survives reset(), so steady-state decoding does not allocate it again.
func TestColumnTypedReuseKeepsBuffer(t *testing.T) {
	src := &column{name: "c", valueType: pbv1.ValueTypeInt64, values: genTypedValues(pbv1.ValueTypeInt64, "gauge", 1024, false)}
	c := &column{}
	readBack(t, src, c, true)
	first := &c.int64s[0]
	c.reset()
	readBack(t, src, c, true)
	require.Same(t, first, &c.int64s[0])
}

// buildTypedTestCursor builds a cursor whose columns went through a real
// write/read round trip, in the typed form when typedNumeric is set.
func buildTypedTestCursor(t *testing.T, kind string, withNull, typedNumeric bool) *blockCursor {
	t.Helper()
	bc := &blockCursor{
		bm:             blockMetadata{seriesID: common.SeriesID(7)},
		shardID:        3,
		schemaTagTypes: map[string]pbv1.ValueType{"t_int": pbv1.ValueTypeInt64, "t_str": pbv1.ValueTypeStr},
	}
	for i := range typedTestRows {
		bc.timestamps = append(bc.timestamps, int64(i+1))
		bc.versions = append(bc.versions, int64(100+i))
	}
	load := func(name string, vt pbv1.ValueType, values [][]byte) column {
		out := column{}
		readBack(t, &column{name: name, valueType: vt, values: values}, &out, typedNumeric)
		return out
	}
	strs := make([][]byte, typedTestRows)
	for i := range strs {
		strs[i] = []byte(fmt.Sprintf("s%d", i))
	}
	bc.tagFamilies = []columnFamily{{name: "default", columns: []column{
		load("t_int", pbv1.ValueTypeInt64, genTypedValues(pbv1.ValueTypeInt64, kind, typedTestRows, withNull)),
		load("t_str", pbv1.ValueTypeStr, strs),
	}}}
	bc.fields = columnFamily{columns: []column{
		load("f_int", pbv1.ValueTypeInt64, genTypedValues(pbv1.ValueTypeInt64, kind, typedTestRows, withNull)),
		load("f_float", pbv1.ValueTypeFloat64, genTypedValues(pbv1.ValueTypeFloat64, kind, typedTestRows, withNull)),
	}}
	return bc
}

func typedTestSchema(native bool) *vectorized.BatchSchema {
	tagInt, tagStr, fInt, fFloat := vectorized.ColumnTypeTagValue, vectorized.ColumnTypeTagValue,
		vectorized.ColumnTypeFieldValue, vectorized.ColumnTypeFieldValue
	if native {
		tagInt, tagStr, fInt, fFloat = vectorized.ColumnTypeInt64, vectorized.ColumnTypeString,
			vectorized.ColumnTypeInt64, vectorized.ColumnTypeFloat64
	}
	return vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleTimestamp, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleVersion, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleSeriesID, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleShardID, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "t_int", Type: tagInt},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "t_str", Type: tagStr},
		{Role: vectorized.RoleField, Name: "f_int", Type: fInt},
		{Role: vectorized.RoleField, Name: "f_float", Type: fFloat},
	})
}

// requireBatchesEqual compares two batches cell by cell, including nulls.
func requireBatchesEqual(t *testing.T, want, got *model.MeasureBatch) {
	t.Helper()
	require.Equal(t, want.Timestamps, got.Timestamps)
	require.Equal(t, want.Versions, got.Versions)
	require.Equal(t, want.SeriesIDs, got.SeriesIDs)
	require.Equal(t, want.ShardIDs, got.ShardIDs)
	cols := func(b *model.MeasureBatch) []vectorized.Column {
		return append(append([]vectorized.Column{}, b.Tags...), b.Fields...)
	}
	wantCols, gotCols := cols(want), cols(got)
	require.Len(t, gotCols, len(wantCols))
	for c := range wantCols {
		require.Equal(t, wantCols[c].Type(), gotCols[c].Type(), "column %d", c)
		require.Equal(t, wantCols[c].Len(), gotCols[c].Len(), "column %d", c)
		for i := 0; i < wantCols[c].Len(); i++ {
			require.Equal(t, wantCols[c].IsNull(i), gotCols[c].IsNull(i), "column %d row %d null", c, i)
			switch w := wantCols[c].(type) {
			case *vectorized.TypedColumn[int64]:
				require.Equal(t, w.Data()[i], gotCols[c].(*vectorized.TypedColumn[int64]).Data()[i], "column %d row %d", c, i)
			case *vectorized.TypedColumn[float64]:
				require.Equal(t, math.Float64bits(w.Data()[i]),
					math.Float64bits(gotCols[c].(*vectorized.TypedColumn[float64]).Data()[i]), "column %d row %d", c, i)
			case *vectorized.TypedColumn[string]:
				require.Equal(t, w.Data()[i], gotCols[c].(*vectorized.TypedColumn[string]).Data()[i], "column %d row %d", c, i)
			case *vectorized.TypedColumn[*modelv1.TagValue]:
				require.True(t, proto.Equal(w.Data()[i], gotCols[c].(*vectorized.TypedColumn[*modelv1.TagValue]).Data()[i]),
					"column %d row %d", c, i)
			case *vectorized.TypedColumn[*modelv1.FieldValue]:
				require.True(t, proto.Equal(w.Data()[i], gotCols[c].(*vectorized.TypedColumn[*modelv1.FieldValue]).Data()[i]),
					"column %d row %d", c, i)
			default:
				t.Fatalf("unexpected column type %T", w)
			}
		}
	}
}

func TestCopyAllToBatchTypedMatchesBytes(t *testing.T) {
	hidden := map[common.SeriesID]map[string]*modelv1.TagValue{
		7: {"t_int": {Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: 4242}}}},
	}
	for _, native := range []bool{true, false} {
		for _, kind := range []string{"counter", "gauge"} {
			for _, withNull := range []bool{false, true} {
				for _, desc := range []bool{false, true} {
					for _, mode := range []string{"plain", "hidden", "schemaMismatch"} {
						name := fmt.Sprintf("native=%v/%s/null=%v/desc=%v/%s", native, kind, withNull, desc, mode)
						t.Run(name, func(t *testing.T) {
							schema := typedTestSchema(native)
							var index map[common.SeriesID]map[string]*modelv1.TagValue
							build := func(typed bool) *blockCursor {
								bc := buildTypedTestCursor(t, kind, withNull, typed)
								bc.idx = typedTestRows / 4
								if desc {
									bc.idx = typedTestRows - typedTestRows/4
								}
								switch mode {
								case "hidden":
									index = hidden
								case "schemaMismatch":
									bc.schemaTagTypes["t_int"] = pbv1.ValueTypeStr
								}
								return bc
							}
							want := newMeasureBatchForSchema(schema, typedTestRows)
							build(false).copyAllToBatch(want, schema, index, desc)
							typedCursor := build(true)
							if !withNull {
								require.Equal(t, numericFormInt64, typedCursor.fields.columns[0].form)
								require.Equal(t, numericFormFloat64, typedCursor.fields.columns[1].form)
							}
							got := newMeasureBatchForSchema(schema, typedTestRows)
							typedCursor.copyAllToBatch(got, schema, index, desc)
							require.Positive(t, got.RowCount())
							requireBatchesEqual(t, want, got)
						})
					}
				}
			}
		}
	}
}

func TestCopyAndReplaceInBatchTypedMatchesBytes(t *testing.T) {
	hidden := map[common.SeriesID]map[string]*modelv1.TagValue{
		7: {"t_int": {Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: 4242}}}},
	}
	for _, native := range []bool{true, false} {
		for _, withNull := range []bool{false, true} {
			for _, useHidden := range []bool{false, true} {
				t.Run(fmt.Sprintf("native=%v/null=%v/hidden=%v", native, withNull, useHidden), func(t *testing.T) {
					schema := typedTestSchema(native)
					var index map[common.SeriesID]map[string]*modelv1.TagValue
					if useHidden {
						index = hidden
					}
					run := func(typed bool) *model.MeasureBatch {
						bc := buildTypedTestCursor(t, "gauge", withNull, typed)
						b := newMeasureBatchForSchema(schema, typedTestRows)
						for _, row := range []int{0, 5, 6, typedTestRows / 2, typedTestRows - 1} {
							bc.idx = row
							bc.copyToBatch(b, schema, index)
						}
						// Overwrite the last row with other rows, as a newer duplicate version would.
						for _, row := range []int{9, typedTestRows / 2} {
							bc.idx = row
							bc.replaceInBatch(b, schema, index)
						}
						return b
					}
					requireBatchesEqual(t, run(false), run(true))
				})
			}
		}
	}
}

// TestLoadDataTypedMatchesBytes runs real parts through loadData in both modes
// and checks the batches are identical, and that the typed mode really is typed.
func TestLoadDataTypedMatchesBytes(t *testing.T) {
	h := setupBenchStorageHarness(t)
	for name, schema := range map[string]*vectorized.BatchSchema{"native": benchNativeSchema(), "passthrough": benchPassthroughSchema()} {
		for _, desc := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/desc=%v", name, desc), func(t *testing.T) {
				collect := func(typed bool) []*model.MeasureBatch {
					var out []*model.MeasureBatch
					h.drainCursors(typed, func(bc *blockCursor) {
						if typed {
							require.Equal(t, numericFormInt64, bc.fields.columns[0].form)
							require.Equal(t, numericFormInt64, bc.tagFamilies[0].columns[1].form)
						} else {
							require.Equal(t, numericFormBytes, bc.fields.columns[0].form)
						}
						if desc {
							bc.idx = len(bc.timestamps) - 1
						}
						b := newMeasureBatchForSchema(schema, len(bc.timestamps))
						bc.copyAllToBatch(b, schema, nil, desc)
						out = append(out, b)
					})
					return out
				}
				want, got := collect(false), collect(true)
				require.NotEmpty(t, want)
				require.Len(t, got, len(want))
				for i := range want {
					requireBatchesEqual(t, want[i], got[i])
				}
			})
		}
	}
}
