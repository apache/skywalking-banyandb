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
	"testing"

	"github.com/stretchr/testify/require"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

func TestAppendColumnRangeCopiesSupportedTypes(t *testing.T) {
	tests := []struct {
		assert func(t *testing.T, dst Column)
		src    Column
		dst    Column
		name   string
	}{
		{
			name: "int64",
			src: func() Column {
				col := NewInt64Column(3)
				col.Append(1)
				col.Append(2)
				col.Append(3)
				return col
			}(),
			dst: NewInt64Column(2),
			assert: func(t *testing.T, dst Column) {
				require.Equal(t, []int64{2, 3}, dst.(*TypedColumn[int64]).Data())
			},
		},
		{
			name: "float64",
			src: func() Column {
				col := NewFloat64Column(3)
				col.Append(1.5)
				col.Append(2.5)
				col.Append(3.5)
				return col
			}(),
			dst: NewFloat64Column(2),
			assert: func(t *testing.T, dst Column) {
				require.Equal(t, []float64{2.5, 3.5}, dst.(*TypedColumn[float64]).Data())
			},
		},
		{
			name: "string",
			src: func() Column {
				col := NewStringColumn(3)
				col.Append("a")
				col.Append("b")
				col.Append("c")
				return col
			}(),
			dst: NewStringColumn(2),
			assert: func(t *testing.T, dst Column) {
				require.Equal(t, []string{"b", "c"}, dst.(*TypedColumn[string]).Data())
			},
		},
		{
			name: "bytes",
			src: func() Column {
				col := NewBytesColumn(3)
				col.Append([]byte("a"))
				col.Append([]byte("b"))
				col.Append([]byte("c"))
				return col
			}(),
			dst: NewBytesColumn(2),
			assert: func(t *testing.T, dst Column) {
				require.Equal(t, [][]byte{[]byte("b"), []byte("c")}, dst.(*TypedColumn[[]byte]).Data())
			},
		},
		{
			name: "int64 array",
			src: func() Column {
				col := NewInt64ArrayColumn(3)
				col.Append([]int64{1})
				col.Append([]int64{2})
				col.Append([]int64{3})
				return col
			}(),
			dst: NewInt64ArrayColumn(2),
			assert: func(t *testing.T, dst Column) {
				require.Equal(t, [][]int64{{2}, {3}}, dst.(*TypedColumn[[]int64]).Data())
			},
		},
		{
			name: "string array",
			src: func() Column {
				col := NewStrArrayColumn(3)
				col.Append([]string{"a"})
				col.Append([]string{"b"})
				col.Append([]string{"c"})
				return col
			}(),
			dst: NewStrArrayColumn(2),
			assert: func(t *testing.T, dst Column) {
				require.Equal(t, [][]string{{"b"}, {"c"}}, dst.(*TypedColumn[[]string]).Data())
			},
		},
		{
			name: "tag value",
			src: func() Column {
				col := NewTagValueColumn(3)
				col.Append(&modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "a"}}})
				col.Append(&modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "b"}}})
				col.Append(&modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "c"}}})
				return col
			}(),
			dst: NewTagValueColumn(2),
			assert: func(t *testing.T, dst Column) {
				data := dst.(*TypedColumn[*modelv1.TagValue]).Data()
				require.Equal(t, "b", data[0].GetStr().Value)
				require.Equal(t, "c", data[1].GetStr().Value)
			},
		},
		{
			name: "field value",
			src: func() Column {
				col := NewFieldValueColumn(3)
				col.Append(&modelv1.FieldValue{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 1}}})
				col.Append(&modelv1.FieldValue{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 2}}})
				col.Append(&modelv1.FieldValue{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 3}}})
				return col
			}(),
			dst: NewFieldValueColumn(2),
			assert: func(t *testing.T, dst Column) {
				data := dst.(*TypedColumn[*modelv1.FieldValue]).Data()
				require.Equal(t, int64(2), data[0].GetInt().Value)
				require.Equal(t, int64(3), data[1].GetInt().Value)
			},
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			require.NoError(t, AppendColumnRange(testCase.dst, testCase.src, 1, 2))
			testCase.assert(t, testCase.dst)
		})
	}
}

func TestAppendColumnRangePropagatesNulls(t *testing.T) {
	src := NewStringColumn(3)
	src.Append("a")
	src.Append("b")
	src.Append("c")
	src.MarkNullAt(1)
	dst := NewStringColumn(2)

	require.NoError(t, AppendColumnRange(dst, src, 1, 2))
	require.True(t, dst.IsNull(0))
	require.False(t, dst.IsNull(1))
}

func TestAppendColumnRangeRejectsTypeMismatch(t *testing.T) {
	require.Error(t, AppendColumnRange(NewStringColumn(1), NewInt64Column(1), 0, 0))
}
