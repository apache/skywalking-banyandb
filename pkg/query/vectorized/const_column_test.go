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
)

func TestConstColumnSemantics(t *testing.T) {
	cases := []struct {
		col      *ConstColumn
		wantType ColumnType
	}{
		{NewConstInt64(7, 5), ColumnTypeInt64},
		{NewConstFloat64(1.5, 5), ColumnTypeFloat64},
		{NewConstString("svc", 5), ColumnTypeString},
		{NewConstBytes([]byte{1, 2}, 5), ColumnTypeBytes},
		{NewConstNull(ColumnTypeString, 5), ColumnTypeString},
	}
	for _, tc := range cases {
		t.Run(tc.wantType.String(), func(t *testing.T) {
			require.Equal(t, tc.wantType, tc.col.Type(), "a constant reports the logical type of the column it stands for")
			require.Equal(t, 5, tc.col.Len())
			for i := range 5 {
				require.Equal(t, tc.col.IsNullValue(), tc.col.IsNull(i))
			}
			require.False(t, tc.col.IsNull(-1))
			require.False(t, tc.col.IsNull(5))
			require.Panics(t, func() { tc.col.AppendNull() })
			require.Panics(t, func() { tc.col.MarkNullAt(0) })

			expanded := tc.col.Expand()
			require.Equal(t, tc.wantType, expanded.Type())
			require.Equal(t, 5, expanded.Len())
			for i := range 5 {
				require.Equal(t, tc.col.IsNull(i), expanded.IsNull(i))
			}
			// Materialize returns a typed column for a constant and the column itself otherwise.
			require.Equal(t, 5, Materialize(tc.col).Len())
			typed := NewInt64Column(1)
			require.Same(t, typed, Materialize(typed))
		})
	}
	expanded := NewConstInt64(7, 3).Expand().(*TypedColumn[int64])
	require.Equal(t, []int64{7, 7, 7}, expanded.Data())
	require.Equal(t, []string{"a", "a"}, NewConstString("a", 2).Expand().(*TypedColumn[string]).Data())
	nullInts := NewConstNull(ColumnTypeInt64, 2).Expand().(*TypedColumn[int64])
	require.True(t, nullInts.IsNull(0) && nullInts.IsNull(1))
	require.Equal(t, []int64{0, 0}, nullInts.Data(), "a null cell holds the zero placeholder, as in a typed column")
}

func TestConstColumnAppendTo(t *testing.T) {
	dst := NewStringColumn(4)
	require.NoError(t, NewConstString("x", 3).AppendTo(dst, 2))
	require.Equal(t, []string{"x", "x"}, dst.Data())
	require.Error(t, NewConstString("x", 3).AppendTo(NewInt64Column(1), 1), "a type mismatch is an error")
	require.Error(t, NewConstInt64(1, 3).AppendTo(NewInt64ArrayColumn(1), 1), "an unsupported destination is an error")

	nullDst := NewFloat64Column(2)
	require.NoError(t, NewConstNull(ColumnTypeFloat64, 2).AppendTo(nullDst, 2))
	require.True(t, nullDst.IsNull(0) && nullDst.IsNull(1))
}

func TestAppendColumnRangeFromConst(t *testing.T) {
	dst := NewInt64Column(4)
	dst.Append(1)
	require.NoError(t, AppendColumnRange(dst, NewConstInt64(9, 4), 1, 3))
	require.Equal(t, []int64{1, 9, 9, 9}, dst.Data())
	require.Error(t, AppendColumnRange(dst, NewConstInt64(9, 4), 2, 5), "a range past the constant's rows is an error")
}

func TestAcquireConstFrom(t *testing.T) {
	strs := NewStringColumn(3)
	strs.Append("a")
	strs.AppendNull()
	c, ok := AcquireConstFrom(strs, 0, 10)
	require.True(t, ok)
	require.Equal(t, "a", c.Str())
	require.Equal(t, 10, c.Len())
	ReleaseColumn(c)

	c, ok = AcquireConstFrom(strs, 1, 10)
	require.True(t, ok)
	require.True(t, c.IsNullValue())
	require.Empty(t, c.Str())
	ReleaseColumn(c)

	_, ok = AcquireConstFrom(strs, 2, 10)
	require.False(t, ok, "a row past the column is refused")
	_, ok = AcquireConstFrom(NewInt64ArrayColumn(1), 0, 1)
	require.False(t, ok, "an array column cannot be constant")

	// A pooled column forgets its value.
	c, ok = AcquireConstFrom(strs, 0, 3)
	require.True(t, ok)
	ReleaseColumn(c)
	require.Zero(t, c.Len())
	require.Empty(t, c.Str())
}

func TestConstColumnEqual(t *testing.T) {
	require.True(t, NewConstInt64(1, 3).Equal(NewConstInt64(1, 9)))
	require.False(t, NewConstInt64(1, 3).Equal(NewConstInt64(2, 3)))
	require.False(t, NewConstInt64(0, 3).Equal(NewConstNull(ColumnTypeInt64, 3)))
	require.True(t, NewConstNull(ColumnTypeString, 1).Equal(NewConstNull(ColumnTypeString, 2)))
	require.False(t, NewConstString("a", 1).Equal(NewConstBytes([]byte("a"), 1)))
	require.True(t, NewConstBytes([]byte("ab"), 1).Equal(NewConstBytes([]byte("ab"), 1)))
}

func TestEstimatedRetainedBytesOfConst(t *testing.T) {
	schema := NewBatchSchema([]ColumnDef{{Role: RoleTag, Name: "t", Type: ColumnTypeString}})
	typed := NewRecordBatch(schema, 8192)
	col := typed.Columns[0].(*TypedColumn[string])
	for range 8192 {
		col.Append("an-endpoint-like-entity-value")
	}
	typed.Len = 8192
	lent := &RecordBatch{Schema: schema, Columns: []Column{NewConstString("an-endpoint-like-entity-value", 8192)}, Len: 8192}
	require.Less(t, EstimatedRetainedBytes(lent)*2, EstimatedRetainedBytes(typed), "a constant costs one value, not one per row")
}

func TestSeriesRuns(t *testing.T) {
	batch := func(n int, ends []int, known bool, sel []uint16) *RecordBatch {
		return &RecordBatch{Len: n, Selection: sel, Hints: BatchHints{SeriesKnown: known, SeriesEnds: ends}}
	}
	cases := []struct {
		name  string
		batch *RecordBatch
		want  []int
		known bool
	}{
		{"unknown series", batch(5, nil, false, nil), nil, false},
		{"single series", batch(5, nil, true, nil), []int{5}, true},
		{"empty", batch(0, nil, true, nil), nil, true},
		{"two runs", batch(5, []int{2, 5}, true, nil), []int{2, 5}, true},
		{"same series twice", batch(6, []int{2, 4, 6}, true, nil), []int{2, 4, 6}, true},
		{"selection inside one run", batch(6, []int{3, 6}, true, []uint16{0, 2}), []int{2}, true},
		{"selection across runs", batch(6, []int{3, 6}, true, []uint16{1, 2, 4, 5}), []int{2, 4}, true},
		{"reversed selection", batch(6, []int{3, 6}, true, []uint16{5, 4, 3, 2, 1, 0}), []int{3, 6}, true},
		{"empty selection", batch(6, []int{3, 6}, true, []uint16{}), nil, true},
		{"selection skipping a whole run", batch(9, []int{3, 6, 9}, true, []uint16{0, 1, 7, 8}), []int{2, 4}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, known := tc.batch.SeriesRuns(nil)
			require.Equal(t, tc.known, known)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestRecordBatchResetClearsHints(t *testing.T) {
	schema := NewBatchSchema([]ColumnDef{{Role: RoleTag, Name: "t", Type: ColumnTypeInt64}})
	b := NewRecordBatch(schema, 4)
	b.Hints = BatchHints{SeriesKnown: true, SeriesEnds: []int{1}, TimeOrder: TimeOrderAsc}
	b.Reset()
	require.Equal(t, BatchHints{}, b.Hints)
}

func TestTypedColumnMayHaveNulls(t *testing.T) {
	c := NewInt64Column(4)
	c.Append(1)
	require.False(t, c.MayHaveNulls())
	c.AppendNull()
	require.True(t, c.MayHaveNulls())
	c.Reset()
	require.False(t, c.MayHaveNulls(), "Reset clears the nulls")
	c.Append(1)
	require.False(t, c.IsNull(0))
}

func TestSelectRows(t *testing.T) {
	schema := NewBatchSchema([]ColumnDef{{Role: RoleTag, Name: "svc", Type: ColumnTypeString}, {Role: RoleField, Name: "v", Type: ColumnTypeInt64}})
	isA := func(col Column, row int) bool {
		switch c := col.(type) {
		case *ConstColumn:
			return c.Str() == "a"
		case *TypedColumn[string]:
			return c.Data()[row] == "a"
		}
		return false
	}
	t.Run("constant kept", func(t *testing.T) {
		b := &RecordBatch{Schema: schema, Len: 4, Columns: []Column{NewConstString("a", 4), NewInt64Column(4)}}
		SelectRows(b, 0, isA)
		require.Nil(t, b.Selection, "a whole batch that passes is left alone")
	})
	t.Run("constant dropped", func(t *testing.T) {
		b := &RecordBatch{Schema: schema, Len: 4, Columns: []Column{NewConstString("b", 4), NewInt64Column(4)}}
		SelectRows(b, 0, isA)
		require.NotNil(t, b.Selection)
		require.Empty(t, b.Selection)
		require.Zero(t, b.ActiveLen())
	})
	t.Run("constant evaluated once", func(t *testing.T) {
		b := &RecordBatch{Schema: schema, Len: 1000, Columns: []Column{NewConstString("a", 1000), NewInt64Column(4)}}
		calls := 0
		SelectRows(b, 0, func(col Column, row int) bool { calls++; return isA(col, row) })
		require.Equal(t, 1, calls)
	})
	t.Run("typed column row by row", func(t *testing.T) {
		svc := NewStringColumn(4)
		for _, s := range []string{"a", "b", "a", "b"} {
			svc.Append(s)
		}
		b := &RecordBatch{Schema: schema, Len: 4, Columns: []Column{svc, NewInt64Column(4)}}
		SelectRows(b, 0, isA)
		require.Equal(t, []uint16{0, 2}, b.Selection)
		b.Selection = []uint16{3, 2, 1}
		SelectRows(b, 0, isA)
		require.Equal(t, []uint16{2}, b.Selection)
	})
}
