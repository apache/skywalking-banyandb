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
	"context"
	"testing"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func topTestSchema() *vectorized.BatchSchema {
	return vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleField, Name: "v", Type: vectorized.ColumnTypeInt64},
	})
}

// mkTopBatch builds a single-column int64 batch. nullMask[i] true → AppendNull.
func mkTopBatch(s *vectorized.BatchSchema, vals []int64, nullMask []bool) *vectorized.RecordBatch {
	b := vectorized.NewRecordBatch(s, len(vals))
	col := b.Columns[0].(*vectorized.TypedColumn[int64])
	for i, v := range vals {
		if i < len(nullMask) && nullMask[i] {
			col.AppendNull()
		} else {
			col.Append(v)
		}
	}
	b.Len = len(vals)
	return b
}

func drainTop(t *testing.T, top *BatchTop) []int64 {
	t.Helper()
	var out []int64
	for {
		b, err := top.NextBatch(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			break
		}
		col := b.Columns[0].(*vectorized.TypedColumn[int64])
		for i := range b.Len {
			if col.IsNull(i) {
				out = append(out, -999) // sentinel for null in test
			} else {
				out = append(out, col.Data()[i])
			}
		}
	}
	return out
}

func TestBatchTop_AscendingHeap_KeepsLowestN(t *testing.T) {
	s := topTestSchema()
	top := NewBatchTop(s, 0, 3, true, 8)
	_ = top.Init(context.Background())
	defer top.Close()
	_ = top.Consume(context.Background(), mkTopBatch(s, []int64{5, 2, 8, 1, 7, 3}, nil))
	_ = top.Finalize(context.Background())
	got := drainTop(t, top)
	want := []int64{1, 2, 3}
	if len(got) != len(want) {
		t.Fatalf("len: want %d, got %d (%v)", len(want), len(got), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("row %d: want %d, got %d", i, want[i], got[i])
		}
	}
}

func TestBatchTop_DescendingHeap_KeepsHighestN(t *testing.T) {
	s := topTestSchema()
	top := NewBatchTop(s, 0, 3, false, 8)
	_ = top.Init(context.Background())
	defer top.Close()
	_ = top.Consume(context.Background(), mkTopBatch(s, []int64{5, 2, 8, 1, 7, 3}, nil))
	_ = top.Finalize(context.Background())
	got := drainTop(t, top)
	want := []int64{8, 7, 5}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("row %d: want %d, got %d (full: %v)", i, want[i], got[i], got)
		}
	}
}

func TestBatchTop_FewerInputThanN_ReturnsAll_InOrder(t *testing.T) {
	s := topTestSchema()
	top := NewBatchTop(s, 0, 5, true, 8)
	_ = top.Init(context.Background())
	defer top.Close()
	_ = top.Consume(context.Background(), mkTopBatch(s, []int64{3, 1, 2}, nil))
	_ = top.Finalize(context.Background())
	got := drainTop(t, top)
	want := []int64{1, 2, 3}
	if len(got) != len(want) {
		t.Fatalf("len: want %d, got %d (%v)", len(want), len(got), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("row %d: want %d, got %d", i, want[i], got[i])
		}
	}
}

func TestBatchTop_TieBreaker_Stable(t *testing.T) {
	// All values equal; only first 2 should be retained — earlier-row-wins.
	s := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleField, Name: "v", Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleField, Name: "seq", Type: vectorized.ColumnTypeInt64},
	})
	top := NewBatchTop(s, 0, 2, true, 8)
	_ = top.Init(context.Background())
	defer top.Close()

	b := vectorized.NewRecordBatch(s, 4)
	v := b.Columns[0].(*vectorized.TypedColumn[int64])
	seq := b.Columns[1].(*vectorized.TypedColumn[int64])
	for i := range 4 {
		v.Append(5)
		seq.Append(int64(i))
	}
	b.Len = 4
	_ = top.Consume(context.Background(), b)
	_ = top.Finalize(context.Background())

	out, _ := top.NextBatch(context.Background())
	if out.Len != 2 {
		t.Fatalf("Len: want 2, got %d", out.Len)
	}
	seqs := out.Columns[1].(*vectorized.TypedColumn[int64]).Data()
	// Expected: rows with seq 0 and 1 — the earlier ones.
	want := []int64{0, 1}
	for i := range want {
		if seqs[i] != want[i] {
			t.Fatalf("seq[%d]: want %d, got %d", i, want[i], seqs[i])
		}
	}
}

func TestBatchTop_NullField_TreatedAsLowest(t *testing.T) {
	s := topTestSchema()
	top := NewBatchTop(s, 0, 3, true, 8)
	_ = top.Init(context.Background())
	defer top.Close()
	// Values: [5, null, 3, 8, 1]. Asc top-3 should keep lowest: [null, 1, 3].
	_ = top.Consume(context.Background(), mkTopBatch(s,
		[]int64{5, 0, 3, 8, 1},
		[]bool{false, true, false, false, false},
	))
	_ = top.Finalize(context.Background())
	got := drainTop(t, top)
	// drainTop replaces null with -999 sentinel.
	if len(got) != 3 || got[0] != -999 {
		t.Fatalf("null must come first in asc order; got %v", got)
	}
	if got[1] != 1 || got[2] != 3 {
		t.Fatalf("rest of top-3: want [1, 3], got %v", got[1:])
	}
}

// Pins the regression flagged by Copilot: BatchTop with n <= 0 must be a
// no-op rather than panicking on first row.
func TestBatchTop_ZeroN_NoOp(t *testing.T) {
	s := topTestSchema()
	top := NewBatchTop(s, 0, 0, true, 8)
	_ = top.Init(context.Background())
	defer top.Close()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("BatchTop with n=0 must not panic; got %v", r)
		}
	}()
	if err := top.Consume(context.Background(), mkTopBatch(s, []int64{1, 2, 3}, nil)); err != nil {
		t.Fatal(err)
	}
	if err := top.Finalize(context.Background()); err != nil {
		t.Fatal(err)
	}
	out, err := top.NextBatch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if out != nil {
		t.Fatalf("BatchTop with n=0 must yield no output; got batch with Len=%d", out.Len)
	}
}

// TestBatchTop_FieldValuePassthroughKey_Float pins the G9a regression: the
// non-Agg Scan→Top→Limit path leaves the projected field as a passthrough
// ColumnTypeFieldValue column (BuildBatchSchema only promotes Agg fields to
// native typed columns). BatchTop must read the float sort key out of the
// *modelv1.FieldValue rather than panicking on a TypedColumn[int64] cast.
func TestBatchTop_FieldValuePassthroughKey_Float(t *testing.T) {
	s := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleField, Name: "value", Type: vectorized.ColumnTypeFieldValue},
	})
	top := NewBatchTop(s, 0, 3, false, 8) // desc top-3
	_ = top.Init(context.Background())
	defer top.Close()

	vals := []float64{12.5, 3.0, 99.25, 7.1, 42.0, 88.0}
	b := vectorized.NewRecordBatch(s, len(vals))
	col := b.Columns[0].(*vectorized.TypedColumn[*modelv1.FieldValue])
	for _, v := range vals {
		col.Append(&modelv1.FieldValue{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: v}}})
	}
	b.Len = len(vals)

	if err := top.Consume(context.Background(), b); err != nil {
		t.Fatalf("Consume must not panic on passthrough FieldValue key: %v", err)
	}
	if err := top.Finalize(context.Background()); err != nil {
		t.Fatal(err)
	}
	out, err := top.NextBatch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if out == nil || out.Len != 3 {
		t.Fatalf("want 3 rows, got %v", out)
	}
	got := out.Columns[0].(*vectorized.TypedColumn[*modelv1.FieldValue]).Data()
	want := []float64{99.25, 88.0, 42.0} // highest 3, descending
	for i := range want {
		if g := got[i].GetFloat().GetValue(); g != want[i] {
			t.Fatalf("row %d: want %v, got %v", i, want[i], g)
		}
	}
}

func TestBatchTop_Close_AfterPartialConsume_ReleasesHeapAllocation(t *testing.T) {
	s := topTestSchema()
	top := NewBatchTop(s, 0, 3, true, 8)
	_ = top.Init(context.Background())
	_ = top.Consume(context.Background(), mkTopBatch(s, []int64{5, 2, 8}, nil))
	if err := top.Close(); err != nil {
		t.Fatal(err)
	}
	// Inspect the (unexported) heap field via the type's behavior — calling
	// Close again must be a no-op and not panic.
	if err := top.Close(); err != nil {
		t.Fatalf("second Close must be no-op, got %v", err)
	}
}
