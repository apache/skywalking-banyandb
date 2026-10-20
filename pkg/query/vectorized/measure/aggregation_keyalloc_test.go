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
	"unsafe"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func aggKeyAllocBatch(schema *vectorized.BatchSchema, keys []string, shard bool) *vectorized.RecordBatch {
	b := vectorized.NewRecordBatch(schema, len(keys))
	for i, k := range keys {
		col := 0
		if shard {
			b.Columns[0].(*vectorized.TypedColumn[int64]).Append(1)
			col = 1
		}
		b.Columns[col].(*vectorized.TypedColumn[string]).Append(k)
		b.Columns[col+1].(*vectorized.TypedColumn[int64]).Append(int64(i + 1))
	}
	b.Len = len(keys)
	// An explicit selection keeps the per-batch activeIndices allocation out of the per-row measurement.
	for i := range keys {
		b.Selection = append(b.Selection, uint16(i))
	}
	return b
}

// TestBatchAggregation_ExistingGroupLookupDoesNotAllocate pins that the
// per-row group lookup (and the AggModeReduce replica-dedup lookup) builds its
// key in reusable scratch and only allocates an owned key on insert.
func TestBatchAggregation_ExistingGroupLookupDoesNotAllocate(t *testing.T) {
	keys := []string{"alpha", "beta", "gamma", "delta"}
	tracker := vectorized.NewMemoryTracker(1 << 30)
	ctx := context.Background()

	s := aggIntSchema()
	op := NewBatchAggregation(s, []int{0}, []AggSpec{{Func: AggSum, InputCol: 1, Output: "sum_v"}}, AggModeAll, 8, tracker, 0)
	defer op.Close()
	if err := op.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := op.Consume(ctx, aggKeyAllocBatch(s, keys, false)); err != nil {
		t.Fatal(err)
	}
	b := aggKeyAllocBatch(s, keys, false)
	if allocs := testing.AllocsPerRun(50, func() {
		if err := op.Consume(ctx, b); err != nil {
			t.Fatal(err)
		}
	}); allocs != 0 {
		t.Fatalf("AggModeAll Consume over existing groups allocs = %v, want 0", allocs)
	}

	// Reduce: replica duplicates are dropped via dedupSeen, which must also
	// look up without allocating once the pair is recorded.
	rs := aggReduceSchema()
	rop := NewBatchAggregation(rs, []int{1}, []AggSpec{{Func: AggSum, InputCol: 2, Output: "sum_v"}}, AggModeReduce, 8, tracker, 0)
	defer rop.Close()
	if err := rop.Init(ctx); err != nil {
		t.Fatal(err)
	}
	rb := aggKeyAllocBatch(rs, keys, true)
	if err := rop.Consume(ctx, rb); err != nil {
		t.Fatal(err)
	}
	if allocs := testing.AllocsPerRun(50, func() {
		if err := rop.Consume(ctx, rb); err != nil {
			t.Fatal(err)
		}
	}); allocs != 0 {
		t.Fatalf("AggModeReduce Consume over seen pairs allocs = %v, want 0", allocs)
	}
}

// TestBatchAggregation_KeysOwnedAfterSourceMutation proves retained group
// keys do not alias the key scratch or a borrowed string cell: the backing
// bytes of the input cells are overwritten after Consume and groups must still
// be matched by their original contents.
func TestBatchAggregation_KeysOwnedAfterSourceMutation(t *testing.T) {
	s := aggIntSchema()
	op := NewBatchAggregation(s, []int{0}, []AggSpec{{Func: AggSum, InputCol: 1, Output: "sum_v"}}, AggModeAll, 8,
		vectorized.NewMemoryTracker(1<<30), 0)
	defer op.Close()
	if err := op.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	backing := []byte("aaaa" + "bbbb")
	b := vectorized.NewRecordBatch(s, 2)
	b.Columns[0].(*vectorized.TypedColumn[string]).Append(unsafe.String(&backing[0], 4))
	b.Columns[1].(*vectorized.TypedColumn[int64]).Append(1)
	b.Columns[0].(*vectorized.TypedColumn[string]).Append(unsafe.String(&backing[4], 4))
	b.Columns[1].(*vectorized.TypedColumn[int64]).Append(2)
	b.Len = 2
	if err := op.Consume(ctx, b); err != nil {
		t.Fatal(err)
	}
	for i := range backing {
		backing[i] = 'X' // simulate released/poisoned storage memory
	}
	if err := op.Consume(ctx, aggKeyAllocBatch(s, []string{"aaaa", "bbbb", "cccc"}, false)); err != nil {
		t.Fatal(err)
	}
	if got := len(op.groups); got != 3 {
		t.Fatalf("groups = %d, want 3 (aaaa, bbbb, cccc)", got)
	}
}

// TestBatchTop_FullHeapDisplacementDoesNotAllocate pins that a row rejected by
// or displacing the root of a full heap recycles the root's storage instead of
// allocating per input row, and that the retained rows still own their cells.
func TestBatchTop_FullHeapDisplacementDoesNotAllocate(t *testing.T) {
	s := aggIntSchema()
	top := NewBatchTop(s, 1, 3, false, 8)
	ctx := context.Background()
	if err := top.Init(ctx); err != nil {
		t.Fatal(err)
	}
	defer top.Close()
	backing := []byte("k0k1k2k3k4k5k6k7")
	mk := func(base int64, n int) *vectorized.RecordBatch {
		b := vectorized.NewRecordBatch(s, n)
		for i := 0; i < n; i++ {
			b.Columns[0].(*vectorized.TypedColumn[string]).Append(unsafe.String(&backing[2*i], 2))
			b.Columns[1].(*vectorized.TypedColumn[int64]).Append(base + int64(i))
			b.Selection = append(b.Selection, uint16(i))
		}
		b.Len = n
		return b
	}
	if err := top.Consume(ctx, mk(0, 3)); err != nil {
		t.Fatal(err)
	}
	displacing, rejected := mk(100, 8), mk(-100, 8)
	if allocs := testing.AllocsPerRun(20, func() {
		if err := top.Consume(ctx, displacing); err != nil {
			t.Fatal(err)
		}
		if err := top.Consume(ctx, rejected); err != nil {
			t.Fatal(err)
		}
	}); allocs != 0 {
		t.Fatalf("full-heap Consume allocs = %v, want 0", allocs)
	}
	for i := range backing {
		backing[i] = 'X'
	}
	if err := top.Finalize(ctx); err != nil {
		t.Fatal(err)
	}
	out, err := top.NextBatch(ctx)
	if err != nil || out == nil || out.Len != 3 {
		t.Fatalf("NextBatch = %v, %v", out, err)
	}
	vals := out.Columns[1].(*vectorized.TypedColumn[int64]).Data()
	if vals[0] != 107 || vals[1] != 107 || vals[2] != 107 {
		t.Fatalf("top values = %v, want [107 107 107] (repeated batches converge on the max)", vals)
	}
	keys := out.Columns[0].(*vectorized.TypedColumn[string]).Data()
	if keys[0] != "k7" || keys[1] != "k7" || keys[2] != "k7" {
		t.Fatalf("retained keys = %v, want all k7; cells must own their bytes", keys)
	}
}
