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
	"errors"
	"math"
	"testing"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// groupbyTestSchema is "tag.svc (string), field value (int64)".
func groupbyTestSchema() *vectorized.BatchSchema {
	return vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "svc", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleField, Name: "v", Type: vectorized.ColumnTypeInt64},
	})
}

// mkGroupbyBatch constructs a batch with one (svc, v) row per pair.
func mkGroupbyBatch(s *vectorized.BatchSchema, pairs ...struct {
	svc string
	v   int64
},
) *vectorized.RecordBatch {
	b := vectorized.NewRecordBatch(s, len(pairs))
	svcCol := b.Columns[0].(*vectorized.TypedColumn[string])
	vCol := b.Columns[1].(*vectorized.TypedColumn[int64])
	for _, p := range pairs {
		svcCol.Append(p.svc)
		vCol.Append(p.v)
	}
	b.Len = len(pairs)
	return b
}

// drainGroupBy pulls every output batch and returns them concatenated as
// (svc, v) pairs in emission order.
func drainGroupBy(t *testing.T, g *BatchGroupBy) []struct {
	svc string
	v   int64
} {
	t.Helper()
	var out []struct {
		svc string
		v   int64
	}
	for {
		b, err := g.NextBatch(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			break
		}
		svcCol := b.Columns[0].(*vectorized.TypedColumn[string])
		vCol := b.Columns[1].(*vectorized.TypedColumn[int64])
		for i := range b.Len {
			out = append(out, struct {
				svc string
				v   int64
			}{svcCol.Data()[i], vCol.Data()[i]})
		}
	}
	return out
}

func TestBatchGroupBy_SingleKeyColumn_GroupsByDistinctValue(t *testing.T) {
	s := groupbyTestSchema()
	pool := vectorized.NewBatchPool(s, 8)
	tracker := vectorized.NewMemoryTracker(1 << 20)
	g := NewBatchGroupBy(s, []int{0}, pool, 8, tracker, 64, 0)
	_ = g.Init(context.Background())
	defer g.Close()

	b := mkGroupbyBatch(s,
		struct {
			svc string
			v   int64
		}{"a", 1},
		struct {
			svc string
			v   int64
		}{"b", 2},
		struct {
			svc string
			v   int64
		}{"a", 3},
		struct {
			svc string
			v   int64
		}{"c", 4},
	)
	if err := g.Consume(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	if err := g.Finalize(context.Background()); err != nil {
		t.Fatal(err)
	}
	rows := drainGroupBy(t, g)
	// Expected emission order (insertion order): all of "a" rows, then "b", then "c".
	want := []string{"a", "a", "b", "c"}
	if len(rows) != len(want) {
		t.Fatalf("row count: want %d, got %d", len(want), len(rows))
	}
	for i, w := range want {
		if rows[i].svc != w {
			t.Fatalf("row %d svc: want %q, got %q", i, w, rows[i].svc)
		}
	}
}

func TestBatchGroupBy_MultipleKeyColumns_HashCombines(t *testing.T) {
	s := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "a", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "b", Type: vectorized.ColumnTypeString},
	})
	pool := vectorized.NewBatchPool(s, 8)
	tracker := vectorized.NewMemoryTracker(1 << 20)
	g := NewBatchGroupBy(s, []int{0, 1}, pool, 8, tracker, 64, 0)
	_ = g.Init(context.Background())
	defer g.Close()

	b := vectorized.NewRecordBatch(s, 4)
	b.Columns[0].(*vectorized.TypedColumn[string]).Append("x")
	b.Columns[1].(*vectorized.TypedColumn[string]).Append("1")
	b.Columns[0].(*vectorized.TypedColumn[string]).Append("x")
	b.Columns[1].(*vectorized.TypedColumn[string]).Append("2")
	b.Columns[0].(*vectorized.TypedColumn[string]).Append("x")
	b.Columns[1].(*vectorized.TypedColumn[string]).Append("1")
	b.Len = 3

	if err := g.Consume(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	if err := g.Finalize(context.Background()); err != nil {
		t.Fatal(err)
	}
	rows := drainGroupBy(t, &BatchGroupBy{}) // placeholder
	_ = rows
	// We have 2 distinct (a,b) groups: ("x","1") with 2 rows, ("x","2") with 1 row.
	// Re-pull through the configured g (the placeholder above is just a compile-test;
	// actual drain below).
	out, _ := g.NextBatch(context.Background())
	if out == nil {
		t.Fatal("expected output batch")
	}
	if out.Len != 3 {
		t.Fatalf("row count: want 3, got %d", out.Len)
	}
	bs := out.Columns[1].(*vectorized.TypedColumn[string]).Data()
	// Group order: ("x","1") rows first (2 rows), then ("x","2") (1 row).
	want := []string{"1", "1", "2"}
	for i, w := range want {
		if bs[i] != w {
			t.Fatalf("row %d b: want %q, got %q", i, w, bs[i])
		}
	}
}

func TestBatchGroupBy_RepeatedKeyAcrossBatches_AccumulatesIntoSameGroup(t *testing.T) {
	s := groupbyTestSchema()
	pool := vectorized.NewBatchPool(s, 8)
	tracker := vectorized.NewMemoryTracker(1 << 20)
	g := NewBatchGroupBy(s, []int{0}, pool, 8, tracker, 64, 0)
	_ = g.Init(context.Background())
	defer g.Close()

	_ = g.Consume(context.Background(), mkGroupbyBatch(s,
		struct {
			svc string
			v   int64
		}{"a", 1},
		struct {
			svc string
			v   int64
		}{"b", 2},
	))
	_ = g.Consume(context.Background(), mkGroupbyBatch(s,
		struct {
			svc string
			v   int64
		}{"a", 3}, // same group as first batch's "a"
	))
	_ = g.Finalize(context.Background())

	rows := drainGroupBy(t, g)
	// Expected: "a" group has 2 rows, "b" has 1; insertion order ["a","b"].
	want := []string{"a", "a", "b"}
	if len(rows) != len(want) {
		t.Fatalf("row count: want %d, got %d", len(want), len(rows))
	}
	for i, w := range want {
		if rows[i].svc != w {
			t.Fatalf("row %d svc: want %q, got %q", i, w, rows[i].svc)
		}
	}
}

func TestBatchGroupBy_MemoryReserve_FailsAtLimit_BubblesError(t *testing.T) {
	s := groupbyTestSchema()
	pool := vectorized.NewBatchPool(s, 8)
	tracker := vectorized.NewMemoryTracker(50) // tight
	g := NewBatchGroupBy(s, []int{0}, pool, 8, tracker, 100, 0)
	_ = g.Init(context.Background())
	defer g.Close()

	b := mkGroupbyBatch(s, struct {
		svc string
		v   int64
	}{"a", 1})
	err := g.Consume(context.Background(), b)
	if err == nil {
		t.Fatal("Consume must surface tracker error when reserve exceeds limit")
	}
}

func TestBatchGroupBy_MemoryRefund_OnRepeatedKeys_LeavesUsedAccurate(t *testing.T) {
	s := groupbyTestSchema()
	pool := vectorized.NewBatchPool(s, 8)
	tracker := vectorized.NewMemoryTracker(1 << 20)
	const entrySize int64 = 100
	g := NewBatchGroupBy(s, []int{0}, pool, 8, tracker, entrySize, 0)
	_ = g.Init(context.Background())
	defer g.Close()

	// Consume a batch with all 4 rows in the same group → only 1 new group.
	// Worst-case reserve = 4 * 100 = 400; actual usage = 1 * 100 = 100.
	// After refund, tracker.Used must be 100.
	b := mkGroupbyBatch(s,
		struct {
			svc string
			v   int64
		}{"a", 1},
		struct {
			svc string
			v   int64
		}{"a", 2},
		struct {
			svc string
			v   int64
		}{"a", 3},
		struct {
			svc string
			v   int64
		}{"a", 4},
	)
	if err := g.Consume(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	if got := tracker.Used(); got != entrySize {
		t.Fatalf("tracker.Used after refund: want %d, got %d", entrySize, got)
	}
}

func TestBatchGroupBy_NextBatch_PaginatesGroupsAcrossOutputBatches(t *testing.T) {
	s := groupbyTestSchema()
	pool := vectorized.NewBatchPool(s, 2)
	tracker := vectorized.NewMemoryTracker(1 << 20)
	g := NewBatchGroupBy(s, []int{0}, pool, 2, tracker, 64, 0) // batchSize=2
	_ = g.Init(context.Background())
	defer g.Close()

	_ = g.Consume(context.Background(), mkGroupbyBatch(s,
		struct {
			svc string
			v   int64
		}{"a", 1},
		struct {
			svc string
			v   int64
		}{"b", 2},
		struct {
			svc string
			v   int64
		}{"c", 3},
		struct {
			svc string
			v   int64
		}{"d", 4},
		struct {
			svc string
			v   int64
		}{"e", 5},
	))
	_ = g.Finalize(context.Background())

	var batches int
	for {
		b, err := g.NextBatch(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			break
		}
		batches++
		if b.Len > 2 {
			t.Fatalf("batch %d Len exceeds batchSize: %d", batches, b.Len)
		}
	}
	if batches < 3 {
		t.Fatalf("5 groups @ batchSize=2 should yield ≥3 batches, got %d", batches)
	}
}

// Pins the regression flagged by Copilot: NUL-byte separators alone cannot
// disambiguate string tuples whose components contain embedded NUL bytes.
// Distinct tuples like ("a\x00b","c") and ("a","b\x00c") must NOT collapse
// into one group.
func TestBatchGroupBy_NULInStringKey_NoCollision(t *testing.T) {
	s := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "a", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "b", Type: vectorized.ColumnTypeString},
	})
	pool := vectorized.NewBatchPool(s, 8)
	tracker := vectorized.NewMemoryTracker(1 << 20)
	const entrySize = int64(64)
	g := NewBatchGroupBy(s, []int{0, 1}, pool, 8, tracker, entrySize, 0)
	_ = g.Init(context.Background())
	defer g.Close()

	b := vectorized.NewRecordBatch(s, 2)
	b.Columns[0].(*vectorized.TypedColumn[string]).Append("a\x00b")
	b.Columns[1].(*vectorized.TypedColumn[string]).Append("c")
	b.Columns[0].(*vectorized.TypedColumn[string]).Append("a")
	b.Columns[1].(*vectorized.TypedColumn[string]).Append("b\x00c")
	b.Len = 2

	if err := g.Consume(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	if got := tracker.Used(); got != 2*entrySize {
		t.Fatalf("two distinct (a,b) tuples must produce two groups; tracker.Used=%d, want %d (NUL-in-key collision merged the rows)",
			got, 2*entrySize)
	}
}

// Pins the regression flagged by Copilot: float keys must canonicalize +0.0
// and -0.0 before hashing so they fall into the same group, matching what
// BatchAggregation already does.
func TestBatchGroupBy_PositiveAndNegativeZero_SameGroup(t *testing.T) {
	s := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "k", Type: vectorized.ColumnTypeFloat64},
	})
	pool := vectorized.NewBatchPool(s, 8)
	tracker := vectorized.NewMemoryTracker(1 << 20)
	const entrySize = int64(64)
	g := NewBatchGroupBy(s, []int{0}, pool, 8, tracker, entrySize, 0)
	_ = g.Init(context.Background())
	defer g.Close()

	b := vectorized.NewRecordBatch(s, 2)
	b.Columns[0].(*vectorized.TypedColumn[float64]).Append(0.0)
	b.Columns[0].(*vectorized.TypedColumn[float64]).Append(math.Copysign(0, -1))
	b.Len = 2

	if err := g.Consume(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	if got := tracker.Used(); got != entrySize {
		t.Fatalf("+0 and -0 must produce the same group; tracker.Used=%d, want %d",
			got, entrySize)
	}
}

func TestBatchGroupBy_Close_AfterPartialConsume_ReleasesEveryReservation(t *testing.T) {
	s := groupbyTestSchema()
	pool := vectorized.NewBatchPool(s, 8)
	tracker := vectorized.NewMemoryTracker(1 << 20)
	g := NewBatchGroupBy(s, []int{0}, pool, 8, tracker, 100, 0)
	_ = g.Init(context.Background())

	_ = g.Consume(context.Background(), mkGroupbyBatch(s,
		struct {
			svc string
			v   int64
		}{"a", 1},
		struct {
			svc string
			v   int64
		}{"b", 2},
	))
	if tracker.Used() == 0 {
		t.Fatal("tracker should have outstanding reservation after Consume")
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	if got := tracker.Used(); got != 0 {
		t.Fatalf("Close after partial Consume must release everything: tracker.Used=%d", got)
	}
}

func TestBatchGroupBy_Close_Idempotent_NoDoubleRelease(t *testing.T) {
	s := groupbyTestSchema()
	pool := vectorized.NewBatchPool(s, 8)
	tracker := vectorized.NewMemoryTracker(1 << 20)
	g := NewBatchGroupBy(s, []int{0}, pool, 8, tracker, 100, 0)
	_ = g.Init(context.Background())

	_ = g.Consume(context.Background(), mkGroupbyBatch(s, struct {
		svc string
		v   int64
	}{"a", 1}))
	_ = g.Close()
	used1 := tracker.Used()
	_ = g.Close() // second Close should not double-release
	used2 := tracker.Used()
	if used1 != used2 {
		t.Fatalf("second Close changed tracker.Used: before=%d after=%d", used1, used2)
	}
	if !errors.Is(nil, nil) {
		_ = used2 // unused-import guard
	}
}
