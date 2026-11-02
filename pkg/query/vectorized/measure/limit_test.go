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

func limitTestSchema() *vectorized.BatchSchema {
	return vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleTimestamp, Type: vectorized.ColumnTypeInt64},
	})
}

func mkBatchN(s *vectorized.BatchSchema, n int) *vectorized.RecordBatch {
	b := vectorized.NewRecordBatch(s, n)
	col := b.Columns[0].(*vectorized.TypedColumn[int64])
	for i := range n {
		col.Append(int64(i))
	}
	b.Len = n
	return b
}

func processAllowExhausted(t *testing.T, op *BatchLimit, b *vectorized.RecordBatch) {
	t.Helper()
	err := op.Process(context.Background(), b)
	if err != nil && !errors.Is(err, vectorized.ErrLimitExhausted) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBatchLimit_OffsetZero_LimitN_KeepsFirstN(t *testing.T) {
	s := limitTestSchema()
	op := NewBatchLimit(s, 0, 3)
	_ = op.Init(context.Background())
	b := mkBatchN(s, 5)
	processAllowExhausted(t, op, b) // limit closes mid-batch
	if got := b.ActiveLen(); got != 3 {
		t.Fatalf("ActiveLen: want 3, got %d", got)
	}
	for i, want := range []uint16{0, 1, 2} {
		if b.Selection[i] != want {
			t.Fatalf("selection[%d]: got %d, want %d", i, b.Selection[i], want)
		}
	}
}

func TestBatchLimit_OffsetN_LimitM_KeepsRowsNToNPlusM(t *testing.T) {
	s := limitTestSchema()
	op := NewBatchLimit(s, 2, 2)
	_ = op.Init(context.Background())
	b := mkBatchN(s, 5)
	processAllowExhausted(t, op, b)
	if got := b.ActiveLen(); got != 2 {
		t.Fatalf("ActiveLen: want 2, got %d", got)
	}
	for i, want := range []uint16{2, 3} {
		if b.Selection[i] != want {
			t.Fatalf("selection[%d]: got %d, want %d", i, b.Selection[i], want)
		}
	}
}

func TestBatchLimit_OffsetBeyondData_EmptyNonNilSelection(t *testing.T) {
	s := limitTestSchema()
	op := NewBatchLimit(s, 100, 5)
	_ = op.Init(context.Background())
	b := mkBatchN(s, 3)
	_ = op.Process(context.Background(), b)
	if b.Selection == nil {
		t.Fatal("Selection must be non-nil empty slice, got nil")
	}
	if len(b.Selection) != 0 {
		t.Fatalf("Selection must be empty, got %v", b.Selection)
	}
}

func TestBatchLimit_LimitExhausted_ReturnsErrLimitExhausted_AndCurrentBatchSliced(t *testing.T) {
	s := limitTestSchema()
	op := NewBatchLimit(s, 0, 2)
	_ = op.Init(context.Background())
	b := mkBatchN(s, 5)
	err := op.Process(context.Background(), b)
	if !errors.Is(err, vectorized.ErrLimitExhausted) {
		t.Fatalf("want ErrLimitExhausted, got %v", err)
	}
	if got := b.ActiveLen(); got != 2 {
		t.Fatalf("current batch must be sliced to limit: got ActiveLen=%d", got)
	}
}

func TestBatchLimit_PriorSelectionRespected(t *testing.T) {
	s := limitTestSchema()
	op := NewBatchLimit(s, 0, 2)
	_ = op.Init(context.Background())
	b := mkBatchN(s, 5)
	b.Selection = []uint16{1, 3, 4} // 3 active rows
	processAllowExhausted(t, op, b)
	if got := b.ActiveLen(); got != 2 {
		t.Fatalf("limit windows over active rows; ActiveLen want 2, got %d", got)
	}
	for i, want := range []uint16{1, 3} {
		if b.Selection[i] != want {
			t.Fatalf("selection[%d]: got %d, want %d", i, b.Selection[i], want)
		}
	}
}

func TestBatchLimit_AcrossMultipleBatches_StateCarriesViaSeenCounter(t *testing.T) {
	s := limitTestSchema()
	op := NewBatchLimit(s, 1, 4)
	_ = op.Init(context.Background())

	// Batch 1 has 3 rows. After Process: rows seen=3, kept indices [1, 2] (offset 1, count 2).
	b1 := mkBatchN(s, 3)
	if err := op.Process(context.Background(), b1); err != nil {
		t.Fatalf("batch 1: %v", err)
	}
	if got := b1.ActiveLen(); got != 2 {
		t.Fatalf("batch 1: ActiveLen want 2, got %d", got)
	}

	// Batch 2 has 4 rows. State: seen=3, want 2 more. After Process: rows seen=5, last 2
	// admitted; remaining 2 dropped via ErrLimitExhausted on next batch start? Actually
	// limit closes on this batch — should return ErrLimitExhausted with selection sliced.
	b2 := mkBatchN(s, 4)
	err := op.Process(context.Background(), b2)
	if !errors.Is(err, vectorized.ErrLimitExhausted) {
		t.Fatalf("batch 2: want ErrLimitExhausted, got %v", err)
	}
	if got := b2.ActiveLen(); got != 2 {
		t.Fatalf("batch 2: tail kept rows want 2, got %d", got)
	}
}

// TestActiveIndices_AtMaxNilSelectionLen_Passes pins the safe edge of design
// §5's boundary: a uint16 fully covers [0, maxNilSelectionLen), so a nil
// Selection at exactly that length must still materialize correctly, with
// no error and no wrap.
func TestActiveIndices_AtMaxNilSelectionLen_Passes(t *testing.T) {
	b := &vectorized.RecordBatch{Len: maxNilSelectionLen}
	active, err := activeIndices(b)
	if err != nil {
		t.Fatalf("Len == maxNilSelectionLen must not error, got %v", err)
	}
	if len(active) != maxNilSelectionLen {
		t.Fatalf("len(active) = %d, want %d", len(active), maxNilSelectionLen)
	}
	if active[0] != 0 || active[len(active)-1] != math.MaxUint16 {
		t.Fatalf("active must cover [0, MaxUint16] with no wrap, got first=%d last=%d", active[0], active[len(active)-1])
	}
}

// TestActiveIndices_PastMaxNilSelectionLen_Errors is C3: past the boundary a
// nil Selection can no longer be represented as []uint16 at all, so
// activeIndices must fail loudly instead of silently wrapping.
func TestActiveIndices_PastMaxNilSelectionLen_Errors(t *testing.T) {
	b := &vectorized.RecordBatch{Len: maxNilSelectionLen + 1}
	if _, err := activeIndices(b); err == nil {
		t.Fatal("Len > maxNilSelectionLen with a nil Selection must error, not silently wrap")
	}
}

// TestActiveIndices_NonNilSelection_NeverErrorsRegardlessOfLen pins that the
// guard is specific to the nil-Selection materialization path: an existing
// Selection is returned as-is, so Len is irrelevant to it.
func TestActiveIndices_NonNilSelection_NeverErrorsRegardlessOfLen(t *testing.T) {
	sel := []uint16{5, 9, 12}
	b := &vectorized.RecordBatch{Len: maxNilSelectionLen + 1, Selection: sel}
	active, err := activeIndices(b)
	if err != nil {
		t.Fatalf("non-nil Selection must never error, got %v", err)
	}
	if len(active) != len(sel) {
		t.Fatalf("active = %v, want %v", active, sel)
	}
}
