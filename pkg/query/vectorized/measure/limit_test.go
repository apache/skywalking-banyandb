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
