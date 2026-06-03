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
	"context"
	"errors"
	"fmt"
	"testing"
)

// fakePull is a test PullOperator that yields a hand-built sequence of batches.
type fakePull struct {
	pullErr  error
	schema   *BatchSchema
	batches  []*RecordBatch
	idx      int
	closeCnt int
	initCnt  int
}

func (f *fakePull) Init(_ context.Context) error { f.initCnt++; return nil }
func (f *fakePull) OutputSchema() *BatchSchema   { return f.schema }
func (f *fakePull) Close() error                 { f.closeCnt++; return nil }
func (f *fakePull) NextBatch(_ context.Context) (*RecordBatch, error) {
	if f.pullErr != nil {
		return nil, f.pullErr
	}
	if f.idx >= len(f.batches) {
		return nil, nil
	}
	b := f.batches[f.idx]
	f.idx++
	return b, nil
}

// fakeFusible is a test FusibleOperator. If processFn is set, it overrides the default
// "increment first column by 1". processErr makes Process fail before any mutation.
type fakeFusible struct {
	processErr error
	processFn  func(b *RecordBatch) error
	schema     *BatchSchema
	processed  int
	closeCnt   int
}

func (f *fakeFusible) Init(_ context.Context) error { return nil }
func (f *fakeFusible) OutputSchema() *BatchSchema   { return f.schema }
func (f *fakeFusible) Close() error                 { f.closeCnt++; return nil }
func (f *fakeFusible) Process(_ context.Context, b *RecordBatch) error {
	if f.processErr != nil {
		return f.processErr
	}
	f.processed++
	if f.processFn != nil {
		return f.processFn(b)
	}
	col := b.Columns[0].(*TypedColumn[int64])
	for i := range col.Data() {
		col.Data()[i]++
	}
	return nil
}

func mkInt64Batch(s *BatchSchema, vals ...int64) *RecordBatch {
	b := NewRecordBatch(s, len(vals))
	col := b.Columns[0].(*TypedColumn[int64])
	for _, v := range vals {
		col.Append(v)
	}
	b.Len = len(vals)
	return b
}

func TestFusedStage_HappyPath_AppliesEveryOperatorInOrder(t *testing.T) {
	s := NewBatchSchema([]ColumnDef{{Role: RoleTimestamp, Type: ColumnTypeInt64}})
	src := &fakePull{schema: s, batches: []*RecordBatch{mkInt64Batch(s, 1, 2, 3)}}
	f1 := &fakeFusible{schema: s} // +1
	f2 := &fakeFusible{schema: s} // +1
	stage := newFusedStage(src, []FusibleOperator{f1, f2})
	if err := stage.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	out, err := stage.NextBatch(context.Background())
	if err != nil || out == nil {
		t.Fatalf("happy path: err=%v out=%v", err, out)
	}
	got := out.Columns[0].(*TypedColumn[int64]).Data()
	want := []int64{3, 4, 5}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("row %d: want %d, got %d", i, want[i], got[i])
		}
	}
	if f1.processed != 1 || f2.processed != 1 {
		t.Fatalf("each fusible processed once: f1=%d f2=%d", f1.processed, f2.processed)
	}
}

func TestFusedStage_FirstFusibleErrors_ReturnsNilBatchAndError(t *testing.T) {
	s := NewBatchSchema([]ColumnDef{{Role: RoleTimestamp, Type: ColumnTypeInt64}})
	src := &fakePull{schema: s, batches: []*RecordBatch{mkInt64Batch(s, 1)}}
	boom := errors.New("first fusible boom")
	f1 := &fakeFusible{schema: s, processErr: boom}
	f2 := &fakeFusible{schema: s}
	stage := newFusedStage(src, []FusibleOperator{f1, f2})
	_ = stage.Init(context.Background())
	out, err := stage.NextBatch(context.Background())
	if !errors.Is(err, boom) {
		t.Fatalf("want first-fusible error, got %v", err)
	}
	if out != nil {
		t.Fatal("error path must return nil batch (R3: discard, do not pool)")
	}
	if f2.processed != 0 {
		t.Fatalf("second fusible must not run after first errors: processed=%d", f2.processed)
	}
}

func TestFusedStage_SecondFusibleErrors_StillReturnsNilBatch(t *testing.T) {
	s := NewBatchSchema([]ColumnDef{{Role: RoleTimestamp, Type: ColumnTypeInt64}})
	src := &fakePull{schema: s, batches: []*RecordBatch{mkInt64Batch(s, 1)}}
	boom := errors.New("second fusible boom")
	f1 := &fakeFusible{schema: s}
	f2 := &fakeFusible{schema: s, processErr: boom}
	stage := newFusedStage(src, []FusibleOperator{f1, f2})
	_ = stage.Init(context.Background())
	out, err := stage.NextBatch(context.Background())
	if !errors.Is(err, boom) {
		t.Fatalf("want second-fusible error, got %v", err)
	}
	if out != nil {
		t.Fatal("error path must return nil batch even after first fusible mutated")
	}
}

func TestFusedStage_LimitExhausted_EmitsCurrentBatchThenEOF(t *testing.T) {
	s := NewBatchSchema([]ColumnDef{{Role: RoleTimestamp, Type: ColumnTypeInt64}})
	src := &fakePull{schema: s, batches: []*RecordBatch{
		mkInt64Batch(s, 1, 2, 3),
		mkInt64Batch(s, 4, 5, 6),
	}}
	limiter := &fakeFusible{
		schema: s,
		processFn: func(_ *RecordBatch) error {
			return fmt.Errorf("limit done: %w", ErrLimitExhausted)
		},
	}
	stage := newFusedStage(src, []FusibleOperator{limiter})
	_ = stage.Init(context.Background())
	first, err := stage.NextBatch(context.Background())
	if err != nil {
		t.Fatalf("first call after limit-exhausted: want (b, nil), got err=%v", err)
	}
	if first == nil {
		t.Fatal("first call must emit the current batch with truncated selection")
	}
	second, err := stage.NextBatch(context.Background())
	if err != nil || second != nil {
		t.Fatalf("second call after limit-exhausted: want (nil, nil), got (%v, %v)", second, err)
	}
}

func TestFusedStage_SourceEOF_NoFusibleInvoked(t *testing.T) {
	s := NewBatchSchema([]ColumnDef{{Role: RoleTimestamp, Type: ColumnTypeInt64}})
	src := &fakePull{schema: s, batches: nil} // EOF immediately
	f := &fakeFusible{schema: s}
	stage := newFusedStage(src, []FusibleOperator{f})
	_ = stage.Init(context.Background())
	out, err := stage.NextBatch(context.Background())
	if err != nil || out != nil {
		t.Fatalf("source EOF: want (nil, nil), got (%v, %v)", out, err)
	}
	if f.processed != 0 {
		t.Fatalf("fusible must not run on EOF: processed=%d", f.processed)
	}
}

func TestFusedStage_SourceError_PropagatesUnchanged(t *testing.T) {
	s := NewBatchSchema([]ColumnDef{{Role: RoleTimestamp, Type: ColumnTypeInt64}})
	boom := errors.New("source boom")
	src := &fakePull{schema: s, pullErr: boom}
	f := &fakeFusible{schema: s}
	stage := newFusedStage(src, []FusibleOperator{f})
	_ = stage.Init(context.Background())
	out, err := stage.NextBatch(context.Background())
	if !errors.Is(err, boom) {
		t.Fatalf("want source error, got %v", err)
	}
	if out != nil {
		t.Fatal("source error must return nil batch")
	}
	if f.processed != 0 {
		t.Fatal("fusible must not run on source error")
	}
}

func TestFusedStage_Close_Idempotent_CallsChildrenOnce(t *testing.T) {
	s := NewBatchSchema([]ColumnDef{{Role: RoleTimestamp, Type: ColumnTypeInt64}})
	src := &fakePull{schema: s}
	f1 := &fakeFusible{schema: s}
	f2 := &fakeFusible{schema: s}
	stage := newFusedStage(src, []FusibleOperator{f1, f2})
	_ = stage.Close()
	_ = stage.Close()
	if src.closeCnt != 1 || f1.closeCnt != 1 || f2.closeCnt != 1 {
		t.Fatalf("Close must be idempotent (children invoked once): src=%d f1=%d f2=%d",
			src.closeCnt, f1.closeCnt, f2.closeCnt)
	}
}
