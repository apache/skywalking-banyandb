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
	"testing"
)

// fakeBreaker is a test BreakerOperator that records its lifecycle and
// emits a hand-built sequence of output batches after Finalize.
type fakeBreaker struct {
	consumeErr  error
	finalizeErr error
	schema      *BatchSchema
	consumed    []*RecordBatch
	output      []*RecordBatch
	outIdx      int
	consumeCnt  int
	finalizeCnt int
	emitCnt     int
	closeCnt    int
}

func (b *fakeBreaker) Init(_ context.Context) error { return nil }
func (b *fakeBreaker) OutputSchema() *BatchSchema   { return b.schema }
func (b *fakeBreaker) Close() error                 { b.closeCnt++; return nil }
func (b *fakeBreaker) Consume(_ context.Context, batch *RecordBatch) error {
	b.consumeCnt++
	if b.consumeErr != nil {
		return b.consumeErr
	}
	b.consumed = append(b.consumed, batch)
	return nil
}
func (b *fakeBreaker) Finalize(_ context.Context) error { b.finalizeCnt++; return b.finalizeErr }
func (b *fakeBreaker) NextBatch(_ context.Context) (*RecordBatch, error) {
	if b.outIdx >= len(b.output) {
		return nil, nil
	}
	batch := b.output[b.outIdx]
	b.outIdx++
	b.emitCnt++
	return batch, nil
}

func TestPipelineBuilder_BuildWithoutSource_ReturnsError(t *testing.T) {
	if _, err := NewPipelineBuilder().Build(); err == nil {
		t.Fatal("Build without From() should return an error")
	}
}

func TestPipelineBuilder_FromOnly_BuildsSingleFusedStage(t *testing.T) {
	s := NewBatchSchema([]ColumnDef{{Role: RoleTimestamp, Type: ColumnTypeInt64}})
	src := &fakePull{schema: s, batches: []*RecordBatch{mkInt64Batch(s, 1)}}
	p, err := NewPipelineBuilder().From(src).Build()
	if err != nil {
		t.Fatal(err)
	}
	if initErr := p.head.Init(context.Background()); initErr != nil {
		t.Fatal(initErr)
	}
	out, err := p.Next(context.Background())
	if err != nil || out == nil {
		t.Fatalf("From-only pipeline should yield the source's batch: out=%v err=%v", out, err)
	}
	eof, err := p.Next(context.Background())
	if err != nil || eof != nil {
		t.Fatalf("expected EOF: out=%v err=%v", eof, err)
	}
}

func TestPipelineBuilder_FromApplyApply_BuildsFusedStageWithTwoFusibles(t *testing.T) {
	s := NewBatchSchema([]ColumnDef{{Role: RoleTimestamp, Type: ColumnTypeInt64}})
	src := &fakePull{schema: s, batches: []*RecordBatch{mkInt64Batch(s, 1, 2)}}
	f1 := &fakeFusible{schema: s}
	f2 := &fakeFusible{schema: s}
	p, err := NewPipelineBuilder().From(src).Apply(f1).Apply(f2).Build()
	if err != nil {
		t.Fatal(err)
	}
	_ = p.head.Init(context.Background())
	out, err := p.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if out.Columns[0].(*TypedColumn[int64]).Data()[0] != 3 {
		t.Fatalf("two fusibles should each +1 the row: got %d", out.Columns[0].(*TypedColumn[int64]).Data()[0])
	}
}

func TestPipelineBuilder_FromBreak_BuildsBreakerStageOnTopOfSource(t *testing.T) {
	s := NewBatchSchema([]ColumnDef{{Role: RoleTimestamp, Type: ColumnTypeInt64}})
	src := &fakePull{schema: s, batches: []*RecordBatch{mkInt64Batch(s, 1)}}
	br := &fakeBreaker{schema: s, output: []*RecordBatch{mkInt64Batch(s, 99)}}
	p, err := NewPipelineBuilder().From(src).Break(br).Build()
	if err != nil {
		t.Fatal(err)
	}
	_ = p.head.Init(context.Background())
	out, err := p.Next(context.Background())
	if err != nil || out == nil {
		t.Fatalf("breaker stage should emit output after draining: err=%v out=%v", err, out)
	}
	if out.Columns[0].(*TypedColumn[int64]).Data()[0] != 99 {
		t.Fatalf("expected breaker output 99, got %d", out.Columns[0].(*TypedColumn[int64]).Data()[0])
	}
}

func TestBreakerStage_DrainsUpstreamBeforeEmittingOutput(t *testing.T) {
	s := NewBatchSchema([]ColumnDef{{Role: RoleTimestamp, Type: ColumnTypeInt64}})
	src := &fakePull{schema: s, batches: []*RecordBatch{
		mkInt64Batch(s, 1),
		mkInt64Batch(s, 2),
		mkInt64Batch(s, 3),
	}}
	br := &fakeBreaker{schema: s, output: []*RecordBatch{mkInt64Batch(s, 100)}}
	p, _ := NewPipelineBuilder().From(src).Break(br).Build()
	_ = p.head.Init(context.Background())
	if br.consumeCnt != 0 || br.finalizeCnt != 0 {
		t.Fatal("Init must not invoke Consume/Finalize")
	}
	_, _ = p.Next(context.Background())
	if br.consumeCnt != 3 {
		t.Fatalf("breaker should Consume every upstream batch: got %d", br.consumeCnt)
	}
	if br.finalizeCnt != 1 {
		t.Fatalf("breaker should Finalize once: got %d", br.finalizeCnt)
	}
}

func TestBreakerStage_UpstreamError_ShortCircuitsBeforeFinalize(t *testing.T) {
	s := NewBatchSchema([]ColumnDef{{Role: RoleTimestamp, Type: ColumnTypeInt64}})
	boom := errors.New("upstream boom")
	src := &fakePull{schema: s, pullErr: boom}
	br := &fakeBreaker{schema: s}
	p, _ := NewPipelineBuilder().From(src).Break(br).Build()
	_ = p.head.Init(context.Background())
	_, err := p.Next(context.Background())
	if !errors.Is(err, boom) {
		t.Fatalf("want upstream error, got %v", err)
	}
	if br.finalizeCnt != 0 {
		t.Fatalf("Finalize must not run on upstream error: got %d", br.finalizeCnt)
	}
}

func TestBreakerStage_FinalizeError_StickyOnRetry(t *testing.T) {
	s := NewBatchSchema([]ColumnDef{{Role: RoleTimestamp, Type: ColumnTypeInt64}})
	src := &fakePull{schema: s} // EOF immediately
	boom := errors.New("finalize boom")
	br := &fakeBreaker{schema: s, finalizeErr: boom}
	p, _ := NewPipelineBuilder().From(src).Break(br).Build()
	_ = p.head.Init(context.Background())

	_, err1 := p.Next(context.Background())
	if !errors.Is(err1, boom) {
		t.Fatalf("first Next: want finalize error, got %v", err1)
	}
	_, err2 := p.Next(context.Background())
	if !errors.Is(err2, boom) {
		t.Fatalf("second Next must return sticky finalize error, got %v", err2)
	}
	if br.finalizeCnt != 1 {
		t.Fatalf("Finalize must be called exactly once across retries; got %d", br.finalizeCnt)
	}
}

func TestBreakerStage_ConsumeError_StickyOnRetry(t *testing.T) {
	s := NewBatchSchema([]ColumnDef{{Role: RoleTimestamp, Type: ColumnTypeInt64}})
	src := &fakePull{schema: s, batches: []*RecordBatch{mkInt64Batch(s, 1)}}
	boom := errors.New("consume boom")
	br := &fakeBreaker{schema: s, consumeErr: boom}
	p, _ := NewPipelineBuilder().From(src).Break(br).Build()
	_ = p.head.Init(context.Background())

	_, err1 := p.Next(context.Background())
	if !errors.Is(err1, boom) {
		t.Fatalf("first Next: want consume error, got %v", err1)
	}
	_, err2 := p.Next(context.Background())
	if !errors.Is(err2, boom) {
		t.Fatalf("second Next must return sticky consume error, got %v", err2)
	}
	if br.finalizeCnt != 0 {
		t.Fatalf("Finalize must not run after consume error; got %d", br.finalizeCnt)
	}
}

func TestBreakerStage_Close_Idempotent_CallsChildrenOnce(t *testing.T) {
	s := NewBatchSchema([]ColumnDef{{Role: RoleTimestamp, Type: ColumnTypeInt64}})
	src := &fakePull{schema: s}
	br := &fakeBreaker{schema: s}
	stage := newBreakerStage(src, br)
	_ = stage.Close()
	_ = stage.Close()
	if src.closeCnt != 1 || br.closeCnt != 1 {
		t.Fatalf("breakerStage.Close must be idempotent: src=%d br=%d", src.closeCnt, br.closeCnt)
	}
}

func TestPipeline_Close_Idempotent(t *testing.T) {
	s := NewBatchSchema([]ColumnDef{{Role: RoleTimestamp, Type: ColumnTypeInt64}})
	src := &fakePull{schema: s}
	p, _ := NewPipelineBuilder().From(src).Build()
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("second Close should be no-op, got %v", err)
	}
	if src.closeCnt != 1 {
		t.Fatalf("second Close must not propagate to source: got %d", src.closeCnt)
	}
}
