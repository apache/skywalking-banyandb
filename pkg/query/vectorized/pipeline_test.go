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

// TestPipelineBuilder_ApplyAfterBreak_RunsAfterBreaker pins the
// observable Apply/Break stage ordering. A fusible added after a Break
// must execute on the breaker's output, not on the raw source rows —
// otherwise a Limit Apply'd after a GroupByAgg Break would clip source
// batches before aggregation and silently return wrong groups.
func TestPipelineBuilder_ApplyAfterBreak_RunsAfterBreaker(t *testing.T) {
	s := NewBatchSchema([]ColumnDef{{Role: RoleTimestamp, Type: ColumnTypeInt64}})
	src := &fakePull{schema: s, batches: []*RecordBatch{mkInt64Batch(s, 1, 2)}}
	var preSeen, postSeen []int64
	pre := &fakeFusible{schema: s, processFn: func(b *RecordBatch) error {
		c := b.Columns[0].(*TypedColumn[int64])
		preSeen = append(preSeen, c.Data()...)
		return nil
	}}
	br := &fakeBreaker{schema: s, output: []*RecordBatch{mkInt64Batch(s, 99)}}
	post := &fakeFusible{schema: s, processFn: func(b *RecordBatch) error {
		c := b.Columns[0].(*TypedColumn[int64])
		postSeen = append(postSeen, c.Data()...)
		return nil
	}}
	p, err := NewPipelineBuilder().From(src).Apply(pre).Break(br).Apply(post).Build()
	if err != nil {
		t.Fatal(err)
	}
	_ = p.head.Init(context.Background())
	for {
		next, e := p.Next(context.Background())
		if e != nil {
			t.Fatal(e)
		}
		if next == nil {
			break
		}
	}
	wantPre := []int64{1, 2}
	if len(preSeen) != len(wantPre) || preSeen[0] != wantPre[0] || preSeen[1] != wantPre[1] {
		t.Fatalf("pre-break fusible should see source rows %v, got %v", wantPre, preSeen)
	}
	wantPost := []int64{99}
	if len(postSeen) != len(wantPost) || postSeen[0] != wantPost[0] {
		t.Fatalf("post-break fusible should see breaker output %v (not source rows), got %v", wantPost, postSeen)
	}
}

// doublingPull wraps an upstream PullOperator and doubles every int64 value
// it sees — a minimal stand-in for an operator that genuinely transforms
// its upstream via NextBatch, the shape Transform exists for.
type doublingPull struct {
	upstream PullOperator
}

func (d *doublingPull) Init(ctx context.Context) error { return d.upstream.Init(ctx) }
func (d *doublingPull) OutputSchema() *BatchSchema     { return d.upstream.OutputSchema() }
func (d *doublingPull) Close() error                   { return d.upstream.Close() }
func (d *doublingPull) NextBatch(ctx context.Context) (*RecordBatch, error) {
	b, err := d.upstream.NextBatch(ctx)
	if err != nil || b == nil {
		return b, err
	}
	col := b.Columns[0].(*TypedColumn[int64])
	data := col.Data()
	for i, v := range data {
		col.SetAt(i, v*2)
	}
	return b, nil
}

func TestPipelineBuilder_Transform_WrapsSourceDirectly(t *testing.T) {
	s := NewBatchSchema([]ColumnDef{{Role: RoleTimestamp, Type: ColumnTypeInt64}})
	src := &fakePull{schema: s, batches: []*RecordBatch{mkInt64Batch(s, 1, 2, 3)}}
	p, err := NewPipelineBuilder().
		From(src).
		Transform(func(upstream PullOperator) PullOperator { return &doublingPull{upstream: upstream} }).
		Build()
	if err != nil {
		t.Fatal(err)
	}
	if initErr := p.head.Init(context.Background()); initErr != nil {
		t.Fatal(initErr)
	}
	out, err := p.Next(context.Background())
	if err != nil || out == nil {
		t.Fatalf("Transform-wrapped pipeline should yield a batch: out=%v err=%v", out, err)
	}
	got := out.Columns[0].(*TypedColumn[int64]).Data()
	want := []int64{2, 4, 6}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Transform should apply doublingPull to the source's rows: got %v, want %v", got, want)
		}
	}
}

// TestPipelineBuilder_Transform_ClosesPendingSegmentFirst pins that
// Transform sees the fully-fused upstream — any Applied fusibles queued
// before it must already have run — not the raw source.
func TestPipelineBuilder_Transform_ClosesPendingSegmentFirst(t *testing.T) {
	s := NewBatchSchema([]ColumnDef{{Role: RoleTimestamp, Type: ColumnTypeInt64}})
	src := &fakePull{schema: s, batches: []*RecordBatch{mkInt64Batch(s, 1, 2, 3)}}
	incrementBeforeTransform := &fakeFusible{schema: s} // +1
	p, err := NewPipelineBuilder().
		From(src).
		Apply(incrementBeforeTransform).
		Transform(func(upstream PullOperator) PullOperator { return &doublingPull{upstream: upstream} }).
		Build()
	if err != nil {
		t.Fatal(err)
	}
	_ = p.head.Init(context.Background())
	out, err := p.Next(context.Background())
	if err != nil || out == nil {
		t.Fatalf("pipeline should yield a batch: out=%v err=%v", out, err)
	}
	got := out.Columns[0].(*TypedColumn[int64]).Data()
	want := []int64{4, 6, 8} // (v+1)*2
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Transform's upstream must be the fully-fused chain (+1 then *2): got %v, want %v", got, want)
		}
	}
}

// TestPipelineBuilder_BreakAfterTransform_RunsOnTransformedOutput pins that
// a Break following a Transform drains the Transform-wrapped operator, not
// the original source — proving Transform correctly becomes the new base
// for subsequent stages.
func TestPipelineBuilder_BreakAfterTransform_RunsOnTransformedOutput(t *testing.T) {
	s := NewBatchSchema([]ColumnDef{{Role: RoleTimestamp, Type: ColumnTypeInt64}})
	src := &fakePull{schema: s, batches: []*RecordBatch{mkInt64Batch(s, 1, 2, 3)}}
	br := &fakeBreaker{schema: s}
	p, err := NewPipelineBuilder().
		From(src).
		Transform(func(upstream PullOperator) PullOperator { return &doublingPull{upstream: upstream} }).
		Break(br).
		Build()
	if err != nil {
		t.Fatal(err)
	}
	_ = p.head.Init(context.Background())
	_, _ = p.Next(context.Background())
	if len(br.consumed) != 1 {
		t.Fatalf("breaker should have consumed exactly 1 batch, got %d", len(br.consumed))
	}
	got := br.consumed[0].Columns[0].(*TypedColumn[int64]).Data()
	want := []int64{2, 4, 6}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("breaker after Transform should see doubled rows: got %v, want %v", got, want)
		}
	}
}
