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

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// fakeBatchResult yields a hand-built sequence of *MeasureBatch
// instances for BatchSourceFromBatchResult tests. The interface is
// minimal — PullBatch returns the next batch in seq, then nil.
type fakeBatchResult struct {
	err        error
	seq        []*model.MeasureBatch
	idx        int
	releaseCnt int
}

func (f *fakeBatchResult) PullBatch(_ context.Context) (*model.MeasureBatch, error) {
	if f.err != nil {
		return nil, f.err
	}
	if f.idx >= len(f.seq) {
		return nil, nil
	}
	b := f.seq[f.idx]
	f.idx++
	return b, nil
}

func (f *fakeBatchResult) Release() {
	f.releaseCnt++
}

// passthroughSchema mirrors what BuildBatchSchema (G5a) emits — a single
// passthrough tag column + one passthrough field column on top of the
// metadata roles.
func passthroughSchema() *vectorized.BatchSchema {
	return vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleTimestamp, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleVersion, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleSeriesID, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleShardID, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "svc", Type: vectorized.ColumnTypeTagValue},
		{Role: vectorized.RoleField, Name: "value", Type: vectorized.ColumnTypeFieldValue},
	})
}

// makePassthroughBatch constructs a MeasureBatch with the passthrough
// schema, populating ts/version/sid/shard, one tag value (svc=alpha),
// and one field value (value=int 42) per row.
func makePassthroughBatch(schema *vectorized.BatchSchema, sid common.SeriesID, baseTS int64, n int) *model.MeasureBatch {
	tagCol := vectorized.NewTagValueColumn(n)
	fldCol := vectorized.NewFieldValueColumn(n)
	tv := &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "alpha"}}}
	fv := &modelv1.FieldValue{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 42}}}
	timestamps := make([]int64, n)
	versions := make([]int64, n)
	shardIDs := make([]common.ShardID, n)
	seriesIDs := make([]common.SeriesID, n)
	for i := range n {
		timestamps[i] = baseTS + int64(i)
		versions[i] = 1
		seriesIDs[i] = sid
		tagCol.Append(tv)
		fldCol.Append(fv)
	}
	return &model.MeasureBatch{
		Schema:     schema,
		Timestamps: timestamps,
		Versions:   versions,
		ShardIDs:   shardIDs,
		SeriesIDs:  seriesIDs,
		Tags:       []vectorized.Column{tagCol},
		Fields:     []vectorized.Column{fldCol},
	}
}

func TestBatchSource_EOFOnEmptySource(t *testing.T) {
	schema := passthroughSchema()
	pool := vectorized.NewBatchPool(schema, 4)
	src := NewBatchSourceFromBatchResult(&fakeBatchResult{}, schema, pool, 4)
	defer src.Close()
	b, err := src.NextBatch(context.Background())
	if err != nil {
		t.Fatalf("EOF must not error; got %v", err)
	}
	if b != nil {
		t.Fatalf("EOF must yield nil batch; got %v", b)
	}
}

func TestBatchSource_SingleBatchSmallerThanBatchSize(t *testing.T) {
	schema := passthroughSchema()
	pool := vectorized.NewBatchPool(schema, 8)
	mb := makePassthroughBatch(schema, 1, 100, 3)
	src := NewBatchSourceFromBatchResult(&fakeBatchResult{seq: []*model.MeasureBatch{mb}},
		schema, pool, 8)
	defer src.Close()

	b, err := src.NextBatch(context.Background())
	if err != nil {
		t.Fatalf("NextBatch: %v", err)
	}
	if b == nil {
		t.Fatal("first NextBatch must return a non-nil batch")
	}
	if b.Len != 3 {
		t.Fatalf("Len: want 3, got %d", b.Len)
	}
	// Metadata copied through.
	if got := b.Columns[schema.TimestampIndex()].(*vectorized.TypedColumn[int64]).Data(); !equalsInt64s(got, []int64{100, 101, 102}) {
		t.Fatalf("Timestamps: want [100 101 102], got %v", got)
	}
	if got := b.Columns[schema.SeriesIDIndex()].(*vectorized.TypedColumn[int64]).Data(); !equalsInt64s(got, []int64{1, 1, 1}) {
		t.Fatalf("SeriesIDs: want [1 1 1], got %v", got)
	}
	// Subsequent call: EOF.
	next, err2 := src.NextBatch(context.Background())
	if err2 != nil {
		t.Fatalf("post-EOF NextBatch: %v", err2)
	}
	if next != nil {
		t.Fatalf("post-EOF must return nil; got %v", next)
	}
}

func TestBatchSource_AccumulatesAcrossPulls(t *testing.T) {
	schema := passthroughSchema()
	pool := vectorized.NewBatchPool(schema, 4)
	a := makePassthroughBatch(schema, 1, 100, 2)
	b := makePassthroughBatch(schema, 2, 200, 3)
	src := NewBatchSourceFromBatchResult(&fakeBatchResult{seq: []*model.MeasureBatch{a, b}},
		schema, pool, 4)
	defer src.Close()

	first, err := src.NextBatch(context.Background())
	if err != nil {
		t.Fatalf("NextBatch1: %v", err)
	}
	if first == nil || first.Len != 4 {
		t.Fatalf("first batch Len: want 4, got %v", first)
	}
	second, err := src.NextBatch(context.Background())
	if err != nil {
		t.Fatalf("NextBatch2: %v", err)
	}
	if second == nil || second.Len != 1 {
		t.Fatalf("second batch Len: want 1, got %v", second)
	}
	final, err := src.NextBatch(context.Background())
	if err != nil {
		t.Fatalf("NextBatch3: %v", err)
	}
	if final != nil {
		t.Fatalf("third call must EOF; got %v", final)
	}
}

func TestBatchSource_StickyError(t *testing.T) {
	boom := errors.New("scan failure")
	schema := passthroughSchema()
	pool := vectorized.NewBatchPool(schema, 4)
	src := NewBatchSourceFromBatchResult(&fakeBatchResult{err: boom}, schema, pool, 4)
	defer src.Close()
	if _, err := src.NextBatch(context.Background()); !errors.Is(err, boom) {
		t.Fatalf("first call: want %v, got %v", boom, err)
	}
	if _, err := src.NextBatch(context.Background()); !errors.Is(err, boom) {
		t.Fatalf("second call must surface sticky error; got %v", err)
	}
}

func TestBatchSource_CloseReleasesSource(t *testing.T) {
	schema := passthroughSchema()
	pool := vectorized.NewBatchPool(schema, 4)
	fr := &fakeBatchResult{}
	src := NewBatchSourceFromBatchResult(fr, schema, pool, 4)
	if err := src.Close(); err != nil {
		t.Fatal(err)
	}
	if err := src.Close(); err != nil {
		t.Fatalf("second Close must be no-op; got %v", err)
	}
	if fr.releaseCnt != 1 {
		t.Fatalf("Release called %d times; want 1 (idempotent Close)", fr.releaseCnt)
	}
}

func TestBatchSource_PassthroughTagFieldPointersPreserved(t *testing.T) {
	// Passthrough columns must not deep-copy *modelv1.TagValue or
	// *modelv1.FieldValue pointers — the storage emits the canonical
	// pointer and the serializer expects it back unchanged.
	schema := passthroughSchema()
	pool := vectorized.NewBatchPool(schema, 4)
	mb := makePassthroughBatch(schema, 1, 100, 2)
	srcTag := mb.Tags[0].(*vectorized.TypedColumn[*modelv1.TagValue]).Data()[0]
	srcFld := mb.Fields[0].(*vectorized.TypedColumn[*modelv1.FieldValue]).Data()[0]
	src := NewBatchSourceFromBatchResult(&fakeBatchResult{seq: []*model.MeasureBatch{mb}},
		schema, pool, 4)
	defer src.Close()
	b, _ := src.NextBatch(context.Background())
	tagIdx, _ := schema.TagIndex("default", "svc")
	fldIdx, _ := schema.FieldIndex("value")
	gotTag := b.Columns[tagIdx].(*vectorized.TypedColumn[*modelv1.TagValue]).Data()[0]
	gotFld := b.Columns[fldIdx].(*vectorized.TypedColumn[*modelv1.FieldValue]).Data()[0]
	if gotTag != srcTag {
		t.Fatalf("tag pointer was copied; want passthrough preservation")
	}
	if gotFld != srcFld {
		t.Fatalf("field pointer was copied; want passthrough preservation")
	}
}

func equalsInt64s(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
