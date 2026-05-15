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

package model

import (
	"context"
	"errors"
	"testing"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func TestMeasureBatch_RowCount_Nil(t *testing.T) {
	var b *MeasureBatch
	if got := b.RowCount(); got != 0 {
		t.Fatalf("nil batch RowCount: want 0, got %d", got)
	}
}

func TestMeasureBatch_RowCount_Empty(t *testing.T) {
	b := &MeasureBatch{}
	if got := b.RowCount(); got != 0 {
		t.Fatalf("empty batch RowCount: want 0, got %d", got)
	}
}

func TestMeasureBatch_SingleTagColumn(t *testing.T) {
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleTimestamp, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "svc", Type: vectorized.ColumnTypeString},
	})
	tagCol := vectorized.NewStringColumn(4)
	tagCol.Append("alpha")
	tagCol.Append("beta")
	b := &MeasureBatch{
		Schema:     schema,
		Timestamps: []int64{1, 2},
		Versions:   []int64{1, 1},
		ShardIDs:   []common.ShardID{0, 0},
		SeriesIDs:  []common.SeriesID{7, 7},
		Tags:       []vectorized.Column{tagCol},
	}
	if got := b.RowCount(); got != 2 {
		t.Fatalf("RowCount: want 2, got %d", got)
	}
	if got := len(b.Tags); got != 1 {
		t.Fatalf("len(Tags): want 1, got %d", got)
	}
	if got := b.Tags[0].Len(); got != 2 {
		t.Fatalf("Tags[0].Len: want 2, got %d", got)
	}
}

func TestMeasureBatch_MixedTagAndField(t *testing.T) {
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleTimestamp, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "svc", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleField, Name: "value", Type: vectorized.ColumnTypeInt64},
	})
	tagCol := vectorized.NewStringColumn(4)
	tagCol.Append("alpha")
	tagCol.Append("beta")
	tagCol.Append("gamma")
	fieldCol := vectorized.NewInt64Column(4)
	fieldCol.Append(10)
	fieldCol.Append(20)
	fieldCol.Append(30)
	b := &MeasureBatch{
		Schema:     schema,
		Timestamps: []int64{1, 2, 3},
		Versions:   []int64{1, 1, 1},
		ShardIDs:   []common.ShardID{0, 0, 0},
		SeriesIDs:  []common.SeriesID{1, 1, 1},
		Tags:       []vectorized.Column{tagCol},
		Fields:     []vectorized.Column{fieldCol},
	}
	if got := b.RowCount(); got != 3 {
		t.Fatalf("RowCount: want 3, got %d", got)
	}
	if got := len(b.Fields); got != 1 {
		t.Fatalf("len(Fields): want 1, got %d", got)
	}
	if got := b.Fields[0].Len(); got != 3 {
		t.Fatalf("Fields[0].Len: want 3, got %d", got)
	}
	// Schema lookup helpers must align with the columns.
	if idx, ok := schema.TagIndex("default", "svc"); !ok || idx != 1 {
		t.Fatalf("TagIndex: want (1, true), got (%d, %v)", idx, ok)
	}
	if idx, ok := schema.FieldIndex("value"); !ok || idx != 2 {
		t.Fatalf("FieldIndex: want (2, true), got (%d, %v)", idx, ok)
	}
}

func TestMeasureBatch_SeriesBoundariesMultiSeries(t *testing.T) {
	// A batch holding 2 rows of series A followed by 3 rows of series B
	// reports SeriesBoundaries = [2, 5].
	b := &MeasureBatch{
		Timestamps:       []int64{1, 2, 3, 4, 5},
		Versions:         []int64{1, 1, 1, 1, 1},
		ShardIDs:         []common.ShardID{0, 0, 0, 0, 0},
		SeriesIDs:        []common.SeriesID{1, 1, 2, 2, 2},
		SeriesBoundaries: []int{2, 5},
	}
	if got := b.RowCount(); got != 5 {
		t.Fatalf("RowCount: want 5, got %d", got)
	}
	if got := b.SeriesBoundaries; len(got) != 2 || got[0] != 2 || got[1] != 5 {
		t.Fatalf("SeriesBoundaries: want [2 5], got %v", got)
	}
}

// fakeMeasureBatchResult is the minimal MeasureBatchResult used to verify
// the interface contract (sticky errors, EOF semantics, idempotent Release).
type fakeMeasureBatchResult struct {
	err        error
	seq        []*MeasureBatch
	idx        int
	releaseCnt int
}

func (f *fakeMeasureBatchResult) PullBatch(_ context.Context) (*MeasureBatch, error) {
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

func (f *fakeMeasureBatchResult) Release() {
	f.releaseCnt++
}

func TestMeasureBatchResult_PullBatchEOF(t *testing.T) {
	r := &fakeMeasureBatchResult{seq: nil}
	b, err := r.PullBatch(context.Background())
	if err != nil {
		t.Fatalf("EOF must not return an error; got %v", err)
	}
	if b != nil {
		t.Fatalf("EOF must return nil batch; got %v", b)
	}
}

func TestMeasureBatchResult_PullBatchStickyError(t *testing.T) {
	boom := errors.New("scan failed")
	r := &fakeMeasureBatchResult{err: boom}
	for i := range 3 {
		b, err := r.PullBatch(context.Background())
		if !errors.Is(err, boom) {
			t.Fatalf("call %d: want sticky error %v, got %v", i, boom, err)
		}
		if b != nil {
			t.Fatalf("call %d: error must yield nil batch, got %v", i, b)
		}
	}
}

func TestMeasureBatchResult_ReleaseIdempotent(t *testing.T) {
	r := &fakeMeasureBatchResult{}
	r.Release()
	r.Release()
	if r.releaseCnt != 2 {
		t.Fatalf("Release calls counted: want 2, got %d", r.releaseCnt)
	}
}
