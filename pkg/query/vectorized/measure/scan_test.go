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

func minimalSchema() *vectorized.BatchSchema {
	return vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleTimestamp, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleVersion, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleSeriesID, Type: vectorized.ColumnTypeInt64},
	})
}

func newScan(qr model.MeasureQueryResult, schema *vectorized.BatchSchema, batchSize int) *BatchScan {
	pool := vectorized.NewBatchPool(schema, batchSize)
	return NewBatchScan(qr, schema, pool, batchSize)
}

func TestBatchScan_SingleSeries_FillsBatchViaBulkPath(t *testing.T) {
	qr := &fakeMeasureQueryResult{seq: []*model.MeasureResult{mkResult(1, 100, 200, 300)}}
	schema := minimalSchema()
	scan := newScan(qr, schema, 4)
	if err := scan.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer scan.Close()
	b, err := scan.NextBatch(context.Background())
	if err != nil || b == nil {
		t.Fatalf("err=%v b=%v", err, b)
	}
	if b.Len != 3 {
		t.Fatalf("Len: want 3, got %d", b.Len)
	}
	ts := b.Columns[0].(*vectorized.TypedColumn[int64]).Data()
	for i, want := range []int64{100, 200, 300} {
		if ts[i] != want {
			t.Fatalf("ts[%d]: got %d, want %d", i, ts[i], want)
		}
	}
}

func TestBatchScan_MultiSeries_CrossingBoundaryUsesPerRowPath(t *testing.T) {
	qr := &fakeMeasureQueryResult{seq: []*model.MeasureResult{
		mkResult(1, 1, 2),
		mkResult(2, 3, 4, 5),
	}}
	schema := minimalSchema()
	scan := newScan(qr, schema, 4)
	_ = scan.Init(context.Background())
	defer scan.Close()
	b, err := scan.NextBatch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if b.Len != 4 {
		t.Fatalf("first batch Len: want 4, got %d", b.Len)
	}
	sids := b.Columns[2].(*vectorized.TypedColumn[int64]).Data()
	want := []int64{1, 1, 2, 2}
	for i := range want {
		if sids[i] != want[i] {
			t.Fatalf("sid[%d]: got %d, want %d", i, sids[i], want[i])
		}
	}
	b2, _ := scan.NextBatch(context.Background())
	if b2.Len != 1 || b2.Columns[2].(*vectorized.TypedColumn[int64]).Data()[0] != 2 {
		t.Fatalf("second batch: Len=%d sid[0]=%d", b2.Len, b2.Columns[2].(*vectorized.TypedColumn[int64]).Data()[0])
	}
}

func TestBatchScan_BatchSizeExceedsTotalRows_OneBatchThenEOF(t *testing.T) {
	qr := &fakeMeasureQueryResult{seq: []*model.MeasureResult{mkResult(1, 10, 20)}}
	schema := minimalSchema()
	scan := newScan(qr, schema, 16)
	_ = scan.Init(context.Background())
	defer scan.Close()
	b, _ := scan.NextBatch(context.Background())
	if b.Len != 2 {
		t.Fatalf("Len: want 2, got %d", b.Len)
	}
	eof, err := scan.NextBatch(context.Background())
	if err != nil || eof != nil {
		t.Fatalf("expected EOF, err=%v eof=%v", err, eof)
	}
}

func TestBatchScan_BatchSizeBelowSeriesSize_MultipleFullBatches(t *testing.T) {
	qr := &fakeMeasureQueryResult{seq: []*model.MeasureResult{mkResult(1, 1, 2, 3, 4, 5)}}
	schema := minimalSchema()
	scan := newScan(qr, schema, 2)
	_ = scan.Init(context.Background())
	defer scan.Close()
	b1, _ := scan.NextBatch(context.Background())
	if b1.Len != 2 {
		t.Fatalf("first batch Len: want 2, got %d", b1.Len)
	}
	b2, _ := scan.NextBatch(context.Background())
	if b2.Len != 2 {
		t.Fatalf("second batch Len: want 2, got %d", b2.Len)
	}
	b3, _ := scan.NextBatch(context.Background())
	if b3.Len != 1 {
		t.Fatalf("third (tail) batch Len: want 1, got %d", b3.Len)
	}
	eof, _ := scan.NextBatch(context.Background())
	if eof != nil {
		t.Fatal("expected EOF after tail")
	}
}

func TestBatchScan_EmptyResult_EOFImmediately(t *testing.T) {
	qr := &fakeMeasureQueryResult{seq: nil}
	schema := minimalSchema()
	scan := newScan(qr, schema, 4)
	_ = scan.Init(context.Background())
	defer scan.Close()
	b, err := scan.NextBatch(context.Background())
	if err != nil || b != nil {
		t.Fatalf("expected (nil, nil), got (%v, %v)", b, err)
	}
}

// Pins the regression flagged by Copilot: when a valid series precedes an
// errored MeasureResult, the partial batch must be discarded on the same
// NextBatch call — not returned as if successful with the error buried.
func TestBatchScan_CursorErrorAfterValidSeries_DiscardsPartialBatch(t *testing.T) {
	boom := errors.New("storage boom mid-stream")
	qr := &fakeMeasureQueryResult{seq: []*model.MeasureResult{
		mkResult(1, 100, 200),
		mkResultErr(boom),
	}}
	schema := minimalSchema()
	scan := newScan(qr, schema, 4)
	_ = scan.Init(context.Background())
	defer scan.Close()
	b, err := scan.NextBatch(context.Background())
	if !errors.Is(err, boom) {
		t.Fatalf("want storage error, got err=%v", err)
	}
	if b != nil {
		t.Fatalf("partial batch must be discarded on cursor error; got b.Len=%d", b.Len)
	}
}

func TestBatchScan_CursorError_PropagatesAsError_NoBatchEmitted(t *testing.T) {
	boom := errors.New("storage boom")
	qr := &fakeMeasureQueryResult{seq: []*model.MeasureResult{mkResultErr(boom)}}
	schema := minimalSchema()
	scan := newScan(qr, schema, 4)
	_ = scan.Init(context.Background())
	defer scan.Close()
	b, err := scan.NextBatch(context.Background())
	if !errors.Is(err, boom) {
		t.Fatalf("want storage error, got %v", err)
	}
	if b != nil {
		t.Fatal("error path must return nil batch")
	}
}

func TestBatchScan_TagAndFieldColumns_PopulatedFromAlreadyDecodedValues(t *testing.T) {
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleTimestamp, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleSeriesID, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "service", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleField, Name: "value", Type: vectorized.ColumnTypeInt64},
	})
	mr := &model.MeasureResult{
		SID:        common.SeriesID(1),
		Timestamps: []int64{100, 200},
		Versions:   []int64{0, 0},
		ShardIDs:   []common.ShardID{0, 0},
		TagFamilies: []model.TagFamily{
			{Name: "default", Tags: []model.Tag{
				{Name: "service", Values: []*modelv1.TagValue{tvStr("a"), tvStr("b")}},
			}},
		},
		Fields: []model.Field{
			{Name: "value", Values: []*modelv1.FieldValue{fvInt(10), fvInt(20)}},
		},
	}
	qr := &fakeMeasureQueryResult{seq: []*model.MeasureResult{mr}}
	scan := newScan(qr, schema, 4)
	_ = scan.Init(context.Background())
	defer scan.Close()
	b, err := scan.NextBatch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	tagIdx, _ := schema.TagIndex("default", "service")
	tags := b.Columns[tagIdx].(*vectorized.TypedColumn[string]).Data()
	if tags[0] != "a" || tags[1] != "b" {
		t.Fatalf("tags: got %v", tags)
	}
	fieldIdx, _ := schema.FieldIndex("value")
	fields := b.Columns[fieldIdx].(*vectorized.TypedColumn[int64]).Data()
	if fields[0] != 10 || fields[1] != 20 {
		t.Fatalf("fields: got %v", fields)
	}
}

func TestBatchScan_SeriesIDColumn_AlwaysPopulated(t *testing.T) {
	qr := &fakeMeasureQueryResult{seq: []*model.MeasureResult{
		mkResult(common.SeriesID(42), 100, 200),
	}}
	schema := minimalSchema()
	scan := newScan(qr, schema, 4)
	_ = scan.Init(context.Background())
	defer scan.Close()
	b, _ := scan.NextBatch(context.Background())
	sids := b.Columns[2].(*vectorized.TypedColumn[int64]).Data()
	for i, sid := range sids {
		if sid != 42 {
			t.Fatalf("sid[%d]: got %d, want 42", i, sid)
		}
	}
}

func TestBatchScan_Close_ReleasesCursor(t *testing.T) {
	qr := &fakeMeasureQueryResult{seq: []*model.MeasureResult{mkResult(1, 100)}}
	schema := minimalSchema()
	scan := newScan(qr, schema, 4)
	_ = scan.Init(context.Background())
	if err := scan.Close(); err != nil {
		t.Fatal(err)
	}
	if qr.releaseCnt != 1 {
		t.Fatalf("Close must release the underlying MeasureQueryResult: got releaseCnt=%d", qr.releaseCnt)
	}
}
