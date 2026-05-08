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
	"testing"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// schemaSingleTagSingleField is the canonical mini-schema used across the
// build_batch tests: timestamp + version + sid metadata + one tag (svc) +
// one field (value). Passthrough column types match what the storage-side
// converter emits in the G5b wrapper-style PullBatch.
func schemaSingleTagSingleField() *vectorized.BatchSchema {
	return vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleTimestamp, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleVersion, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleSeriesID, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "svc", Type: vectorized.ColumnTypeTagValue},
		{Role: vectorized.RoleField, Name: "value", Type: vectorized.ColumnTypeFieldValue},
	})
}

func TestBuildMeasureBatchFromResult_NilResult(t *testing.T) {
	batch, err := BuildMeasureBatchFromResult(nil, schemaSingleTagSingleField())
	if err != nil {
		t.Fatalf("nil result must return (nil, nil); got err %v", err)
	}
	if batch != nil {
		t.Fatalf("nil result must return (nil, nil); got batch %v", batch)
	}
}

func TestBuildMeasureBatchFromResult_NilSchema(t *testing.T) {
	r := &model.MeasureResult{SID: 1, Timestamps: []int64{1}}
	if _, err := BuildMeasureBatchFromResult(r, nil); err == nil {
		t.Fatal("nil schema must return an error")
	}
}

func TestBuildMeasureBatchFromResult_FullRow(t *testing.T) {
	svcVal := &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "alpha"}}}
	valueFld := &modelv1.FieldValue{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 42}}}
	r := &model.MeasureResult{
		SID:        7,
		Timestamps: []int64{100, 200, 300},
		Versions:   []int64{1, 1, 1},
		ShardIDs:   []common.ShardID{2, 2, 2},
		TagFamilies: []model.TagFamily{
			{Name: "default", Tags: []model.Tag{
				{Name: "svc", Values: []*modelv1.TagValue{svcVal, svcVal, svcVal}},
			}},
		},
		Fields: []model.Field{
			{Name: "value", Values: []*modelv1.FieldValue{valueFld, valueFld, valueFld}},
		},
	}
	batch, err := BuildMeasureBatchFromResult(r, schemaSingleTagSingleField())
	if err != nil {
		t.Fatalf("BuildMeasureBatchFromResult: %v", err)
	}
	if got := batch.RowCount(); got != 3 {
		t.Fatalf("RowCount: want 3, got %d", got)
	}
	for i, ts := range []int64{100, 200, 300} {
		if batch.Timestamps[i] != ts {
			t.Fatalf("Timestamps[%d]: want %d, got %d", i, ts, batch.Timestamps[i])
		}
	}
	for i, sid := range batch.SeriesIDs {
		if sid != 7 {
			t.Fatalf("SeriesIDs[%d]: want 7, got %d", i, sid)
		}
	}
	for i, sh := range batch.ShardIDs {
		if sh != 2 {
			t.Fatalf("ShardIDs[%d]: want 2, got %d", i, sh)
		}
	}
	if len(batch.Tags) != 1 {
		t.Fatalf("len(Tags): want 1, got %d", len(batch.Tags))
	}
	tagCol, ok := batch.Tags[0].(*vectorized.TypedColumn[*modelv1.TagValue])
	if !ok {
		t.Fatalf("Tags[0] is not TypedColumn[*modelv1.TagValue]; got %T", batch.Tags[0])
	}
	if tagCol.Len() != 3 {
		t.Fatalf("Tags[0].Len: want 3, got %d", tagCol.Len())
	}
	for i, v := range tagCol.Data() {
		if v != svcVal {
			t.Fatalf("Tags[0][%d]: want passthrough pointer to svcVal, got %v", i, v)
		}
	}
	if len(batch.Fields) != 1 {
		t.Fatalf("len(Fields): want 1, got %d", len(batch.Fields))
	}
	fldCol, ok := batch.Fields[0].(*vectorized.TypedColumn[*modelv1.FieldValue])
	if !ok {
		t.Fatalf("Fields[0] is not TypedColumn[*modelv1.FieldValue]; got %T", batch.Fields[0])
	}
	for i, v := range fldCol.Data() {
		if v != valueFld {
			t.Fatalf("Fields[0][%d]: want passthrough pointer to valueFld, got %v", i, v)
		}
	}
}

func TestBuildMeasureBatchFromResult_MissingTagNullFilled(t *testing.T) {
	// Schema declares tag "svc" + field "value", but the result omits "svc".
	// Output column for svc must be null-filled with pbv1.NullTagValue per
	// the multi-group projection contract.
	valueFld := &modelv1.FieldValue{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 42}}}
	r := &model.MeasureResult{
		SID:        1,
		Timestamps: []int64{10, 20},
		Versions:   []int64{1, 1},
		ShardIDs:   []common.ShardID{0, 0},
		Fields: []model.Field{
			{Name: "value", Values: []*modelv1.FieldValue{valueFld, valueFld}},
		},
	}
	batch, err := BuildMeasureBatchFromResult(r, schemaSingleTagSingleField())
	if err != nil {
		t.Fatalf("BuildMeasureBatchFromResult: %v", err)
	}
	if batch.RowCount() != 2 {
		t.Fatalf("RowCount: want 2, got %d", batch.RowCount())
	}
	tagCol, ok := batch.Tags[0].(*vectorized.TypedColumn[*modelv1.TagValue])
	if !ok {
		t.Fatalf("Tags[0] type")
	}
	for i, v := range tagCol.Data() {
		if v != pbv1.NullTagValue {
			t.Fatalf("Tags[0][%d]: want pbv1.NullTagValue (singleton), got %v", i, v)
		}
	}
}

func TestBuildMeasureBatchFromResult_MissingFieldNullFilled(t *testing.T) {
	svcVal := &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "alpha"}}}
	r := &model.MeasureResult{
		SID:        1,
		Timestamps: []int64{10, 20},
		Versions:   []int64{1, 1},
		ShardIDs:   []common.ShardID{0, 0},
		TagFamilies: []model.TagFamily{
			{Name: "default", Tags: []model.Tag{
				{Name: "svc", Values: []*modelv1.TagValue{svcVal, svcVal}},
			}},
		},
	}
	batch, err := BuildMeasureBatchFromResult(r, schemaSingleTagSingleField())
	if err != nil {
		t.Fatalf("BuildMeasureBatchFromResult: %v", err)
	}
	fldCol, ok := batch.Fields[0].(*vectorized.TypedColumn[*modelv1.FieldValue])
	if !ok {
		t.Fatalf("Fields[0] type")
	}
	for i, v := range fldCol.Data() {
		if v != pbv1.NullFieldValue {
			t.Fatalf("Fields[0][%d]: want pbv1.NullFieldValue (singleton), got %v", i, v)
		}
	}
}

func TestBuildMeasureBatchFromResult_TagValueLengthMismatchErrors(t *testing.T) {
	// The schema demands 2 timestamps' worth of cells but the tag's Values
	// slice is shorter — the converter must surface the length invariant
	// rather than silently truncating.
	r := &model.MeasureResult{
		SID:        1,
		Timestamps: []int64{1, 2},
		Versions:   []int64{1, 1},
		ShardIDs:   []common.ShardID{0, 0},
		TagFamilies: []model.TagFamily{
			{Name: "default", Tags: []model.Tag{
				{Name: "svc", Values: []*modelv1.TagValue{
					{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "x"}}},
				}},
			}},
		},
	}
	if _, err := BuildMeasureBatchFromResult(r, schemaSingleTagSingleField()); err == nil {
		t.Fatal("length-invariant violation must return an error")
	}
}

func TestBuildMeasureBatchFromResult_FieldValueLengthMismatchErrors(t *testing.T) {
	r := &model.MeasureResult{
		SID:        1,
		Timestamps: []int64{1, 2},
		Versions:   []int64{1, 1},
		ShardIDs:   []common.ShardID{0, 0},
		Fields: []model.Field{
			{Name: "value", Values: []*modelv1.FieldValue{
				{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 1}}},
			}},
		},
	}
	if _, err := BuildMeasureBatchFromResult(r, schemaSingleTagSingleField()); err == nil {
		t.Fatal("length-invariant violation must return an error")
	}
}
