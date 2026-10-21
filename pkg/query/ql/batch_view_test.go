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

package ql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func TestRetainBatchViewRepeatedColumnSurvivesSourceReuseAndClose(t *testing.T) {
	ctx := vectorized.WithQueryMemoryTracker(context.Background(), 1<<20)
	tracker := vectorized.QueryMemoryTracker(ctx, 0)
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{{Name: "payload", Role: vectorized.RoleTag, Type: vectorized.ColumnTypeBytes}})
	batch := vectorized.NewRecordBatch(schema, 2)
	column := batch.Columns[0].(*vectorized.TypedColumn[[]byte])
	payload := []byte("first")
	column.Append(payload)
	column.AppendNull()
	batch.Len = 2
	batch.Selection = []uint16{1, 0}
	source := &scanSource{schema: schema, batch: batch}
	scan := NewNativeScan(source)
	lent, err := scan.NextBatch(ctx)
	require.NoError(t, err)
	view, err := RetainBatchView(ctx, lent, []int{0, 0})
	require.NoError(t, err)
	require.Same(t, view.Columns[0], view.Columns[1])
	require.Positive(t, tracker.Used())
	// Source buffers and descriptor are deliberately reused, including nulls and selection.
	copy(payload, "xxxxx")
	batch.Reset()
	column.Append([]byte("second"))
	batch.Len = 1
	_, err = scan.NextBatch(ctx)
	require.NoError(t, err)
	require.NoError(t, scan.Close())
	retained := view.Columns[0].(*vectorized.TypedColumn[[]byte])
	require.Equal(t, []byte("first"), retained.Data()[0])
	require.True(t, retained.IsNull(1))
	require.Equal(t, []uint16{1, 0}, view.Selection)
	require.Equal(t, 2, view.Len)
	view.ReleaseOwners()
	view.ReleaseOwners()
	require.Zero(t, tracker.Used())
}

func TestRetainBatchViewRejectsBeforeCopyWhenBudgetExhausted(t *testing.T) {
	ctx := vectorized.WithQueryMemoryTracker(context.Background(), 1)
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{{Role: vectorized.RoleField, Type: vectorized.ColumnTypeInt64}})
	batch := vectorized.NewRecordBatch(schema, 1)
	batch.Columns[0].(*vectorized.TypedColumn[int64]).Append(42)
	batch.Len = 1
	view, err := RetainBatchView(ctx, batch, []int{0})
	require.Error(t, err)
	require.Nil(t, view)
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
}

func TestRetainBatchViewPreservesProtobufUnknownsAndNullConstants(t *testing.T) {
	ctx := vectorized.WithQueryMemoryTracker(context.Background(), 1<<20)
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleTag, Name: "value", Type: vectorized.ColumnTypeTagValue},
		{Role: vectorized.RoleField, Name: "null", Type: vectorized.ColumnTypeInt64},
	})
	value := &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "original"}}}
	value.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01})
	batch := vectorized.NewRecordBatch(schema, 1)
	batch.Columns[0].(*vectorized.TypedColumn[*modelv1.TagValue]).Append(value)
	batch.Columns[1] = vectorized.NewConstNull(vectorized.ColumnTypeInt64, 1)
	batch.Len = 1
	view, err := RetainBatchView(ctx, batch, []int{0, 1})
	require.NoError(t, err)
	value.GetStr().Value = "poison"
	value.ProtoReflect().SetUnknown(nil)
	copied := view.Columns[0].(*vectorized.TypedColumn[*modelv1.TagValue]).Data()[0]
	require.Equal(t, "original", copied.GetStr().Value)
	require.Equal(t, []byte{0xa0, 0x06, 0x01}, []byte(copied.ProtoReflect().GetUnknown()))
	require.True(t, view.Columns[1].IsNull(0))
	view.ReleaseOwners()
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
}

type unsupportedViewColumn struct{ vectorized.Column }

func TestRetainBatchViewRejectsUnsupportedColumnWithoutReservation(t *testing.T) {
	ctx := vectorized.WithQueryMemoryTracker(context.Background(), 1<<20)
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{{Type: vectorized.ColumnTypeInt64}})
	batch := &vectorized.RecordBatch{Schema: schema, Columns: []vectorized.Column{unsupportedViewColumn{}}, Len: 1}
	view, err := RetainBatchView(ctx, batch, []int{0})
	require.Error(t, err)
	require.Nil(t, view)
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
}
