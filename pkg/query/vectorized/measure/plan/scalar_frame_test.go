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

package plan

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/data"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	baseframe "github.com/apache/skywalking-banyandb/pkg/query/vectorized/frame"
)

func wideBoundaryBatch() *vectorized.RecordBatch {
	schema := rowsBatch(0).Schema
	batch := vectorized.NewRecordBatch(schema, 8192)
	for row := range 8192 {
		for col, column := range batch.Columns {
			if typed, ok := column.(*vectorized.TypedColumn[int64]); ok {
				value := int64(row)
				if col == 1 {
					value = 1
				}
				typed.Append(value)
			} else {
				column.(*vectorized.TypedColumn[string]).Append(strings.Repeat("w", 256))
			}
		}
	}
	batch.Len = 8192
	return batch
}

func TestBatchBoundaryScalarFramesBorrowAndFitBudget(t *testing.T) {
	previous := data.MeasureWireModeRaw()
	data.SetMeasureWireModeRaw(true)
	t.Cleanup(func() { data.SetMeasureWireModeRaw(previous) })
	baseline := vectorized.OutstandingOwnerPins()
	trackerCtx := vectorized.WithQueryMemoryTracker(t.Context(), 32<<20)
	tracker := vectorized.QueryMemoryTracker(trackerCtx, 32<<20)
	ctx := executor.WithDistributedExecutionContext(trackerCtx, &fakeCluster{nodes: 2, build: func(int) *vectorized.RecordBatch { return wideBoundaryBatch() }})
	result, err := batchBoundaryPlan(t).ExecuteBatches(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, result.Close()) }()
	rows := 0
	for {
		batch, pullErr := result.NextBatch(ctx)
		require.NoError(t, pullErr)
		if batch == nil {
			break
		}
		require.True(t, batch.Columns[0].(*vectorized.TypedColumn[int64]).Borrowed(), "contiguous metadata must borrow the decoded frame")
		require.True(t, batch.Columns[5].(*vectorized.TypedColumn[string]).Borrowed(), "contiguous string headers must share decoded storage")
		rows += batch.ActiveLen()
	}
	require.Equal(t, 8192, rows)
	require.NoError(t, result.Close())
	require.Zero(t, tracker.Used())
	require.Equal(t, baseline, vectorized.OutstandingOwnerPins())
}

func TestBatchBoundaryScalarInterleavingUsesCopyFallback(t *testing.T) {
	previous := data.MeasureWireModeRaw()
	data.SetMeasureWireModeRaw(true)
	t.Cleanup(func() { data.SetMeasureWireModeRaw(previous) })
	before := vectorized.OutstandingOwnerPins()
	ctx := executor.WithDistributedExecutionContext(t.Context(), &fakeCluster{nodes: 2, build: rowsBatch})
	result, err := batchBoundaryPlan(t).ExecuteBatches(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, result.Close()) }()
	rows := 0
	previousTimestamp := int64(-1)
	for {
		batch, pullErr := result.NextBatch(ctx)
		require.NoError(t, pullErr)
		if batch == nil {
			break
		}
		timestamps := batch.Columns[0].(*vectorized.TypedColumn[int64])
		require.False(t, timestamps.Borrowed(), "interleaving must use the bounded copy fallback")
		for _, timestamp := range timestamps.Data() {
			require.Greater(t, timestamp, previousTimestamp)
			previousTimestamp = timestamp
		}
		rows += batch.ActiveLen()
	}
	require.Equal(t, 120, rows)
	require.NoError(t, result.Close())
	require.Equal(t, before, vectorized.OutstandingOwnerPins())
}

func scalarMixedBoundaryBatch(node int) *vectorized.RecordBatch {
	batch := wideBoundaryBatch()
	if node == 0 {
		return batch
	}
	definitions := append([]vectorized.ColumnDef(nil), batch.Schema.Columns...)
	definitions[5].Type = vectorized.ColumnTypeTagValue
	batch.Schema = vectorized.NewBatchSchema(definitions)
	source := batch.Columns[5].(*vectorized.TypedColumn[string])
	column := vectorized.NewTagValueColumn(batch.Len)
	for _, value := range source.Data() {
		column.Append(&modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: value}}})
	}
	batch.Columns[5] = column
	return batch
}

func TestBatchBoundaryMixedFramesRetainConservativeFallbackAndRecover(t *testing.T) {
	previous := data.MeasureWireModeRaw()
	data.SetMeasureWireModeRaw(true)
	t.Cleanup(func() { data.SetMeasureWireModeRaw(previous) })
	before := vectorized.OutstandingOwnerPins()
	trackerCtx := vectorized.WithQueryMemoryTracker(t.Context(), 32<<20)
	tracker := vectorized.QueryMemoryTracker(trackerCtx, 32<<20)
	ctx := executor.WithDistributedExecutionContext(trackerCtx, &fakeCluster{nodes: 2, build: scalarMixedBoundaryBatch})
	result, err := batchBoundaryPlan(t).ExecuteBatches(ctx)
	require.ErrorIs(t, err, vectorized.ErrMemoryBudgetExceeded)
	require.Nil(t, result)
	require.Zero(t, tracker.Used())
	require.Equal(t, before, vectorized.OutstandingOwnerPins())
	ctx = executor.WithDistributedExecutionContext(trackerCtx, &fakeCluster{nodes: 2, build: func(int) *vectorized.RecordBatch { return wideBoundaryBatch() }})
	result, err = batchBoundaryPlan(t).ExecuteBatches(ctx)
	require.NoError(t, err)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = result.NextBatch(canceled)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, tracker.Used())
	require.Equal(t, before, vectorized.OutstandingOwnerPins())
}

func TestBatchBoundaryBorrowedScalarResponseOutlivesClose(t *testing.T) {
	previous := data.MeasureWireModeRaw()
	data.SetMeasureWireModeRaw(true)
	t.Cleanup(func() { data.SetMeasureWireModeRaw(previous) })
	poison := baseframe.SetPoisonReleasedMessages(true)
	defer baseframe.SetPoisonReleasedMessages(poison)
	before := vectorized.OutstandingOwnerPins()
	ctx := executor.WithDistributedExecutionContext(t.Context(), &fakeCluster{nodes: 2, build: func(int) *vectorized.RecordBatch { return rowsBatch(0) }})
	result, err := batchBoundaryPlan(t).ExecuteBatches(ctx)
	require.NoError(t, err)
	iterator, err := result.NativeIterator(ctx)
	require.NoError(t, err)
	var stringsAfterClose []string
	for iterator.Next() {
		point := iterator.Current()[0].GetDataPoint()
		for _, family := range point.GetTagFamilies() {
			for _, tag := range family.GetTags() {
				if tag.Key == "svc" {
					stringsAfterClose = append(stringsAfterClose, tag.GetValue().GetStr().GetValue())
				}
			}
		}
	}
	require.NoError(t, iterator.Close())
	require.Equal(t, []string{"service-1-of-node-0", "service-2-of-node-0"}, stringsAfterClose)
	require.Equal(t, before, vectorized.OutstandingOwnerPins())
}
