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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/data"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
)

func batchBoundaryPlan(t *testing.T) *DistributedPlan {
	t.Helper()
	req := &measurev1.QueryRequest{
		Name: "demo", Groups: []string{defaultName}, TagProjection: projTagProj(),
		FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{fieldValue}}, Offset: 1, Limit: 2,
	}
	plan, err := AnalyzeDistributed(req, []*databasev1.Measure{testMeasureSchema()}, nil, vmeasure.VectorizedConfig{BatchSize: 16, QueryMemoryMiB: 256})
	require.NoError(t, err)
	return plan
}

func TestBatchBoundaryOwnershipPaginationAndCancellation(t *testing.T) {
	previous := data.MeasureWireModeRaw()
	data.SetMeasureWireModeRaw(true)
	t.Cleanup(func() { data.SetMeasureWireModeRaw(previous) })
	plan := batchBoundaryPlan(t)
	before := vectorized.OutstandingOwnerPins()
	initialMemory := vectorized.QueryMemoryStats().LiveReservedBytes
	serialization := vmeasure.BatchExecutionStats()
	ctx := executor.WithDistributedExecutionContext(t.Context(), &fakeCluster{nodes: 2, build: rowsBatch})
	result, err := plan.ExecuteBatches(ctx)
	require.NoError(t, err)
	require.NotNil(t, result.OutputSchema())
	var rows int
	for {
		batch, pullErr := result.NextBatch(ctx)
		require.NoError(t, pullErr)
		if batch == nil {
			break
		}
		rows += batch.ActiveLen()
	}
	require.Greater(t, rows, 2, "batch seam must precede native pagination")
	require.Equal(t, serialization.IntermediateDataPoints, vmeasure.BatchExecutionStats().IntermediateDataPoints)
	require.NoError(t, result.Close())
	require.NoError(t, result.Close())
	require.Equal(t, before, vectorized.OutstandingOwnerPins())
	require.Equal(t, initialMemory, vectorized.QueryMemoryStats().LiveReservedBytes)
	result, err = plan.ExecuteBatches(ctx)
	require.NoError(t, err)
	iter, err := result.NativeIterator(ctx)
	require.NoError(t, err)
	var nativeRows int
	for iter.Next() {
		nativeRows++
	}
	require.Equal(t, 2, nativeRows)
	require.NoError(t, iter.Close())
	result, err = plan.ExecuteBatches(ctx)
	require.NoError(t, err)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = result.NextBatch(canceled)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, before, vectorized.OutstandingOwnerPins())
	require.Equal(t, initialMemory, vectorized.QueryMemoryStats().LiveReservedBytes)
}

func TestBatchBoundaryBudgetRejectsBeforeDecode(t *testing.T) {
	previous := data.MeasureWireModeRaw()
	data.SetMeasureWireModeRaw(true)
	t.Cleanup(func() { data.SetMeasureWireModeRaw(previous) })
	plan := batchBoundaryPlan(t)
	before := vectorized.OutstandingOwnerPins()
	initialMemory := vectorized.QueryMemoryStats().LiveReservedBytes
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 4<<20)
	cluster := &fakeCluster{nodes: 1, build: func(node int) *vectorized.RecordBatch {
		batch := rowsBatch(node)
		batch.Columns[5] = vectorized.NewConstString(string(make([]byte, 2<<20)), batch.Len)
		return batch
	}}
	result, err := plan.ExecuteBatches(executor.WithDistributedExecutionContext(ctx, cluster))
	require.ErrorContains(t, err, "memory budget exceeded")
	require.Nil(t, result)
	require.Equal(t, before, vectorized.OutstandingOwnerPins())
	require.Equal(t, initialMemory, vectorized.QueryMemoryStats().LiveReservedBytes)
	ctx = vectorized.WithQueryMemoryTracker(t.Context(), 256<<20)
	result, err = plan.ExecuteBatches(executor.WithDistributedExecutionContext(ctx, cluster))
	require.NoError(t, err)
	require.NoError(t, result.Close())
	require.Equal(t, before, vectorized.OutstandingOwnerPins())
	require.Equal(t, initialMemory, vectorized.QueryMemoryStats().LiveReservedBytes)
}

func TestBatchBoundaryThousandQueriesReleaseAllOwnersAndReservations(t *testing.T) {
	previous := data.MeasureWireModeRaw()
	data.SetMeasureWireModeRaw(true)
	t.Cleanup(func() { data.SetMeasureWireModeRaw(previous) })
	plan := batchBoundaryPlan(t)
	before := vectorized.OutstandingOwnerPins()
	initial := vectorized.QueryMemoryStats().LiveReservedBytes
	handles := vmeasure.BatchExecutionStats().LiveResults
	ctx := executor.WithDistributedExecutionContext(t.Context(), &fakeCluster{nodes: 2, build: rowsBatch})
	for range 1000 {
		result, err := plan.ExecuteBatches(ctx)
		require.NoError(t, err)
		_, err = result.NextBatch(ctx)
		require.NoError(t, err)
		require.NoError(t, result.Close())
	}
	require.Equal(t, before, vectorized.OutstandingOwnerPins())
	require.Equal(t, initial, vectorized.QueryMemoryStats().LiveReservedBytes)
	require.Equal(t, handles, vmeasure.BatchExecutionStats().LiveResults)
}
