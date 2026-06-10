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

package trace

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/internal/sidx"
	vtrace "github.com/apache/skywalking-banyandb/pkg/query/vectorized/trace"
)

func TestSidxResponseToVectorizedBatch(t *testing.T) {
	resp := &sidx.QueryResponse{
		Keys:    []int64{1},
		Data:    [][]byte{{0x01, 'a'}},
		SIDs:    []common.SeriesID{2},
		PartIDs: []uint64{3},
	}
	batch, err := sidxResponseToVectorizedBatch(resp)
	require.NoError(t, err)
	require.Equal(t, []int64{1}, batch.Keys)
	require.Equal(t, [][]byte{{0x01, 'a'}}, batch.Data)
	require.Equal(t, []int64{2}, batch.SIDs)
	require.Equal(t, []int64{3}, batch.PartIDs)

	resp.Data[0][1] = 'b'
	require.Equal(t, []byte{0x01, 'a'}, batch.Data[0])
}

func TestSidxResponseToVectorizedBatchRejectsInvalidShape(t *testing.T) {
	_, err := sidxResponseToVectorizedBatch(&sidx.QueryResponse{
		Keys: []int64{1},
		Data: [][]byte{{0x01, 'a'}},
	})
	require.Error(t, err)
}

func TestDrainVectorizedPhase1(t *testing.T) {
	plan, err := vtrace.BuildStaticPhase1([]string{"trace-a", "trace-b", "trace-c"}, map[string]int64{"trace-b": 2}, 2, 10)
	require.NoError(t, err)
	batch, err := drainVectorizedPhase1(context.Background(), plan, 7)
	require.NoError(t, err)
	require.Equal(t, 7, batch.seq)
	require.Equal(t, map[uint64][]string{0: {"trace-a", "trace-b"}}, batch.traceIDs)
	require.Equal(t, []string{"trace-a", "trace-b"}, batch.traceIDsOrder)
	require.Equal(t, map[string]int64{"trace-a": 0, "trace-b": 2}, batch.keys)
}

func TestBuildVectorizedPhase1TraceBatchStatic(t *testing.T) {
	tr := &trace{vectorized: vtrace.VectorizedConfig{Enabled: true, BatchSize: 1, QueryMemoryMiB: 1}}
	batch, err := tr.buildVectorizedPhase1TraceBatch(context.Background(), queryOptions{
		traceIDs: []string{"trace-a", "trace-b"},
	}, nil, sidx.QueryRequest{}, false, 1)
	require.NoError(t, err)
	require.Equal(t, []string{"trace-a"}, batch.traceIDsOrder)
	require.Equal(t, map[uint64][]string{0: {"trace-a"}}, batch.traceIDs)
	require.Equal(t, map[string]int64{"trace-a": 0}, batch.keys)
}

func TestNewVectorizedTraceQueryResultEmptyBatch(t *testing.T) {
	finished := false
	result, err := newVectorizedTraceQueryResult(context.Background(), &scanBatch{}, queryOptions{}, nil, nil, func(hit int, resultErr error) {
		finished = true
		require.Equal(t, 0, hit)
		require.NoError(t, resultErr)
	}, nil)
	require.NoError(t, err)
	require.Nil(t, result.Pull())
	result.Release()
	require.True(t, finished)
}

func TestNewVectorizedTraceQueryResultReturnsBatchError(t *testing.T) {
	_, err := newVectorizedTraceQueryResult(context.Background(), &scanBatch{
		traceBatch: traceBatch{err: fmt.Errorf("batch failed")},
		err:        fmt.Errorf("batch failed"),
	}, queryOptions{}, nil, nil, nil, nil)
	require.Error(t, err)
}

func TestQueryResultSyncCursorLoaderHonorsContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// loadTraceCursorsSync owns the cursors: on cancellation it releases
	// every loaded and pending cursor itself, so no defer-release here.
	cursor := generateBlockCursor()
	filtered, err := loadTraceCursorsSync(ctx, []*blockCursor{cursor})
	require.Nil(t, filtered)
	require.Error(t, err)
}

func TestSyncSIDXQuerierToVectorizedIterators(t *testing.T) {
	iters, err := syncSIDXQuerierToVectorizedIterators(context.Background(), []syncSIDXQuerier{
		&fakeSyncSIDX{responses: []*sidx.QueryResponse{
			{
				Keys:    []int64{1},
				Data:    [][]byte{{0x01, 'a'}},
				SIDs:    []common.SeriesID{2},
				PartIDs: []uint64{3},
			},
		}},
	}, sidx.QueryRequest{})
	require.NoError(t, err)
	require.Len(t, iters, 1)
	require.True(t, iters[0].Next())
	item := iters[0].Val()
	require.Equal(t, int64(1), item.Key)
	require.Equal(t, int64(2), item.SeriesID)
	require.Equal(t, int64(3), item.PartID)
	require.Equal(t, []byte{0x01, 'a'}, item.Payload)
	require.NoError(t, iters[0].Close())
}

func TestSyncSIDXQuerierToVectorizedIteratorsReturnsQueryError(t *testing.T) {
	_, err := syncSIDXQuerierToVectorizedIterators(context.Background(), []syncSIDXQuerier{
		&fakeSyncSIDX{err: fmt.Errorf("boom")},
	}, sidx.QueryRequest{})
	require.Error(t, err)
}

type fakeSyncSIDX struct {
	err       error
	responses []*sidx.QueryResponse
}

func (f *fakeSyncSIDX) QuerySync(context.Context, sidx.QueryRequest) ([]*sidx.QueryResponse, error) {
	return f.responses, f.err
}

func TestLoadedCursorSource(t *testing.T) {
	// Build two fake blockCursors with 2 spans each.
	cursorA := generateBlockCursor()
	cursorA.bm.traceID = "trace-a"
	cursorA.spans = [][]byte{[]byte("spanA1"), []byte("spanA2")}
	cursorA.spanIDs = []string{"sA1", "sA2"}

	cursorB := generateBlockCursor()
	cursorB.bm.traceID = "trace-b"
	cursorB.spans = [][]byte{[]byte("spanB1"), []byte("spanB2")}
	cursorB.spanIDs = []string{"sB1", "sB2"}

	keys := map[string]int64{"trace-a": 10, "trace-b": 20}
	schema := vtrace.NewPhase2Schema(nil)
	source := newLoadedCursorSource([]*blockCursor{cursorA, cursorB}, schema, keys, nil, nil)

	ctx := context.Background()
	require.NoError(t, source.Init(ctx))

	batchA, err := source.NextBatch(ctx)
	require.NoError(t, err)
	require.NotNil(t, batchA)
	require.Equal(t, 2, batchA.Len)
	require.Equal(t, []string{"trace-a", "trace-a"}, vtrace.Phase2TraceIDs(batchA).Data())
	require.Equal(t, []int64{10, 10}, vtrace.Phase2Keys(batchA).Data())
	require.Equal(t, [][]byte{[]byte("spanA1"), []byte("spanA2")}, vtrace.Phase2Spans(batchA).Data())
	require.Equal(t, []string{"sA1", "sA2"}, vtrace.Phase2SpanIDs(batchA).Data())

	batchB, err := source.NextBatch(ctx)
	require.NoError(t, err)
	require.NotNil(t, batchB)
	require.Equal(t, 2, batchB.Len)
	require.Equal(t, []string{"trace-b", "trace-b"}, vtrace.Phase2TraceIDs(batchB).Data())
	require.Equal(t, []int64{20, 20}, vtrace.Phase2Keys(batchB).Data())

	done, err := source.NextBatch(ctx)
	require.NoError(t, err)
	require.Nil(t, done)

	require.NoError(t, source.Close())
}
