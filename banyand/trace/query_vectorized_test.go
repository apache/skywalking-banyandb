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
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/sidx"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
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

// TestBuildVectorizedPhase1TraceBatchStaticDedupLimitParity guards against the
// regression where deduplication happened BEFORE limit truncation, causing
// vectorized to return different results than the push path for duplicate IDs.
// Push path: traceIDs[:limit] → emit first occurrence only.
// Vectorized (fixed): same — truncate first, then dedup within the window.
func TestBuildVectorizedPhase1TraceBatchStaticDedupLimitParity(t *testing.T) {
	tr := &trace{vectorized: vtrace.VectorizedConfig{Enabled: true, BatchSize: 10, QueryMemoryMiB: 1}}
	// [a, a, b] limit 2: push takes first 2 raw = [a, a], emits a once.
	// Vectorized must do the same: truncate to [a, a], dedup → [a].
	batch, err := tr.buildVectorizedPhase1TraceBatch(context.Background(), queryOptions{
		traceIDs: []string{"a", "a", "b"},
	}, nil, sidx.QueryRequest{}, false, 2)
	require.NoError(t, err)
	require.Equal(t, []string{"a"}, batch.traceIDsOrder,
		"[a,a,b] limit 2 must yield [a], not [a,b] or [a,a]")
}

// TestLoadTraceCursorsSyncBudgetPreCheck verifies that the hard-stop and metadata
// preflight gates in loadTraceCursorsSync prevent over-loading when a budget is set.
// The test uses budget=1 byte so that after cursor[0] loads any real span data,
// usedBytes >= 1 and every subsequent cursor is stopped by the hard-stop gate.
// Loaded cursors are verified to have non-empty spans, confirming loadData ran for
// them. Cursors absent from the result were released without loading — proven by the
// fact that filtered count is strictly less than the total.
func TestLoadTraceCursorsSyncBudgetPreCheck(t *testing.T) {
	tst, cleanup := newParityTSTable(t, tsTS1)
	defer cleanup()

	tr := newParityTrace()
	ctx := context.Background()
	qo := queryOptions{
		TraceQueryOptions: model.TraceQueryOptions{TagProjection: allTagProjections},
		schemaTagTypes:    testSchemaTagTypes,
		traceIDs:          []string{"trace1", "trace2", "trace3"},
	}

	phase1Batch, phase1Err := tr.buildVectorizedPhase1TraceBatch(ctx, qo, nil, sidx.QueryRequest{}, false, 0)
	require.NoError(t, phase1Err)

	sb, sbErr := tr.buildVectorizedScanBatch(ctx, []*tsTable{tst}, qo, phase1Batch)
	require.NoError(t, sbErr)
	require.NotNil(t, sb)

	totalCursors := len(sb.cursors)
	cursors := sb.cursors
	sb.cursors = nil
	releaseVectorizedScanBatch(sb)

	require.GreaterOrEqual(t, totalCursors, 2,
		"fixture must produce ≥2 cursors to exercise the budget gates; check newParityTSTable data")

	// Budget = 1 byte: cursor[0] always loads (usedBytes starts at 0, hard-stop fires only
	// when usedBytes >= 1). After cursor[0] loads any real span bytes, usedBytes >= 1 and
	// every subsequent cursor is stopped by the hard-stop gate before loadData is called.
	loaded, loadErr := loadTraceCursorsSync(ctx, cursors, 1)
	require.NoError(t, loadErr)
	require.Less(t, len(loaded), totalCursors,
		"hard-stop gate must prevent loading cursors once prior spans fill the 1-byte budget")
	for _, c := range loaded {
		require.NotEmpty(t, c.spans, "a cursor in the loaded set must have span data (loadData ran)")
		releaseBlockCursor(c)
	}
}

// TestLoadTraceCursorsSyncMetadataPreflight verifies that the metadata preflight
// gate (not the hard-stop gate) skips a cursor when its bm.uncompressedSpanSizeBytes
// estimate would push usedBytes over the budget.
// The test uses budget = firstSize + 1, so after cursor[0] loads (usedBytes = firstSize)
// the hard-stop condition (usedBytes >= budget) is FALSE, but the preflight condition
// (usedBytes + cursor[1].estimate > budget) is TRUE — proving the preflight gate fires.
func TestLoadTraceCursorsSyncMetadataPreflight(t *testing.T) {
	tst, cleanup := newParityTSTable(t, tsTS1)
	defer cleanup()

	tr := newParityTrace()
	ctx := context.Background()
	qo := queryOptions{
		TraceQueryOptions: model.TraceQueryOptions{TagProjection: allTagProjections},
		schemaTagTypes:    testSchemaTagTypes,
		traceIDs:          []string{"trace1", "trace2", "trace3"},
	}

	phase1Batch, phase1Err := tr.buildVectorizedPhase1TraceBatch(ctx, qo, nil, sidx.QueryRequest{}, false, 0)
	require.NoError(t, phase1Err)

	// First pass: measure cursor[0]'s actual span size with an unlimited budget.
	sb, sbErr := tr.buildVectorizedScanBatch(ctx, []*tsTable{tst}, qo, phase1Batch)
	require.NoError(t, sbErr)
	require.NotNil(t, sb)
	require.GreaterOrEqual(t, len(sb.cursors), 2,
		"fixture must produce ≥2 cursors; check newParityTSTable data")

	for _, c := range sb.cursors[1:] {
		releaseBlockCursor(c)
	}
	c0Only := sb.cursors[:1]
	sb.cursors = nil
	releaseVectorizedScanBatch(sb)

	loaded0, loadErr0 := loadTraceCursorsSync(ctx, c0Only, 0)
	require.NoError(t, loadErr0)
	require.Len(t, loaded0, 1, "cursor[0] must load from the test fixture")
	var firstSize int64
	for _, span := range loaded0[0].spans {
		firstSize += int64(len(span))
	}
	releaseBlockCursor(loaded0[0])
	require.Greater(t, firstSize, int64(0), "cursor[0] must have non-empty span data")

	// Second pass: fresh cursors for the actual preflight test.
	sb2, sbErr2 := tr.buildVectorizedScanBatch(ctx, []*tsTable{tst}, qo, phase1Batch)
	require.NoError(t, sbErr2)
	require.NotNil(t, sb2)
	require.GreaterOrEqual(t, len(sb2.cursors), 2)
	cursors2 := sb2.cursors
	sb2.cursors = nil
	releaseVectorizedScanBatch(sb2)
	for _, c := range cursors2[2:] {
		releaseBlockCursor(c)
	}

	// Set cursor[1]'s metadata estimate so the preflight fires:
	//   usedBytes (= firstSize) + estimate (= firstSize+1) > budget (= firstSize+1)
	// The hard-stop condition usedBytes >= budget is FALSE (firstSize < firstSize+1),
	// so only the preflight branch is responsible for skipping cursor[1].
	cursors2[1].bm.uncompressedSpanSizeBytes = uint64(firstSize) + 1
	budget := firstSize + 1

	loaded, loadErr := loadTraceCursorsSync(ctx, cursors2[:2], budget)
	require.NoError(t, loadErr)
	require.Len(t, loaded, 1,
		"metadata preflight must skip cursor[1]: usedBytes+estimate > budget while usedBytes < budget")
	require.NotEmpty(t, loaded[0].spans, "loaded cursor must have span data (loadData ran)")
	for _, c := range loaded {
		releaseBlockCursor(c)
	}
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
	filtered, err := loadTraceCursorsSync(ctx, []*blockCursor{cursor}, 0)
	require.Nil(t, filtered)
	require.Error(t, err)
}

func TestSyncSIDXQuerierToVectorizedIterators(t *testing.T) {
	iters, err := sidxInstancesToVectorizedIterators(context.Background(), []sidx.SIDX{
		&fakeSyncSIDX{fakeSIDX: &fakeSIDX{}, responses: []*sidx.QueryResponse{
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
	_, err := sidxInstancesToVectorizedIterators(context.Background(), []sidx.SIDX{
		&fakeSyncSIDX{fakeSIDX: &fakeSIDX{}, err: fmt.Errorf("boom")},
	}, sidx.QueryRequest{})
	require.Error(t, err)
}

type fakeSyncSIDX struct {
	err error
	*fakeSIDX
	responses []*sidx.QueryResponse
}

func (f *fakeSyncSIDX) QuerySync(_ context.Context, _ sidx.QueryRequest) ([]*sidx.QueryResponse, error) {
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

// fakeSIDXWithSync wraps fakeSIDX (satisfies sidx.SIDX) and adds QuerySync so that
// sidxInstancesToVectorizedIterators can cast it to syncSIDXQuerier.
type fakeSIDXWithSync struct {
	syncErr error
	*fakeSIDX
	syncResponses []*sidx.QueryResponse
}

func (f *fakeSIDXWithSync) QuerySync(_ context.Context, _ sidx.QueryRequest) ([]*sidx.QueryResponse, error) {
	return f.syncResponses, f.syncErr
}

// TestVectorizedOrderModeSmokeTest exercises the full order-mode vectorized path end-to-end:
// fake sidx → Phase-1 merge batch → Phase-2 scan → Pull results.
func TestVectorizedOrderModeSmokeTest(t *testing.T) {
	tst, cleanup := newParityTSTable(t, tsTS1)
	defer cleanup()

	tr := newParityTrace()

	ctx := context.Background()
	req := sidx.QueryRequest{
		Order: &index.OrderBy{Sort: modelv1.Sort_SORT_ASC},
	}

	// fakeSIDXInst returns trace1 < trace2 < trace3 in ASC key order.
	// PartIDs=0 won't match any real part; the bloom filter fallback picks them up.
	fakeSIDXInst := &fakeSIDXWithSync{
		fakeSIDX: &fakeSIDX{},
		syncResponses: []*sidx.QueryResponse{
			{
				Keys:    []int64{1, 2, 3},
				Data:    [][]byte{encodeTraceIDForTest("trace1"), encodeTraceIDForTest("trace2"), encodeTraceIDForTest("trace3")},
				SIDs:    []common.SeriesID{0, 0, 0},
				PartIDs: []uint64{0, 0, 0},
			},
		},
	}

	qo := queryOptions{
		TraceQueryOptions: model.TraceQueryOptions{
			TagProjection: allTagProjections,
			Order:         req.Order,
		},
		schemaTagTypes: testSchemaTagTypes,
	}

	// Phase 1: build ordered traceBatch from the fake sidx.
	batch, phase1Err := tr.buildVectorizedPhase1TraceBatch(ctx, qo, []sidx.SIDX{fakeSIDXInst}, req, true, 0)
	require.NoError(t, phase1Err)
	require.ElementsMatch(t, []string{"trace1", "trace2", "trace3"}, batch.traceIDsOrder)
	require.Equal(t, map[string]int64{"trace1": 1, "trace2": 2, "trace3": 3}, batch.keys)

	// Phase 2: scan parts → load span data.
	sb, phase2Err := tr.buildVectorizedScanBatch(ctx, []*tsTable{tst}, qo, batch)
	require.NoError(t, phase2Err)
	require.NotNil(t, sb)

	result, matErr := newVectorizedTraceQueryResult(ctx, sb, qo, nil, nil, nil, nil)
	require.NoError(t, matErr)
	require.NotNil(t, result)
	defer result.Release()

	got := collectResults(t, result)
	require.Len(t, got, 3, "order-mode vectorized query must return one result per trace")
	require.Equal(t, "trace1", got[0].TID)
	require.Equal(t, [][]byte{[]byte("span1")}, got[0].Spans)
	require.Equal(t, []string{"span1"}, got[0].SpanIDs)
	require.Equal(t, "trace2", got[1].TID)
	require.Equal(t, [][]byte{[]byte("span2")}, got[1].Spans)
	require.Equal(t, []string{"span2"}, got[1].SpanIDs)
	require.Equal(t, "trace3", got[2].TID)
	require.Equal(t, [][]byte{[]byte("span3")}, got[2].Spans)
	require.Equal(t, []string{"span3"}, got[2].SpanIDs)
}
