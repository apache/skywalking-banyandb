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

// newScanBatchFromCursors wraps already-scanned cursors in a *scanBatch so the
// budget tests can hand them straight to assembleVectorizedTraceResults, which
// owns and releases the cursors.
func newScanBatchFromCursors(phase1Batch traceBatch, cursors []*blockCursor) *scanBatch {
	return &scanBatch{traceBatch: phase1Batch, cursors: cursors}
}

// TestMaterializeBudgetHardStop verifies that the hard-stop gate in
// assembleVectorizedTraceResults prevents over-loading when a tiny budget is set.
// The test uses budget=1 byte so that after the first cursor decodes any real span
// data, usedBytes >= 1 and every subsequent cursor is stopped by the hard-stop gate
// (decoding ends early). Fewer than all traces are returned, proving later cursors
// were released without decoding.
func TestMaterializeBudgetHardStop(t *testing.T) {
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

	// Budget = 1 byte: the first cursor always decodes (usedBytes starts at 0, hard-stop fires
	// only when usedBytes >= 1). After it decodes any real span bytes, usedBytes >= 1 and every
	// subsequent cursor is stopped by the hard-stop gate before decode.
	batchForAssembly := newScanBatchFromCursors(phase1Batch, cursors)
	results, err := assembleVectorizedTraceResults(ctx, batchForAssembly, allTagProjections.Names, qo.schemaTagTypes, 1)
	require.NoError(t, err)

	require.GreaterOrEqual(t, len(results), 1, "the first block must have been decoded into at least one result")
	require.Less(t, len(results), totalCursors,
		"hard-stop gate must stop decoding cursors once prior spans fill the 1-byte budget")
	for _, result := range results {
		require.NotEmpty(t, result.Spans, "an emitted result must carry decoded spans")
	}
}

// TestMaterializeMetadataPreflight verifies that the metadata preflight gate (not
// the hard-stop gate) skips a cursor when its bm.uncompressedSpanSizeBytes estimate
// would push usedBytes over the budget.
// The test uses budget = firstSize + 1, so after the first cursor decodes
// (usedBytes = firstSize) the hard-stop condition (usedBytes >= budget) is FALSE,
// but the preflight condition (usedBytes + cursor[1].estimate > budget) is TRUE —
// proving the preflight gate fires and exactly one trace is materialized
// (first-block exception holds).
func TestMaterializeMetadataPreflight(t *testing.T) {
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

	batch0 := newScanBatchFromCursors(phase1Batch, c0Only)
	results0, err0 := assembleVectorizedTraceResults(ctx, batch0, allTagProjections.Names, qo.schemaTagTypes, 0)
	require.NoError(t, err0)
	require.Len(t, results0, 1, "cursor[0] must decode into exactly one trace from the test fixture")
	var firstSize int64
	for _, span := range results0[0].Spans {
		firstSize += int64(len(span))
	}
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

	batch2 := newScanBatchFromCursors(phase1Batch, cursors2[:2])
	results, err := assembleVectorizedTraceResults(ctx, batch2, allTagProjections.Names, qo.schemaTagTypes, budget)
	require.NoError(t, err)
	require.Len(t, results, 1,
		"metadata preflight must skip cursor[1]: usedBytes+estimate > budget while usedBytes < budget")
	require.NotEmpty(t, results[0].Spans, "the decoded cursor must have span data")
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

func TestMaterializeHonorsContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// assembleVectorizedTraceResults owns the cursors: on cancellation it releases
	// every remaining cursor (and the reusable tmpBlock) itself, so no manual
	// release here.
	cursor := generateBlockCursor()
	batch := newScanBatchFromCursors(traceBatch{}, []*blockCursor{cursor})
	results, err := assembleVectorizedTraceResults(ctx, batch, nil, nil, 0)
	require.Nil(t, results)
	require.Error(t, err)
	require.Nil(t, batch.cursors, "cancellation must clear batch.cursors after releasing them")
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

// TestMaterialize exercises the budget-unbounded direct-assembly path end-to-end:
// scanned cursors are decoded straight into []*model.TraceResult carrying the
// on-disk span/spanID/traceID/key data, emitted in phase-1 arrival order. With the
// tsTS1 fixture each trace has one span.
func TestMaterialize(t *testing.T) {
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

	results, matErr := materializeVectorizedTraceResults(ctx, sb, qo)
	require.NoError(t, matErr)
	require.Nil(t, sb.cursors, "materialize must clear batch.cursors after consuming them")

	require.GreaterOrEqual(t, len(results), 3, "tsTS1 contributes three traces, each with a span")

	gotTraceIDs := make(map[string]struct{})
	for _, result := range results {
		gotTraceIDs[result.TID] = struct{}{}
		require.Equal(t, phase1Batch.keys[result.TID], result.Key, "result key must match the phase-1 key for the traceID")
		require.Len(t, result.Spans, len(result.SpanIDs), "spans and spanIDs must be aligned")
		require.NotEmpty(t, result.Spans, "decoded span bytes must be non-empty")
		for _, span := range result.Spans {
			require.NotEmpty(t, span, "decoded span bytes must be non-empty")
		}
		require.Len(t, result.Tags, len(allTagProjections.Names), "each projected tag must produce a tag column")
		for tagIdx, tag := range result.Tags {
			require.Equal(t, allTagProjections.Names[tagIdx], tag.Name)
			require.Len(t, tag.Values, len(result.Spans), "tag values must be aligned with spans")
		}
	}
	require.Subset(t, []string{"trace1", "trace2", "trace3"}, mapsKeysForTest(gotTraceIDs))
}

func mapsKeysForTest(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
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
