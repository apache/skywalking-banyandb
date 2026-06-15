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
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/sidx"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	vtrace "github.com/apache/skywalking-banyandb/pkg/query/vectorized/trace"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// collectResults drains a TraceQueryResult into a slice, asserting no per-result error.
func collectResults(t *testing.T, result model.TraceQueryResult) []*model.TraceResult {
	t.Helper()
	var got []*model.TraceResult
	for {
		r := result.Pull()
		if r == nil {
			break
		}
		require.NoError(t, r.Error)
		got = append(got, r)
	}
	return got
}

// newParityTSTable sets up an in-memory tsTable populated with the given trace sets.
func newParityTSTable(tb testing.TB, traceSets ...*traces) (*tsTable, func()) {
	tb.Helper()
	tmpPath, defFn := test.Space(require.New(tb))
	tst, tsErr := newTSTable(
		fs.NewLocalFileSystem(), tmpPath, common.Position{},
		logger.GetLogger("test"), timestamp.TimeRange{},
		option{flushTimeout: 0, mergePolicy: newDefaultMergePolicyForTesting(), protector: protector.Nop{}},
		nil,
	)
	require.NoError(tb, tsErr)
	for _, ts := range traceSets {
		tst.mustAddTraces(ts, nil)
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond)
	return tst, func() { tst.Close(); defFn() }
}

func newParityTrace() *trace {
	return &trace{
		pm:         protector.Nop{},
		vectorized: vtrace.VectorizedConfig{Enabled: true, BatchSize: 10, QueryMemoryMiB: 100},
	}
}

func pushLookupResult(
	ctx context.Context,
	tr *trace,
	tables []*tsTable,
	qo queryOptions,
	traceIDs []string,
	maxTraceSize int,
) *queryResult {
	pushKeys := make(map[string]int64)
	traceBatchCh := staticTraceBatchSource(ctx, traceIDs, maxTraceSize, pushKeys)
	cursorBatchCh := tr.startBlockScanStage(ctx, tables, qo, traceBatchCh)
	return &queryResult{
		ctx:           ctx,
		keys:          pushKeys,
		tagProjection: qo.TagProjection,
		cursorBatchCh: cursorBatchCh,
	}
}

func pullLookupResult(
	ctx context.Context,
	tr *trace,
	tables []*tsTable,
	qo queryOptions,
	traceIDs []string,
	maxTraceSize int,
) (*vectorizedTraceQueryResult, error) {
	qo.traceIDs = traceIDs
	batch, batchErr := tr.buildVectorizedPhase1TraceBatch(ctx, qo, nil, sidx.QueryRequest{}, false, maxTraceSize)
	if batchErr != nil {
		return nil, batchErr
	}
	sb, sbErr := tr.buildVectorizedScanBatch(ctx, tables, qo, batch)
	if sbErr != nil {
		return nil, sbErr
	}
	return newVectorizedTraceQueryResult(ctx, sb, qo, nil, nil, nil, nil)
}

func pushOrderResult(
	ctx context.Context,
	tr *trace,
	tables []*tsTable,
	qo queryOptions,
	fakeSIDXInst *fakeSIDXWithSync,
	req sidx.QueryRequest,
	maxTraceSize int,
) *queryResult {
	traceBatchCh, streamDone := tr.streamSIDXTraceBatches(ctx, []sidx.SIDX{fakeSIDXInst}, req, maxTraceSize)
	cursorBatchCh := tr.startBlockScanStage(ctx, tables, qo, traceBatchCh)
	return &queryResult{
		ctx:           ctx,
		keys:          make(map[string]int64),
		tagProjection: qo.TagProjection,
		cursorBatchCh: cursorBatchCh,
		streamDone:    streamDone,
	}
}

func pullOrderResult(
	ctx context.Context,
	tr *trace,
	tables []*tsTable,
	qo queryOptions,
	fakeSIDXInst *fakeSIDXWithSync,
	req sidx.QueryRequest,
	maxTraceSize int,
) (*vectorizedTraceQueryResult, error) {
	batch, batchErr := tr.buildVectorizedPhase1TraceBatch(ctx, qo, []sidx.SIDX{fakeSIDXInst}, req, true, maxTraceSize)
	if batchErr != nil {
		return nil, batchErr
	}
	sb, sbErr := tr.buildVectorizedScanBatch(ctx, tables, qo, batch)
	if sbErr != nil {
		return nil, sbErr
	}
	return newVectorizedTraceQueryResult(ctx, sb, qo, nil, nil, nil, nil)
}

// TestVectorizedParityLookup verifies that the push and pull paths return
// byte-identical model.TraceResult sequences for traceID lookup mode.
func TestVectorizedParityLookup(t *testing.T) {
	tst, cleanup := newParityTSTable(t, tsTS1, tsTS2)
	defer cleanup()

	tr := newParityTrace()
	tables := []*tsTable{tst}

	tests := []struct {
		tagProjection *model.TagProjection
		name          string
		traceIDs      []string
		maxTraceSize  int
		wantCount     int
		wantNilTags   bool
	}{
		{name: "single trace", traceIDs: []string{"trace1"}, tagProjection: allTagProjections, wantCount: 1},
		{name: "all traces", traceIDs: []string{"trace1", "trace2", "trace3"}, tagProjection: allTagProjections, wantCount: 3},
		{name: "maxTraceSize=2", traceIDs: []string{"trace1", "trace2", "trace3"}, maxTraceSize: 2, tagProjection: allTagProjections, wantCount: 2},
		{name: "missing trace", traceIDs: []string{"trace-not-exist"}, tagProjection: allTagProjections, wantCount: 0},
		// AC8: identity-only projection → after omitIdentityTagProjection strips identity tags the
		// storage-level projection is nil or empty; both push and pull must produce r.Tags == nil.
		{name: "nil projection (identity-stripped)", traceIDs: []string{"trace1"}, tagProjection: nil, wantCount: 1, wantNilTags: true},
		{name: "empty projection (identity-stripped)", traceIDs: []string{"trace1"}, tagProjection: &model.TagProjection{Names: []string{}}, wantCount: 1, wantNilTags: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			qo := queryOptions{
				TraceQueryOptions: model.TraceQueryOptions{TagProjection: tt.tagProjection},
				schemaTagTypes:    testSchemaTagTypes,
			}

			pushRes := pushLookupResult(ctx, tr, tables, qo, tt.traceIDs, tt.maxTraceSize)
			defer pushRes.Release()
			pushGot := collectResults(t, pushRes)

			pullRes, pullErr := pullLookupResult(ctx, tr, tables, qo, tt.traceIDs, tt.maxTraceSize)
			require.NoError(t, pullErr)
			defer pullRes.Release()
			pullGot := collectResults(t, pullRes)

			require.Len(t, pushGot, tt.wantCount, "push path result count")
			require.Len(t, pullGot, tt.wantCount, "pull path result count")

			if tt.wantNilTags {
				for _, r := range pushGot {
					require.Nil(t, r.Tags, "push path must produce nil Tags for empty projection")
				}
				for _, r := range pullGot {
					require.Nil(t, r.Tags, "pull path must produce nil Tags for empty projection")
				}
			}

			if diff := cmp.Diff(pushGot, pullGot, protocmp.Transform()); diff != "" {
				t.Errorf("push vs pull lookup mismatch (-push +pull):\n%s", diff)
			}
		})
	}
}

// TestVectorizedParityOrderMode verifies that the push and pull paths return
// byte-identical model.TraceResult sequences for order mode (ASC and DESC).
func TestVectorizedParityOrderMode(t *testing.T) {
	tst, cleanup := newParityTSTable(t, tsTS1)
	defer cleanup()

	tr := newParityTrace()
	tables := []*tsTable{tst}
	qo := queryOptions{
		TraceQueryOptions: model.TraceQueryOptions{TagProjection: allTagProjections},
		schemaTagTypes:    testSchemaTagTypes,
	}

	tests := []struct {
		name         string
		keys         []int64
		traceIDs     []string
		maxTraceSize int
		wantCount    int
		sortDir      modelv1.Sort
	}{
		{
			name:      "ASC order all traces",
			sortDir:   modelv1.Sort_SORT_ASC,
			keys:      []int64{1, 2, 3},
			traceIDs:  []string{"trace1", "trace2", "trace3"},
			wantCount: 3,
		},
		{
			name:      "DESC order all traces",
			sortDir:   modelv1.Sort_SORT_DESC,
			keys:      []int64{3, 2, 1},
			traceIDs:  []string{"trace3", "trace2", "trace1"},
			wantCount: 3,
		},
		{
			// MaxTraceSize is a batch-size hint for ordered SIDX queries, not a total cap.
			// The push path emits all 3 traces in batches of 2; vectorized must match.
			name:         "ASC order MaxTraceSize=2 with 3 traces returns all 3",
			sortDir:      modelv1.Sort_SORT_ASC,
			keys:         []int64{1, 2, 3},
			traceIDs:     []string{"trace1", "trace2", "trace3"},
			maxTraceSize: 2,
			wantCount:    3,
		},
		{
			name:         "DESC order MaxTraceSize=2 with 3 traces returns all 3",
			sortDir:      modelv1.Sort_SORT_DESC,
			keys:         []int64{3, 2, 1},
			traceIDs:     []string{"trace3", "trace2", "trace1"},
			maxTraceSize: 2,
			wantCount:    3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()

			req := sidx.QueryRequest{
				Order: &index.OrderBy{Sort: tt.sortDir},
			}

			// Build response data — PartIDs=0 so bloom-filter fallback picks up the parts.
			respData := make([][]byte, len(tt.traceIDs))
			for dataIdx, tid := range tt.traceIDs {
				respData[dataIdx] = encodeTraceIDForTest(tid)
			}
			sids := make([]common.SeriesID, len(tt.traceIDs))
			partIDs := make([]uint64, len(tt.traceIDs))

			// Push (StreamingQuery) and pull (QuerySync) share one response slice so
			// they always use identical data and cannot drift if either path is rewired.
			sharedResp := []*sidx.QueryResponse{
				{Keys: tt.keys, Data: respData, SIDs: sids, PartIDs: partIDs},
			}
			fakeSIDXInst := &fakeSIDXWithSync{
				fakeSIDX:      &fakeSIDX{responses: sharedResp},
				syncResponses: sharedResp,
			}

			pushRes := pushOrderResult(ctx, tr, tables, qo, fakeSIDXInst, req, tt.maxTraceSize)
			defer pushRes.Release()
			pushGot := collectResults(t, pushRes)

			pullRes, pullErr := pullOrderResult(ctx, tr, tables, qo, fakeSIDXInst, req, tt.maxTraceSize)
			require.NoError(t, pullErr)
			defer pullRes.Release()
			pullGot := collectResults(t, pullRes)

			require.Len(t, pushGot, tt.wantCount, "push path result count")
			require.Len(t, pullGot, tt.wantCount, "pull path result count")

			if diff := cmp.Diff(pushGot, pullGot, protocmp.Transform()); diff != "" {
				t.Errorf("push vs pull order mismatch (-push +pull):\n%s", diff)
			}
		})
	}
}

// TestVectorizedOverScanBound verifies AC3: with a MaxTraceSize cap, the pull path
// builds a phase-1 batch limited to MaxTraceSize trace IDs, so the phase-2 cursor
// count is strictly less than the uncapped cursor count on the same dataset.
func TestVectorizedOverScanBound(t *testing.T) {
	tst, cleanup := newParityTSTable(t, tsTS1, tsTS2)
	defer cleanup()

	tr := newParityTrace()
	ctx := context.Background()
	tables := []*tsTable{tst}
	qo := queryOptions{
		TraceQueryOptions: model.TraceQueryOptions{TagProjection: allTagProjections},
		schemaTagTypes:    testSchemaTagTypes,
	}

	buildCursorCount := func(traceIDs []string, maxSize int) int {
		localQO := qo
		localQO.traceIDs = traceIDs
		batch, batchErr := tr.buildVectorizedPhase1TraceBatch(ctx, localQO, nil, sidx.QueryRequest{}, false, maxSize)
		require.NoError(t, batchErr)
		sb, sbErr := tr.buildVectorizedScanBatch(ctx, tables, localQO, batch)
		require.NoError(t, sbErr)
		count := len(sb.cursors)
		releaseVectorizedScanBatch(sb)
		return count
	}

	allIDs := []string{"trace1", "trace2", "trace3"}
	uncappedCursors := buildCursorCount(allIDs, 0) // no cap → all 3 traces
	cappedCursors := buildCursorCount(allIDs, 2)   // capped → at most 2 traces

	require.Greater(t, uncappedCursors, 0, "uncapped query must scan at least one cursor")
	require.LessOrEqual(t, cappedCursors, uncappedCursors,
		"AC3: pull path with maxTraceSize=2 must scan ≤ cursors of uncapped query")
	require.Less(t, cappedCursors, uncappedCursors,
		"AC3: pull path with maxTraceSize=2 must scan fewer cursors than uncapped (early termination)")
}

// zeroQuotaProtector implements protector.Memory with zero available bytes,
// simulating a fully-exhausted memory quota.
type zeroQuotaProtector struct{ protector.Nop }

func (zeroQuotaProtector) AvailableBytes() int64 { return 0 }

func (zeroQuotaProtector) AcquireResource(_ context.Context, _ uint64) error {
	return fmt.Errorf("quota exhausted")
}

// TestVectorizedMemoryQuotaAC5 verifies AC5: the pull path respects the memory
// quota via t.pm.AvailableBytes() — quota=0 on a non-empty table returns an error
// (no cursors accumulated), matching the push path's scanPartsInline quota semantics.
func TestVectorizedMemoryQuotaAC5(t *testing.T) {
	tst, cleanup := newParityTSTable(t, tsTS1)
	defer cleanup()

	ctx := context.Background()
	tables := []*tsTable{tst}
	qo := queryOptions{
		TraceQueryOptions: model.TraceQueryOptions{TagProjection: allTagProjections},
		schemaTagTypes:    testSchemaTagTypes,
	}

	// Pull path with unlimited quota succeeds.
	unlimitedTrace := &trace{
		pm:         protector.Nop{},
		vectorized: vtrace.VectorizedConfig{Enabled: true, BatchSize: 10, QueryMemoryMiB: 100},
	}
	qo.traceIDs = []string{"trace1"}
	unlimitedBatch, unlimitedBatchErr := unlimitedTrace.buildVectorizedPhase1TraceBatch(ctx, qo, nil, sidx.QueryRequest{}, false, 0)
	require.NoError(t, unlimitedBatchErr)
	unlimitedSB, unlimitedSBErr := unlimitedTrace.buildVectorizedScanBatch(ctx, tables, qo, unlimitedBatch)
	require.NoError(t, unlimitedSBErr, "unlimited quota must succeed on non-empty table")
	require.NotEmpty(t, unlimitedSB.cursors, "unlimited quota must yield cursors")
	releaseVectorizedScanBatch(unlimitedSB)

	// Pull path with zero quota (fully exhausted) returns a scan error when no cursors
	// have been accumulated yet, matching push path's "quota exceeded" early-exit.
	zeroQuotaTrace := &trace{
		pm:         zeroQuotaProtector{},
		vectorized: vtrace.VectorizedConfig{Enabled: true, BatchSize: 10, QueryMemoryMiB: 100},
	}
	qo.traceIDs = []string{"trace1"}
	zeroBatch, zeroBatchErr := zeroQuotaTrace.buildVectorizedPhase1TraceBatch(ctx, qo, nil, sidx.QueryRequest{}, false, 0)
	require.NoError(t, zeroBatchErr)
	zeroSB, zeroSBErr := zeroQuotaTrace.buildVectorizedScanBatch(ctx, tables, qo, zeroBatch)
	require.Error(t, zeroSBErr, "AC5: zero-quota pull path must return error when no cursors were accumulated")
	require.Nil(t, zeroSB, "AC5: zero-quota scan batch must be nil on error")
}

// collectAllSpanMessages recursively collects span Message fields from a span tree.
func collectAllSpanMessages(spans []*commonv1.Span) []string {
	var msgs []string
	for _, s := range spans {
		if s == nil {
			continue
		}
		msgs = append(msgs, s.Message)
		msgs = append(msgs, collectAllSpanMessages(s.Children)...)
	}
	return msgs
}

// TestVectorizedTracingSpansAC7 verifies AC7: the pull path emits the same key
// spans as the push path — "part-selection" and "scan-blocks" — when a tracer
// is present in the context.
func TestVectorizedTracingSpansAC7(t *testing.T) {
	tst, cleanup := newParityTSTable(t, tsTS1)
	defer cleanup()

	tr := newParityTrace()
	tables := []*tsTable{tst}
	qo := queryOptions{
		TraceQueryOptions: model.TraceQueryOptions{TagProjection: allTagProjections},
		schemaTagTypes:    testSchemaTagTypes,
	}
	qo.traceIDs = []string{"trace1"}

	tracer, tracingCtx := query.NewTracer(context.Background(), "ac7-test")

	batch, batchErr := tr.buildVectorizedPhase1TraceBatch(tracingCtx, qo, nil, sidx.QueryRequest{}, false, 0)
	require.NoError(t, batchErr)
	sb, sbErr := tr.buildVectorizedScanBatch(tracingCtx, tables, qo, batch)
	require.NoError(t, sbErr)
	releaseVectorizedScanBatch(sb)

	allMsgs := collectAllSpanMessages(tracer.ToProto().Spans)
	require.Contains(t, allMsgs, "part-selection", "AC7: pull path must emit part-selection span")
	require.Contains(t, allMsgs, "scan-blocks", "AC7: pull path must emit scan-blocks span")
}

// TestVectorizedTTLExpiredParity verifies AC9: when TTL-expired segment filtering
// produces an empty tables slice (as SelectSegments returns nothing for expired data),
// both push and pull paths return zero results identically.
func TestVectorizedTTLExpiredParity(t *testing.T) {
	tr := newParityTrace()
	ctx := context.Background()
	qo := queryOptions{
		TraceQueryOptions: model.TraceQueryOptions{TagProjection: allTagProjections},
		schemaTagTypes:    testSchemaTagTypes,
	}
	traceIDs := []string{"trace1", "trace2"}

	// Empty tables simulates all segments being TTL-expired (SelectSegments returned nothing).
	emptyTables := []*tsTable{}

	pushRes := pushLookupResult(ctx, tr, emptyTables, qo, traceIDs, 0)
	defer pushRes.Release()
	pushGot := collectResults(t, pushRes)

	pullRes, pullErr := pullLookupResult(ctx, tr, emptyTables, qo, traceIDs, 0)
	require.NoError(t, pullErr)
	defer pullRes.Release()
	pullGot := collectResults(t, pullRes)

	require.Empty(t, pushGot, "push path must return no results for TTL-expired (empty) tables")
	require.Empty(t, pullGot, "pull path must return no results for TTL-expired (empty) tables")
}
