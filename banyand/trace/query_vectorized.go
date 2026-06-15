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
	"maps"
	"slices"
	"sort"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/sidx"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	itersort "github.com/apache/skywalking-banyandb/pkg/iter/sort"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vtrace "github.com/apache/skywalking-banyandb/pkg/query/vectorized/trace"
)

type vectorizedTraceQueryResult struct {
	ctx              context.Context
	cancel           context.CancelFunc
	finishResultSpan func(int, error)
	recordResult     func(*model.TraceResult)
	err              error
	results          []*model.TraceResult
	segments         []storage.Segment[*tsTable, option]
	idx              int
	released         bool
}

func newVectorizedTraceQueryResult(
	ctx context.Context,
	batch *scanBatch,
	qo queryOptions,
	segments []storage.Segment[*tsTable, option],
	cancel context.CancelFunc,
	finishResultSpan func(int, error),
	recordResult func(*model.TraceResult),
) (*vectorizedTraceQueryResult, error) {
	results, materializeErr := materializeVectorizedTraceResults(ctx, batch, qo)
	releaseVectorizedScanBatch(batch)
	if materializeErr != nil {
		return nil, materializeErr
	}
	return &vectorizedTraceQueryResult{
		ctx:              ctx,
		cancel:           cancel,
		finishResultSpan: finishResultSpan,
		recordResult:     recordResult,
		results:          results,
		segments:         segments,
	}, nil
}

func (r *vectorizedTraceQueryResult) Pull() *model.TraceResult {
	select {
	case <-r.ctx.Done():
		r.err = r.ctx.Err()
		return &model.TraceResult{Error: r.err}
	default:
	}
	if r.err != nil {
		return &model.TraceResult{Error: r.err}
	}
	if r.idx >= len(r.results) {
		return nil
	}
	result := r.results[r.idx]
	r.idx++
	if r.recordResult != nil {
		r.recordResult(result)
	}
	return result
}

func (r *vectorizedTraceQueryResult) Release() {
	if r.released {
		return
	}
	r.released = true
	traceQueryResultTracker.Release(r)
	if r.cancel != nil {
		r.cancel()
	}
	for i := range r.segments {
		r.segments[i].DecRef()
	}
	r.segments = nil
	if r.finishResultSpan != nil {
		r.finishResultSpan(r.idx, r.err)
		r.finishResultSpan = nil
	}
}

func materializeVectorizedTraceResults(ctx context.Context, batch *scanBatch, qo queryOptions) ([]*model.TraceResult, error) {
	if batch == nil {
		return nil, nil
	}
	if batch.err != nil {
		return nil, batch.err
	}

	var budgetBytes int64
	if qo.QueryMemoryMiB > 0 {
		budgetBytes = int64(qo.QueryMemoryMiB) * 1024 * 1024
	}
	loaded, loadErr := loadTraceCursorsSync(ctx, batch.cursors, budgetBytes)
	batch.cursors = nil
	if loadErr != nil {
		return nil, loadErr
	}
	if len(loaded) == 0 {
		return nil, nil
	}

	var tagCols []string
	if qo.TagProjection != nil && len(qo.TagProjection.Names) > 0 {
		tagCols = append([]string(nil), qo.TagProjection.Names...)
	}
	schema := vtrace.NewPhase2Schema(tagCols)
	source := newLoadedCursorSource(loaded, schema, batch.keys, tagCols, qo.schemaTagTypes)

	phase2Plan, buildErr := vtrace.BuildPhase2(source, batch.traceIDsOrder)
	if buildErr != nil {
		source.closeAll()
		return nil, fmt.Errorf("build vectorized trace phase2: %w", buildErr)
	}
	if initErr := phase2Plan.Pipeline.Init(ctx); initErr != nil {
		phase2Plan.Pipeline.Close() //nolint:errcheck
		return nil, fmt.Errorf("init vectorized trace phase2: %w", initErr)
	}
	defer phase2Plan.Pipeline.Close() //nolint:errcheck

	results := make([]*model.TraceResult, 0, len(batch.traceIDsOrder))
	for {
		outBatch, nextErr := phase2Plan.Pipeline.Next(ctx)
		if nextErr != nil {
			return nil, fmt.Errorf("pull vectorized trace phase2: %w", nextErr)
		}
		if outBatch == nil {
			break
		}
		result := vtrace.BatchToTraceResult(outBatch, schema)
		if result != nil {
			results = append(results, result)
		}
	}
	return results, nil
}

// loadedCursorSource is a PullOperator that wraps already-loaded []*blockCursor
// and emits one Phase-2 RecordBatch per cursor.
type loadedCursorSource struct {
	schema         *vectorized.BatchSchema
	keys           map[string]int64
	schemaTagTypes map[string]pbv1.ValueType
	cursors        []*blockCursor
	tagCols        []string
	idx            int
	closed         bool
}

func newLoadedCursorSource(
	cursors []*blockCursor,
	schema *vectorized.BatchSchema,
	keys map[string]int64,
	tagCols []string,
	schemaTagTypes map[string]pbv1.ValueType,
) *loadedCursorSource {
	return &loadedCursorSource{
		cursors:        cursors,
		schema:         schema,
		keys:           keys,
		tagCols:        tagCols,
		schemaTagTypes: schemaTagTypes,
	}
}

// closeAll releases any remaining cursors without marking the source closed.
func (s *loadedCursorSource) closeAll() {
	for remainIdx := s.idx; remainIdx < len(s.cursors); remainIdx++ {
		releaseBlockCursor(s.cursors[remainIdx])
	}
	s.idx = len(s.cursors)
}

func (s *loadedCursorSource) Init(context.Context) error { return nil }

func (s *loadedCursorSource) OutputSchema() *vectorized.BatchSchema { return s.schema }

// Close releases all remaining unread cursors. Idempotent.
func (s *loadedCursorSource) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	s.closeAll()
	return nil
}

// NextBatch emits one Phase-2 RecordBatch per loaded blockCursor.
// Each span in the cursor becomes one row in the batch.
func (s *loadedCursorSource) NextBatch(ctx context.Context) (*vectorized.RecordBatch, error) {
	for s.idx < len(s.cursors) {
		if ctxErr := ctx.Err(); ctxErr != nil {
			s.closeAll()
			return nil, ctxErr
		}
		bc := s.cursors[s.idx]
		s.idx++
		if len(bc.spans) == 0 {
			releaseBlockCursor(bc)
			continue
		}

		batch := vectorized.NewRecordBatch(s.schema, len(bc.spans))
		tid := bc.bm.traceID
		key := s.keys[tid]

		tidCol := vtrace.Phase2TraceIDs(batch)
		keyCol := vtrace.Phase2Keys(batch)
		spanCol := vtrace.Phase2Spans(batch)
		spanIDCol := vtrace.Phase2SpanIDs(batch)
		// Tag-column handles and the cursor's matching tag are loop-invariant
		// across spans; resolve them once per cursor.
		tagCols := make([]*vectorized.TypedColumn[*modelv1.TagValue], len(s.tagCols))
		cursorTags := make([]*tag, len(s.tagCols))
		for tagIdx, tagName := range s.tagCols {
			tagCols[tagIdx] = vtrace.Phase2TagCol(batch, tagIdx)
			cursorTags[tagIdx] = findCursorTag(bc, tagName, s.schemaTagTypes)
		}

		for spanIdx, span := range bc.spans {
			tidCol.Append(tid)
			keyCol.Append(key)
			spanCol.Append(span)
			spanID := ""
			if spanIdx < len(bc.spanIDs) {
				spanID = bc.spanIDs[spanIdx]
			}
			spanIDCol.Append(spanID)

			for tagIdx := range s.tagCols {
				tv := pbv1.NullTagValue
				if cursorTag := cursorTags[tagIdx]; cursorTag != nil && spanIdx < len(cursorTag.values) {
					tv = mustDecodeTagValue(cursorTag.valueType, cursorTag.values[spanIdx])
				}
				tagCols[tagIdx].Append(tv)
			}
		}
		batch.Len = len(bc.spans)
		releaseBlockCursor(bc)
		return batch, nil
	}
	return nil, nil
}

// findCursorTag returns the cursor's tag whose decoded name and value type match
// the schema expectation, or nil when the cursor has no such tag. A name match
// with a mismatched type keeps scanning so a correctly-typed variant still wins.
func findCursorTag(bc *blockCursor, tagName string, schemaTagTypes map[string]pbv1.ValueType) *tag {
	schemaType, hasSchemaType := schemaTagTypes[tagName]
	if !hasSchemaType {
		return nil
	}
	for tagIdx := range bc.tags {
		cursorTag := &bc.tags[tagIdx]
		if decodeTypedTag(cursorTag.name) != tagName || cursorTag.valueType != schemaType {
			continue
		}
		return cursorTag
	}
	return nil
}

// loadTraceCursorsSync loads span data for each cursor from disk.
// budgetBytes is a soft span-loading threshold: it caps the cumulative uncompressed
// span bytes loaded across cursors. SIDX responses, tags, record-batch overhead, and
// other per-query allocations are not counted. Pass 0 to disable the cap.
// Two complementary gates enforce the threshold:
//  1. Hard stop: once usedBytes >= budgetBytes, remaining cursors are released
//     without calling loadData.
//  2. Metadata preflight: cursor.bm.uncompressedSpanSizeBytes (written at flush,
//     available without loading) is used to predict whether a cursor would push
//     usedBytes over budgetBytes. If so the cursor is skipped before loadData.
//
// First-block exception: the first cursor always loads regardless of the budget
// so that a query never returns zero results due to a too-small budget. This means
// a single oversized block can exceed budgetBytes; the threshold is best-effort,
// not a hard memory cap.
func loadTraceCursorsSync(ctx context.Context, cursors []*blockCursor, budgetBytes int64) ([]*blockCursor, error) {
	if len(cursors) == 0 {
		return nil, nil
	}
	var usedBytes int64
	filtered := cursors[:0]
	for curIdx, cursor := range cursors {
		select {
		case <-ctx.Done():
			// filtered aliases cursors[:0]: its entries are the loaded cursors,
			// cursors[curIdx:] are untouched; everything in between was already released.
			for _, loadedCursor := range filtered {
				releaseBlockCursor(loadedCursor)
			}
			for _, pendingCursor := range cursors[curIdx:] {
				releaseBlockCursor(pendingCursor)
			}
			return nil, fmt.Errorf("interrupt while loading trace data: %w", ctx.Err())
		default:
		}
		if budgetBytes > 0 {
			// Hard stop: prior cursors already filled the budget.
			if usedBytes >= budgetBytes {
				releaseBlockCursor(cursor)
				for _, pendingCursor := range cursors[curIdx+1:] {
					releaseBlockCursor(pendingCursor)
				}
				return filtered, nil
			}
			// Metadata preflight: skip this cursor without calling loadData when its
			// uncompressed-size estimate would push usedBytes over the budget.
			// The guard usedBytes > 0 ensures the first cursor always loads.
			if usedBytes > 0 && usedBytes+int64(cursor.bm.uncompressedSpanSizeBytes) > budgetBytes {
				releaseBlockCursor(cursor)
				for _, pendingCursor := range cursors[curIdx+1:] {
					releaseBlockCursor(pendingCursor)
				}
				return filtered, nil
			}
		}
		tmpBlock := generateBlock()
		loaded := cursor.loadData(tmpBlock)
		releaseBlock(tmpBlock)
		if !loaded {
			releaseBlockCursor(cursor)
			continue
		}
		if budgetBytes > 0 {
			for _, span := range cursor.spans {
				usedBytes += int64(len(span))
			}
		}
		filtered = append(filtered, cursor)
	}
	return filtered, nil
}

func releaseVectorizedScanBatch(batch *scanBatch) {
	if batch == nil {
		return
	}
	for _, cursor := range batch.cursors {
		if cursor != nil {
			releaseBlockCursor(cursor)
		}
	}
	batch.cursors = nil
	for _, snapshot := range batch.snapshots {
		snapshot.decRef()
	}
	batch.snapshots = nil
}

func sidxInstancesToVectorizedIterators(
	ctx context.Context,
	instances []sidx.SIDX,
	req sidx.QueryRequest,
) ([]itersort.Iterator[*vtrace.MergeItem], error) {
	iters := make([]itersort.Iterator[*vtrace.MergeItem], 0, len(instances))
	for instanceIdx, instance := range instances {
		responses, queryErr := instance.QuerySync(ctx, req)
		if queryErr != nil {
			return nil, fmt.Errorf("query sidx instance %d synchronously: %w", instanceIdx, queryErr)
		}
		batches, convertErr := sidxResponsesToVectorizedBatches(responses)
		if convertErr != nil {
			return nil, fmt.Errorf("convert sidx instance %d response: %w", instanceIdx, convertErr)
		}
		iters = append(iters, vtrace.NewSidxResponseIterator(batches))
	}
	return iters, nil
}

func (t *trace) buildVectorizedPhase1TraceBatch(
	ctx context.Context,
	qo queryOptions,
	sidxInstances []sidx.SIDX,
	req sidx.QueryRequest,
	useSIDX bool,
	maxTraceSize int,
) (traceBatch, error) {
	batchSize := t.vectorized.BatchSize
	maxRows := uint32(0)
	if maxTraceSize > 0 {
		maxRows = uint32(maxTraceSize)
	}
	switch {
	case len(qo.traceIDs) > 0:
		// Mirror the push path: truncate the raw list to maxTraceSize first (preserving
		// duplicates), then let Phase-1's DistinctTraceID deduplicate within that window.
		ids := qo.traceIDs
		if maxRows > 0 && int(maxRows) < len(ids) {
			ids = ids[:maxRows]
		}
		plan, buildErr := vtrace.BuildStaticPhase1(ids, nil, 0, batchSize)
		if buildErr != nil {
			return traceBatch{}, fmt.Errorf("build static vectorized trace phase1: %w", buildErr)
		}
		return drainVectorizedPhase1(ctx, plan, 0)
	case useSIDX:
		iters, iterErr := sidxInstancesToVectorizedIterators(ctx, sidxInstances, req)
		if iterErr != nil {
			return traceBatch{}, iterErr
		}
		// Ordered SIDX path: MaxTraceSize controls SIDX batch size (MaxBatchSize in the
		// request), not the total result cap. The push path emits all matching traces in
		// batches; vectorized must do the same. Pass 0 to disable the Phase-1 trace cap.
		plan, buildErr := vtrace.BuildMergePhase1(iters, sidxRequestDesc(req), 0, batchSize)
		if buildErr != nil {
			return traceBatch{}, fmt.Errorf("build ordered vectorized trace phase1: %w", buildErr)
		}
		return drainVectorizedPhase1(ctx, plan, 0)
	default:
		return traceBatch{}, fmt.Errorf("invalid query options: either traceIDs or order must be specified")
	}
}

func (t *trace) buildVectorizedScanBatch(ctx context.Context, tables []*tsTable, qo queryOptions, batch traceBatch) (*scanBatch, error) {
	if batch.err != nil {
		return &scanBatch{traceBatch: batch, err: batch.err}, nil
	}
	snapshots := make([]*snapshot, 0, len(tables))
	for _, table := range tables {
		s := table.currentSnapshot()
		if s == nil {
			continue
		}
		snapshots = append(snapshots, s)
	}
	if len(snapshots) == 0 {
		return &scanBatch{traceBatch: batch}, nil
	}

	partSelectionCtx, finishPartSelection := startPartSelectionSpan(ctx, &batch, snapshots)
	parts, groupedIDs, metrics := selectVectorizedTraceParts(batch, snapshots)
	if finishPartSelection != nil {
		finishPartSelection(metrics, len(parts))
	}
	cursors, scanErr := t.scanPartsInlineSync(partSelectionCtx, parts, groupedIDs, qo)
	if scanErr != nil {
		for _, s := range snapshots {
			s.decRef()
		}
		return nil, scanErr
	}
	return &scanBatch{
		traceBatch: batch,
		cursors:    cursors,
		snapshots:  snapshots,
	}, nil
}

func selectVectorizedTraceParts(batch traceBatch, snapshots []*snapshot) ([]*part, [][]string, *partSelectionMetrics) {
	parts := make([]*part, 0)
	groupedIDs := make([][]string, 0)
	allTraceIDs := make([]string, 0)
	for _, ids := range batch.traceIDs {
		allTraceIDs = append(allTraceIDs, ids...)
	}
	sort.Strings(allTraceIDs)

	bloomFilteredPartIDs := make([]uint64, 0)
	totalGroupedIDs := 0
	for _, s := range snapshots {
		for _, pw := range s.parts {
			p := pw.p
			partID := p.partMetadata.ID

			var idsFromSIDX []string
			if traceIDsFromSIDX, exists := batch.traceIDs[partID]; exists {
				idsFromSIDX = append([]string(nil), traceIDsFromSIDX...)
			}
			var idsForPart []string
			for _, traceID := range allTraceIDs {
				if slices.Contains(idsFromSIDX, traceID) || p.traceIDFilter.filter.MightContain(convert.StringToBytes(traceID)) {
					idsForPart = append(idsForPart, traceID)
				}
			}
			if len(idsForPart) > 0 {
				parts = append(parts, p)
				groupedIDs = append(groupedIDs, idsForPart)
				totalGroupedIDs += len(idsForPart)
				continue
			}
			bloomFilteredPartIDs = append(bloomFilteredPartIDs, partID)
		}
	}
	return parts, groupedIDs, &partSelectionMetrics{
		bloomFilteredPartIDs: bloomFilteredPartIDs,
		totalGroupedIDs:      totalGroupedIDs,
	}
}

func sidxRequestDesc(req sidx.QueryRequest) bool {
	return req.Order != nil && req.Order.Sort == modelv1.Sort_SORT_DESC
}

func sidxResponseToVectorizedBatch(resp *sidx.QueryResponse) (*vtrace.SidxRowBatch, error) {
	if resp == nil {
		return nil, nil
	}
	if validateErr := resp.Validate(); validateErr != nil {
		return nil, fmt.Errorf("invalid sidx query response: %w", validateErr)
	}
	out := &vtrace.SidxRowBatch{
		Error:   resp.Error,
		Keys:    append([]int64(nil), resp.Keys...),
		Data:    make([][]byte, len(resp.Data)),
		SIDs:    make([]int64, len(resp.SIDs)),
		PartIDs: make([]int64, len(resp.PartIDs)),
	}
	for rowIdx, data := range resp.Data {
		out.Data[rowIdx] = append([]byte(nil), data...)
	}
	for rowIdx, seriesID := range resp.SIDs {
		out.SIDs[rowIdx] = int64(seriesID)
	}
	for rowIdx, partID := range resp.PartIDs {
		out.PartIDs[rowIdx] = int64(partID)
	}
	return out, nil
}

func sidxResponsesToVectorizedBatches(responses []*sidx.QueryResponse) ([]*vtrace.SidxRowBatch, error) {
	out := make([]*vtrace.SidxRowBatch, 0, len(responses))
	for _, resp := range responses {
		batch, convertErr := sidxResponseToVectorizedBatch(resp)
		if convertErr != nil {
			return nil, convertErr
		}
		if batch != nil {
			out = append(out, batch)
		}
	}
	return out, nil
}

func drainVectorizedPhase1(ctx context.Context, plan *vtrace.Phase1Plan, seq int) (traceBatch, error) {
	if plan == nil || plan.Pipeline == nil || plan.Carry == nil {
		return traceBatch{}, fmt.Errorf("vectorized trace phase1 plan is nil")
	}
	if initErr := plan.Pipeline.Init(ctx); initErr != nil {
		return traceBatch{}, fmt.Errorf("initialize vectorized trace phase1: %w", initErr)
	}
	defer plan.Pipeline.Close() //nolint:errcheck // best-effort cleanup; query result path reports pull errors.
	for {
		batch, nextErr := plan.Pipeline.Next(ctx)
		if nextErr != nil {
			return traceBatch{}, fmt.Errorf("pull vectorized trace phase1: %w", nextErr)
		}
		if batch == nil {
			break
		}
	}
	carry := plan.Carry
	return traceBatch{
		seq:           seq,
		traceIDs:      cloneTraceIDsByPart(carry.TraceIDsByPart()),
		traceIDsOrder: append([]string(nil), carry.Order()...),
		keys:          maps.Clone(carry.Keys()),
	}, nil
}

func cloneTraceIDsByPart(in map[int64][]string) map[uint64][]string {
	out := make(map[uint64][]string, len(in))
	for partID, traceIDs := range in {
		out[uint64(partID)] = slices.Clone(traceIDs)
	}
	return out
}
