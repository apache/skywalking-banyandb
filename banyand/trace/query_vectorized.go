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
	"bytes"
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

// traceResultBucket accumulates the decoded spans, span IDs, and per-projected-tag
// values for a single traceID across all of its blocks. tid and key are constant
// across every span in the bucket. tagVals holds one slice per projected tag
// column (aligned with the tagCols order).
type traceResultBucket struct {
	tid     string
	spans   [][]byte
	spanIDs []string
	tagVals [][]*modelv1.TagValue
	key     int64
}

// materializeVectorizedTraceResults assembles []*model.TraceResult directly from
// the scanned (but not data-loaded) on-disk blocks, reproducing the exact
// semantics the former Phase-2 operator pipeline had (decode → group-by-traceID →
// per-trace emission) without any RecordBatch or operator overhead.
//
// budgetBytes is a soft span-loading threshold capping the cumulative uncompressed
// span bytes decoded across cursors. Pass QueryMemoryMiB<=0 (budgetBytes 0) to
// disable the cap. Two gates enforce it, matching the former blockBatchSource:
//  1. Hard stop: once usedBytes >= budgetBytes, remaining cursors are released
//     without decoding.
//  2. Metadata preflight: cursor.bm.uncompressedSpanSizeBytes predicts whether a
//     cursor would push usedBytes over budgetBytes; if so it is skipped before
//     decode. The usedBytes>0 guard makes the first cursor always decode so a
//     too-small budget never returns zero results.
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
	if len(batch.cursors) == 0 {
		return nil, nil
	}

	var tagCols []string
	if qo.TagProjection != nil && len(qo.TagProjection.Names) > 0 {
		tagCols = append([]string(nil), qo.TagProjection.Names...)
	}
	return assembleVectorizedTraceResults(ctx, batch, tagCols, qo.schemaTagTypes, budgetBytes)
}

// assembleVectorizedTraceResults performs the direct block-to-TraceResult
// assembly with an explicit byte budget. It owns batch.cursors and releases each
// after use. Callers must have validated that batch has at least one cursor.
func assembleVectorizedTraceResults(
	ctx context.Context,
	batch *scanBatch,
	tagCols []string,
	schemaTagTypes map[string]pbv1.ValueType,
	budgetBytes int64,
) ([]*model.TraceResult, error) {
	known := make(map[string]struct{}, len(batch.traceIDsOrder))
	for _, tid := range batch.traceIDsOrder {
		known[tid] = struct{}{}
	}
	buckets := make(map[string]*traceResultBucket)

	tmpBlock := generateBlock()
	defer releaseBlock(tmpBlock)

	var usedBytes int64
	for i := 0; i < len(batch.cursors); i++ {
		cursor := batch.cursors[i]
		if ctxErr := ctx.Err(); ctxErr != nil {
			for releaseIdx := i; releaseIdx < len(batch.cursors); releaseIdx++ {
				releaseBlockCursor(batch.cursors[releaseIdx])
			}
			batch.cursors = nil
			return nil, fmt.Errorf("interrupt while loading trace data: %w", ctxErr)
		}
		if budgetBytes > 0 {
			// Hard stop: prior cursors already filled the budget.
			if usedBytes >= budgetBytes {
				for releaseIdx := i; releaseIdx < len(batch.cursors); releaseIdx++ {
					releaseBlockCursor(batch.cursors[releaseIdx])
				}
				break
			}
			// Metadata preflight: skip the remaining cursors without decoding when
			// this cursor's uncompressed-size estimate would push usedBytes over
			// the budget. The usedBytes>0 guard ensures the first cursor decodes.
			if usedBytes > 0 && usedBytes+int64(cursor.bm.uncompressedSpanSizeBytes) > budgetBytes {
				for releaseIdx := i; releaseIdx < len(batch.cursors); releaseIdx++ {
					releaseBlockCursor(batch.cursors[releaseIdx])
				}
				break
			}
		}

		cursor.resolveTagProjection()
		tmpBlock.reset()
		tmpBlock.mustReadFrom(&cursor.tagValuesDecoder, cursor.p, cursor.bm)
		if len(tmpBlock.spans) == 0 {
			releaseBlockCursor(cursor)
			continue
		}

		// Account for the decoded span bytes before applying the known-trace drop
		// so the budget progression matches the former blockBatchSource, which
		// counted bytes for every non-empty decoded block (GroupByTraceID dropped
		// unknown rows afterward without affecting usedBytes).
		var spanBytes int64
		for spanIdx := range tmpBlock.spans {
			spanBytes += int64(len(tmpBlock.spans[spanIdx]))
		}
		usedBytes += spanBytes

		tid := cursor.bm.traceID
		if _, ok := known[tid]; !ok {
			releaseBlockCursor(cursor)
			continue
		}

		bucket, exists := buckets[tid]
		if !exists {
			bucket = &traceResultBucket{
				tid:     tid,
				key:     batch.keys[tid],
				tagVals: make([][]*modelv1.TagValue, len(tagCols)),
			}
			buckets[tid] = bucket
		}

		// Tag-column block matches are loop-invariant across spans; resolve once.
		blockTags := make([]*tag, len(tagCols))
		for tagIdx, tagName := range tagCols {
			blockTags[tagIdx] = findBlockTag(tmpBlock.tags, tagName, schemaTagTypes)
		}

		for spanIdx := range tmpBlock.spans {
			// tmpBlock is reused across blocks; the span bytes must be cloned.
			bucket.spans = append(bucket.spans, bytes.Clone(tmpBlock.spans[spanIdx]))
			spanID := ""
			if spanIdx < len(tmpBlock.spanIDs) {
				spanID = tmpBlock.spanIDs[spanIdx]
			}
			bucket.spanIDs = append(bucket.spanIDs, spanID)
			for tagIdx := range tagCols {
				tv := pbv1.NullTagValue
				if blockTag := blockTags[tagIdx]; blockTag != nil && spanIdx < len(blockTag.values) {
					tv = mustDecodeTagValue(blockTag.valueType, blockTag.values[spanIdx])
				}
				bucket.tagVals[tagIdx] = append(bucket.tagVals[tagIdx], tv)
			}
		}
		releaseBlockCursor(cursor)
	}
	batch.cursors = nil

	results := make([]*model.TraceResult, 0, len(batch.traceIDsOrder))
	for _, tid := range batch.traceIDsOrder {
		bucket, ok := buckets[tid]
		if !ok || len(bucket.spans) == 0 {
			continue
		}
		// Delete the bucket before emitting so a duplicate entry in traceIDsOrder
		// is skipped on the second encounter, matching GroupByTraceID.
		delete(buckets, tid)
		result := &model.TraceResult{
			TID:     tid,
			Key:     bucket.key,
			Spans:   bucket.spans,
			SpanIDs: bucket.spanIDs,
		}
		if len(tagCols) > 0 {
			result.Tags = make([]model.Tag, len(tagCols))
			for tagIdx := range tagCols {
				result.Tags[tagIdx] = model.Tag{Name: tagCols[tagIdx], Values: bucket.tagVals[tagIdx]}
			}
		}
		results = append(results, result)
	}
	return results, nil
}

// findBlockTag returns the tag whose decoded name and value type match the schema
// expectation, or nil when no such tag exists. A name match with a mismatched type
// keeps scanning so a correctly-typed variant still wins. It matches a projected
// tag name+type against a decoded block's []tag (here tmpBlock.tags).
func findBlockTag(tags []tag, tagName string, schemaTagTypes map[string]pbv1.ValueType) *tag {
	schemaType, hasSchemaType := schemaTagTypes[tagName]
	if !hasSchemaType {
		return nil
	}
	for tagIdx := range tags {
		blockTag := &tags[tagIdx]
		if decodeTypedTag(blockTag.name) != tagName || blockTag.valueType != schemaType {
			continue
		}
		return blockTag
	}
	return nil
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
		// Ordered SIDX path: cap Phase-1 at maxRows (= offset+limit) so only the top-N
		// distinct trace IDs in sorted-merge order are carried forward and materialized.
		// The merge is globally sorted, so the first maxRows traces are exactly the window
		// the outer traceLimit keeps; carrying the full match set (maxRows=0) wastes span
		// decode on traces the limit discards. maxRows=0 (no limit set) stays uncapped.
		plan, buildErr := vtrace.BuildMergePhase1(iters, sidxRequestDesc(req), maxRows, batchSize)
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

			sidxSet := make(map[string]struct{})
			if traceIDsFromSIDX, exists := batch.traceIDs[partID]; exists {
				for _, id := range traceIDsFromSIDX {
					sidxSet[id] = struct{}{}
				}
			}
			var idsForPart []string
			for _, traceID := range allTraceIDs {
				if _, inSIDX := sidxSet[traceID]; inSIDX || p.traceIDFilter.filter.MightContain(convert.StringToBytes(traceID)) {
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
