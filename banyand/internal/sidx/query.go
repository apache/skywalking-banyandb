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

package sidx

import (
	"context"
	"math"
	"sort"
	"sync"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/cgroups"
	"github.com/apache/skywalking-banyandb/pkg/query"
)

// StreamingQuery implements the streaming query API defined on SIDX.
func (s *sidx) StreamingQuery(ctx context.Context, req QueryRequest) (<-chan *QueryResponse, <-chan error) {
	chanSize := req.MaxBatchSize
	if chanSize < 0 {
		chanSize = 0
	}

	resultsCh := make(chan *QueryResponse, chanSize)
	errCh := make(chan error, 1)

	go func() {
		defer close(resultsCh)
		defer close(errCh)

		if err := req.Validate(); err != nil {
			errCh <- err
			return
		}

		if err := s.runStreamingQuery(ctx, req, resultsCh); err != nil {
			// Propagate fatal errors to the caller.
			errCh <- err
		}
	}()

	return resultsCh, errCh
}

type streamingStats struct {
	batches  int
	elements int
}

func (s *streamingStats) record(chunk *QueryResponse) {
	if s == nil || chunk == nil {
		return
	}
	s.batches++
	s.elements += chunk.Len()
}

type batchMetrics struct {
	totalBatchBlockCount   int
	totalCursorsBuilt      int
	totalHeapSizeAfterPush int
	batchesProcessed       int
}

// runStreamingQuery executes the streaming query workflow and pushes batches to resultsCh.
func (s *sidx) runStreamingQuery(ctx context.Context, req QueryRequest, resultsCh chan<- *QueryResponse) (err error) {
	s.totalQueries.Add(1)

	var stats streamingStats

	ctx, span := startStreamingSpan(ctx, req)
	defer func() {
		finalizeStreamingSpan(span, stats, &err)
	}()

	snap := s.currentSnapshot()
	if snap == nil {
		if span != nil {
			span.Tag("snapshot", "nil")
		}
		return nil
	}
	defer snap.decRef()

	resources, ok := s.prepareStreamingResources(ctx, req, snap, span)
	if !ok {
		return nil
	}
	defer resources.cleanup()

	if loopErr := s.processStreamingLoop(ctx, req, resources, resultsCh, &stats); loopErr != nil {
		return loopErr
	}

	return nil
}

type streamingQueryResources struct {
	tagsToLoad map[string]struct{}
	blockCh    <-chan *blockScanResultBatch
	heap       *blockCursorHeap
	cleanup    func()
	asc        bool
}

func startStreamingSpan(ctx context.Context, req QueryRequest) (context.Context, *query.Span) {
	tracer := query.GetTracer(ctx)
	if tracer == nil {
		return ctx, nil
	}

	span, spanCtx := tracer.StartSpan(ctx, "sidx.run-streaming-query")
	span.Tagf("max_batch_size", "%d", req.MaxBatchSize)
	if req.Filter != nil {
		span.Tag("filter_present", "true")
	} else {
		span.Tag("filter_present", "false")
	}
	if req.Order != nil {
		if req.Order.Index != nil && req.Order.Index.GetMetadata() != nil {
			span.Tag("order_index", req.Order.Index.GetMetadata().GetName())
		}
		span.Tag("order_sort", req.Order.Sort.String())
		span.Tagf("order_type", "%d", req.Order.Type)
	} else {
		span.Tag("order_sort", "none")
	}

	return spanCtx, span
}

func finalizeStreamingSpan(span *query.Span, stats streamingStats, errPtr *error) {
	if span == nil {
		return
	}
	span.Tagf("responses_emitted", "%d", stats.batches)
	span.Tagf("elements_emitted", "%d", stats.elements)
	if errPtr != nil && *errPtr != nil {
		span.Error(*errPtr)
	}
	span.Stop()
}

func (s *sidx) prepareStreamingResources(
	ctx context.Context,
	req QueryRequest,
	snap *snapshot,
	span *query.Span,
) (*streamingQueryResources, bool) {
	var prepareSpan *query.Span
	if tracer := query.GetTracer(ctx); tracer != nil {
		prepareSpan, ctx = tracer.StartSpan(ctx, "sidx.prepare-streaming-resources")
		defer prepareSpan.Stop()
	}

	minKey, maxKey := extractKeyRange(req)
	asc := extractOrdering(req)

	parts := selectPartsForQuery(snap, minKey, maxKey)
	if span != nil {
		span.Tagf("min_key", "%d", minKey)
		span.Tagf("max_key", "%d", maxKey)
		span.Tagf("ascending", "%t", asc)
		span.Tagf("part_count", "%d", len(parts))
	}
	if prepareSpan != nil {
		prepareSpan.Tagf("min_key", "%d", minKey)
		prepareSpan.Tagf("max_key", "%d", maxKey)
		prepareSpan.Tagf("ascending", "%t", asc)
		prepareSpan.Tagf("part_count", "%d", len(parts))
		// Sample the first few part paths
		sampleSize := 5
		if len(parts) < sampleSize {
			sampleSize = len(parts)
		}
		for i := 0; i < sampleSize; i++ {
			prepareSpan.Tag("part_path_"+string(rune('0'+i)), parts[i].path)
		}
	}
	if len(parts) == 0 {
		return nil, false
	}

	seriesIDs := make([]common.SeriesID, len(req.SeriesIDs))
	copy(seriesIDs, req.SeriesIDs)
	sort.Slice(seriesIDs, func(i, j int) bool {
		return seriesIDs[i] < seriesIDs[j]
	})
	if span != nil {
		span.Tagf("series_id_count", "%d", len(seriesIDs))
	}
	if prepareSpan != nil {
		prepareSpan.Tagf("series_id_count", "%d", len(seriesIDs))
	}

	tagsToLoad := determineTagsToLoad(req)
	if span != nil {
		span.Tagf("projected_tags", "%d", len(tagsToLoad))
	}
	if prepareSpan != nil {
		prepareSpan.Tagf("projected_tags", "%d", len(tagsToLoad))
	}

	bs := &blockScanner{
		pm:        s.pm,
		filter:    req.Filter,
		l:         s.l,
		parts:     parts,
		seriesIDs: seriesIDs,
		minKey:    minKey,
		maxKey:    maxKey,
		asc:       asc,
	}

	blockCh := make(chan *blockScanResultBatch, 1)
	go func() {
		bs.scan(ctx, blockCh)
		close(blockCh)
	}()

	blockHeap := generateBlockCursorHeap(asc)

	cleanup := func() {
		bs.close()
		releaseBlockCursorHeap(blockHeap)
	}

	return &streamingQueryResources{
		tagsToLoad: tagsToLoad,
		asc:        asc,
		blockCh:    blockCh,
		heap:       blockHeap,
		cleanup:    cleanup,
	}, true
}

func (s *sidx) processStreamingLoop(
	ctx context.Context,
	req QueryRequest,
	resources *streamingQueryResources,
	resultsCh chan<- *QueryResponse,
	stats *streamingStats,
) error {
	var loopSpan *query.Span
	var metrics *batchMetrics
	if tracer := query.GetTracer(ctx); tracer != nil {
		metrics = &batchMetrics{}
		loopSpan, ctx = tracer.StartSpan(ctx, "sidx.process-streaming-loop")
		defer func() {
			if loopSpan != nil {
				loopSpan.Tagf("total_responses_emitted", "%d", stats.batches)
				s.addBatchMetricsToSpan(loopSpan, metrics)
				loopSpan.Stop()
			}
		}()
	}

	scannerBatchCount := 0
	for {
		select {
		case <-ctx.Done():
			if loopSpan != nil {
				loopSpan.Tag("termination_reason", "context_canceled")
				loopSpan.Tagf("scanner_batches_before_cancel", "%d", scannerBatchCount)
			}
			return ctx.Err()
		case batch, ok := <-resources.blockCh:
			if !ok {
				if loopSpan != nil {
					loopSpan.Tag("termination_reason", "channel_closed")
					loopSpan.Tagf("total_scanner_batches", "%d", scannerBatchCount)
				}
				return resources.heap.merge(ctx, req.MaxBatchSize, resultsCh, stats)
			}
			scannerBatchCount++
			if err := s.handleStreamingBatch(ctx, batch, resources, req, resultsCh, stats, metrics); err != nil {
				if loopSpan != nil {
					loopSpan.Tag("termination_reason", "batch_error")
					loopSpan.Tagf("scanner_batches_before_error", "%d", scannerBatchCount)
					loopSpan.Error(err)
				}
				return err
			}
		}
	}
}

func (s *sidx) handleStreamingBatch(
	ctx context.Context,
	batch *blockScanResultBatch,
	resources *streamingQueryResources,
	req QueryRequest,
	resultsCh chan<- *QueryResponse,
	stats *streamingStats,
	metrics *batchMetrics,
) error {
	if batch.err != nil {
		err := batch.err
		releaseBlockScanResultBatch(batch)
		return err
	}

	// Collect batch block count metric
	if metrics != nil {
		batchBlockCount := len(batch.bss)
		metrics.totalBatchBlockCount += batchBlockCount
	}

	cursors, cursorsErr := s.buildCursorsForBatch(ctx, batch, resources.tagsToLoad, req, resources.asc)
	releaseBlockScanResultBatch(batch)
	if cursorsErr != nil {
		return cursorsErr
	}

	// Collect cursors built metric
	if metrics != nil {
		cursorsBuilt := len(cursors)
		metrics.totalCursorsBuilt += cursorsBuilt
	}

	resources.heap.pushCursors(cursors)

	// Collect heap size metric
	if metrics != nil {
		heapSize := resources.heap.Len()
		metrics.totalHeapSizeAfterPush += heapSize
		metrics.batchesProcessed++
	}

	return resources.heap.merge(ctx, req.MaxBatchSize, resultsCh, stats)
}

func (s *sidx) addBatchMetricsToSpan(span *query.Span, metrics *batchMetrics) {
	if span == nil || metrics == nil {
		return
	}
	span.Tagf("batches_processed", "%d", metrics.batchesProcessed)
	span.Tagf("total_batch_block_count", "%d", metrics.totalBatchBlockCount)
	span.Tagf("total_cursors_built", "%d", metrics.totalCursorsBuilt)
	span.Tagf("total_heap_size_after_push", "%d", metrics.totalHeapSizeAfterPush)
	if metrics.batchesProcessed > 0 {
		span.Tagf("avg_batch_block_count", "%.2f", float64(metrics.totalBatchBlockCount)/float64(metrics.batchesProcessed))
		span.Tagf("avg_cursors_built", "%.2f", float64(metrics.totalCursorsBuilt)/float64(metrics.batchesProcessed))
		span.Tagf("avg_heap_size_after_push", "%.2f", float64(metrics.totalHeapSizeAfterPush)/float64(metrics.batchesProcessed))
	}
}

func determineTagsToLoad(req QueryRequest) map[string]struct{} {
	tagsToLoad := make(map[string]struct{})
	if len(req.TagProjection) == 0 {
		return tagsToLoad
	}

	for _, proj := range req.TagProjection {
		for _, tagName := range proj.Names {
			tagsToLoad[tagName] = struct{}{}
		}
	}

	return tagsToLoad
}

func (s *sidx) buildCursorsForBatch(
	ctx context.Context,
	batch *blockScanResultBatch,
	tagsToLoad map[string]struct{},
	req QueryRequest,
	asc bool,
) ([]*blockCursor, error) {
	if len(batch.bss) == 0 {
		return nil, nil
	}

	workerCount := cgroups.CPUs()
	if workerCount <= 0 {
		workerCount = 1
	}
	if workerCount > len(batch.bss) {
		workerCount = len(batch.bss)
	}

	jobCh := make(chan blockScanResult, workerCount)
	resultCh := make(chan *blockCursor, len(batch.bss))

	var workerWg sync.WaitGroup
	workerWg.Add(workerCount)

	for i := 0; i < workerCount; i++ {
		go func() {
			defer workerWg.Done()

			tmpBlock := generateBlock()
			defer releaseBlock(tmpBlock)

			for {
				select {
				case <-ctx.Done():
					return
				case bsResult, ok := <-jobCh:
					if !ok {
						return
					}

					bc := generateBlockCursor()
					bc.init(bsResult.p, &bsResult.bm, req)

					if s.loadBlockCursor(bc, tmpBlock, bsResult, tagsToLoad, req, s.pm) {
						if asc {
							bc.idx = 0
						} else {
							bc.idx = len(bc.userKeys) - 1
						}

						select {
						case resultCh <- bc:
						case <-ctx.Done():
							releaseBlockCursor(bc)
							return
						}
					} else {
						releaseBlockCursor(bc)
					}
				}
			}
		}()
	}

	for i := range batch.bss {
		select {
		case <-ctx.Done():
			close(jobCh)
			workerWg.Wait()
			close(resultCh)
			for bc := range resultCh {
				releaseBlockCursor(bc)
			}
			return nil, ctx.Err()
		case jobCh <- batch.bss[i]:
		}
	}
	close(jobCh)

	workerWg.Wait()
	close(resultCh)

	cursors := make([]*blockCursor, 0, len(batch.bss))
	for bc := range resultCh {
		cursors = append(cursors, bc)
	}

	if err := ctx.Err(); err != nil {
		for i := range cursors {
			releaseBlockCursor(cursors[i])
		}
		return nil, err
	}

	return cursors, nil
}

// extractKeyRange extracts min/max key range from QueryRequest.
func extractKeyRange(req QueryRequest) (int64, int64) {
	minKey := int64(math.MinInt64)
	maxKey := int64(math.MaxInt64)

	if req.MinKey != nil {
		minKey = *req.MinKey
	}
	if req.MaxKey != nil {
		maxKey = *req.MaxKey
	}

	return minKey, maxKey
}

// extractOrdering extracts ordering direction from QueryRequest.
func extractOrdering(req QueryRequest) bool {
	if req.Order == nil {
		return true // Default ascending
	}
	return req.Order.Sort != modelv1.Sort_SORT_DESC
}

// selectPartsForQuery selects relevant parts from snapshot based on key range.
func selectPartsForQuery(snap *snapshot, minKey, maxKey int64) []*part {
	var selectedParts []*part

	for _, pw := range snap.parts {
		if pw.overlapsKeyRange(minKey, maxKey) {
			selectedParts = append(selectedParts, pw.p)
		}
	}

	return selectedParts
}
