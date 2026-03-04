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
	"errors"
	"math"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/cgroups"
	"github.com/apache/skywalking-banyandb/pkg/query"
)

// StreamingQuery implements the streaming query API defined on SIDX.
func (s *sidx) StreamingQuery(ctx context.Context, req QueryRequest) (<-chan *QueryResponse, <-chan error) {
	resultsCh := make(chan *QueryResponse)
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

type batchMetrics struct {
	totalBlocksScanned    int
	totalCursorsCreated   int
	cumulativeHeapSize    int
	inputBatchesProcessed int
	blockElementsLoaded   atomic.Int64
	outputElementsEmitted atomic.Int64
	elementsDeduplicated  atomic.Int64
	blocksSkipped         atomic.Int64
	outputBatchCount      int
	outputElementCount    int
}

func (s *batchMetrics) record(chunk *QueryResponse) {
	if s == nil || chunk == nil {
		return
	}
	s.outputBatchCount++
	s.outputElementCount += chunk.Len()
}

// runStreamingQuery executes the streaming query workflow and pushes batches to resultsCh.
func (s *sidx) runStreamingQuery(ctx context.Context, req QueryRequest, resultsCh chan<- *QueryResponse) (err error) {
	s.totalQueries.Add(1)

	ctx, span := startStreamingSpan(ctx, req)
	defer func() {
		finalizeStreamingSpan(span, &err)
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

	return s.processStreamingLoop(ctx, req, resources, resultsCh)
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

func finalizeStreamingSpan(span *query.Span, errPtr *error) {
	if span == nil {
		return
	}
	if errPtr != nil && *errPtr != nil {
		span.Error(*errPtr)
	}
	span.Stop()
}

func (s *sidx) prepareStreamingResources(
	ctx context.Context,
	req QueryRequest,
	snap *Snapshot,
	span *query.Span,
) (*streamingQueryResources, bool) {
	var prepareSpan *query.Span
	if tracer := query.GetTracer(ctx); tracer != nil {
		prepareSpan, ctx = tracer.StartSpan(ctx, "sidx.prepare-streaming-resources")
		defer prepareSpan.Stop()
	}

	minKey, maxKey := extractKeyRange(req)
	asc := extractOrdering(req)

	parts := selectPartsForQuery(snap, minKey, maxKey, req.MinTimestamp, req.MaxTimestamp)
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
		batchSize: req.MaxBatchSize,
	}

	blockCh := make(chan *blockScanResultBatch)
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
) error {
	var loopSpan *query.Span
	var metrics *batchMetrics
	if tracer := query.GetTracer(ctx); tracer != nil {
		metrics = &batchMetrics{}
		loopSpan, ctx = tracer.StartSpan(ctx, "sidx.process-streaming-loop")
		defer func() {
			if loopSpan != nil {
				s.addBatchMetricsToSpan(loopSpan, metrics)
				loopSpan.Stop()
			}
		}()
	}

	scannerBatchCount := 0
	for batch := range resources.blockCh {
		scannerBatchCount++
		if err := s.handleStreamingBatch(ctx, batch, resources, req, resultsCh, metrics); err != nil {
			if loopSpan != nil {
				if errors.Is(err, context.Canceled) {
					loopSpan.Tag("termination_reason", "context_canceled")
					loopSpan.Tagf("scanner_batches_before_cancel", "%d", scannerBatchCount)
				} else {
					loopSpan.Tag("termination_reason", "batch_error")
					loopSpan.Tagf("scanner_batches_before_error", "%d", scannerBatchCount)
					loopSpan.Error(err)
				}
			}
			return err
		}
	}
	if loopSpan != nil {
		loopSpan.Tag("termination_reason", "channel_closed")
		loopSpan.Tagf("total_scanner_batches", "%d", scannerBatchCount)
	}
	return resources.heap.merge(ctx, req.MaxBatchSize, resultsCh, metrics)
}

func (s *sidx) handleStreamingBatch(
	ctx context.Context,
	batch *blockScanResultBatch,
	resources *streamingQueryResources,
	req QueryRequest,
	resultsCh chan<- *QueryResponse,
	metrics *batchMetrics,
) error {
	defer releaseBlockScanResultBatch(batch)
	if batch.err != nil {
		err := batch.err
		return err
	}

	// Collect batch block count metric
	if metrics != nil {
		batchBlockCount := len(batch.bss)
		metrics.totalBlocksScanned += batchBlockCount
	}

	cursors, cursorsErr := s.buildCursorsForBatch(ctx, batch, resources.tagsToLoad, req, resources.asc, metrics)
	if cursorsErr != nil {
		return cursorsErr
	}

	// Collect cursors built metric
	if metrics != nil {
		cursorsBuilt := len(cursors)
		metrics.totalCursorsCreated += cursorsBuilt
	}

	resources.heap.pushCursors(cursors)

	// Collect heap size metric
	if metrics != nil {
		heapSize := resources.heap.Len()
		metrics.cumulativeHeapSize += heapSize
		metrics.inputBatchesProcessed++
	}

	return resources.heap.merge(ctx, req.MaxBatchSize, resultsCh, metrics)
}

func (s *sidx) addBatchMetricsToSpan(span *query.Span, metrics *batchMetrics) {
	if span == nil || metrics == nil {
		return
	}
	span.Tagf("input_batches_processed", "%d", metrics.inputBatchesProcessed)
	span.Tagf("total_blocks_scanned", "%d", metrics.totalBlocksScanned)
	span.Tagf("total_cursors_created", "%d", metrics.totalCursorsCreated)
	span.Tagf("cumulative_heap_size", "%d", metrics.cumulativeHeapSize)
	if metrics.inputBatchesProcessed > 0 {
		span.Tagf("avg_blocks_scanned_per_batch", "%.2f", float64(metrics.totalBlocksScanned)/float64(metrics.inputBatchesProcessed))
		span.Tagf("avg_cursors_created_per_batch", "%.2f", float64(metrics.totalCursorsCreated)/float64(metrics.inputBatchesProcessed))
		span.Tagf("avg_heap_size", "%.2f", float64(metrics.cumulativeHeapSize)/float64(metrics.inputBatchesProcessed))
	}
	span.Tagf("block_elements_loaded", "%d", metrics.blockElementsLoaded.Load())
	span.Tagf("output_elements_emitted", "%d", metrics.outputElementsEmitted.Load())
	span.Tagf("elements_deduplicated", "%d", metrics.elementsDeduplicated.Load())
	span.Tagf("blocks_skipped", "%d", metrics.blocksSkipped.Load())
	span.Tagf("output_element_count", "%d", metrics.outputElementCount)
	span.Tagf("output_batch_count", "%d", metrics.outputBatchCount)
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
	metrics *batchMetrics,
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

					if s.loadBlockCursor(bc, tmpBlock, bsResult, tagsToLoad, req, s.pm, metrics) {
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

// selectPartsForQuery selects relevant parts from snapshot based on key range and time range.
// If time-based filtering would exclude all key-overlapping parts (due to missing or
// inconsistent timestamp metadata), it falls back to key-only selection to preserve
// correctness and avoid dropping valid data.
func selectPartsForQuery(snap *Snapshot, minKey, maxKey int64, minTimestamp, maxTimestamp *int64) []*part {
	var selectedParts []*part
	keyOverlapCount := 0

	for _, pw := range snap.parts {
		// Check key range overlap
		if !pw.overlapsKeyRange(minKey, maxKey) {
			continue
		}
		keyOverlapCount++
		// Check time range overlap (only filters if both query and part have timestamps)
		if !pw.overlapsTimeRange(minTimestamp, maxTimestamp) {
			continue
		}
		selectedParts = append(selectedParts, pw.p)
	}

	// Safety fallback: if time range filtering excluded all parts that overlapped by key,
	// fall back to key-only filtering. This guarantees we never miss data solely because
	// of timestamp metadata issues and preserves the previous behavior.
	if len(selectedParts) == 0 && keyOverlapCount > 0 {
		for _, pw := range snap.parts {
			if !pw.overlapsKeyRange(minKey, maxKey) {
				continue
			}
			selectedParts = append(selectedParts, pw.p)
		}
	}

	return selectedParts
}
