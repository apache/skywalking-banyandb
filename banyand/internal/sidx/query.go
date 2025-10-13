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
	"container/heap"
	"context"
	"math"
	"sort"
	"sync"

	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/cgroups"
)

// Query implements SIDX interface.
func (s *sidx) Query(ctx context.Context, req QueryRequest) (*QueryResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	// The blocking query path returns the full result set. Treat MaxElementSize as
	// a streaming-only hint so it does not truncate the blocking response.
	req.MaxElementSize = 0

	// StreamingQuery shares the validation logic but not execution flow.
	// We retain the existing blocking semantics here.

	// Increment query counter
	s.totalQueries.Add(1)

	// Get current snapshot
	snap := s.currentSnapshot()
	if snap == nil {
		return &QueryResponse{
			Keys: make([]int64, 0),
			Data: make([][]byte, 0),
			Tags: make([][]Tag, 0),
			SIDs: make([]common.SeriesID, 0),
		}, nil
	}

	// Extract parameters directly from request
	minKey, maxKey := extractKeyRange(req)
	asc := extractOrdering(req)

	// Select relevant parts
	parts := selectPartsForQuery(snap, minKey, maxKey)
	if len(parts) == 0 {
		snap.decRef()
		return &QueryResponse{
			Keys: make([]int64, 0),
			Data: make([][]byte, 0),
			Tags: make([][]Tag, 0),
			SIDs: make([]common.SeriesID, 0),
		}, nil
	}

	// Sort SeriesIDs for efficient processing
	seriesIDs := make([]common.SeriesID, len(req.SeriesIDs))
	copy(seriesIDs, req.SeriesIDs)
	sort.Slice(seriesIDs, func(i, j int) bool {
		return seriesIDs[i] < seriesIDs[j]
	})

	// Initialize block scanner using request parameters directly
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

	// Execute query with worker pool pattern directly
	defer func() {
		if bs != nil {
			bs.close()
		}
		if snap != nil {
			snap.decRef()
		}
	}()

	response := s.executeBlockScannerQuery(ctx, bs, req)
	if response == nil {
		return &QueryResponse{
			Keys: make([]int64, 0),
			Data: make([][]byte, 0),
			Tags: make([][]Tag, 0),
			SIDs: make([]common.SeriesID, 0),
		}, nil
	}

	// If there's an error in the response, return it as a function error
	if response.Error != nil {
		return nil, response.Error
	}

	return response, nil
}

// StreamingQuery implements the streaming query API defined on SIDX.
func (s *sidx) StreamingQuery(ctx context.Context, req QueryRequest) (<-chan *QueryResponse, <-chan error) {
	chanSize := req.MaxElementSize
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

// runStreamingQuery executes the streaming query workflow and pushes batches to resultsCh.
func (s *sidx) runStreamingQuery(ctx context.Context, req QueryRequest, resultsCh chan<- *QueryResponse) error {
	// Increment query counter for observability parity with Query.
	s.totalQueries.Add(1)

	snap := s.currentSnapshot()
	if snap == nil {
		return nil
	}
	defer snap.decRef()

	minKey, maxKey := extractKeyRange(req)
	asc := extractOrdering(req)

	parts := selectPartsForQuery(snap, minKey, maxKey)
	if len(parts) == 0 {
		return nil
	}

	seriesIDs := make([]common.SeriesID, len(req.SeriesIDs))
	copy(seriesIDs, req.SeriesIDs)
	sort.Slice(seriesIDs, func(i, j int) bool {
		return seriesIDs[i] < seriesIDs[j]
	})

	tagsToLoad := determineTagsToLoad(req)

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
	defer bs.close()

	blockCh := make(chan *blockScanResultBatch, 1)
	go func() {
		bs.scan(ctx, blockCh)
		close(blockCh)
	}()

	blockHeap := generateBlockCursorHeap(asc)
	defer releaseBlockCursorHeap(blockHeap)

	heapInitialized := false

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case batch, ok := <-blockCh:
			if !ok {
				if !heapInitialized || blockHeap.Len() == 0 {
					return nil
				}
				for heapInitialized && blockHeap.Len() > 0 {
					done, err := flushStreamingHeap(ctx, blockHeap, req.MaxElementSize, resultsCh)
					if err != nil {
						return err
					}
					if blockHeap.Len() == 0 {
						break
					}
					if !done {
						break
					}
				}
				return nil
			}

			if batch.err != nil {
				err := batch.err
				releaseBlockScanResultBatch(batch)
				return err
			}

			cursors, err := s.buildCursorsForBatch(ctx, batch, tagsToLoad, req, asc)
			if err != nil {
				releaseBlockScanResultBatch(batch)
				return err
			}

			releaseBlockScanResultBatch(batch)

			for i := range cursors {
				if !heapInitialized {
					blockHeap.Push(cursors[i])
				} else {
					heap.Push(blockHeap, cursors[i])
				}
			}

			if !heapInitialized && blockHeap.Len() > 0 {
				heap.Init(blockHeap)
				heapInitialized = true
			}

			for heapInitialized && blockHeap.Len() > 0 {
				done, err := flushStreamingHeap(ctx, blockHeap, req.MaxElementSize, resultsCh)
				if err != nil {
					return err
				}
				if blockHeap.Len() == 0 {
					break
				}
				if !done {
					break
				}
			}
		}
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

// flushStreamingHeap drains block cursors into QueryResponse batches and pushes them to resultsCh.
// It returns true when the current batch hit the MaxElementSize constraint so the caller can resume
// flushing in a subsequent iteration.
func flushStreamingHeap(
	ctx context.Context, blockHeap *blockCursorHeap, maxElementSize int, resultsCh chan<- *QueryResponse,
) (bool, error) {
	limitReached := false

	for blockHeap.Len() > 0 {
		chunk, hitLimit := blockHeap.merge(maxElementSize)
		if chunk == nil || chunk.Len() == 0 {
			if hitLimit {
				return true, nil
			}
			// Safeguard against tight loops if no data was emitted.
			if blockHeap.Len() == 0 {
				return limitReached, nil
			}
			continue
		}

		select {
		case <-ctx.Done():
			return limitReached, ctx.Err()
		case resultsCh <- chunk:
		}

		if hitLimit {
			limitReached = true
			break
		}

		// If no limit is provided, merge drains the entire heap in one iteration.
		if maxElementSize <= 0 {
			break
		}
	}

	return limitReached, nil
}

// executeBlockScannerQuery coordinates the worker pool with block scanner following tsResult pattern.
func (s *sidx) executeBlockScannerQuery(ctx context.Context, bs *blockScanner, req QueryRequest) *QueryResponse {
	workerSize := cgroups.CPUs()
	batchCh := make(chan *blockScanResultBatch, workerSize)

	// Determine which tags to load once for all workers (shared optimization)
	tagsToLoad := make(map[string]struct{})
	if len(req.TagProjection) > 0 {
		// Load only projected tags
		for _, proj := range req.TagProjection {
			for _, tagName := range proj.Names {
				tagsToLoad[tagName] = struct{}{}
			}
		}
	}

	// Initialize worker result shards
	shards := make([]*QueryResponse, workerSize)
	for i := range shards {
		shards[i] = &QueryResponse{
			Keys: make([]int64, 0),
			Data: make([][]byte, 0),
			Tags: make([][]Tag, 0),
			SIDs: make([]common.SeriesID, 0),
		}
	}

	// Launch worker pool
	var workerWg sync.WaitGroup
	workerWg.Add(workerSize)

	for i := range workerSize {
		go func(workerID int) {
			defer workerWg.Done()
			s.processWorkerBatches(workerID, batchCh, shards[workerID], tagsToLoad, req, s.pm)
		}(i)
	}

	// Start block scanning
	go func() {
		bs.scan(ctx, batchCh)
		close(batchCh)
	}()

	workerWg.Wait()

	// Merge results from all workers
	return s.mergeWorkerResults(shards, req.MaxElementSize)
}

// processWorkerBatches processes batches in a worker goroutine using heap-based approach.
func (s *sidx) processWorkerBatches(
	_ int, batchCh chan *blockScanResultBatch, shard *QueryResponse,
	tagsToLoad map[string]struct{}, req QueryRequest, pm protector.Memory,
) {
	tmpBlock := generateBlock()
	defer releaseBlock(tmpBlock)

	asc := extractOrdering(req)
	blockHeap := generateBlockCursorHeap(asc)
	defer releaseBlockCursorHeap(blockHeap)

	for batch := range batchCh {
		if batch.err != nil {
			shard.Error = batch.err
			releaseBlockScanResultBatch(batch)
			continue
		}

		// Load all blocks in this batch and create block cursors
		for _, bs := range batch.bss {
			bc := generateBlockCursor()
			bc.init(bs.p, &bs.bm, req)

			if s.loadBlockCursor(bc, tmpBlock, bs, tagsToLoad, req, pm) {
				// Set starting index based on sort order
				if asc {
					bc.idx = 0
				} else {
					bc.idx = len(bc.userKeys) - 1
				}
				blockHeap.Push(bc)
			} else {
				releaseBlockCursor(bc)
			}
		}

		releaseBlockScanResultBatch(batch)
	}

	// Initialize heap and merge results
	if blockHeap.Len() > 0 {
		heap.Init(blockHeap)
		result, _ := blockHeap.merge(req.MaxElementSize)
		shard.CopyFrom(result)
	}
}

// mergeWorkerResults merges results from all worker shards with error handling.
func (s *sidx) mergeWorkerResults(shards []*QueryResponse, maxElementSize int) *QueryResponse {
	// Check for errors first
	var err error
	for i := range shards {
		if shards[i].Error != nil {
			err = multierr.Append(err, shards[i].Error)
		}
	}

	if err != nil {
		return &QueryResponse{Error: err}
	}

	// Merge results - shards are already in the requested order
	// Just use ascending merge since shards are pre-sorted
	return mergeQueryResponseShards(shards, maxElementSize)
}
