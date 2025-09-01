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
	"sync/atomic"

	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/cgroups"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/pool"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/watcher"
)

// sidx implements the SIDX interface with introduction channels for async operations.
type sidx struct {
	snapshot                   *snapshot
	introductions              chan *introduction
	flushCh                    chan *flusherIntroduction
	mergeCh                    chan *mergerIntroduction
	loopCloser                 *run.Closer
	gc                         *gc
	l                          *logger.Logger
	pm                         protector.Memory
	totalIntroduceLoopStarted  atomic.Int64
	totalIntroduceLoopFinished atomic.Int64
	totalQueries               atomic.Int64
	totalWrites                atomic.Int64
	mu                         sync.RWMutex
}

// NewSIDX creates a new SIDX instance with introduction channels.
func NewSIDX(opts *Options) (SIDX, error) {
	if opts == nil {
		opts = NewDefaultOptions()
	}

	if err := opts.Validate(); err != nil {
		return nil, err
	}

	s := &sidx{
		introductions: make(chan *introduction),
		flushCh:       make(chan *flusherIntroduction),
		mergeCh:       make(chan *mergerIntroduction),
		loopCloser:    run.NewCloser(1),
		l:             logger.GetLogger().Named("sidx"),
		pm:            opts.Memory,
	}

	// Initialize garbage collector
	s.gc = newGC(opts)

	// Start introducer loop
	watcherCh := make(watcher.Channel, 10)
	go s.introducerLoop(s.flushCh, s.mergeCh, watcherCh, 0)

	return s, nil
}

// Write implements SIDX interface.
func (s *sidx) Write(ctx context.Context, reqs []WriteRequest) error {
	// Validate requests
	for _, req := range reqs {
		if err := req.Validate(); err != nil {
			return err
		}
	}

	// Increment write counter
	s.totalWrites.Add(1)

	// Create elements from requests
	es := generateElements()
	defer releaseElements(es)

	for _, req := range reqs {
		es.mustAppend(req.SeriesID, req.Key, req.Data, req.Tags)
	}

	// Create memory part from elements
	mp := generateMemPart()
	mp.mustInitFromElements(es)

	// Create introduction
	intro := generateIntroduction()
	intro.memPart = mp
	intro.applied = make(chan struct{})

	// Send to introducer loop
	select {
	case s.introductions <- intro:
		// Wait for introduction to be applied
		<-intro.applied
		releaseIntroduction(intro)
		return nil
	case <-ctx.Done():
		releaseIntroduction(intro)
		releaseMemPart(mp)
		return ctx.Err()
	}
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
		if pw.isActive() && pw.overlapsKeyRange(minKey, maxKey) {
			selectedParts = append(selectedParts, pw.p)
		}
	}

	return selectedParts
}

// Query implements SIDX interface.
func (s *sidx) Query(ctx context.Context, req QueryRequest) (*QueryResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

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
		result := blockHeap.merge(req.MaxElementSize)
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

// Stats implements SIDX interface.
func (s *sidx) Stats(_ context.Context) (*Stats, error) {
	snap := s.currentSnapshot()
	if snap == nil {
		return &Stats{}, nil
	}
	defer snap.decRef()

	stats := &Stats{
		PartCount: int64(snap.getPartCount()),
	}

	// Load atomic counters
	stats.QueryCount.Store(s.totalQueries.Load())
	stats.WriteCount.Store(s.totalWrites.Load())

	return stats, nil
}

// Flush implements Flusher interface.
func (s *sidx) Flush() error {
	// Get current memory parts that need flushing
	snap := s.currentSnapshot()
	if snap == nil {
		return nil
	}
	defer snap.decRef()

	// Create flush introduction
	flushIntro := generateFlusherIntroduction()
	flushIntro.applied = make(chan struct{})

	// Select memory parts to flush (simplified logic)
	for _, pw := range snap.parts {
		if pw.isMemPart() && pw.isActive() {
			flushIntro.flushed[pw.ID()] = pw.p
		}
	}

	if len(flushIntro.flushed) == 0 {
		releaseFlusherIntroduction(flushIntro)
		return nil
	}

	// Send to introducer loop
	s.flushCh <- flushIntro

	// Wait for flush to complete
	<-flushIntro.applied
	releaseFlusherIntroduction(flushIntro)

	return nil
}

// Merge implements Merger interface.
func (s *sidx) Merge() error {
	// Get current snapshot
	snap := s.currentSnapshot()
	if snap == nil {
		return nil
	}
	defer snap.decRef()

	// Create merge introduction
	mergeIntro := generateMergerIntroduction()
	mergeIntro.applied = make(chan struct{})

	// Select parts to merge (simplified logic - merge first 2 parts)
	var partsToMerge []*partWrapper
	for _, pw := range snap.parts {
		if pw.isActive() && !pw.isMemPart() {
			partsToMerge = append(partsToMerge, pw)
			if len(partsToMerge) >= 2 {
				break
			}
		}
	}

	if len(partsToMerge) < 2 {
		releaseMergerIntroduction(mergeIntro)
		return nil
	}

	// Mark parts for merging
	for _, pw := range partsToMerge {
		mergeIntro.merged[pw.ID()] = struct{}{}
	}

	// Create new merged part (simplified - would actually merge the parts)
	mergeIntro.newPart = partsToMerge[0].p

	// Send to introducer loop
	s.mergeCh <- mergeIntro

	// Wait for merge to complete
	<-mergeIntro.applied
	releaseMergerIntroduction(mergeIntro)

	return nil
}

// Close implements SIDX interface.
func (s *sidx) Close() error {
	s.loopCloser.CloseThenWait()

	// Close current snapshot
	s.mu.Lock()
	if s.snapshot != nil {
		s.snapshot.decRef()
		s.snapshot = nil
	}
	s.mu.Unlock()

	return nil
}

// currentSnapshot returns the current snapshot with incremented reference count.
func (s *sidx) currentSnapshot() *snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.snapshot == nil {
		return nil
	}

	if s.snapshot.acquire() {
		return s.snapshot
	}

	return nil
}

// decRef decrements the snapshot reference count (helper for snapshot interface).
func (s *snapshot) decRef() {
	s.release()
}

// Helper methods for metrics.
func (s *sidx) incTotalIntroduceLoopStarted(_ string) {
	s.totalIntroduceLoopStarted.Add(1)
}

func (s *sidx) incTotalIntroduceLoopFinished(_ string) {
	s.totalIntroduceLoopFinished.Add(1)
}

// persistSnapshot persists the snapshot to disk (placeholder).
func (s *sidx) persistSnapshot(_ *snapshot) {
	// TODO: Implement snapshot persistence
}

// Lock/Unlock methods for introducer loop.
func (s *sidx) Lock() {
	s.mu.Lock()
}

func (s *sidx) Unlock() {
	s.mu.Unlock()
}

// gc represents garbage collector (placeholder).
type gc struct{}

func newGC(_ *Options) *gc {
	return &gc{}
}

func (g *gc) clean() {
	// TODO: Implement garbage collection
}

// blockCursor represents a cursor for iterating through a loaded block, similar to query_by_ts.go.
type blockCursor struct {
	p        *part
	bm       *blockMetadata
	tags     map[string][]Tag
	userKeys []int64
	data     [][]byte
	request  QueryRequest
	seriesID common.SeriesID
	idx      int
}

// init initializes the block cursor.
func (bc *blockCursor) init(p *part, bm *blockMetadata, req QueryRequest) {
	bc.p = p
	bc.bm = bm
	bc.request = req
	bc.seriesID = bm.seriesID
	bc.idx = 0
}

// loadBlockCursor loads block data into the cursor, similar to loadBlockCursor in query_by_ts.go.
func (s *sidx) loadBlockCursor(bc *blockCursor, tmpBlock *block, bs blockScanResult, tagsToLoad map[string]struct{}, req QueryRequest, pm protector.Memory) bool {
	tmpBlock.reset()

	// Create a temporary queryResult to reuse existing logic
	qr := &queryResult{
		request:    req,
		tagsToLoad: tagsToLoad,
		pm:         pm,
		l:          s.l,
	}

	// Load the block data
	if !qr.loadBlockData(tmpBlock, bs.p, &bs.bm) {
		return false
	}

	// Copy data to block cursor
	bc.userKeys = make([]int64, len(tmpBlock.userKeys))
	copy(bc.userKeys, tmpBlock.userKeys)

	bc.data = make([][]byte, len(tmpBlock.data))
	for i, data := range tmpBlock.data {
		bc.data[i] = make([]byte, len(data))
		copy(bc.data[i], data)
	}

	// Copy tags
	bc.tags = make(map[string][]Tag)
	for tagName, tagData := range tmpBlock.tags {
		tagSlice := make([]Tag, len(tagData.values))
		for i, value := range tagData.values {
			tagSlice[i] = Tag{
				Name:      tagName,
				Value:     value,
				ValueType: tagData.valueType,
			}
		}
		bc.tags[tagName] = tagSlice
	}

	return len(bc.userKeys) > 0
}

// copyTo copies the current element to a QueryResponse, similar to query_by_ts.go.
func (bc *blockCursor) copyTo(result *QueryResponse) bool {
	if bc.idx < 0 || bc.idx >= len(bc.userKeys) {
		return false
	}

	// Apply key range filtering
	key := bc.userKeys[bc.idx]
	if bc.request.MinKey != nil && key < *bc.request.MinKey {
		return false
	}
	if bc.request.MaxKey != nil && key > *bc.request.MaxKey {
		return false
	}

	result.Keys = append(result.Keys, key)
	result.Data = append(result.Data, bc.data[bc.idx])
	result.SIDs = append(result.SIDs, bc.seriesID)

	// Copy tags for this element
	var elementTags []Tag
	for _, tagSlice := range bc.tags {
		if bc.idx < len(tagSlice) {
			elementTags = append(elementTags, tagSlice[bc.idx])
		}
	}
	result.Tags = append(result.Tags, elementTags)
	return true
}

// blockCursorHeap implements heap.Interface for sorting block cursors.
type blockCursorHeap struct {
	bcc []*blockCursor
	asc bool
}

func (bch blockCursorHeap) Len() int {
	return len(bch.bcc)
}

func (bch blockCursorHeap) Less(i, j int) bool {
	leftIdx, rightIdx := bch.bcc[i].idx, bch.bcc[j].idx
	if leftIdx >= len(bch.bcc[i].userKeys) || rightIdx >= len(bch.bcc[j].userKeys) {
		return false // Handle bounds check
	}
	leftTS := bch.bcc[i].userKeys[leftIdx]
	rightTS := bch.bcc[j].userKeys[rightIdx]
	if bch.asc {
		return leftTS < rightTS
	}
	return leftTS > rightTS
}

func (bch *blockCursorHeap) Swap(i, j int) {
	bch.bcc[i], bch.bcc[j] = bch.bcc[j], bch.bcc[i]
}

func (bch *blockCursorHeap) Push(x interface{}) {
	bch.bcc = append(bch.bcc, x.(*blockCursor))
}

func (bch *blockCursorHeap) Pop() interface{} {
	old := bch.bcc
	n := len(old)
	x := old[n-1]
	bch.bcc = old[0 : n-1]
	releaseBlockCursor(x)
	return x
}

func (bch *blockCursorHeap) reset() {
	for i := range bch.bcc {
		releaseBlockCursor(bch.bcc[i])
	}
	bch.bcc = bch.bcc[:0]
}

// merge performs heap-based merge similar to query_by_ts.go.
func (bch *blockCursorHeap) merge(limit int) *QueryResponse {
	step := -1
	if bch.asc {
		step = 1
	}
	result := &QueryResponse{
		Keys: make([]int64, 0),
		Data: make([][]byte, 0),
		Tags: make([][]Tag, 0),
		SIDs: make([]common.SeriesID, 0),
	}

	for bch.Len() > 0 {
		topBC := bch.bcc[0]
		if topBC.idx < 0 || topBC.idx >= len(topBC.userKeys) {
			heap.Pop(bch)
			continue
		}

		// Try to copy the element (returns false if filtered out by key range)
		if topBC.copyTo(result) {
			if limit > 0 && result.Len() >= limit {
				break
			}
		}
		topBC.idx += step

		if bch.asc {
			if topBC.idx >= len(topBC.userKeys) {
				heap.Pop(bch)
			} else {
				heap.Fix(bch, 0)
			}
		} else {
			if topBC.idx < 0 {
				heap.Pop(bch)
			} else {
				heap.Fix(bch, 0)
			}
		}
	}

	return result
}

var blockCursorHeapPool = pool.Register[*blockCursorHeap]("sidx-blockCursorHeap")

func generateBlockCursorHeap(asc bool) *blockCursorHeap {
	v := blockCursorHeapPool.Get()
	if v == nil {
		return &blockCursorHeap{
			asc: asc,
			bcc: make([]*blockCursor, 0, blockScannerBatchSize),
		}
	}
	v.asc = asc
	return v
}

func releaseBlockCursorHeap(bch *blockCursorHeap) {
	bch.reset()
	blockCursorHeapPool.Put(bch)
}

var blockCursorPool = pool.Register[*blockCursor]("sidx-blockCursor")

func generateBlockCursor() *blockCursor {
	v := blockCursorPool.Get()
	if v == nil {
		return &blockCursor{}
	}
	return v
}

func releaseBlockCursor(bc *blockCursor) {
	bc.p = nil
	bc.bm = nil
	bc.userKeys = bc.userKeys[:0]
	bc.data = bc.data[:0]
	bc.tags = nil
	bc.seriesID = 0
	bc.idx = 0
	blockCursorPool.Put(bc)
}
