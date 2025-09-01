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
	"encoding/json"
	"fmt"
	"math"
	"path/filepath"
	"slices"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/watcher"
)

const (
	maxUncompressedBlockSize = 2 * 1024 * 1024
	maxBlockLength           = 8 * 1024
)

// sidx implements the SIDX interface with introduction channels for async operations.
type sidx struct {
	fileSystem                 fs.FileSystem
	snapshot                   *snapshot
	introductions              chan *introduction
	flushCh                    chan *flusherIntroduction
	mergeCh                    chan *mergerIntroduction
	loopCloser                 *run.Closer
	l                          *logger.Logger
	pm                         protector.Memory
	root                       string
	gc                         garbageCleaner
	totalIntroduceLoopStarted  atomic.Int64
	totalIntroduceLoopFinished atomic.Int64
	totalQueries               atomic.Int64
	totalWrites                atomic.Int64
	mu                         sync.RWMutex
}

// NewSIDX creates a new SIDX instance with introduction channels.
func NewSIDX(fileSystem fs.FileSystem, opts *Options) (SIDX, error) {
	if opts == nil {
		opts = NewDefaultOptions()
	}

	if err := opts.Validate(); err != nil {
		return nil, err
	}

	s := &sidx{
		fileSystem:    fileSystem,
		introductions: make(chan *introduction),
		flushCh:       make(chan *flusherIntroduction),
		mergeCh:       make(chan *mergerIntroduction),
		loopCloser:    run.NewCloser(1),
		l:             logger.GetLogger().Named("sidx"),
		pm:            opts.Memory,
		root:          opts.Path,
	}

	// Initialize garbage collector
	s.gc.init(s)

	// Start introducer loop
	watcherCh := make(watcher.Channel, 10)
	go s.introducerLoop(s.flushCh, s.mergeCh, watcherCh, 0)

	return s, nil
}

// Write implements SIDX interface.
func (s *sidx) Write(ctx context.Context, reqs []WriteRequest, partID uint64) error {
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
	intro.memPart.partMetadata.ID = partID
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

	// Create queryResult and call Pull() once to get the QueryResponse
	qr := &queryResult{
		snapshot: snap,
		request:  req,
		ctx:      ctx,
		bs:       bs,
		asc:      asc,
		pm:       s.pm,
		l:        s.l,
	}
	defer qr.Release()

	// Pull once to get all results
	response := qr.Pull()
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

	// Select memory parts to flush
	for _, pw := range snap.parts {
		if !pw.isMemPart() || !pw.isActive() {
			continue
		}
		partPath := partPath(s.root, pw.ID())
		pw.mp.mustFlush(s.fileSystem, partPath)
		p := mustOpenPart(partPath, s.fileSystem)
		flushIntro.flushed[p.partMetadata.ID] = p
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
func (s *sidx) Merge(partIDs []uint64, newPartID uint64, closeCh <-chan struct{}) error {
	// Get current snapshot
	snap := s.currentSnapshot()
	if snap == nil {
		return nil
	}
	defer snap.decRef()

	// Create merge introduction
	mergeIntro := generateMergerIntroduction()
	defer releaseMergerIntroduction(mergeIntro)
	mergeIntro.applied = make(chan struct{})

	// Select parts to merge
	var partsToMerge []*partWrapper
	for _, pw := range snap.parts {
		if pw.isActive() && !pw.isMemPart() && slices.Contains(partIDs, pw.ID()) {
			partsToMerge = append(partsToMerge, pw)
		}
	}

	if len(partsToMerge) < 2 {
		return nil
	}

	// Mark parts for merging
	for _, pw := range partsToMerge {
		mergeIntro.merged[pw.ID()] = struct{}{}
	}

	// Create new merged part
	newPart, err := s.mergeParts(s.fileSystem, closeCh, partsToMerge, newPartID, s.root)
	if err != nil {
		return err
	}
	mergeIntro.newPart = newPart.p

	// Send to introducer loop
	s.mergeCh <- mergeIntro

	// Wait for merge to complete
	<-mergeIntro.applied

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

var errNoPartToMerge = fmt.Errorf("no part to merge")

func (s *sidx) mergeParts(fileSystem fs.FileSystem, closeCh <-chan struct{}, parts []*partWrapper, partID uint64, root string) (*partWrapper, error) {
	if len(parts) == 0 {
		return nil, errNoPartToMerge
	}
	dstPath := partPath(root, partID)
	var totalSize int64
	pii := make([]*partMergeIter, 0, len(parts))
	for i := range parts {
		pmi := generatePartMergeIter()
		pmi.mustInitFromPart(parts[i].p)
		pii = append(pii, pmi)
		totalSize += int64(parts[i].p.partMetadata.CompressedSizeBytes)
	}
	shouldCache := s.pm.ShouldCache(totalSize)
	br := generateBlockReader()
	br.init(pii)
	bw := generateBlockWriter()
	bw.mustInitForFilePart(fileSystem, dstPath, shouldCache)

	pm, err := mergeBlocks(closeCh, bw, br)
	releaseBlockWriter(bw)
	releaseBlockReader(br)
	for i := range pii {
		releasePartMergeIter(pii[i])
	}
	if err != nil {
		return nil, err
	}
	pm.mustWriteMetadata(fileSystem, dstPath)
	fileSystem.SyncPath(dstPath)
	p := mustOpenPart(dstPath, fileSystem)
	return newPartWrapper(nil, p), nil
}

var errClosed = fmt.Errorf("the merger is closed")

func mergeBlocks(closeCh <-chan struct{}, bw *blockWriter, br *blockReader) (*partMetadata, error) {
	pendingBlockIsEmpty := true
	pendingBlock := generateBlockPointer()
	defer releaseBlockPointer(pendingBlock)
	var tmpBlock, tmpBlock2 *blockPointer
	var decoder *encoding.BytesBlockDecoder
	getDecoder := func() *encoding.BytesBlockDecoder {
		if decoder == nil {
			decoder = generateColumnValuesDecoder()
		}
		return decoder
	}
	releaseDecoder := func() {
		if decoder != nil {
			releaseColumnValuesDecoder(decoder)
			decoder = nil
		}
	}
	for br.nextBlockMetadata() {
		select {
		case <-closeCh:
			return nil, errClosed
		default:
		}
		b := br.block

		if pendingBlockIsEmpty {
			br.loadBlockData(getDecoder())
			pendingBlock.copyFrom(b)
			pendingBlockIsEmpty = false
			continue
		}

		if pendingBlock.bm.seriesID != b.bm.seriesID ||
			(pendingBlock.isFull() && pendingBlock.bm.maxKey <= b.bm.minKey) {
			bw.mustWriteBlock(pendingBlock.bm.seriesID, &pendingBlock.block)
			releaseDecoder()
			br.loadBlockData(getDecoder())
			pendingBlock.copyFrom(b)
			continue
		}

		if tmpBlock == nil {
			tmpBlock = generateBlockPointer()
			defer releaseBlockPointer(tmpBlock)
		}
		tmpBlock.reset()
		tmpBlock.bm.seriesID = b.bm.seriesID
		br.loadBlockData(getDecoder())
		mergeTwoBlocks(tmpBlock, pendingBlock, b)
		if len(tmpBlock.userKeys) <= maxBlockLength && tmpBlock.uncompressedSizeBytes() <= maxUncompressedBlockSize {
			if len(tmpBlock.userKeys) == 0 {
				pendingBlockIsEmpty = true
			}
			pendingBlock, tmpBlock = tmpBlock, pendingBlock
			continue
		}

		if len(tmpBlock.userKeys) <= maxBlockLength {
			bw.mustWriteBlock(tmpBlock.bm.seriesID, &tmpBlock.block)
			pendingBlock.reset()
			pendingBlockIsEmpty = true
			releaseDecoder()
			continue
		}
		tmpBlock.idx = maxBlockLength
		pendingBlock.copyFrom(tmpBlock)
		l := tmpBlock.idx
		tmpBlock.idx = 0
		if tmpBlock2 == nil {
			tmpBlock2 = generateBlockPointer()
			defer releaseBlockPointer(tmpBlock2)
		}
		tmpBlock2.reset()
		tmpBlock2.append(tmpBlock, l)
		bw.mustWriteBlock(tmpBlock.bm.seriesID, &tmpBlock2.block)
		releaseDecoder()
	}
	if err := br.error(); err != nil {
		return nil, fmt.Errorf("cannot read block to merge: %w", err)
	}
	if !pendingBlockIsEmpty {
		bw.mustWriteBlock(pendingBlock.bm.seriesID, &pendingBlock.block)
	}
	releaseDecoder()
	var result partMetadata
	bw.Flush(&result)
	return &result, nil
}

func mergeTwoBlocks(target, left, right *blockPointer) {
	appendIfEmpty := func(ib1, ib2 *blockPointer) bool {
		if ib1.idx >= len(ib1.userKeys) {
			target.appendAll(ib2)
			return true
		}
		return false
	}

	defer target.updateMetadata()

	if left.bm.maxKey < right.bm.minKey {
		target.appendAll(left)
		target.appendAll(right)
		return
	}
	if right.bm.maxKey < left.bm.minKey {
		target.appendAll(right)
		target.appendAll(left)
		return
	}
	if appendIfEmpty(left, right) || appendIfEmpty(right, left) {
		return
	}

	for {
		i := left.idx
		uk2 := right.userKeys[right.idx]
		for i < len(left.userKeys) && left.userKeys[i] <= uk2 {
			i++
		}
		target.append(left, i)
		left.idx = i
		if appendIfEmpty(left, right) {
			return
		}
		left, right = right, left
	}
}

// Helper methods for metrics.
func (s *sidx) incTotalIntroduceLoopStarted(_ string) {
	s.totalIntroduceLoopStarted.Add(1)
}

func (s *sidx) incTotalIntroduceLoopFinished(_ string) {
	s.totalIntroduceLoopFinished.Add(1)
}

// persistSnapshot persists the snapshot to disk.
func (s *sidx) persistSnapshot(snapshot *snapshot) {
	var partNames []string
	for i := range snapshot.parts {
		partNames = append(partNames, partName(snapshot.parts[i].ID()))
	}
	s.mustWriteSnapshot(snapshot.epoch, partNames)
	s.gc.registerSnapshot(snapshot)
}

func (s *sidx) mustWriteSnapshot(snapshot uint64, partNames []string) {
	data, err := json.Marshal(partNames)
	if err != nil {
		logger.Panicf("cannot marshal partNames to JSON: %s", err)
	}
	snapshotPath := filepath.Join(s.root, snapshotName(snapshot))
	lf, err := s.fileSystem.CreateLockFile(snapshotPath, storage.FilePerm)
	if err != nil {
		logger.Panicf("cannot create lock file %s: %s", snapshotPath, err)
	}
	n, err := lf.Write(data)
	if err != nil {
		logger.Panicf("cannot write snapshot %s: %s", snapshotPath, err)
	}
	if n != len(data) {
		logger.Panicf("unexpected number of bytes written to %s; got %d; want %d", snapshotPath, n, len(data))
	}
}

// Lock/Unlock methods for introducer loop.
func (s *sidx) Lock() {
	s.mu.Lock()
}

func (s *sidx) Unlock() {
	s.mu.Unlock()
}

// emptyQueryResult represents an empty query result.
type emptyQueryResult struct{}

func (e *emptyQueryResult) Pull() *QueryResponse {
	return nil
}

func (e *emptyQueryResult) Release() {
	// Nothing to release
}
