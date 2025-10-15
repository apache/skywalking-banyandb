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
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/pool"
)

const maxBlockLength = 8 * 1024

// sidx implements the SIDX interface with introduction channels for async operations.
type sidx struct {
	fileSystem   fs.FileSystem
	snapshot     *snapshot
	l            *logger.Logger
	pm           protector.Memory
	root         string
	totalQueries atomic.Int64
	totalWrites  atomic.Int64
	mu           sync.RWMutex
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
		fileSystem: fileSystem,
		l:          logger.GetLogger().Named("sidx"),
		pm:         opts.Memory,
		root:       opts.Path,
	}

	// Initialize sidx
	s.init(opts.AvailablePartIDs)
	return s, nil
}

// ConvertToMemPart converts a write request to a memPart.
func (s *sidx) ConvertToMemPart(reqs []WriteRequest, segmentID int64) (*MemPart, error) {
	// Validate requests
	for _, req := range reqs {
		if err := req.Validate(); err != nil {
			return nil, err
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
	mp := GenerateMemPart()
	mp.mustInitFromElements(es)
	mp.partMetadata.SegmentID = segmentID
	return mp, nil
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
func (s *sidx) Flush(partIDsToFlush map[uint64]struct{}) (*FlusherIntroduction, error) {
	// Get current memory parts that need flushing
	snap := s.currentSnapshot()
	if snap == nil {
		return nil, nil
	}
	defer snap.decRef()

	// Create flush introduction
	flushIntro := generateFlusherIntroduction()

	// Select memory parts to flush
	for _, pw := range snap.parts {
		if _, ok := partIDsToFlush[pw.ID()]; !ok {
			continue
		}
		if !pw.isMemPart() {
			logger.Panicf("sidx part %d is not a memory part", pw.ID())
		}
		partPath := partPath(s.root, pw.ID())
		pw.mp.mustFlush(s.fileSystem, partPath)
		if l := s.l.Debug(); l.Enabled() {
			s.l.Debug().
				Uint64("part_id", pw.ID()).
				Str("part_path", partPath).
				Msg("flushing sidx part")
		}
		newPW := newPartWrapper(nil, mustOpenPart(pw.ID(), partPath, s.fileSystem))
		flushIntro.flushed[newPW.ID()] = newPW
	}

	if len(flushIntro.flushed) != len(partIDsToFlush) {
		logger.Panicf("expected %d parts to flush, but got %d", len(partIDsToFlush), len(flushIntro.flushed))
		return nil, nil
	}
	return flushIntro, nil
}

// Close implements SIDX interface.
func (s *sidx) Close() error {
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
// It returns a QueryResponse along with a flag indicating whether the merge
// stopped because the MaxElementSize limit has been reached.
func (bch *blockCursorHeap) merge(limit int) (*QueryResponse, bool) {
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

	limitHit := false

	for bch.Len() > 0 {
		if limit > 0 && result.Len() >= limit {
			limitHit = true
			break
		}

		topBC := bch.bcc[0]
		if topBC.idx < 0 || topBC.idx >= len(topBC.userKeys) {
			heap.Pop(bch)
			continue
		}

		// Copy the element (may be filtered out by key range)
		topBC.copyTo(result)

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

	return result, limitHit
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

func (s *sidx) init(availablePartIDs []uint64) {
	if _, err := os.Stat(s.root); os.IsNotExist(err) {
		s.l.Debug().Str("path", s.root).Msg("sidx directory does not exist")
		return
	}

	entries := s.fileSystem.ReadDir(s.root)
	if len(entries) == 0 {
		s.l.Debug().Str("path", s.root).Msg("no existing sidx data found")
		return
	}

	var loadedParts []uint64
	var needToDelete []string

	for i := range entries {
		if entries[i].IsDir() {
			p, err := parseEpoch(entries[i].Name())
			if err != nil {
				s.l.Info().Err(err).Str("name", entries[i].Name()).Msg("cannot parse part directory name, skipping")
				needToDelete = append(needToDelete, entries[i].Name())
				continue
			}
			loadedParts = append(loadedParts, p)
			continue
		}
	}

	for i := range needToDelete {
		s.l.Info().Str("path", filepath.Join(s.root, needToDelete[i])).Msg("deleting invalid directory or file")
		s.fileSystem.MustRMAll(filepath.Join(s.root, needToDelete[i]))
	}

	if len(loadedParts) == 0 {
		s.l.Debug().Msg("no valid parts found")
		return
	}

	s.loadSnapshot(loadedParts, availablePartIDs)

	s.l.Info().Int("parts", len(loadedParts)).Msg("loaded existing sidx data")
}

func (s *sidx) loadSnapshot(loadedParts, availablePartIDs []uint64) {
	snp := newSnapshot(nil)
	for _, id := range loadedParts {
		var find bool
		for j := range availablePartIDs {
			if id == availablePartIDs[j] {
				find = true
				break
			}
		}
		if !find {
			s.fileSystem.MustRMAll(partPath(s.root, id))
			continue
		}
		err := validatePartMetadata(s.fileSystem, partPath(s.root, id))
		if err != nil {
			s.l.Info().Err(err).Uint64("id", id).Msg("cannot validate part metadata. skip and delete it")
			s.fileSystem.MustRMAll(partPath(s.root, id))
			continue
		}
		partPath := partPath(s.root, id)
		part := mustOpenPart(id, partPath, s.fileSystem)
		pw := newPartWrapper(nil, part)
		snp.addPart(pw)
	}
	if snp.getPartCount() < 1 {
		snp.release()
		return
	}
	snp.acquire()
	s.snapshot = snp
}
