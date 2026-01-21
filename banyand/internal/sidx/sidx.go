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
	"bytes"
	"container/heap"
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/pool"
)

const (
	maxBlockLength           = 8 * 1024
	maxUncompressedBlockSize = 2 * 1024 * 1024
)

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

	if len(snap.parts) == 0 {
		return nil, nil
	}

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
		if d := s.l.Debug(); d.Enabled() {
			d.Int("flushed_count", len(flushIntro.flushed)).
				Str("root", s.root).
				Int("part_ids_to_flush_count", len(partIDsToFlush)).
				Msg("part ids to flush count does not match flushed count")
		}
	}
	return flushIntro, nil
}

// PartPaths implements SIDX interface and returns on-disk paths for the requested part IDs.
func (s *sidx) PartPaths(partIDs map[uint64]struct{}) map[uint64]string {
	if len(partIDs) == 0 {
		return map[uint64]string{}
	}

	snap := s.currentSnapshot()
	if snap == nil {
		return map[uint64]string{}
	}
	defer snap.decRef()

	result := make(map[uint64]string, len(partIDs))

	for _, pw := range snap.parts {
		var (
			id   uint64
			path string
		)

		switch {
		case pw.p != nil && pw.p.partMetadata != nil:
			id = pw.p.partMetadata.ID
			path = pw.p.path
		case pw.mp != nil && pw.mp.partMetadata != nil:
			id = pw.mp.partMetadata.ID
		default:
			continue
		}

		if _, ok := partIDs[id]; !ok {
			continue
		}
		// Skip mem parts since they do not have stable on-disk paths yet.
		if pw.isMemPart() || pw.p == nil {
			continue
		}
		if path == "" {
			path = partPath(s.root, id)
		}
		result[id] = path

		// All requested IDs found.
		if len(result) == len(partIDs) {
			break
		}
	}

	return result
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

type blockCursorBuilder struct {
	bc      *blockCursor
	block   *block
	metrics *batchMetrics
	seen    map[uint64][][]byte
	minKey  int64
	maxKey  int64
	hasMin  bool
	hasMax  bool
}

func (b *blockCursorBuilder) processWithFilter(req QueryRequest, log *logger.Logger) error {
	if req.TagFilter == nil {
		return nil
	}

	tags := make([]*modelv1.Tag, 0, len(b.block.tags))
	decoder := req.TagFilter.GetDecoder()

	for i := 0; i < len(b.block.userKeys); i++ {
		dataBytes := b.block.data[i]
		hash, duplicate := b.checkDuplicate(dataBytes, true)
		if duplicate {
			continue
		}

		tags = b.collectTagsForFilter(tags, decoder, i)

		matched, err := req.TagFilter.Match(tags)
		if err != nil {
			log.Error().Err(err).Msg("tag filter match error")
			return err
		}
		if !matched {
			continue
		}

		b.appendElement(i, hash, dataBytes)
	}

	return nil
}

func (b *blockCursorBuilder) processWithoutFilter() {
	for i := 0; i < len(b.block.userKeys); i++ {
		dataBytes := b.block.data[i]
		hash, duplicate := b.checkDuplicate(dataBytes, false)
		if duplicate {
			continue
		}

		b.appendElement(i, hash, dataBytes)
	}
}

func (b *blockCursorBuilder) collectTagsForFilter(buf []*modelv1.Tag, decoder func(pbv1.ValueType, []byte, [][]byte) *modelv1.TagValue, index int) []*modelv1.Tag {
	buf = buf[:0]

	for tagName, tagData := range b.block.tags {
		if index >= len(tagData.values) {
			continue
		}

		row := &tagData.values[index]
		tagValue := decoder(tagData.valueType, row.value, row.valueArr)
		if tagValue != nil {
			buf = append(buf, &modelv1.Tag{
				Key:   tagName,
				Value: tagValue,
			})
		}
	}
	return buf
}

func (b *blockCursorBuilder) appendElement(index int, hash uint64, dataBytes []byte) {
	key := b.block.userKeys[index]
	if !b.keyInRange(key) {
		return
	}

	b.markSeen(hash, dataBytes)

	b.bc.userKeys = append(b.bc.userKeys, key)

	dataCopy := make([]byte, len(dataBytes))
	copy(dataCopy, dataBytes)
	b.bc.data = append(b.bc.data, dataCopy)

	for tagName, tagData := range b.block.tags {
		if index < len(tagData.values) {
			row := &tagData.values[index]
			tag := Tag{
				Name:      tagName,
				ValueType: tagData.valueType,
			}
			if len(row.valueArr) > 0 {
				tag.ValueArr = row.valueArr
			} else if len(row.value) > 0 {
				tag.Value = row.value
			}
			b.bc.tags[tagName] = append(b.bc.tags[tagName], tag)
		}
	}
}

func (b *blockCursorBuilder) checkDuplicate(dataBytes []byte, recordMetric bool) (uint64, bool) {
	hash := convert.Hash(dataBytes)
	bucket := b.seen[hash]
	for _, existing := range bucket {
		if bytes.Equal(existing, dataBytes) {
			if recordMetric && b.metrics != nil {
				b.metrics.elementsDeduplicated.Add(1)
			}
			return hash, true
		}
	}
	return hash, false
}

func (b *blockCursorBuilder) markSeen(hash uint64, dataBytes []byte) {
	b.seen[hash] = append(b.seen[hash], dataBytes)
}

func (b *blockCursorBuilder) keyInRange(key int64) bool {
	if b.hasMin && key < b.minKey {
		return false
	}
	if b.hasMax && key > b.maxKey {
		return false
	}
	return true
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
func (s *sidx) loadBlockCursor(bc *blockCursor, tmpBlock *block, bs blockScanResult,
	tagsToLoad map[string]struct{}, req QueryRequest, pm protector.Memory, metrics *batchMetrics,
) bool {
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

	if metrics != nil {
		metrics.blockElementsLoaded.Add(int64(len(tmpBlock.userKeys)))
	}

	totalElements := len(tmpBlock.userKeys)
	if totalElements == 0 {
		return false
	}

	var (
		minKey int64
		maxKey int64
		hasMin = req.MinKey != nil
		hasMax = req.MaxKey != nil
	)
	if hasMin {
		minKey = *req.MinKey
	}
	if hasMax {
		maxKey = *req.MaxKey
	}

	// Pre-allocate slices for filtered data (optimize for common case where most elements match)
	bc.userKeys = make([]int64, 0, totalElements)
	bc.data = make([][]byte, 0, totalElements)

	// Pre-allocate tag slices
	bc.tags = make(map[string][]Tag)
	for tagName := range tmpBlock.tags {
		bc.tags[tagName] = make([]Tag, 0, totalElements)
	}

	// Track seen data for deduplication using hash buckets with collision checks
	builder := &blockCursorBuilder{
		bc:      bc,
		block:   tmpBlock,
		hasMin:  hasMin,
		minKey:  minKey,
		hasMax:  hasMax,
		maxKey:  maxKey,
		metrics: metrics,
		seen:    make(map[uint64][][]byte),
	}

	if req.TagFilter != nil {
		if err := builder.processWithFilter(req, s.l); err != nil {
			return false
		}
	} else {
		builder.processWithoutFilter()
	}

	if metrics != nil {
		metrics.outputElementsEmitted.Add(int64(len(bc.userKeys)))
		if len(bc.userKeys) == 0 {
			metrics.blocksSkipped.Add(1)
		}
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
	result.PartIDs = append(result.PartIDs, bc.p.ID())

	// Copy projected tags if specified
	if len(bc.request.TagProjection) > 0 && len(result.Tags) > 0 {
		for _, proj := range bc.request.TagProjection {
			for _, tagName := range proj.Names {
				if tagData, exists := bc.tags[tagName]; exists && bc.idx < len(tagData) {
					tagValue := formatTagValue(tagData[bc.idx])
					result.Tags[tagName] = append(result.Tags[tagName], tagValue)
				} else {
					// Tag not present for this row
					result.Tags[tagName] = append(result.Tags[tagName], "")
				}
			}
		}
	}

	return true
}

// formatTagValue converts a Tag to its string form: arrays are serialized as
// bracketed, comma-separated values, while scalar values are returned as-is.
// When both Value and ValueArr are empty, the function returns an empty string.
func formatTagValue(tag Tag) string {
	if len(tag.ValueArr) > 0 {
		// Array of values
		values := make([]string, len(tag.ValueArr))
		for i, v := range tag.ValueArr {
			values[i] = string(v)
		}
		return "[" + strings.Join(values, ",") + "]"
	}
	return string(tag.Value)
}

// blockCursorHeap implements heap.Interface for sorting block cursors.
type blockCursorHeap struct {
	bcc         []*blockCursor
	asc         bool
	initialized bool
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
	bch.initialized = false
}

// pushCursors adds cursors to the heap and initializes it if necessary.
func (bch *blockCursorHeap) pushCursors(cursors []*blockCursor) {
	if len(cursors) == 0 {
		return
	}

	for i := range cursors {
		if bch.initialized {
			heap.Push(bch, cursors[i])
		} else {
			bch.Push(cursors[i])
		}
	}

	if !bch.initialized && bch.Len() > 0 {
		heap.Init(bch)
		bch.initialized = true
	}
}

// merge performs heap-based merge similar to query_by_ts.go.
// It returns a QueryResponse along with a flag indicating whether the merge
// stopped because the MaxBatchSize limit has been reached.
func (bch *blockCursorHeap) merge(ctx context.Context, batchSize int, resultsCh chan<- *QueryResponse, metrics *batchMetrics) error {
	if !bch.initialized || bch.Len() == 0 {
		return nil
	}

	step := -1
	if bch.asc {
		step = 1
	}

	// Initialize first batch
	batch := &QueryResponse{
		Keys:    make([]int64, 0),
		Data:    make([][]byte, 0),
		SIDs:    make([]common.SeriesID, 0),
		PartIDs: make([]uint64, 0),
	}

	// Track seen data for deduplication across all batches using hash buckets with collision checks
	seenData := make(map[uint64][][]byte)

	for bch.Len() > 0 {
		topBC := bch.bcc[0]
		if topBC.idx < 0 || topBC.idx >= len(topBC.userKeys) {
			heap.Pop(bch)
			continue
		}

		// Check for duplicate data before copying via hash + bytes.Equal on collisions
		currentData := topBC.data[topBC.idx]
		h := convert.Hash(currentData)
		bucket := seenData[h]
		duplicate := false
		for _, b := range bucket {
			if bytes.Equal(b, currentData) {
				duplicate = true
				break
			}
		}

		// Only copy if this data hasn't been seen across all batches
		if !duplicate {
			// Copy the element (may be filtered out by key range)
			if topBC.copyTo(batch) {
				// Mark this data as seen
				seenData[h] = append(bucket, currentData)
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

		// Send the batch when it reaches batchSize
		if batchSize > 0 && batch.Len() >= batchSize {
			if metrics != nil {
				metrics.record(batch)
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case resultsCh <- batch:
			}

			// Create a new batch but keep the deduplication map for global deduplication
			batch = &QueryResponse{
				Keys:    make([]int64, 0),
				Data:    make([][]byte, 0),
				SIDs:    make([]common.SeriesID, 0),
				PartIDs: make([]uint64, 0),
			}
			// Do NOT reset seenData here - maintain it across batches for global deduplication
		}
	}

	// Send remaining elements in the last batch
	if batch.Len() > 0 {
		if metrics != nil {
			metrics.record(batch)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case resultsCh <- batch:
		}
	}

	return nil
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
