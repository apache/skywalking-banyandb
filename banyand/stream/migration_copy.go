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

package stream

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/internal/migration"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/compress/zstd"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
)

// directStreamCopyDayFormat / directStreamCopyHourFormat mirror segmentController.format.
const (
	directStreamCopyDayFormat   = "20060102"
	directStreamCopyHourFormat  = "2006010215"
	directStreamCopySegPrefix   = "seg-"
	directStreamCopyShardPrefix = "shard-"
	directStreamCopySidxDirName = "sidx"
	directStreamCopySnpSuffix   = ".snp"
)

var directStreamCopyPartDirPattern = regexp.MustCompile(`^[0-9a-f]{16}$`)

// FastPathHits returns the number of source parts that took the fast (byte-copy) path.
func FastPathHits() int64 { return streamFastPathHits.Load() }

// SlowPathHits returns the number of source parts that went through the slow path.
func SlowPathHits() int64 { return streamSlowPathHits.Load() }

// SlowPathRows returns the total source-row count across slow-path parts.
func SlowPathRows() int64 { return streamSlowPathRows.Load() }

var (
	streamFastPathHits atomic.Int64
	streamSlowPathHits atomic.Int64
	streamSlowPathRows atomic.Int64
)

// streamMigrationLogPrefix tags every stream migration log line; the
// executor's LogPrefix() must return the same value so orchestrator-side
// and executor-side lines stay uniform.
const streamMigrationLogPrefix = "[migration/stream]"

func logStreamStep(format string, args ...any) {
	fmt.Fprintf(os.Stdout, time.Now().Format("2006/01/02 15:04:05")+" "+streamMigrationLogPrefix+" "+format+"\n", args...)
}

// ── Part discovery ────────────────────────────────────────────────.

type streamPartTask struct {
	srcSegName string
	shardName  string
	shardDir   string
	partIDStr  string
}

func discoverStreamPartTasks(ctx context.Context, srcGroupRoots []string) ([]streamPartTask, error) {
	var tasks []streamPartTask
	for _, srcGroupRoot := range srcGroupRoots {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		segEntries, err := os.ReadDir(srcGroupRoot)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, fmt.Errorf("read %s: %w", srcGroupRoot, err)
		}
		for _, segE := range segEntries {
			if !segE.IsDir() || !strings.HasPrefix(segE.Name(), directStreamCopySegPrefix) {
				continue
			}
			srcSegName := segE.Name()
			srcSegDir := filepath.Join(srcGroupRoot, srcSegName)
			shardEntries, err := os.ReadDir(srcSegDir)
			if err != nil {
				return nil, fmt.Errorf("read %s: %w", srcSegDir, err)
			}
			for _, shardE := range shardEntries {
				if !shardE.IsDir() || !strings.HasPrefix(shardE.Name(), directStreamCopyShardPrefix) {
					continue
				}
				shardName := shardE.Name()
				shardDir := filepath.Join(srcSegDir, shardName)
				partEntries, err := os.ReadDir(shardDir)
				if err != nil {
					return nil, fmt.Errorf("read %s: %w", shardDir, err)
				}
				for _, partE := range partEntries {
					if !partE.IsDir() || !directStreamCopyPartDirPattern.MatchString(partE.Name()) {
						continue
					}
					tasks = append(tasks, streamPartTask{
						srcSegName: srcSegName,
						shardName:  shardName,
						shardDir:   shardDir,
						partIDStr:  partE.Name(),
					})
				}
			}
		}
	}
	return tasks, nil
}

// ── Segment state registry ─────────────────────────────────────────────.

type streamSegStateForGroup struct {
	alignedTime time.Time
	shards      map[string][]uint64 // shard-N -> partIDs
}

type streamSegStateRegistry struct {
	m  map[string]*streamSegStateForGroup
	mu sync.Mutex
}

func newStreamSegStateRegistry() *streamSegStateRegistry {
	return &streamSegStateRegistry{m: map[string]*streamSegStateForGroup{}}
}

func (r *streamSegStateRegistry) register(alignedSegName string, alignedTime time.Time, shardName string, partID uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ss, ok := r.m[alignedSegName]
	if !ok {
		ss = &streamSegStateForGroup{
			alignedTime: alignedTime,
			shards:      map[string][]uint64{},
		}
		r.m[alignedSegName] = ss
	}
	ss.shards[shardName] = append(ss.shards[shardName], partID)
}

func (r *streamSegStateRegistry) snapshot() map[string]*streamSegStateForGroup {
	return r.m
}

// ── Aligned segment cache ──────────────────────────────────────────────.

type streamAlignedSegCache struct {
	name       string
	startNanos int64
	endNanos   int64
}

func (c *streamAlignedSegCache) segNameFor(ir storage.IntervalRule, ts int64) string {
	if c.name != "" && ts >= c.startNanos && ts < c.endNanos {
		return c.name
	}
	start := ir.Standard(time.Unix(0, ts))
	c.name = formatStreamDirectCopySegName(start, ir.Unit)
	c.startNanos = start.UnixNano()
	c.endNanos = ir.NextTime(start).UnixNano()
	return c.name
}

// ── Tag projection ────────────────────────────────────────────────.

func buildStreamTagProjectionFromGroupSchemas(schemas map[string]*streamSchemaInfo) []model.TagProjection {
	if len(schemas) == 0 {
		return nil
	}
	families := map[string]map[string]bool{}
	for _, s := range schemas {
		for _, tf := range s.TagFamilies {
			if families[tf.Name] == nil {
				families[tf.Name] = map[string]bool{}
			}
			for _, t := range tf.Tags {
				families[tf.Name][t] = true
			}
		}
	}
	out := make([]model.TagProjection, 0, len(families))
	famNames := make([]string, 0, len(families))
	for name := range families {
		famNames = append(famNames, name)
	}
	sort.Strings(famNames)
	for _, name := range famNames {
		tagSet := families[name]
		tags := make([]string, 0, len(tagSet))
		for t := range tagSet {
			tags = append(tags, t)
		}
		sort.Strings(tags)
		out = append(out, model.TagProjection{Family: name, Names: tags})
	}
	return out
}

// ── Main group copy driver ─────────────────────────────────────────────.

func directCopyStreamGroup(
	ctx context.Context,
	entryTag string,
	group, stage, dstGroupRoot string,
	ir storage.IntervalRule, tagProjection []model.TagProjection,
	tasks []streamPartTask, unionSidxPath string,
	indexLocators map[string]*streamIndexLocator,
) (migration.EntryGroupResult, error) {
	var res migration.EntryGroupResult

	if err := directCopyStreamPrepareTarget(dstGroupRoot); err != nil {
		return res, err
	}

	segStates := newStreamSegStateRegistry()
	elementIdx := newStreamElementIndexRegistry()
	var partIDGen atomic.Uint64
	fileSystem := migration.NoFsyncFS{FileSystem: fs.NewLocalFileSystem()}

	totalTasks := len(tasks)
	// GOMAXPROCS honors the container CPU quota (set by automaxprocs);
	// NumCPU reports the node's physical cores and over-spawns workers in
	// containers, multiplying the slow path's per-worker memory footprint.
	workerCount := runtime.GOMAXPROCS(0)
	if workerCount > totalTasks {
		workerCount = totalTasks
	}
	if workerCount < 1 {
		workerCount = 1
	}

	flushWorkerCount := workerCount
	flushCh := make(chan streamFlushJob, flushWorkerCount*2)
	var flushWg sync.WaitGroup
	for i := 0; i < flushWorkerCount; i++ {
		flushWg.Add(1)
		go func() {
			defer flushWg.Done()
			runStreamFlushWorker(flushCh, fileSystem)
		}()
	}
	defer func() {
		close(flushCh)
		flushWg.Wait()
	}()

	taskCh := make(chan streamPartTask, workerCount*2)
	var (
		wg             sync.WaitGroup
		resMu          sync.Mutex
		sourceParts    int
		targetParts    int
		rowsCopied     int64
		bytesWritten   int64
		firstErr       error
		errMu          sync.Mutex
		partsCompleted atomic.Int64
	)

	workerCtx, cancelWorkers := context.WithCancel(ctx)
	defer cancelWorkers()

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			decoder := &encoding.BytesBlockDecoder{}
			for task := range taskCh {
				if workerCtx.Err() != nil {
					return
				}
				partResult, err := func() (pr streamProcessPartResult, err error) {
					defer func() {
						if r := recover(); r != nil {
							err = fmt.Errorf("panic processing %s/%s/%s: %v",
								task.srcSegName, task.shardName, task.partIDStr, r)
						}
					}()
					return processOneStreamSourcePart(
						streamProcessPartInput{
							ir:            ir,
							decoder:       decoder,
							fileSystem:    fileSystem,
							tagProjection: tagProjection,
							srcSegName:    task.srcSegName,
							shardName:     task.shardName,
							shardDir:      task.shardDir,
							partIDStr:     task.partIDStr,
							dstGroupRoot:  dstGroupRoot,
							segStates:     segStates,
							elementIdx:    elementIdx,
							partIDGen:     &partIDGen,
							flushCh:       flushCh,
						},
					)
				}()
				if err != nil {
					errMu.Lock()
					if firstErr == nil {
						firstErr = err
						cancelWorkers()
					}
					errMu.Unlock()
					return
				}
				done := partsCompleted.Add(1)
				logStreamStep("%s stage=%s group %s: part %d/%d (%.1f%%) done %s/%s/%s rows=%d targetParts=%d",
					entryTag, stage, group, done, totalTasks,
					100.0*float64(done)/float64(totalTasks),
					task.srcSegName, task.shardName, task.partIDStr,
					partResult.rows, partResult.targetParts)
				resMu.Lock()
				sourceParts++
				targetParts += partResult.targetParts
				rowsCopied += partResult.rows
				bytesWritten += partResult.bytes
				resMu.Unlock()
			}
		}()
	}

	for _, t := range tasks {
		select {
		case taskCh <- t:
		case <-workerCtx.Done():
		}
	}
	close(taskCh)
	wg.Wait()
	if firstErr != nil {
		return res, firstErr
	}
	res.SourceParts = sourceParts
	res.TargetParts = targetParts
	res.Rows = rowsCopied
	res.Bytes = bytesWritten

	unionSidxAvailable := false
	if unionSidxPath != "" {
		if info, statErr := os.Stat(unionSidxPath); statErr == nil && info.IsDir() {
			unionSidxAvailable = true
		}
	}
	segSnapshot := segStates.snapshot()
	logStreamStep("%s stage=%s group %s: finalizing %d aligned target segments (writing metadata + snp + union sidx)",
		entryTag, stage, group, len(segSnapshot))
	for alignedSeg, ss := range segSnapshot {
		res.Segments++
		segDir := filepath.Join(dstGroupRoot, alignedSeg)
		endTime := ir.NextTime(ss.alignedTime)
		metaBytes, err := writeStreamDirectCopySegmentMetadata(segDir, endTime)
		if err != nil {
			return res, fmt.Errorf("seg %s metadata: %w", alignedSeg, err)
		}
		res.Bytes += metaBytes
		// Copy the union sidx BEFORE writing the .snp: the .snp is the
		// per-shard part-snapshot marker BanyanDB loads parts from, so writing
		// it last means an ENOSPC (or any error) mid-sidx leaves a segment with
		// no .snp — its parts stay invisible (fail-safe) rather than visible
		// with a truncated sidx.
		if unionSidxAvailable {
			sidxBytes, err := migration.CopyDir(
				unionSidxPath,
				filepath.Join(segDir, directStreamCopySidxDirName),
			)
			if err != nil {
				return res, fmt.Errorf("seg %s union sidx: %w", alignedSeg, err)
			}
			res.Bytes += sidxBytes
		}
		for shardName, partIDs := range ss.shards {
			sort.Slice(partIDs, func(i, j int) bool { return partIDs[i] < partIDs[j] })
			names := make([]string, len(partIDs))
			for i, pid := range partIDs {
				names[i] = fmt.Sprintf("%016x", pid)
			}
			snpBytes, err := writeStreamDirectCopySnp(
				filepath.Join(segDir, shardName), names,
			)
			if err != nil {
				return res, fmt.Errorf("seg %s shard %s snp: %w", alignedSeg, shardName, err)
			}
			res.Bytes += snpBytes
		}
		logStreamStep("%s stage=%s group %s: seg %s finalized (metadata + %d shard snp(s), union sidx=%v)",
			entryTag, stage, group, alignedSeg, len(ss.shards), unionSidxAvailable)
	}

	// Element index (idx/) finalize: per source (seg, shard), byte-copy when its
	// rows landed in exactly one target seg, otherwise rebuild per target seg from
	// rows + index rules. Runs single-threaded (slow path is rare) so concurrent
	// writers never open the same target idx store.
	idxBytes, err := finalizeStreamElementIndex(ctx, finalizeStreamElementIndexInput{
		entryTag:      entryTag,
		stage:         stage,
		group:         group,
		dstGroupRoot:  dstGroupRoot,
		ir:            ir,
		fileSystem:    fileSystem,
		decoder:       &encoding.BytesBlockDecoder{},
		tagProjection: tagProjection,
		indexLocators: indexLocators,
		registry:      elementIdx,
	})
	if err != nil {
		return res, fmt.Errorf("finalize element index: %w", err)
	}
	res.Bytes += idxBytes

	return res, nil
}

// ── Per-part processing ──────────────────────────────────────────────.

//nolint:govet // internal-only helper, readability > minor padding savings
type streamProcessPartInput struct {
	fileSystem    fs.FileSystem
	decoder       *encoding.BytesBlockDecoder
	segStates     *streamSegStateRegistry
	elementIdx    *streamElementIndexRegistry
	partIDGen     *atomic.Uint64
	flushCh       chan<- streamFlushJob
	shardDir      string
	srcSegName    string
	shardName     string
	partIDStr     string
	dstGroupRoot  string
	tagProjection []model.TagProjection
	ir            storage.IntervalRule
}

type streamProcessPartResult struct {
	rows        int64
	bytes       int64
	targetParts int
}

func processOneStreamSourcePart(in streamProcessPartInput) (streamProcessPartResult, error) {
	var pr streamProcessPartResult
	srcPartID, err := strconv.ParseUint(in.partIDStr, 16, 64)
	if err != nil {
		return pr, fmt.Errorf("parse partID %s: %w", in.partIDStr, err)
	}
	srcPartDir := filepath.Join(in.shardDir, in.partIDStr)

	var srcMeta partMetadata
	srcMeta.mustReadMetadata(in.fileSystem, srcPartDir)

	alignedMin := in.ir.Standard(time.Unix(0, srcMeta.MinTimestamp))
	alignedMax := in.ir.Standard(time.Unix(0, srcMeta.MaxTimestamp))
	if alignedMin.Equal(alignedMax) {
		alignedSegName := formatStreamDirectCopySegName(alignedMin, in.ir.Unit)
		streamFastPathHits.Add(1)
		return fastCopyOneStreamPart(in, srcPartDir, alignedSegName, alignedMin, srcMeta.TotalCount)
	}

	streamSlowPathHits.Add(1)
	streamSlowPathRows.Add(int64(srcMeta.TotalCount))
	return slowCopyOneStreamPart(in, srcPartID)
}

// fastCopyOneStreamPart handles the case where every row in a source part lands
// in the same target aligned segment. Byte-copies part dir AND the shard's idx/.
func fastCopyOneStreamPart(
	in streamProcessPartInput,
	srcPartDir, alignedSegName string,
	alignedTime time.Time,
	totalCount uint64,
) (streamProcessPartResult, error) {
	var pr streamProcessPartResult
	targetPartID := in.partIDGen.Add(1)
	targetPartIDStr := fmt.Sprintf("%016x", targetPartID)
	dstPart := filepath.Join(in.dstGroupRoot, alignedSegName, in.shardName, targetPartIDStr)
	pr.rows = int64(totalCount)
	pr.targetParts = 1

	if err := os.MkdirAll(filepath.Dir(dstPart), storage.DirPerm); err != nil {
		return pr, fmt.Errorf("mkdir %s: %w", filepath.Dir(dstPart), err)
	}
	bytesCopied, err := migration.CopyDir(srcPartDir, dstPart)
	if err != nil {
		return pr, fmt.Errorf("fast-copy %s -> %s: %w", srcPartDir, dstPart, err)
	}
	pr.bytes = bytesCopied

	in.segStates.register(alignedSegName, alignedTime, in.shardName, targetPartID)

	// Element index handling is decided per source (seg, shard) in a finalize step,
	// NOT per part: only record which target seg this part's rows landed in. If all
	// of this source (seg, shard)'s rows land in this one target seg, the finalize
	// step byte-copies the source idx/ faithfully; otherwise it rebuilds per target.
	if in.elementIdx != nil {
		in.elementIdx.recordTargetSeg(in.srcSegName, in.shardName, in.shardDir, alignedSegName)
	}
	return pr, nil
}

// ── Slow path ──────────────────────────────────────────────────.

const streamSlowCopyOnePartChunkRows = 50_000

// streamSlowCopyArena is a per-call slab for tagValue and tagValues backing arrays.
type streamSlowCopyArena struct {
	tvs      []tagValue
	tvPtrs   []*tagValue
	famSlots []tagValues
}

func (a *streamSlowCopyArena) reset() {
	a.tvs = a.tvs[:0]
	a.tvPtrs = a.tvPtrs[:0]
	a.famSlots = a.famSlots[:0]
}

func (a *streamSlowCopyArena) allocTagValue(tag string, value []byte, valueType pbv1.ValueType) *tagValue {
	if len(a.tvs)+1 > cap(a.tvs) {
		return &tagValue{tag: tag, value: value, valueType: valueType}
	}
	a.tvs = append(a.tvs, tagValue{tag: tag, value: value, valueType: valueType})
	return &a.tvs[len(a.tvs)-1]
}

func streamArenaTakePtrs(a *streamSlowCopyArena, n int) []*tagValue {
	if len(a.tvPtrs)+n > cap(a.tvPtrs) {
		return make([]*tagValue, n)
	}
	start := len(a.tvPtrs)
	a.tvPtrs = a.tvPtrs[:start+n]
	return a.tvPtrs[start : start+n : start+n]
}

func streamArenaTakeFamSlots(a *streamSlowCopyArena, n int) []tagValues {
	if len(a.famSlots)+n > cap(a.famSlots) {
		return make([]tagValues, n)
	}
	start := len(a.famSlots)
	a.famSlots = a.famSlots[:start+n]
	return a.famSlots[start : start+n : start+n]
}

var streamSlowCopyArenaPool = sync.Pool{
	New: func() any {
		const estColumnsPerRow = 8
		return &streamSlowCopyArena{
			tvs:      make([]tagValue, 0, streamSlowCopyOnePartChunkRows*estColumnsPerRow),
			tvPtrs:   make([]*tagValue, 0, streamSlowCopyOnePartChunkRows*estColumnsPerRow),
			famSlots: make([]tagValues, 0, streamSlowCopyOnePartChunkRows*2),
		}
	},
}

func acquireStreamSlowCopyArena() *streamSlowCopyArena {
	a := streamSlowCopyArenaPool.Get().(*streamSlowCopyArena)
	a.reset()
	return a
}

func releaseStreamSlowCopyArena(a *streamSlowCopyArena) {
	a.reset()
	streamSlowCopyArenaPool.Put(a)
}

// releaseArenaElements returns elements to the pool WITHOUT releasing its
// tagValues into the tagValue pool. The slow-copy path fills elements with
// tagValues owned by a per-call streamSlowCopyArena; releasing them via the
// normal releaseElements would put arena-backed *tagValue pointers into the
// global tagValuePool, where they alias memory the arena still owns and reuses —
// corrupting later writes. Clearing tagFamilies drops the arena pointers so none
// linger in the pooled elements.
func releaseArenaElements(e *elements) {
	e.seriesIDs = e.seriesIDs[:0]
	e.timestamps = e.timestamps[:0]
	e.elementIDs = e.elementIDs[:0]
	for i := range e.tagFamilies {
		e.tagFamilies[i] = nil
	}
	e.tagFamilies = e.tagFamilies[:0]
	elementsPool.Put(e)
}

// appendStreamBlockRowToBuckets routes one row to the correct per-aligned-segment bucket.
func appendStreamBlockRowToBuckets(
	ir storage.IntervalRule,
	b *block,
	seriesID common.SeriesID,
	k uint64,
	arena *streamSlowCopyArena,
	buckets map[string]*elements,
	segCache *streamAlignedSegCache,
) {
	ts := b.timestamps[k]
	alignedSegName := segCache.segNameFor(ir, ts)

	el, exists := buckets[alignedSegName]
	if !exists {
		el = generateElements()
		el.reset()
		buckets[alignedSegName] = el
	}
	el.seriesIDs = append(el.seriesIDs, seriesID)
	el.timestamps = append(el.timestamps, ts)
	el.elementIDs = append(el.elementIDs, b.elementIDs[k])

	rowTagFamilies := streamArenaTakeFamSlots(arena, len(b.tagFamilies))
	for fi := range b.tagFamilies {
		cf := &b.tagFamilies[fi]
		ptrs := streamArenaTakePtrs(arena, len(cf.tags))
		for ci := range cf.tags {
			c := &cf.tags[ci]
			var v []byte
			if uint64(len(c.values)) > k {
				v = c.values[k]
			}
			ptrs[ci] = arena.allocTagValue(c.name, v, c.valueType)
		}
		rowTagFamilies[fi] = tagValues{tag: cf.name, values: ptrs}
	}
	el.tagFamilies = append(el.tagFamilies, rowTagFamilies)
}

// slowCopyOneStreamPart handles the row-level rewrite path for stream parts.
// No dedup: stream preserves every element including same (seriesID, ts) with different elementID.
func slowCopyOneStreamPart(in streamProcessPartInput, srcPartID uint64) (streamProcessPartResult, error) {
	var pr streamProcessPartResult
	p := mustOpenFilePart(srcPartID, in.shardDir, in.fileSystem)
	defer p.close()

	flushBuckets := func(buckets map[string]*elements) error {
		if len(buckets) == 0 {
			return nil
		}
		var (
			chunkWg    sync.WaitGroup
			chunkErrMu sync.Mutex
			chunkErr   error
			chunkBytes atomic.Int64
		)
		setErr := func(e error) {
			chunkErrMu.Lock()
			if chunkErr == nil {
				chunkErr = e
			}
			chunkErrMu.Unlock()
		}
		addBytes := func(b int64) { chunkBytes.Add(b) }
		for alignedSegName, el := range buckets {
			if in.elementIdx != nil {
				in.elementIdx.recordTargetSeg(in.srcSegName, in.shardName, in.shardDir, alignedSegName)
			}
			chunkWg.Add(1)
			targetPartID := in.partIDGen.Add(1)
			targetPartIDStr := fmt.Sprintf("%016x", targetPartID)
			dstPart := filepath.Join(in.dstGroupRoot, alignedSegName, in.shardName, targetPartIDStr)
			in.flushCh <- streamFlushJob{
				el:           el,
				alignedSeg:   alignedSegName,
				dstPart:      dstPart,
				targetPartID: targetPartID,
				shardName:    in.shardName,
				ir:           in.ir,
				segStates:    in.segStates,
				chunkWg:      &chunkWg,
				setErr:       setErr,
				addBytes:     addBytes,
			}
		}
		chunkWg.Wait()
		pr.bytes += chunkBytes.Load()
		if chunkErr != nil {
			return chunkErr
		}
		pr.targetParts += len(buckets)
		return nil
	}

	buckets := map[string]*elements{}
	liveBlocks := make([]*block, 0, 8)
	chunkRows := 0
	arena := acquireStreamSlowCopyArena()
	defer releaseStreamSlowCopyArena(arena)
	var segCache streamAlignedSegCache

	releaseLive := func() {
		for _, b := range liveBlocks {
			releaseBlock(b)
		}
		liveBlocks = liveBlocks[:0]
	}
	defer releaseLive()
	defer func() {
		for _, el := range buckets {
			releaseArenaElements(el)
		}
	}()

	flushChunk := func() error {
		if chunkRows == 0 {
			return nil
		}
		err := flushBuckets(buckets)
		buckets = map[string]*elements{}
		if err != nil {
			return err
		}
		releaseLive()
		arena.reset()
		// The chunk's rows are flushed and its blocks released; nothing
		// references the decoder's internal buffer anymore (elements alias
		// it zero-copy, hence the reset must wait until here). Without this
		// the buffer accumulates every chunk the worker ever processed.
		in.decoder.Reset()
		chunkRows = 0
		return nil
	}

	var (
		compressed []byte
		raw        []byte
		bms        []blockMetadata
	)
	for i := range p.primaryBlockMetadata {
		pbm := &p.primaryBlockMetadata[i]
		compressed = bytes.ResizeOver(compressed, int(pbm.size))
		fs.MustReadData(p.primary, int64(pbm.offset), compressed)
		var err error
		raw, err = zstd.Decompress(raw[:0], compressed)
		if err != nil {
			releaseLive()
			return pr, fmt.Errorf("decompress primary block: %w", err)
		}
		bms, err = unmarshalBlockMetadata(bms[:0], raw)
		if err != nil {
			releaseLive()
			return pr, fmt.Errorf("unmarshal block metadata: %w", err)
		}
		for j := range bms {
			bm := &bms[j]
			bm.tagProjection = in.tagProjection
			b := generateBlock()
			b.mustReadFrom(in.decoder, p, *bm)
			liveBlocks = append(liveBlocks, b)

			for k := uint64(0); k < bm.count; k++ {
				appendStreamBlockRowToBuckets(in.ir, b, bm.seriesID, k, arena, buckets, &segCache)
				pr.rows++
				chunkRows++
			}
			if chunkRows >= streamSlowCopyOnePartChunkRows {
				if err := flushChunk(); err != nil {
					return pr, err
				}
			}
		}
	}
	if err := flushChunk(); err != nil {
		return pr, err
	}
	return pr, nil
}

// ── Flush pool ─────────────────────────────────────────────────.

type streamFlushJob struct {
	el           *elements
	chunkWg      *sync.WaitGroup
	setErr       func(error)
	addBytes     func(int64)
	segStates    *streamSegStateRegistry
	alignedSeg   string
	dstPart      string
	shardName    string
	targetPartID uint64
	ir           storage.IntervalRule
}

func runStreamFlushWorker(flushCh <-chan streamFlushJob, fileSystem fs.FileSystem) {
	for job := range flushCh {
		sz, err := directCopyStreamFlushBucket(job.el, fileSystem, job.dstPart)
		if err != nil {
			job.setErr(fmt.Errorf("flush %s: %w", job.dstPart, err))
			job.chunkWg.Done()
			continue
		}
		job.addBytes(sz)
		alignedTime, err := parseStreamDirectCopySegStart(job.alignedSeg, job.ir.Unit)
		if err != nil {
			job.setErr(fmt.Errorf("parse seg start %s: %w", job.alignedSeg, err))
			job.chunkWg.Done()
			continue
		}
		job.segStates.register(job.alignedSeg, alignedTime, job.shardName, job.targetPartID)
		job.chunkWg.Done()
	}
}

func directCopyStreamFlushBucket(el *elements, fileSystem fs.FileSystem, partPath string) (sz int64, err error) {
	defer releaseArenaElements(el)
	if mkErr := os.MkdirAll(filepath.Dir(partPath), storage.DirPerm); mkErr != nil {
		return 0, mkErr
	}
	mp := generateMemPart()
	defer releaseMemPart(mp)
	// mustInitFromElements / mustFlush panic on unrecoverable conditions (e.g. a
	// full disk). Convert the panic into an error so the flush pool reports it via
	// setErr and the run aborts gracefully (first error + staging cleanup) instead
	// of an unrecovered goroutine panic crashing the whole process.
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("flush part %s panicked: %v", partPath, r)
		}
	}()
	mp.mustInitFromElements(el)
	mp.mustFlush(fileSystem, partPath)
	return int64(mp.partMetadata.CompressedSizeBytes), nil
}

// ── Utility helpers ───────────────────────────────────────────────.

func directCopyStreamPrepareTarget(dstGroupRoot string) error {
	entries, err := os.ReadDir(dstGroupRoot)
	if err != nil {
		if os.IsNotExist(err) {
			return os.MkdirAll(dstGroupRoot, storage.DirPerm)
		}
		return err
	}
	if len(entries) > 0 {
		return fmt.Errorf("target %s is not empty; remove it before re-running", dstGroupRoot)
	}
	return nil
}

func formatStreamDirectCopySegName(t time.Time, unit storage.IntervalUnit) string {
	switch unit {
	case storage.HOUR:
		return directStreamCopySegPrefix + t.Format(directStreamCopyHourFormat)
	case storage.DAY:
		return directStreamCopySegPrefix + t.Format(directStreamCopyDayFormat)
	}
	panic(fmt.Sprintf("formatStreamDirectCopySegName: unsupported interval unit %v", unit))
}

func parseStreamDirectCopySegStart(segName string, unit storage.IntervalUnit) (time.Time, error) {
	suffix := strings.TrimPrefix(segName, directStreamCopySegPrefix)
	switch unit {
	case storage.HOUR:
		return time.ParseInLocation(directStreamCopyHourFormat, suffix, time.Local)
	case storage.DAY:
		return time.ParseInLocation(directStreamCopyDayFormat, suffix, time.Local)
	}
	return time.Time{}, fmt.Errorf("unrecognized interval unit %v", unit)
}

func writeStreamDirectCopySegmentMetadata(segDir string, endTime time.Time) (int64, error) {
	if err := os.MkdirAll(segDir, storage.DirPerm); err != nil {
		return 0, err
	}
	body := storage.SegmentMetadata{
		Version: storage.CurrentSegmentVersion,
		EndTime: endTime.Format(time.RFC3339Nano),
	}
	data, err := json.Marshal(body)
	if err != nil {
		return 0, err
	}
	if err := os.WriteFile(
		filepath.Join(segDir, storage.SegmentMetadataFilename),
		data, storage.FilePerm,
	); err != nil {
		return 0, err
	}
	return int64(len(data)), nil
}

func writeStreamDirectCopySnp(dstShard string, partNames []string) (int64, error) {
	if err := os.MkdirAll(dstShard, storage.DirPerm); err != nil {
		return 0, err
	}
	data, err := json.Marshal(partNames)
	if err != nil {
		return 0, err
	}
	snpPath := filepath.Join(dstShard,
		fmt.Sprintf("%016x%s", time.Now().UnixNano(), directStreamCopySnpSuffix))
	if err := os.WriteFile(snpPath, data, storage.FilePerm); err != nil {
		return 0, err
	}
	return int64(len(data)), nil
}
