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

package measure

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

// directCopyDayFormat / directCopyHourFormat mirror segmentController.format
// in banyand/internal/storage/segment.go.
const (
	directCopyDayFormat   = "20060102"
	directCopyHourFormat  = "2006010215"
	directCopySegPrefix   = "seg-"
	directCopyShardPrefix = "shard-"
	directCopySidxDirName = "sidx"
	directCopySnpSuffix   = ".snp"
)

var directCopyPartDirPattern = regexp.MustCompile(`^[0-9a-f]{16}$`)

// FastPathHits returns the number of source parts that took the fast (byte-copy) path.
func FastPathHits() int64 { return fastPathHits.Load() }

// SlowPathHits returns the number of source parts that went through the
// row-level rewrite path.
func SlowPathHits() int64 { return slowPathHits.Load() }

// SlowPathRows returns the total source-row count across all parts that
// were forced onto the slow path.
func SlowPathRows() int64 { return slowPathRows.Load() }

// measureMigrationLogPrefix tags every measure migration log line; the
// executor's LogPrefix() must return the same value so orchestrator-side
// and executor-side lines stay uniform.
const measureMigrationLogPrefix = "[migration/measure]"

func logStep(format string, args ...any) {
	fmt.Fprintf(os.Stdout, time.Now().Format("2006/01/02 15:04:05")+" "+measureMigrationLogPrefix+" "+format+"\n", args...)
}

func rejectIndexModeGroups(groups []string, schemas map[string]map[string]*measureSchemaInfo) error {
	var offenders []string
	for _, g := range groups {
		for _, m := range schemas[g] {
			if m.IndexMode {
				offenders = append(offenders, fmt.Sprintf("%s/%s", g, m.Name))
			}
		}
	}
	if len(offenders) == 0 {
		return nil
	}
	sort.Strings(offenders)
	return fmt.Errorf("refusing to copy IndexMode measures (their data lives inside sidx; "+
		"the union-sidx broadcast strategy would break query correctness): %s",
		strings.Join(offenders, ", "))
}

var (
	fastPathHits atomic.Int64
	slowPathHits atomic.Int64
	slowPathRows atomic.Int64
)

type partTask struct {
	srcSegName string
	shardName  string
	shardDir   string
	partIDStr  string
}

// discoverPartTasks walks every <srcGroupRoot>/seg-*/shard-*/<partID>
// directory across all supplied roots and produces a flat task list
// for the worker pool to consume without further IO contention.
func discoverPartTasks(ctx context.Context, srcGroupRoots []string) ([]partTask, error) {
	var tasks []partTask
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
			if !segE.IsDir() || !strings.HasPrefix(segE.Name(), directCopySegPrefix) {
				continue
			}
			srcSegName := segE.Name()
			srcSegDir := filepath.Join(srcGroupRoot, srcSegName)
			shardEntries, err := os.ReadDir(srcSegDir)
			if err != nil {
				return nil, fmt.Errorf("read %s: %w", srcSegDir, err)
			}
			for _, shardE := range shardEntries {
				if !shardE.IsDir() || !strings.HasPrefix(shardE.Name(), directCopyShardPrefix) {
					continue
				}
				shardName := shardE.Name()
				shardDir := filepath.Join(srcSegDir, shardName)
				partEntries, err := os.ReadDir(shardDir)
				if err != nil {
					return nil, fmt.Errorf("read %s: %w", shardDir, err)
				}
				for _, partE := range partEntries {
					if !partE.IsDir() || !directCopyPartDirPattern.MatchString(partE.Name()) {
						continue
					}
					tasks = append(tasks, partTask{
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

// segStateForGroup tracks bookkeeping needed at end-of-group to write
// segment-level metadata.json and per-shard .snp files.
type segStateForGroup struct {
	alignedTime time.Time
	shards      map[string][]uint64 // shard-N -> partIDs landed here
}

// segStateRegistry is a goroutine-safe registry of segStateForGroup,
// used so the per-part workers can register their landed (alignedSeg,
// shard, partID) tuples concurrently without stepping on each other.
type segStateRegistry struct {
	m  map[string]*segStateForGroup
	mu sync.Mutex
}

func newSegStateRegistry() *segStateRegistry {
	return &segStateRegistry{m: map[string]*segStateForGroup{}}
}

// register attaches one target partID to (alignedSegName, shardName) and
// guarantees a segStateForGroup exists for alignedSegName with the
// supplied alignedTime. Concurrent callers serialize on the registry's
// mutex; the critical section is tiny (slice append + map lookup) so
// contention stays well below worker compute cost.
func (r *segStateRegistry) register(alignedSegName string, alignedTime time.Time, shardName string, partID uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ss, ok := r.m[alignedSegName]
	if !ok {
		ss = &segStateForGroup{
			alignedTime: alignedTime,
			shards:      map[string][]uint64{},
		}
		r.m[alignedSegName] = ss
	}
	ss.shards[shardName] = append(ss.shards[shardName], partID)
}

// snapshot returns the underlying map for serial end-of-group
// processing (metadata.json + .snp writes). Callers may only iterate
// it after every worker has finished.
func (r *segStateRegistry) snapshot() map[string]*segStateForGroup {
	return r.m
}

// directCopyGroup walks every source part for one group, splits each
// source part's rows by aligned target segment, flushes one memPart per
// (alignedSeg, sourcePart) bucket, then writes per-segment metadata and
// per-shard .snp at the end. The group's union sidx (pre-built in
// staging) is broadcast 1:1 into every aligned target segment that
// received any rows; an empty unionSidxPath skips the sidx broadcast.
func directCopyGroup(ctx context.Context,
	entryTag string,
	group, stage, dstGroupRoot string,
	ir storage.IntervalRule, groupSchemas map[string]*measureSchemaInfo,
	tasks []partTask, unionSidxPath string,
) (migration.EntryGroupResult, error) {
	var res migration.EntryGroupResult

	if err := directCopyPrepareTarget(dstGroupRoot); err != nil {
		return res, err
	}

	tagProjection := buildTagProjectionFromGroupSchemas(groupSchemas)

	segStates := newSegStateRegistry()
	// partIDGen issues monotonic partIDs (1, 2, 3, ...) shared across
	// every srcGroupRoot and every worker so concurrent writes never
	// collide. Starting at 0 is safe because directCopyPrepareTarget
	// asserts the target group dir is empty and the migration runs
	// while banyandb is not writing to it; on restart banyandb seeds
	// its own counter past the highest partID we wrote.
	var partIDGen atomic.Uint64
	fileSystem := migration.NoFsyncFS{FileSystem: fs.NewLocalFileSystem()}

	totalTasks := len(tasks)

	// Worker pool: N = NumCPU, each worker owns a fresh BytesBlockDecoder
	// (the decoder caches state across reads and is not safe for concurrent
	// use). Workers fan out over the task list; the segStateRegistry,
	// partIDGen and result counters are the only shared mutable state.
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

	// Shared bounded flush pool: a fixed set of consumers drain
	// flushCh and do the heavy encode + zstd + write per bucket. Outer
	// workers (per source part) post one job per bucket per chunk and
	// wait on a per-chunk WaitGroup; this caps simultaneous encode+zstd
	// at flushWorkerCount rather than `outerWorkers × bucketsPerChunk`
	// (which had no bound and caused OOM on cold-tier fan-out workloads).
	flushWorkerCount := workerCount
	flushCh := make(chan flushJob, flushWorkerCount*2)
	var flushWg sync.WaitGroup
	for i := 0; i < flushWorkerCount; i++ {
		flushWg.Add(1)
		go func() {
			defer flushWg.Done()
			runFlushWorker(flushCh, fileSystem)
		}()
	}
	defer func() {
		close(flushCh)
		flushWg.Wait()
	}()

	// Buffered so producers don't rendezvous with each consumer pull.
	taskCh := make(chan partTask, workerCount*2)
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
				partResult, err := func() (pr processPartResult, err error) {
					// processOneSourcePart's mustReadMetadata / mustOpenFilePart /
					// b.mustReadFrom panic on corrupted parts. Convert panics into
					// firstErr so one bad source part doesn't tear down the whole
					// migration before the deferred staging-dir cleanup runs.
					defer func() {
						if r := recover(); r != nil {
							err = fmt.Errorf("panic processing %s/%s/%s: %v",
								task.srcSegName, task.shardName, task.partIDStr, r)
						}
					}()
					return processOneSourcePart(
						processPartInput{
							ir:            ir,
							decoder:       decoder,
							fileSystem:    fileSystem,
							tagProjection: tagProjection,
							shardName:     task.shardName,
							shardDir:      task.shardDir,
							partIDStr:     task.partIDStr,
							dstGroupRoot:  dstGroupRoot,
							segStates:     segStates,
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
				logStep("%s stage=%s group %s: part %d/%d (%.1f%%) done %s/%s/%s rows=%d targetParts=%d",
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
	logStep("%s stage=%s group %s: finalizing %d aligned target segments (writing metadata + snp + union sidx)",
		entryTag, stage, group, len(segSnapshot))
	for alignedSeg, ss := range segSnapshot {
		res.Segments++
		segDir := filepath.Join(dstGroupRoot, alignedSeg)
		endTime := ir.NextTime(ss.alignedTime)
		metaBytes, err := writeDirectCopySegmentMetadata(segDir, endTime)
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
				filepath.Join(segDir, directCopySidxDirName),
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
			snpBytes, err := writeDirectCopySnp(
				filepath.Join(segDir, shardName), names,
			)
			if err != nil {
				return res, fmt.Errorf("seg %s shard %s snp: %w", alignedSeg, shardName, err)
			}
			res.Bytes += snpBytes
		}
		logStep("%s stage=%s group %s: seg %s finalized (metadata + %d shard snp(s), union sidx=%v)",
			entryTag, stage, group, alignedSeg, len(ss.shards), unionSidxAvailable)
	}

	return res, nil
}

// buildTagProjectionFromGroupSchemas merges every measure's tag families
// in the group into a single tagProjection set. Reading more tags than
// the part actually contains is safe — block.mustReadFrom silently skips
// families/tags that aren't in the block.
func buildTagProjectionFromGroupSchemas(measures map[string]*measureSchemaInfo) []model.TagProjection {
	if len(measures) == 0 {
		return nil
	}
	families := map[string]map[string]bool{} // family -> set(tag)
	for _, m := range measures {
		for _, tf := range m.TagFamilies {
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

// processPartInput is the closed-over context passed to processOneSourcePart;
// keeping the helper free of long signatures. Field order is readability-first
// for an internal helper; fieldalignment churn here would obscure intent.
//
//nolint:govet // internal-only helper, readability > minor padding savings
type processPartInput struct {
	decoder       *encoding.BytesBlockDecoder
	fileSystem    fs.FileSystem
	segStates     *segStateRegistry
	partIDGen     *atomic.Uint64
	flushCh       chan<- flushJob
	tagProjection []model.TagProjection
	shardName     string
	shardDir      string
	partIDStr     string
	dstGroupRoot  string
	ir            storage.IntervalRule
}

type processPartResult struct {
	rows        int64
	bytes       int64
	targetParts int
}

// processOneSourcePart processes one source part. When the part is
// entirely inside one aligned target segment (the common case for
// already-grid-aligned backups), it takes the fast path: copy the
// whole part directory file-for-file and skip the decode / re-encode
// loop. Otherwise it falls back to the slow row-level split path
// that re-routes each row by timestamp.
func processOneSourcePart(in processPartInput) (processPartResult, error) {
	var pr processPartResult
	srcPartID, err := strconv.ParseUint(in.partIDStr, 16, 64)
	if err != nil {
		return pr, fmt.Errorf("parse partID %s: %w", in.partIDStr, err)
	}
	srcPartDir := filepath.Join(in.shardDir, in.partIDStr)

	// Cheap metadata-only probe to decide between fast and slow paths.
	var srcMeta partMetadata
	srcMeta.mustReadMetadata(in.fileSystem, srcPartDir)

	alignedMin := in.ir.Standard(time.Unix(0, srcMeta.MinTimestamp))
	alignedMax := in.ir.Standard(time.Unix(0, srcMeta.MaxTimestamp))
	if alignedMin.Equal(alignedMax) {
		// Fast path: entire part lands in a single target aligned segment.
		alignedSegName := formatDirectCopySegName(alignedMin, in.ir.Unit)
		fastPathHits.Add(1)
		return fastCopyOnePart(in, srcPartDir, alignedSegName, alignedMin, srcMeta.TotalCount)
	}

	slowPathHits.Add(1)
	slowPathRows.Add(int64(srcMeta.TotalCount))
	return slowCopyOnePart(in, srcPartID)
}

// slowCopyArena is a per-call slab that holds every nameValue and the
// supporting []*nameValue / []nameValues backing arrays produced by
// the row-level rewrite. Holding all of them in pre-sized contiguous
// arrays — instead of doing one heap allocation per (row, family,
// column) — was worth ~40-50% of the wall time on a cold-grid
// workload by collapsing the GC scan footprint.
//
// The arena is reset (length truncated to 0; cap retained) every time
// a chunk is flushed, so the same underlying memory serves the next
// chunk without further allocator pressure. Buckets that still hold
// *nameValue pointers into the arena are flushed before reset.
type slowCopyArena struct {
	nvs      []nameValue
	nvPtrs   []*nameValue
	famSlots []nameValues
}

func (a *slowCopyArena) reset() {
	a.nvs = a.nvs[:0]
	a.nvPtrs = a.nvPtrs[:0]
	a.famSlots = a.famSlots[:0]
}

// allocNameValue appends a nameValue to the arena and returns a stable
// pointer into the slab. The caller must guarantee cap(nvs) is large
// enough that the append doesn't reallocate (i.e. arena was sized for
// the chunk). On overflow we fall back to a fresh heap alloc to stay
// correct under pathological inputs.
func (a *slowCopyArena) allocNameValue(name string, value []byte, valueType pbv1.ValueType) *nameValue {
	if len(a.nvs)+1 > cap(a.nvs) {
		return &nameValue{name: name, value: value, valueType: valueType}
	}
	a.nvs = append(a.nvs, nameValue{name: name, value: value, valueType: valueType})
	return &a.nvs[len(a.nvs)-1]
}

// arenaTakePtrs carves out a contiguous []*nameValue of length n from
// the arena's pointer backing array. Like allocNameValue, it falls back
// to a fresh slice if the arena would have to grow (which would
// invalidate pointers we handed out earlier in the chunk).
func arenaTakePtrs(a *slowCopyArena, n int) []*nameValue {
	if len(a.nvPtrs)+n > cap(a.nvPtrs) {
		return make([]*nameValue, n)
	}
	start := len(a.nvPtrs)
	a.nvPtrs = a.nvPtrs[:start+n]
	return a.nvPtrs[start : start+n : start+n]
}

// arenaTakeFamSlots carves out a contiguous []nameValues of length n
// for the per-row tag-family slot table.
func arenaTakeFamSlots(a *slowCopyArena, n int) []nameValues {
	if len(a.famSlots)+n > cap(a.famSlots) {
		return make([]nameValues, n)
	}
	start := len(a.famSlots)
	a.famSlots = a.famSlots[:start+n]
	return a.famSlots[start : start+n : start+n]
}

// slowCopyOnePartChunkRows caps how many rows the slow path keeps
// in-memory across all buckets before forcing a flush. Tuned on the
// cold-grid workload:
//
//   - 100K rows: ~36s wall but ~26 GB RSS peak with NumCPU=16
//     workers — overran a kind-on-Mac 32 GB Docker VM.
//   - 50K rows: ~50s wall (~40% slower) but ~13 GB RSS peak — fits
//     comfortably in a 20 GB pod limit and leaves headroom for the
//     in-flight flush-pool encoders.
//
// The arena slab below is sized off this constant; halving it directly
// halves the long-lived per-worker allocation.
const slowCopyOnePartChunkRows = 50_000

// slowCopyArenaPool pools per-call slowCopyArena instances so
// successive parts processed by the same worker don't re-allocate the
// nameValue / *nameValue / nameValues backing arrays. The pool retains
// each arena's underlying cap; reset() truncates len to 0 but keeps
// cap so the next part reuses the slab in-place.
var slowCopyArenaPool = sync.Pool{
	New: func() any {
		// SkyWalking metric measures have 4-8 tag/field columns per row
		// in practice. Sizing the slab at 8 (was 16) halves the long-
		// lived per-worker allocation; pathological inputs fall back
		// to fresh heap allocs in allocNameValue / arenaTakePtrs.
		const estColumnsPerRow = 8
		return &slowCopyArena{
			nvs:      make([]nameValue, 0, slowCopyOnePartChunkRows*estColumnsPerRow),
			nvPtrs:   make([]*nameValue, 0, slowCopyOnePartChunkRows*estColumnsPerRow),
			famSlots: make([]nameValues, 0, slowCopyOnePartChunkRows*2),
		}
	},
}

func acquireSlowCopyArena() *slowCopyArena {
	a := slowCopyArenaPool.Get().(*slowCopyArena)
	a.reset()
	return a
}

func releaseSlowCopyArena(a *slowCopyArena) {
	a.reset()
	slowCopyArenaPool.Put(a)
}

// appendBlockRowToBuckets routes one row of one block to the correct
// per-aligned-segment bucket and writes its (seriesID, ts, version,
// tagFamilies, field) tuple via the arena. Block buffers are aliased
// directly into nameValue.value — the block must outlive the bucket
// until flush completes, which slowCopyOnePart guarantees by deferring
// releaseBlock until after flushChunk.
//
// Lives outside slowCopyOnePart so when banyandb's block struct gains
// or drops a field, only this helper needs updating.
// alignedSegCache memoises the last (alignedSegName, segment window)
// pair so the row loop avoids ir.Standard + formatDirectCopySegName
// on consecutive rows that share a target segment — typically the
// common case once banyandb data is roughly time-clustered per block.
type alignedSegCache struct {
	name       string
	startNanos int64 // inclusive
	endNanos   int64 // exclusive
}

func (c *alignedSegCache) segNameFor(ir storage.IntervalRule, ts int64) string {
	if c.name != "" && ts >= c.startNanos && ts < c.endNanos {
		return c.name
	}
	start := ir.Standard(time.Unix(0, ts))
	c.name = formatDirectCopySegName(start, ir.Unit)
	c.startNanos = start.UnixNano()
	c.endNanos = ir.NextTime(start).UnixNano()
	return c.name
}

func appendBlockRowToBuckets(
	ir storage.IntervalRule,
	b *block,
	seriesID common.SeriesID,
	k uint64,
	arena *slowCopyArena,
	buckets map[string]*dataPoints,
	segCache *alignedSegCache,
) {
	ts := b.timestamps[k]
	alignedSegName := segCache.segNameFor(ir, ts)

	dp, exists := buckets[alignedSegName]
	if !exists {
		dp = generateDataPoints()
		dp.reset()
		buckets[alignedSegName] = dp
	}
	dp.seriesIDs = append(dp.seriesIDs, seriesID)
	dp.timestamps = append(dp.timestamps, ts)
	dp.versions = append(dp.versions, b.versions[k])

	rowTagFamilies := arenaTakeFamSlots(arena, len(b.tagFamilies))
	for fi := range b.tagFamilies {
		cf := &b.tagFamilies[fi]
		ptrs := arenaTakePtrs(arena, len(cf.columns))
		for ci := range cf.columns {
			c := &cf.columns[ci]
			var v []byte
			if uint64(len(c.values)) > k {
				v = c.values[k]
			}
			ptrs[ci] = arena.allocNameValue(c.name, v, c.valueType)
		}
		rowTagFamilies[fi] = nameValues{name: cf.name, values: ptrs}
	}
	dp.tagFamilies = append(dp.tagFamilies, rowTagFamilies)

	fieldPtrs := arenaTakePtrs(arena, len(b.field.columns))
	for ci := range b.field.columns {
		c := &b.field.columns[ci]
		var v []byte
		if uint64(len(c.values)) > k {
			v = c.values[k]
		}
		fieldPtrs[ci] = arena.allocNameValue(c.name, v, c.valueType)
	}
	dp.fields = append(dp.fields, nameValues{name: b.field.name, values: fieldPtrs})
}

// slowCopyOnePart handles the row-level rewrite path: opens the source
// part, decodes blocks, and routes rows into per-aligned-target
// buckets. Buckets are flushed when the cumulative row count reaches
// slowCopyOnePartChunkRows or at end of part. Live blocks are held
// alive across multiple block iterations so bucket nameValue.value
// slices can alias the block buffers (zero-copy) — they are all
// released only after the buckets that reference them have been
// flushed.
//
// The primaryBlockMetadata walk + b.mustReadFrom pattern intentionally
// mirrors migration_reader.go:readPartRows (the MigrationReplay sibling).
// partIter (used by query / merge paths) carries a sids-filter + heap
// merge that adds no value when we want every block of one part — and
// would force an upfront scan to enumerate seriesIDs. Keep the two
// migration_* functions in lockstep: if banyandb changes its block
// layout, both update together.
func slowCopyOnePart(in processPartInput, srcPartID uint64) (processPartResult, error) {
	var pr processPartResult
	p := mustOpenFilePart(srcPartID, in.shardDir, in.fileSystem)
	defer p.close()

	flushBuckets := func(buckets map[string]*dataPoints) error {
		if len(buckets) == 0 {
			return nil
		}
		// Submit one job per bucket to the shared flush pool and wait
		// on a per-chunk WaitGroup. The shared pool caps concurrent
		// encode + zstd at flushWorkerCount instead of spawning one
		// goroutine per (outerWorker, bucket) — that unbounded fan-out
		// ballooned heap to 224 GB compressed on cold-tier loads and
		// got jetsam-killed.
		var (
			chunkWg    sync.WaitGroup
			chunkErrMu sync.Mutex
			chunkErr   error
			chunkBytes atomic.Int64
		)
		setErr := func(err error) {
			chunkErrMu.Lock()
			if chunkErr == nil {
				chunkErr = err
			}
			chunkErrMu.Unlock()
		}
		addBytes := func(b int64) { chunkBytes.Add(b) }
		for alignedSegName, dp := range buckets {
			chunkWg.Add(1)
			targetPartID := in.partIDGen.Add(1)
			targetPartIDStr := fmt.Sprintf("%016x", targetPartID)
			dstPart := filepath.Join(in.dstGroupRoot, alignedSegName, in.shardName, targetPartIDStr)
			in.flushCh <- flushJob{
				dp:           dp,
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

	buckets := map[string]*dataPoints{}
	liveBlocks := make([]*block, 0, 8)
	chunkRows := 0
	arena := acquireSlowCopyArena()
	defer releaseSlowCopyArena(arena)
	var segCache alignedSegCache

	releaseLive := func() {
		for _, b := range liveBlocks {
			releaseBlock(b)
		}
		liveBlocks = liveBlocks[:0]
	}
	// Guarantee live blocks return to the pool even on flush errors,
	// chunk-walk errors, or early returns below. Successful flushChunk
	// calls drain liveBlocks first; this defer is a safety net.
	defer releaseLive()
	// Safety net for unflushed bucket dp's on early-return error paths
	// (decompress / unmarshal failures below). Once flushBuckets has been
	// called the dp's are owned by the workers (directCopyFlushBucket
	// defers releaseDataPoints), so flushChunk clears the map afterwards
	// to keep this defer a no-op in the normal path.
	defer func() {
		for _, dp := range buckets {
			releaseDataPoints(dp)
		}
	}()
	flushChunk := func() error {
		if chunkRows == 0 {
			return nil
		}
		err := flushBuckets(buckets)
		// Workers have already returned every submitted dp to the pool
		// via directCopyFlushBucket's defer (regardless of per-job
		// error). Drop our map references so the outer defer doesn't
		// double-release.
		buckets = map[string]*dataPoints{}
		if err != nil {
			return err
		}
		releaseLive()
		arena.reset()
		// The chunk's rows are flushed and its blocks released; nothing
		// references the decoder's internal buffer anymore (columns alias
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
				appendBlockRowToBuckets(in.ir, b, bm.seriesID, k, arena, buckets, &segCache)
				pr.rows++
				chunkRows++
			}
			if chunkRows >= slowCopyOnePartChunkRows {
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

// fastCopyOnePart handles the case where every row in a source part
// already lands in the same target aligned segment. We can skip all
// decoding / re-encoding and just copy the part directory verbatim
// (under a fresh target partID so concurrent sources don't collide).
// The caller pre-resolved alignedSegName + alignedTime from the part
// metadata so this function does no further timestamp math.
func fastCopyOnePart(
	in processPartInput,
	srcPartDir, alignedSegName string,
	alignedTime time.Time,
	totalCount uint64,
) (processPartResult, error) {
	var pr processPartResult
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
	return pr, nil
}

// flushJob is one bucket's worth of work submitted to the shared flush
// pool: write dp out as a target part, register the segment, and report
// bytes / errors back to the submitting chunk.
type flushJob struct {
	dp           *dataPoints
	chunkWg      *sync.WaitGroup
	setErr       func(error)
	addBytes     func(int64)
	segStates    *segStateRegistry
	alignedSeg   string
	dstPart      string
	shardName    string
	targetPartID uint64
	ir           storage.IntervalRule
}

// runFlushWorker drains flushCh until it is closed. The flush pool size
// is the only cap on concurrent encode + zstd + write — sized to NumCPU
// in directCopyGroup so the host can't be drowned by a chunked slow path.
func runFlushWorker(flushCh <-chan flushJob, fileSystem fs.FileSystem) {
	for job := range flushCh {
		sz, err := directCopyFlushBucket(job.dp, fileSystem, job.dstPart)
		if err != nil {
			job.setErr(fmt.Errorf("flush %s: %w", job.dstPart, err))
			job.chunkWg.Done()
			continue
		}
		job.addBytes(sz)
		alignedTime, err := parseDirectCopySegStart(job.alignedSeg, job.ir.Unit)
		if err != nil {
			job.setErr(fmt.Errorf("parse seg start %s: %w", job.alignedSeg, err))
			job.chunkWg.Done()
			continue
		}
		job.segStates.register(job.alignedSeg, alignedTime, job.shardName, job.targetPartID)
		job.chunkWg.Done()
	}
}

// directCopyFlushBucket builds a memPart from dp and flushes it to disk at
// partPath, then releases pooled objects. mustFlush internally calls
// MkdirPanicIfExist(partPath), so the caller must only pre-create the
// parent shard dir; this helper handles that. dp is always returned to
// the pool — including on the MkdirAll error path.
func directCopyFlushBucket(dp *dataPoints, fileSystem fs.FileSystem, partPath string) (sz int64, err error) {
	defer releaseDataPoints(dp)
	if mkdirErr := os.MkdirAll(filepath.Dir(partPath), storage.DirPerm); mkdirErr != nil {
		return 0, mkdirErr
	}
	mp := generateMemPart()
	defer releaseMemPart(mp)
	// mustInitFromDataPoints / mustFlush panic on unrecoverable conditions
	// (e.g. a full disk). Convert the panic into an error so the flush pool
	// reports it instead of an unrecovered goroutine panic killing the
	// consumer and stalling slow-path senders.
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("flush part %s panicked: %v", partPath, r)
		}
	}()
	mp.mustInitFromDataPoints(dp)
	mp.mustFlush(fileSystem, partPath)
	return int64(mp.partMetadata.CompressedSizeBytes), nil
}

// directCopyPrepareTarget refuses to write into a non-empty group dir.
// The operator must remove (or relocate) the previous target manually
// before re-running.
func directCopyPrepareTarget(dstGroupRoot string) error {
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

// formatDirectCopySegName mirrors segmentController.format.
func formatDirectCopySegName(t time.Time, unit storage.IntervalUnit) string {
	switch unit {
	case storage.HOUR:
		return directCopySegPrefix + t.Format(directCopyHourFormat)
	case storage.DAY:
		return directCopySegPrefix + t.Format(directCopyDayFormat)
	}
	panic(fmt.Sprintf("formatDirectCopySegName: unsupported interval unit %v", unit))
}

// parseDirectCopySegStart inverts formatDirectCopySegName.
func parseDirectCopySegStart(segName string, unit storage.IntervalUnit) (time.Time, error) {
	suffix := strings.TrimPrefix(segName, directCopySegPrefix)
	switch unit {
	case storage.HOUR:
		return time.ParseInLocation(directCopyHourFormat, suffix, time.Local)
	case storage.DAY:
		return time.ParseInLocation(directCopyDayFormat, suffix, time.Local)
	}
	return time.Time{}, fmt.Errorf("unrecognized interval unit %v", unit)
}

// writeDirectCopySegmentMetadata writes <segDir>/metadata matching the
// runtime's segmentMeta layout (banyand/internal/storage/version.go).
// The filename, JSON tags, and Version string all come from the exported
// storage helpers so a banyandb runtime that reads compatibleVersions
// from versions.yml will accept these files without modification.
func writeDirectCopySegmentMetadata(segDir string, endTime time.Time) (int64, error) {
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

// writeDirectCopySnp writes a <epoch-hex16>.snp file listing the part
// names for one (alignedSeg, shard) so tsTable.loadSnapshot finds them
// at next startup.
func writeDirectCopySnp(dstShard string, partNames []string) (int64, error) {
	if err := os.MkdirAll(dstShard, storage.DirPerm); err != nil {
		return 0, err
	}
	data, err := json.Marshal(partNames)
	if err != nil {
		return 0, err
	}
	snpPath := filepath.Join(dstShard,
		fmt.Sprintf("%016x%s", time.Now().UnixNano(), directCopySnpSuffix))
	if err := os.WriteFile(snpPath, data, storage.FilePerm); err != nil {
		return 0, err
	}
	return int64(len(data)), nil
}
