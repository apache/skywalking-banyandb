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
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"

	"github.com/apache/skywalking-banyandb/banyand/internal/sidx"
	"github.com/apache/skywalking-banyandb/pkg/cgroups"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/watcher"
)

var mergeMaxConcurrencyCh = make(chan struct{}, cgroups.CPUs())

const (
	mergeTypeMem  = "mem"
	mergeTypeFile = "file"
	mergeLaneFast = "fast"
	mergeLaneSlow = "slow"
)

const defaultSmallMergeThreshold = 32 << 20 // 32MB fallback

func computeSmallMergeThreshold() uint64 {
	memLimit, err := cgroups.MemoryLimit()
	if err != nil || memLimit <= 0 {
		return defaultSmallMergeThreshold
	}
	threshold := uint64(memLimit) / 16
	if threshold < defaultSmallMergeThreshold {
		return defaultSmallMergeThreshold
	}
	return threshold
}

const (
	// defaultStageBudgetFloor is the lower bound on the staged-set budget so the
	// retention filter still makes useful progress on small or unknown limits.
	defaultStageBudgetFloor = 16 << 20 // 16MB
	// stageBudgetAggregateDivisor bounds the aggregate bytes staged across all
	// concurrent merges to ~memLimit/divisor: each of the up-to-CPUs() concurrent
	// merges gets memLimit/(divisor*CPUs).
	stageBudgetAggregateDivisor = 4
)

// testStageBudgetOverride forces the staging budget when non-zero. Test-only
// seam (mirrors forceSlowMerge); production always derives it from the memory
// limit via stageBudgetFromLimit.
var testStageBudgetOverride uint64

// resolveStageBudget returns the soft cap on bytes the in-merge retention filter
// may stage before a trace-boundary chunk flush, derived from the protector's
// memory limit. There is no operator flag; the budget self-tunes from the limit
// and the merge concurrency.
func resolveStageBudget(opt option) uint64 {
	if testStageBudgetOverride > 0 {
		return testStageBudgetOverride
	}
	var limit uint64
	if opt.protector != nil {
		limit = opt.protector.GetLimit()
	}
	return stageBudgetFromLimit(limit)
}

// stageBudgetFromLimit derives the per-merge staged-byte budget from the memory
// limit. With up to cgroups.CPUs() merges staging at once, each merge is allowed
// memLimit/(stageBudgetAggregateDivisor*CPUs) so the aggregate stays ~memLimit/
// stageBudgetAggregateDivisor, clamped to [defaultStageBudgetFloor,
// computeSmallMergeThreshold()] (never more than the fast/slow lane boundary).
// A zero limit (protector disabled) falls back to the lane-split floor.
func stageBudgetFromLimit(limit uint64) uint64 {
	if limit == 0 {
		return defaultSmallMergeThreshold
	}
	cpus := uint64(max(1, cgroups.CPUs()))
	budget := limit / (stageBudgetAggregateDivisor * cpus)
	budget = max(budget, uint64(defaultStageBudgetFloor))
	budget = min(budget, computeSmallMergeThreshold())
	return budget
}

type mergeDispatchRequest struct {
	enqueuedAt time.Time
	toBeMerged map[uint64]struct{}
	typ        string
	lane       string
	parts      []*partWrapper
}

func (tst *tsTable) mergeLoop(merges chan *mergerIntroduction, flusherNotifier watcher.Channel) {
	defer tst.loopCloser.Done()

	var lastProcessedEpoch uint64

	ew := flusherNotifier.Add(0, tst.loopCloser.CloseNotify())
	if ew == nil {
		return
	}

	threshold := computeSmallMergeThreshold()
	fastWorkers := max(1, cgroups.CPUs()/2)
	fastCh := make(chan *mergeDispatchRequest, fastWorkers)
	slowCh := make(chan *mergeDispatchRequest, 1)
	triggerCh := make(chan struct{}, 1)

	var workersWg, dispatcherWg sync.WaitGroup

	for i := 0; i < fastWorkers; i++ {
		workersWg.Add(1)
		run.Go(context.Background(), "trace.merger.fast-lane", tst.l, func(_ context.Context) {
			defer workersWg.Done()
			tst.mergeLaneWorker(fastCh, merges) //nolint:contextcheck
		})
	}
	workersWg.Add(1)
	run.Go(context.Background(), "trace.merger.slow-lane", tst.l, func(_ context.Context) {
		defer workersWg.Done()
		tst.mergeLaneWorker(slowCh, merges) //nolint:contextcheck
	})

	dispatcherWg.Add(1)
	run.Go(context.Background(), "trace.merger.dispatcher", tst.l, func(_ context.Context) {
		defer dispatcherWg.Done()
		tst.dispatcherLoop(triggerCh, threshold, fastCh, slowCh)
	})

	// Shutdown order: stop dispatcher first so no new work enters the lane
	// channels, then close the lane channels so idle workers exit their range
	// loops, then wait for workers to drain any in-flight merges.
	defer func() {
		close(triggerCh)
		dispatcherWg.Wait()
		close(fastCh)
		close(slowCh)
		workersWg.Wait()
	}()

	for {
		select {
		case <-tst.loopCloser.CloseNotify():
			return
		case <-ew.Watch():
			if curSnapshot := tst.currentSnapshot(); curSnapshot != nil {
				if curSnapshot.epoch > lastProcessedEpoch {
					select {
					case triggerCh <- struct{}{}:
					default:
					}
					lastProcessedEpoch = curSnapshot.epoch
				}
				curSnapshot.decRef()
			}
			ew = flusherNotifier.Add(lastProcessedEpoch, tst.loopCloser.CloseNotify())
			if ew == nil {
				return
			}
		}
	}
}

func (tst *tsTable) dispatcherLoop(triggerCh chan struct{}, threshold uint64, fastCh, slowCh chan *mergeDispatchRequest) {
	for {
		select {
		case <-tst.loopCloser.CloseNotify():
			return
		case _, ok := <-triggerCh:
			if !ok {
				return
			}
			if tst.dispatchAllMerges(threshold, fastCh, slowCh) {
				return
			}
		}
	}
}

func (tst *tsTable) dispatchAllMerges(threshold uint64, fastCh, slowCh chan *mergeDispatchRequest) bool {
	for {
		curSnapshot := tst.currentSnapshot()
		if curSnapshot == nil {
			return false
		}
		freeDiskSize := tst.freeDiskSpace(tst.root)
		var dst []*partWrapper
		dst, toBeMerged := tst.getPartsToMerge(curSnapshot, freeDiskSize, dst)
		if len(dst) < 2 {
			curSnapshot.decRef()
			return false
		}
		for _, pw := range dst {
			pw.incRef()
		}
		curSnapshot.decRef()

		tst.inFlightMu.Lock()
		if tst.inFlight == nil {
			tst.inFlight = make(map[uint64]struct{})
		}
		for _, pw := range dst {
			tst.inFlight[pw.ID()] = struct{}{}
		}
		tst.inFlightMu.Unlock()

		var totalSize uint64
		for _, pw := range dst {
			totalSize += pw.p.partMetadata.CompressedSizeBytes
		}

		lane := mergeLaneSlow
		targetCh := slowCh
		if totalSize < threshold {
			lane = mergeLaneFast
			targetCh = fastCh
		}

		req := &mergeDispatchRequest{
			parts:      dst,
			toBeMerged: toBeMerged,
			typ:        mergeTypeFile,
			lane:       lane,
			enqueuedAt: time.Now(),
		}

		tst.l.Info().
			Str("lane", lane).
			Uint64("totalSize", totalSize).
			Uint64("threshold", threshold).
			Int("partCount", len(dst)).
			Msg("dispatching merge")

		select {
		case targetCh <- req:
		case <-tst.loopCloser.CloseNotify():
			tst.releaseDispatchRequest(req)
			return true
		}
	}
}

func (tst *tsTable) mergeLaneWorker(ch chan *mergeDispatchRequest, merges chan *mergerIntroduction) {
	for req := range ch {
		if !req.enqueuedAt.IsZero() {
			tst.incTotalMergeQueueLatency(time.Since(req.enqueuedAt).Seconds(), req.typ, req.lane)
		}
		select {
		case mergeMaxConcurrencyCh <- struct{}{}:
		case <-tst.loopCloser.CloseNotify():
			tst.releaseDispatchRequest(req)
			// Drain remaining buffered requests so their inFlight entries and
			// part references are released. The lane channel is closed by the
			// mergeLoop shutdown defer after the dispatcher exits, which lets
			// this range loop terminate.
			for pending := range ch {
				tst.releaseDispatchRequest(pending)
			}
			return
		}

		tst.incTotalMergeLoopStarted(1)
		_, mergeErr := tst.mergePartsThenSendIntroduction(
			snapshotCreatorMerger, req.parts, req.toBeMerged, merges,
			tst.loopCloser.CloseNotify(), req.typ, req.lane,
		)
		tst.incTotalMergeLoopFinished(1)
		<-mergeMaxConcurrencyCh

		tst.releaseDispatchRequest(req)

		if mergeErr != nil {
			if !errors.Is(mergeErr, errClosed) {
				tst.l.Logger.Warn().Err(mergeErr).Str("typ", req.typ).Str("lane", req.lane).Msg("merge lane worker error")
				tst.incTotalMergeLoopErr(1)
			}
		}
	}
}

func (tst *tsTable) releaseDispatchRequest(req *mergeDispatchRequest) {
	tst.inFlightMu.Lock()
	for _, pw := range req.parts {
		delete(tst.inFlight, pw.ID())
	}
	tst.inFlightMu.Unlock()
	for _, pw := range req.parts {
		pw.decRef()
	}
}

func (tst *tsTable) mergePartsThenSendIntroduction(creator snapshotCreator, parts []*partWrapper, merged map[uint64]struct{}, merges chan *mergerIntroduction,
	closeCh <-chan struct{}, typ string, lane string,
) (*partWrapper, error) {
	reservedSpace := tst.reserveSpace(parts)
	defer releaseDiskSpace(reservedSpace)
	start := time.Now()
	newPartID := atomic.AddUint64(&tst.curPartID, 1)
	var filter *mergeFilter
	if tst.option.nativePipelineEnabled {
		if samplers := lookupSamplers(tst.group); len(samplers) > 0 {
			graceNs := lookupMergeGrace(tst.group)
			if graceNs <= 0 {
				graceNs = int64(tst.option.mergeGraceDefault)
			}
			if !isMergeHot(parts, graceNs, time.Now().UnixNano()) {
				chain := newMergeChain(tst.group, "", samplers, tst.option.decideTimeoutCircuitBreak)
				filter = &mergeFilter{
					chain:       chain,
					timeout:     tst.option.decideTimeout,
					stageBudget: resolveStageBudget(tst.option),
					forceSlow:   len(chain.projection.Tags) > 0,
				}
			}
		}
	}
	newPart, dropped, err := tst.mergeParts(tst.fileSystem, closeCh, parts, newPartID, tst.root, filter)
	if err != nil {
		return nil, err
	}
	elapsed := time.Since(start)
	tst.incTotalMergeLatency(elapsed.Seconds(), typ, lane)
	tst.incTotalMerged(1, typ, lane)
	tst.incTotalMergedParts(len(parts), typ, lane)
	if elapsed > 30*time.Second {
		var totalCount uint64
		for _, pw := range parts {
			totalCount += pw.p.partMetadata.TotalCount
		}
		tst.l.Warn().
			Uint64("beforeTotalCount", totalCount).
			Uint64("afterTotalCount", newPart.p.partMetadata.TotalCount).
			Int("beforePartCount", len(parts)).
			Dur("elapsed", elapsed).
			Msg("background merger takes too long")
	} else if snapshotCreatorMerger == creator && tst.l.Info().Enabled() && len(parts) > 2 {
		var minSize, maxSize, totalSize, totalCount uint64
		for _, pw := range parts {
			totalCount += pw.p.partMetadata.TotalCount
			totalSize += pw.p.partMetadata.CompressedSizeBytes
			if minSize == 0 || minSize > pw.p.partMetadata.CompressedSizeBytes {
				minSize = pw.p.partMetadata.CompressedSizeBytes
			}
			if maxSize < pw.p.partMetadata.CompressedSizeBytes {
				maxSize = pw.p.partMetadata.CompressedSizeBytes
			}
		}
		if totalSize > 10<<20 && minSize*uint64(len(parts)) < maxSize {
			// it's an unbalanced merge. but it's ok when the size is small.
			tst.l.Info().
				Str("beforeTotalCount", humanize.Comma(int64(totalCount))).
				Str("afterTotalCount", humanize.Comma(int64(newPart.p.partMetadata.TotalCount))).
				Int("beforePartCount", len(parts)).
				Str("minSize", humanize.IBytes(minSize)).
				Str("maxSize", humanize.IBytes(maxSize)).
				Dur("elapsedMS", elapsed).
				Msg("background merger merges unbalanced parts")
		}
	}
	partIDMap := make(map[uint64]struct{})
	for _, pw := range parts {
		partIDMap[pw.ID()] = struct{}{}
	}
	// When the core merge dropped any trace, prune the same trace ids from every
	// sibling sidx part via an opaque per-element predicate. The trace layer owns
	// the encoding (decodeTraceID); sidx stays encoding-agnostic. Undecodable
	// elements fail open (retain).
	var keepFn func([]byte) bool
	if len(dropped) > 0 {
		keepFn = func(data []byte) bool {
			id, decErr := decodeTraceID(data)
			if decErr != nil {
				return true
			}
			_, isDropped := dropped[id]
			return !isDropped
		}
	}
	mergerIntroductionMap := make(map[string]*sidx.MergerIntroduction)
	for sidxName, sidxInstance := range tst.getAllSidx() {
		start = time.Now()
		mergerIntroduction, mergeErr := sidxInstance.Merge(closeCh, partIDMap, newPartID, keepFn)
		if mergeErr != nil {
			tst.l.Warn().Err(mergeErr).Msg("sidx merge mem parts failed")
			tst.removeSidxPartOnFailure(sidxName, newPartID)
			tst.removeTracePartOnFailure(newPart)
			for doneSidxName, intro := range mergerIntroductionMap {
				intro.ReleaseNewPart()
				tst.removeSidxPartOnFailure(doneSidxName, newPartID)
				intro.Release()
			}
			return nil, mergeErr
		}
		if mergerIntroduction == nil {
			continue
		}
		mergerIntroductionMap[sidxName] = mergerIntroduction
		elapsed = time.Since(start)
		sidxTyp := fmt.Sprintf("%s_%s", typ, sidxName)
		tst.incTotalMergeLatency(elapsed.Seconds(), sidxTyp, lane)
		tst.incTotalMerged(1, sidxTyp, lane)
		tst.incTotalMergedParts(len(parts), sidxTyp, lane)
		if elapsed > 30*time.Second {
			tst.l.Warn().Int("mergedPartsCount", len(parts)).Str("sidxName", sidxName).Dur("elapsed", elapsed).Msg("sidx merge parts took too long")
		}
	}
	if len(mergerIntroductionMap) > 0 {
		defer func() {
			for _, mergerIntroduction := range mergerIntroductionMap {
				mergerIntroduction.Release()
			}
		}()
	}

	mi := generateMergerIntroduction()
	defer releaseMergerIntroduction(mi)
	mi.creator = creator
	mi.newPart = newPart
	mi.merged = merged
	mi.sidxMergerIntroduced = mergerIntroductionMap
	mi.applied = make(chan struct{})
	select {
	case merges <- mi:
	case <-tst.loopCloser.CloseNotify():
		return newPart, errClosed
	}
	<-mi.applied
	return newPart, nil
}

func (tst *tsTable) freeDiskSpace(path string) uint64 {
	free := tst.fileSystem.MustGetFreeSpace(path)
	reserved := atomic.LoadUint64(&reservedDiskSpace)
	if free < reserved {
		return 0
	}
	return free - reserved
}

func (tst *tsTable) tryReserveDiskSpace(n uint64) bool {
	available := tst.fileSystem.MustGetFreeSpace(tst.root)
	reserved := reserveDiskSpace(n)
	if available > reserved {
		return true
	}
	releaseDiskSpace(n)
	return false
}

func reserveDiskSpace(n uint64) uint64 {
	return atomic.AddUint64(&reservedDiskSpace, n)
}

func releaseDiskSpace(n uint64) {
	atomic.AddUint64(&reservedDiskSpace, ^(n - 1))
}

var reservedDiskSpace uint64

func (tst *tsTable) getPartsToMerge(snapshot *snapshot, freeDiskSize uint64, dst []*partWrapper) ([]*partWrapper, map[uint64]struct{}) {
	var parts []*partWrapper

	tst.inFlightMu.RLock()
	for _, pw := range snapshot.parts {
		if pw.mp != nil || pw.p.partMetadata.TotalCount < 1 {
			continue
		}
		if _, inFlight := tst.inFlight[pw.ID()]; inFlight {
			continue
		}
		parts = append(parts, pw)
	}
	tst.inFlightMu.RUnlock()

	dst = tst.option.mergePolicy.getPartsToMerge(dst, parts, freeDiskSize)
	if len(dst) == 0 {
		return nil, nil
	}

	toBeMerged := make(map[uint64]struct{})
	for _, pw := range dst {
		toBeMerged[pw.ID()] = struct{}{}
	}
	return dst, toBeMerged
}

func (tst *tsTable) reserveSpace(parts []*partWrapper) uint64 {
	var needSize uint64
	for i := range parts {
		needSize += parts[i].p.partMetadata.CompressedSizeBytes
	}
	if tst.tryReserveDiskSpace(needSize) {
		return needSize
	}
	return 0
}

var errNoPartToMerge = fmt.Errorf("no part to merge")

// removeTracePartOnFailure closes the part and removes its directory from disk.
// Used when a merge fails after the trace part was created so the directory is not left as trash.
func (tst *tsTable) removeTracePartOnFailure(pw *partWrapper) {
	if pw == nil {
		return
	}
	pathToRemove := pw.p.path
	pw.decRef()
	tst.fileSystem.MustRMAll(pathToRemove)
}

// sidxPartPath returns the on-disk path for a sidx part (same layout as sidx package).
func sidxPartPath(traceRoot, sidxName string, partID uint64) string {
	return filepath.Join(traceRoot, sidxDirName, sidxName, fmt.Sprintf("%016x", partID))
}

// removeSidxPartOnFailure removes a sidx part directory from disk.
// Used when a merge fails after one or more sidx parts were created.
func (tst *tsTable) removeSidxPartOnFailure(sidxName string, partID uint64) {
	pathToRemove := sidxPartPath(tst.root, sidxName, partID)
	tst.fileSystem.MustRMAll(pathToRemove)
}

func (tst *tsTable) mergeParts(fileSystem fs.FileSystem, closeCh <-chan struct{}, parts []*partWrapper, partID uint64, root string,
	filter *mergeFilter,
) (*partWrapper, map[string]struct{}, error) {
	if len(parts) == 0 {
		return nil, nil, errNoPartToMerge
	}
	dstPath := partPath(root, partID)
	var totalSize int64
	var traceSize uint64
	pii := make([]*partMergeIter, 0, len(parts))
	for i := range parts {
		pmi := generatePartMergeIter()
		pmi.mustInitFromPart(parts[i].p)
		pii = append(pii, pmi)
		totalSize += int64(parts[i].p.partMetadata.CompressedSizeBytes)
		traceSize += parts[i].p.partMetadata.BlocksCount
	}
	shouldCache := tst.pm.ShouldCache(totalSize)
	br := generateBlockReader()
	br.init(pii)
	bw := generateBlockWriter()
	bw.mustInitForFilePart(fileSystem, dstPath, shouldCache, int(traceSize))
	conflictTags := collectConflictTags(parts)

	var minTimestamp, maxTimestamp int64
	for i, pw := range parts {
		pm := pw.p.partMetadata
		if i == 0 {
			minTimestamp = pm.MinTimestamp
			maxTimestamp = pm.MaxTimestamp
			continue
		}
		if pm.MinTimestamp < minTimestamp {
			minTimestamp = pm.MinTimestamp
		}
		if pm.MaxTimestamp > maxTimestamp {
			maxTimestamp = pm.MaxTimestamp
		}
	}

	pm, tf, tt, dropped, err := mergeBlocks(closeCh, bw, br, conflictTags, filter)
	releaseBlockWriter(bw)
	releaseBlockReader(br)
	for i := range pii {
		releasePartMergeIter(pii[i])
	}
	if err != nil {
		return nil, nil, err
	}
	pm.MinTimestamp = minTimestamp
	pm.MaxTimestamp = maxTimestamp
	pm.mustWriteMetadata(fileSystem, dstPath)
	tf.mustWriteTraceIDFilter(fileSystem, dstPath)
	tf.reset()
	tt.mustWriteTagType(fileSystem, dstPath)
	// No SyncPath here: each mustWrite* helper goes through fileSystem.WriteAtomic
	// which already fsyncs the parent directory after rename. The last atomic
	// metadata write covers all prior dirent changes (data file creations).
	p := mustOpenFilePart(partID, root, fileSystem)
	return newPartWrapper(nil, p), dropped, nil
}

var errClosed = fmt.Errorf("the merger is closed")

// forceSlowMerge is used for testing to disable the fast raw merge path.
var forceSlowMerge = false

func collectConflictTags(parts []*partWrapper) map[string]struct{} {
	tagTypes := make(map[string]map[pbv1.ValueType]struct{})
	for _, pw := range parts {
		for tag, vt := range pw.p.tagType {
			t := decodeTypedTag(tag)
			if tagTypes[t] == nil {
				tagTypes[t] = make(map[pbv1.ValueType]struct{})
			}
			tagTypes[t][vt] = struct{}{}
		}
	}
	var result map[string]struct{}
	for tag, types := range tagTypes {
		if len(types) > 1 {
			if result == nil {
				result = make(map[string]struct{})
			}
			result[tag] = struct{}{}
		}
	}
	return result
}

// stagedTrace holds a deep copy of a trace's block(s) deferred for an ordered
// post-Decide write. When the hook is active EVERY trace is staged (in ascending
// traceID stream order) so the final writes stay ordered. A raw-fast-path
// trace carries its rawBlock pieces; a slow trace carries an allocated
// blockPointer (released after the write decision).
type stagedTrace struct {
	rawTags        map[string][]byte
	rawTagMetadata map[string][]byte
	slowBlock      *blockPointer
	traceID        string
	rawSpans       []byte
	rawBM          blockMetadata
	isRaw          bool
}

// mergeFilter carries the resolved in-merge retention hook state into
// mergeBlocks. When nil, mergeBlocks behaves exactly as before (no staging, no
// decode changes).
type mergeFilter struct {
	chain       *mergeChain
	timeout     time.Duration
	stageBudget uint64 // soft cap on staged bytes; a trace-boundary chunk flush fires once exceeded (0 disables chunking)
	forceSlow   bool   // forces the slow assembly path when the chain projects tags
}

// approxBytes estimates the deep-copied heap a staged trace holds so mergeBlocks
// can bound the total staged set rather than holding the whole merge in memory.
func (st *stagedTrace) approxBytes() uint64 {
	var n uint64
	if st.isRaw {
		n += uint64(len(st.rawSpans))
		for _, v := range st.rawTags {
			n += uint64(len(v))
		}
		for _, v := range st.rawTagMetadata {
			n += uint64(len(v))
		}
		return n
	}
	if st.slowBlock != nil {
		for _, s := range st.slowBlock.block.spans {
			n += uint64(len(s))
		}
		for i := range st.slowBlock.block.tags {
			for _, v := range st.slowBlock.block.tags[i].values {
				n += uint64(len(v))
			}
		}
	}
	return n
}

// isMergeHot reports true when any part being merged contains data written
// within graceNs of now. A hot merge means some traces may still have in-flight
// spans arriving in newer parts, so the caller should skip filter evaluation.
func isMergeHot(parts []*partWrapper, graceNs int64, now int64) bool {
	for _, pw := range parts {
		if pw.p.partMetadata.MaxTimestamp > now-graceNs {
			return true
		}
	}
	return false
}

// stageRawTrace deep-copies the shared rawBlk into a stagedTrace so the next
// mustReadRaw may overwrite rawBlk without corrupting the staged copy.
func stageRawTrace(rawBlk *rawBlock) stagedTrace {
	st := stagedTrace{
		isRaw:   true,
		traceID: rawBlk.bm.traceID,
	}
	st.rawBM.copyFrom(rawBlk.bm)
	if rawBlk.spans != nil {
		st.rawSpans = append([]byte(nil), rawBlk.spans...)
	}
	if len(rawBlk.tags) > 0 {
		st.rawTags = make(map[string][]byte, len(rawBlk.tags))
		for k, v := range rawBlk.tags {
			st.rawTags[k] = append([]byte(nil), v...)
		}
	}
	if len(rawBlk.tagMetadata) > 0 {
		st.rawTagMetadata = make(map[string][]byte, len(rawBlk.tagMetadata))
		for k, v := range rawBlk.tagMetadata {
			st.rawTagMetadata[k] = append([]byte(nil), v...)
		}
	}
	return st
}

// writeStagedKeep persists a kept staged trace from its own deep-copied bytes.
func writeStagedKeep(bw *blockWriter, st *stagedTrace) {
	if st.isRaw {
		rawBlk := rawBlock{
			bm:          &st.rawBM,
			tags:        st.rawTags,
			tagMetadata: st.rawTagMetadata,
			spans:       st.rawSpans,
		}
		bw.mustWriteRawBlock(&rawBlk)
		return
	}
	bw.mustWriteBlock(st.traceID, &st.slowBlock.block)
}

// flushStaged evaluates the staged traces through the chain and writes every
// staged trace back in its original (ascending traceID) order — traces are
// written only if the verdict keeps them — recording dropped trace ids in
// droppedSet. Allocated slow blocks are released.
// fail-open: any chain error retains the whole evaluated batch.
func flushStaged(bw *blockWriter, filter *mergeFilter, staged []stagedTrace, droppedSet map[string]struct{}) {
	if len(staged) == 0 {
		return
	}
	// Build the Decide batch: one TraceBlock per unique trace_id. A trace split
	// across multiple staged blocks (spans exceeding maxUncompressedSpanSize) is
	// decided once and dropped as a unit. The first staged block of each trace
	// assembles its batch entry.
	var batch sdk.TraceBatch
	batchTraceIDs := make([]string, 0, len(staged))
	seenEval := make(map[string]struct{})
	for i := range staged {
		if _, seen := seenEval[staged[i].traceID]; seen {
			continue
		}
		seenEval[staged[i].traceID] = struct{}{}
		batchTraceIDs = append(batchTraceIDs, staged[i].traceID)
		if staged[i].isRaw {
			batch.Traces = append(batch.Traces, assembleRawTraceBlock(staged[i].traceID, &staged[i].rawBM))
		} else {
			batch.Traces = append(batch.Traces, assembleTraceBlock(staged[i].traceID, staged[i].slowBlock, filter.chain.projection))
		}
	}
	dropMature := make(map[string]struct{})
	if len(batch.Traces) > 0 {
		verdict, execErr := filter.chain.Execute(&batch, filter.timeout)
		keepMask := verdict.Keep
		if execErr == nil && len(keepMask) == len(batchTraceIDs) {
			for j, traceID := range batchTraceIDs {
				if !keepMask[j] {
					dropMature[traceID] = struct{}{}
				}
			}
		}
	}
	for i := range staged {
		if _, isDropped := dropMature[staged[i].traceID]; isDropped {
			droppedSet[staged[i].traceID] = struct{}{}
		} else {
			writeStagedKeep(bw, &staged[i])
		}
		if !staged[i].isRaw && staged[i].slowBlock != nil {
			releaseBlockPointer(staged[i].slowBlock)
			staged[i].slowBlock = nil
		}
	}
}

// rawFastPathEligible reports whether the current block can be copied raw
// (without unmarshaling): raw merge is not force-disabled, the active retention
// filter does not require the slow tag-projecting path, and this is the only
// block for its traceID (the next block, if any, has a different traceID).
func rawFastPathEligible(filter *mergeFilter, nextB, b *blockPointer) bool {
	if forceSlowMerge {
		return false
	}
	if filter != nil && filter.forceSlow {
		return false
	}
	return nextB == nil || nextB.bm.traceID != b.bm.traceID
}

func mergeBlocks(closeCh <-chan struct{}, bw *blockWriter, br *blockReader, conflictTags map[string]struct{},
	filter *mergeFilter,
) (*partMetadata, *traceIDFilter, *tagType, map[string]struct{}, error) {
	pendingBlockIsEmpty := true
	pendingBlock := generateBlockPointer()
	defer releaseBlockPointer(pendingBlock)
	var tmpBlock *blockPointer
	var decoder *encoding.BytesBlockDecoder
	var rawBlk rawBlock
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
	loadAndRename := func() {
		br.loadBlockData(getDecoder())
		renameConflictTags(&br.block.block, conflictTags)
	}
	readAndRename := func(bm *blockMetadata) {
		br.mustReadRaw(&rawBlk, bm)
		renameRawConflictTags(&rawBlk, conflictTags)
	}
	var staged []stagedTrace
	var stagedBytes uint64
	var lastStagedTraceID string
	var droppedSet map[string]struct{}
	if filter != nil {
		droppedSet = make(map[string]struct{})
	}
	stage := func(st stagedTrace) {
		staged = append(staged, st)
		stagedBytes += st.approxBytes()
		lastStagedTraceID = st.traceID
	}
	// flushChunk decides + writes the currently staged traces, then frees them.
	// It is invoked at trace boundaries once stagedBytes exceeds stageBudget, and
	// once more at end-of-merge for the remainder. droppedSet accumulates across
	// chunks so the downstream sidx pruning still sees every dropped trace id.
	flushChunk := func() {
		if len(staged) == 0 {
			return
		}
		flushStaged(bw, filter, staged, droppedSet)
		staged = nil
		stagedBytes = 0
	}
	// writeRawBlock writes the just-read rawBlk. When the hook is inactive it
	// writes immediately (byte-identical to the legacy path). When active the
	// trace is staged (in ascending traceID stream order) for an ordered,
	// post-Decide flush. Staging keeps the final block writes in ascending
	// traceID order (mustWriteBlock requires it); the chunked flush bounds heap.
	writeRawBlock := func() {
		if filter == nil {
			bw.mustWriteRawBlock(&rawBlk)
			return
		}
		stage(stageRawTrace(&rawBlk))
	}
	// writeSlowBlock writes (or, on the active hook path, stages) an accumulated
	// slow-path block.
	writeSlowBlock := func(bp *blockPointer) {
		if filter == nil {
			bw.mustWriteBlock(bp.bm.traceID, &bp.block)
			return
		}
		newBP := generateBlockPointer()
		newBP.copyFrom(bp)
		// copyFrom copies slice headers only; the bytes alias the decoder's
		// internal buffer which is reset when the next trace is loaded.
		newBP.block.deepCopyValues()
		stage(stagedTrace{
			traceID:   bp.bm.traceID,
			slowBlock: newBP,
		})
	}
	for br.nextBlockMetadata() {
		select {
		case <-closeCh:
			return nil, nil, nil, nil, errClosed
		default:
		}
		b := br.block
		// Bounded staging: at a trace boundary (the current block belongs to a
		// different traceID than the last staged one), once staged bytes exceed
		// the budget, decide+write the accumulated chunk before reading further.
		// The boundary guard guarantees a trace's blocks are never split across
		// chunks, so each trace stays a single Decide unit and the chunk write
		// order remains ascending. A single trace larger than the budget is still
		// held whole (a trace is an indivisible decision), so the cap is a soft
		// per-merge bound, not a hard ceiling.
		if filter != nil && filter.stageBudget > 0 && stagedBytes >= filter.stageBudget &&
			len(staged) > 0 && b.bm.traceID != lastStagedTraceID {
			flushChunk()
		}
		// Fast path: if this is the only block for this traceID AND we have no pending block,
		// copy it raw without unmarshaling
		nextB := br.peek()
		if pendingBlockIsEmpty && rawFastPathEligible(filter, nextB, b) {
			// fast path: only a single block for the trace id and no pending data
			readAndRename(&b.bm)
			writeRawBlock()
			continue
		}

		if pendingBlockIsEmpty {
			loadAndRename()
			pendingBlock.copyFrom(b)
			pendingBlockIsEmpty = false
			continue
		}

		if pendingBlock.bm.traceID != b.bm.traceID || pendingBlock.block.spanSize() >= maxUncompressedSpanSize {
			writeSlowBlock(pendingBlock)
			releaseDecoder()
			pendingBlock.reset()
			// After writing the pending block, check if the new block can be copied raw
			// This is the same fast path check as at the beginning of the loop
			nextB = br.peek()
			if rawFastPathEligible(filter, nextB, b) {
				// fast path: only a single block for this new trace id
				readAndRename(&b.bm)
				writeRawBlock()
				continue
			}
			// Slow path: start accumulating the new block
			loadAndRename()
			pendingBlock.copyFrom(b)
			pendingBlockIsEmpty = false
			continue
		}

		if tmpBlock == nil {
			tmpBlock = generateBlockPointer()
			defer releaseBlockPointer(tmpBlock)
		}
		tmpBlock.reset()
		tmpBlock.bm.traceID = b.bm.traceID
		loadAndRename()
		mergeTwoBlocks(tmpBlock, pendingBlock, b)
		if tmpBlock.block.spanSize() <= maxUncompressedSpanSize {
			if len(tmpBlock.spans) == 0 {
				pendingBlockIsEmpty = true
			}
			pendingBlock, tmpBlock = tmpBlock, pendingBlock
			continue
		}
		writeSlowBlock(tmpBlock)
		releaseDecoder()
		pendingBlock.reset()
		tmpBlock.reset()
		pendingBlockIsEmpty = true
	}
	if err := br.error(); err != nil {
		return nil, nil, nil, nil, fmt.Errorf("cannot read block to merge: %w", err)
	}
	if !pendingBlockIsEmpty {
		writeSlowBlock(pendingBlock)
	}
	releaseDecoder()
	if filter != nil {
		flushChunk()
	}
	var pm partMetadata
	var tf traceIDFilter
	tt := make(tagType)
	bw.Flush(&pm, &tf, &tt)
	if len(droppedSet) == 0 {
		droppedSet = nil
	}
	return &pm, &tf, &tt, droppedSet, nil
}

func mergeTwoBlocks(target, left, right *blockPointer) {
	target.appendAll(left)
	target.appendAll(right)
}

func renameConflictTags(b *block, conflictTags map[string]struct{}) {
	if len(conflictTags) == 0 {
		return
	}
	for i := range b.tags {
		if _, ok := conflictTags[b.tags[i].name]; ok {
			b.tags[i].name = encodeTypedTag(b.tags[i].name, b.tags[i].valueType)
		}
	}
}

func renameRawConflictTags(r *rawBlock, conflictTags map[string]struct{}) {
	if len(conflictTags) == 0 {
		return
	}
	bm := r.bm
	for tag := range conflictTags {
		if _, ok := bm.tags[tag]; !ok {
			continue
		}
		valueType := bm.tagType[tag]
		typedTag := encodeTypedTag(tag, valueType)
		bm.tags[typedTag] = bm.tags[tag]
		delete(bm.tags, tag)
		bm.tagType[typedTag] = valueType
		delete(bm.tagType, tag)
		if rawData, ok := r.tags[tag]; ok {
			r.tags[typedTag] = rawData
			delete(r.tags, tag)
		}
		if rawMeta, ok := r.tagMetadata[tag]; ok {
			r.tagMetadata[typedTag] = rawMeta
			delete(r.tagMetadata, tag)
		}
	}
}
