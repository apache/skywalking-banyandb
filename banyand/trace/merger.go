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
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"

	"github.com/apache/skywalking-banyandb/pkg/cgroups"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/watcher"
)

var mergeMaxConcurrencyCh = make(chan struct{}, cgroups.CPUs())

func (tst *tsTable) mergeLoop(merges chan *mergerIntroduction, flusherNotifier watcher.Channel) {
	defer tst.loopCloser.Done()

	var epoch uint64

	ew := flusherNotifier.Add(0, tst.loopCloser.CloseNotify())
	if ew == nil {
		return
	}

	var pwsChunk []*partWrapper

	for {
		select {
		case <-tst.loopCloser.CloseNotify():
			return
		case <-ew.Watch():
			if func() bool {
				curSnapshot := tst.currentSnapshot()
				if curSnapshot == nil {
					return false
				}
				defer curSnapshot.decRef()
				if curSnapshot.epoch != epoch {
					select {
					case mergeMaxConcurrencyCh <- struct{}{}:
						defer func() {
							<-mergeMaxConcurrencyCh
						}()
					case <-tst.loopCloser.CloseNotify():
						return true
					}
					tst.incTotalMergeLoopStarted(1)
					defer tst.incTotalMergeLoopFinished(1)
					var err error
					if pwsChunk, err = tst.mergeSnapshot(curSnapshot, merges, pwsChunk[:0]); err != nil {
						if errors.Is(err, errClosed) {
							return true
						}
						tst.l.Logger.Warn().Err(err).Msgf("cannot merge snapshot: %d", curSnapshot.epoch)
						tst.incTotalMergeLoopErr(1)
						return false
					}
					epoch = curSnapshot.epoch
				}
				ew = flusherNotifier.Add(epoch, tst.loopCloser.CloseNotify())
				return ew == nil
			}() {
				return
			}
		}
	}
}

func (tst *tsTable) mergeSnapshot(curSnapshot *snapshot, merges chan *mergerIntroduction, dst []*partWrapper) ([]*partWrapper, error) {
	freeDiskSize := tst.freeDiskSpace(tst.root)
	var toBeMerged map[uint64]struct{}
	dst, toBeMerged = tst.getPartsToMerge(curSnapshot, freeDiskSize, dst)
	if len(dst) < 2 {
		return nil, nil
	}
	if _, err := tst.mergePartsThenSendIntroduction(snapshotCreatorMerger, dst,
		toBeMerged, merges, tst.loopCloser.CloseNotify(), "file"); err != nil {
		return dst, err
	}
	return dst, nil
}

func (tst *tsTable) mergePartsThenSendIntroduction(creator snapshotCreator, parts []*partWrapper, merged map[uint64]struct{}, merges chan *mergerIntroduction,
	closeCh <-chan struct{}, typ string,
) (*partWrapper, error) {
	reservedSpace := tst.reserveSpace(parts)
	defer releaseDiskSpace(reservedSpace)
	start := time.Now()
	newPart, err := tst.mergeParts(tst.fileSystem, closeCh, parts, atomic.AddUint64(&tst.curPartID, 1), tst.root)
	if err != nil {
		return nil, err
	}
	elapsed := time.Since(start)
	tst.incTotalMergeLatency(elapsed.Seconds(), typ)
	tst.incTotalMerged(1, typ)
	tst.incTotalMergedParts(len(parts), typ)
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

	mi := generateMergerIntroduction()
	defer releaseMergerIntroduction(mi)
	mi.creator = creator
	mi.newPart = newPart
	mi.merged = merged
	mi.applied = make(chan struct{})
	select {
	case merges <- mi:
	case <-tst.loopCloser.CloseNotify():
		return newPart, errClosed
	}
	select {
	case <-mi.applied:
	case <-tst.loopCloser.CloseNotify():
		return newPart, errClosed
	}
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

	for _, pw := range snapshot.parts {
		if pw.mp != nil || pw.p.partMetadata.TotalCount < 1 {
			continue
		}
		parts = append(parts, pw)
	}

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

func (tst *tsTable) mergeParts(fileSystem fs.FileSystem, closeCh <-chan struct{}, parts []*partWrapper, partID uint64, root string) (*partWrapper, error) {
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
	shouldCache := tst.pm.ShouldCache(totalSize)
	br := generateBlockReader()
	br.init(pii)
	bw := generateBlockWriter()
	bw.mustInitForFilePart(fileSystem, dstPath, shouldCache)
	for _, pw := range parts {
		for _, pbm := range pw.p.primaryBlockMetadata {
			if len(pbm.traceID) > int(bw.traceIDLen) {
				bw.traceIDLen = uint32(len(pbm.traceID))
			}
		}
	}

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

	pm, tf, tt, err := mergeBlocks(closeCh, bw, br)
	pm.MinTimestamp = minTimestamp
	pm.MaxTimestamp = maxTimestamp
	releaseBlockWriter(bw)
	releaseBlockReader(br)
	for i := range pii {
		releasePartMergeIter(pii[i])
	}
	if err != nil {
		return nil, err
	}
	pm.mustWriteMetadata(fileSystem, dstPath)
	tf.mustWriteTraceIDFilter(fileSystem, dstPath)
	tt.mustWriteTagType(fileSystem, dstPath)
	fileSystem.SyncPath(dstPath)
	p := mustOpenFilePart(partID, root, fileSystem)

	return newPartWrapper(nil, p), nil
}

var errClosed = fmt.Errorf("the merger is closed")

func mergeBlocks(closeCh <-chan struct{}, bw *blockWriter, br *blockReader) (*partMetadata, *traceIDFilter, *tagType, error) {
	pendingBlockIsEmpty := true
	pendingBlock := generateBlockPointer()
	defer releaseBlockPointer(pendingBlock)
	var tmpBlock *blockPointer
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
			return nil, nil, nil, errClosed
		default:
		}
		b := br.block

		if pendingBlockIsEmpty {
			br.loadBlockData(getDecoder())
			pendingBlock.copyFrom(b)
			pendingBlockIsEmpty = false
			continue
		}

		if pendingBlock.bm.traceID != b.bm.traceID || pendingBlock.block.spanSize() >= maxUncompressedSpanSize {
			bw.mustWriteBlock(pendingBlock.bm.traceID, &pendingBlock.block)
			releaseDecoder()
			pendingBlock.reset()
			br.loadBlockData(getDecoder())
			pendingBlock.copyFrom(b)
			continue
		}

		if tmpBlock == nil {
			tmpBlock = generateBlockPointer()
			defer releaseBlockPointer(tmpBlock)
		}
		tmpBlock.reset()
		tmpBlock.bm.traceID = b.bm.traceID
		br.loadBlockData(getDecoder())
		mergeTwoBlocks(tmpBlock, pendingBlock, b)
		if tmpBlock.block.spanSize() <= maxUncompressedSpanSize {
			if len(tmpBlock.spans) == 0 {
				pendingBlockIsEmpty = true
			}
			pendingBlock, tmpBlock = tmpBlock, pendingBlock
			continue
		}
		bw.mustWriteBlock(tmpBlock.bm.traceID, &tmpBlock.block)
		releaseDecoder()
		pendingBlock.reset()
		tmpBlock.reset()
		pendingBlockIsEmpty = true
	}
	if err := br.error(); err != nil {
		return nil, nil, nil, fmt.Errorf("cannot read block to merge: %w", err)
	}
	if !pendingBlockIsEmpty {
		bw.mustWriteBlock(pendingBlock.bm.traceID, &pendingBlock.block)
	}
	releaseDecoder()
	var pm partMetadata
	var tf traceIDFilter
	tt := make(tagType)
	bw.Flush(&pm, &tf, &tt)
	return &pm, &tf, &tt, nil
}

func mergeTwoBlocks(target, left, right *blockPointer) {
	target.appendAll(left)
	target.appendAll(right)
}
