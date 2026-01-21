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
	"fmt"

	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/fs"
)

var (
	errNoPartToMerge = fmt.Errorf("no part to merge")
	errClosed        = fmt.Errorf("the merger is closed")
)

// Merge implements Merger interface.
func (s *sidx) Merge(closeCh <-chan struct{}, partIDtoMerge map[uint64]struct{}, newPartID uint64) (*MergerIntroduction, error) {
	// Get current snapshot
	snap := s.currentSnapshot()
	if snap == nil {
		return nil, nil
	}
	defer snap.decRef()

	// Select parts to merge (all active non-memory parts)
	var partsToMerge []*partWrapper
	for _, pw := range snap.parts {
		if _, ok := partIDtoMerge[pw.ID()]; ok {
			partsToMerge = append(partsToMerge, pw)
		}
	}
	if d := s.l.Debug(); d.Enabled() {
		if len(partsToMerge) != len(partIDtoMerge) {
			d.Int("parts_to_merge_count", len(partsToMerge)).
				Int("part_ids_to_merge_count", len(partIDtoMerge)).
				Str("root", s.root).
				Msg("parts to merge count does not match part ids to merge count")
		}
	}

	// Create new merged part
	newPart, err := s.mergeParts(s.fileSystem, closeCh, partsToMerge, newPartID, s.root)
	if err != nil {
		return nil, err
	}

	// Create merge introduction
	mergeIntro := generateMergerIntroduction()

	// Mark parts for merging
	for _, pw := range partsToMerge {
		mergeIntro.merged[pw.ID()] = struct{}{}
	}

	mergeIntro.newPart = newPart
	return mergeIntro, nil
}

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
	p := mustOpenPart(partID, dstPath, fileSystem)

	return newPartWrapper(nil, p), nil
}

func mergeBlocks(closeCh <-chan struct{}, bw *blockWriter, br *blockReader) (*partMetadata, error) {
	pendingBlockIsEmpty := true
	pendingBlock := generateBlockPointer()
	defer releaseBlockPointer(pendingBlock)
	var tmpBlock *blockPointer
	var decoder *encoding.BytesBlockDecoder
	getDecoder := func() *encoding.BytesBlockDecoder {
		if decoder == nil {
			decoder = generateTagValuesDecoder()
		}
		return decoder
	}
	releaseDecoder := func() {
		if decoder != nil {
			releaseTagValuesDecoder(decoder)
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
			(pendingBlock.isFull() && pendingBlock.bm.maxKey <= b.bm.minKey) ||
			pendingBlock.block.uncompressedSizeBytes() >= maxUncompressedBlockSize {
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
		if len(tmpBlock.userKeys) <= maxBlockLength && tmpBlock.block.uncompressedSizeBytes() <= maxUncompressedBlockSize {
			if len(tmpBlock.userKeys) == 0 {
				pendingBlockIsEmpty = true
			}
			pendingBlock, tmpBlock = tmpBlock, pendingBlock
			continue
		}
		bw.mustWriteBlock(tmpBlock.bm.seriesID, &tmpBlock.block)
		releaseDecoder()
		pendingBlock.reset()
		tmpBlock.reset()
		pendingBlockIsEmpty = true
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
