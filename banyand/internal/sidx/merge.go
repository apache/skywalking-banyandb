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
	"io"

	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
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
	if len(partsToMerge) == 0 {
		return nil, nil
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
	conflictTags := collectConflictTags(parts)
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

	pm, err := mergeBlocks(closeCh, bw, br, conflictTags)
	releaseBlockWriter(bw)
	releaseBlockReader(br)
	for i := range pii {
		releasePartMergeIter(pii[i])
	}
	if err != nil {
		return nil, err
	}
	// Aggregate optional timestamp range from merged parts
	var minVal, maxVal int64
	var hasMinTS, hasMaxTS bool
	for i := range parts {
		p := parts[i].p.partMetadata
		if p.MinTimestamp != nil {
			if !hasMinTS || *p.MinTimestamp < minVal {
				minVal = *p.MinTimestamp
				hasMinTS = true
			}
		}
		if p.MaxTimestamp != nil {
			if !hasMaxTS || *p.MaxTimestamp > maxVal {
				maxVal = *p.MaxTimestamp
				hasMaxTS = true
			}
		}
	}
	if hasMinTS && hasMaxTS {
		pm.MinTimestamp = &minVal
		pm.MaxTimestamp = &maxVal
	}
	pm.mustWriteMetadata(fileSystem, dstPath)
	fileSystem.SyncPath(dstPath)
	p := mustOpenPart(partID, dstPath, fileSystem)

	return newPartWrapper(nil, p), nil
}

func mergeBlocks(closeCh <-chan struct{}, bw *blockWriter, br *blockReader, conflictTags map[string]struct{}) (*partMetadata, error) {
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
	loadAndRename := func() {
		br.loadBlockData(getDecoder())
		renameConflictTags(&br.block.block, conflictTags)
	}
	for br.nextBlockMetadata() {
		select {
		case <-closeCh:
			return nil, errClosed
		default:
		}
		b := br.block

		if pendingBlockIsEmpty {
			loadAndRename()
			pendingBlock.copyFrom(b)
			pendingBlockIsEmpty = false
			continue
		}

		if pendingBlock.bm.seriesID != b.bm.seriesID ||
			(pendingBlock.isFull() && pendingBlock.bm.maxKey <= b.bm.minKey) ||
			pendingBlock.block.uncompressedSizeBytes() >= maxUncompressedBlockSize {
			bw.mustWriteBlock(pendingBlock.bm.seriesID, &pendingBlock.block)
			releaseDecoder()
			loadAndRename()
			pendingBlock.copyFrom(b)
			continue
		}

		if tmpBlock == nil {
			tmpBlock = generateBlockPointer()
			defer releaseBlockPointer(tmpBlock)
		}
		tmpBlock.reset()
		tmpBlock.bm.seriesID = b.bm.seriesID
		loadAndRename()
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

func collectConflictTags(parts []*partWrapper) map[string]struct{} {
	tagTypes := make(map[string]map[pbv1.ValueType]struct{})
	for _, pw := range parts {
		p := pw.p
		for tt := range p.tagMetadata {
			t := tt
			var vt pbv1.ValueType
			if hasTypeSuffix(tt) {
				t = decodeTypedTag(tt)
				vt = valueType(tt)
			} else {
				var readErr error
				vt, readErr = readFirstTagValueType(p.tagMetadata[tt])
				if readErr != nil {
					continue
				}
			}
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

func readFirstTagValueType(tmReader fs.Reader) (pbv1.ValueType, error) {
	sr := tmReader.SequentialRead()
	defer fs.MustClose(sr)
	buf := make([]byte, 512)
	n, readErr := io.ReadFull(sr, buf)
	if readErr != nil && n == 0 {
		return pbv1.ValueTypeUnknown, fmt.Errorf("cannot read tag metadata: %w", readErr)
	}
	tm := generateTagMetadata()
	defer releaseTagMetadata(tm)
	if unmarshalErr := tm.unmarshal(buf[:n]); unmarshalErr != nil {
		return pbv1.ValueTypeUnknown, fmt.Errorf("cannot unmarshal tag metadata: %w", unmarshalErr)
	}
	return tm.valueType, nil
}

func renameConflictTags(b *block, conflictTags map[string]struct{}) {
	if len(conflictTags) == 0 {
		return
	}
	for t := range conflictTags {
		td, exists := b.tags[t]
		if !exists {
			continue
		}
		typedName := encodeTypedTag(t, td.valueType)
		td.name = typedName
		b.tags[typedName] = td
		delete(b.tags, t)
	}
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
