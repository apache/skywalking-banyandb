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
	"errors"
	"fmt"
	"io"
	"sort"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/compress/zstd"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/pool"
)

// lessByKey compares two blockMetadata by key range, then by seriesID and finally by data block offset.
func (bm *blockMetadata) lessByKey(other *blockMetadata) bool {
	if bm.minKey != other.minKey {
		return bm.minKey < other.minKey
	}
	if bm.maxKey != other.maxKey {
		return bm.maxKey < other.maxKey
	}
	if bm.seriesID != other.seriesID {
		return bm.seriesID < other.seriesID
	}
	return bm.dataBlock.offset < other.dataBlock.offset
}

type blockRef struct {
	primaryIdx int
	blockIdx   int
	seriesID   common.SeriesID
	minKey     int64
	maxKey     int64
}

type seriesCursor struct {
	iter      *partKeyIter
	refs      []blockRef
	curBlock  blockMetadata
	seriesID  common.SeriesID
	refIdx    int
	curLoaded bool
}

func (sc *seriesCursor) init(iter *partKeyIter, sid common.SeriesID, refs []blockRef) {
	sc.reset()
	sc.iter = iter
	sc.seriesID = sid
	// Reuse underlying slice when possible
	if cap(sc.refs) < len(refs) {
		sc.refs = make([]blockRef, len(refs))
		copy(sc.refs, refs)
	} else {
		sc.refs = sc.refs[:len(refs)]
		copy(sc.refs, refs)
	}
}

func (sc *seriesCursor) reset() {
	sc.iter = nil
	sc.seriesID = 0
	sc.refIdx = 0
	if sc.curLoaded {
		sc.curBlock.reset()
	}
	sc.curLoaded = false
	sc.refs = sc.refs[:0]
}

func (sc *seriesCursor) current() *blockMetadata {
	if !sc.curLoaded {
		return nil
	}
	return &sc.curBlock
}

func (sc *seriesCursor) advance() (bool, error) {
	if sc.iter == nil {
		return false, nil
	}
	for sc.refIdx < len(sc.refs) {
		ref := sc.refs[sc.refIdx]
		sc.refIdx++
		bma, err := sc.iter.ensurePrimaryBlocks(ref.primaryIdx)
		if err != nil {
			return false, err
		}
		if ref.blockIdx >= len(bma.arr) {
			return false, fmt.Errorf("block index %d out of range for primary %d", ref.blockIdx, ref.primaryIdx)
		}
		bm := &bma.arr[ref.blockIdx]
		if bm.maxKey < sc.iter.minKey || bm.minKey > sc.iter.maxKey {
			continue
		}
		if sc.iter.blockFilter != nil {
			shouldSkip, err := sc.iter.shouldSkipBlock(bm)
			if err != nil {
				return false, err
			}
			if shouldSkip {
				continue
			}
		}
		sc.curBlock.copyFrom(bm)
		sc.curLoaded = true
		return true, nil
	}
	sc.curLoaded = false
	return false, nil
}

type seriesCursorHeap []*seriesCursor

func (sch *seriesCursorHeap) Len() int {
	return len(*sch)
}

func (sch *seriesCursorHeap) Less(i, j int) bool {
	x := *sch
	ci := x[i].current()
	cj := x[j].current()
	if ci == nil {
		return false
	}
	if cj == nil {
		return true
	}
	return ci.lessByKey(cj)
}

func (sch *seriesCursorHeap) Swap(i, j int) {
	x := *sch
	x[i], x[j] = x[j], x[i]
}

func (sch *seriesCursorHeap) Push(x any) {
	*sch = append(*sch, x.(*seriesCursor))
}

func (sch *seriesCursorHeap) Pop() any {
	a := *sch
	v := a[len(a)-1]
	*sch = a[:len(a)-1]
	return v
}

type partKeyIter struct {
	err                  error
	blockFilter          index.Filter
	sidSet               map[common.SeriesID]struct{}
	p                    *part
	primaryCache         map[int]*blockMetadataArray
	sids                 []common.SeriesID
	curBlocks            []*blockMetadata
	cursorPool           []seriesCursor
	cursorHeap           seriesCursorHeap
	compressedPrimaryBuf []byte
	primaryBuf           []byte
	minKey               int64
	maxKey               int64
}

func (pki *partKeyIter) releaseCurBlocks() {
	for i := range pki.curBlocks {
		releaseBlockMetadata(pki.curBlocks[i])
		pki.curBlocks[i] = nil
	}
	pki.curBlocks = pki.curBlocks[:0]
}

func (pki *partKeyIter) reset() {
	pki.err = nil
	pki.p = nil
	pki.minKey = 0
	pki.maxKey = 0
	pki.blockFilter = nil

	pki.releaseCurBlocks()

	for i := range pki.cursorHeap {
		pki.cursorHeap[i].reset()
	}
	pki.cursorHeap = pki.cursorHeap[:0]

	for i := range pki.cursorPool {
		pki.cursorPool[i].reset()
	}
	pki.cursorPool = pki.cursorPool[:0]

	for idx, cache := range pki.primaryCache {
		if cache != nil {
			releaseBlockMetadataArray(cache)
			pki.primaryCache[idx] = nil
		}
	}
	if pki.primaryCache != nil {
		clear(pki.primaryCache)
	}

	if pki.sidSet != nil {
		clear(pki.sidSet)
	}
	pki.sids = pki.sids[:0]

	pki.compressedPrimaryBuf = pki.compressedPrimaryBuf[:0]
	pki.primaryBuf = pki.primaryBuf[:0]
}

func (pki *partKeyIter) init(p *part, sids []common.SeriesID, minKey, maxKey int64, blockFilter index.Filter) {
	pki.reset()
	pki.p = p
	pki.minKey = minKey
	pki.maxKey = maxKey
	pki.blockFilter = blockFilter

	if len(sids) == 0 {
		pki.err = io.EOF
		return
	}

	pki.sids = append(pki.sids[:0], sids...)
	sort.Slice(pki.sids, func(i, j int) bool {
		return pki.sids[i] < pki.sids[j]
	})

	if pki.sidSet == nil {
		pki.sidSet = make(map[common.SeriesID]struct{}, len(pki.sids))
	} else {
		clear(pki.sidSet)
	}
	for _, sid := range pki.sids {
		pki.sidSet[sid] = struct{}{}
	}

	maxSID := pki.sids[len(pki.sids)-1]
	minSID := pki.sids[0]

	seriesRefs := make(map[common.SeriesID][]blockRef, len(pki.sids))

	for idx := range p.primaryBlockMetadata {
		pbm := &p.primaryBlockMetadata[idx]

		if pbm.seriesID > maxSID {
			break
		}

		if pbm.maxKey < pki.minKey || pbm.minKey > pki.maxKey {
			continue
		}

		bma, err := pki.ensurePrimaryBlocks(idx)
		if err != nil {
			pki.err = fmt.Errorf("cannot load primary block metadata: %w", err)
			return
		}
		if len(bma.arr) == 0 {
			continue
		}

		if bma.arr[len(bma.arr)-1].seriesID < minSID {
			continue
		}

		lastSeries := bma.arr[len(bma.arr)-1].seriesID
		for _, sid := range pki.sids {
			if sid < pbm.seriesID {
				continue
			}
			if sid > lastSeries {
				continue
			}

			start := sort.Search(len(bma.arr), func(i int) bool {
				return bma.arr[i].seriesID >= sid
			})
			if start == len(bma.arr) || bma.arr[start].seriesID != sid {
				continue
			}

			for i := start; i < len(bma.arr) && bma.arr[i].seriesID == sid; i++ {
				bm := &bma.arr[i]
				if bm.maxKey < pki.minKey || bm.minKey > pki.maxKey {
					continue
				}
				if _, ok := pki.sidSet[bm.seriesID]; !ok {
					continue
				}
				seriesRefs[sid] = append(seriesRefs[sid], blockRef{
					primaryIdx: idx,
					blockIdx:   i,
					seriesID:   sid,
					minKey:     bm.minKey,
					maxKey:     bm.maxKey,
				})
			}
		}
	}

	activeSeries := 0
	for _, sid := range pki.sids {
		if refs := seriesRefs[sid]; len(refs) > 0 {
			activeSeries++
		}
	}

	if activeSeries == 0 {
		pki.err = io.EOF
		return
	}

	if n := activeSeries - cap(pki.cursorPool); n > 0 {
		pki.cursorPool = append(pki.cursorPool[:cap(pki.cursorPool)], make([]seriesCursor, n)...)
	}
	pki.cursorPool = pki.cursorPool[:activeSeries]

	pki.cursorHeap = pki.cursorHeap[:0]
	cursorIdx := 0
	for _, sid := range pki.sids {
		refs := seriesRefs[sid]
		if len(refs) == 0 {
			continue
		}
		cursor := &pki.cursorPool[cursorIdx]
		cursorIdx++
		cursor.init(pki, sid, refs)
		ok, err := cursor.advance()
		if err != nil {
			pki.err = fmt.Errorf("cannot initialize cursor for series %d: %w", sid, err)
			return
		}
		if !ok {
			cursor.reset()
			continue
		}
		pki.cursorHeap = append(pki.cursorHeap, cursor)
	}
	pki.cursorPool = pki.cursorPool[:cursorIdx]

	if len(pki.cursorHeap) == 0 {
		pki.err = io.EOF
		return
	}
	heap.Init(&pki.cursorHeap)
}

func (pki *partKeyIter) nextBlock() bool {
	if pki.err != nil {
		return false
	}

	pki.releaseCurBlocks()

	if len(pki.cursorHeap) == 0 {
		pki.err = io.EOF
		return false
	}

	var (
		requeue     []*seriesCursor
		boundary    int64
		boundarySet bool
	)

	processCursor := func(cursor *seriesCursor) error {
		current := cursor.current()
		if current == nil {
			cursor.reset()
			return fmt.Errorf("series cursor %d has no current block", cursor.seriesID)
		}

		bmCopy := generateBlockMetadata()
		bmCopy.copyFrom(current)
		pki.curBlocks = append(pki.curBlocks, bmCopy)

		if !boundarySet || current.maxKey > boundary {
			boundary = current.maxKey
			boundarySet = true
		}

		ok, err := cursor.advance()
		if err != nil {
			cursor.reset()
			return err
		}
		if ok {
			requeue = append(requeue, cursor)
		} else {
			cursor.reset()
		}
		return nil
	}

	for {
		cursor := heap.Pop(&pki.cursorHeap).(*seriesCursor)

		if err := processCursor(cursor); err != nil {
			pki.releaseCurBlocks()
			pki.err = err
			return false
		}

		if len(pki.cursorHeap) == 0 {
			break
		}

		next := pki.cursorHeap[0].current()
		if next == nil {
			pki.releaseCurBlocks()
			pki.err = fmt.Errorf("series cursor %d has no current block", pki.cursorHeap[0].seriesID)
			return false
		}

		if boundarySet && next.minKey > boundary {
			break
		}
	}

	for _, cursor := range requeue {
		heap.Push(&pki.cursorHeap, cursor)
	}

	if len(pki.curBlocks) == 0 {
		pki.err = io.EOF
		return false
	}
	return true
}

func (pki *partKeyIter) error() error {
	if errors.Is(pki.err, io.EOF) {
		return nil
	}
	return pki.err
}

func (pki *partKeyIter) ensurePrimaryBlocks(primaryIdx int) (*blockMetadataArray, error) {
	if pki.primaryCache == nil {
		pki.primaryCache = make(map[int]*blockMetadataArray)
	}
	if bma, ok := pki.primaryCache[primaryIdx]; ok && bma != nil {
		return bma, nil
	}

	pbm := &pki.p.primaryBlockMetadata[primaryIdx]
	bma := generateBlockMetadataArray()

	pki.compressedPrimaryBuf = bytes.ResizeOver(pki.compressedPrimaryBuf, int(pbm.size))
	fs.MustReadData(pki.p.primary, int64(pbm.offset), pki.compressedPrimaryBuf)

	var err error
	pki.primaryBuf, err = zstd.Decompress(pki.primaryBuf[:0], pki.compressedPrimaryBuf)
	if err != nil {
		releaseBlockMetadataArray(bma)
		return nil, fmt.Errorf("cannot decompress primary block: %w", err)
	}

	bma.arr, err = unmarshalBlockMetadata(bma.arr[:0], pki.primaryBuf)
	if err != nil {
		releaseBlockMetadataArray(bma)
		return nil, fmt.Errorf("cannot unmarshal primary block metadata: %w", err)
	}

	pki.primaryCache[primaryIdx] = bma
	return bma, nil
}

func (pki *partKeyIter) shouldSkipBlock(bm *blockMetadata) (bool, error) {
	tfo := generateTagFilterOp(bm, pki.p)
	defer releaseTagFilterOp(tfo)
	return pki.blockFilter.ShouldSkip(tfo)
}

func (pki *partKeyIter) currentBlocks() []*blockMetadata {
	return pki.curBlocks
}

func generatePartKeyIter() *partKeyIter {
	v := partKeyIterPool.Get()
	if v == nil {
		return &partKeyIter{}
	}
	return v
}

func releasePartKeyIter(pki *partKeyIter) {
	if pki == nil {
		return
	}
	pki.reset()
	partKeyIterPool.Put(pki)
}

var partKeyIterPool = pool.Register[*partKeyIter]("sidx-partKeyIter")
