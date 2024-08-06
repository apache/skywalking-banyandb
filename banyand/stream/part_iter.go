// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package stream

import (
	"errors"
	"fmt"
	"io"
	"sort"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/compress/zstd"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/pool"
)

type partIter struct {
	err                  error
	p                    *part
	curBlock             *blockMetadata
	sids                 []common.SeriesID
	primaryBlockMetadata []primaryBlockMetadata
	bms                  []blockMetadata
	compressedPrimaryBuf []byte
	primaryBuf           []byte
	sidIdx               int
	minTimestamp         int64
	maxTimestamp         int64
}

func (pi *partIter) reset() {
	pi.curBlock = nil
	pi.p = nil
	pi.sids = nil
	pi.sidIdx = 0
	pi.primaryBlockMetadata = nil
	pi.bms = nil
	pi.compressedPrimaryBuf = pi.compressedPrimaryBuf[:0]
	pi.primaryBuf = pi.primaryBuf[:0]
	pi.err = nil
}

func (pi *partIter) init(bma *blockMetadataArray, p *part, sids []common.SeriesID, minTimestamp, maxTimestamp int64) {
	pi.reset()
	pi.curBlock = &blockMetadata{}
	pi.p = p

	pi.bms = bma.arr
	pi.sids = sids
	pi.minTimestamp = minTimestamp
	pi.maxTimestamp = maxTimestamp

	pi.primaryBlockMetadata = p.primaryBlockMetadata

	pi.nextSeriesID()
}

func (pi *partIter) nextBlock() bool {
	for {
		if pi.err != nil {
			return false
		}
		if len(pi.bms) == 0 {
			if !pi.loadNextBlockMetadata() {
				return false
			}
		}
		if pi.findBlock() {
			return true
		}
	}
}

func (pi *partIter) error() error {
	if errors.Is(pi.err, io.EOF) {
		return nil
	}
	return pi.err
}

func (pi *partIter) nextSeriesID() bool {
	if pi.sidIdx >= len(pi.sids) {
		pi.err = io.EOF
		return false
	}
	pi.curBlock.seriesID = pi.sids[pi.sidIdx]
	pi.sidIdx++
	return true
}

func (pi *partIter) searchTargetSeriesID(sid common.SeriesID) bool {
	if pi.curBlock.seriesID >= sid {
		return true
	}
	if !pi.nextSeriesID() {
		return false
	}
	if pi.curBlock.seriesID >= sid {
		return true
	}
	sids := pi.sids[pi.sidIdx:]
	pi.sidIdx += sort.Search(len(sids), func(i int) bool {
		return sid <= sids[i]
	})
	if pi.sidIdx >= len(pi.sids) {
		pi.sidIdx = len(pi.sids)
		pi.err = io.EOF
		return false
	}
	pi.curBlock.seriesID = pi.sids[pi.sidIdx]
	pi.sidIdx++
	return true
}

func (pi *partIter) loadNextBlockMetadata() bool {
	for len(pi.primaryBlockMetadata) > 0 {
		if !pi.searchTargetSeriesID(pi.primaryBlockMetadata[0].seriesID) {
			return false
		}
		pi.primaryBlockMetadata = searchPBM(pi.primaryBlockMetadata, pi.curBlock.seriesID)

		pbm := &pi.primaryBlockMetadata[0]
		pi.primaryBlockMetadata = pi.primaryBlockMetadata[1:]
		if pi.curBlock.seriesID < pbm.seriesID {
			logger.Panicf("invariant violation: pi.curBlock.seriesID cannot be smaller than pbm.seriesID; got %+v vs %+v", &pi.curBlock.seriesID, &pbm.seriesID)
		}

		if pbm.maxTimestamp < pi.minTimestamp || pbm.minTimestamp > pi.maxTimestamp {
			continue
		}

		var err error
		pi.bms, err = pi.readPrimaryBlock(pi.bms[:0], pbm)
		if err != nil {
			pi.err = fmt.Errorf("cannot read primary block for part %q at offset %d with size %d: %w",
				&pi.p.partMetadata, pbm.offset, pbm.size, err)
			return false
		}
		return true
	}
	pi.err = io.EOF
	return false
}

func searchPBM(pbmIndex []primaryBlockMetadata, sid common.SeriesID) []primaryBlockMetadata {
	if sid < pbmIndex[0].seriesID {
		logger.Panicf("invariant violation: sid cannot be smaller than pbmIndex[0]; got %d vs %d", sid, &pbmIndex[0].seriesID)
	}

	if sid == pbmIndex[0].seriesID {
		return pbmIndex
	}

	n := sort.Search(len(pbmIndex), func(i int) bool {
		return sid <= pbmIndex[i].seriesID
	})
	if n == 0 {
		logger.Panicf("invariant violation: sort.Search returned 0 for sid > pbmIndex[0].seriesID; sid=%+v; pbmIndex[0].seriesID=%+v",
			sid, &pbmIndex[0].seriesID)
	}
	return pbmIndex[n-1:]
}

func (pi *partIter) readPrimaryBlock(bms []blockMetadata, mr *primaryBlockMetadata) ([]blockMetadata, error) {
	pi.compressedPrimaryBuf = bytes.ResizeOver(pi.compressedPrimaryBuf, int(mr.size))
	fs.MustReadData(pi.p.primary, int64(mr.offset), pi.compressedPrimaryBuf)

	var err error
	pi.primaryBuf, err = zstd.Decompress(pi.primaryBuf[:0], pi.compressedPrimaryBuf)
	if err != nil {
		return nil, fmt.Errorf("cannot decompress index block: %w", err)
	}
	bms, err = unmarshalBlockMetadata(bms, pi.primaryBuf)
	if err != nil {
		return nil, fmt.Errorf("cannot unmarshal index block: %w", err)
	}
	return bms, nil
}

func (pi *partIter) findBlock() bool {
	bhs := pi.bms
	for len(bhs) > 0 {
		sid := pi.curBlock.seriesID
		if bhs[0].seriesID < sid {
			n := sort.Search(len(bhs), func(i int) bool {
				return sid <= bhs[i].seriesID
			})
			if n == len(bhs) {
				break
			}
			bhs = bhs[n:]
		}
		bm := &bhs[0]

		if bm.seriesID != sid {
			if !pi.searchTargetSeriesID(bm.seriesID) {
				return false
			}
			continue
		}

		if bm.timestamps.max < pi.minTimestamp {
			bhs = bhs[1:]
			continue
		}
		if bm.timestamps.min > pi.maxTimestamp {
			if !pi.nextSeriesID() {
				return false
			}
			continue
		}

		pi.curBlock = bm

		pi.bms = bhs[1:]
		return true
	}
	pi.bms = nil
	return false
}

type partMergeIter struct {
	seqReaders           seqReaders
	err                  error
	primaryBlockMetadata []primaryBlockMetadata
	compressedPrimaryBuf []byte
	primaryBuf           []byte
	block                blockPointer
	primaryMetadataIdx   int
}

func (pmi *partMergeIter) reset() {
	pmi.err = nil
	pmi.seqReaders.reset()
	pmi.primaryBlockMetadata = nil
	pmi.primaryMetadataIdx = 0
	pmi.primaryBuf = pmi.primaryBuf[:0]
	pmi.compressedPrimaryBuf = pmi.compressedPrimaryBuf[:0]
	pmi.block.reset()
}

func (pmi *partMergeIter) mustInitFromPart(p *part) {
	pmi.reset()
	pmi.seqReaders.init(p)
	pmi.primaryBlockMetadata = p.primaryBlockMetadata
}

func (pmi *partMergeIter) error() error {
	if pmi.err == nil || errors.Is(pmi.err, io.EOF) {
		return nil
	}
	return pmi.err
}

func (pmi *partMergeIter) nextBlockMetadata() bool {
	if pmi.err != nil {
		return false
	}
	pmi.block.reset()
	if len(pmi.primaryBuf) == 0 {
		if err := pmi.loadPrimaryBuf(); err != nil {
			pmi.err = err
			return false
		}
	}
	if err := pmi.loadBlockMetadata(); err != nil {
		pmi.err = err
		return false
	}
	return true
}

func (pmi *partMergeIter) loadPrimaryBuf() error {
	if pmi.primaryMetadataIdx >= len(pmi.primaryBlockMetadata) {
		return io.EOF
	}
	pm := pmi.primaryBlockMetadata[pmi.primaryMetadataIdx]
	pmi.compressedPrimaryBuf = bytes.ResizeOver(pmi.compressedPrimaryBuf, int(pm.size))
	pmi.seqReaders.primary.mustReadFull(pmi.compressedPrimaryBuf)
	var err error
	pmi.primaryBuf, err = zstd.Decompress(pmi.primaryBuf[:0], pmi.compressedPrimaryBuf)
	if err != nil {
		return fmt.Errorf("cannot decompress primary block: %w", err)
	}
	pmi.primaryMetadataIdx++
	return nil
}

func (pmi *partMergeIter) loadBlockMetadata() error {
	pmi.block.reset()
	var err error
	pmi.primaryBuf, err = pmi.block.bm.unmarshal(pmi.primaryBuf)
	if err != nil {
		pm := pmi.primaryBlockMetadata[pmi.primaryMetadataIdx-1]
		return fmt.Errorf("can't read block metadata from primary at %d: %w", pm.offset, err)
	}
	return nil
}

func (pmi *partMergeIter) mustLoadBlockData(decoder *encoding.BytesBlockDecoder, block *blockPointer) {
	block.block.mustSeqReadFrom(decoder, &pmi.seqReaders, pmi.block.bm)
}

func generatePartMergeIter() *partMergeIter {
	v := pmiPool.Get()
	if v == nil {
		return &partMergeIter{}
	}
	return v
}

func releasePartMergeIter(pmi *partMergeIter) {
	pmi.reset()
	pmiPool.Put(pmi)
}

var pmiPool = pool.Register[*partMergeIter]("stream-partMergeIter")

type partMergeIterHeap []*partMergeIter

func (pih *partMergeIterHeap) Len() int {
	return len(*pih)
}

func (pih *partMergeIterHeap) Less(i, j int) bool {
	x := *pih
	return x[i].block.bm.less(&x[j].block.bm)
}

func (pih *partMergeIterHeap) Swap(i, j int) {
	x := *pih
	x[i], x[j] = x[j], x[i]
}

func (pih *partMergeIterHeap) Push(x interface{}) {
	*pih = append(*pih, x.(*partMergeIter))
}

func (pih *partMergeIterHeap) Pop() interface{} {
	a := *pih
	v := a[len(a)-1]
	*pih = a[:len(a)-1]
	return v
}

func generateColumnValuesDecoder() *encoding.BytesBlockDecoder {
	v := columnValuesDecoderPool.Get()
	if v == nil {
		return &encoding.BytesBlockDecoder{}
	}
	return v
}

func releaseColumnValuesDecoder(d *encoding.BytesBlockDecoder) {
	d.Reset()
	columnValuesDecoderPool.Put(d)
}

var columnValuesDecoderPool = pool.Register[*encoding.BytesBlockDecoder]("stream-columnValuesDecoder")
