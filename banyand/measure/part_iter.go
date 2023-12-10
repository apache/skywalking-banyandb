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

package measure

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/compress/zstd"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

type partIter struct {
	bm *blockMetadata

	p *part

	sids []common.SeriesID

	sidIdx int

	minTimestamp int64
	maxTimestamp int64

	primaryBlockMetadata []primaryBlockMetadata

	bms []blockMetadata

	compressedPrimaryBuf []byte
	indexBuf             []byte

	err error
}

func (ps *partIter) reset() {
	ps.bm = nil
	ps.p = nil
	ps.sids = nil
	ps.sidIdx = 0
	ps.primaryBlockMetadata = nil
	ps.bms = nil
	ps.compressedPrimaryBuf = ps.compressedPrimaryBuf[:0]
	ps.indexBuf = ps.indexBuf[:0]
	ps.err = nil
}

var isInTest = func() bool {
	return strings.HasSuffix(os.Args[0], ".test")
}()

func (ps *partIter) init(p *part, sids []common.SeriesID, opt *QueryOptions) {
	ps.reset()
	ps.p = p

	ps.sids = sids
	ps.minTimestamp = opt.minTimestamp
	ps.maxTimestamp = opt.maxTimestamp

	ps.primaryBlockMetadata = p.primaryBlockMetadata

	ps.nextSeriesID()
}

func (ps *partIter) NextBlock() bool {
	for {
		if ps.err != nil {
			return false
		}
		if len(ps.bms) == 0 {
			if !ps.nextBHS() {
				return false
			}
		}
		if ps.searchBHS() {
			return true
		}
	}
}

// Error returns the last error.
func (ps *partIter) Error() error {
	if ps.err == io.EOF {
		return nil
	}
	return ps.err
}

func (ps *partIter) nextSeriesID() bool {
	if ps.sidIdx >= len(ps.sids) {
		ps.err = io.EOF
		return false
	}
	ps.bm.seriesID = ps.sids[ps.sidIdx]
	ps.sidIdx++
	return true
}

func (ps *partIter) skipSeriesIDsSmallerThan(sid common.SeriesID) bool {
	sids := ps.sids[ps.sidIdx:]
	ps.sidIdx += sort.Search(len(sids), func(i int) bool {
		return sid < sids[i]
	})
	if ps.sidIdx >= len(ps.sids) {
		ps.sidIdx = len(ps.sids)
		ps.err = io.EOF
		return false
	}
	ps.bm.seriesID = ps.sids[ps.sidIdx]
	ps.sidIdx++
	return true
}

func (ps *partIter) nextBHS() bool {
	for len(ps.primaryBlockMetadata) > 0 {
		if !ps.skipSeriesIDsSmallerThan(ps.primaryBlockMetadata[0].seriesID) {
			return false
		}
		ps.primaryBlockMetadata = skipSmallMetaindexRows(ps.primaryBlockMetadata, ps.bm.seriesID)

		mr := &ps.primaryBlockMetadata[0]
		ps.primaryBlockMetadata = ps.primaryBlockMetadata[1:]
		if ps.bm.seriesID < mr.seriesID {
			logger.Panicf("BUG: invariant violation: ps.BlockRef.bh.TSID cannot be smaller than mr.TSID; got %+v vs %+v", &ps.bm.seriesID, &mr.seriesID)
		}

		if mr.maxTimestamp < ps.minTimestamp || mr.minTimestamp > ps.maxTimestamp {
			continue
		}

		bm, err := ps.readPrimaryBlock(mr)
		if err != nil {
			ps.err = fmt.Errorf("cannot read index block for part %q at offset %d with size %d: %w",
				&ps.p.partMetadata, mr.offset, mr.size, err)
			return false
		}
		ps.bms = bm
		return true
	}

	// No more metaindex rows to search.
	ps.err = io.EOF
	return false
}

func skipSmallMetaindexRows(pbmIndex []primaryBlockMetadata, sid common.SeriesID) []primaryBlockMetadata {
	if sid < pbmIndex[0].seriesID {
		logger.Panicf("BUG: invariant violation: tsid cannot be smaller than metaindex[0]; got %d vs %d", sid, &pbmIndex[0].seriesID)
	}

	if sid == pbmIndex[0].seriesID {
		return pbmIndex
	}

	n := sort.Search(len(pbmIndex), func(i int) bool {
		return sid > pbmIndex[i].seriesID
	})
	if n == 0 {
		logger.Panicf("BUG: invariant violation: sort.Search returned 0 for tsid > metaindex[0].TSID; tsid=%+v; metaindex[0].TSID=%+v",
			sid, &pbmIndex[0].seriesID)
	}
	return pbmIndex[n-1:]
}

func (ps *partIter) readPrimaryBlock(mr *primaryBlockMetadata) ([]blockMetadata, error) {
	ps.compressedPrimaryBuf = bytes.ResizeOver(ps.compressedPrimaryBuf, int(mr.size))
	fs.MustReadData(ps.p.primary, int64(mr.offset), ps.compressedPrimaryBuf)

	var err error
	ps.indexBuf, err = zstd.Decompress(ps.indexBuf[:0], ps.compressedPrimaryBuf)
	if err != nil {
		return nil, fmt.Errorf("cannot decompress index block: %w", err)
	}
	//TODO: cache bm
	bm := make([]blockMetadata, 0)
	bm, err = unmarshalBlockMetadata(bm, ps.indexBuf)
	if err != nil {
		return nil, fmt.Errorf("cannot unmarshal index block: %w", err)
	}
	return bm, nil
}

func (ps *partIter) searchBHS() bool {
	bhs := ps.bms
	for len(bhs) > 0 {
		// Skip block headers with tsids smaller than the given sid.
		sid := ps.bm.seriesID
		if bhs[0].seriesID < sid {
			n := sort.Search(len(bhs), func(i int) bool {
				return sid > bhs[i].seriesID
			})
			if n == len(bhs) {
				// Nothing found.
				break
			}
			bhs = bhs[n:]
		}
		bh := &bhs[0]

		// Invariant: tsid <= bh.TSID

		if bh.seriesID != sid {
			// tsid < bh.TSID: no more blocks with the given tsid.
			// Proceed to the next (bigger) tsid.
			if !ps.skipSeriesIDsSmallerThan(bh.seriesID) {
				return false
			}
			continue
		}

		// Found the block with the given tsid. Verify timestamp range.
		// While blocks for the same TSID are sorted by MinTimestamp,
		// the may contain overlapped time ranges.
		// So use linear search instead of binary search.
		if bh.timestamps.max < ps.minTimestamp {
			// Skip the block with too small timestamps.
			bhs = bhs[1:]
			continue
		}
		if bh.timestamps.min > ps.maxTimestamp {
			// Proceed to the next tsid, since the remaining blocks
			// for the current tsid contain too big timestamps.
			if !ps.nextSeriesID() {
				return false
			}
			continue
		}

		// Found the tsid block with the matching timestamp range.
		// Read it.
		ps.bm = bh

		ps.bms = bhs[1:]
		return true
	}
	ps.bms = nil
	return false
}
