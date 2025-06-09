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
	"fmt"
	"sort"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/posting"
	"github.com/apache/skywalking-banyandb/pkg/index/posting/roaring"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/pool"
	"github.com/apache/skywalking-banyandb/pkg/query"
	logicalstream "github.com/apache/skywalking-banyandb/pkg/query/logical/stream"
)

const blockScannerBatchSize = 32

type blockScanResult struct {
	p  *part
	qo queryOptions
	bm blockMetadata
}

func (bs *blockScanResult) reset() {
	bs.p = nil
	bs.qo.reset()
	bs.bm.reset()
}

type blockScanResultBatch struct {
	err error
	bss []blockScanResult
}

func (bsb *blockScanResultBatch) reset() {
	bsb.err = nil
	for i := range bsb.bss {
		bsb.bss[i].reset()
	}
	bsb.bss = bsb.bss[:0]
}

func generateBlockScanResultBatch() *blockScanResultBatch {
	v := blockScanResultBatchPool.Get()
	if v == nil {
		return &blockScanResultBatch{
			bss: make([]blockScanResult, 0, blockScannerBatchSize),
		}
	}
	return v
}

func releaseBlockScanResultBatch(bsb *blockScanResultBatch) {
	bsb.reset()
	blockScanResultBatchPool.Put(bsb)
}

var blockScanResultBatchPool = pool.Register[*blockScanResultBatch]("stream-blockScannerBatch")

func searchSeries(ctx context.Context, qo queryOptions, segment storage.Segment[*tsTable, *option], series []*pbv1.Series) (queryOptions, error) {
	seriesFilter := roaring.NewPostingList()
	sl, err := segment.Lookup(ctx, series)
	if err != nil {
		return qo, err
	}
	for i := range sl {
		if seriesFilter.Contains(uint64(sl[i].ID)) {
			continue
		}
		seriesFilter.Insert(uint64(sl[i].ID))
		if qo.seriesToEntity == nil {
			qo.seriesToEntity = make(map[common.SeriesID][]*modelv1.TagValue)
		}
		qo.seriesToEntity[sl[i].ID] = sl[i].EntityValues
		qo.sortedSids = append(qo.sortedSids, sl[i].ID)
	}
	if seriesFilter.IsEmpty() {
		return qo, nil
	}
	sort.Slice(qo.sortedSids, func(i, j int) bool { return qo.sortedSids[i] < qo.sortedSids[j] })
	return qo, nil
}

func getBlockScanner(ctx context.Context, segment storage.Segment[*tsTable, *option], qo queryOptions,
	l *logger.Logger, pm protector.Memory, tr *index.RangeOpts,
) (bc *blockScanner, err error) {
	tabs := segment.Tables()
	finalizers := make([]scanFinalizer, 0, len(tabs)+1)
	finalizers = append(finalizers, segment.DecRef)
	defer func() {
		if bc == nil || err != nil {
			for i := range finalizers {
				finalizers[i]()
			}
		}
	}()
	var parts []*part
	var size, offset int
	filterIndex := make(map[uint64]posting.List)
	for i := range tabs {
		filter, filterTS, err := search(ctx, qo, qo.sortedSids, tabs[i], tr)
		if err != nil {
			return nil, err
		}
		if filter != nil && filter.IsEmpty() {
			continue
		}
		minTimestamp, maxTimestamp := updateTimeRange(filterTS, qo.minTimestamp, qo.maxTimestamp)
		snp := tabs[i].currentSnapshot()
		parts, size = snp.getParts(parts, minTimestamp, maxTimestamp)
		if size < 1 {
			snp.decRef()
			continue
		}
		finalizers = append(finalizers, snp.decRef)
		for j := offset; j < offset+size; j++ {
			filterIndex[parts[j].partMetadata.ID] = filter
		}
		offset += size
	}
	if len(parts) < 1 {
		return nil, nil
	}
	var asc bool
	if qo.Order == nil {
		asc = true
	} else {
		asc = qo.Order.Sort == modelv1.Sort_SORT_ASC || qo.Order.Sort == modelv1.Sort_SORT_UNSPECIFIED
	}
	return &blockScanner{
		parts:       getDisjointParts(parts, asc),
		filterIndex: filterIndex,
		qo:          qo,
		asc:         asc,
		l:           l,
		pm:          pm,
		finalizers:  finalizers,
	}, nil
}

func search(ctx context.Context, qo queryOptions, seriesList []common.SeriesID, tw *tsTable, tr *index.RangeOpts) (pl posting.List, plTS posting.List, err error) {
	if qo.Filter == nil || qo.Filter == logicalstream.ENode {
		return nil, nil, nil
	}
	tracer := query.GetTracer(ctx)
	if tracer != nil {
		span, _ := tracer.StartSpan(ctx, "scan local index")
		span.Tagf("sids", "%d", len(seriesList))
		span.Tag("tab", tw.p.String())
		defer func() {
			if pl != nil {
				span.Tagf("got", "%d", pl.Len())
			}
			if err != nil {
				span.Error(err)
			}
			span.Stop()
		}()
	}
	sid := make([]uint64, len(seriesList))
	for i := range seriesList {
		sid[i] = uint64(seriesList[i])
	}
	pl, plTS, err = tw.Index().Search(ctx, sid, qo.Filter, tr)
	if err != nil {
		return nil, nil, err
	}
	if pl == nil {
		return roaring.DummyPostingList, roaring.DummyPostingList, nil
	}
	return pl, plTS, nil
}

type scanFinalizer func()

type blockScanner struct {
	filterIndex map[uint64]posting.List
	l           *logger.Logger
	pm          protector.Memory
	parts       [][]*part
	finalizers  []scanFinalizer
	qo          queryOptions
	asc         bool
}

func (bsn *blockScanner) scan(ctx context.Context, blockCh chan *blockScanResultBatch) {
	if len(bsn.parts) < 1 {
		return
	}
	var parts []*part
	if bsn.asc {
		parts = bsn.parts[0]
		bsn.parts = bsn.parts[1:]
	} else {
		parts = bsn.parts[len(bsn.parts)-1]
		bsn.parts = bsn.parts[:len(bsn.parts)-1]
	}
	bma := generateBlockMetadataArray()
	defer releaseBlockMetadataArray(bma)
	ti := generateTstIter()
	defer releaseTstIter(ti)
	ti.init(bma, parts, bsn.qo.sortedSids, bsn.qo.minTimestamp, bsn.qo.maxTimestamp)
	batch := generateBlockScanResultBatch()
	if ti.Error() != nil {
		batch.err = fmt.Errorf("cannot init tstIter: %w", ti.Error())
		select {
		case blockCh <- batch:
		case <-ctx.Done():
			releaseBlockScanResultBatch(batch)
			bsn.l.Warn().Err(ti.Error()).Msg("cannot init tstIter")
		}
		return
	}
	var totalBlockBytes uint64
	for ti.nextBlock() {
		p := ti.piHeap[0]
		batch.bss = append(batch.bss, blockScanResult{
			p: p.p,
		})
		bs := &batch.bss[len(batch.bss)-1]
		bs.qo.copyFrom(&bsn.qo)
		bs.qo.elementFilter = bsn.filterIndex[p.p.partMetadata.ID]
		bs.bm.copyFrom(p.curBlock)
		quota := bsn.pm.AvailableBytes()
		for i := range batch.bss {
			totalBlockBytes += batch.bss[i].bm.uncompressedSizeBytes
			if quota >= 0 && totalBlockBytes > uint64(quota) {
				err := fmt.Errorf("block scan quota exceeded: used %d bytes, quota is %d bytes", totalBlockBytes, quota)
				batch.err = err
				select {
				case blockCh <- batch:
				case <-ctx.Done():
					releaseBlockScanResultBatch(batch)
					bsn.l.Warn().Err(err).Msg("quota exceeded, context canceled")
				}
				return
			}
		}
		if len(batch.bss) >= cap(batch.bss) {
			if err := bsn.pm.AcquireResource(ctx, totalBlockBytes); err != nil {
				batch.err = fmt.Errorf("cannot acquire resource: %w", err)
				select {
				case blockCh <- batch:
				case <-ctx.Done():
					releaseBlockScanResultBatch(batch)
					bsn.l.Warn().Err(err).Msg("cannot acquire resource")
				}
				return
			}
			select {
			case blockCh <- batch:
			case <-ctx.Done():
				releaseBlockScanResultBatch(batch)
				bsn.l.Warn().Int("batch.len", len(batch.bss)).Msg("context canceled while sending block")
				return
			}
			batch = generateBlockScanResultBatch()
		}
	}
	if ti.Error() != nil {
		batch.err = fmt.Errorf("cannot iterate tstIter: %w", ti.Error())
		select {
		case blockCh <- batch:
		case <-ctx.Done():

			releaseBlockScanResultBatch(batch)
		}
		return
	}
	if len(batch.bss) > 0 {
		select {
		case blockCh <- batch:
		case <-ctx.Done():
			releaseBlockScanResultBatch(batch)
		}
		return
	}
	releaseBlockScanResultBatch(batch)
}

func (bsn *blockScanner) close() {
	for i := range bsn.finalizers {
		bsn.finalizers[i]()
	}
}
