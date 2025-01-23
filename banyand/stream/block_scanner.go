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
	"sync"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/cgroups"
	"github.com/apache/skywalking-banyandb/pkg/index/posting"
	"github.com/apache/skywalking-banyandb/pkg/index/posting/roaring"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/pool"
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
	bs.bm.reset()
}

type blockScanResultBatch struct {
	err error
	bss []blockScanResult
}

func (bsb *blockScanResultBatch) reset() {
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

var shardScanConcurrencyCh = make(chan struct{}, cgroups.CPUs())

type blockScanner struct {
	segment   storage.Segment[*tsTable, *option]
	pm        *protector.Memory
	l         *logger.Logger
	series    []*pbv1.Series
	seriesIDs []uint64
	qo        queryOptions
}

func (q *blockScanner) searchSeries(ctx context.Context) error {
	seriesFilter := roaring.NewPostingList()
	sl, err := q.segment.Lookup(ctx, q.series)
	if err != nil {
		return err
	}
	for i := range sl {
		if seriesFilter.Contains(uint64(sl[i].ID)) {
			continue
		}
		seriesFilter.Insert(uint64(sl[i].ID))
		if q.qo.seriesToEntity == nil {
			q.qo.seriesToEntity = make(map[common.SeriesID][]*modelv1.TagValue)
		}
		q.qo.seriesToEntity[sl[i].ID] = sl[i].EntityValues
		q.qo.sortedSids = append(q.qo.sortedSids, sl[i].ID)
	}
	if seriesFilter.IsEmpty() {
		return nil
	}
	q.seriesIDs = seriesFilter.ToSlice()
	sort.Slice(q.qo.sortedSids, func(i, j int) bool { return q.qo.sortedSids[i] < q.qo.sortedSids[j] })
	return nil
}

func (q *blockScanner) scanShardsInParallel(ctx context.Context, wg *sync.WaitGroup, blockCh chan *blockScanResultBatch) []scanFinalizer {
	tabs := q.segment.Tables()
	finalizers := make([]scanFinalizer, len(tabs))
	for i := range tabs {
		select {
		case shardScanConcurrencyCh <- struct{}{}:
		case <-ctx.Done():
			return finalizers
		}
		wg.Add(1)
		go func(idx int, tab *tsTable) {
			finalizers[idx] = q.scanBlocks(ctx, q.seriesIDs, tab, blockCh)
			wg.Done()
			<-shardScanConcurrencyCh
		}(i, tabs[i])
	}
	return finalizers
}

func (q *blockScanner) scanBlocks(ctx context.Context, seriesList []uint64, tab *tsTable, blockCh chan *blockScanResultBatch) (sf scanFinalizer) {
	s := tab.currentSnapshot()
	if s == nil {
		return nil
	}
	sf = s.decRef
	filter, err := q.indexSearch(ctx, seriesList, tab)
	if err != nil {
		select {
		case blockCh <- &blockScanResultBatch{err: err}:
		case <-ctx.Done():
		}
		return
	}
	select {
	case <-ctx.Done():
		return
	default:
	}

	parts, n := s.getParts(nil, q.qo.minTimestamp, q.qo.maxTimestamp)
	if n < 1 {
		return
	}
	bma := generateBlockMetadataArray()
	defer releaseBlockMetadataArray(bma)
	ti := generateTstIter()
	defer releaseTstIter(ti)
	ti.init(bma, parts, q.qo.sortedSids, q.qo.minTimestamp, q.qo.maxTimestamp)
	batch := generateBlockScanResultBatch()
	if ti.Error() != nil {
		batch.err = fmt.Errorf("cannot init tstIter: %w", ti.Error())
		select {
		case blockCh <- batch:
		case <-ctx.Done():
			releaseBlockScanResultBatch(batch)
			q.l.Warn().Err(ti.Error()).Msg("cannot init tstIter")
		}
		return
	}
	for ti.nextBlock() {
		p := ti.piHeap[0]
		batch.bss = append(batch.bss, blockScanResult{
			p: p.p,
		})
		bs := &batch.bss[len(batch.bss)-1]
		bs.qo.copyFrom(&q.qo)
		bs.qo.elementFilter = filter
		bs.bm.copyFrom(p.curBlock)
		if len(batch.bss) >= cap(batch.bss) {
			var totalBlockBytes uint64
			for i := range batch.bss {
				totalBlockBytes += batch.bss[i].bm.uncompressedSizeBytes
			}
			if err := q.pm.AcquireResource(ctx, totalBlockBytes); err != nil {
				batch.err = fmt.Errorf("cannot acquire resource: %w", err)
				select {
				case blockCh <- batch:
				case <-ctx.Done():
					releaseBlockScanResultBatch(batch)
					q.l.Warn().Err(err).Msg("cannot acquire resource")
				}
				return
			}
			select {
			case blockCh <- batch:
			case <-ctx.Done():
				releaseBlockScanResultBatch(batch)
				q.l.Warn().Int("batch.len", len(batch.bss)).Msg("context canceled while sending block")
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
	return
}

func (q *blockScanner) indexSearch(ctx context.Context, seriesList []uint64, tw *tsTable) (posting.List, error) {
	if q.qo.Filter == nil || q.qo.Filter == logicalstream.ENode {
		return nil, nil
	}
	pl, err := tw.Index().Search(ctx, seriesList, q.qo.Filter)
	if err != nil {
		return nil, err
	}
	if pl == nil {
		return roaring.DummyPostingList, nil
	}
	return pl, nil
}

func (q *blockScanner) close() {
	q.segment.DecRef()
}

type scanFinalizer func()
