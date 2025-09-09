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
	"context"
	"fmt"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/pool"
)

const blockScannerBatchSize = 32

type blockScanResult struct {
	p  *part
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

var blockScanResultBatchPool = pool.Register[*blockScanResultBatch]("sidx-blockScannerBatch")

type scanFinalizer func()

type blockScanner struct {
	pm         protector.Memory
	filter     index.Filter
	l          *logger.Logger
	parts      []*part
	finalizers []scanFinalizer
	seriesIDs  []common.SeriesID
	minKey     int64
	maxKey     int64
	asc        bool
}

func (bsn *blockScanner) scan(ctx context.Context, blockCh chan *blockScanResultBatch) {
	if len(bsn.parts) < 1 {
		return
	}

	bma := generateBlockMetadataArray()
	defer releaseBlockMetadataArray(bma)

	it := generateIter()
	defer releaseIter(it)

	it.init(bma, bsn.parts, bsn.seriesIDs, bsn.minKey, bsn.maxKey, bsn.filter)

	batch := generateBlockScanResultBatch()
	if it.Error() != nil {
		batch.err = fmt.Errorf("cannot init iter: %w", it.Error())
		select {
		case blockCh <- batch:
		case <-ctx.Done():
			releaseBlockScanResultBatch(batch)
			bsn.l.Warn().Err(it.Error()).Msg("cannot init iter")
		}
		return
	}

	var totalBlockBytes uint64
	for it.nextBlock() {
		p := it.piHeap[0]
		batch.bss = append(batch.bss, blockScanResult{
			p: p.p,
		})
		bs := &batch.bss[len(batch.bss)-1]
		bs.bm.copyFrom(p.curBlock)

		quota := bsn.pm.AvailableBytes()
		for i := range batch.bss {
			totalBlockBytes += batch.bss[i].bm.uncompressedSize
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

	if it.Error() != nil {
		batch.err = fmt.Errorf("cannot iterate iter: %w", it.Error())
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
