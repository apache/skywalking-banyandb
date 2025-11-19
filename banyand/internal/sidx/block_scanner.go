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

	"github.com/dustin/go-humanize"

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
	batchSize  int
}

func (bsn *blockScanner) scan(ctx context.Context, blockCh chan *blockScanResultBatch) {
	if len(bsn.parts) < 1 {
		return
	}

	if !bsn.checkContext(ctx) {
		return
	}

	it := generateIter()
	defer releaseIter(it)

	it.init(bsn.parts, bsn.seriesIDs, bsn.minKey, bsn.maxKey, bsn.filter, bsn.asc)

	batch := generateBlockScanResultBatch()
	if it.Error() != nil {
		batch.err = fmt.Errorf("cannot init iter: %w", it.Error())
		bsn.sendBatch(ctx, blockCh, batch)
		return
	}

	batchThreshold := bsn.batchSize
	if batchThreshold <= 0 {
		batchThreshold = blockScannerBatchSize
	}

	var totalBlockBytes uint64
	for it.nextBlock() {
		if !bsn.checkContext(ctx) {
			releaseBlockScanResultBatch(batch)
			return
		}

		bm, p := it.current()
		if err := bsn.validateBlockMetadata(bm, p, it); err != nil {
			batch.err = err
			bsn.sendBatch(ctx, blockCh, batch)
			return
		}

		blockSize := bm.uncompressedSize

		// Check if adding this block would exceed quota
		if exceeded, err := bsn.checkQuotaExceeded(totalBlockBytes, blockSize, batch); exceeded {
			if err != nil {
				batch.err = err
			}
			bsn.sendBatch(ctx, blockCh, batch)
			return
		}

		// Quota OK, add block to batch
		bsn.addBlockToBatch(batch, bm, p)
		totalBlockBytes += blockSize

		// Check if batch is full
		if len(batch.bss) >= batchThreshold || len(batch.bss) >= cap(batch.bss) {
			if !bsn.sendBatch(ctx, blockCh, batch) {
				if dl := bsn.l.Debug(); dl.Enabled() {
					dl.Int("batch.len", len(batch.bss)).Msg("context canceled while sending block")
				}
				return
			}
			batch = generateBlockScanResultBatch()
		}
	}

	if it.Error() != nil {
		batch.err = fmt.Errorf("cannot iterate iter: %w", it.Error())
		bsn.sendBatch(ctx, blockCh, batch)
		return
	}

	if len(batch.bss) > 0 {
		bsn.sendBatch(ctx, blockCh, batch)
		return
	}

	releaseBlockScanResultBatch(batch)
}

// checkContext returns false if context is canceled.
func (bsn *blockScanner) checkContext(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return false
	default:
		return true
	}
}

// sendBatch sends a batch to the channel, handling context cancellation.
// Returns false if context was canceled, true otherwise.
func (bsn *blockScanner) sendBatch(ctx context.Context, blockCh chan *blockScanResultBatch, batch *blockScanResultBatch) bool {
	select {
	case blockCh <- batch:
		return true
	case <-ctx.Done():
		releaseBlockScanResultBatch(batch)
		return false
	}
}

// validateBlockMetadata checks if block metadata and part are valid.
func (bsn *blockScanner) validateBlockMetadata(bm *blockMetadata, p *part, it *iter) error {
	if bm == nil {
		it.err = fmt.Errorf("sidx iterator returned nil block")
		return it.err
	}
	if p == nil {
		it.err = fmt.Errorf("block missing part reference")
		return it.err
	}
	return nil
}

// checkQuotaExceeded checks if adding a block would exceed the memory quota.
// Returns (exceeded, error) where exceeded is true if quota would be exceeded.
func (bsn *blockScanner) checkQuotaExceeded(totalBlockBytes, blockSize uint64, batch *blockScanResultBatch) (bool, error) {
	quota := bsn.pm.AvailableBytes()
	if quota < 0 || totalBlockBytes+blockSize <= uint64(quota) {
		return false, nil
	}

	// Quota would be exceeded
	if len(batch.bss) > 0 {
		// Send current batch without error
		return true, nil
	}

	// Batch is empty, return error
	return true, fmt.Errorf("sidx block scan quota exceeded: block size %s, quota is %s",
		humanize.Bytes(blockSize), humanize.Bytes(uint64(quota)))
}

// addBlockToBatch adds a block to the batch.
func (bsn *blockScanner) addBlockToBatch(batch *blockScanResultBatch, bm *blockMetadata, p *part) {
	batch.bss = append(batch.bss, blockScanResult{p: p})
	bs := &batch.bss[len(batch.bss)-1]
	bs.bm.copyFrom(bm)
}

func (bsn *blockScanner) close() {
	for i := range bsn.finalizers {
		bsn.finalizers[i]()
	}
}
