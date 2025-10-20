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
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/dustin/go-humanize"

	"github.com/apache/skywalking-banyandb/pkg/query"
)

const (
	partMetadataHeader = "MinTimestamp, MaxTimestamp, CompressionSize, UncompressedSize, TotalCount, BlocksCount"
	blockHeader        = "PartID, TraceID, Count, UncompressedSize"
	traceIDSampleLimit = 5
	blockHeadLimit     = 10
	blockTailLimit     = 10
)

type blockInfo struct {
	blockString string
	size        uint64
}

type blockQueue struct {
	blocks   []blockInfo
	capacity int
	keepMin  bool
}

func newBlockQueue(capacity int, keepMin bool) *blockQueue {
	return &blockQueue{
		blocks:   make([]blockInfo, 0, capacity),
		capacity: capacity,
		keepMin:  keepMin,
	}
}

func (bq *blockQueue) add(block blockInfo) {
	// If queue is not full, just add
	if len(bq.blocks) < bq.capacity {
		bq.blocks = append(bq.blocks, block)
		bq.heapifyUp(len(bq.blocks) - 1)
		return
	}

	// Queue is full, check if we should replace the root
	if bq.shouldReplace(block) {
		// Emit (discard) the root before adding
		bq.blocks[0] = block
		bq.heapifyDown(0)
	}
	// Otherwise, emit (discard) the incoming block
}

func (bq *blockQueue) shouldReplace(block blockInfo) bool {
	if len(bq.blocks) == 0 {
		return true
	}
	if bq.keepMin {
		// For min heap (tail), replace if new block is smaller
		return block.size < bq.blocks[0].size
	}
	// For max heap (head), replace if new block is larger
	return block.size > bq.blocks[0].size
}

func (bq *blockQueue) compare(i, j int) bool {
	if bq.keepMin {
		// Max heap for keeping smallest values (evict largest)
		return bq.blocks[i].size > bq.blocks[j].size
	}
	// Min heap for keeping largest values (evict smallest)
	return bq.blocks[i].size < bq.blocks[j].size
}

func (bq *blockQueue) heapifyUp(index int) {
	for index > 0 {
		parent := (index - 1) / 2
		if !bq.compare(index, parent) {
			break
		}
		bq.blocks[index], bq.blocks[parent] = bq.blocks[parent], bq.blocks[index]
		index = parent
	}
}

func (bq *blockQueue) heapifyDown(index int) {
	for {
		largest := index
		left := 2*index + 1
		right := 2*index + 2

		if left < len(bq.blocks) && bq.compare(left, largest) {
			largest = left
		}
		if right < len(bq.blocks) && bq.compare(right, largest) {
			largest = right
		}

		if largest == index {
			break
		}

		bq.blocks[index], bq.blocks[largest] = bq.blocks[largest], bq.blocks[index]
		index = largest
	}
}

func (bq *blockQueue) getAll() []blockInfo {
	// Sort the blocks before returning
	result := make([]blockInfo, len(bq.blocks))
	copy(result, bq.blocks)
	sort.Slice(result, func(i, j int) bool {
		return result[i].size < result[j].size
	})
	return result
}

func (pm *partMetadata) String() string {
	minTimestamp := time.Unix(0, pm.MinTimestamp).Format(time.Stamp)
	maxTimestamp := time.Unix(0, pm.MaxTimestamp).Format(time.Stamp)

	return fmt.Sprintf("%s, %s, %s, %s, %s, %s",
		minTimestamp, maxTimestamp, humanize.Bytes(pm.CompressedSizeBytes),
		humanize.Bytes(pm.UncompressedSpanSizeBytes), humanize.Comma(int64(pm.TotalCount)),
		humanize.Comma(int64(pm.BlocksCount)))
}

func (bc *blockCursor) String() string {
	return fmt.Sprintf("%d, %s, %d, %s",
		bc.p.partMetadata.ID, bc.bm.traceID, bc.bm.count, humanize.Bytes(bc.bm.uncompressedSpanSizeBytes))
}

func startBlockScanSpan(ctx context.Context, traceIDs []string, parts []*part) (func(*blockCursor), func(error)) {
	tracer := query.GetTracer(ctx)
	if tracer == nil {
		return nil, func(error) {}
	}

	span, _ := tracer.StartSpan(ctx, "scan-blocks")
	span.Tag("trace_id_count", strconv.Itoa(len(traceIDs)))
	if len(traceIDs) > 0 {
		limit := traceIDSampleLimit
		if limit > len(traceIDs) {
			limit = len(traceIDs)
		}
		span.Tag("trace_ids_sample", strings.Join(traceIDs[:limit], ","))
		if len(traceIDs) > limit {
			span.Tagf("trace_ids_omitted", "%d", len(traceIDs)-limit)
		}
	}
	span.Tag("part_header", partMetadataHeader)
	span.Tag("part_count", strconv.Itoa(len(parts)))
	for i := range parts {
		if parts[i] == nil {
			continue
		}
		span.Tag(fmt.Sprintf("part_%d_%s", parts[i].partMetadata.ID, parts[i].path), parts[i].partMetadata.String())
	}

	var (
		totalBytes uint64
		blockCount int
		tailQueue  = newBlockQueue(blockTailLimit, true)  // Keep smallest blocks
		headQueue  = newBlockQueue(blockHeadLimit, false) // Keep largest blocks
	)

	return func(bc *blockCursor) {
			if bc == nil {
				return
			}
			blockSize := bc.bm.uncompressedSpanSizeBytes
			blockString := bc.String()
			blockCount++
			totalBytes += blockSize

			block := blockInfo{
				blockString: blockString,
				size:        blockSize,
			}

			// Add to both queues - they will emit items before adding if at capacity
			tailQueue.add(block)
			headQueue.add(block)
		}, func(err error) {
			// Get blocks from both queues (already sorted)
			tailBlocks := tailQueue.getAll()
			headBlocks := headQueue.getAll()

			// Merge and deduplicate blocks
			blockMap := make(map[string]blockInfo)
			for _, block := range tailBlocks {
				blockMap[block.blockString] = block
			}
			for _, block := range headBlocks {
				blockMap[block.blockString] = block
			}

			// Convert to sorted slice
			var allBlocks []blockInfo
			for _, block := range blockMap {
				allBlocks = append(allBlocks, block)
			}
			sort.Slice(allBlocks, func(i, j int) bool {
				return allBlocks[i].size < allBlocks[j].size
			})

			// Prepare blocks for reporting
			var limitedBlocks []string
			for i := range allBlocks {
				limitedBlocks = append(limitedBlocks, allBlocks[i].blockString)
			}

			span.Tag("block_header", blockHeader)
			span.Tag("block_total_bytes", humanize.Bytes(totalBytes))
			span.Tag("block_count", strconv.Itoa(blockCount))
			span.Tag("block_limited_count", strconv.Itoa(len(limitedBlocks)))
			for i := range limitedBlocks {
				span.Tag(fmt.Sprintf("block_%d", i), limitedBlocks[i])
			}
			if err != nil {
				span.Error(err)
			}
			span.Stop()
		}
}
