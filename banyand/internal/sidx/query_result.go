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
	"sync"

	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/cgroups"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// queryResult implements QueryResult interface with worker pool pattern.
// Following the tsResult architecture from the stream module.
type queryResult struct {
	// Core query components
	snapshot *snapshot    // Reference to current snapshot
	request  QueryRequest // Original query request (contains all parameters)
	ctx      context.Context

	// Block scanning components
	bs    *blockScanner // Block scanner for iteration
	parts []*part       // Selected parts for query

	// Worker coordination
	shards []*QueryResponse // Result shards from parallel workers
	pm     protector.Memory // Memory quota management
	l      *logger.Logger   // Logger instance

	// Query parameters (derived from request)
	asc bool // Ordering direction

	// State management
	released bool
}

// Pull returns the next batch of query results using parallel worker processing.
func (qr *queryResult) Pull() *QueryResponse {
	if qr.released || qr.bs == nil {
		return nil
	}

	return qr.runBlockScanner()
}

// runBlockScanner coordinates the worker pool with block scanner following tsResult pattern.
func (qr *queryResult) runBlockScanner() *QueryResponse {
	workerSize := cgroups.CPUs()
	batchCh := make(chan *blockScanResultBatch, workerSize)

	// Initialize worker result shards
	if qr.shards == nil {
		qr.shards = make([]*QueryResponse, workerSize)
		for i := range qr.shards {
			qr.shards[i] = &QueryResponse{
				Keys: make([]int64, 0),
				Data: make([][]byte, 0),
				Tags: make([][]Tag, 0),
				SIDs: make([]common.SeriesID, 0),
			}
		}
	} else {
		// Reset existing shards
		for i := range qr.shards {
			qr.shards[i].Reset()
		}
	}

	// Launch worker pool
	var workerWg sync.WaitGroup
	workerWg.Add(workerSize)

	for i := range workerSize {
		go func(workerID int) {
			defer workerWg.Done()
			qr.processWorkerBatches(workerID, batchCh)
		}(i)
	}

	// Start block scanning
	go func() {
		qr.bs.scan(qr.ctx, batchCh)
		close(batchCh)
	}()

	workerWg.Wait()

	// Check for completion
	if len(qr.bs.parts) == 0 {
		qr.bs.close()
		qr.bs = nil
	}

	// Merge results from all workers
	return qr.mergeWorkerResults()
}

// processWorkerBatches processes batches in a worker goroutine.
func (qr *queryResult) processWorkerBatches(workerID int, batchCh chan *blockScanResultBatch) {
	tmpBlock := generateBlock()
	defer releaseBlock(tmpBlock)

	for batch := range batchCh {
		if batch.err != nil {
			qr.shards[workerID].Error = batch.err
			releaseBlockScanResultBatch(batch)
			continue
		}

		for _, bs := range batch.bss {
			qr.loadAndProcessBlock(tmpBlock, bs, qr.shards[workerID])
		}

		releaseBlockScanResultBatch(batch)
	}
}

// loadAndProcessBlock loads a block from part and processes it into QueryResponse format.
func (qr *queryResult) loadAndProcessBlock(tmpBlock *block, bs blockScanResult, result *QueryResponse) bool {
	tmpBlock.reset()

	// Load block data from part (similar to stream's loadBlockCursor)
	if !qr.loadBlockData(tmpBlock, bs.p, &bs.bm) {
		return false
	}

	// Convert block data to QueryResponse format
	qr.convertBlockToResponse(tmpBlock, bs.bm.seriesID, result)

	return true
}

// loadBlockData loads block data from part using block metadata.
func (qr *queryResult) loadBlockData(tmpBlock *block, p *part, bm *blockMetadata) bool {
	// TODO: Implement actual block loading from part
	// This should use the existing block reader functionality
	_ = tmpBlock // TODO: use when implementing block loading
	_ = p        // TODO: use when implementing block loading
	_ = bm       // TODO: use when implementing block loading
	// For now, return false to avoid processing
	return false
}

// convertBlockToResponse converts SIDX block data to QueryResponse format.
func (qr *queryResult) convertBlockToResponse(block *block, seriesID common.SeriesID, result *QueryResponse) {
	elemCount := len(block.userKeys)

	for i := 0; i < elemCount; i++ {
		// Apply MaxElementSize limit from request
		if result.Len() >= qr.request.MaxElementSize {
			break
		}

		// Copy parallel arrays
		result.Keys = append(result.Keys, block.userKeys[i])
		result.Data = append(result.Data, block.data[i])
		result.SIDs = append(result.SIDs, seriesID)

		// Convert tag map to tag slice for this element
		elementTags := qr.extractElementTags(block, i)
		result.Tags = append(result.Tags, elementTags)
	}
}

// extractElementTags extracts tags for a specific element with projection support.
func (qr *queryResult) extractElementTags(block *block, elemIndex int) []Tag {
	var elementTags []Tag

	// Apply tag projection from request
	if len(qr.request.TagProjection) > 0 {
		elementTags = make([]Tag, 0, len(qr.request.TagProjection))
		for _, proj := range qr.request.TagProjection {
			for _, tagName := range proj.Names {
				if tagData, exists := block.tags[tagName]; exists && elemIndex < len(tagData.values) {
					elementTags = append(elementTags, Tag{
						name:      tagName,
						value:     tagData.values[elemIndex],
						valueType: tagData.valueType,
					})
				}
			}
		}
	} else {
		// Include all tags if no projection specified
		elementTags = make([]Tag, 0, len(block.tags))
		for tagName, tagData := range block.tags {
			if elemIndex < len(tagData.values) {
				elementTags = append(elementTags, Tag{
					name:      tagName,
					value:     tagData.values[elemIndex],
					valueType: tagData.valueType,
				})
			}
		}
	}

	return elementTags
}

// mergeWorkerResults merges results from all worker shards with error handling.
func (qr *queryResult) mergeWorkerResults() *QueryResponse {
	// Check for errors first
	var err error
	for i := range qr.shards {
		if qr.shards[i].Error != nil {
			err = multierr.Append(err, qr.shards[i].Error)
		}
	}

	if err != nil {
		return &QueryResponse{Error: err}
	}

	// Merge results with ordering from request
	if qr.asc {
		return mergeQueryResponseShardsAsc(qr.shards, qr.request.MaxElementSize)
	} else {
		return mergeQueryResponseShardsDesc(qr.shards, qr.request.MaxElementSize)
	}
}

// Release releases resources associated with the query result.
func (qr *queryResult) Release() {
	if qr.released {
		return
	}
	qr.released = true

	if qr.bs != nil {
		qr.bs.close()
	}

	if qr.snapshot != nil {
		qr.snapshot.decRef()
		qr.snapshot = nil
	}
}

// mergeQueryResponseShardsAsc merges multiple QueryResponse shards in ascending order.
func mergeQueryResponseShardsAsc(shards []*QueryResponse, maxElements int) *QueryResponse {
	result := &QueryResponse{
		Keys: make([]int64, 0, maxElements),
		Data: make([][]byte, 0, maxElements),
		Tags: make([][]Tag, 0, maxElements),
		SIDs: make([]common.SeriesID, 0, maxElements),
	}

	// Simple concatenation for now - TODO: implement proper merge sort
	for _, shard := range shards {
		for i := 0; i < shard.Len() && result.Len() < maxElements; i++ {
			result.Keys = append(result.Keys, shard.Keys[i])
			result.Data = append(result.Data, shard.Data[i])
			result.Tags = append(result.Tags, shard.Tags[i])
			result.SIDs = append(result.SIDs, shard.SIDs[i])
		}
	}

	return result
}

// mergeQueryResponseShardsDesc merges multiple QueryResponse shards in descending order.
func mergeQueryResponseShardsDesc(shards []*QueryResponse, maxElements int) *QueryResponse {
	result := &QueryResponse{
		Keys: make([]int64, 0, maxElements),
		Data: make([][]byte, 0, maxElements),
		Tags: make([][]Tag, 0, maxElements),
		SIDs: make([]common.SeriesID, 0, maxElements),
	}

	// Simple concatenation for now - TODO: implement proper merge sort
	for _, shard := range shards {
		for i := 0; i < shard.Len() && result.Len() < maxElements; i++ {
			result.Keys = append(result.Keys, shard.Keys[i])
			result.Data = append(result.Data, shard.Data[i])
			result.Tags = append(result.Tags, shard.Tags[i])
			result.SIDs = append(result.SIDs, shard.SIDs[i])
		}
	}

	return result
}
