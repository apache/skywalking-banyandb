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
	"math"

	"github.com/apache/skywalking-banyandb/api/common"
)

// ScanQuery executes a synchronous full-scan query.
func (s *sidx) ScanQuery(ctx context.Context, req ScanQueryRequest) ([]*QueryResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	snap := s.currentSnapshot()
	if snap == nil {
		return nil, nil
	}
	defer snap.decRef()

	// Set default batch size
	maxBatchSize := req.MaxBatchSize
	if maxBatchSize <= 0 {
		maxBatchSize = 1000
	}

	// Set key range
	minKey := int64(math.MinInt64)
	maxKey := int64(math.MaxInt64)
	if req.MinKey != nil {
		minKey = *req.MinKey
	}
	if req.MaxKey != nil {
		maxKey = *req.MaxKey
	}

	var results []*QueryResponse

	// Prepare Tags map if projection is specified
	var tagsMap map[string][]string
	if len(req.TagProjection) > 0 {
		tagsMap = make(map[string][]string)
		for _, proj := range req.TagProjection {
			for _, tagName := range proj.Names {
				tagsMap[tagName] = make([]string, 0, maxBatchSize)
			}
		}
	}

	currentBatch := &QueryResponse{
		Keys:    make([]int64, 0, maxBatchSize),
		Data:    make([][]byte, 0, maxBatchSize),
		SIDs:    make([]common.SeriesID, 0, maxBatchSize),
		PartIDs: make([]uint64, 0, maxBatchSize),
		Tags:    tagsMap,
	}

	// Filter parts by key and time range
	var filteredParts []*partWrapper
	for _, pw := range snap.parts {
		if !pw.overlapsKeyRange(minKey, maxKey) {
			continue
		}
		if !pw.overlapsTimeRange(req.MinTimestamp, req.MaxTimestamp) {
			continue
		}
		filteredParts = append(filteredParts, pw)
	}

	// Scan filtered parts
	totalParts := len(filteredParts)
	for partIdx, pw := range filteredParts {
		var err error
		if currentBatch, err = s.scanPart(ctx, pw, req, minKey, maxKey, &results, currentBatch, maxBatchSize); err != nil {
			return nil, err
		}

		// Count total rows found so far
		totalRowsFound := 0
		for _, res := range results {
			totalRowsFound += res.Len()
		}
		totalRowsFound += currentBatch.Len()

		// Report progress if callback is provided
		if req.OnProgress != nil {
			req.OnProgress(partIdx+1, totalParts, totalRowsFound)
		}
	}

	// Add remaining batch if not empty
	if currentBatch.Len() > 0 {
		results = append(results, currentBatch)
	}

	return results, nil
}

func (s *sidx) scanPart(ctx context.Context, pw *partWrapper, req ScanQueryRequest,
	minKey, maxKey int64, results *[]*QueryResponse, currentBatch *QueryResponse,
	maxBatchSize int,
) (*QueryResponse, error) {
	p := pw.p
	bma := generateBlockMetadataArray()
	defer releaseBlockMetadataArray(bma)

	// Create partIter on stack
	pi := &partIter{}

	// Initialize iterator for full scan
	pi.init(bma, p, minKey, maxKey)

	// Iterate through all blocks
	for pi.nextBlock() {
		if err := ctx.Err(); err != nil {
			return currentBatch, err
		}

		// Create a temporary QueryRequest to reuse existing blockCursor infrastructure
		tmpReq := QueryRequest{
			TagFilter:     req.TagFilter,
			MinKey:        &minKey,
			MaxKey:        &maxKey,
			TagProjection: req.TagProjection,
			MaxBatchSize:  maxBatchSize,
		}

		bc := generateBlockCursor()
		bc.init(p, pi.curBlock, tmpReq)

		// Load block data
		tmpBlock := generateBlock()

		// When we have a TagFilter, we need to load ALL tags so the filter can check them.
		// Pass nil to loadBlockCursor to load all available tags.
		var tagsToLoad map[string]struct{}
		if req.TagFilter != nil {
			// Load all tags when filtering (nil/empty map triggers loading all tags)
			tagsToLoad = nil
		} else if len(req.TagProjection) > 0 {
			// Optimize: only load projected tags when no filter
			tagsToLoad = make(map[string]struct{})
			for _, proj := range req.TagProjection {
				for _, tagName := range proj.Names {
					tagsToLoad[tagName] = struct{}{}
				}
			}
		}

		if s.loadBlockCursor(bc, tmpBlock, blockScanResult{
			p:  p,
			bm: *pi.curBlock,
		}, tagsToLoad, tmpReq, s.pm, nil) {
			// Copy all rows from this block cursor to current batch
			for idx := 0; idx < len(bc.userKeys); idx++ {
				bc.idx = idx

				// Check if batch is full before adding
				if currentBatch.Len() >= maxBatchSize {
					*results = append(*results, currentBatch)

					// Prepare Tags map for new batch if projection is specified
					var newTagsMap map[string][]string
					if len(req.TagProjection) > 0 {
						newTagsMap = make(map[string][]string)
						for _, proj := range req.TagProjection {
							for _, tagName := range proj.Names {
								newTagsMap[tagName] = make([]string, 0, maxBatchSize)
							}
						}
					}

					currentBatch = &QueryResponse{
						Keys:    make([]int64, 0, maxBatchSize),
						Data:    make([][]byte, 0, maxBatchSize),
						SIDs:    make([]common.SeriesID, 0, maxBatchSize),
						PartIDs: make([]uint64, 0, maxBatchSize),
						Tags:    newTagsMap,
					}
				}

				// Add to current batch
				_ = bc.copyTo(currentBatch)
			}
		}

		releaseBlock(tmpBlock)
		releaseBlockCursor(bc)
	}

	return currentBatch, pi.error()
}
