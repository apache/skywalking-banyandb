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

	"github.com/apache/skywalking-banyandb/api/common"
	internalencoding "github.com/apache/skywalking-banyandb/banyand/internal/encoding"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/compress/zstd"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

// queryResult is used internally for processing logic only.
type queryResult struct {
	pm         protector.Memory
	l          *logger.Logger
	tagsToLoad map[string]struct{}
	request    QueryRequest
}

// loadBlockData loads block data from part using block metadata.
// Uses blockCursor pattern for optimal performance with selective tag loading.
func (qr *queryResult) loadBlockData(tmpBlock *block, p *part, bm *blockMetadata) bool {
	tmpBlock.reset()

	// Early exit if no data
	if bm.count == 0 {
		return false
	}

	// Check if readers are properly initialized
	if p.keys == nil || p.data == nil {
		return false
	}

	// Read user keys (always needed)
	bb := bigValuePool.Get()
	if bb == nil {
		bb = &bytes.Buffer{}
	}
	defer func() {
		bb.Buf = bb.Buf[:0]
		bigValuePool.Put(bb)
	}()

	bb.Buf = bytes.ResizeOver(bb.Buf[:0], int(bm.keysBlock.size))
	fs.MustReadData(p.keys, int64(bm.keysBlock.offset), bb.Buf)

	// Decode user keys directly
	var err error
	tmpBlock.userKeys, err = encoding.BytesToInt64List(tmpBlock.userKeys[:0], bb.Buf, bm.keysEncodeType, bm.minKey, int(bm.count))
	if err != nil {
		return false
	}

	// Read and decompress data payloads (always needed)
	bb2 := bigValuePool.Get()
	if bb2 == nil {
		bb2 = &bytes.Buffer{}
	}
	defer func() {
		bb2.Buf = bb2.Buf[:0]
		bigValuePool.Put(bb2)
	}()

	bb2.Buf = bytes.ResizeOver(bb2.Buf[:0], int(bm.dataBlock.size))
	fs.MustReadData(p.data, int64(bm.dataBlock.offset), bb2.Buf)

	dataBuf, err := zstd.Decompress(bb.Buf[:0], bb2.Buf)
	if err != nil {
		return false
	}

	// Decode data payloads
	decoder := &encoding.BytesBlockDecoder{}
	tmpBlock.data, err = decoder.Decode(tmpBlock.data[:0], dataBuf, bm.count)
	if err != nil {
		return false
	}

	// Use shared tagsToLoad map, or determine available tags for this block
	var tagsToLoad map[string]struct{}
	if len(qr.tagsToLoad) > 0 {
		// Use the shared projected tags map
		tagsToLoad = qr.tagsToLoad
	} else {
		// Load all available tags for this specific block
		tagsToLoad = make(map[string]struct{})
		for tagName := range bm.tagsBlocks {
			tagsToLoad[tagName] = struct{}{}
		}
	}

	// Early exit if no tags to load
	if len(tagsToLoad) == 0 {
		return len(tmpBlock.userKeys) > 0
	}

	// Load tag data for selected tags only
	for tagName := range tagsToLoad {
		tagBlockInfo, exists := bm.tagsBlocks[tagName]
		if !exists {
			continue // Skip missing tags
		}

		if !qr.loadTagData(tmpBlock, p, tagName, &tagBlockInfo, int(bm.count), decoder) {
			// Continue loading other tags even if one fails
			continue
		}
	}

	return len(tmpBlock.userKeys) > 0
}

// loadTagData loads data for a specific tag, following the pattern from readBlockTags.
func (qr *queryResult) loadTagData(tmpBlock *block, p *part, tagName string, tagBlockInfo *dataBlock, count int, decoder *encoding.BytesBlockDecoder) bool {
	// Get tag metadata reader
	tmReader, tmExists := p.getTagMetadataReader(tagName)
	if !tmExists {
		return false
	}

	// Get tag data reader
	tdReader, tdExists := p.getTagDataReader(tagName)
	if !tdExists {
		return false
	}

	// Read tag metadata
	bb := bigValuePool.Get()
	if bb == nil {
		bb = &bytes.Buffer{}
	}
	defer func() {
		bb.Buf = bb.Buf[:0]
		bigValuePool.Put(bb)
	}()

	bb.Buf = bytes.ResizeOver(bb.Buf[:0], int(tagBlockInfo.size))
	fs.MustReadData(tmReader, int64(tagBlockInfo.offset), bb.Buf)

	tm, err := unmarshalTagMetadata(bb.Buf)
	if err != nil {
		return false
	}
	defer releaseTagMetadata(tm)

	// Read and decompress tag data
	bb2 := bigValuePool.Get()
	if bb2 == nil {
		bb2 = &bytes.Buffer{}
	}
	defer func() {
		bb2.Buf = bb2.Buf[:0]
		bigValuePool.Put(bb2)
	}()

	bb2.Buf = bytes.ResizeOver(bb2.Buf[:0], int(tm.dataBlock.size))
	fs.MustReadData(tdReader, int64(tm.dataBlock.offset), bb2.Buf)

	// Create tag data structure and populate block
	td := generateTagData()
	// Decode tag values directly (no compression)
	td.values, err = internalencoding.DecodeTagValues(td.values[:0], decoder, bb2, tm.valueType, count)
	if err != nil {
		return false
	}

	td.name = tagName
	td.valueType = tm.valueType
	td.indexed = tm.indexed

	// Set min/max for int64 tags
	if tm.valueType == pbv1.ValueTypeInt64 {
		td.min = tm.min
		td.max = tm.max
	}

	// Create bloom filter for indexed tags if needed
	if tm.indexed {
		td.filter = generateBloomFilter(count)
		for _, value := range td.values {
			if value != nil {
				td.filter.Add(value)
			}
		}
	}

	tmpBlock.tags[tagName] = td
	return true
}

// convertBlockToResponse converts SIDX block data to QueryResponse format.
func (qr *queryResult) convertBlockToResponse(block *block, seriesID common.SeriesID, result *QueryResponse) {
	elemCount := len(block.userKeys)

	// Initialize unique Data tracking if needed
	if qr.request.MaxElementSize > 0 && result.uniqueDataMap == nil {
		result.uniqueDataMap = make(map[string]struct{})
		// Count existing unique data elements in result if any
		for _, data := range result.Data {
			result.uniqueDataMap[string(data)] = struct{}{}
		}
		result.uniqueDataCount = len(result.uniqueDataMap)
	}

	for i := 0; i < elemCount; i++ {
		// Filter by key range from QueryRequest
		key := block.userKeys[i]
		if qr.request.MinKey != nil && key < *qr.request.MinKey {
			continue
		}
		if qr.request.MaxKey != nil && key > *qr.request.MaxKey {
			continue
		}

		// Check unique Data element limit from request (only if positive)
		if qr.request.MaxElementSize > 0 {
			dataStr := string(block.data[i])
			if _, exists := result.uniqueDataMap[dataStr]; !exists {
				// New unique data element
				if result.uniqueDataCount >= qr.request.MaxElementSize {
					break
				}
				result.uniqueDataMap[dataStr] = struct{}{}
				result.uniqueDataCount++
			}
		}

		// Copy parallel arrays
		result.Keys = append(result.Keys, key)
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
						Name:      tagName,
						Value:     tagData.values[elemIndex],
						ValueType: tagData.valueType,
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
					Name:      tagName,
					Value:     tagData.values[elemIndex],
					ValueType: tagData.valueType,
				})
			}
		}
	}

	return elementTags
}

// mergeQueryResponseShards merges multiple QueryResponse shards.
func mergeQueryResponseShards(shards []*QueryResponse, maxElements int) *QueryResponse {
	// Create heap for ascending merge
	qrh := &QueryResponseHeap{asc: true}

	// Initialize cursors for non-empty shards
	for _, shard := range shards {
		if shard.Len() > 0 {
			qrh.cursors = append(qrh.cursors, &QueryResponseCursor{
				response: shard,
				idx:      0,
			})
		}
	}

	if len(qrh.cursors) == 0 {
		return &QueryResponse{
			Keys: make([]int64, 0),
			Data: make([][]byte, 0),
			Tags: make([][]Tag, 0),
			SIDs: make([]common.SeriesID, 0),
		}
	}

	// Initialize heap
	heap.Init(qrh)

	// Perform heap-based merge
	result := qrh.mergeWithHeap(maxElements)

	// Reset heap
	qrh.reset()

	return result
}

// mergeQueryResponseShardsDesc merges multiple QueryResponse shards in descending order.
func mergeQueryResponseShardsDesc(shards []*QueryResponse, maxElements int) *QueryResponse {
	// Create heap for descending merge
	qrh := &QueryResponseHeap{asc: false}

	// Initialize cursors for non-empty shards (start from end for descending)
	for _, shard := range shards {
		if shard.Len() > 0 {
			qrh.cursors = append(qrh.cursors, &QueryResponseCursor{
				response: shard,
				idx:      shard.Len() - 1, // Start from last element for descending
			})
		}
	}

	if len(qrh.cursors) == 0 {
		return &QueryResponse{
			Keys: make([]int64, 0),
			Data: make([][]byte, 0),
			Tags: make([][]Tag, 0),
			SIDs: make([]common.SeriesID, 0),
		}
	}

	// Initialize heap
	heap.Init(qrh)

	// Perform heap-based merge
	result := qrh.mergeWithHeap(maxElements)

	// Reset heap
	qrh.reset()

	return result
}

// QueryResponseCursor wraps a QueryResponse with current iteration position.
type QueryResponseCursor struct {
	response *QueryResponse
	idx      int
}

// QueryResponseHeap implements heap.Interface for merging QueryResponse shards.
type QueryResponseHeap struct {
	cursors []*QueryResponseCursor
	asc     bool
}

func (qrh QueryResponseHeap) Len() int {
	return len(qrh.cursors)
}

func (qrh QueryResponseHeap) Less(i, j int) bool {
	leftKey := qrh.cursors[i].response.Keys[qrh.cursors[i].idx]
	rightKey := qrh.cursors[j].response.Keys[qrh.cursors[j].idx]
	if qrh.asc {
		return leftKey < rightKey
	}
	return leftKey > rightKey
}

func (qrh *QueryResponseHeap) Swap(i, j int) {
	qrh.cursors[i], qrh.cursors[j] = qrh.cursors[j], qrh.cursors[i]
}

// Push adds an element to the heap.
func (qrh *QueryResponseHeap) Push(x interface{}) {
	qrh.cursors = append(qrh.cursors, x.(*QueryResponseCursor))
}

// Pop removes and returns the top element from the heap.
func (qrh *QueryResponseHeap) Pop() interface{} {
	old := qrh.cursors
	n := len(old)
	x := old[n-1]
	qrh.cursors = old[0 : n-1]
	return x
}

func (qrh *QueryResponseHeap) reset() {
	qrh.cursors = qrh.cursors[:0]
}

// mergeWithHeap performs heap-based merge of QueryResponse shards.
func (qrh *QueryResponseHeap) mergeWithHeap(limit int) *QueryResponse {
	result := &QueryResponse{
		Keys: make([]int64, 0, limit),
		Data: make([][]byte, 0, limit),
		Tags: make([][]Tag, 0, limit),
		SIDs: make([]common.SeriesID, 0, limit),
	}

	step := -1
	if qrh.asc {
		step = 1
	}

	// Track unique Data elements for limit enforcement
	var uniqueDataCount int
	var uniqueDataMap map[string]struct{}
	if limit > 0 {
		uniqueDataMap = make(map[string]struct{})
	}

	for qrh.Len() > 0 {
		topCursor := qrh.cursors[0]
		idx := topCursor.idx
		resp := topCursor.response

		// Check unique Data element limit before adding
		if limit > 0 {
			dataStr := string(resp.Data[idx])
			if _, exists := uniqueDataMap[dataStr]; !exists {
				// New unique data element
				if uniqueDataCount >= limit {
					break
				}
				uniqueDataMap[dataStr] = struct{}{}
				uniqueDataCount++
			}
		}

		// Copy element from top cursor
		result.Keys = append(result.Keys, resp.Keys[idx])
		result.Data = append(result.Data, resp.Data[idx])
		result.Tags = append(result.Tags, resp.Tags[idx])
		result.SIDs = append(result.SIDs, resp.SIDs[idx])

		// Advance cursor
		topCursor.idx += step

		if qrh.asc {
			if topCursor.idx >= resp.Len() {
				heap.Pop(qrh)
			} else {
				heap.Fix(qrh, 0)
			}
		} else {
			if topCursor.idx < 0 {
				heap.Pop(qrh)
			} else {
				heap.Fix(qrh, 0)
			}
		}
	}

	return result
}
