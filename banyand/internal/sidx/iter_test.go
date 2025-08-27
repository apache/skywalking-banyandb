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
	"fmt"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/index"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

func TestIterComprehensive(t *testing.T) {
	testFS := fs.NewLocalFileSystem()
	tempDir := t.TempDir()

	// Test cases for comprehensive iterator testing
	testCases := []struct {
		blockFilter  index.Filter
		name         string
		parts        [][]testElement
		querySids    []common.SeriesID
		expectOrder  []blockExpectation
		minKey       int64
		maxKey       int64
		expectBlocks int
	}{
		{
			name: "single_part_single_block",
			parts: [][]testElement{
				{
					{
						seriesID: 1,
						userKey:  100,
						data:     []byte("data1"),
						tags: []tag{
							{name: "service", value: []byte("order-service"), valueType: pbv1.ValueTypeStr, indexed: true},
						},
					},
				},
			},
			querySids:    []common.SeriesID{1},
			minKey:       50,
			maxKey:       150,
			blockFilter:  nil,
			expectBlocks: 1,
			expectOrder: []blockExpectation{
				{seriesID: 1, minKey: 100, maxKey: 100},
			},
		},
		{
			name: "multiple_parts_multiple_series",
			parts: [][]testElement{
				// Part 1
				{
					{
						seriesID: 1,
						userKey:  100,
						data:     []byte("data1"),
						tags: []tag{
							{name: "service", value: []byte("order-service"), valueType: pbv1.ValueTypeStr, indexed: true},
						},
					},
					{
						seriesID: 3,
						userKey:  300,
						data:     []byte("data3"),
						tags: []tag{
							{name: "service", value: []byte("user-service"), valueType: pbv1.ValueTypeStr, indexed: true},
						},
					},
				},
				// Part 2
				{
					{
						seriesID: 2,
						userKey:  200,
						data:     []byte("data2"),
						tags: []tag{
							{name: "service", value: []byte("payment-service"), valueType: pbv1.ValueTypeStr, indexed: true},
						},
					},
					{
						seriesID: 4,
						userKey:  400,
						data:     []byte("data4"),
						tags: []tag{
							{name: "service", value: []byte("notification-service"), valueType: pbv1.ValueTypeStr, indexed: true},
						},
					},
				},
			},
			querySids:    []common.SeriesID{1, 2, 3, 4},
			minKey:       0,
			maxKey:       500,
			blockFilter:  nil,
			expectBlocks: 4,
			expectOrder: []blockExpectation{
				{seriesID: 1, minKey: 100, maxKey: 100},
				{seriesID: 2, minKey: 200, maxKey: 200},
				{seriesID: 3, minKey: 300, maxKey: 300},
				{seriesID: 4, minKey: 400, maxKey: 400},
			},
		},
		{
			name: "filtering_by_series_id",
			parts: [][]testElement{
				// Part 1
				{
					{seriesID: 1, userKey: 100, data: []byte("data1"), tags: []tag{{name: "service", value: []byte("order-service"), valueType: pbv1.ValueTypeStr, indexed: true}}},
					{seriesID: 2, userKey: 200, data: []byte("data2"), tags: []tag{{name: "service", value: []byte("payment-service"), valueType: pbv1.ValueTypeStr, indexed: true}}},
				},
				// Part 2
				{
					{seriesID: 3, userKey: 300, data: []byte("data3"), tags: []tag{{name: "service", value: []byte("user-service"), valueType: pbv1.ValueTypeStr, indexed: true}}},
					{
						seriesID: 4, userKey: 400, data: []byte("data4"),
						tags: []tag{{name: "service", value: []byte("notification-service"), valueType: pbv1.ValueTypeStr, indexed: true}},
					},
				},
			},
			querySids:    []common.SeriesID{2, 4}, // Only series 2 and 4
			minKey:       0,
			maxKey:       500,
			blockFilter:  nil,
			expectBlocks: 2,
			expectOrder: []blockExpectation{
				{seriesID: 2, minKey: 200, maxKey: 200},
				{seriesID: 4, minKey: 400, maxKey: 400},
			},
		},
		{
			name: "filtering_by_key_range",
			parts: [][]testElement{
				// Part 1
				{
					{seriesID: 1, userKey: 50, data: []byte("data1"), tags: []tag{{name: "service", value: []byte("order-service"), valueType: pbv1.ValueTypeStr, indexed: true}}},
					{seriesID: 2, userKey: 150, data: []byte("data2"), tags: []tag{{name: "service", value: []byte("payment-service"), valueType: pbv1.ValueTypeStr, indexed: true}}},
				},
				// Part 2
				{
					{seriesID: 3, userKey: 250, data: []byte("data3"), tags: []tag{{name: "service", value: []byte("user-service"), valueType: pbv1.ValueTypeStr, indexed: true}}},
					{
						seriesID: 4, userKey: 350, data: []byte("data4"),
						tags: []tag{{name: "service", value: []byte("notification-service"), valueType: pbv1.ValueTypeStr, indexed: true}},
					},
				},
			},
			querySids:    []common.SeriesID{1, 2, 3, 4},
			minKey:       100, // Should include series 2, 3, 4 (blocks with maxKey >= 100)
			maxKey:       300, // Should include series 1, 2, 3 (blocks with minKey <= 300)
			blockFilter:  nil,
			expectBlocks: 2, // Series 2, 3 overlap with range [100, 300], series 1 has maxKey=50 < minKey=100, series 4 has minKey=350 > maxKey=300
			expectOrder: []blockExpectation{
				{seriesID: 2, minKey: 150, maxKey: 150},
				{seriesID: 3, minKey: 250, maxKey: 250},
			},
		},
		{
			name: "with_block_filter_allow_all",
			parts: [][]testElement{
				{
					{seriesID: 1, userKey: 100, data: []byte("data1"), tags: []tag{{name: "service", value: []byte("order-service"), valueType: pbv1.ValueTypeStr, indexed: true}}},
					{seriesID: 2, userKey: 200, data: []byte("data2"), tags: []tag{{name: "service", value: []byte("payment-service"), valueType: pbv1.ValueTypeStr, indexed: true}}},
				},
			},
			querySids:    []common.SeriesID{1, 2},
			minKey:       0,
			maxKey:       300,
			blockFilter:  &mockBlockFilter{shouldSkip: false},
			expectBlocks: 2,
			expectOrder: []blockExpectation{
				{seriesID: 1, minKey: 100, maxKey: 100},
				{seriesID: 2, minKey: 200, maxKey: 200},
			},
		},
		{
			name: "with_block_filter_skip_all",
			parts: [][]testElement{
				{
					{seriesID: 1, userKey: 100, data: []byte("data1"), tags: []tag{{name: "service", value: []byte("order-service"), valueType: pbv1.ValueTypeStr, indexed: true}}},
					{seriesID: 2, userKey: 200, data: []byte("data2"), tags: []tag{{name: "service", value: []byte("payment-service"), valueType: pbv1.ValueTypeStr, indexed: true}}},
				},
			},
			querySids:    []common.SeriesID{1, 2},
			minKey:       0,
			maxKey:       300,
			blockFilter:  &mockBlockFilter{shouldSkip: true},
			expectBlocks: 0,
			expectOrder:  []blockExpectation{},
		},
	}

	// Test both file-based and memory-based parts
	for _, partType := range []string{"file_based", "memory_based"} {
		t.Run(partType, func(t *testing.T) {
			for _, tc := range testCases {
				t.Run(tc.name, func(t *testing.T) {
					var parts []*part

					// Create parts based on type
					for i, partElements := range tc.parts {
						elements := createTestElements(partElements)
						defer releaseElements(elements)

						mp := generateMemPart()
						defer releaseMemPart(mp)
						mp.mustInitFromElements(elements)

						var testPart *part
						if partType == "file_based" {
							partDir := filepath.Join(tempDir, fmt.Sprintf("%s_%s_part%d", partType, tc.name, i))
							mp.mustFlush(testFS, partDir)
							testPart = mustOpenPart(partDir, testFS)
						} else {
							testPart = openMemPart(mp)
						}
						defer testPart.close()
						parts = append(parts, testPart)
					}

					// Test the iterator
					runIteratorTest(t, tc, parts)
				})
			}
		})
	}
}

func TestIterEdgeCases(t *testing.T) {
	testFS := fs.NewLocalFileSystem()
	tempDir := t.TempDir()

	t.Run("empty_parts_list", func(t *testing.T) {
		bma := generateBlockMetadataArray()
		defer releaseBlockMetadataArray(bma)

		it := generateIter()
		defer releaseIter(it)

		it.init(bma, nil, []common.SeriesID{1, 2, 3}, 100, 200, nil)
		assert.False(t, it.nextBlock())
		assert.Nil(t, it.Error())
	})

	t.Run("empty_series_list", func(t *testing.T) {
		// Create a part with data
		elements := createTestElements([]testElement{
			{seriesID: 1, userKey: 100, data: []byte("data1"), tags: []tag{{name: "service", value: []byte("test-service"), valueType: pbv1.ValueTypeStr, indexed: true}}},
		})
		defer releaseElements(elements)

		mp := generateMemPart()
		defer releaseMemPart(mp)
		mp.mustInitFromElements(elements)

		partDir := filepath.Join(tempDir, "empty_series")
		mp.mustFlush(testFS, partDir)
		testPart := mustOpenPart(partDir, testFS)
		defer testPart.close()

		bma := generateBlockMetadataArray()
		defer releaseBlockMetadataArray(bma)

		it := generateIter()
		defer releaseIter(it)

		it.init(bma, []*part{testPart}, []common.SeriesID{}, 0, 1000, nil)
		assert.False(t, it.nextBlock())
		assert.Nil(t, it.Error())
	})

	t.Run("no_matching_key_range", func(t *testing.T) {
		// Create parts with data at key 100-300
		elements1 := createTestElements([]testElement{
			{seriesID: 1, userKey: 100, data: []byte("data1"), tags: []tag{{name: "service", value: []byte("test-service1"), valueType: pbv1.ValueTypeStr, indexed: true}}},
		})
		defer releaseElements(elements1)

		elements2 := createTestElements([]testElement{
			{seriesID: 2, userKey: 300, data: []byte("data2"), tags: []tag{{name: "service", value: []byte("test-service2"), valueType: pbv1.ValueTypeStr, indexed: true}}},
		})
		defer releaseElements(elements2)

		mp1 := generateMemPart()
		defer releaseMemPart(mp1)
		mp1.mustInitFromElements(elements1)

		mp2 := generateMemPart()
		defer releaseMemPart(mp2)
		mp2.mustInitFromElements(elements2)

		partDir1 := filepath.Join(tempDir, "no_match_part1")
		partDir2 := filepath.Join(tempDir, "no_match_part2")
		mp1.mustFlush(testFS, partDir1)
		mp2.mustFlush(testFS, partDir2)

		testPart1 := mustOpenPart(partDir1, testFS)
		defer testPart1.close()
		testPart2 := mustOpenPart(partDir2, testFS)
		defer testPart2.close()

		bma := generateBlockMetadataArray()
		defer releaseBlockMetadataArray(bma)

		it := generateIter()
		defer releaseIter(it)

		// Query range that doesn't overlap with any blocks
		it.init(bma, []*part{testPart1, testPart2}, []common.SeriesID{1, 2}, 400, 500, nil)
		assert.False(t, it.nextBlock())
		assert.Nil(t, it.Error())
	})

	t.Run("single_part_single_block", func(t *testing.T) {
		elements := createTestElements([]testElement{
			{seriesID: 1, userKey: 100, data: []byte("data1"), tags: []tag{{name: "service", value: []byte("test-service"), valueType: pbv1.ValueTypeStr, indexed: true}}},
		})
		defer releaseElements(elements)

		mp := generateMemPart()
		defer releaseMemPart(mp)
		mp.mustInitFromElements(elements)

		partDir := filepath.Join(tempDir, "single_part")
		mp.mustFlush(testFS, partDir)
		testPart := mustOpenPart(partDir, testFS)
		defer testPart.close()

		bma := generateBlockMetadataArray()
		defer releaseBlockMetadataArray(bma)

		it := generateIter()
		defer releaseIter(it)

		it.init(bma, []*part{testPart}, []common.SeriesID{1}, 50, 150, nil)

		assert.True(t, it.nextBlock())
		assert.False(t, it.nextBlock()) // Should be only one block
		assert.Nil(t, it.Error())
	})

	t.Run("block_filter_error", func(t *testing.T) {
		elements := createTestElements([]testElement{
			{seriesID: 1, userKey: 100, data: []byte("data1"), tags: []tag{{name: "service", value: []byte("test-service"), valueType: pbv1.ValueTypeStr, indexed: true}}},
		})
		defer releaseElements(elements)

		mp := generateMemPart()
		defer releaseMemPart(mp)
		mp.mustInitFromElements(elements)

		testPart := openMemPart(mp)
		defer testPart.close()

		bma := generateBlockMetadataArray()
		defer releaseBlockMetadataArray(bma)

		it := generateIter()
		defer releaseIter(it)

		expectedErr := fmt.Errorf("test filter error")
		mockFilter := &mockBlockFilter{err: expectedErr}

		it.init(bma, []*part{testPart}, []common.SeriesID{1}, 0, 200, mockFilter)

		assert.False(t, it.nextBlock())
		assert.Error(t, it.Error())
		assert.Contains(t, it.Error().Error(), "cannot initialize sidx iteration")
	})
}

func TestIterOrdering(t *testing.T) {
	testFS := fs.NewLocalFileSystem()
	tempDir := t.TempDir()

	// Test ordering across multiple parts with interleaved series
	t.Run("interleaved_series_ordering", func(t *testing.T) {
		// Part 1: series 1, 3, 5
		elements1 := createTestElements([]testElement{
			{seriesID: 1, userKey: 100, data: []byte("data1"), tags: []tag{{name: "service", value: []byte("service1"), valueType: pbv1.ValueTypeStr, indexed: true}}},
			{seriesID: 3, userKey: 300, data: []byte("data3"), tags: []tag{{name: "service", value: []byte("service3"), valueType: pbv1.ValueTypeStr, indexed: true}}},
			{seriesID: 5, userKey: 500, data: []byte("data5"), tags: []tag{{name: "service", value: []byte("service5"), valueType: pbv1.ValueTypeStr, indexed: true}}},
		})
		defer releaseElements(elements1)

		// Part 2: series 2, 4, 6
		elements2 := createTestElements([]testElement{
			{seriesID: 2, userKey: 200, data: []byte("data2"), tags: []tag{{name: "service", value: []byte("service2"), valueType: pbv1.ValueTypeStr, indexed: true}}},
			{seriesID: 4, userKey: 400, data: []byte("data4"), tags: []tag{{name: "service", value: []byte("service4"), valueType: pbv1.ValueTypeStr, indexed: true}}},
			{seriesID: 6, userKey: 600, data: []byte("data6"), tags: []tag{{name: "service", value: []byte("service6"), valueType: pbv1.ValueTypeStr, indexed: true}}},
		})
		defer releaseElements(elements2)

		mp1 := generateMemPart()
		defer releaseMemPart(mp1)
		mp1.mustInitFromElements(elements1)

		mp2 := generateMemPart()
		defer releaseMemPart(mp2)
		mp2.mustInitFromElements(elements2)

		partDir1 := filepath.Join(tempDir, "ordering_part1")
		partDir2 := filepath.Join(tempDir, "ordering_part2")
		mp1.mustFlush(testFS, partDir1)
		mp2.mustFlush(testFS, partDir2)

		testPart1 := mustOpenPart(partDir1, testFS)
		defer testPart1.close()
		testPart2 := mustOpenPart(partDir2, testFS)
		defer testPart2.close()

		bma := generateBlockMetadataArray()
		defer releaseBlockMetadataArray(bma)

		it := generateIter()
		defer releaseIter(it)

		it.init(bma, []*part{testPart1, testPart2}, []common.SeriesID{1, 2, 3, 4, 5, 6}, 0, 1000, nil)

		// Blocks should come in series ID order: 1, 2, 3, 4, 5, 6
		var foundSeries []common.SeriesID
		for it.nextBlock() {
			// Access the current block from the heap - need to be careful about heap structure
			if len(it.piHeap) > 0 {
				foundSeries = append(foundSeries, it.piHeap[0].curBlock.seriesID)
			}
		}

		assert.NoError(t, it.Error())

		// We expect to find all 6 series
		assert.Equal(t, 6, len(foundSeries))

		// Verify ordering
		expectedOrder := []common.SeriesID{1, 2, 3, 4, 5, 6}
		assert.True(t, sort.SliceIsSorted(foundSeries, func(i, j int) bool {
			return foundSeries[i] < foundSeries[j]
		}), "found series should be in ascending order: %v", foundSeries)

		// All expected series should be found
		for _, expectedSeries := range expectedOrder {
			assert.Contains(t, foundSeries, expectedSeries, "should find series %d", expectedSeries)
		}
	})
}

func TestIterPoolOperations(t *testing.T) {
	t.Run("pool_operations", func(t *testing.T) {
		// Test pool operations
		it1 := generateIter()
		assert.NotNil(t, it1)

		it2 := generateIter()
		assert.NotNil(t, it2)

		// Release and get again
		releaseIter(it1)
		it3 := generateIter()
		assert.NotNil(t, it3)

		releaseIter(it2)
		releaseIter(it3)
	})

	t.Run("reset_functionality", func(t *testing.T) {
		it := generateIter()
		defer releaseIter(it)

		// Set some state
		it.err = fmt.Errorf("test error")
		it.nextBlockNoop = true

		// Reset should clear everything
		it.reset()
		assert.Nil(t, it.err)
		assert.Equal(t, 0, len(it.parts))
		assert.Equal(t, 0, len(it.piPool))
		assert.Equal(t, 0, len(it.piHeap))
		assert.False(t, it.nextBlockNoop)
	})
}

func TestBlockMetadataLess(t *testing.T) {
	t.Run("comparison_logic", func(t *testing.T) {
		// Test the less method for blockMetadata
		bm1 := &blockMetadata{seriesID: 1, minKey: 100}
		bm2 := &blockMetadata{seriesID: 1, minKey: 200}
		bm3 := &blockMetadata{seriesID: 2, minKey: 50}

		// Same seriesID, compare by minKey
		assert.True(t, bm1.less(bm2))
		assert.False(t, bm2.less(bm1))

		// Different seriesID, compare by seriesID
		assert.True(t, bm1.less(bm3))
		assert.False(t, bm3.less(bm1))

		// Same values should not be less
		bm4 := &blockMetadata{seriesID: 1, minKey: 100}
		assert.False(t, bm1.less(bm4))
		assert.False(t, bm4.less(bm1))
	})
}

// Helper types and functions.

type blockExpectation struct {
	seriesID common.SeriesID
	minKey   int64
	maxKey   int64
}

// mockBlockFilter is already defined in part_iter_test.go.

// runIteratorTest runs the iterator test with the given test case and parts.
func runIteratorTest(t *testing.T, tc struct {
	blockFilter  index.Filter
	name         string
	parts        [][]testElement
	querySids    []common.SeriesID
	expectOrder  []blockExpectation
	minKey       int64
	maxKey       int64
	expectBlocks int
}, parts []*part,
) {
	bma := generateBlockMetadataArray()
	defer releaseBlockMetadataArray(bma)

	it := generateIter()
	defer releaseIter(it)

	it.init(bma, parts, tc.querySids, tc.minKey, tc.maxKey, tc.blockFilter)

	var foundBlocks []blockExpectation
	blockCount := 0

	for it.nextBlock() {
		blockCount++
		// Get the minimum block from heap (the one currently being processed)
		require.True(t, len(it.piHeap) > 0, "heap should not be empty when nextBlock returns true")

		curBlock := it.piHeap[0].curBlock
		t.Logf("Found block for seriesID %d, key range [%d, %d]", curBlock.seriesID, curBlock.minKey, curBlock.maxKey)

		// Verify the block overlaps with query range
		overlaps := curBlock.maxKey >= tc.minKey && curBlock.minKey <= tc.maxKey
		assert.True(t, overlaps, "block should overlap with query range [%d, %d], but got block range [%d, %d]",
			tc.minKey, tc.maxKey, curBlock.minKey, curBlock.maxKey)
		assert.Contains(t, tc.querySids, curBlock.seriesID, "block seriesID should be in query sids")

		foundBlocks = append(foundBlocks, blockExpectation{
			seriesID: curBlock.seriesID,
			minKey:   curBlock.minKey,
			maxKey:   curBlock.maxKey,
		})
	}

	// Check for errors
	require.NoError(t, it.Error(), "iterator should not have errors")

	// Verify the number of blocks found
	assert.Equal(t, tc.expectBlocks, len(foundBlocks), "should find expected number of blocks")

	// Verify ordering - blocks should come out in sorted order by (seriesID, minKey)
	assert.True(t, sort.SliceIsSorted(foundBlocks, func(i, j int) bool {
		if foundBlocks[i].seriesID == foundBlocks[j].seriesID {
			return foundBlocks[i].minKey < foundBlocks[j].minKey
		}
		return foundBlocks[i].seriesID < foundBlocks[j].seriesID
	}), "blocks should be in sorted order by (seriesID, minKey)")

	// If specific order is expected, verify it
	if len(tc.expectOrder) > 0 {
		require.Equal(t, len(tc.expectOrder), len(foundBlocks), "number of found blocks should match expected")
		for i, expected := range tc.expectOrder {
			assert.Equal(t, expected.seriesID, foundBlocks[i].seriesID, "block %d seriesID should match", i)
			assert.Equal(t, expected.minKey, foundBlocks[i].minKey, "block %d minKey should match", i)
			assert.Equal(t, expected.maxKey, foundBlocks[i].maxKey, "block %d maxKey should match", i)
		}
	}

	t.Logf("Test %s completed: found %d blocks, expected %d", tc.name, blockCount, tc.expectBlocks)
}
