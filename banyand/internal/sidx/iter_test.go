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
	testCases := []iterTestCase{
		{
			name: "single_part_single_block",
			parts: [][]testElement{
				{
					{
						seriesID: 1,
						userKey:  100,
						data:     []byte("data1"),
						tags: []tag{
							{name: "service", value: []byte("order-service"), valueType: pbv1.ValueTypeStr},
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
							{name: "service", value: []byte("order-service"), valueType: pbv1.ValueTypeStr},
						},
					},
					{
						seriesID: 3,
						userKey:  300,
						data:     []byte("data3"),
						tags: []tag{
							{name: "service", value: []byte("user-service"), valueType: pbv1.ValueTypeStr},
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
							{name: "service", value: []byte("payment-service"), valueType: pbv1.ValueTypeStr},
						},
					},
					{
						seriesID: 4,
						userKey:  400,
						data:     []byte("data4"),
						tags: []tag{
							{name: "service", value: []byte("notification-service"), valueType: pbv1.ValueTypeStr},
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
					{seriesID: 1, userKey: 100, data: []byte("data1"), tags: []tag{{name: "service", value: []byte("order-service"), valueType: pbv1.ValueTypeStr}}},
					{seriesID: 2, userKey: 200, data: []byte("data2"), tags: []tag{{name: "service", value: []byte("payment-service"), valueType: pbv1.ValueTypeStr}}},
				},
				// Part 2
				{
					{seriesID: 3, userKey: 300, data: []byte("data3"), tags: []tag{{name: "service", value: []byte("user-service"), valueType: pbv1.ValueTypeStr}}},
					{
						seriesID: 4, userKey: 400, data: []byte("data4"),
						tags: []tag{{name: "service", value: []byte("notification-service"), valueType: pbv1.ValueTypeStr}},
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
					{seriesID: 1, userKey: 50, data: []byte("data1"), tags: []tag{{name: "service", value: []byte("order-service"), valueType: pbv1.ValueTypeStr}}},
					{seriesID: 2, userKey: 150, data: []byte("data2"), tags: []tag{{name: "service", value: []byte("payment-service"), valueType: pbv1.ValueTypeStr}}},
				},
				// Part 2
				{
					{seriesID: 3, userKey: 250, data: []byte("data3"), tags: []tag{{name: "service", value: []byte("user-service"), valueType: pbv1.ValueTypeStr}}},
					{
						seriesID: 4, userKey: 350, data: []byte("data4"),
						tags: []tag{{name: "service", value: []byte("notification-service"), valueType: pbv1.ValueTypeStr}},
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
					{seriesID: 1, userKey: 100, data: []byte("data1"), tags: []tag{{name: "service", value: []byte("order-service"), valueType: pbv1.ValueTypeStr}}},
					{seriesID: 2, userKey: 200, data: []byte("data2"), tags: []tag{{name: "service", value: []byte("payment-service"), valueType: pbv1.ValueTypeStr}}},
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
					{seriesID: 1, userKey: 100, data: []byte("data1"), tags: []tag{{name: "service", value: []byte("order-service"), valueType: pbv1.ValueTypeStr}}},
					{seriesID: 2, userKey: 200, data: []byte("data2"), tags: []tag{{name: "service", value: []byte("payment-service"), valueType: pbv1.ValueTypeStr}}},
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
		partType := partType
		t.Run(partType, func(t *testing.T) {
			for _, tc := range testCases {
				tc := tc
				t.Run(tc.name, func(t *testing.T) {
					var parts []*part

					// Create parts based on type
					for i, partElements := range tc.parts {
						elements := createTestElements(partElements)
						defer releaseElements(elements)

						mp := GenerateMemPart()
						defer ReleaseMemPart(mp)
						mp.mustInitFromElements(elements)

						var testPart *part
						if partType == "file_based" {
							partDir := filepath.Join(tempDir, fmt.Sprintf("%s_%s_part%d", partType, tc.name, i))
							mp.mustFlush(testFS, partDir)
							testPart = mustOpenPart(uint64(i), partDir, testFS)
						} else {
							testPart = openMemPart(mp)
						}
						defer testPart.close()
						parts = append(parts, testPart)
					}

					ascBlocks := runIteratorPass(t, tc, parts, true)
					descBlocks := runIteratorPass(t, tc, parts, false)

					assertSameBlocksIgnoreOrder(t, ascBlocks, descBlocks)

					require.True(t, sort.SliceIsSorted(ascBlocks, func(i, j int) bool {
						if ascBlocks[i].minKey == ascBlocks[j].minKey {
							return ascBlocks[i].seriesID <= ascBlocks[j].seriesID
						}
						return ascBlocks[i].minKey <= ascBlocks[j].minKey
					}), "ascending pass should be ordered by non-decreasing minKey")

					require.True(t, sort.SliceIsSorted(descBlocks, func(i, j int) bool {
						if descBlocks[i].minKey == descBlocks[j].minKey {
							return descBlocks[i].seriesID >= descBlocks[j].seriesID
						}
						return descBlocks[i].minKey >= descBlocks[j].minKey
					}), "descending pass should be ordered by non-increasing minKey")

					if len(tc.expectOrder) > 0 {
						require.Equal(t, tc.expectOrder, ascBlocks, "ascending pass order should match expectation")
						require.Equal(t, reverseExpectations(tc.expectOrder), descBlocks, "descending pass should be reverse of expectation")
					}
				})
			}
		})
	}
}

func TestIterEdgeCases(t *testing.T) {
	testFS := fs.NewLocalFileSystem()
	tempDir := t.TempDir()

	t.Run("empty_parts_list", func(t *testing.T) {
		for _, asc := range []bool{true, false} {
			it := generateIter()
			it.init(nil, []common.SeriesID{1, 2, 3}, 100, 200, nil, asc)
			assert.False(t, it.nextBlock())
			assert.Nil(t, it.Error())
			releaseIter(it)
		}
	})

	t.Run("empty_series_list", func(t *testing.T) {
		// Create a part with data
		elements := createTestElements([]testElement{
			{seriesID: 1, userKey: 100, data: []byte("data1"), tags: []tag{{name: "service", value: []byte("test-service"), valueType: pbv1.ValueTypeStr}}},
		})
		defer releaseElements(elements)

		mp := GenerateMemPart()
		defer ReleaseMemPart(mp)
		mp.mustInitFromElements(elements)

		partDir := filepath.Join(tempDir, "empty_series")
		mp.mustFlush(testFS, partDir)
		testPart := mustOpenPart(1, partDir, testFS)
		defer testPart.close()

		for _, asc := range []bool{true, false} {
			it := generateIter()
			it.init([]*part{testPart}, []common.SeriesID{}, 0, 1000, nil, asc)
			assert.False(t, it.nextBlock())
			assert.Nil(t, it.Error())
			releaseIter(it)
		}
	})

	t.Run("no_matching_key_range", func(t *testing.T) {
		// Create parts with data at key 100-300
		elements1 := createTestElements([]testElement{
			{seriesID: 1, userKey: 100, data: []byte("data1"), tags: []tag{{name: "service", value: []byte("test-service1"), valueType: pbv1.ValueTypeStr}}},
		})
		defer releaseElements(elements1)

		elements2 := createTestElements([]testElement{
			{seriesID: 2, userKey: 300, data: []byte("data2"), tags: []tag{{name: "service", value: []byte("test-service2"), valueType: pbv1.ValueTypeStr}}},
		})
		defer releaseElements(elements2)

		mp1 := GenerateMemPart()
		defer ReleaseMemPart(mp1)
		mp1.mustInitFromElements(elements1)

		mp2 := GenerateMemPart()
		defer ReleaseMemPart(mp2)
		mp2.mustInitFromElements(elements2)

		partDir1 := filepath.Join(tempDir, "no_match_part1")
		partDir2 := filepath.Join(tempDir, "no_match_part2")
		mp1.mustFlush(testFS, partDir1)
		mp2.mustFlush(testFS, partDir2)

		testPart1 := mustOpenPart(1, partDir1, testFS)
		defer testPart1.close()
		testPart2 := mustOpenPart(2, partDir2, testFS)
		defer testPart2.close()

		for _, asc := range []bool{true, false} {
			it := generateIter()
			// Query range that doesn't overlap with any blocks
			it.init([]*part{testPart1, testPart2}, []common.SeriesID{1, 2}, 400, 500, nil, asc)
			assert.False(t, it.nextBlock())
			assert.Nil(t, it.Error())
			releaseIter(it)
		}
	})

	t.Run("single_part_single_block", func(t *testing.T) {
		elements := createTestElements([]testElement{
			{seriesID: 1, userKey: 100, data: []byte("data1"), tags: []tag{{name: "service", value: []byte("test-service"), valueType: pbv1.ValueTypeStr}}},
		})
		defer releaseElements(elements)

		mp := GenerateMemPart()
		defer ReleaseMemPart(mp)
		mp.mustInitFromElements(elements)

		partDir := filepath.Join(tempDir, "single_part")
		mp.mustFlush(testFS, partDir)
		testPart := mustOpenPart(1, partDir, testFS)
		defer testPart.close()

		for _, asc := range []bool{true, false} {
			it := generateIter()
			it.init([]*part{testPart}, []common.SeriesID{1}, 50, 150, nil, asc)

			assert.True(t, it.nextBlock())
			assert.False(t, it.nextBlock()) // Should be only one block
			assert.Nil(t, it.Error())
			releaseIter(it)
		}
	})

	t.Run("block_filter_error", func(t *testing.T) {
		elements := createTestElements([]testElement{
			{seriesID: 1, userKey: 100, data: []byte("data1"), tags: []tag{{name: "service", value: []byte("test-service"), valueType: pbv1.ValueTypeStr}}},
		})
		defer releaseElements(elements)

		mp := GenerateMemPart()
		defer ReleaseMemPart(mp)
		mp.mustInitFromElements(elements)

		testPart := openMemPart(mp)
		defer testPart.close()

		expectedErr := fmt.Errorf("test filter error")
		mockFilter := &mockBlockFilter{err: expectedErr}

		for _, asc := range []bool{true, false} {
			it := generateIter()
			it.init([]*part{testPart}, []common.SeriesID{1}, 0, 200, mockFilter, asc)

			assert.False(t, it.nextBlock())
			assert.Error(t, it.Error())
			assert.Contains(t, it.Error().Error(), "cannot initialize sidx iteration")
			releaseIter(it)
		}
	})
}

func TestIterOrdering(t *testing.T) {
	testFS := fs.NewLocalFileSystem()
	tempDir := t.TempDir()

	// Test ordering across multiple parts with interleaved series
	t.Run("interleaved_series_ordering", func(t *testing.T) {
		// Part 1: series 1, 3, 5
		elements1 := createTestElements([]testElement{
			{seriesID: 1, userKey: 100, data: []byte("data1"), tags: []tag{{name: "service", value: []byte("service1"), valueType: pbv1.ValueTypeStr}}},
			{seriesID: 3, userKey: 300, data: []byte("data3"), tags: []tag{{name: "service", value: []byte("service3"), valueType: pbv1.ValueTypeStr}}},
			{seriesID: 5, userKey: 500, data: []byte("data5"), tags: []tag{{name: "service", value: []byte("service5"), valueType: pbv1.ValueTypeStr}}},
		})
		defer releaseElements(elements1)

		// Part 2: series 2, 4, 6
		elements2 := createTestElements([]testElement{
			{seriesID: 2, userKey: 200, data: []byte("data2"), tags: []tag{{name: "service", value: []byte("service2"), valueType: pbv1.ValueTypeStr}}},
			{seriesID: 4, userKey: 400, data: []byte("data4"), tags: []tag{{name: "service", value: []byte("service4"), valueType: pbv1.ValueTypeStr}}},
			{seriesID: 6, userKey: 600, data: []byte("data6"), tags: []tag{{name: "service", value: []byte("service6"), valueType: pbv1.ValueTypeStr}}},
		})
		defer releaseElements(elements2)

		mp1 := GenerateMemPart()
		defer ReleaseMemPart(mp1)
		mp1.mustInitFromElements(elements1)

		mp2 := GenerateMemPart()
		defer ReleaseMemPart(mp2)
		mp2.mustInitFromElements(elements2)

		partDir1 := filepath.Join(tempDir, "ordering_part1")
		partDir2 := filepath.Join(tempDir, "ordering_part2")
		mp1.mustFlush(testFS, partDir1)
		mp2.mustFlush(testFS, partDir2)

		testPart1 := mustOpenPart(1, partDir1, testFS)
		defer testPart1.close()
		testPart2 := mustOpenPart(2, partDir2, testFS)
		defer testPart2.close()

		tc := iterTestCase{
			name:         "interleaved_series_ordering",
			parts:        nil, // not used by helper during verification
			querySids:    []common.SeriesID{1, 2, 3, 4, 5, 6},
			minKey:       0,
			maxKey:       1000,
			blockFilter:  nil,
			expectBlocks: 6,
			expectOrder: []blockExpectation{
				{seriesID: 1, minKey: 100, maxKey: 100},
				{seriesID: 2, minKey: 200, maxKey: 200},
				{seriesID: 3, minKey: 300, maxKey: 300},
				{seriesID: 4, minKey: 400, maxKey: 400},
				{seriesID: 5, minKey: 500, maxKey: 500},
				{seriesID: 6, minKey: 600, maxKey: 600},
			},
		}

		ascBlocks := runIteratorPass(t, tc, []*part{testPart1, testPart2}, true)
		descBlocks := runIteratorPass(t, tc, []*part{testPart1, testPart2}, false)

		assertSameBlocksIgnoreOrder(t, ascBlocks, descBlocks)
		require.Len(t, ascBlocks, 6)

		require.True(t, sort.SliceIsSorted(ascBlocks, func(i, j int) bool {
			return ascBlocks[i].seriesID <= ascBlocks[j].seriesID
		}), "ascending iteration should retain increasing series order")

		require.True(t, sort.SliceIsSorted(descBlocks, func(i, j int) bool {
			return descBlocks[i].seriesID >= descBlocks[j].seriesID
		}), "descending iteration should retain decreasing series order")

		for _, expected := range tc.expectOrder {
			assert.Contains(t, ascBlocks, expected)
			assert.Contains(t, descBlocks, expected)
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
		assert.Equal(t, 0, len(it.partIters))
		assert.Equal(t, 0, len(it.heap))
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

func TestIterOverlappingBlockGroups(t *testing.T) {
	elementsPart1 := createTestElements([]testElement{
		{seriesID: 1, userKey: 100, data: []byte("p1-a")},
		{seriesID: 1, userKey: 200, data: []byte("p1-b")},
	})
	defer releaseElements(elementsPart1)

	mp1 := GenerateMemPart()
	defer ReleaseMemPart(mp1)
	mp1.mustInitFromElements(elementsPart1)
	part1 := openMemPart(mp1)
	defer part1.close()

	elementsPart2 := createTestElements([]testElement{
		{seriesID: 2, userKey: 150, data: []byte("p2-a")},
		{seriesID: 2, userKey: 180, data: []byte("p2-b")},
	})
	defer releaseElements(elementsPart2)

	mp2 := GenerateMemPart()
	defer ReleaseMemPart(mp2)
	mp2.mustInitFromElements(elementsPart2)
	part2 := openMemPart(mp2)
	defer part2.close()

	for _, asc := range []bool{true, false} {
		it := generateIter()
		it.init([]*part{part1, part2}, []common.SeriesID{1, 2}, 0, 500, nil, asc)

		require.True(t, it.nextBlock(), "expected overlapping groups on first iteration")
		require.Len(t, it.currentGroups, 2, "both parts should produce overlapping groups")

		partsSeen := make(map[*part]struct{})
		totalBlocks := 0
		for _, group := range it.currentGroups {
			require.NotNil(t, group)
			require.NotNil(t, group.part)
			require.NotEmpty(t, group.blocks)
			partsSeen[group.part] = struct{}{}
			totalBlocks += len(group.blocks)
		}

		require.Len(t, partsSeen, 2, "expected contributions from both parts")
		require.Equal(t, 2, totalBlocks, "should surface one block per part in overlapping aggregation")
		for _, group := range it.currentGroups {
			require.Len(t, group.blocks, 1, "each group should expose a single block metadata entry")
		}

		assert.False(t, it.nextBlock(), "no additional groups expected after initial overlap")
		assert.NoError(t, it.Error())
		releaseIter(it)
	}
}

// Helper types and functions.

type iterTestCase struct {
	blockFilter  index.Filter
	name         string
	parts        [][]testElement
	querySids    []common.SeriesID
	expectOrder  []blockExpectation
	minKey       int64
	maxKey       int64
	expectBlocks int
}

// mockBlockFilter is already defined in part_iter_test.go.

func runIteratorPass(t *testing.T, tc iterTestCase, parts []*part, asc bool) []blockExpectation {
	t.Helper()

	it := generateIter()
	defer releaseIter(it)

	it.init(parts, tc.querySids, tc.minKey, tc.maxKey, tc.blockFilter, asc)

	var foundBlocks []blockExpectation

	for it.nextBlock() {
		require.NotEmpty(t, it.currentGroups, "currentGroups should not be empty when nextBlock returns true (order=%s)", orderName(asc))
		for _, group := range it.currentGroups {
			require.NotNil(t, group)
			require.NotEmpty(t, group.blocks)
			for _, curBlock := range group.blocks {
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
		}
	}

	// Check for errors
	require.NoError(t, it.Error(), "iterator should not have errors")

	// Verify the number of blocks found
	assert.Equal(t, tc.expectBlocks, len(foundBlocks), "should find expected number of blocks")

	return foundBlocks
}

func reverseExpectations(src []blockExpectation) []blockExpectation {
	if len(src) == 0 {
		return nil
	}
	out := make([]blockExpectation, len(src))
	for i := range src {
		out[i] = src[len(src)-1-i]
	}
	return out
}

func assertSameBlocksIgnoreOrder(t *testing.T, left, right []blockExpectation) {
	t.Helper()

	require.Equal(t, len(left), len(right), "block counts should match across orders")

	counts := make(map[blockExpectation]int, len(left))
	for _, block := range left {
		counts[block]++
	}
	for _, block := range right {
		counts[block]--
		require.GreaterOrEqual(t, counts[block], 0, "unexpected block encountered %+v", block)
	}
	for block, count := range counts {
		require.Equal(t, 0, count, "missing block %+v in comparison", block)
	}
}
