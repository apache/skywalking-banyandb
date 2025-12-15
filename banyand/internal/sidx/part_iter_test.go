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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/posting"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

func TestPartIterVerification(t *testing.T) {
	// Shared test cases for both subtests
	tests := []struct {
		name        string
		elements    []testElement
		minKey      int64
		maxKey      int64
		expectedLen int
	}{
		{
			name: "single series single element",
			elements: []testElement{
				{
					seriesID: 1,
					userKey:  100,
					data:     []byte("data1"),
					tags: []tag{
						{
							name:      "service",
							value:     []byte("order-service"),
							valueType: pbv1.ValueTypeStr,
						},
					},
				},
			},
			minKey:      50,
			maxKey:      150,
			expectedLen: 1,
		},
		{
			name: "single series multiple elements",
			elements: []testElement{
				{
					seriesID: 1,
					userKey:  100,
					data:     []byte("data1"),
					tags: []tag{
						{
							name:      "service",
							value:     []byte("order-service"),
							valueType: pbv1.ValueTypeStr,
						},
					},
				},
				{
					seriesID: 1,
					userKey:  200,
					data:     []byte("data2"),
					tags: []tag{
						{
							name:      "service",
							value:     []byte("order-service"),
							valueType: pbv1.ValueTypeStr,
						},
					},
				},
			},
			minKey:      50,
			maxKey:      250,
			expectedLen: 1, // Elements from same series are grouped into 1 block
		},
		{
			name: "multiple series",
			elements: []testElement{
				{
					seriesID: 1,
					userKey:  100,
					data:     []byte("data1"),
					tags: []tag{
						{
							name:      "service",
							value:     []byte("order-service"),
							valueType: pbv1.ValueTypeStr,
						},
					},
				},
				{
					seriesID: 2,
					userKey:  150,
					data:     []byte("data2"),
					tags: []tag{
						{
							name:      "service",
							value:     []byte("payment-service"),
							valueType: pbv1.ValueTypeStr,
						},
					},
				},
				{
					seriesID: 3,
					userKey:  200,
					data:     []byte("data3"),
					tags: []tag{
						{
							name:      "service",
							value:     []byte("user-service"),
							valueType: pbv1.ValueTypeStr,
						},
					},
				},
			},
			minKey:      50,
			maxKey:      250,
			expectedLen: 3, // All series returned in full scan
		},
		{
			name: "filtered by key range",
			elements: []testElement{
				{
					seriesID: 1,
					userKey:  50,
					data:     []byte("data1"),
					tags: []tag{
						{
							name:      "service",
							value:     []byte("order-service"),
							valueType: pbv1.ValueTypeStr,
						},
					},
				},
				{
					seriesID: 1,
					userKey:  100,
					data:     []byte("data2"),
					tags: []tag{
						{
							name:      "service",
							value:     []byte("order-service"),
							valueType: pbv1.ValueTypeStr,
						},
					},
				},
				{
					seriesID: 1,
					userKey:  200,
					data:     []byte("data3"),
					tags: []tag{
						{
							name:      "service",
							value:     []byte("order-service"),
							valueType: pbv1.ValueTypeStr,
						},
					},
				},
			},
			minKey:      75,
			maxKey:      150,
			expectedLen: 1, // Block contains all elements [50-200], overlaps query range [75-150]
		},
		{
			name: "all series returned in range",
			elements: []testElement{
				{
					seriesID: 1,
					userKey:  100,
					data:     []byte("data1"),
					tags: []tag{
						{
							name:      "service",
							value:     []byte("order-service"),
							valueType: pbv1.ValueTypeStr,
						},
					},
				},
				{
					seriesID: 2,
					userKey:  100,
					data:     []byte("data2"),
					tags: []tag{
						{
							name:      "service",
							value:     []byte("payment-service"),
							valueType: pbv1.ValueTypeStr,
						},
					},
				},
				{
					seriesID: 3,
					userKey:  100,
					data:     []byte("data3"),
					tags: []tag{
						{
							name:      "service",
							value:     []byte("user-service"),
							valueType: pbv1.ValueTypeStr,
						},
					},
				},
			},
			minKey:      50,
			maxKey:      150,
			expectedLen: 3, // All series in full scan mode
		},
	}

	// Helper function to run test cases against a part
	runTestCase := func(t *testing.T, tt struct {
		name        string
		elements    []testElement
		minKey      int64
		maxKey      int64
		expectedLen int
	}, part *part,
	) {
		// Create partIter and blockMetadataArray
		bma := &blockMetadataArray{}
		defer bma.reset()

		pi := &partIter{}

		// Initialize partIter with clean blockMetadataArray
		bma.reset() // Keep blockMetadataArray clean before passing to init
		pi.init(bma, part, tt.minKey, tt.maxKey)

		// Iterate through blocks and collect results
		var foundElements []testElement
		blockCount := 0

		for pi.nextBlock() {
			blockCount++
			curBlock := pi.curBlock

			t.Logf("Found block for seriesID %d, key range [%d, %d], count: %d",
				curBlock.seriesID, curBlock.minKey, curBlock.maxKey, curBlock.count)

			// Verify the block overlaps with query range (partIter returns overlapping blocks)
			overlaps := curBlock.maxKey >= tt.minKey && curBlock.minKey <= tt.maxKey
			assert.True(t, overlaps, "block should overlap with query range [%d, %d], but got block range [%d, %d]",
				tt.minKey, tt.maxKey, curBlock.minKey, curBlock.maxKey)

			// For verification, create a test element representing this block
			// Note: In a real scenario, you'd read the actual block data
			foundElements = append(foundElements, testElement{
				seriesID: curBlock.seriesID,
				userKey:  curBlock.minKey, // Use minKey as representative
				data:     nil,             // Not reading actual data in this test
				tags:     nil,             // Not reading actual tags in this test
			})
		}

		// Check for iteration errors
		require.NoError(t, pi.error(), "partIter should not have errors")

		// Verify results
		assert.Equal(t, tt.expectedLen, len(foundElements), "should find expected number of elements")

		t.Logf("Test %s completed: found %d blocks, expected %d", tt.name, blockCount, tt.expectedLen)
	}

	// Subtest 1: File-based part (existing approach)
	t.Run("file_based_part", func(t *testing.T) {
		testFS := fs.NewLocalFileSystem()
		tempDir := t.TempDir()

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				// Step 1: Create elements and initialize memPart
				elements := createTestElements(tt.elements)
				defer releaseElements(elements)

				mp := GenerateMemPart()
				defer ReleaseMemPart(mp)

				mp.mustInitFromElements(elements)

				// Step 2: Create part from memPart by flushing to disk
				partDir := filepath.Join(tempDir, fmt.Sprintf("part_%s", tt.name))
				mp.mustFlush(testFS, partDir)

				part := mustOpenPart(1, partDir, testFS)
				defer part.close()

				// Run the test case
				runTestCase(t, tt, part)
			})
		}
	})

	// Subtest 2: Memory-based part using openMemPart
	t.Run("memory_based_part", func(t *testing.T) {
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				// Step 1: Create elements and initialize memPart
				elements := createTestElements(tt.elements)
				defer releaseElements(elements)

				mp := GenerateMemPart()
				defer ReleaseMemPart(mp)

				mp.mustInitFromElements(elements)

				// Step 2: Create part directly from memPart using openMemPart
				part := openMemPart(mp)
				defer part.close()

				// Run the test case
				runTestCase(t, tt, part)
			})
		}
	})
}

func TestPartIterEdgeCases(t *testing.T) {
	testFS := fs.NewLocalFileSystem()
	tempDir := t.TempDir()

	t.Run("full scan with data", func(t *testing.T) {
		// Create a simple part with data
		elements := createTestElements([]testElement{
			{
				seriesID: 1,
				userKey:  100,
				data:     []byte("data1"),
				tags: []tag{
					{
						name:      "service",
						value:     []byte("test-service"),
						valueType: pbv1.ValueTypeStr,
					},
				},
			},
		})
		defer releaseElements(elements)

		mp := GenerateMemPart()
		defer ReleaseMemPart(mp)
		mp.mustInitFromElements(elements)

		partDir := filepath.Join(tempDir, "full_scan_test")
		mp.mustFlush(testFS, partDir)

		part := mustOpenPart(1, partDir, testFS)
		defer part.close()

		// Test full scan
		bma := &blockMetadataArray{}
		defer bma.reset()
		pi := &partIter{}

		bma.reset()
		pi.init(bma, part, 0, 1000)

		// Should find the block in full scan mode
		foundAny := pi.nextBlock()
		assert.True(t, foundAny, "should find blocks in full scan mode")
	})

	t.Run("no matching key range", func(t *testing.T) {
		// Create a part with data at key 100
		elements := createTestElements([]testElement{
			{
				seriesID: 1,
				userKey:  100,
				data:     []byte("data1"),
				tags: []tag{
					{
						name:      "service",
						value:     []byte("test-service"),
						valueType: pbv1.ValueTypeStr,
					},
				},
			},
		})
		defer releaseElements(elements)

		mp := GenerateMemPart()
		defer ReleaseMemPart(mp)
		mp.mustInitFromElements(elements)

		partDir := filepath.Join(tempDir, "no_match_key_range")
		mp.mustFlush(testFS, partDir)

		part := mustOpenPart(1, partDir, testFS)
		defer part.close()

		// Test with non-overlapping key range
		bma := &blockMetadataArray{}
		defer bma.reset()
		pi := &partIter{}

		bma.reset()
		pi.init(bma, part, 200, 300) // No overlap with key 100

		// Should not find any blocks
		foundAny := pi.nextBlock()
		assert.False(t, foundAny, "should not find any blocks with non-overlapping key range")
	})

	t.Run("all series returned in full scan", func(t *testing.T) {
		// Create a part with multiple series
		elements := createTestElements([]testElement{
			{
				seriesID: 1,
				userKey:  100,
				data:     []byte("data1"),
				tags: []tag{
					{
						name:      "service",
						value:     []byte("test-service-1"),
						valueType: pbv1.ValueTypeStr,
					},
				},
			},
			{
				seriesID: 2,
				userKey:  100,
				data:     []byte("data2"),
				tags: []tag{
					{
						name:      "service",
						value:     []byte("test-service-2"),
						valueType: pbv1.ValueTypeStr,
					},
				},
			},
		})
		defer releaseElements(elements)

		mp := GenerateMemPart()
		defer ReleaseMemPart(mp)
		mp.mustInitFromElements(elements)

		partDir := filepath.Join(tempDir, "all_series")
		mp.mustFlush(testFS, partDir)

		part := mustOpenPart(1, partDir, testFS)
		defer part.close()

		// Full scan returns all series
		bma := &blockMetadataArray{}
		defer bma.reset()
		pi := &partIter{}

		bma.reset()
		pi.init(bma, part, 0, 200)

		// Should find blocks for all series
		count := 0
		for pi.nextBlock() {
			count++
		}
		assert.Equal(t, 2, count, "should find blocks for all series in full scan mode")
	})
}

func TestPartIterBlockFilter(t *testing.T) {
	tempDir := t.TempDir()
	testFS := fs.NewLocalFileSystem()

	t.Run("blockFilter nil should not filter blocks", func(t *testing.T) {
		// Create test elements
		elements := createTestElements([]testElement{
			{
				seriesID: 1,
				userKey:  100,
				data:     []byte("data1"),
				tags: []tag{
					{
						name:      "service",
						value:     []byte("test-service"),
						valueType: pbv1.ValueTypeStr,
					},
				},
			},
		})
		defer releaseElements(elements)

		mp := GenerateMemPart()
		defer ReleaseMemPart(mp)
		mp.mustInitFromElements(elements)

		partDir := filepath.Join(tempDir, "nil_filter")
		mp.mustFlush(testFS, partDir)

		part := mustOpenPart(1, partDir, testFS)
		defer part.close()

		// Test with nil blockFilter (note: blockFilter removed from init, always nil now)
		bma := &blockMetadataArray{}
		defer bma.reset()
		pi := &partIter{}

		bma.reset()
		pi.init(bma, part, 0, 200)

		// Should find the block
		foundAny := pi.nextBlock()
		assert.True(t, foundAny, "should find blocks")
		assert.NoError(t, pi.error())
	})

	// Note: blockFilter tests removed as blockFilter is no longer supported in partIter.init()
	// The blockFilter functionality is still available in partIter.findBlock() but is always nil
	// when initialized through init(). For block filtering, use the ScanQuery API with TagFilter instead.
}

func TestPartIterFullScan(t *testing.T) {
	tempDir := t.TempDir()
	testFS := fs.NewLocalFileSystem()

	// Test full-scan behavior: Create elements for multiple series
	var allElements []testElement

	// First series: seriesID=1
	for i := 0; i < 100; i++ {
		allElements = append(allElements, testElement{
			seriesID: 1,
			userKey:  int64(i),
			data:     []byte(fmt.Sprintf("series1_data_%d", i)),
			tags: []tag{
				{
					name:      "service",
					value:     []byte("service-1"),
					valueType: pbv1.ValueTypeStr,
				},
			},
		})
	}

	// Second series: seriesID=2
	for i := 0; i < 100; i++ {
		allElements = append(allElements, testElement{
			seriesID: 2,
			userKey:  int64(i),
			data:     []byte(fmt.Sprintf("series2_data_%d", i)),
			tags: []tag{
				{
					name:      "service",
					value:     []byte("service-2"),
					valueType: pbv1.ValueTypeStr,
				},
			},
		})
	}

	// Third series: seriesID=3
	allElements = append(allElements, testElement{
		seriesID: 3,
		userKey:  5000,
		data:     []byte("series3_data"),
		tags: []tag{
			{
				name:      "service",
				value:     []byte("service-3"),
				valueType: pbv1.ValueTypeStr,
			},
		},
	})

	elements := createTestElements(allElements)
	defer releaseElements(elements)

	// Create memPart and flush it
	mp := GenerateMemPart()
	defer ReleaseMemPart(mp)
	mp.mustInitFromElements(elements)

	partDir := filepath.Join(tempDir, "test_part")
	mp.mustFlush(testFS, partDir)

	part := mustOpenPart(1, partDir, testFS)
	defer part.close()

	// Log the blocks in the part to understand the structure
	t.Logf("Part has %d primary block metadata entries", len(part.primaryBlockMetadata))
	for i, pbm := range part.primaryBlockMetadata {
		t.Logf("  Primary block %d: seriesID=%d, keys [%d-%d]", i, pbm.seriesID, pbm.minKey, pbm.maxKey)
	}

	// Test full scan - should return all series
	bma := &blockMetadataArray{}
	defer bma.reset()
	pi := &partIter{}

	bma.reset()
	pi.init(bma, part, 0, 10000)

	// Iterate through blocks and collect results
	var foundBlocks []struct {
		seriesID common.SeriesID
		minKey   int64
		maxKey   int64
	}

	for pi.nextBlock() {
		foundBlocks = append(foundBlocks, struct {
			seriesID common.SeriesID
			minKey   int64
			maxKey   int64
		}{
			seriesID: pi.curBlock.seriesID,
			minKey:   pi.curBlock.minKey,
			maxKey:   pi.curBlock.maxKey,
		})
		t.Logf("Found block: seriesID=%d, minKey=%d, maxKey=%d",
			pi.curBlock.seriesID, pi.curBlock.minKey, pi.curBlock.maxKey)
	}

	require.NoError(t, pi.error())

	// Verify all series are found in full scan mode
	foundSeries := make(map[common.SeriesID]bool)
	for _, block := range foundBlocks {
		foundSeries[block.seriesID] = true
	}

	assert.True(t, foundSeries[1], "should find series 1 in full scan")
	assert.True(t, foundSeries[2], "should find series 2 in full scan")
	assert.True(t, foundSeries[3], "should find series 3 in full scan")
	assert.Equal(t, 3, len(foundSeries), "should find all 3 series in full scan mode")
}

// mockBlockFilter is a mock implementation of index.Filter for testing.
// NOTE: This is kept for other test files that still reference it.
type mockBlockFilter struct {
	err        error
	shouldSkip bool
}

func (mbf *mockBlockFilter) ShouldSkip(_ index.FilterOp) (bool, error) {
	if mbf.err != nil {
		return false, mbf.err
	}
	return mbf.shouldSkip, nil
}

// These methods are required to satisfy the index.Filter interface.
func (mbf *mockBlockFilter) String() string {
	return "mockBlockFilter"
}

func (mbf *mockBlockFilter) Execute(_ index.GetSearcher, _ common.SeriesID, _ *index.RangeOpts) (posting.List, posting.List, error) {
	// Not used in our tests, return empty implementation
	return nil, nil, nil
}

// selectiveMockBlockFilter is a mock filter that selectively skips blocks based on tag values.
// NOTE: This is kept for other test files that still reference it.
type selectiveMockBlockFilter struct {
	tagName       string
	skipValue     string
	skipCallCount int
}

func (smf *selectiveMockBlockFilter) ShouldSkip(filterOp index.FilterOp) (bool, error) {
	smf.skipCallCount++
	// Check if the block contains the skip value for the specified tag
	hasSkipValue := filterOp.Eq(smf.tagName, smf.skipValue)
	return hasSkipValue, nil
}

func (smf *selectiveMockBlockFilter) String() string {
	return fmt.Sprintf("selectiveMockBlockFilter(tag=%s, skipValue=%s)", smf.tagName, smf.skipValue)
}

func (smf *selectiveMockBlockFilter) Execute(_ index.GetSearcher, _ common.SeriesID, _ *index.RangeOpts) (posting.List, posting.List, error) {
	return nil, nil, nil
}
