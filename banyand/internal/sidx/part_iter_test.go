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
		querySids   []common.SeriesID
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
							indexed:   true,
						},
					},
				},
			},
			querySids:   []common.SeriesID{1},
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
							indexed:   true,
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
							indexed:   true,
						},
					},
				},
			},
			querySids:   []common.SeriesID{1},
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
							indexed:   true,
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
							indexed:   true,
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
							indexed:   true,
						},
					},
				},
			},
			querySids:   []common.SeriesID{1, 2, 3},
			minKey:      50,
			maxKey:      250,
			expectedLen: 3,
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
							indexed:   true,
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
							indexed:   true,
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
							indexed:   true,
						},
					},
				},
			},
			querySids:   []common.SeriesID{1},
			minKey:      75,
			maxKey:      150,
			expectedLen: 1, // Block contains all elements [50-200], overlaps query range [75-150]
		},
		{
			name: "filtered by series ID",
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
							indexed:   true,
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
							indexed:   true,
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
							indexed:   true,
						},
					},
				},
			},
			querySids:   []common.SeriesID{2},
			minKey:      50,
			maxKey:      150,
			expectedLen: 1, // Only series 2 should match
		},
	}

	// Helper function to run test cases against a part
	runTestCase := func(t *testing.T, tt struct {
		name        string
		elements    []testElement
		querySids   []common.SeriesID
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
		pi.init(bma, part, tt.querySids, tt.minKey, tt.maxKey, nil)

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
			assert.Contains(t, tt.querySids, curBlock.seriesID, "block seriesID should be in query sids")

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

		// Additional verification: ensure all found elements match expected series
		for _, elem := range foundElements {
			assert.Contains(t, tt.querySids, elem.seriesID, "found element should have expected seriesID")
		}

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

				mp := generateMemPart()
				defer releaseMemPart(mp)

				mp.mustInitFromElements(elements)

				// Step 2: Create part from memPart by flushing to disk
				partDir := filepath.Join(tempDir, fmt.Sprintf("part_%s", tt.name))
				mp.mustFlush(testFS, partDir)

				part := mustOpenPart(partDir, testFS)
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

				mp := generateMemPart()
				defer releaseMemPart(mp)

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

	t.Run("empty series list", func(t *testing.T) {
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
						indexed:   true,
					},
				},
			},
		})
		defer releaseElements(elements)

		mp := generateMemPart()
		defer releaseMemPart(mp)
		mp.mustInitFromElements(elements)

		partDir := filepath.Join(tempDir, "empty_series_test")
		mp.mustFlush(testFS, partDir)

		part := mustOpenPart(partDir, testFS)
		defer part.close()

		// Test with empty series list
		bma := &blockMetadataArray{}
		defer bma.reset()
		pi := &partIter{}

		bma.reset()
		pi.init(bma, part, []common.SeriesID{}, 0, 1000, nil)

		// Should not find any blocks with empty series list
		foundAny := pi.nextBlock()
		assert.False(t, foundAny, "should not find any blocks with empty series list")
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
						indexed:   true,
					},
				},
			},
		})
		defer releaseElements(elements)

		mp := generateMemPart()
		defer releaseMemPart(mp)
		mp.mustInitFromElements(elements)

		partDir := filepath.Join(tempDir, "no_match_key_range")
		mp.mustFlush(testFS, partDir)

		part := mustOpenPart(partDir, testFS)
		defer part.close()

		// Test with non-overlapping key range
		bma := &blockMetadataArray{}
		defer bma.reset()
		pi := &partIter{}

		bma.reset()
		pi.init(bma, part, []common.SeriesID{1}, 200, 300, nil) // No overlap with key 100

		// Should not find any blocks
		foundAny := pi.nextBlock()
		assert.False(t, foundAny, "should not find any blocks with non-overlapping key range")
	})

	t.Run("no matching series ID", func(t *testing.T) {
		// Create a part with seriesID 1
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
						indexed:   true,
					},
				},
			},
		})
		defer releaseElements(elements)

		mp := generateMemPart()
		defer releaseMemPart(mp)
		mp.mustInitFromElements(elements)

		partDir := filepath.Join(tempDir, "no_match_series")
		mp.mustFlush(testFS, partDir)

		part := mustOpenPart(partDir, testFS)
		defer part.close()

		// Test with different series ID
		bma := &blockMetadataArray{}
		defer bma.reset()
		pi := &partIter{}

		bma.reset()
		pi.init(bma, part, []common.SeriesID{2}, 0, 200, nil) // Different series ID

		// Should not find any blocks
		foundAny := pi.nextBlock()
		assert.False(t, foundAny, "should not find any blocks with non-matching series ID")
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
						indexed:   true,
					},
				},
			},
		})
		defer releaseElements(elements)

		mp := generateMemPart()
		defer releaseMemPart(mp)
		mp.mustInitFromElements(elements)

		partDir := filepath.Join(tempDir, "nil_filter")
		mp.mustFlush(testFS, partDir)

		part := mustOpenPart(partDir, testFS)
		defer part.close()

		// Test with nil blockFilter
		bma := &blockMetadataArray{}
		defer bma.reset()
		pi := &partIter{}

		bma.reset()
		pi.init(bma, part, []common.SeriesID{1}, 0, 200, nil) // nil blockFilter

		// Should find the block
		foundAny := pi.nextBlock()
		assert.True(t, foundAny, "should find blocks when blockFilter is nil")
		assert.NoError(t, pi.error())
	})

	t.Run("blockFilter with mock filter that allows all", func(t *testing.T) {
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
						indexed:   true,
					},
				},
			},
		})
		defer releaseElements(elements)

		mp := generateMemPart()
		defer releaseMemPart(mp)
		mp.mustInitFromElements(elements)

		partDir := filepath.Join(tempDir, "allow_all_filter")
		mp.mustFlush(testFS, partDir)

		part := mustOpenPart(partDir, testFS)
		defer part.close()

		// Create a mock filter that allows all blocks
		mockFilter := &mockBlockFilter{shouldSkip: false}

		// Test with blockFilter that allows all
		bma := &blockMetadataArray{}
		defer bma.reset()
		pi := &partIter{}

		bma.reset()
		pi.init(bma, part, []common.SeriesID{1}, 0, 200, mockFilter)

		// Should find the block
		foundAny := pi.nextBlock()
		assert.True(t, foundAny, "should find blocks when blockFilter allows all")
		assert.NoError(t, pi.error())
	})

	t.Run("blockFilter with mock filter that skips all", func(t *testing.T) {
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
						indexed:   true,
					},
				},
			},
		})
		defer releaseElements(elements)

		mp := generateMemPart()
		defer releaseMemPart(mp)
		mp.mustInitFromElements(elements)

		partDir := filepath.Join(tempDir, "skip_all_filter")
		mp.mustFlush(testFS, partDir)

		part := mustOpenPart(partDir, testFS)
		defer part.close()

		// Create a mock filter that skips all blocks
		mockFilter := &mockBlockFilter{shouldSkip: true}

		// Test with blockFilter that skips all
		bma := &blockMetadataArray{}
		defer bma.reset()
		pi := &partIter{}

		bma.reset()
		pi.init(bma, part, []common.SeriesID{1}, 0, 200, mockFilter)

		// Should not find any blocks
		foundAny := pi.nextBlock()
		assert.False(t, foundAny, "should not find blocks when blockFilter skips all")
		assert.NoError(t, pi.error())
	})

	t.Run("blockFilter with error should propagate error", func(t *testing.T) {
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
						indexed:   true,
					},
				},
			},
		})
		defer releaseElements(elements)

		mp := generateMemPart()
		defer releaseMemPart(mp)
		mp.mustInitFromElements(elements)

		partDir := filepath.Join(tempDir, "error_filter")
		mp.mustFlush(testFS, partDir)

		part := mustOpenPart(partDir, testFS)
		defer part.close()

		// Create a mock filter that returns an error
		expectedErr := fmt.Errorf("test filter error")
		mockFilter := &mockBlockFilter{shouldSkip: false, err: expectedErr}

		// Test with blockFilter that returns an error
		bma := &blockMetadataArray{}
		defer bma.reset()
		pi := &partIter{}

		bma.reset()
		pi.init(bma, part, []common.SeriesID{1}, 0, 200, mockFilter)

		// Should not find any blocks and should have error
		foundAny := pi.nextBlock()
		assert.False(t, foundAny, "should not find blocks when blockFilter returns error")
		assert.Error(t, pi.error())
		assert.Contains(t, pi.error().Error(), "test filter error")
	})
}

// mockBlockFilter is a mock implementation of index.Filter for testing.
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
