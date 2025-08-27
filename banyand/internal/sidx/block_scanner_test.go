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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/internal/test"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

func TestBlockScannerStructures(t *testing.T) {
	// Test blockScanResult
	bsr := &blockScanResult{}
	assert.NotNil(t, bsr)
	bsr.reset()
	assert.Nil(t, bsr.p)

	// Test blockScanResultBatch
	batch := generateBlockScanResultBatch()
	require.NotNil(t, batch)
	assert.Equal(t, 0, len(batch.bss))
	assert.Equal(t, blockScannerBatchSize, cap(batch.bss))
	assert.NoError(t, batch.err)

	// Test batch reset
	batch.err = errors.New("test error")
	batch.bss = append(batch.bss, blockScanResult{})
	batch.reset()
	assert.NoError(t, batch.err)
	assert.Equal(t, 0, len(batch.bss))

	releaseBlockScanResultBatch(batch)
}

func TestBlockScanner_EmptyPartsHandling(t *testing.T) {
	// Test that scanner handles empty parts list correctly
	mockProtector := &test.MockMemoryProtector{
		ExpectQuotaExceeded: false,
	}

	scanner := &blockScanner{
		pm:        mockProtector,
		l:         logger.GetLogger(),
		parts:     []*part{}, // Empty parts list
		seriesIDs: []common.SeriesID{1},
		minKey:    0,
		maxKey:    1000,
		filter:    nil,
		asc:       true,
	}

	// Create channel for results
	ctx := context.Background()
	blockCh := make(chan *blockScanResultBatch, 1)

	// Run scanner - should return immediately due to empty parts
	go func() {
		defer close(blockCh)
		scanner.scan(ctx, blockCh)
	}()

	// Should receive no batches for empty parts
	receivedBatches := 0
	for range blockCh {
		receivedBatches++
	}

	assert.Equal(t, 0, receivedBatches, "should receive no batches for empty parts")
	scanner.close()
}

func TestBlockScanner_QuotaExceeded(t *testing.T) {
	type testCtx struct {
		name                string
		seriesIDs           []common.SeriesID
		seriesCount         int
		elementsPerSeries   int
		minKey              int64
		maxKey              int64
		expectQuotaExceeded bool
		asc                 bool
	}

	tests := []testCtx{
		{
			name:                "QuotaNotExceeded_Success",
			seriesCount:         2,
			elementsPerSeries:   3,
			expectQuotaExceeded: false,
			seriesIDs:           []common.SeriesID{1, 2},
			minKey:              0,
			maxKey:              9999999999999999, // Very large max to include all data
			asc:                 true,
		},
		{
			name:                "QuotaExceeded_ExpectError",
			seriesCount:         3,
			elementsPerSeries:   5,
			expectQuotaExceeded: true,
			seriesIDs:           []common.SeriesID{1, 2, 3},
			minKey:              0,
			maxKey:              9999999999999999, // Very large max to include all data
			asc:                 true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create real parts with actual data using the pattern from part_test.go
			var parts []*part

			// Create test elements for each series
			for i := 1; i <= tt.seriesCount; i++ {
				var testElements []testElement
				for j := 1; j <= tt.elementsPerSeries; j++ {
					testElements = append(testElements, testElement{
						seriesID: common.SeriesID(i),
						userKey:  int64(j * 100),
						data:     []byte("test-data-" + string(rune('0'+i)) + "-" + string(rune('0'+j))),
						tags: []tag{
							{
								name:      "service",
								value:     []byte("test-service"),
								valueType: pbv1.ValueTypeStr,
								indexed:   true,
							},
							{
								name:      "instance",
								value:     []byte("instance-" + string(rune('0'+i))),
								valueType: pbv1.ValueTypeStr,
								indexed:   true,
							},
						},
					})
				}

				// Create elements from test data
				elements := createTestElements(testElements)
				defer releaseElements(elements)

				// Create memPart and initialize with elements
				mp := generateMemPart()
				defer releaseMemPart(mp)
				mp.mustInitFromElements(elements)

				// Create part from memPart (in-memory approach - faster for tests)
				part := openMemPart(mp)
				defer part.close()

				parts = append(parts, part)
			}

			// Create block scanner with memory protector
			scanner := &blockScanner{
				pm:        &test.MockMemoryProtector{ExpectQuotaExceeded: tt.expectQuotaExceeded},
				l:         logger.GetLogger(),
				parts:     parts,
				seriesIDs: tt.seriesIDs,
				minKey:    tt.minKey,
				maxKey:    tt.maxKey,
				filter:    nil,
				asc:       tt.asc,
			}

			// Run the scanner
			ctx := context.Background()
			blockCh := make(chan *blockScanResultBatch, 10)
			var (
				got      []blockMetadata
				errSeen  bool
				quotaErr error
			)

			// Start scanner in goroutine
			go func() {
				defer close(blockCh)
				scanner.scan(ctx, blockCh)
			}()

			// Process results
			for batch := range blockCh {
				if batch.err != nil {
					errSeen = true
					quotaErr = batch.err
					if tt.expectQuotaExceeded {
						require.Error(t, batch.err)
						require.Contains(t, batch.err.Error(), "quota exceeded")
						t.Logf("Successfully caught quota exceeded error: %v", batch.err)
					} else {
						t.Errorf("Unexpected error: %v", batch.err)
					}
				} else {
					for _, bs := range batch.bss {
						got = append(got, bs.bm)
					}
				}
				releaseBlockScanResultBatch(batch)
			}

			// Verify results based on expectations
			if tt.expectQuotaExceeded {
				assert.True(t, errSeen, "Expected to see quota exceeded error")
				if quotaErr != nil {
					assert.Contains(t, quotaErr.Error(), "quota exceeded", "Error should contain 'quota exceeded'")
				}
			} else {
				assert.False(t, errSeen, "Should not see any errors with sufficient quota")
				// Should have scanned some blocks with real data
				assert.Greater(t, len(got), 0, "Should have scanned some blocks with real data")
				t.Logf("Successfully scanned %d blocks", len(got))

				// Verify we got blocks from the expected series
				for _, block := range got {
					assert.Contains(t, tt.seriesIDs, block.seriesID, "Block should be from expected series")
					assert.Greater(t, block.count, uint64(0), "Block should have some elements")
					assert.Greater(t, block.uncompressedSize, uint64(0), "Block should have uncompressed size data")
				}
			}

			scanner.close()
		})
	}
}

func TestBlockScanner_ContextCancellation(t *testing.T) {
	// Create mock memory protector
	mockProtector := &test.MockMemoryProtector{
		ExpectQuotaExceeded: false,
	}

	// Use empty parts to test context cancellation behavior
	parts := []*part{}

	scanner := &blockScanner{
		pm:        mockProtector,
		l:         logger.GetLogger(),
		parts:     parts,
		seriesIDs: []common.SeriesID{1, 2, 3, 4, 5},
		minKey:    0,
		maxKey:    1000,
		filter:    nil,
		asc:       true,
	}

	ctx, cancel := context.WithCancel(context.Background())
	blockCh := make(chan *blockScanResultBatch, 10)

	// Start scanner
	go func() {
		defer close(blockCh)
		scanner.scan(ctx, blockCh)
	}()

	// Cancel context immediately
	cancel()

	// Verify scanner handles cancellation gracefully - with empty parts, should complete quickly
	batchesReceived := 0
	for batch := range blockCh {
		batchesReceived++
		releaseBlockScanResultBatch(batch)
	}

	// With empty parts, no batches should be received
	assert.Equal(t, 0, batchesReceived, "Empty parts should produce no batches even with canceled context")
	scanner.close()
}

func TestBlockScanner_FilteredScan(t *testing.T) {
	// Create mock memory protector
	mockProtector := &test.MockMemoryProtector{
		ExpectQuotaExceeded: false,
	}

	// Test with nil filter and empty parts
	parts := []*part{}
	var mockFilter index.Filter

	scanner := &blockScanner{
		pm:        mockProtector,
		l:         logger.GetLogger(),
		parts:     parts,
		seriesIDs: []common.SeriesID{1, 2, 3},
		minKey:    0,
		maxKey:    1000,
		filter:    mockFilter,
		asc:       true,
	}

	ctx := context.Background()
	blockCh := make(chan *blockScanResultBatch, 10)

	var scannedSeriesIDs []common.SeriesID

	// Run scanner
	go func() {
		defer close(blockCh)
		scanner.scan(ctx, blockCh)
	}()

	// Collect results
	for batch := range blockCh {
		if batch.err != nil {
			t.Errorf("Unexpected error: %v", batch.err)
		} else {
			for _, bs := range batch.bss {
				scannedSeriesIDs = append(scannedSeriesIDs, bs.bm.seriesID)
			}
		}
		releaseBlockScanResultBatch(batch)
	}

	// With empty parts, no series should be scanned
	assert.Empty(t, scannedSeriesIDs, "Empty parts should produce no scan results")
	scanner.close()
}

func TestBlockScanner_AscendingDescendingOrder(t *testing.T) {
	testCases := []struct {
		name string
		desc string
		asc  bool
	}{
		{
			name: "AscendingOrder",
			asc:  true,
			desc: "Scanner should process blocks in ascending order",
		},
		{
			name: "DescendingOrder",
			asc:  false,
			desc: "Scanner should process blocks in descending order",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockProtector := &test.MockMemoryProtector{
				ExpectQuotaExceeded: false,
			}

			// Test with empty parts - focus on configuration correctness
			parts := []*part{}

			scanner := &blockScanner{
				pm:        mockProtector,
				l:         logger.GetLogger(),
				parts:     parts,
				seriesIDs: []common.SeriesID{1, 2, 3},
				minKey:    0,
				maxKey:    1000,
				filter:    nil,
				asc:       tc.asc,
			}

			// Verify the scanner was configured with correct order
			assert.Equal(t, tc.asc, scanner.asc, "Scanner should be configured with correct ascending/descending order")

			ctx := context.Background()
			blockCh := make(chan *blockScanResultBatch, 10)

			// Run scanner to verify no panic with empty data
			go func() {
				defer close(blockCh)
				scanner.scan(ctx, blockCh)
			}()

			// Process results - should complete without issues
			batchCount := 0
			for batch := range blockCh {
				batchCount++
				assert.NoError(t, batch.err, "Should not error with empty parts")
				releaseBlockScanResultBatch(batch)
			}

			assert.Equal(t, 0, batchCount, "Empty parts should produce no batches")

			scanner.close()
		})
	}
}

func TestBlockScanner_BatchSizeHandling(t *testing.T) {
	mockProtector := &test.MockMemoryProtector{
		ExpectQuotaExceeded: false,
	}

	// Test batch size configuration with empty parts
	parts := []*part{}

	scanner := &blockScanner{
		pm:        mockProtector,
		l:         logger.GetLogger(),
		parts:     parts,
		seriesIDs: []common.SeriesID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		minKey:    0,
		maxKey:    1000,
		filter:    nil,
		asc:       true,
	}

	ctx := context.Background()
	blockCh := make(chan *blockScanResultBatch, 20)

	// Run scanner
	go func() {
		defer close(blockCh)
		scanner.scan(ctx, blockCh)
	}()

	// Analyze batch characteristics - should complete without issues
	batchCount := 0
	for batch := range blockCh {
		batchCount++
		assert.NoError(t, batch.err, "Should not error with empty parts")

		// Verify batch size doesn't exceed the configured limit (even if empty)
		batchSize := len(batch.bss)
		assert.LessOrEqual(t, batchSize, blockScannerBatchSize,
			"Batch size should not exceed configured limit")

		releaseBlockScanResultBatch(batch)
	}

	// With empty parts, expect 0 batches
	assert.Equal(t, 0, batchCount, "Empty parts should produce no batches")

	// Verify the constant is reasonable
	assert.Greater(t, blockScannerBatchSize, 0, "Batch size constant should be positive")
	t.Logf("Configured batch size limit: %d", blockScannerBatchSize)

	scanner.close()
}
