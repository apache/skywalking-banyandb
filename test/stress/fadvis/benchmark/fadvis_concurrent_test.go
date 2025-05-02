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

package benchmark

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/skywalking-banyandb/test/stress/fadvis/utils"

	"github.com/stretchr/testify/require"
)

// BenchmarkConcurrentOperations tests the performance of concurrent file operations
func BenchmarkConcurrentOperations(b *testing.B) {
	testDir, cleanup := utils.SetupTestEnvironment(b)
	defer cleanup()

	// Create test files
	numFiles := 10 // Default number of concurrent files
	files := make([]string, numFiles)
	for i := 0; i < numFiles; i++ {
		files[i] = filepath.Join(testDir, fmt.Sprintf("test_file_%d", i))
		err := utils.CreateTestFile(b, files[i], 200*1024*1024)
		require.NoError(b, err)
	}

	b.Run("ConcurrentReads", func(b *testing.B) {
		utils.WithMonitoringLegacy(b, func(b *testing.B) {
			benchmarkConcurrentReads(b, files)
		})
	})
}

// BenchmarkConcurrentMerges tests the performance of concurrent merge operations
func BenchmarkConcurrentMerges(b *testing.B) {
	testDir, cleanup := utils.SetupTestEnvironment(b)
	defer cleanup()

	// Create test parts for merging
	numParts := 5 // Default number of parts
	parts := utils.CreateTestParts(b, testDir, numParts, 10*1024*1024)

	b.Run("ConcurrentMerges", func(b *testing.B) {
		utils.WithMonitoringLegacy(b, func(b *testing.B) {
			benchmarkConcurrentMerges(b, testDir, parts)
		})
	})
}

func benchmarkConcurrentReads(b *testing.B, files []string) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, file := range files {
			data, err := utils.ReadFileWithFadvise(b, file)
			require.NoError(b, err)
			require.NotEmpty(b, data)
		}
	}
	utils.CapturePageCacheStats(b, "after_concurrent_read")
	utils.CapturePageCacheStatsWithDelay(b, "after_concurrent_read", 3)
}

func benchmarkConcurrentMerges(b *testing.B, testDir string, parts []string) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		outputFile := filepath.Join(testDir, fmt.Sprintf("merged_%d", i))
		err := utils.SimulateMergeOperation(b, parts, outputFile)
		require.NoError(b, err)
	}
	utils.CapturePageCacheStats(b, "after_concurrent_merge")
	utils.CapturePageCacheStatsWithDelay(b, "after_concurrent_merge", 3)
}

// BenchmarkThresholdAdaptation tests how the system adapts to changing memory thresholds
func BenchmarkThresholdAdaptation(b *testing.B) {
	tests := []struct {
		name           string
		fileSize       int64
		memoryPressure string
	}{
		{
			name:           "SmallFileNormalMemory",
			fileSize:       10 * 1024 * 1024,
			memoryPressure: "normal",
		},
		{
			name:           "LargeFileHighMemoryPressure",
			fileSize:       200 * 1024 * 1024,
			memoryPressure: "high",
		},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			tempDir, cleanup := utils.SetupTestEnvironment(b)
			defer cleanup()

			// Set a realistic utils threshold based on system memory
			utils.SetRealisticThreshold()

			// Create test file
			testFile := filepath.Join(tempDir, "test_file")
			err := utils.CreateTestFile(b, testFile, tt.fileSize)
			require.NoError(b, err)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if tt.memoryPressure == "high" {
					// Simulate high memory pressure by setting a lower threshold
					// In real system, this would be automatically adjusted based on memory pressure
					// Here we simulate by manually setting a lower threshold (25% of normal)
					normalThreshold := utils.CalculateRealisticThreshold()
					lowThreshold := normalThreshold / 4 // 25% of normal threshold
					utils.SetTestThreshold(lowThreshold)
					time.Sleep(100 * time.Millisecond)
					// Reset to normal threshold
					utils.SetRealisticThreshold()
				}
				time.Sleep(100 * time.Millisecond)

				// Read the file using our integrated read function
				data, err := utils.ReadFileStreamingWithFadvise(b, testFile, false)
				require.NoError(b, err)
				require.NotEmpty(b, data)
			}
		})
	}
}
