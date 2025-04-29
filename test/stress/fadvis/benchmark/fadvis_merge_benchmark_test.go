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
	"github.com/apache/skywalking-banyandb/test/stress/fadvis/utils"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

// BenchmarkMergeOperations tests the performance of merge operations with and without utils.
func BenchmarkMergeOperations(b *testing.B) {
	if runtime.GOOS != "linux" {
		b.Skip("utilse is only supported on Linux")
	}

	// Create a temporary directory for the test
	testDir, err := os.MkdirTemp("", "utils_merge_benchmark")
	require.NoError(b, err)
	defer os.RemoveAll(testDir)

	// Prepare test files
	parts := utils.CreateTestParts(b, testDir, 10, utils.LargeFileSize)

	// Run benchmark with utilse disabled
	b.Run("Withoututilse", func(b *testing.B) {
		// Set the utils threshold to a high value to disable it
		utils.SetTestThreshold(utils.Terabyte)
		for i := 0; i < b.N; i++ {
			outputFile := filepath.Join(testDir, fmt.Sprintf("merged_%d", i))
			// Use the simulateMergeOperation from test_helpers.go which uses fs package
			err := utils.SimulateMergeOperation(b, parts, outputFile)
			require.NoError(b, err)
		}
		utils.CapturePageCacheStats(b, "after_merge_utils_disabled")
		utils.CapturePageCacheStatsWithDelay(b, "after_merge_utils_disabled_delay", 3)
	})

	// Run benchmark with utilse enabled
	b.Run("Withutilse", func(b *testing.B) {
		// Set a realistic utils threshold based on system memory
		utils.SetRealisticThreshold()
		for i := 0; i < b.N; i++ {
			outputFile := filepath.Join(testDir, fmt.Sprintf("merged_utils_%d", i))
			// Use the simulateMergeOperation from test_helpers.go which uses fs package
			err := utils.SimulateMergeOperation(b, parts, outputFile)
			require.NoError(b, err)
		}
		utils.CapturePageCacheStats(b, "after_merge_utils_enabled")
		utils.CapturePageCacheStatsWithDelay(b, "after_merge_utils_enabled_delay", 3)
	})
}

// BenchmarkSequentialMergeOperations tests the performance of sequential merge operations.
func BenchmarkSequentialMergeOperations(b *testing.B) {
	if runtime.GOOS != "linux" {
		b.Skip("utilse is only supported on Linux")
	}

	// Create a temporary directory for the test
	testDir, err := os.MkdirTemp("", "utils_sequential_merge_benchmark")
	require.NoError(b, err)
	defer os.RemoveAll(testDir)

	// Prepare test files
	parts := utils.CreateTestParts(b, testDir, 5, utils.LargeFileSize)

	// Run benchmark with utilse disabled
	b.Run("Withoututilse", func(b *testing.B) {
		// Set the utils threshold to a high value to disable it
		utils.SetTestThreshold(utils.Terabyte)
		for i := 0; i < b.N; i++ {
			// Perform multiple sequential merge operations
			for j := 0; j < 3; j++ {
				outputFile := filepath.Join(testDir, fmt.Sprintf("seq_merged_%d_%d", i, j))
				// Use the simulateMergeOperation from test_helpers.go which uses fs package
				err := utils.SimulateMergeOperation(b, parts, outputFile)
				require.NoError(b, err)
			}
		}
		utils.CapturePageCacheStats(b, "after_sequential_merge_utils_disabled")
		utils.CapturePageCacheStatsWithDelay(b, "after_sequential_merge_utils_disabled_delay", 3)
	})

	// Run benchmark with utilse enabled
	b.Run("Withutilse", func(b *testing.B) {
		// Set a realistic utils threshold based on system memory
		utils.SetRealisticThreshold()
		for i := 0; i < b.N; i++ {
			// Perform multiple sequential merge operations
			for j := 0; j < 3; j++ {
				outputFile := filepath.Join(testDir, fmt.Sprintf("seq_merged_utils_%d_%d", i, j))
				// Use the simulateMergeOperation from test_helpers.go which uses fs package
				err := utils.SimulateMergeOperation(b, parts, outputFile)
				require.NoError(b, err)
			}
		}
		utils.CapturePageCacheStats(b, "after_sequential_merge_utils_enabled")
		utils.CapturePageCacheStatsWithDelay(b, "after_sequential_merge_utils_enabled_delay", 3)
	})
}
