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

package fadvis

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

// BenchmarkMergeOperations tests the performance of merge operations with and without fadvis.
func BenchmarkMergeOperations(b *testing.B) {
	if runtime.GOOS != "linux" {
		b.Skip("fadvise is only supported on Linux")
	}

	// Create a temporary directory for the test
	testDir, err := os.MkdirTemp("", "fadvis_merge_benchmark")
	require.NoError(b, err)
	defer os.RemoveAll(testDir)

	// Prepare test files
	parts := createTestParts(b, testDir, 10, LargeFileSize)

	// Run benchmark with fadvise disabled
	b.Run("WithoutFadvise", func(b *testing.B) {
		// Set the fadvis threshold to a high value to disable it
		setTestThreshold(terabyte)
		for i := 0; i < b.N; i++ {
			outputFile := filepath.Join(testDir, fmt.Sprintf("merged_%d", i))
			// Use the simulateMergeOperation from test_helpers.go which uses fs package
			err := simulateMergeOperation(b, parts, outputFile)
			require.NoError(b, err)
		}
	})

	// Run benchmark with fadvise enabled
	b.Run("WithFadvise", func(b *testing.B) {
		// Set the fadvis threshold to enable it for our test files
		setTestThreshold(DefaultThreshold)
		for i := 0; i < b.N; i++ {
			outputFile := filepath.Join(testDir, fmt.Sprintf("merged_fadvis_%d", i))
			// Use the simulateMergeOperation from test_helpers.go which uses fs package
			err := simulateMergeOperation(b, parts, outputFile)
			require.NoError(b, err)
		}
	})
}

// BenchmarkSequentialMergeOperations tests the performance of sequential merge operations.
func BenchmarkSequentialMergeOperations(b *testing.B) {
	if runtime.GOOS != "linux" {
		b.Skip("fadvise is only supported on Linux")
	}

	// Create a temporary directory for the test
	testDir, err := os.MkdirTemp("", "fadvis_sequential_merge_benchmark")
	require.NoError(b, err)
	defer os.RemoveAll(testDir)

	// Prepare test files
	parts := createTestParts(b, testDir, 5, LargeFileSize)

	// Run benchmark with fadvise disabled
	b.Run("WithoutFadvise", func(b *testing.B) {
		// Set the fadvis threshold to a high value to disable it
		setTestThreshold(terabyte)
		for i := 0; i < b.N; i++ {
			// Perform multiple sequential merge operations
			for j := 0; j < 3; j++ {
				outputFile := filepath.Join(testDir, fmt.Sprintf("seq_merged_%d_%d", i, j))
				// Use the simulateMergeOperation from test_helpers.go which uses fs package
				err := simulateMergeOperation(b, parts, outputFile)
				require.NoError(b, err)
			}
		}
	})

	// Run benchmark with fadvise enabled
	b.Run("WithFadvise", func(b *testing.B) {
		// Set the fadvis threshold to enable it for our test files
		setTestThreshold(DefaultThreshold)
		for i := 0; i < b.N; i++ {
			// Perform multiple sequential merge operations
			for j := 0; j < 3; j++ {
				outputFile := filepath.Join(testDir, fmt.Sprintf("seq_merged_fadvis_%d_%d", i, j))
				// Use the simulateMergeOperation from test_helpers.go which uses fs package
				err := simulateMergeOperation(b, parts, outputFile)
				require.NoError(b, err)
			}
		}
	})
}
