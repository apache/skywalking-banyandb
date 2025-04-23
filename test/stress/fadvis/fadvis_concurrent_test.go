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
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/skywalking-banyandb/banyand/fadvis"
	"github.com/stretchr/testify/require"
)

// BenchmarkConcurrentOperations tests the performance of concurrent file operations
func BenchmarkConcurrentOperations(b *testing.B) {
	testDir, cleanup := setupTestEnvironment(b)
	defer cleanup()

	// Create test files
	numFiles := 10 // Default number of concurrent files
	files := make([]string, numFiles)
	for i := 0; i < numFiles; i++ {
		files[i] = filepath.Join(testDir, fmt.Sprintf("test_file_%d", i))
		err := createTestFile(b, files[i], 200*1024*1024)
		require.NoError(b, err)
	}

	b.Run("ConcurrentReads", func(b *testing.B) {
		benchmarkConcurrentReads(b, files)
	})
}

// BenchmarkConcurrentMerges tests the performance of concurrent merge operations
func BenchmarkConcurrentMerges(b *testing.B) {
	testDir, cleanup := setupTestEnvironment(b)
	defer cleanup()

	// Create test parts for merging
	numParts := 5 // Default number of parts
	parts := createTestParts(b, testDir, numParts, 10*1024*1024)

	b.Run("ConcurrentMerges", func(b *testing.B) {
		benchmarkConcurrentMerges(b, testDir, parts)
	})
}

func benchmarkConcurrentReads(b *testing.B, files []string) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, file := range files {
			data, err := readFileWithFadvise(b, file)
			require.NoError(b, err)
			require.NotEmpty(b, data)
		}
	}
}

func benchmarkConcurrentMerges(b *testing.B, testDir string, parts []string) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		outputFile := filepath.Join(testDir, fmt.Sprintf("merged_%d", i))
		simulateMergeOperation(b, outputFile, parts)
	}
}

// BenchmarkThresholdAdaptation tests how the system adapts to changing memory thresholds
func BenchmarkThresholdAdaptation(b *testing.B) {
	tests := []struct {
		name           string
		fileSize       int64
		threshold      int64
		memoryPressure string
	}{
		{
			name:           "SmallFileHighThreshold",
			fileSize:       10 * 1024 * 1024,
			threshold:      100 * 1024 * 1024,
			memoryPressure: "normal",
		},
		{
			name:           "LargeFileLowThreshold",
			fileSize:       200 * 1024 * 1024,
			threshold:      50 * 1024 * 1024,
			memoryPressure: "high",
		},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			tempDir, cleanup := setupTestEnvironment(b)
			defer cleanup()

			// Set the threshold based on memory pressure
			oldThreshold := fadvis.GetThreshold()
			fadvis.SetThreshold(tt.threshold)
			defer fadvis.SetThreshold(oldThreshold)

			// Create test file
			testFile := filepath.Join(tempDir, "test_file")
			err := createTestFile(b, testFile, tt.fileSize)
			require.NoError(b, err)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if tt.memoryPressure == "high" {
					// Simulate memory pressure by lowering threshold
					fadvis.SetThreshold(40 * 1024 * 1024)
					time.Sleep(100 * time.Millisecond)
					fadvis.SetThreshold(tt.threshold)
				}
				time.Sleep(100 * time.Millisecond)

				// Read the file using our integrated read function
				data, err := readFileWithFadvise(b, testFile)
				require.NoError(b, err)
				require.NotEmpty(b, data)
			}
		})
	}
}
