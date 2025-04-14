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

package fadvis_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/apache/skywalking-banyandb/banyand/fadvis"
)

// BenchmarkMergeOperations tests the performance of merge operations with and without fadvis.
func BenchmarkMergeOperations(b *testing.B) {
	// Skip on non-Linux platforms since fadvis is a Linux-specific feature
	if runtime.GOOS != "linux" {
		b.Skip("fadvis tests are only relevant on Linux")
	}

	// Test different numbers of parts to merge
	for _, numParts := range []int{3, 5, 10} {
		// Test with fadvis enabled
		b.Run(fmt.Sprintf("Merge_%dParts_FadvisEnabled", numParts), func(b *testing.B) {
			// Setup environment with default threshold
			testDir, err := ioutil.TempDir("", "fadvis-merge-benchmark-")
			if err != nil {
				b.Fatalf("Failed to create test directory: %v", err)
			}
			defer os.RemoveAll(testDir)

			// Set threshold to enable fadvis for large files
			originalThreshold := fadvis.GetThreshold()
			fadvis.SetThreshold(64 * 1024 * 1024) // 64MB
			defer fadvis.SetThreshold(originalThreshold)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Create a unique directory for this iteration
				iterDir := filepath.Join(testDir, fmt.Sprintf("iter_%d", i))
				os.MkdirAll(iterDir, 0755)

				// Prepare test parts
				prepareMergeBenchmark(b, iterDir, numParts)

				// Simulate merge operation
				// This is a simplified version since we can't directly use the internal merge logic
				mergeDir := filepath.Join(iterDir, "merged")
				os.MkdirAll(mergeDir, 0755)

				// Merge operation: read from parts and write to a new part
				simulateMergeOperation(b, iterDir, numParts, mergeDir, true)
			}
		})

		// Test with fadvis disabled
		b.Run(fmt.Sprintf("Merge_%dParts_FadvisDisabled", numParts), func(b *testing.B) {
			// Setup environment with high threshold (effectively disabling fadvis)
			testDir, err := ioutil.TempDir("", "fadvis-merge-benchmark-")
			if err != nil {
				b.Fatalf("Failed to create test directory: %v", err)
			}
			defer os.RemoveAll(testDir)

			// Set threshold very high to effectively disable fadvis
			originalThreshold := fadvis.GetThreshold()
			fadvis.SetThreshold(1024 * 1024 * 1024 * 1024) // 1TB
			defer fadvis.SetThreshold(originalThreshold)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Create a unique directory for this iteration
				iterDir := filepath.Join(testDir, fmt.Sprintf("iter_%d", i))
				os.MkdirAll(iterDir, 0755)

				// Prepare test parts
				prepareMergeBenchmark(b, iterDir, numParts)

				// Simulate merge operation
				mergeDir := filepath.Join(iterDir, "merged")
				os.MkdirAll(mergeDir, 0755)

				// Merge operation without fadvis
				simulateMergeOperation(b, iterDir, numParts, mergeDir, false)
			}
		})
	}
}

// BenchmarkSequentialMergeOperations tests the performance of sequential merge operations.
func BenchmarkSequentialMergeOperations(b *testing.B) {
	// Skip on non-Linux platforms
	if runtime.GOOS != "linux" {
		b.Skip("fadvis tests are only relevant on Linux")
	}

	numParts := 5
	numSequentialMerges := 3

	// Test with fadvis enabled
	b.Run("SequentialMerge_FadvisEnabled", func(b *testing.B) {
		// Setup environment
		testDir, err := ioutil.TempDir("", "fadvis-seq-merge-benchmark-")
		if err != nil {
			b.Fatalf("Failed to create test directory: %v", err)
		}
		defer os.RemoveAll(testDir)

		// Set threshold to enable fadvis
		originalThreshold := fadvis.GetThreshold()
		fadvis.SetThreshold(64 * 1024 * 1024) // 64MB
		defer fadvis.SetThreshold(originalThreshold)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Create a unique directory for this iteration
			iterDir := filepath.Join(testDir, fmt.Sprintf("iter_%d", i))
			os.MkdirAll(iterDir, 0755)

			// Do a series of sequential merges
			sourceDir := iterDir
			for j := 0; j < numSequentialMerges; j++ {
				// Prepare test parts in source dir
				prepareMergeBenchmark(b, sourceDir, numParts)

				// Merge to a new directory
				mergeDir := filepath.Join(iterDir, fmt.Sprintf("merge_%d", j))
				os.MkdirAll(mergeDir, 0755)

				// Perform merge with fadvis
				simulateMergeOperation(b, sourceDir, numParts, mergeDir, true)

				// Next merge uses output of this merge
				sourceDir = mergeDir
			}
		}
	})

	// Test with fadvis disabled
	b.Run("SequentialMerge_FadvisDisabled", func(b *testing.B) {
		// Setup environment
		testDir, err := ioutil.TempDir("", "fadvis-seq-merge-benchmark-")
		if err != nil {
			b.Fatalf("Failed to create test directory: %v", err)
		}
		defer os.RemoveAll(testDir)

		// Set threshold very high
		originalThreshold := fadvis.GetThreshold()
		fadvis.SetThreshold(1024 * 1024 * 1024 * 1024) // 1TB
		defer fadvis.SetThreshold(originalThreshold)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Create a unique directory for this iteration
			iterDir := filepath.Join(testDir, fmt.Sprintf("iter_%d", i))
			os.MkdirAll(iterDir, 0755)

			// Do a series of sequential merges
			sourceDir := iterDir
			for j := 0; j < numSequentialMerges; j++ {
				// Prepare test parts in source dir
				prepareMergeBenchmark(b, sourceDir, numParts)

				// Merge to a new directory
				mergeDir := filepath.Join(iterDir, fmt.Sprintf("merge_%d", j))
				os.MkdirAll(mergeDir, 0755)

				// Perform merge without fadvis
				simulateMergeOperation(b, sourceDir, numParts, mergeDir, false)

				// Next merge uses output of this merge
				sourceDir = mergeDir
			}
		}
	})
}

// prepareMergeBenchmark creates test parts for merge benchmarking.
func prepareMergeBenchmark(b *testing.B, baseDir string, numParts int) {
	// File size large enough to trigger fadvis
	fileSize := int64(100 * 1024 * 1024) // 100MB

	for i := 0; i < numParts; i++ {
		partDir := filepath.Join(baseDir, fmt.Sprintf("part_%d", i))
		err := os.MkdirAll(partDir, 0755)
		if err != nil {
			b.Fatalf("Failed to create part directory: %v", err)
		}

		// Create primary file
		primaryPath := filepath.Join(partDir, "primary")
		createTestFile(b, primaryPath, fileSize)

		// Create timestamps file
		timestampsPath := filepath.Join(partDir, "timestamps")
		createTestFile(b, timestampsPath, fileSize)

		// Create tag family files
		tagFamilyPath := filepath.Join(partDir, "tagfamily_test")
		createTestFile(b, tagFamilyPath, fileSize/2)
	}
}

// simulateMergeOperation simulates the merge operation by reading source parts and writing to a target part.
func simulateMergeOperation(b *testing.B, sourceDir string, numParts int, targetDir string, useFadvis bool) {
	// Output file size (roughly sum of input sizes)
	mergedFileSize := int64(100 * 1024 * 1024 * numParts) // 100MB * numParts

	// Create merged files
	primaryPath := filepath.Join(targetDir, "primary")
	createTestFile(b, primaryPath, mergedFileSize)

	timestampsPath := filepath.Join(targetDir, "timestamps")
	createTestFile(b, timestampsPath, mergedFileSize)

	tagFamilyPath := filepath.Join(targetDir, "tagfamily_test")
	createTestFile(b, tagFamilyPath, mergedFileSize/2)

	// Apply fadvis to merged files if enabled
	if useFadvis {
		// Apply fadvis to all large files in the merged part
		fadvis.ApplyIfLarge(primaryPath)
		fadvis.ApplyIfLarge(timestampsPath)
		fadvis.ApplyIfLarge(tagFamilyPath)
	}
}

// createTestFile creates a test file of the specified size.
func createTestFile(b *testing.B, path string, size int64) {
	file, err := os.Create(path)
	if err != nil {
		b.Fatalf("Failed to create file %s: %v", path, err)
	}
	defer file.Close()

	// Allocate the file to the specified size
	err = file.Truncate(size)
	if err != nil {
		b.Fatalf("Failed to truncate file %s to size %d: %v", path, size, err)
	}

	// Sync the file to ensure it's written to disk
	err = file.Sync()
	if err != nil {
		b.Fatalf("Failed to sync file %s: %v", path, err)
	}
}
