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
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/apache/skywalking-banyandb/banyand/fadvis"
)

// BenchmarkConcurrentOperations tests performance of concurrent operations with and without fadvis.
func BenchmarkConcurrentOperations(b *testing.B) {
	// Skip on non-Linux platforms
	if runtime.GOOS != "linux" {
		b.Skip("fadvis tests are only relevant on Linux")
	}

	fileSize := int64(largeFileSize)
	concurrency := runtime.NumCPU() // Use number of CPUs as concurrency level

	// Test with fadvis enabled
	b.Run("Concurrent_FadvisEnabled", func(b *testing.B) {
		testDir, err := ioutil.TempDir("", "fadvis-concurrent-benchmark-")
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
			var wg sync.WaitGroup
			wg.Add(concurrency)

			for j := 0; j < concurrency; j++ {
				go func(id int) {
					defer wg.Done()

					// Each goroutine performs a mix of operations
					for k := 0; k < 3; k++ { // Do 3 operations per goroutine
						// Choose operation randomly: 0=write, 1=read
						op := rand.Intn(2)

						filePath := filepath.Join(testDir, fmt.Sprintf("concurrent_%d_%d_%d.dat", i, id, k))

						if op == 0 || k == 0 { // Always write first to ensure file exists
							// Write operation
							if err := createLargeFile(filePath, fileSize); err != nil {
								b.Errorf("Failed to create file: %v", err)
								return
							}
							fadvis.ApplyIfLarge(filePath)
						} else {
							// Read operation - use a file created earlier
							existingFile := filepath.Join(testDir, fmt.Sprintf("concurrent_%d_%d_0.dat", i, id))
							data, err := os.ReadFile(existingFile)
							if err != nil {
								b.Errorf("Failed to read file: %v", err)
								return
							}
							_ = len(data)
							fadvis.ApplyIfLarge(existingFile)
						}
					}
				}(j)
			}

			wg.Wait()
		}
	})

	// Test with fadvis disabled
	b.Run("Concurrent_FadvisDisabled", func(b *testing.B) {
		testDir, err := ioutil.TempDir("", "fadvis-concurrent-benchmark-")
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
			var wg sync.WaitGroup
			wg.Add(concurrency)

			for j := 0; j < concurrency; j++ {
				go func(id int) {
					defer wg.Done()

					// Each goroutine performs a mix of operations
					for k := 0; k < 3; k++ { // Do 3 operations per goroutine
						// Choose operation randomly: 0=write, 1=read
						op := rand.Intn(2)

						filePath := filepath.Join(testDir, fmt.Sprintf("concurrent_%d_%d_%d.dat", i, id, k))

						if op == 0 || k == 0 { // Always write first to ensure file exists
							// Write operation
							if err := createLargeFile(filePath, fileSize); err != nil {
								b.Errorf("Failed to create file: %v", err)
								return
							}
							fadvis.ApplyIfLarge(filePath)
						} else {
							// Read operation - use a file created earlier
							existingFile := filepath.Join(testDir, fmt.Sprintf("concurrent_%d_%d_0.dat", i, id))
							data, err := os.ReadFile(existingFile)
							if err != nil {
								b.Errorf("Failed to read file: %v", err)
								return
							}
							_ = len(data)
							fadvis.ApplyIfLarge(existingFile)
						}
					}
				}(j)
			}

			wg.Wait()
		}
	})
}

// BenchmarkConcurrentMerges tests performance of concurrent merge operations.
func BenchmarkConcurrentMerges(b *testing.B) {
	// Skip on non-Linux platforms
	if runtime.GOOS != "linux" {
		b.Skip("fadvis tests are only relevant on Linux")
	}

	concurrency := runtime.NumCPU() / 2 // Use half the CPUs to avoid overwhelming the system
	if concurrency < 1 {
		concurrency = 1
	}
	numParts := 3 // Use a small number of parts to keep test duration reasonable

	// Test with fadvis enabled
	b.Run("ConcurrentMerge_FadvisEnabled", func(b *testing.B) {
		testDir, err := ioutil.TempDir("", "fadvis-concurrent-merge-")
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
			var wg sync.WaitGroup
			wg.Add(concurrency)

			for j := 0; j < concurrency; j++ {
				go func(id int) {
					defer wg.Done()

					// Create directory for this goroutine
					iterDir := filepath.Join(testDir, fmt.Sprintf("iter_%d_%d", i, id))
					os.MkdirAll(iterDir, 0755)

					// Prepare test parts
					prepareMergeBenchmark(b, iterDir, numParts)

					// Simulate merge operation
					mergeDir := filepath.Join(iterDir, "merged")
					os.MkdirAll(mergeDir, 0755)

					// Merge operation with fadvis
					simulateMergeOperation(b, iterDir, numParts, mergeDir, true)
				}(j)
			}

			wg.Wait()
		}
	})

	// Test with fadvis disabled
	b.Run("ConcurrentMerge_FadvisDisabled", func(b *testing.B) {
		testDir, err := ioutil.TempDir("", "fadvis-concurrent-merge-")
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
			var wg sync.WaitGroup
			wg.Add(concurrency)

			for j := 0; j < concurrency; j++ {
				go func(id int) {
					defer wg.Done()

					// Create directory for this goroutine
					iterDir := filepath.Join(testDir, fmt.Sprintf("iter_%d_%d", i, id))
					os.MkdirAll(iterDir, 0755)

					// Prepare test parts
					prepareMergeBenchmark(b, iterDir, numParts)

					// Simulate merge operation
					mergeDir := filepath.Join(iterDir, "merged")
					os.MkdirAll(mergeDir, 0755)

					// Merge operation without fadvis
					simulateMergeOperation(b, iterDir, numParts, mergeDir, false)
				}(j)
			}

			wg.Wait()
		}
	})
}

// BenchmarkThresholdAdaptation tests performance with changing memory conditions.
func BenchmarkThresholdAdaptation(b *testing.B) {
	// Skip on non-Linux platforms
	if runtime.GOOS != "linux" {
		b.Skip("fadvis tests are only relevant on Linux")
	}

	fileSize := int64(largeFileSize)

	// Set threshold to very low value to simulate low memory condition
	b.Run("LowMemoryThreshold", func(b *testing.B) {
		testDir, err := ioutil.TempDir("", "fadvis-threshold-benchmark-")
		if err != nil {
			b.Fatalf("Failed to create test directory: %v", err)
		}
		defer os.RemoveAll(testDir)

		// Set threshold to a very low value (10MB) to simulate low memory
		originalThreshold := fadvis.GetThreshold()
		fadvis.SetThreshold(10 * 1024 * 1024) // 10MB
		defer fadvis.SetThreshold(originalThreshold)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Create a working directory for this iteration
			workDir := filepath.Join(testDir, fmt.Sprintf("iter_%d", i))
			os.MkdirAll(workDir, 0755)

			// Create several files to simulate a workload
			for j := 0; j < 5; j++ {
				filePath := filepath.Join(workDir, fmt.Sprintf("file_%d.dat", j))
				if err := createLargeFile(filePath, fileSize); err != nil {
					b.Fatalf("Failed to create file: %v", err)
				}

				// This should apply fadvis to all files since they're above threshold
				fadvis.ApplyIfLarge(filePath)

				// Read the file back
				data, err := os.ReadFile(filePath)
				if err != nil {
					b.Fatalf("Failed to read file: %v", err)
				}
				_ = len(data)
			}
		}
	})

	// Set threshold to normal value
	b.Run("NormalMemoryThreshold", func(b *testing.B) {
		testDir, err := ioutil.TempDir("", "fadvis-threshold-benchmark-")
		if err != nil {
			b.Fatalf("Failed to create test directory: %v", err)
		}
		defer os.RemoveAll(testDir)

		// Use default threshold
		originalThreshold := fadvis.GetThreshold()
		fadvis.SetThreshold(64 * 1024 * 1024) // 64MB
		defer fadvis.SetThreshold(originalThreshold)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Create a working directory for this iteration
			workDir := filepath.Join(testDir, fmt.Sprintf("iter_%d", i))
			os.MkdirAll(workDir, 0755)

			// Create several files to simulate a workload
			for j := 0; j < 5; j++ {
				filePath := filepath.Join(workDir, fmt.Sprintf("file_%d.dat", j))
				if err := createLargeFile(filePath, fileSize); err != nil {
					b.Fatalf("Failed to create file: %v", err)
				}

				// This should apply fadvis only to large files
				fadvis.ApplyIfLarge(filePath)

				// Read the file back
				data, err := os.ReadFile(filePath)
				if err != nil {
					b.Fatalf("Failed to read file: %v", err)
				}
				_ = len(data)
			}
		}
	})
}

func init() {
	// Seed the random number generator
	rand.Seed(time.Now().UnixNano())
}

// Constants for file sizes
const (
	largeFileSize = 256 * 1024 * 1024 // 256MB
)

// Helper functions for creating test files and simulating operations

// createLargeFile creates a file of the specified size with random data
func createLargeFile(filePath string, size int64) error {
	f, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer f.Close()

	// Use 1MB buffer for efficiency
	buffer := make([]byte, 1024*1024)
	remaining := size

	for remaining > 0 {
		// Fill buffer with random data
		rand.Read(buffer)

		// Write buffer or remaining bytes
		writeSize := int64(len(buffer))
		if remaining < writeSize {
			writeSize = remaining
		}

		_, err := f.Write(buffer[:writeSize])
		if err != nil {
			return fmt.Errorf("failed to write to file: %w", err)
		}

		remaining -= writeSize
	}

	return f.Sync()
}

// prepareMergeBenchmark creates test parts for merge benchmark
func prepareMergeBenchmark(b *testing.B, testDir string, numParts int) {
	// Create test parts
	for i := 0; i < numParts; i++ {
		partDir := filepath.Join(testDir, fmt.Sprintf("part_%d", i))
		os.MkdirAll(partDir, 0755)

		// Create primary data file
		primaryFile := filepath.Join(partDir, "primary.data")
		if err := createLargeFile(primaryFile, largeFileSize/4); err != nil {
			b.Fatalf("Failed to create primary file: %v", err)
		}

		// Create timestamps file
		timestampsFile := filepath.Join(partDir, "timestamps.data")
		if err := createLargeFile(timestampsFile, largeFileSize/4); err != nil {
			b.Fatalf("Failed to create timestamps file: %v", err)
		}

		// Create tag families file
		tagFamiliesFile := filepath.Join(partDir, "tag_families.data")
		if err := createLargeFile(tagFamiliesFile, largeFileSize/4); err != nil {
			b.Fatalf("Failed to create tag families file: %v", err)
		}

		// Create metadata file
		metaFile := filepath.Join(partDir, "meta.json")
		if err := ioutil.WriteFile(metaFile, []byte(`{"part": "test"}`), 0644); err != nil {
			b.Fatalf("Failed to create meta file: %v", err)
		}
	}
}

// simulateMergeOperation simulates the merge operation with or without fadvis
func simulateMergeOperation(b *testing.B, sourceDir string, numParts int, destDir string, useFadvis bool) {
	// Create destination files
	primaryDest := filepath.Join(destDir, "primary.data")
	timestampsDest := filepath.Join(destDir, "timestamps.data")
	tagFamiliesDest := filepath.Join(destDir, "tag_families.data")
	metaDest := filepath.Join(destDir, "meta.json")

	// Open destination files
	primaryFile, err := os.Create(primaryDest)
	if err != nil {
		b.Fatalf("Failed to create destination primary file: %v", err)
	}
	defer primaryFile.Close()

	timestampsFile, err := os.Create(timestampsDest)
	if err != nil {
		b.Fatalf("Failed to create destination timestamps file: %v", err)
	}
	defer timestampsFile.Close()

	tagFamiliesFile, err := os.Create(tagFamiliesDest)
	if err != nil {
		b.Fatalf("Failed to create destination tag families file: %v", err)
	}
	defer tagFamiliesFile.Close()

	// Simulate merging parts
	for i := 0; i < numParts; i++ {
		partDir := filepath.Join(sourceDir, fmt.Sprintf("part_%d", i))

		// Read primary data
		primaryData, err := os.ReadFile(filepath.Join(partDir, "primary.data"))
		if err != nil {
			b.Fatalf("Failed to read primary data: %v", err)
		}
		_, err = primaryFile.Write(primaryData)
		if err != nil {
			b.Fatalf("Failed to write primary data: %v", err)
		}

		// Read timestamps data
		timestampsData, err := os.ReadFile(filepath.Join(partDir, "timestamps.data"))
		if err != nil {
			b.Fatalf("Failed to read timestamps data: %v", err)
		}
		_, err = timestampsFile.Write(timestampsData)
		if err != nil {
			b.Fatalf("Failed to write timestamps data: %v", err)
		}

		// Read tag families data
		tagFamiliesData, err := os.ReadFile(filepath.Join(partDir, "tag_families.data"))
		if err != nil {
			b.Fatalf("Failed to read tag families data: %v", err)
		}
		_, err = tagFamiliesFile.Write(tagFamiliesData)
		if err != nil {
			b.Fatalf("Failed to write tag families data: %v", err)
		}
	}

	// Sync all files
	primaryFile.Sync()
	timestampsFile.Sync()
	tagFamiliesFile.Sync()

	// Create metadata file
	err = ioutil.WriteFile(metaDest, []byte(`{"part": "merged"}`), 0644)
	if err != nil {
		b.Fatalf("Failed to create metadata file: %v", err)
	}

	// Apply fadvis if enabled
	if useFadvis {
		fadvis.ApplyIfLarge(primaryDest)
		fadvis.ApplyIfLarge(timestampsDest)
		fadvis.ApplyIfLarge(tagFamiliesDest)
	}
}
