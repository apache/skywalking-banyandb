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
	"testing"

	"github.com/apache/skywalking-banyandb/banyand/fadvis"
)

const (
	// Large file threshold for fadvis
	defaultThreshold  = 64 * 1024 * 1024          // 64MB
	disabledThreshold = 1024 * 1024 * 1024 * 1024 // 1TB (effectively disabled)
	smallFileSize     = 1 * 1024 * 1024           // 1MB
	largeFileSize     = 100 * 1024 * 1024         // 100MB
	defaultPartCount  = 5
	testDirPrefix     = "fadvis-benchmark-"
)

// setupTestEnvironment prepares a test environment with the specified fadvis threshold.
func setupTestEnvironment(b *testing.B, threshold int64) (string, func()) {
	// Set fadvis threshold
	fadvis.SetThreshold(threshold)

	// Create a temporary directory for test files
	testDir, err := ioutil.TempDir("", testDirPrefix)
	if err != nil {
		b.Fatalf("Failed to create test directory: %v", err)
	}

	// Return cleanup function
	cleanup := func() {
		os.RemoveAll(testDir)
	}

	return testDir, cleanup
}

// createLargeFile creates a file of the specified size.
func createLargeFile(path string, size int64) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	return file.Truncate(size)
}

// createTestParts creates test part files for merge testing.
func createTestParts(baseDir string, count int, fileSize int64) []string {
	paths := make([]string, count)
	for i := 0; i < count; i++ {
		partDir := filepath.Join(baseDir, fmt.Sprintf("part_%d", i))
		os.MkdirAll(partDir, 0755)

		// Create primary file
		primaryPath := filepath.Join(partDir, "primary")
		createLargeFile(primaryPath, fileSize)

		// Create timestamps file
		timestampsPath := filepath.Join(partDir, "timestamps")
		createLargeFile(timestampsPath, fileSize)

		paths[i] = partDir
	}
	return paths
}

// BenchmarkWritePerformance tests write performance with and without fadvis.
func BenchmarkWritePerformance(b *testing.B) {
	// Test cases with different file sizes
	for _, fileSize := range []int64{smallFileSize, largeFileSize} {
		// Test with fadvis enabled
		b.Run(fmt.Sprintf("Size_%dMB_FadvisEnabled", fileSize/(1024*1024)), func(b *testing.B) {
			testDir, cleanup := setupTestEnvironment(b, defaultThreshold)
			defer cleanup()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				filePath := filepath.Join(testDir, fmt.Sprintf("write_test_%d.dat", i))
				if err := createLargeFile(filePath, fileSize); err != nil {
					b.Fatalf("Failed to create test file: %v", err)
				}

				// Apply fadvis if file is large
				if fileSize > defaultThreshold {
					fadvis.ApplyIfLarge(filePath)
				}
			}
		})

		// Test with fadvis disabled
		b.Run(fmt.Sprintf("Size_%dMB_FadvisDisabled", fileSize/(1024*1024)), func(b *testing.B) {
			testDir, cleanup := setupTestEnvironment(b, disabledThreshold)
			defer cleanup()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				filePath := filepath.Join(testDir, fmt.Sprintf("write_test_%d.dat", i))
				if err := createLargeFile(filePath, fileSize); err != nil {
					b.Fatalf("Failed to create test file: %v", err)
				}

				// This won't actually apply fadvis due to high threshold
				fadvis.ApplyIfLarge(filePath)
			}
		})
	}
}

// BenchmarkReadPerformance tests read performance with and without fadvis.
func BenchmarkReadPerformance(b *testing.B) {
	// Test cases with different file sizes
	for _, fileSize := range []int64{smallFileSize, largeFileSize} {
		// Test with fadvis enabled
		b.Run(fmt.Sprintf("Size_%dMB_FadvisEnabled", fileSize/(1024*1024)), func(b *testing.B) {
			testDir, cleanup := setupTestEnvironment(b, defaultThreshold)
			defer cleanup()

			// Create a test file
			filePath := filepath.Join(testDir, "read_test.dat")
			if err := createLargeFile(filePath, fileSize); err != nil {
				b.Fatalf("Failed to create test file: %v", err)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Open and read the file
				data, err := os.ReadFile(filePath)
				if err != nil {
					b.Fatalf("Failed to read file: %v", err)
				}

				// Ensure data is used to prevent compiler optimization
				_ = len(data)

				// Apply fadvis if file is large
				if fileSize > defaultThreshold {
					fadvis.ApplyIfLarge(filePath)
				}
			}
		})

		// Test with fadvis disabled
		b.Run(fmt.Sprintf("Size_%dMB_FadvisDisabled", fileSize/(1024*1024)), func(b *testing.B) {
			testDir, cleanup := setupTestEnvironment(b, disabledThreshold)
			defer cleanup()

			// Create a test file
			filePath := filepath.Join(testDir, "read_test.dat")
			if err := createLargeFile(filePath, fileSize); err != nil {
				b.Fatalf("Failed to create test file: %v", err)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Open and read the file
				data, err := os.ReadFile(filePath)
				if err != nil {
					b.Fatalf("Failed to read file: %v", err)
				}

				// Ensure data is used to prevent compiler optimization
				_ = len(data)

				// This won't actually apply fadvis due to high threshold
				fadvis.ApplyIfLarge(filePath)
			}
		})
	}
}

// BenchmarkMultipleReads tests the performance impact of multiple reads on the same file.
func BenchmarkMultipleReads(b *testing.B) {
	fileSize := int64(largeFileSize)
	readCount := 5

	// Test with fadvis enabled
	b.Run(fmt.Sprintf("MultipleReads_FadvisEnabled"), func(b *testing.B) {
		testDir, cleanup := setupTestEnvironment(b, defaultThreshold)
		defer cleanup()

		// Create a test file
		filePath := filepath.Join(testDir, "multiple_read_test.dat")
		if err := createLargeFile(filePath, fileSize); err != nil {
			b.Fatalf("Failed to create test file: %v", err)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for j := 0; j < readCount; j++ {
				// Open and read the file
				data, err := os.ReadFile(filePath)
				if err != nil {
					b.Fatalf("Failed to read file: %v", err)
				}

				// Ensure data is used to prevent compiler optimization
				_ = len(data)

				// Apply fadvis after each read
				fadvis.ApplyIfLarge(filePath)
			}
		}
	})

	// Test with fadvis disabled
	b.Run(fmt.Sprintf("MultipleReads_FadvisDisabled"), func(b *testing.B) {
		testDir, cleanup := setupTestEnvironment(b, disabledThreshold)
		defer cleanup()

		// Create a test file
		filePath := filepath.Join(testDir, "multiple_read_test.dat")
		if err := createLargeFile(filePath, fileSize); err != nil {
			b.Fatalf("Failed to create test file: %v", err)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for j := 0; j < readCount; j++ {
				// Open and read the file
				data, err := os.ReadFile(filePath)
				if err != nil {
					b.Fatalf("Failed to read file: %v", err)
				}

				// Ensure data is used to prevent compiler optimization
				_ = len(data)

				// This won't actually apply fadvis due to high threshold
				fadvis.ApplyIfLarge(filePath)
			}
		}
	})
}

// BenchmarkMixedWorkload tests the performance of a mixed workload of reads and writes.
func BenchmarkMixedWorkload(b *testing.B) {
	fileSize := int64(largeFileSize)

	// Test with fadvis enabled
	b.Run("MixedWorkload_FadvisEnabled", func(b *testing.B) {
		testDir, cleanup := setupTestEnvironment(b, defaultThreshold)
		defer cleanup()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Create a new file
			writeFilePath := filepath.Join(testDir, fmt.Sprintf("mixed_write_%d.dat", i))
			if err := createLargeFile(writeFilePath, fileSize); err != nil {
				b.Fatalf("Failed to create write file: %v", err)
			}
			fadvis.ApplyIfLarge(writeFilePath)

			// Read the file we just created
			data, err := os.ReadFile(writeFilePath)
			if err != nil {
				b.Fatalf("Failed to read file: %v", err)
			}
			_ = len(data)
			fadvis.ApplyIfLarge(writeFilePath)

			// Create another file
			writeFilePath2 := filepath.Join(testDir, fmt.Sprintf("mixed_write2_%d.dat", i))
			if err := createLargeFile(writeFilePath2, fileSize); err != nil {
				b.Fatalf("Failed to create second write file: %v", err)
			}
			fadvis.ApplyIfLarge(writeFilePath2)
		}
	})

	// Test with fadvis disabled
	b.Run("MixedWorkload_FadvisDisabled", func(b *testing.B) {
		testDir, cleanup := setupTestEnvironment(b, disabledThreshold)
		defer cleanup()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Create a new file
			writeFilePath := filepath.Join(testDir, fmt.Sprintf("mixed_write_%d.dat", i))
			if err := createLargeFile(writeFilePath, fileSize); err != nil {
				b.Fatalf("Failed to create write file: %v", err)
			}
			fadvis.ApplyIfLarge(writeFilePath)

			// Read the file we just created
			data, err := os.ReadFile(writeFilePath)
			if err != nil {
				b.Fatalf("Failed to read file: %v", err)
			}
			_ = len(data)
			fadvis.ApplyIfLarge(writeFilePath)

			// Create another file
			writeFilePath2 := filepath.Join(testDir, fmt.Sprintf("mixed_write2_%d.dat", i))
			if err := createLargeFile(writeFilePath2, fileSize); err != nil {
				b.Fatalf("Failed to create second write file: %v", err)
			}
			fadvis.ApplyIfLarge(writeFilePath2)
		}
	})
}
