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
	"io"
	"path/filepath"
	"testing"

	"github.com/apache/skywalking-banyandb/pkg/fs"
)

// ensureImportsAreUsed is a helper function to ensure imports are used.
// This prevents the compiler from removing imports that are needed by other files.
func ensureImportsAreUsed() {
	// This function is never called, it just ensures imports are kept
	// when the file is compiled separately
	var (
		_ io.Reader
		_ fs.File
	)
}

// BenchmarkWritePerformance tests write performance with and without fadvis.
func BenchmarkWritePerformance(b *testing.B) {
	// Test cases with different file sizes - using larger files to better show fadvis effects
	for _, fileSize := range []int64{256 * 1024 * 1024, 1024 * 1024 * 1024} { // 256MB and 1GB
		// Test with fadvis enabled
		b.Run(fmt.Sprintf("Size_%dMB_FadvisEnabled", fileSize/(1024*1024)), func(b *testing.B) {
			testDir, cleanup := setupTestEnvironment(b)
			defer cleanup()

			// Set a realistic fadvis threshold based on system memory
			// This mimics the actual production logic in fadvis.Manager
			setRealisticThreshold()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				filePath := filepath.Join(testDir, fmt.Sprintf("write_test_%d.dat", i))
				err := createTestFile(b, filePath, fileSize)
				if err != nil {
					b.Fatalf("Failed to create test file: %v", err)
				}
				capturePageCacheStats(b, "after_write_fadvis_enabled")
			}

			capturePageCacheStatsWithDelay(b, "after_write_fadvis_enabled_delay", 3)
		})

		// Test with fadvis disabled
		b.Run(fmt.Sprintf("Size_%dMB_FadvisDisabled", fileSize/(1024*1024)), func(b *testing.B) {
			testDir, cleanup := setupTestEnvironment(b)
			defer cleanup()

			// Set the fadvis threshold to a high value to disable it
			setTestThreshold(1 * 1024 * 1024 * 1024 * 1024) // 1TB threshold effectively disables fadvis

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				filePath := filepath.Join(testDir, fmt.Sprintf("write_test_%d.dat", i))
				// 写入文件但不应用 fadvis
				err := createTestFile(b, filePath, fileSize)
				if err != nil {
					b.Fatalf("Failed to create test file: %v", err)
				}
				capturePageCacheStats(b, "after_write_fadvis_disabled")
			}

			capturePageCacheStatsWithDelay(b, "after_write_fadvis_disabled_delay", 3)
		})
	}
}

// BenchmarkReadPerformance tests read performance with and without fadvis.
func BenchmarkReadPerformance(b *testing.B) {
	// Test cases with different file sizes
	for _, fileSize := range []int64{1 * 1024 * 1024, 256 * 1024 * 1024} {
		// Test with fadvis enabled
		b.Run(fmt.Sprintf("Size_%dMB_FadvisEnabled", fileSize/(1024*1024)), func(b *testing.B) {
			testDir, cleanup := setupTestEnvironment(b)
			defer cleanup()

			// Set a realistic fadvis threshold based on system memory
			setRealisticThreshold()

			// Create a test file
			filePath := filepath.Join(testDir, "read_test.dat")
			if err := createTestFile(b, filePath, fileSize); err != nil {
				b.Fatalf("Failed to create test file: %v", err)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				data, err := readFileWithFadvise(b, filePath)
				if err != nil {
					b.Fatalf("Failed to read file: %v", err)
				}
				b.SetBytes(int64(len(data)))
				capturePageCacheStats(b, "after_read_fadvis_enabled")
			}

			capturePageCacheStatsWithDelay(b, "after_read_fadvis_enabled_delay", 3)
		})

		// Test with fadvis disabled
		b.Run(fmt.Sprintf("Size_%dMB_FadvisDisabled", fileSize/(1024*1024)), func(b *testing.B) {
			testDir, cleanup := setupTestEnvironment(b)
			defer cleanup()

			// Set the fadvis threshold to a high value to disable it
			setTestThreshold(1 * 1024 * 1024 * 1024 * 1024)

			// Create a test file
			filePath := filepath.Join(testDir, "read_test.dat")
			if err := createTestFile(b, filePath, fileSize); err != nil {
				b.Fatalf("Failed to create test file: %v", err)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Read file without applying fadvis
				totalBytes, err := readFileStreamingWithFadvise(b, filePath, true) // skipFadvise=true
				if err != nil {
					b.Fatalf("Failed to read file: %v", err)
				}
				b.SetBytes(totalBytes)
				// Capture page cache statistics
				capturePageCacheStats(b, "after_read_fadvis_disabled")
			}

			capturePageCacheStatsWithDelay(b, "after_read_fadvis_disabled_delay", 3)
		})
	}
}

// BenchmarkMultipleReads tests the performance impact of multiple reads on the same file.
func BenchmarkMultipleReads(b *testing.B) {
	fileSize := int64(256 * 1024 * 1024)
	readCount := 5

	// Test with fadvis enabled
	b.Run("MultipleReads_FadvisEnabled", func(b *testing.B) {
		testDir, cleanup := setupTestEnvironment(b)
		defer cleanup()

		// Set a realistic fadvis threshold based on system memory
		setRealisticThreshold()

		// Create a test file
		filePath := filepath.Join(testDir, "multiple_read_test.dat")
		if err := createTestFile(b, filePath, fileSize); err != nil {
			b.Fatalf("Failed to create test file: %v", err)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for j := 0; j < readCount; j++ {
				// Read the file using our helper function which uses fs package
				data, err := readFileWithFadvise(b, filePath)
				if err != nil {
					b.Fatalf("Failed to read file: %v", err)
				}

				// Ensure data is used to prevent compiler optimization
				_ = len(data)
			}
		}
		capturePageCacheStats(b, "after_multiple_reads_fadvis_enabled")

		capturePageCacheStatsWithDelay(b, "after_multiple_reads_fadvis_enabled_delay", 3)
	})

	// Test with fadvis disabled
	b.Run("MultipleReads_FadvisDisabled", func(b *testing.B) {
		testDir, cleanup := setupTestEnvironment(b)
		defer cleanup()

		// Set the fadvis threshold to a high value to disable it
		setTestThreshold(1 * 1024 * 1024 * 1024 * 1024)

		// Create a test file
		filePath := filepath.Join(testDir, "multiple_read_test.dat")
		if err := createTestFile(b, filePath, fileSize); err != nil {
			b.Fatalf("Failed to create test file: %v", err)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for j := 0; j < readCount; j++ {
				// Read the file using streaming to minimize heap allocations
				totalBytes, err := readFileStreamingWithFadvise(b, filePath, true) // skipFadvise=true
				if err != nil {
					b.Fatalf("Failed to read file: %v", err)
				}

				// Ensure data is used to prevent compiler optimization
				_ = totalBytes
			}
		}

		capturePageCacheStats(b, "after_multiple_reads_fadvis_disabled")

		capturePageCacheStatsWithDelay(b, "after_multiple_reads_fadvis_disabled_delay", 3)
	})
}

// BenchmarkMixedWorkload tests the performance of a mixed workload of reads and writes.
func BenchmarkMixedWorkload(b *testing.B) {
	fileSize := int64(256 * 1024 * 1024)

	// Test with fadvis enabled
	b.Run("MixedWorkload_FadvisEnabled", func(b *testing.B) {
		testDir, cleanup := setupTestEnvironment(b)
		defer cleanup()

		// Set a realistic fadvis threshold based on system memory
		setRealisticThreshold()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Create a new file
			writeFilePath := filepath.Join(testDir, fmt.Sprintf("mixed_write_%d.dat", i))
			if err := createTestFile(b, writeFilePath, fileSize); err != nil {
				b.Fatalf("Failed to create write file: %v", err)
			}

			// Read the file we just created using streaming to minimize heap allocations
			totalBytes, err := readFileStreamingWithFadvise(b, writeFilePath, true) // skipFadvise=true
			if err != nil {
				b.Fatalf("Failed to read file: %v", err)
			}
			_ = totalBytes

			// Create another file
			writeFilePath2 := filepath.Join(testDir, fmt.Sprintf("mixed_write2_%d.dat", i))
			if err := createTestFile(b, writeFilePath2, fileSize); err != nil {
				b.Fatalf("Failed to create second write file: %v", err)
			}
		}
		// 捕获页面缓存统计信息
		capturePageCacheStats(b, "after_mixed_workload_fadvis_enabled")

		capturePageCacheStatsWithDelay(b, "after_mixed_workload_fadvis_enabled_delay", 3)
	})

	// Test with fadvis disabled
	b.Run("MixedWorkload_FadvisDisabled", func(b *testing.B) {
		testDir, cleanup := setupTestEnvironment(b)
		defer cleanup()

		// Set the fadvis threshold to a high value to disable it
		setTestThreshold(1 * 1024 * 1024 * 1024 * 1024)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Create a new file
			writeFilePath := filepath.Join(testDir, fmt.Sprintf("mixed_write_%d.dat", i))
			if err := createTestFile(b, writeFilePath, fileSize); err != nil {
				b.Fatalf("Failed to create write file: %v", err)
			}

			// Read the file we just created using streaming to minimize heap allocations
			totalBytes, err := readFileStreamingWithFadvise(b, writeFilePath, true) // skipFadvise=true
			if err != nil {
				b.Fatalf("Failed to read file: %v", err)
			}
			_ = totalBytes

			// Create another file
			writeFilePath2 := filepath.Join(testDir, fmt.Sprintf("mixed_write2_%d.dat", i))
			if err := createTestFile(b, writeFilePath2, fileSize); err != nil {
				b.Fatalf("Failed to create second write file: %v", err)
			}
		}
		// 捕获页面缓存统计信息
		capturePageCacheStats(b, "after_mixed_workload_fadvis_disabled")

		capturePageCacheStatsWithDelay(b, "after_mixed_workload_fadvis_disabled_delay", 3)
	})
}

// BenchmarkSequentialRead tests sequential read performance using streaming to minimize heap allocations
func BenchmarkSequentialRead(b *testing.B) {
	// Test different file sizes
	fileSizes := []struct {
		name string
		size int64
	}{
		{"256mb", 256 * 1024 * 1024},
		{"1gb", 1024 * 1024 * 1024},
	}

	for _, fs := range fileSizes {
		b.Run(fmt.Sprintf("WithFadvise_%s", fs.name), func(b *testing.B) {
			// Create test directory
			testDir, cleanup := setupTestEnvironment(b)
			defer cleanup()

			// Create test file
			filePath := filepath.Join(testDir, "seqread_test.dat")
			err := createTestFile(b, filePath, fs.size)
			if err != nil {
				b.Fatalf("Failed to create test file: %v", err)
			}

			b.ResetTimer()
			b.SetBytes(fs.size) // Set bytes processed per operation

			for i := 0; i < b.N; i++ {
				// Use streaming read with fadvise enabled
				totalBytes, err := readFileStreamingWithFadvise(b, filePath, false)
				if err != nil {
					b.Fatalf("Failed to read file: %v", err)
				}
				// Ensure data is used to prevent compiler optimization
				b.SetBytes(totalBytes)
			}

			// Capture page cache statistics
			capturePageCacheStats(b, fmt.Sprintf("after_seqread_%s_fadvis_enabled", fs.name))
			capturePageCacheStatsWithDelay(b, fmt.Sprintf("after_seqread_%s_fadvis_enabled_delay", fs.name), 3)
		})

		b.Run(fmt.Sprintf("WithoutFadvise_%s", fs.name), func(b *testing.B) {
			// Create test directory
			testDir, cleanup := setupTestEnvironment(b)
			defer cleanup()

			// Create test file
			filePath := filepath.Join(testDir, "seqread_test.dat")
			err := createTestFile(b, filePath, fs.size)
			if err != nil {
				b.Fatalf("Failed to create test file: %v", err)
			}

			b.ResetTimer()
			b.SetBytes(fs.size) // Set bytes processed per operation

			for i := 0; i < b.N; i++ {
				// Use streaming read with fadvise disabled
				totalBytes, err := readFileStreamingWithFadvise(b, filePath, true)
				if err != nil {
					b.Fatalf("Failed to read file: %v", err)
				}
				// Ensure data is used to prevent compiler optimization
				b.SetBytes(totalBytes)
			}

			// Capture page cache statistics
			capturePageCacheStats(b, fmt.Sprintf("after_seqread_%s_fadvis_disabled", fs.name))
			capturePageCacheStatsWithDelay(b, fmt.Sprintf("after_seqread_%s_fadvis_disabled_delay", fs.name), 3)
		})
	}
}
