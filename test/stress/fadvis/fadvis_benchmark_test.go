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
)

// BenchmarkWritePerformance tests write performance with and without fadvis.
func BenchmarkWritePerformance(b *testing.B) {
	// Test cases with different file sizes
	for _, fileSize := range []int64{1 * 1024 * 1024, 256 * 1024 * 1024} {
		// Test with fadvis enabled
		b.Run(fmt.Sprintf("Size_%dMB_FadvisEnabled", fileSize/(1024*1024)), func(b *testing.B) {
			testDir, cleanup := setupTestEnvironment(b)
			defer cleanup()

			// Set the fadvis threshold
			setTestThreshold(64 * 1024 * 1024)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				filePath := filepath.Join(testDir, fmt.Sprintf("write_test_%d.dat", i))
				if err := createTestFile(b, filePath, fileSize); err != nil {
					b.Fatalf("Failed to create test file: %v", err)
				}
				// No need to manually apply fadvis, the fs package handles it automatically
			}
		})

		// Test with fadvis disabled
		b.Run(fmt.Sprintf("Size_%dMB_FadvisDisabled", fileSize/(1024*1024)), func(b *testing.B) {
			testDir, cleanup := setupTestEnvironment(b)
			defer cleanup()

			// Set the fadvis threshold to a high value to disable it
			setTestThreshold(1 * 1024 * 1024 * 1024 * 1024)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				filePath := filepath.Join(testDir, fmt.Sprintf("write_test_%d.dat", i))
				if err := createTestFile(b, filePath, fileSize); err != nil {
					b.Fatalf("Failed to create test file: %v", err)
				}
				// No need to manually apply fadvis, the fs package handles it automatically
			}
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

			// Set the fadvis threshold
			setTestThreshold(64 * 1024 * 1024)

			// Create a test file
			filePath := filepath.Join(testDir, "read_test.dat")
			if err := createTestFile(b, filePath, fileSize); err != nil {
				b.Fatalf("Failed to create test file: %v", err)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Read the file using our helper function which uses fs package
				data, err := readFileWithFadvise(b, filePath)
				if err != nil {
					b.Fatalf("Failed to read file: %v", err)
				}

				// Ensure data is used to prevent compiler optimization
				_ = len(data)
				// No need to manually apply fadvis, the fs package handles it automatically
			}
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
				// Read the file using our helper function which uses fs package
				data, err := readFileWithFadvise(b, filePath)
				if err != nil {
					b.Fatalf("Failed to read file: %v", err)
				}

				// Ensure data is used to prevent compiler optimization
				_ = len(data)
				// No need to manually apply fadvis, the fs package handles it automatically
			}
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

		// Set the fadvis threshold
		setTestThreshold(64 * 1024 * 1024)

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
				// No need to manually apply fadvis, the fs package handles it automatically
			}
		}
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
				// Read the file using our helper function which uses fs package
				data, err := readFileWithFadvise(b, filePath)
				if err != nil {
					b.Fatalf("Failed to read file: %v", err)
				}

				// Ensure data is used to prevent compiler optimization
				_ = len(data)
				// No need to manually apply fadvis, the fs package handles it automatically
			}
		}
	})
}

// BenchmarkMixedWorkload tests the performance of a mixed workload of reads and writes.
func BenchmarkMixedWorkload(b *testing.B) {
	fileSize := int64(256 * 1024 * 1024)

	// Test with fadvis enabled
	b.Run("MixedWorkload_FadvisEnabled", func(b *testing.B) {
		testDir, cleanup := setupTestEnvironment(b)
		defer cleanup()

		// Set the fadvis threshold
		setTestThreshold(64 * 1024 * 1024)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Create a new file
			writeFilePath := filepath.Join(testDir, fmt.Sprintf("mixed_write_%d.dat", i))
			if err := createTestFile(b, writeFilePath, fileSize); err != nil {
				b.Fatalf("Failed to create write file: %v", err)
			}
			// No need to manually apply fadvis, the fs package handles it automatically

			// Read the file we just created
			data, err := readFileWithFadvise(b, writeFilePath)
			if err != nil {
				b.Fatalf("Failed to read file: %v", err)
			}
			_ = len(data)
			// No need to manually apply fadvis, the fs package handles it automatically

			// Create another file
			writeFilePath2 := filepath.Join(testDir, fmt.Sprintf("mixed_write2_%d.dat", i))
			if err := createTestFile(b, writeFilePath2, fileSize); err != nil {
				b.Fatalf("Failed to create second write file: %v", err)
			}
			// No need to manually apply fadvis, the fs package handles it automatically
		}
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
			// No need to manually apply fadvis, the fs package handles it automatically

			// Read the file we just created
			data, err := readFileWithFadvise(b, writeFilePath)
			if err != nil {
				b.Fatalf("Failed to read file: %v", err)
			}
			_ = len(data)
			// No need to manually apply fadvis, the fs package handles it automatically

			// Create another file
			writeFilePath2 := filepath.Join(testDir, fmt.Sprintf("mixed_write2_%d.dat", i))
			if err := createTestFile(b, writeFilePath2, fileSize); err != nil {
				b.Fatalf("Failed to create second write file: %v", err)
			}
			// No need to manually apply fadvis, the fs package handles it automatically
		}
	})
}
