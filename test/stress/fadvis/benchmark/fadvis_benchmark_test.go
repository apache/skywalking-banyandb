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
	"io"
	"path/filepath"
	"testing"

	"github.com/apache/skywalking-banyandb/test/stress/fadvis/utils"

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

// BenchmarkWritePerformance tests write performance with and without utils.
func BenchmarkWritePerformance(b *testing.B) {
	// Test cases with different file sizes - using larger files to better show utils effects
	for _, fileSize := range []int64{256 << 20, 1024 << 20} {
        // fadvis enabled by default
        b.Run(fmt.Sprintf("Size_%dMB_utilsEnabled", fileSize>>20), func(b *testing.B) {
            testDir, cleanup := utils.SetupTestEnvironment(b)
            defer cleanup()

            // set a realistic threshold based on system memory
            utils.SetRealisticThreshold()

            fileSystem := fs.NewLocalFileSystem()

            utils.WithMonitoringLegacy(b, func(b *testing.B) {
                b.ResetTimer()
                for i := 0; i < b.N; i++ {
                    filePath := filepath.Join(testDir, fmt.Sprintf("write_test_%d.dat", i))

                    // cached=false means need to clean pagecache
                    file := fs.MustCreateFile(fileSystem, filePath, 0644, false)

                    // sequential write
                    writer := file.SequentialWrite()
                    buf := make([]byte, 4096)
                    for written := int64(0); written < fileSize; written += int64(len(buf)) {
                        if _, err := writer.Write(buf); err != nil {
                            b.Fatalf("write failed: %v", err)
                        }
                    }

                    if err := writer.Close(); err != nil {
                        b.Fatalf("writer.Close() failed: %v", err)
                    }
                    if err := file.Close(); err != nil {
                        b.Fatalf("file.Close() failed: %v", err)
                    }
                }
                b.StopTimer()
            })

            utils.CapturePageCacheStatsWithDelay(b, "after_write_utils_enabled_delay", 3)
        })

        // fadvise disabled
        b.Run(fmt.Sprintf("Size_%dMB_utilsDisabled", fileSize>>20), func(b *testing.B) {
            testDir, cleanup := utils.SetupTestEnvironment(b)
            defer cleanup()

            // set the utils threshold to a high value to disable it
            utils.SetTestThreshold(1 << 40) // 1TB

            fileSystem := fs.NewLocalFileSystem()

            utils.WithMonitoringLegacy(b, func(b *testing.B) {
                b.ResetTimer()
                for i := 0; i < b.N; i++ {
                    filePath := filepath.Join(testDir, fmt.Sprintf("write_test_%d.dat", i))

                    // cached=true means  not to clean pagecache
                    file := fs.MustCreateFile(fileSystem, filePath, 0644, true)

                    writer := file.SequentialWrite()
                    buf := make([]byte, 4096)
                    for written := int64(0); written < fileSize; written += int64(len(buf)) {
                        if _, err := writer.Write(buf); err != nil {
                            b.Fatalf("write failed: %v", err)
                        }
                    }

                    if err := writer.Close(); err != nil {
                        b.Fatalf("writer.Close() failed: %v", err)
                    }
                    if err := file.Close(); err != nil {
                        b.Fatalf("file.Close() failed: %v", err)
                    }
                }
                b.StopTimer()
            })
        })
    }
}

// BenchmarkReadPerformance tests read performance with and without utils.
func BenchmarkReadPerformance(b *testing.B) {
	// Test cases with different file sizes
	for _, fileSize := range []int64{1 * 1024 * 1024, 256 * 1024 * 1024} {
		// Test with utils enabled
		b.Run(fmt.Sprintf("Size_%dMB_utilsEnabled", fileSize/(1024*1024)), func(b *testing.B) {
			testDir, cleanup := utils.SetupTestEnvironment(b)
			defer cleanup()

			// Set a realistic utils threshold based on system memory
			utils.SetRealisticThreshold()

			// Create a test file
			filePath := filepath.Join(testDir, "read_test.dat")
			if err := utils.CreateTestFile(b, filePath, fileSize); err != nil {
				b.Fatalf("Failed to create test file: %v", err)
			}

			utils.WithMonitoringLegacy(b, func(b *testing.B) {
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					data, err := utils.ReadFileWithFadvise(b, filePath)
					if err != nil {
						b.Fatalf("Failed to read file: %v", err)
					}
					b.SetBytes(int64(len(data)))
				}
				b.StopTimer()
			})
		})

		// Test with utils disabled
		b.Run(fmt.Sprintf("Size_%dMB_utilsDisabled", fileSize/(1024*1024)), func(b *testing.B) {
			testDir, cleanup := utils.SetupTestEnvironment(b)
			defer cleanup()

			// Set the utils threshold to a high value to disable it
			utils.SetTestThreshold(1 * 1024 * 1024 * 1024 * 1024)

			// Create a test file
			filePath := filepath.Join(testDir, "read_test.dat")
			if err := utils.CreateTestFile(b, filePath, fileSize); err != nil {
				b.Fatalf("Failed to create test file: %v", err)
			}

			utils.WithMonitoringLegacy(b, func(b *testing.B) {
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					// Read file without applying utils
					totalBytes, err := utils.ReadFileStreamingWithFadvise(b, filePath, true) // skiputilse=true
					if err != nil {
						b.Fatalf("Failed to read file: %v", err)
					}
					b.SetBytes(totalBytes)
				}
				b.StopTimer()
			})
		})
	}
}

// BenchmarkMultipleReads tests the performance impact of multiple reads on the same file.
func BenchmarkMultipleReads(b *testing.B) {
	fileSize := int64(256 * 1024 * 1024)
	readCount := 5

	// Test with utils enabled
	b.Run("MultipleReads_utilsEnabled", func(b *testing.B) {
		testDir, cleanup := utils.SetupTestEnvironment(b)
		defer cleanup()

		// Set a realistic utils threshold based on system memory
		utils.SetRealisticThreshold()

		// Create a test file
		filePath := filepath.Join(testDir, "multiple_read_test.dat")
		if err := utils.CreateTestFile(b, filePath, fileSize); err != nil {
			b.Fatalf("Failed to create test file: %v", err)
		}

		utils.WithMonitoringLegacy(b, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for j := 0; j < readCount; j++ {
					// Read the file using our helper function which uses fs package
					data, err := utils.ReadFileWithFadvise(b, filePath)
					if err != nil {
						b.Fatalf("Failed to read file: %v", err)
					}

					// Ensure data is used to prevent compiler optimization
					_ = len(data)
				}
			}
			b.StopTimer()
		})
	})

	// Test with utils disabled
	b.Run("MultipleReads_utilsDisabled", func(b *testing.B) {
		testDir, cleanup := utils.SetupTestEnvironment(b)
		defer cleanup()

		// Set the utils threshold to a high value to disable it
		utils.SetTestThreshold(1 * 1024 * 1024 * 1024 * 1024)

		// Create a test file
		filePath := filepath.Join(testDir, "multiple_read_test.dat")
		if err := utils.CreateTestFile(b, filePath, fileSize); err != nil {
			b.Fatalf("Failed to create test file: %v", err)
		}

		utils.WithMonitoringLegacy(b, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for j := 0; j < readCount; j++ {
					// Read the file using streaming to minimize heap allocations
					totalBytes, err := utils.ReadFileStreamingWithFadvise(b, filePath, true) // skiputilse=true
					if err != nil {
						b.Fatalf("Failed to read file: %v", err)
					}

					// Ensure data is used to prevent compiler optimization
					_ = totalBytes
				}
			}
			b.StopTimer()
		})
	})
}

// BenchmarkMixedWorkload tests the performance of a mixed workload of reads and writes.
func BenchmarkMixedWorkload(b *testing.B) {
	fileSize := int64(256 * 1024 * 1024)

	// Test with utils enabled
	b.Run("MixedWorkload_utilsEnabled", func(b *testing.B) {
		testDir, cleanup := utils.SetupTestEnvironment(b)
		defer cleanup()

		// Set a realistic utils threshold based on system memory
		utils.SetRealisticThreshold()

		utils.WithMonitoringLegacy(b, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Create a new file
				writeFilePath := filepath.Join(testDir, fmt.Sprintf("mixed_write_%d.dat", i))
				if err := utils.CreateTestFile(b, writeFilePath, fileSize); err != nil {
					b.Fatalf("Failed to create write file: %v", err)
				}

				// Read the file we just created using streaming to minimize heap allocations
				totalBytes, err := utils.ReadFileStreamingWithFadvise(b, writeFilePath, true) // skiputilse=true
				if err != nil {
					b.Fatalf("Failed to read file: %v", err)
				}
				_ = totalBytes

				// Create another file
				writeFilePath2 := filepath.Join(testDir, fmt.Sprintf("mixed_write2_%d.dat", i))
				if err := utils.CreateTestFile(b, writeFilePath2, fileSize); err != nil {
					b.Fatalf("Failed to create second write file: %v", err)
				}
			}
		})
	})

	// Test with utils disabled
	b.Run("MixedWorkload_utilsDisabled", func(b *testing.B) {
		testDir, cleanup := utils.SetupTestEnvironment(b)
		defer cleanup()

		// Set the utils threshold to a high value to disable it
		utils.SetTestThreshold(1 * 1024 * 1024 * 1024 * 1024)

		utils.WithMonitoringLegacy(b, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Create a new file
				writeFilePath := filepath.Join(testDir, fmt.Sprintf("mixed_write_%d.dat", i))
				if err := utils.CreateTestFile(b, writeFilePath, fileSize); err != nil {
					b.Fatalf("Failed to create write file: %v", err)
				}

				// Read the file we just created using streaming to minimize heap allocations
				totalBytes, err := utils.ReadFileStreamingWithFadvise(b, writeFilePath, true) // skiputilse=true
				if err != nil {
					b.Fatalf("Failed to read file: %v", err)
				}
				_ = totalBytes

				// Create another file
				writeFilePath2 := filepath.Join(testDir, fmt.Sprintf("mixed_write2_%d.dat", i))
				if err := utils.CreateTestFile(b, writeFilePath2, fileSize); err != nil {
					b.Fatalf("Failed to create second write file: %v", err)
				}
			}
		})
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
		b.Run(fmt.Sprintf("Withutilse_%s", fs.name), func(b *testing.B) {
			// Create test directory
			testDir, cleanup := utils.SetupTestEnvironment(b)
			defer cleanup()

			// Create test file
			filePath := filepath.Join(testDir, "seqread_test.dat")
			err := utils.CreateTestFile(b, filePath, fs.size)
			if err != nil {
				b.Fatalf("Failed to create test file: %v", err)
			}

			utils.WithMonitoringLegacy(b, func(b *testing.B) {
				b.ResetTimer()
				b.SetBytes(fs.size) // Set bytes processed per operation

				for i := 0; i < b.N; i++ {
					// Use streaming read with utilse enabled
					totalBytes, err := utils.ReadFileStreamingWithFadvise(b, filePath, false)
					if err != nil {
						b.Fatalf("Failed to read file: %v", err)
					}
					// Ensure data is used to prevent compiler optimization
					b.SetBytes(totalBytes)
				}

				b.StopTimer()
			})
		})

		b.Run(fmt.Sprintf("Withoututilse_%s", fs.name), func(b *testing.B) {
			// Create test directory
			testDir, cleanup := utils.SetupTestEnvironment(b)
			defer cleanup()

			// Create test file
			filePath := filepath.Join(testDir, "seqread_test.dat")
			err := utils.CreateTestFile(b, filePath, fs.size)
			if err != nil {
				b.Fatalf("Failed to create test file: %v", err)
			}

			utils.WithMonitoringLegacy(b, func(b *testing.B) {
				b.ResetTimer()
				b.SetBytes(fs.size) // Set bytes processed per operation

				for i := 0; i < b.N; i++ {
					// Use streaming read with utilse disabled
					totalBytes, err := utils.ReadFileStreamingWithFadvise(b, filePath, true)
					if err != nil {
						b.Fatalf("Failed to read file: %v", err)
					}
					// Ensure data is used to prevent compiler optimization
					b.SetBytes(totalBytes)
				}

				b.StopTimer()
			})
		})
	}
}
