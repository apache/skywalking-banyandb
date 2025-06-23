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
//go:build linux
// +build linux

// benchmark package provides benchmarks for file read/write operations.
package benchmark

import (
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"testing"

	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/fs/fadvismonitor/utils"
)

// BenchmarkWritePerformance tests write performance with and without utils.
// Uses automatic eBPF mode detection (tracepoint with kprobe fallback).
func BenchmarkWritePerformance(b *testing.B) {
	for _, fileSize := range []int64{256 << 20, 1024 << 20} {
		// fadvis enabled by default - AUTOMATIC MODE DETECTION
		b.Run(fmt.Sprintf("Size_%dMB_WithFadvise_Auto", fileSize>>20), func(b *testing.B) {
			testDir, cleanup := utils.SetupTestEnvironment(b)
			defer cleanup()

			threshold := utils.CalculateRealisticThreshold()

			fileSystem := utils.NewTestFileSystem(threshold)
			// Use default monitoring with automatic mode detection
			utils.WithDefaultMonitoring(b, func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					filePath := filepath.Join(testDir, fmt.Sprintf("write_test_%d.dat", i))

					file := fs.MustCreateFile(fileSystem, filePath, 0o644, false)

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
			})

			utils.CapturePageCacheStatsWithDelay(b, "after_write_fadvise_enabled_delay", 3)
		})

		// fadvise disabled
		b.Run(fmt.Sprintf("Size_%dMB_WithoutFadvise", fileSize>>20), func(b *testing.B) {
			testDir, cleanup := utils.SetupTestEnvironment(b)
			defer cleanup()
			threshold := int64(1 << 40) // 1TB

			fileSystem := utils.NewTestFileSystem(threshold)

			utils.WithDefaultMonitoring(b, func(b *testing.B) {
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					filePath := filepath.Join(testDir, fmt.Sprintf("write_test_%d.dat", i))

					file := fs.MustCreateFile(fileSystem, filePath, 0o644, true)

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

// BenchmarkMultipleReads tests the performance impact of multiple reads on the same file.
func BenchmarkMultipleReads(b *testing.B) {
	fileSize := int64(256 * 1024 * 1024)
	readCount := 5

	// Test with fadvise enabled
	b.Run("WithFadvise", func(b *testing.B) {
		testDir, cleanup := utils.SetupTestEnvironment(b)
		defer cleanup()

		threshold := utils.CalculateRealisticThreshold()
		fileSystem := utils.NewTestFileSystem(threshold)

		filePath := filepath.Join(testDir, "multiple_read_test.dat")
		if err := utils.CreateTestFile(b, filePath, fileSize); err != nil {
			b.Fatalf("Failed to create test file: %v", err)
		}

		utils.WithDefaultMonitoring(b, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for j := 0; j < readCount; j++ {
					// 用注入的 fileSystem 打开并顺序读取
					f, err := fileSystem.OpenFile(filePath)
					if err != nil {
						b.Fatalf("open file failed: %v", err)
					}
					reader := f.SequentialRead()

					total := int64(0)
					buf := make([]byte, 32*1024)
					for {
						n, err := reader.Read(buf)
						total += int64(n)
						if err != nil {
							if errors.Is(err, io.EOF) {
								break
							}
							b.Fatalf("read failed: %v", err)
						}
					}
					reader.Close()
					f.Close()

					b.SetBytes(total)
				}
			}
			b.StopTimer()
		})
	})

	// Test with fadvise disabled
	b.Run("WithoutFadvise", func(b *testing.B) {
		testDir, cleanup := utils.SetupTestEnvironment(b)
		defer cleanup()

		threshold := int64(1 << 40)
		fileSystem := utils.NewTestFileSystem(threshold)

		filePath := filepath.Join(testDir, "multiple_read_test.dat")
		if err := utils.CreateTestFile(b, filePath, fileSize); err != nil {
			b.Fatalf("Failed to create test file: %v", err)
		}

		utils.WithDefaultMonitoring(b, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for j := 0; j < readCount; j++ {
					f, err := fileSystem.OpenFile(filePath)
					if err != nil {
						b.Fatalf("open file failed: %v", err)
					}
					reader := f.SequentialRead()

					total := int64(0)
					buf := make([]byte, 32*1024)
					for {
						n, err := reader.Read(buf)
						total += int64(n)
						if err != nil {
							if errors.Is(err, io.EOF) {
								break
							}
							b.Fatalf("read failed: %v", err)
						}
					}
					reader.Close()
					f.Close()

					b.SetBytes(total)
				}
			}
			b.StopTimer()
		})
	})
}

// BenchmarkMixedWorkload tests the performance of a mixed workload of reads and writes.
func BenchmarkMixedWorkload(b *testing.B) {
	fileSize := int64(256 * 1024 * 1024)

	// Test with fadvise enabled
	b.Run("WithFadvise", func(b *testing.B) {
		testDir, cleanup := utils.SetupTestEnvironment(b)
		defer cleanup()

		threshold := utils.CalculateRealisticThreshold()
		fileSystem := utils.NewTestFileSystem(threshold)

		utils.WithDefaultMonitoring(b, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				writePath := filepath.Join(testDir, fmt.Sprintf("mixed_write_%d.dat", i))
				file := fs.MustCreateFile(fileSystem, writePath, 0o644, false)
				writer := file.SequentialWrite()
				buf := make([]byte, 4096)
				for written := int64(0); written < fileSize; written += int64(len(buf)) {
					if _, err := writer.Write(buf); err != nil {
						b.Fatalf("write failed: %v", err)
					}
				}
				writer.Close()
				file.Close()

				f, err := fileSystem.OpenFile(writePath)
				if err != nil {
					b.Fatalf("open for read failed: %v", err)
				}
				reader := f.SequentialRead()
				total := int64(0)
				tmp := make([]byte, 32*1024)
				for {
					n, err := reader.Read(tmp)
					total += int64(n)
					if err != nil {
						if errors.Is(err, io.EOF) {
							break
						}
						b.Fatalf("read failed: %v", err)
					}
				}
				reader.Close()
				f.Close()
				b.SetBytes(total)

				writePath2 := filepath.Join(testDir, fmt.Sprintf("mixed_write2_%d.dat", i))
				if err := utils.CreateTestFile(b, writePath2, fileSize); err != nil {
					b.Fatalf("create second write failed: %v", err)
				}
			}
		})
	})

	// Test with fadvise disabled
	b.Run("WithoutFadvise", func(b *testing.B) {
		testDir, cleanup := utils.SetupTestEnvironment(b)
		defer cleanup()

		threshold := int64(1 << 40) // 1TB effectively disables fadvise
		fileSystem := utils.NewTestFileSystem(threshold)

		utils.WithDefaultMonitoring(b, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				writePath := filepath.Join(testDir, fmt.Sprintf("mixed_write_%d.dat", i))
				file := fs.MustCreateFile(fileSystem, writePath, 0o644, true)
				writer := file.SequentialWrite()
				buf := make([]byte, 4096)
				for written := int64(0); written < fileSize; written += int64(len(buf)) {
					if _, err := writer.Write(buf); err != nil {
						b.Fatalf("write failed: %v", err)
					}
				}
				writer.Close()
				file.Close()
				b.StopTimer()

				f, err := fileSystem.OpenFile(writePath)
				if err != nil {
					b.Fatalf("open for read failed: %v", err)
				}
				reader := f.SequentialRead()
				total := int64(0)
				tmp := make([]byte, 32*1024)
				for {
					n, err := reader.Read(tmp)
					total += int64(n)
					if err != nil {
						if errors.Is(err, io.EOF) {
							break
						}
						b.Fatalf("read failed: %v", err)
					}
				}
				reader.Close()
				f.Close()
				b.SetBytes(total)

				writePath2 := filepath.Join(testDir, fmt.Sprintf("mixed_write2_%d.dat", i))
				if err := utils.CreateTestFile(b, writePath2, fileSize); err != nil {
					b.Fatalf("create second write failed: %v", err)
				}
				b.StartTimer()
			}
		})
	})
}

// BenchmarkSequentialRead tests sequential read performance using streaming to minimize heap allocations.
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
			testDir, cleanup := utils.SetupTestEnvironment(b)
			defer cleanup()

			filePath := filepath.Join(testDir, "seqread_test.dat")
			err := utils.CreateTestFile(b, filePath, fs.size)
			if err != nil {
				b.Fatalf("Failed to create test file: %v", err)
			}

			utils.WithDefaultMonitoring(b, func(b *testing.B) {
				b.ResetTimer()
				b.SetBytes(fs.size)

				for i := 0; i < b.N; i++ {
					// Use streaming read with fadvise enabled
					totalBytes, err := utils.ReadFileStreamingWithFadvise(b, filePath, false)
					if err != nil {
						b.Fatalf("Failed to read file: %v", err)
					}
					b.SetBytes(totalBytes)
				}

				b.StopTimer()
			})
		})

		b.Run(fmt.Sprintf("WithoutFadvise_%s", fs.name), func(b *testing.B) {
			// Create test directory
			testDir, cleanup := utils.SetupTestEnvironment(b)
			defer cleanup()

			filePath := filepath.Join(testDir, "seqread_test.dat")
			err := utils.CreateTestFile(b, filePath, fs.size)
			if err != nil {
				b.Fatalf("Failed to create test file: %v", err)
			}

			utils.WithDefaultMonitoring(b, func(b *testing.B) {
				b.ResetTimer()
				b.SetBytes(fs.size)

				for i := 0; i < b.N; i++ {
					// Use streaming read with fadvise disabled
					totalBytes, err := utils.ReadFileStreamingWithFadvise(b, filePath, true)
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

// TestKprobeFallback tests the kprobe fallback functionality
func TestKprobeFallback(t *testing.T) {
	testDir, cleanup := utils.SetupTestEnvironment(t)
	defer cleanup()

	filePath := filepath.Join(testDir, "kprobe_test.dat")
	fileSize := int64(10 << 20) // 10MB
	err := utils.CreateTestFile(t, filePath, fileSize)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Test kprobe-only monitor
	t.Run("KprobeOnly", func(t *testing.T) {
		utils.WithKprobeOnlyMonitoring(t, func(t *testing.T) {
			// Perform some file operations to trigger kprobe events
			totalBytes, err := utils.ReadFileStreamingWithFadvise(t, filePath, false)
			if err != nil {
				t.Fatalf("Failed to read file: %v", err)
			}

			if totalBytes != fileSize {
				t.Errorf("Expected to read %d bytes, got %d", fileSize, totalBytes)
			}
		})
	})

	// Test environment variable method
	t.Run("EnvironmentVariable", func(t *testing.T) {
		// Set environment variable to force kprobe
		t.Setenv("FORCE_KPROBE", "1")

		utils.WithDefaultTestMonitoring(t, func(t *testing.T) {
			// Perform some file operations
			totalBytes, err := utils.ReadFileStreamingWithFadvise(t, filePath, false)
			if err != nil {
				t.Fatalf("Failed to read file: %v", err)
			}

			if totalBytes != fileSize {
				t.Errorf("Expected to read %d bytes, got %d", fileSize, totalBytes)
			}
		})
	})
}
