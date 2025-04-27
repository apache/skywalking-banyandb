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
	"time"

	"github.com/stretchr/testify/require"
)

// BenchmarkMergeMemoryUsage tests the memory usage of merge operations with and without fadvis.
// This benchmark focuses specifically on memory usage and page cache behavior.
func BenchmarkMergeMemoryUsage(b *testing.B) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		b.Skip("This test is designed for Linux and macOS")
	}

	// Create a temporary directory for the test
	testDir, err := os.MkdirTemp("", "fadvis_memory_benchmark")
	require.NoError(b, err)
	defer os.RemoveAll(testDir)

	// Prepare test files - use larger files to better observe memory effects
	b.Logf("PHASE_START: %s %s", time.Now().Format(time.RFC3339), "PREPARE_TEST_FILES")
	parts := createTestParts(b, testDir, 5, LargeFileSize*2) // 400MB parts
	b.Logf("PHASE_END: %s %s", time.Now().Format(time.RFC3339), "PREPARE_TEST_FILES")

	// Create output directories
	outputDirNoFadvise := filepath.Join(testDir, "no_fadvise")
	outputDirWithFadvise := filepath.Join(testDir, "with_fadvise")
	require.NoError(b, os.MkdirAll(outputDirNoFadvise, 0755))
	require.NoError(b, os.MkdirAll(outputDirWithFadvise, 0755))

	// Print instructions for memory monitoring
	pid := os.Getpid()
	b.Logf("\n\nIMPORTANT: To monitor memory usage, run in another terminal:\n")
	b.Logf("chmod +x ./test/stress/fadvis/monitor_pagecache.sh")
	b.Logf("./test/stress/fadvis/monitor_pagecache.sh %d 0.5 300\n\n", pid)

	// Run benchmark with fadvise disabled
	b.Run("WithoutFadvise", func(b *testing.B) {
		// Set the fadvis threshold to a high value to disable it
		setTestThreshold(terabyte)
		b.Logf("\n=== STARTING WITHOUT FADVISE TEST (PID: %d) ===", pid)
		b.Logf("Output directory: %s", outputDirNoFadvise)
		runMemoryProfiledMerge(b, parts, outputDirNoFadvise)
	})

	// Wait between tests to allow for monitoring
	b.Log("\n=== WAITING 10 SECONDS BETWEEN TESTS... ===")
	time.Sleep(10 * time.Second)

	// Run benchmark with fadvise enabled
	b.Run("WithFadvise", func(b *testing.B) {
		// Set a realistic fadvis threshold based on system memory
		setRealisticThreshold()
		b.Logf("\n=== STARTING WITH FADVISE TEST (PID: %d) ===", pid)
		b.Logf("Output directory: %s", outputDirWithFadvise)
		runMemoryProfiledMerge(b, parts, outputDirWithFadvise)
	})

	// Print final instructions
	b.Logf("\n\nTests completed. Results are in:\n%s\n%s\n",
		outputDirNoFadvise, outputDirWithFadvise)
}

// runMemoryProfiledMerge performs merge operations with memory profiling
func runMemoryProfiledMerge(b *testing.B, parts []string, outputDir string) {
	// Reset memory stats before starting
	b.Logf("PHASE_START: %s %s", time.Now().Format(time.RFC3339), "MEMORY_RESET")
	runtime.GC()
	b.Logf("PHASE_END: %s %s", time.Now().Format(time.RFC3339), "MEMORY_RESET")

	// Record initial memory stats
	b.Logf("PHASE_START: %s %s", time.Now().Format(time.RFC3339), "MEMORY_STATS_BEFORE")
	var memStatsBefore, memStatsAfter runtime.MemStats
	runtime.ReadMemStats(&memStatsBefore)
	b.Logf("PHASE_END: %s %s", time.Now().Format(time.RFC3339), "MEMORY_STATS_BEFORE")

	// Announce start of operation for monitoring
	b.Log("\n=== STARTING MERGE OPERATION - BEGIN MONITORING NOW ===")
	time.Sleep(2 * time.Second) // Give time to start monitoring

	// Perform the merge operations
	b.Logf("PHASE_START: %s %s", time.Now().Format(time.RFC3339), "MERGE_OPERATION")
	mergeStartTime := time.Now()
	for i := 0; i < b.N; i++ {
		outputFile := filepath.Join(outputDir, fmt.Sprintf("merged_%d", i))
		b.Logf("Merging to: %s", outputFile)
		err := simulateMergeOperation(b, parts, outputFile)
		require.NoError(b, err)

		// Log file size after merge
		fileInfo, err := os.Stat(outputFile)
		if err == nil {
			b.Logf("Merged file size: %d bytes", fileInfo.Size())
		}
	}
	mergeEndTime := time.Now()
	b.Logf("Merge operation took: %v", mergeEndTime.Sub(mergeStartTime))
	b.Logf("PHASE_END: %s %s", time.Now().Format(time.RFC3339), "MERGE_OPERATION")

	// Announce completion of operation
	b.Log("\n=== MERGE OPERATION COMPLETED - CONTINUE MONITORING ===")

	// Force garbage collection to get accurate memory usage
	b.Logf("PHASE_START: %s %s", time.Now().Format(time.RFC3339), "GARBAGE_COLLECTION")
	runtime.GC()
	b.Logf("PHASE_END: %s %s", time.Now().Format(time.RFC3339), "GARBAGE_COLLECTION")

	// Record final memory stats
	b.Logf("PHASE_START: %s %s", time.Now().Format(time.RFC3339), "MEMORY_STATS_AFTER")
	runtime.ReadMemStats(&memStatsAfter)
	b.Logf("PHASE_END: %s %s", time.Now().Format(time.RFC3339), "MEMORY_STATS_AFTER")

	// Write memory stats to a file
	writeMemStats(outputDir, memStatsBefore, memStatsAfter)

	// Sleep to allow for OS-level memory measurements
	b.Log("\n=== WAITING FOR MEMORY RELEASE (30 seconds) ===")
	b.Log(fmt.Sprintf("Process ID: %d", os.Getpid()))
	b.Log(fmt.Sprintf("Output directory: %s", outputDir))

	// Wait longer to observe memory release patterns
	b.Logf("PHASE_START: %s %s", time.Now().Format(time.RFC3339), "MEMORY_RELEASE_WAIT")
	time.Sleep(30 * time.Second)
	b.Logf("PHASE_END: %s %s", time.Now().Format(time.RFC3339), "MEMORY_RELEASE_WAIT")

	b.Log("=== MEMORY MONITORING COMPLETE FOR THIS TEST ===")
}

// writeMemStats writes memory statistics to a file
func writeMemStats(outputDir string, before, after runtime.MemStats) {
	f, err := os.Create(filepath.Join(outputDir, "memory_stats.txt"))
	if err != nil {
		return
	}
	defer f.Close()

	fmt.Fprintf(f, "Memory Statistics\n")
	fmt.Fprintf(f, "=================\n\n")
	fmt.Fprintf(f, "Before Operation:\n")
	fmt.Fprintf(f, "  Alloc: %d bytes\n", before.Alloc)
	fmt.Fprintf(f, "  TotalAlloc: %d bytes\n", before.TotalAlloc)
	fmt.Fprintf(f, "  Sys: %d bytes\n", before.Sys)
	fmt.Fprintf(f, "  HeapAlloc: %d bytes\n", before.HeapAlloc)
	fmt.Fprintf(f, "  HeapSys: %d bytes\n", before.HeapSys)
	fmt.Fprintf(f, "  HeapInuse: %d bytes\n\n", before.HeapInuse)

	fmt.Fprintf(f, "After Operation:\n")
	fmt.Fprintf(f, "  Alloc: %d bytes\n", after.Alloc)
	fmt.Fprintf(f, "  TotalAlloc: %d bytes\n", after.TotalAlloc)
	fmt.Fprintf(f, "  Sys: %d bytes\n", after.Sys)
	fmt.Fprintf(f, "  HeapAlloc: %d bytes\n", after.HeapAlloc)
	fmt.Fprintf(f, "  HeapSys: %d bytes\n", after.HeapSys)
	fmt.Fprintf(f, "  HeapInuse: %d bytes\n\n", after.HeapInuse)

	fmt.Fprintf(f, "Differences:\n")
	fmt.Fprintf(f, "  Alloc: %d bytes\n", after.Alloc-before.Alloc)
	fmt.Fprintf(f, "  TotalAlloc: %d bytes\n", after.TotalAlloc-before.TotalAlloc)
	fmt.Fprintf(f, "  Sys: %d bytes\n", after.Sys-before.Sys)
	fmt.Fprintf(f, "  HeapAlloc: %d bytes\n", after.HeapAlloc-before.HeapAlloc)
	fmt.Fprintf(f, "  HeapSys: %d bytes\n", after.HeapSys-before.HeapSys)
	fmt.Fprintf(f, "  HeapInuse: %d bytes\n", after.HeapInuse-before.HeapInuse)
}

// BenchmarkWriteMemoryUsage tests the memory usage of write operations with and without fadvis.
// This benchmark focuses specifically on memory usage and page cache behavior during file writes.
func BenchmarkWriteMemoryUsage(b *testing.B) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		b.Skip("This test is designed for Linux and macOS")
	}

	// Create a temporary directory for the test
	testDir, err := os.MkdirTemp("", "fadvis_write_memory_benchmark")
	require.NoError(b, err)
	defer os.RemoveAll(testDir)

	// Create output directories
	outputDirNoFadvise := filepath.Join(testDir, "no_fadvise")
	outputDirWithFadvise := filepath.Join(testDir, "with_fadvise")
	require.NoError(b, os.MkdirAll(outputDirNoFadvise, 0755))
	require.NoError(b, os.MkdirAll(outputDirWithFadvise, 0755))

	// Print instructions for memory monitoring
	pid := os.Getpid()
	b.Logf("\n\nIMPORTANT: To monitor memory usage, run in another terminal:\n")
	b.Logf("chmod +x ./test/stress/fadvis/monitor_pagecache.sh")
	b.Logf("./test/stress/fadvis/monitor_pagecache.sh %d 0.5 300\n\n", pid)

	// Run benchmark with fadvise disabled
	b.Run("WithoutFadvise", func(b *testing.B) {
		// Set the fadvis threshold to a high value to disable it
		setTestThreshold(terabyte)
		b.Logf("\n=== STARTING WITHOUT FADVISE TEST (PID: %d) ===", pid)
		b.Logf("Output directory: %s", outputDirNoFadvise)
		runMemoryProfiledWrite(b, outputDirNoFadvise, LargeFileSize*2) // 400MB files
	})

	// Wait between tests to allow for monitoring
	b.Log("\n=== WAITING 10 SECONDS BETWEEN TESTS... ===")
	time.Sleep(10 * time.Second)

	// Run benchmark with fadvise enabled
	b.Run("WithFadvise", func(b *testing.B) {
		// Set a realistic fadvis threshold based on system memory
		setRealisticThreshold()
		b.Logf("\n=== STARTING WITH FADVISE TEST (PID: %d) ===", pid)
		b.Logf("Output directory: %s", outputDirWithFadvise)
		runMemoryProfiledWrite(b, outputDirWithFadvise, LargeFileSize*2) // 400MB files
	})

	// Print final instructions
	b.Logf("\n\nTests completed. Results are in:\n%s\n%s\n",
		outputDirNoFadvise, outputDirWithFadvise)
}

// runMemoryProfiledWrite performs write operations with memory profiling
func runMemoryProfiledWrite(b *testing.B, outputDir string, fileSize int64) {
	// Reset memory stats before starting
	b.Logf("PHASE_START: %s %s", time.Now().Format(time.RFC3339), "MEMORY_RESET")
	runtime.GC()
	b.Logf("PHASE_END: %s %s", time.Now().Format(time.RFC3339), "MEMORY_RESET")

	// Record initial memory stats
	b.Logf("PHASE_START: %s %s", time.Now().Format(time.RFC3339), "MEMORY_STATS_BEFORE")
	var memStatsBefore, memStatsAfter runtime.MemStats
	runtime.ReadMemStats(&memStatsBefore)
	b.Logf("PHASE_END: %s %s", time.Now().Format(time.RFC3339), "MEMORY_STATS_BEFORE")

	// Announce start of operation for monitoring
	b.Log("\n=== STARTING WRITE OPERATION - BEGIN MONITORING NOW ===")
	time.Sleep(2 * time.Second) // Give time to start monitoring

	// Perform the write operations
	b.Logf("PHASE_START: %s %s", time.Now().Format(time.RFC3339), "WRITE_OPERATION")
	writeStartTime := time.Now()
	for i := 0; i < b.N; i++ {
		filePath := filepath.Join(outputDir, fmt.Sprintf("write_test_%d.dat", i))
		b.Logf("Writing to: %s", filePath)
		err := createTestFile(b, filePath, fileSize)
		require.NoError(b, err)

		// Log file size after write
		fileInfo, err := os.Stat(filePath)
		if err == nil {
			b.Logf("File size: %d bytes", fileInfo.Size())
		}
	}
	writeEndTime := time.Now()
	b.Logf("Write operation took: %v", writeEndTime.Sub(writeStartTime))
	b.Logf("PHASE_END: %s %s", time.Now().Format(time.RFC3339), "WRITE_OPERATION")

	// Announce completion of operation
	b.Log("\n=== WRITE OPERATION COMPLETED - CONTINUE MONITORING ===")

	// Force garbage collection to get accurate memory usage
	b.Logf("PHASE_START: %s %s", time.Now().Format(time.RFC3339), "GARBAGE_COLLECTION")
	runtime.GC()
	b.Logf("PHASE_END: %s %s", time.Now().Format(time.RFC3339), "GARBAGE_COLLECTION")

	// Record final memory stats
	b.Logf("PHASE_START: %s %s", time.Now().Format(time.RFC3339), "MEMORY_STATS_AFTER")
	runtime.ReadMemStats(&memStatsAfter)
	b.Logf("PHASE_END: %s %s", time.Now().Format(time.RFC3339), "MEMORY_STATS_AFTER")

	// Write memory stats to a file
	writeMemStats(outputDir, memStatsBefore, memStatsAfter)

	// Sleep to allow for OS-level memory measurements
	b.Log("\n=== WAITING FOR MEMORY RELEASE (30 seconds) ===")
	b.Log(fmt.Sprintf("Process ID: %d", os.Getpid()))
	b.Log(fmt.Sprintf("Output directory: %s", outputDir))

	// Wait longer to observe memory release patterns
	b.Logf("PHASE_START: %s %s", time.Now().Format(time.RFC3339), "MEMORY_RELEASE_WAIT")
	time.Sleep(30 * time.Second)
	b.Logf("PHASE_END: %s %s", time.Now().Format(time.RFC3339), "MEMORY_RELEASE_WAIT")

	b.Log("=== MEMORY MONITORING COMPLETE FOR THIS TEST ===")
}

// BenchmarkSeqReadMemoryUsage tests the memory usage of sequential read operations with and without fadvis.
// This benchmark focuses specifically on memory usage and page cache behavior during sequential reads.
func BenchmarkSeqReadMemoryUsage(b *testing.B) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		b.Skip("This test is designed for Linux and macOS")
	}

	// Create a temporary directory for the test
	testDir, err := os.MkdirTemp("", "fadvis_seqread_memory_benchmark")
	require.NoError(b, err)
	defer os.RemoveAll(testDir)

	// Create a large test file for reading
	testFilePath := filepath.Join(testDir, "large_test_file.dat")
	b.Logf("PHASE_START: %s %s", time.Now().Format(time.RFC3339), "PREPARE_TEST_FILE")
	err = createTestFile(b, testFilePath, LargeFileSize*4) // 800MB file
	require.NoError(b, err)
	b.Logf("PHASE_END: %s %s", time.Now().Format(time.RFC3339), "PREPARE_TEST_FILE")

	// Create output directories for results
	outputDirNoFadvise := filepath.Join(testDir, "no_fadvise")
	outputDirWithFadvise := filepath.Join(testDir, "with_fadvise")
	require.NoError(b, os.MkdirAll(outputDirNoFadvise, 0755))
	require.NoError(b, os.MkdirAll(outputDirWithFadvise, 0755))

	// Print instructions for memory monitoring
	pid := os.Getpid()
	b.Logf("\n\nIMPORTANT: To monitor memory usage, run in another terminal:\n")
	b.Logf("chmod +x ./test/stress/fadvis/monitor_pagecache.sh")
	b.Logf("./test/stress/fadvis/monitor_pagecache.sh %d 0.5 300\n\n", pid)

	// Run benchmark with fadvise disabled
	b.Run("WithoutFadvise", func(b *testing.B) {
		// Set the fadvis threshold to a high value to disable it
		setTestThreshold(terabyte)
		b.Logf("\n=== STARTING WITHOUT FADVISE TEST (PID: %d) ===", pid)
		b.Logf("Output directory: %s", outputDirNoFadvise)
		runMemoryProfiledSeqRead(b, testFilePath, outputDirNoFadvise, true) // skipFadvise=true
	})

	// Wait between tests to allow for monitoring
	b.Log("\n=== WAITING 10 SECONDS BETWEEN TESTS... ===")
	time.Sleep(10 * time.Second)

	// Run benchmark with fadvise enabled
	b.Run("WithFadvise", func(b *testing.B) {
		// Set a realistic fadvis threshold based on system memory
		setRealisticThreshold()
		b.Logf("\n=== STARTING WITH FADVISE TEST (PID: %d) ===", pid)
		b.Logf("Output directory: %s", outputDirWithFadvise)
		runMemoryProfiledSeqRead(b, testFilePath, outputDirWithFadvise, false) // skipFadvise=false
	})

	// Print final instructions
	b.Logf("\n\nTests completed. Results are in:\n%s\n%s\n",
		outputDirNoFadvise, outputDirWithFadvise)
}

// runMemoryProfiledSeqRead performs sequential read operations with memory profiling
func runMemoryProfiledSeqRead(b *testing.B, filePath string, outputDir string, skipFadvise bool) {
	// Reset memory stats before starting
	b.Logf("PHASE_START: %s %s", time.Now().Format(time.RFC3339), "MEMORY_RESET")
	runtime.GC()
	b.Logf("PHASE_END: %s %s", time.Now().Format(time.RFC3339), "MEMORY_RESET")

	// Record initial memory stats
	b.Logf("PHASE_START: %s %s", time.Now().Format(time.RFC3339), "MEMORY_STATS_BEFORE")
	var memStatsBefore, memStatsAfter runtime.MemStats
	runtime.ReadMemStats(&memStatsBefore)
	b.Logf("PHASE_END: %s %s", time.Now().Format(time.RFC3339), "MEMORY_STATS_BEFORE")

	// Announce start of operation for monitoring
	b.Log("\n=== STARTING SEQUENTIAL READ OPERATION - BEGIN MONITORING NOW ===")
	time.Sleep(2 * time.Second) // Give time to start monitoring

	// Perform the sequential read operations
	b.Logf("PHASE_START: %s %s", time.Now().Format(time.RFC3339), "SEQREAD_OPERATION")
	readStartTime := time.Now()
	for i := 0; i < b.N; i++ {
		b.Logf("Reading from: %s (skipFadvise=%v)", filePath, skipFadvise)
		totalBytes, err := readFileStreamingWithFadvise(b, filePath, skipFadvise)
		require.NoError(b, err)
		b.Logf("Read %d bytes", totalBytes)
		
		// Write a small summary file to the output directory
		summaryPath := filepath.Join(outputDir, fmt.Sprintf("read_summary_%d.txt", i))
		summaryContent := fmt.Sprintf("Read %d bytes from %s with skipFadvise=%v\n", 
			totalBytes, filePath, skipFadvise)
		err = os.WriteFile(summaryPath, []byte(summaryContent), 0644)
		require.NoError(b, err)
	}
	readEndTime := time.Now()
	b.Logf("Sequential read operation took: %v", readEndTime.Sub(readStartTime))
	b.Logf("PHASE_END: %s %s", time.Now().Format(time.RFC3339), "SEQREAD_OPERATION")

	// Announce completion of operation
	b.Log("\n=== SEQUENTIAL READ OPERATION COMPLETED - CONTINUE MONITORING ===")

	// Force garbage collection to get accurate memory usage
	b.Logf("PHASE_START: %s %s", time.Now().Format(time.RFC3339), "GARBAGE_COLLECTION")
	runtime.GC()
	b.Logf("PHASE_END: %s %s", time.Now().Format(time.RFC3339), "GARBAGE_COLLECTION")

	// Record final memory stats
	b.Logf("PHASE_START: %s %s", time.Now().Format(time.RFC3339), "MEMORY_STATS_AFTER")
	runtime.ReadMemStats(&memStatsAfter)
	b.Logf("PHASE_END: %s %s", time.Now().Format(time.RFC3339), "MEMORY_STATS_AFTER")

	// Write memory stats to a file
	writeMemStats(outputDir, memStatsBefore, memStatsAfter)

	// Sleep to allow for OS-level memory measurements
	b.Log("\n=== WAITING FOR MEMORY RELEASE (30 seconds) ===")
	b.Log(fmt.Sprintf("Process ID: %d", os.Getpid()))
	b.Log(fmt.Sprintf("Output directory: %s", outputDir))

	// Wait longer to observe memory release patterns
	b.Logf("PHASE_START: %s %s", time.Now().Format(time.RFC3339), "MEMORY_RELEASE_WAIT")
	time.Sleep(30 * time.Second)
	b.Logf("PHASE_END: %s %s", time.Now().Format(time.RFC3339), "MEMORY_RELEASE_WAIT")

	b.Log("=== MEMORY MONITORING COMPLETE FOR THIS TEST ===")
}
