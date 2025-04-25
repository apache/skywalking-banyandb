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
	"runtime/pprof"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// BenchmarkMergeMemoryUsage tests the memory usage of merge operations with and without fadvis.
// This benchmark focuses specifically on memory usage and page cache behavior.
func BenchmarkMergeMemoryUsage(b *testing.B) {
	if runtime.GOOS != "linux" {
		b.Skip("fadvise is only supported on Linux")
	}

	// Create a temporary directory for the test
	testDir, err := os.MkdirTemp("", "fadvis_memory_benchmark")
	require.NoError(b, err)
	defer os.RemoveAll(testDir)

	// Prepare test files - use larger files to better observe memory effects
	parts := createTestParts(b, testDir, 5, LargeFileSize*2) // 400MB parts

	// Create output directories
	outputDirNoFadvise := filepath.Join(testDir, "no_fadvise")
	outputDirWithFadvise := filepath.Join(testDir, "with_fadvise")
	require.NoError(b, os.MkdirAll(outputDirNoFadvise, 0755))
	require.NoError(b, os.MkdirAll(outputDirWithFadvise, 0755))

	// Run benchmark with fadvise disabled
	b.Run("WithoutFadvise", func(b *testing.B) {
		// Set the fadvis threshold to a high value to disable it
		setTestThreshold(terabyte)
		runMemoryProfiledMerge(b, parts, outputDirNoFadvise)
	})

	// Run benchmark with fadvise enabled
	b.Run("WithFadvise", func(b *testing.B) {
		// Set a realistic fadvis threshold based on system memory
		setRealisticThreshold()
		runMemoryProfiledMerge(b, parts, outputDirWithFadvise)
	})
}

// runMemoryProfiledMerge performs merge operations with memory profiling
func runMemoryProfiledMerge(b *testing.B, parts []string, outputDir string) {
	// Reset memory stats before starting
	runtime.GC()

	// Record initial memory stats
	var memStatsBefore, memStatsAfter runtime.MemStats
	runtime.ReadMemStats(&memStatsBefore)

	// Create heap profile before operation
	heapFileBefore := filepath.Join(outputDir, "heap_before.prof")
	f, err := os.Create(heapFileBefore)
	require.NoError(b, err)
	err = pprof.WriteHeapProfile(f)
	require.NoError(b, err)
	f.Close()

	// Perform the merge operations
	for i := 0; i < b.N; i++ {
		outputFile := filepath.Join(outputDir, fmt.Sprintf("merged_%d", i))
		err := simulateMergeOperation(b, parts, outputFile)
		require.NoError(b, err)
	}

	// Force garbage collection to get accurate memory usage
	runtime.GC()

	// Create heap profile after operation
	heapFileAfter := filepath.Join(outputDir, "heap_after.prof")
	f, err = os.Create(heapFileAfter)
	require.NoError(b, err)
	err = pprof.WriteHeapProfile(f)
	require.NoError(b, err)
	f.Close()

	// Record final memory stats
	runtime.ReadMemStats(&memStatsAfter)

	// Write memory stats to a file
	writeMemStats(outputDir, memStatsBefore, memStatsAfter)

	// Sleep to allow for OS-level memory measurements
	b.Log("Merge operation completed. Sleeping to allow for OS-level measurements...")
	b.Log(fmt.Sprintf("Process ID: %d", os.Getpid()))
	b.Log(fmt.Sprintf("Output directory: %s", outputDir))
	time.Sleep(10 * time.Second) // Sleep to allow for external measurements
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
