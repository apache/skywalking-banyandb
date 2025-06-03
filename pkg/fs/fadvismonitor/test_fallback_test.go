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

package fadvis

import (
	"fmt"
	"io"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/cilium/ebpf/rlimit"
	"github.com/apache/skywalking-banyandb/pkg/fs/fadvismonitor/monitor"
)

// TestFallbackMonitor tests the eBPF monitor with fallback support between tracepoints and kprobes.
func TestFallbackMonitor(t *testing.T) {
	t.Log("Testing eBPF monitor with fallback support...")

	// Remove memory limit restrictions for eBPF maps
	// Note: Skip this step if already running as root
	if err := rlimit.RemoveMemlock(); err != nil {
		t.Logf("Warning: Failed to remove memlock limit: %v", err)
		// Don't skip, continue with the test as we might be running with sufficient privileges
	}

	// Create monitor with fallback logic
	mon, err := monitor.NewMonitor()
	if err != nil {
		t.Fatalf("Failed to create monitor: %v", err)
	}
	defer mon.Close()

	t.Logf("Monitor created successfully (using kprobe: %v)", mon.IsUsingKprobe())

	// Create a test file
	testFile := "/tmp/test_fallback_file"
	file, err := os.Create(testFile)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}
	defer os.Remove(testFile)
	defer file.Close()

	// Write some data
	data := make([]byte, 4096)
	for i := range data {
		data[i] = byte(i % 256)
	}
	if _, err := file.Write(data); err != nil {
		t.Fatalf("Failed to write test data: %v", err)
	}
	if err := file.Sync(); err != nil {
		t.Fatalf("Failed to sync file: %v", err)
	}

	t.Log("Performing fadvise operations...")

	// Test different fadvise operations
	fd := int(file.Fd())
	
	// POSIX_FADV_SEQUENTIAL
	_, _, errno := syscall.Syscall6(syscall.SYS_FADVISE64, uintptr(fd), 0, 4096, 2, 0, 0)
	if errno != 0 {
		t.Logf("FADV_SEQUENTIAL failed: %v", errno)
	}

	// POSIX_FADV_DONTNEED
	_, _, errno = syscall.Syscall6(syscall.SYS_FADVISE64, uintptr(fd), 0, 4096, 4, 0, 0)
	if errno != 0 {
		t.Logf("FADV_DONTNEED failed: %v", errno)
	}

	// POSIX_FADV_WILLNEED
	_, _, errno = syscall.Syscall6(syscall.SYS_FADVISE64, uintptr(fd), 0, 4096, 3, 0, 0)
	if errno != 0 {
		t.Logf("FADV_WILLNEED failed: %v", errno)
	}

	// Wait a bit for eBPF to capture events
	time.Sleep(time.Second)

	// Read statistics
	t.Log("Reading fadvise statistics...")
	counts, err := mon.ReadCounts()
	if err != nil {
		t.Logf("Failed to read counts: %v", err)
	} else {
		t.Logf("Fadvise call counts: %+v", counts)
	}

	// Read cache statistics
	t.Log("Reading cache statistics...")
	cacheStats, err := mon.ReadCacheStats()
	if err != nil {
		t.Logf("Failed to read cache stats: %v", err)
	} else {
		t.Logf("Cache statistics: %+v", cacheStats)
	}

	// Read shrink statistics
	t.Log("Reading LRU shrink statistics...")
	shrinkStats, err := mon.ReadShrinkStats()
	if err != nil {
		t.Logf("Failed to read shrink stats: %v", err)
	} else {
		t.Logf("LRU shrink statistics: %+v", shrinkStats)
	}

	t.Log("Test completed successfully!")
}

// TestKernelCompatibility tests the kernel compatibility detection functions.
func TestKernelCompatibility(t *testing.T) {
	version, err := monitor.GetKernelVersion()
	if err != nil {
		t.Fatalf("Failed to get kernel version: %v", err)
	}
	t.Logf("Detected kernel version: %s", version.String())

	shouldUseCORE := monitor.ShouldUseCORE()
	t.Logf("Should use CO-RE: %v", shouldUseCORE)

	if !shouldUseCORE {
		t.Log("CO-RE not supported, kprobe fallback will be used")
	}
}

// TestCacheMissGeneration specifically tests cache miss generation with aggressive I/O
func TestCacheMissGeneration(t *testing.T) {
	if os.Getuid() != 0 {
		t.Skip("This test requires root privileges")
	}

	// Force kprobe mode to test the fallback
	os.Setenv("FORCE_KPROBE", "1")
	defer os.Unsetenv("FORCE_KPROBE")

	mon, err := monitor.NewMonitor()
	if err != nil {
		t.Fatalf("Failed to create monitor: %v", err)
	}
	defer mon.Close()

	t.Log("Testing aggressive cache miss generation...")

	// Create multiple large files to overwhelm page cache
	const numFiles = 5
	const fileSize = 10 * 1024 * 1024 // 10MB each
	var files []*os.File
	var filenames []string

	for i := 0; i < numFiles; i++ {
		filename := fmt.Sprintf("/tmp/cache_test_file_%d", i)
		filenames = append(filenames, filename)
		
		file, err := os.Create(filename)
		if err != nil {
			t.Fatalf("Failed to create test file %d: %v", i, err)
		}
		files = append(files, file)
		
		// Write large amount of data
		data := make([]byte, fileSize)
		for j := range data {
			data[j] = byte((i + j) % 256)
		}
		
		if _, err := file.Write(data); err != nil {
			t.Fatalf("Failed to write to file %d: %v", i, err)
		}
		file.Sync()
		
		// Immediately evict from cache to force future reads to be cache misses
		fd := int(file.Fd())
		syscall.Syscall6(syscall.SYS_FADVISE64, uintptr(fd), 0, uintptr(fileSize), 4, 0, 0) // FADV_DONTNEED
	}

	// Cleanup
	defer func() {
		for _, file := range files {
			file.Close()
		}
		for _, filename := range filenames {
			os.Remove(filename)
		}
	}()

	// Wait for cache eviction
	time.Sleep(500 * time.Millisecond)

	// Now read all files in chunks to trigger cache misses and page additions
	t.Log("Reading files to trigger cache misses...")
	buffer := make([]byte, 4096)
	
	for fileIdx, file := range files {
		file.Seek(0, 0)
		
		// Read the file in chunks
		for chunk := 0; chunk < 20; chunk++ { // Read 20 chunks of 4KB each
			n, err := file.Read(buffer)
			if err != nil && err != io.EOF {
				t.Logf("Error reading file %d chunk %d: %v", fileIdx, chunk, err)
				break
			}
			if n == 0 {
				break
			}
		}
		
		// Add some fadvise calls
		fd := int(file.Fd())
		syscall.Syscall6(syscall.SYS_FADVISE64, uintptr(fd), 0, 4096, 1, 0, 0) // FADV_RANDOM
		syscall.Syscall6(syscall.SYS_FADVISE64, uintptr(fd), 0, 4096, 3, 0, 0) // FADV_WILLNEED
	}

	// Wait for events to be processed
	time.Sleep(2 * time.Second)

	// Read all statistics
	counts, err := mon.ReadCounts()
	if err != nil {
		t.Logf("Failed to read fadvise counts: %v", err)
	} else {
		t.Logf("Fadvise counts after aggressive I/O: %+v", counts)
	}

	cacheStatsMap, err := mon.ReadCacheStats()
	if err != nil {
		t.Logf("Failed to read cache stats: %v", err)
	} else {
		t.Logf("Cache stats after aggressive I/O: %+v", cacheStatsMap)
		
		// Summarize all cache statistics
		var totalHits, totalMisses, totalReads, totalAdds uint64
		for pid, stats := range cacheStatsMap {
			t.Logf("PID %d cache stats: Hits=%d, Misses=%d, ReadCalls=%d, PageAdds=%d, HitRatio=%.2f%%",
				pid, stats.CacheHits, stats.CacheMisses, stats.ReadBatchCalls, stats.PageCacheAdds, stats.HitRatio*100)
			totalHits += stats.CacheHits
			totalMisses += stats.CacheMisses
			totalReads += stats.ReadBatchCalls
			totalAdds += stats.PageCacheAdds
		}
		
		if totalReads > 0 {
			overallHitRatio := float64(totalHits) / float64(totalReads) * 100
			t.Logf("Overall cache stats: Hits=%d, Misses=%d, Reads=%d, Adds=%d, HitRatio=%.2f%%",
				totalHits, totalMisses, totalReads, totalAdds, overallHitRatio)
		}
	}

	shrinkStats, err := mon.ReadShrinkStats()
	if err != nil {
		t.Logf("Failed to read shrink stats: %v", err)
	} else {
		t.Logf("LRU shrink stats after aggressive I/O: %+v", shrinkStats)
	}

	t.Log("Aggressive cache miss test completed!")
}

// TestForceKprobeMonitor tests the eBPF monitor by using environment variable to force kprobe usage.
func TestForceKprobeMonitor(t *testing.T) {
	t.Log("Testing eBPF monitor with forced kprobe usage...")

	// Remove memory limit restrictions for eBPF maps
	if err := rlimit.RemoveMemlock(); err != nil {
		t.Logf("Warning: Failed to remove memlock limit: %v", err)
	}

	// Set environment variable to force kprobe usage
	oldEnv := os.Getenv("FORCE_KPROBE")
	os.Setenv("FORCE_KPROBE", "1")
	defer func() {
		if oldEnv == "" {
			os.Unsetenv("FORCE_KPROBE")
		} else {
			os.Setenv("FORCE_KPROBE", oldEnv)
		}
	}()

	// Create monitor with forced kprobe usage
	mon, err := monitor.NewMonitor()
	if err != nil {
		t.Fatalf("Failed to create kprobe-only monitor: %v", err)
	}
	defer mon.Close()

	t.Logf("Monitor created successfully using kprobe fallback: %v", mon.IsUsingKprobe())

	// Generate some file activity to test monitoring
	tempFile, err := os.CreateTemp("", "test_kprobe_monitor_*.txt")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tempFile.Name())
	defer tempFile.Close()

	// Write some data to the file
	testData := make([]byte, 8192) // 8KB
	for i := range testData {
		testData[i] = byte(i % 256)
	}
	
	if _, err := tempFile.Write(testData); err != nil {
		t.Fatalf("Failed to write test data: %v", err)
	}
	
	// Sync to ensure data is written to disk
	if err := tempFile.Sync(); err != nil {
		t.Fatalf("Failed to sync file: %v", err)
	}

	// Test fadvise operations
	fd := int(tempFile.Fd())
	
	// POSIX_FADV_SEQUENTIAL (value = 2)
	if _, _, errno := syscall.Syscall6(syscall.SYS_FADVISE64, uintptr(fd), 0, 4096, 2, 0, 0); errno != 0 {
		t.Logf("fadvise SEQUENTIAL failed: %v", errno)
	}
	
	// POSIX_FADV_WILLNEED (value = 3)
	if _, _, errno := syscall.Syscall6(syscall.SYS_FADVISE64, uintptr(fd), 0, 4096, 3, 0, 0); errno != 0 {
		t.Logf("fadvise WILLNEED failed: %v", errno)
	}
	
	// POSIX_FADV_DONTNEED (value = 4)
	if _, _, errno := syscall.Syscall6(syscall.SYS_FADVISE64, uintptr(fd), 0, 4096, 4, 0, 0); errno != 0 {
		t.Logf("fadvise DONTNEED failed: %v", errno)
	}

	// Read the file to generate page cache activity
	tempFile.Seek(0, io.SeekStart)
	buffer := make([]byte, 4096)
	for i := 0; i < 2; i++ {
		tempFile.Seek(0, io.SeekStart)
		for {
			n, err := tempFile.Read(buffer)
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Logf("Read error: %v", err)
				break
			}
			if n == 0 {
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Wait a moment for eBPF events to be processed
	time.Sleep(2 * time.Second)

	// Get and display statistics
	fadviseCounts, err := mon.ReadCounts()
	if err != nil {
		t.Fatalf("Failed to get fadvise counts: %v", err)
	}

	cacheStats, err := mon.ReadCacheStats()
	if err != nil {
		t.Logf("Warning: Failed to get cache stats: %v", err)
		cacheStats = make(map[uint32]monitor.CacheStats)
	}

	t.Logf("=== Kprobe Monitor Test Results ===")
	t.Logf("Using kprobe fallback: %v", mon.IsUsingKprobe())
	
	totalFadviseCalls := uint64(0)
	for _, count := range fadviseCounts {
		totalFadviseCalls += count
	}
	t.Logf("Total fadvise calls: %d", totalFadviseCalls)
	
	totalCacheHits := uint64(0)
	totalCacheMisses := uint64(0)
	totalReadCalls := uint64(0)
	totalCacheAdds := uint64(0)
	for _, stats := range cacheStats {
		totalCacheHits += stats.CacheHits
		totalCacheMisses += stats.CacheMisses
		totalReadCalls += stats.ReadBatchCalls
		totalCacheAdds += stats.PageCacheAdds
	}
	
	t.Logf("Total page cache hits: %d", totalCacheHits)
	t.Logf("Total page cache misses: %d", totalCacheMisses)
	t.Logf("Total cache reads: %d", totalReadCalls)
	t.Logf("Total cache adds: %d", totalCacheAdds)

	if !mon.IsUsingKprobe() {
		t.Errorf("Expected monitor to use kprobe, but it's using tracepoint")
	}

	if totalFadviseCalls == 0 {
		t.Logf("Warning: No fadvise calls detected. This might be expected if kprobe attachment failed.")
	} else {
		t.Logf("Successfully detected %d fadvise calls using kprobe", totalFadviseCalls)
	}
}

// TestDirectKprobeMonitor tests the eBPF monitor by directly using kprobe-only mode,
// completely bypassing tracepoint logic.
func TestDirectKprobeMonitor(t *testing.T) {
	t.Log("Testing eBPF monitor with direct kprobe-only mode...")

	// Remove memory limit restrictions for eBPF maps
	if err := rlimit.RemoveMemlock(); err != nil {
		t.Logf("Warning: Failed to remove memlock limit: %v", err)
	}

	// Create monitor using direct kprobe-only method
	mon, err := monitor.NewKprobeOnlyMonitor()
	if err != nil {
		t.Fatalf("Failed to create direct kprobe monitor: %v", err)
	}
	defer mon.Close()

	t.Logf("Monitor created successfully using direct kprobe mode: %v", mon.IsUsingKprobe())

	// Generate some file activity to test monitoring
	tempFile, err := os.CreateTemp("", "test_direct_kprobe_*.txt")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tempFile.Name())
	defer tempFile.Close()

	// Write some data to the file
	testData := make([]byte, 16384) // 16KB
	for i := range testData {
		testData[i] = byte(i % 256)
	}
	
	if _, err := tempFile.Write(testData); err != nil {
		t.Fatalf("Failed to write test data: %v", err)
	}
	
	// Sync to ensure data is written to disk
	if err := tempFile.Sync(); err != nil {
		t.Fatalf("Failed to sync file: %v", err)
	}

	// Test multiple fadvise operations
	fd := int(tempFile.Fd())
	
	// Multiple calls to different fadvise operations
	adviceTypes := []uintptr{2, 3, 4} // SEQUENTIAL, WILLNEED, DONTNEED
	adviceNames := []string{"SEQUENTIAL", "WILLNEED", "DONTNEED"}
	
	for i, advice := range adviceTypes {
		if _, _, errno := syscall.Syscall6(syscall.SYS_FADVISE64, uintptr(fd), 0, 8192, advice, 0, 0); errno != 0 {
			t.Logf("fadvise %s failed: %v", adviceNames[i], errno)
		} else {
			t.Logf("fadvise %s succeeded", adviceNames[i])
		}
		time.Sleep(50 * time.Millisecond) // Small delay between calls
	}

	// Read the file multiple times to generate page cache activity
	buffer := make([]byte, 4096)
	for round := 0; round < 3; round++ {
		tempFile.Seek(0, io.SeekStart)
		totalRead := 0
		for {
			n, err := tempFile.Read(buffer)
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Logf("Read error: %v", err)
				break
			}
			if n == 0 {
				break
			}
			totalRead += n
		}
		t.Logf("Read round %d: %d bytes", round+1, totalRead)
		time.Sleep(200 * time.Millisecond)
	}

	// Wait for eBPF events to be processed
	time.Sleep(3 * time.Second)

	// Get and display detailed statistics
	fadviseCounts, err := mon.ReadCounts()
	if err != nil {
		t.Fatalf("Failed to get fadvise counts: %v", err)
	}

	cacheStats, err := mon.ReadCacheStats()
	if err != nil {
		t.Logf("Warning: Failed to get cache stats: %v", err)
		cacheStats = make(map[uint32]monitor.CacheStats)
	}

	t.Logf("=== Direct Kprobe Monitor Test Results ===")
	t.Logf("Using kprobe fallback: %v", mon.IsUsingKprobe())
	
	if !mon.IsUsingKprobe() {
		t.Errorf("Expected monitor to use kprobe, but it's not using kprobe")
	}
	
	// Display per-process fadvise statistics
	t.Logf("Per-process fadvise counts:")
	totalFadviseCalls := uint64(0)
	for pid, count := range fadviseCounts {
		t.Logf("  PID %d: %d calls", pid, count)
		totalFadviseCalls += count
	}
	t.Logf("Total fadvise calls: %d", totalFadviseCalls)
	
	// Display cache statistics
	totalCacheHits := uint64(0)
	totalCacheMisses := uint64(0)
	totalReadCalls := uint64(0)
	totalCacheAdds := uint64(0)
	
	t.Logf("Per-process cache statistics:")
	for pid, stats := range cacheStats {
		t.Logf("  PID %d: hits=%d, misses=%d, reads=%d, adds=%d, ratio=%.2f%%", 
			pid, stats.CacheHits, stats.CacheMisses, stats.ReadBatchCalls, 
			stats.PageCacheAdds, stats.HitRatio*100)
		totalCacheHits += stats.CacheHits
		totalCacheMisses += stats.CacheMisses
		totalReadCalls += stats.ReadBatchCalls
		totalCacheAdds += stats.PageCacheAdds
	}
	
	t.Logf("Total cache statistics:")
	t.Logf("  Cache hits: %d", totalCacheHits)
	t.Logf("  Cache misses: %d", totalCacheMisses)
	t.Logf("  Read calls: %d", totalReadCalls)
	t.Logf("  Cache adds: %d", totalCacheAdds)
	
	if totalReadCalls > 0 {
		overallHitRatio := float64(totalCacheHits) / float64(totalReadCalls) * 100
		t.Logf("  Overall hit ratio: %.2f%%", overallHitRatio)
	}

	// Verify that we're actually using kprobe
	if !mon.IsUsingKprobe() {
		t.Errorf("FAILED: Expected direct kprobe monitor to use kprobe, but it's not")
	} else {
		t.Logf("SUCCESS: Monitor is correctly using kprobe mode")
	}

	if totalFadviseCalls == 0 {
		t.Logf("WARNING: No fadvise calls detected. This suggests kprobe attachment may have failed.")
		t.Logf("This could be due to:")
		t.Logf("  - Insufficient privileges (try running as root)")
		t.Logf("  - Missing kernel symbols")
		t.Logf("  - Kernel version compatibility issues")
	} else {
		t.Logf("SUCCESS: Detected %d fadvise calls using direct kprobe mode", totalFadviseCalls)
	}
}
