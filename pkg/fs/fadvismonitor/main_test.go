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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//go:build linux
// +build linux

// Package fadvismonitor provides stress testing functionality for the file advice (fadvis) system
// which optimizes memory page cache usage for file operations.
package fadvismonitor

import (
	"fmt"
	"os"
	"regexp"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/apache/skywalking-banyandb/pkg/fs/fadvismonitor/monitor"
	"github.com/apache/skywalking-banyandb/pkg/fs/fadvismonitor/utils"
)

// TestMain is the entry point for the test package, used to initialize the test environment.
// It runs before all tests and benchmarks are executed.
func TestMain(m *testing.M) {
	// Perform initialization before tests start
	fmt.Println("Initializing test environment...")

	// Precompile all regular expressions
	_ = regexp.MustCompile(`Rss:\s+(\d+)\s+kB`)
	_ = regexp.MustCompile(`Pss:\s+(\d+)\s+kB`)
	_ = regexp.MustCompile(`Shared_Clean:\s+(\d+)\s+kB`)

	// Warm up the memory manager
	utils.SetRealisticThreshold()

	// Force a garbage collection
	runtime.GC()

	// Wait for a while to ensure system stability
	// time.Sleep(100 * time.Millisecond)

	// Run all tests and benchmarks
	code := m.Run()

	// Return test result
	os.Exit(code)
}

// TestFallbackMonitor tests the eBPF monitor with fallback support between tracepoints and kprobes.
func TestFallbackMonitor(t *testing.T) {
	t.Log("Testing eBPF monitor with fallback support...")

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

	// Trigger some file I/O to generate cache events
	t.Log("Triggering file I/O for cache monitoring...")

	// Read the file to trigger cache activity
	if _, err := file.Seek(0, 0); err != nil {
		t.Fatalf("Failed to seek file: %v", err)
	}
	buffer := make([]byte, 4096)
	if _, err := file.Read(buffer); err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}

	time.Sleep(time.Second)

	// Read cache stats again
	cacheStats, err = mon.ReadCacheStats()
	if err != nil {
		t.Logf("Failed to read cache stats after I/O: %v", err)
	} else {
		t.Logf("Cache statistics after I/O: %+v", cacheStats)
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

// TestForceKprobeMode tests forcing kprobe mode via environment variable.
func TestForceKprobeMode(t *testing.T) {
	// Set environment variable to force kprobe
	t.Setenv("FORCE_KPROBE", "1")

	t.Log("Testing forced kprobe mode...")

	// Create monitor which should use kprobe due to env var
	mon, err := monitor.NewMonitor()
	if err != nil {
		t.Fatalf("Failed to create monitor: %v", err)
	}
	defer mon.Close()

	// Verify we're using kprobe
	if !mon.IsUsingKprobe() {
		t.Logf("Warning: Expected to use kprobe due to FORCE_KPROBE=1, but using tracepoint")
	} else {
		t.Logf("Successfully forced kprobe mode")
	}

	// Test basic functionality
	testFile := "/tmp/test_force_kprobe_file"
	file, err := os.Create(testFile)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}
	defer os.Remove(testFile)
	defer file.Close()

	// Perform a simple fadvise operation
	fd := int(file.Fd())
	_, _, errno := syscall.Syscall6(syscall.SYS_FADVISE64, uintptr(fd), 0, 4096, 4, 0, 0) // FADV_DONTNEED
	if errno != 0 {
		t.Logf("FADV_DONTNEED failed: %v", errno)
	}

	time.Sleep(time.Second)

	// Check if we captured any events
	counts, err := mon.ReadCounts()
	if err != nil {
		t.Logf("Failed to read counts: %v", err)
	} else {
		t.Logf("Kprobe mode fadvise counts: %+v", counts)
	}
}

// TestTracepointMode tests the eBPF monitor with tracepoint mode.
func TestTracepointMode(t *testing.T) {
	t.Log("Testing eBPF monitor with tracepoint mode...")

	utils.WithTestMonitoringMode(t, monitor.ModeTracepoint, func(t *testing.T) {
		performFadviseOperations(t)
	})

	t.Log("Tracepoint mode test completed successfully!")
}

// TestKprobeMode tests the eBPF monitor with kprobe mode.
func TestKprobeMode(t *testing.T) {
	t.Log("Testing eBPF monitor with kprobe mode...")

	utils.WithTestMonitoringMode(t, monitor.ModeKprobe, func(t *testing.T) {
		performFadviseOperations(t)
	})

	t.Log("Kprobe mode test completed successfully!")
}

// TestAutoMode tests the eBPF monitor with auto mode (fallback logic).
func TestAutoMode(t *testing.T) {
	t.Log("Testing eBPF monitor with auto mode...")

	utils.WithTestMonitoringMode(t, monitor.ModeAuto, func(t *testing.T) {
		performFadviseOperations(t)
	})

	t.Log("Auto mode test completed successfully!")
}

// performFadviseOperations performs common fadvise operations for testing.
func performFadviseOperations(t *testing.T) {
	// Create a test file
	testFile := "/tmp/test_monitor_file"
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

	// Trigger some file I/O to generate cache events
	t.Log("Triggering file I/O for cache monitoring...")

	// Read the file to trigger cache activity
	if _, err := file.Seek(0, 0); err != nil {
		t.Fatalf("Failed to seek file: %v", err)
	}
	buffer := make([]byte, 4096)
	if _, err := file.Read(buffer); err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}

	time.Sleep(time.Second)
}
