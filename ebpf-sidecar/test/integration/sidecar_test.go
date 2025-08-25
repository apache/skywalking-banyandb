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

//go:build integration && linux
// +build integration,linux

package integration_test

import (
	"context"
	"io"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/apache/skywalking-banyandb/ebpf-sidecar/internal/collector"
	"github.com/apache/skywalking-banyandb/ebpf-sidecar/internal/config"
)

func TestIOMonitorIntegration(t *testing.T) {
	// Skip if not root
	if os.Getuid() != 0 {
		t.Skip("Test requires root privileges for eBPF")
	}

	logger := zaptest.NewLogger(t)
	
	// Create and start the I/O monitor module
	module, err := collector.NewIOMonitorModule(logger)
	require.NoError(t, err, "Failed to create module")
	defer module.Stop()

	err = module.Start()
	require.NoError(t, err, "Failed to start module")

	// Let eBPF programs initialize
	time.Sleep(2 * time.Second)

	// Test 1: Verify fadvise monitoring
	t.Run("fadvise_monitoring", func(t *testing.T) {
		// Create test file
		testFile := "/tmp/ebpf_test_fadvise.dat"
		f, err := os.Create(testFile)
		require.NoError(t, err)
		defer os.Remove(testFile)
		defer f.Close()

		// Write some data
		data := make([]byte, 4096)
		_, err = f.Write(data)
		require.NoError(t, err)

		// Trigger fadvise calls
		fd := int(f.Fd())
		
		// Sequential access pattern
		err = syscall.Fadvise(fd, 0, 4096, syscall.FADV_SEQUENTIAL)
		assert.NoError(t, err, "FADV_SEQUENTIAL failed")
		
		// Will need these pages
		err = syscall.Fadvise(fd, 0, 4096, syscall.FADV_WILLNEED)
		assert.NoError(t, err, "FADV_WILLNEED failed")
		
		// Don't need these pages
		err = syscall.Fadvise(fd, 0, 4096, syscall.FADV_DONTNEED)
		assert.NoError(t, err, "FADV_DONTNEED failed")

		// Wait for eBPF to process
		time.Sleep(1 * time.Second)

		// Collect metrics
		metricSet, err := module.Collect()
		require.NoError(t, err, "Failed to collect metrics")

		// Verify we captured fadvise calls
		foundFadvise := false
		for _, metric := range metricSet.GetMetrics() {
			if metric.Name == "ebpf_fadvise_calls_total" {
				assert.Greater(t, metric.Value, float64(0), "Should have captured fadvise calls")
				foundFadvise = true
				t.Logf("Captured %v fadvise calls", metric.Value)
			}
		}
		assert.True(t, foundFadvise, "Should have fadvise metrics")
	})

	// Test 2: Verify cache miss monitoring
	t.Run("cache_miss_monitoring", func(t *testing.T) {
		// Create larger test file for cache testing
		testFile := "/tmp/ebpf_test_cache.dat"
		f, err := os.Create(testFile)
		require.NoError(t, err)
		defer os.Remove(testFile)
		defer f.Close()

		// Write 10MB of data
		data := make([]byte, 1024*1024) // 1MB
		for i := 0; i < 10; i++ {
			_, err = f.Write(data)
			require.NoError(t, err)
		}
		f.Sync()

		// Drop page cache to force cache misses
		// Note: This affects the whole system, use carefully
		dropCaches := "/proc/sys/vm/drop_caches"
		if err := os.WriteFile(dropCaches, []byte("1"), 0644); err == nil {
			// Read file back to trigger cache misses
			f.Seek(0, 0)
			io.Copy(io.Discard, f)
		}

		// Wait for eBPF to process
		time.Sleep(1 * time.Second)

		// Collect metrics
		metricSet, err := module.Collect()
		require.NoError(t, err, "Failed to collect metrics")

		// Check for cache metrics
		foundCache := false
		for _, metric := range metricSet.GetMetrics() {
			if metric.Name == "ebpf_cache_read_attempts_total" || 
			   metric.Name == "ebpf_cache_misses_total" {
				foundCache = true
				t.Logf("Cache metric %s: %v", metric.Name, metric.Value)
			}
			if metric.Name == "ebpf_cache_miss_rate_percent" && metric.Value > 0 {
				t.Logf("Cache miss rate: %.2f%%", metric.Value)
			}
		}
		assert.True(t, foundCache, "Should have cache metrics")
	})

	// Test 3: Verify cleanup strategy
	t.Run("cleanup_strategy", func(t *testing.T) {
		// Set clear-after-read strategy
		module.SetCleanupStrategy(collector.ClearAfterRead, 1*time.Second)

		// Generate some activity
		testFile := "/tmp/ebpf_test_cleanup.dat"
		f, err := os.Create(testFile)
		require.NoError(t, err)
		defer os.Remove(testFile)
		
		fd := int(f.Fd())
		syscall.Fadvise(fd, 0, 4096, syscall.FADV_SEQUENTIAL)
		f.Close()

		// First collection
		ms1, err := module.Collect()
		require.NoError(t, err)
		
		var firstCount float64
		for _, m := range ms1.GetMetrics() {
			if m.Name == "ebpf_fadvise_calls_total" {
				firstCount = m.Value
				break
			}
		}

		// Second collection should have cleared maps
		ms2, err := module.Collect()
		require.NoError(t, err)
		
		var secondCount float64
		for _, m := range ms2.GetMetrics() {
			if m.Name == "ebpf_fadvise_calls_total" {
				secondCount = m.Value
				break
			}
		}

		// With clear-after-read, second collection should be 0 or very small
		t.Logf("First collection: %v, Second collection: %v", firstCount, secondCount)
		assert.LessOrEqual(t, secondCount, float64(1), "Maps should be mostly cleared after read")
	})
}

// TestCollectorIntegration tests the full collector with real eBPF
func TestCollectorIntegration(t *testing.T) {
	if os.Getuid() != 0 {
		t.Skip("Test requires root privileges for eBPF")
	}

	logger := zaptest.NewLogger(t)
	
	// Create collector config
	cfg := config.CollectorConfig{
		Interval: 1 * time.Second,
		Modules:  []string{"iomonitor"},
	}

	// Create and start collector
	coll, err := collector.New(cfg, logger)
	require.NoError(t, err, "Failed to create collector")
	defer coll.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = coll.Start(ctx)
	require.NoError(t, err, "Failed to start collector")

	// Generate test activity
	testFile := "/tmp/ebpf_collector_test.dat"
	f, err := os.Create(testFile)
	require.NoError(t, err)
	defer os.Remove(testFile)
	
	// Trigger fadvise
	fd := int(f.Fd())
	for i := 0; i < 5; i++ {
		syscall.Fadvise(fd, 0, 4096, syscall.FADV_SEQUENTIAL)
		time.Sleep(100 * time.Millisecond)
	}
	f.Close()

	// Wait for collection cycle
	time.Sleep(2 * time.Second)

	// Get metrics from collector
	store := coll.GetMetrics()
	require.NotNil(t, store)

	allMetrics := store.GetAll()
	assert.NotEmpty(t, allMetrics, "Should have collected metrics")

	// Verify iomonitor metrics exist
	ioMetrics, ok := allMetrics["iomonitor"]
	assert.True(t, ok, "Should have iomonitor metrics")
	assert.Greater(t, ioMetrics.Count(), 0, "Should have actual metrics")

	t.Logf("Collected %d metrics from iomonitor", ioMetrics.Count())
}