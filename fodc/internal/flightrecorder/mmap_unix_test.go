//go:build !windows
// +build !windows

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
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied.  See the License for the specific
// language governing permissions and limitations under the License.

package flightrecorder

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/apache/skywalking-banyandb/fodc/internal/metric"
	"github.com/apache/skywalking-banyandb/fodc/internal/poller"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestE2E_Mmap_Unix_BasicFunctionality tests basic mmap functionality on Unix
func TestE2E_Mmap_Unix_BasicFunctionality(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mmap_test.bin")
	
	// Create flight recorder which uses mmap internally
	fr, err := NewFlightRecorder(path, 10)
	require.NoError(t, err)
	defer fr.Close()
	
	// Record a snapshot - this exercises mmap write
	snapshot := poller.MetricsSnapshot{
		Timestamp: time.Now(),
		RawMetrics: []metric.RawMetric{
			{Name: "test_metric", Value: 42.0},
		},
		Histograms: make(map[string]metric.Histogram),
		Errors:     []string{},
	}
	
	err = fr.Record(snapshot)
	require.NoError(t, err)
	
	// Read back - this exercises mmap read
	snapshots, err := fr.ReadAll()
	require.NoError(t, err)
	assert.Equal(t, 1, len(snapshots))
	assert.Equal(t, 42.0, snapshots[0].RawMetrics[0].Value)
}

// TestE2E_Mmap_Unix_Persistence tests that mmap persists data across close/reopen
func TestE2E_Mmap_Unix_Persistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mmap_persistence_test.bin")
	
	// Create and write data
	fr1, err := NewFlightRecorder(path, 10)
	require.NoError(t, err)
	
	snapshot := poller.MetricsSnapshot{
		Timestamp: time.Now(),
		RawMetrics: []metric.RawMetric{
			{Name: "persistent_metric", Value: 100.0},
		},
		Histograms: make(map[string]metric.Histogram),
		Errors:     []string{},
	}
	
	err = fr1.Record(snapshot)
	require.NoError(t, err)
	
	// Close - should sync data via msync
	err = fr1.Close()
	require.NoError(t, err)
	
	// Reopen - should recover data from mmap
	fr2, err := NewFlightRecorder(path, 10)
	require.NoError(t, err)
	defer fr2.Close()
	
	snapshots, err := fr2.ReadAll()
	require.NoError(t, err)
	assert.Equal(t, 1, len(snapshots))
	assert.Equal(t, 100.0, snapshots[0].RawMetrics[0].Value)
}

// TestE2E_Mmap_Unix_MultipleWrites tests multiple writes with msync
func TestE2E_Mmap_Unix_MultipleWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mmap_multiple_test.bin")
	fr, err := NewFlightRecorder(path, 10)
	require.NoError(t, err)
	defer fr.Close()
	
	// Write multiple snapshots - each Record() calls msync
	for i := 0; i < 5; i++ {
		snapshot := poller.MetricsSnapshot{
			Timestamp: time.Now().Add(time.Duration(i) * time.Second),
			RawMetrics: []metric.RawMetric{
				{Name: "metric", Value: float64(i)},
			},
			Histograms: make(map[string]metric.Histogram),
			Errors:     []string{},
		}
		err := fr.Record(snapshot)
		require.NoError(t, err)
	}
	
	// Verify all were persisted
	snapshots, err := fr.ReadAll()
	require.NoError(t, err)
	assert.Equal(t, 5, len(snapshots))
}

// TestE2E_Mmap_Unix_PageAlignment tests msync with various data sizes (page alignment)
func TestE2E_Mmap_Unix_PageAlignment(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mmap_alignment_test.bin")
	fr, err := NewFlightRecorder(path, 10)
	require.NoError(t, err)
	defer fr.Close()
	
	// Test with different snapshot sizes to exercise page alignment in msync
	testCases := []struct {
		name  string
		value float64
	}{
		{"small", 1.0},
		{"medium", 100.0},
		{"large", 1000.0},
	}
	
	for _, tc := range testCases {
		snapshot := poller.MetricsSnapshot{
			Timestamp: time.Now(),
			RawMetrics: []metric.RawMetric{
				{Name: tc.name, Value: tc.value},
			},
			Histograms: make(map[string]metric.Histogram),
			Errors:     []string{},
		}
		err := fr.Record(snapshot)
		require.NoError(t, err, "Failed to record %s snapshot", tc.name)
	}
	
	// Verify all were synced correctly
	snapshots, err := fr.ReadAll()
	require.NoError(t, err)
	assert.Equal(t, len(testCases), len(snapshots))
}

// TestE2E_Mmap_Unix_ConcurrentAccess tests concurrent mmap access
func TestE2E_Mmap_Unix_ConcurrentAccess(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mmap_concurrent_test.bin")
	fr, err := NewFlightRecorder(path, 100)
	require.NoError(t, err)
	defer fr.Close()
	
	// Concurrent writes - each uses mmap
	var wg sync.WaitGroup
	numGoroutines := 10
	snapshotsPerGoroutine := 10
	
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < snapshotsPerGoroutine; j++ {
				snapshot := poller.MetricsSnapshot{
					Timestamp: time.Now(),
					RawMetrics: []metric.RawMetric{
						{Name: "concurrent_metric", Value: float64(id*100 + j)},
					},
					Histograms: make(map[string]metric.Histogram),
					Errors:     []string{},
				}
				_ = fr.Record(snapshot)
			}
		}(i)
	}
	
	wg.Wait()
	
	// Verify all were written
	snapshots, err := fr.ReadAll()
	require.NoError(t, err)
	assert.Equal(t, numGoroutines*snapshotsPerGoroutine, len(snapshots))
}

// TestE2E_Mmap_Unix_GetPageSize tests getPageSize function
func TestE2E_Mmap_Unix_GetPageSize(t *testing.T) {
	// getPageSize should return a valid page size
	pageSize := getPageSize()
	assert.Greater(t, pageSize, 0)
	
	// Should be consistent across calls (uses sync.Once)
	pageSize2 := getPageSize()
	assert.Equal(t, pageSize, pageSize2)
	
	// Should be a reasonable value (typically 4KB, 8KB, or 16KB)
	assert.GreaterOrEqual(t, pageSize, 4096)   // At least 4KB
	assert.LessOrEqual(t, pageSize, 65536)     // At most 64KB
}

// TestE2E_Mmap_Unix_EmptyDataSync tests msync with empty data
func TestE2E_Mmap_Unix_EmptyDataSync(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mmap_empty_test.bin")
	fr, err := NewFlightRecorder(path, 10)
	require.NoError(t, err)
	defer fr.Close()
	
	// Record empty snapshot
	emptySnapshot := poller.MetricsSnapshot{
		Timestamp:  time.Now(),
		RawMetrics: []metric.RawMetric{},
		Histograms: make(map[string]metric.Histogram),
		Errors:     []string{},
	}
	
	err = fr.Record(emptySnapshot)
	require.NoError(t, err)
	
	// Should handle empty data correctly
	snapshots, err := fr.ReadAll()
	require.NoError(t, err)
	assert.Equal(t, 1, len(snapshots))
}

// TestE2E_Mmap_Unix_LargeFile tests mmap with large files
func TestE2E_Mmap_Unix_LargeFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mmap_large_test.bin")
	// Use larger buffer size to create larger mmap
	fr, err := NewFlightRecorder(path, 100)
	require.NoError(t, err)
	defer fr.Close()
	
	// Record many snapshots to fill large buffer
	for i := 0; i < 50; i++ {
		snapshot := poller.MetricsSnapshot{
			Timestamp: time.Now().Add(time.Duration(i) * time.Second),
			RawMetrics: []metric.RawMetric{
				{Name: "large_metric", Value: float64(i)},
			},
			Histograms: make(map[string]metric.Histogram),
			Errors:     []string{},
		}
		err := fr.Record(snapshot)
		require.NoError(t, err)
	}
	
	// Verify large file mmap works
	snapshots, err := fr.ReadAll()
	require.NoError(t, err)
	assert.Equal(t, 50, len(snapshots))
}

// TestE2E_Mmap_Unix_FileResize tests mmap with file resize
func TestE2E_Mmap_Unix_FileResize(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mmap_resize_test.bin")
	
	// Create with small buffer
	fr1, err := NewFlightRecorder(path, 5)
	require.NoError(t, err)
	
	snapshot := poller.MetricsSnapshot{
		Timestamp: time.Now(),
		RawMetrics: []metric.RawMetric{
			{Name: "resize_test", Value: 1.0},
		},
		Histograms: make(map[string]metric.Histogram),
		Errors:     []string{},
	}
	
	err = fr1.Record(snapshot)
	require.NoError(t, err)
	err = fr1.Close()
	require.NoError(t, err)
	
	// Reopen with same size - should work
	fr2, err := NewFlightRecorder(path, 5)
	require.NoError(t, err)
	defer fr2.Close()
	
	snapshots, err := fr2.ReadAll()
	require.NoError(t, err)
	assert.Equal(t, 1, len(snapshots))
}

// TestE2E_Mmap_Unix_Munmap tests munmap functionality
func TestE2E_Mmap_Unix_Munmap(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mmap_munmap_test.bin")
	
	// Create recorder (mmaps file)
	fr, err := NewFlightRecorder(path, 10)
	require.NoError(t, err)
	
	// Record something
	snapshot := poller.MetricsSnapshot{
		Timestamp: time.Now(),
		RawMetrics: []metric.RawMetric{
			{Name: "munmap_test", Value: 42.0},
		},
		Histograms: make(map[string]metric.Histogram),
		Errors:     []string{},
	}
	err = fr.Record(snapshot)
	require.NoError(t, err)
	
	// Close should munmap
	err = fr.Close()
	require.NoError(t, err)
	
	// Verify file still exists and has data
	info, err := os.Stat(path)
	require.NoError(t, err)
	assert.Greater(t, info.Size(), int64(0))
	
	// Reopen should work (new mmap)
	fr2, err := NewFlightRecorder(path, 10)
	require.NoError(t, err)
	defer fr2.Close()
	
	snapshots, err := fr2.ReadAll()
	require.NoError(t, err)
	assert.Equal(t, 1, len(snapshots))
}

// TestE2E_Mmap_Unix_MultipleSyncs tests multiple msync calls
func TestE2E_Mmap_Unix_MultipleSyncs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mmap_multisync_test.bin")
	fr, err := NewFlightRecorder(path, 10)
	require.NoError(t, err)
	defer fr.Close()
	
	// Each Record() calls msync internally
	for i := 0; i < 10; i++ {
		snapshot := poller.MetricsSnapshot{
			Timestamp: time.Now().Add(time.Duration(i) * time.Second),
			RawMetrics: []metric.RawMetric{
				{Name: "multisync_metric", Value: float64(i)},
			},
			Histograms: make(map[string]metric.Histogram),
			Errors:     []string{},
		}
		err := fr.Record(snapshot)
		require.NoError(t, err)
	}
	
	// All should be persisted
	snapshots, err := fr.ReadAll()
	require.NoError(t, err)
	assert.Equal(t, 10, len(snapshots))
}

// TestE2E_Mmap_Unix_RealWorldScenario tests realistic usage scenario
func TestE2E_Mmap_Unix_RealWorldScenario(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mmap_realworld_test.bin")
	
	// Simulate real-world usage: create, write, close, reopen, read
	fr1, err := NewFlightRecorder(path, 100)
	require.NoError(t, err)
	
	// Write realistic snapshots
	for i := 0; i < 20; i++ {
		snapshot := poller.MetricsSnapshot{
			Timestamp: time.Now().Add(time.Duration(i) * time.Second),
			RawMetrics: []metric.RawMetric{
				{Name: "http_requests_total", Labels: []metric.Label{{Name: "method", Value: "GET"}}, Value: float64(100 + i)},
				{Name: "memory_usage_bytes", Value: float64(1024 * 1024 * i)},
			},
			Histograms: map[string]metric.Histogram{
				"http_request_duration": {
					Name: "http_request_duration",
					Bins: []metric.Bin{
						{Value: 0.1, Count: uint64(10 + i)},
					},
				},
			},
			Errors: []string{},
		}
		err := fr1.Record(snapshot)
		require.NoError(t, err)
	}
	
	err = fr1.Close()
	require.NoError(t, err)
	
	// Reopen and verify persistence
	fr2, err := NewFlightRecorder(path, 100)
	require.NoError(t, err)
	defer fr2.Close()
	
	snapshots, err := fr2.ReadAll()
	require.NoError(t, err)
	assert.Equal(t, 20, len(snapshots))
	
	// Verify data integrity
	for i, snapshot := range snapshots {
		assert.Equal(t, float64(100+i), snapshot.RawMetrics[0].Value)
		assert.Equal(t, 2, len(snapshot.RawMetrics))
		assert.Equal(t, 1, len(snapshot.Histograms))
	}
}

