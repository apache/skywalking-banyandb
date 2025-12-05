//go:build windows
// +build windows

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

// TestE2E_Mmap_Windows_BasicFunctionality tests basic mmap functionality on Windows.
func TestE2E_Mmap_Windows_BasicFunctionality(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mmap_test.bin")
	
	// Create flight recorder which uses file-backed approach on Windows
	fr, err := NewFlightRecorder(path, 10)
	require.NoError(t, err)
	defer fr.Close()
	
	// Record a snapshot - this exercises Windows file-backed write
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
	
	// Read back - this exercises Windows file-backed read
	snapshots, err := fr.ReadAll()
	require.NoError(t, err)
	assert.Equal(t, 1, len(snapshots))
	assert.Equal(t, 42.0, snapshots[0].RawMetrics[0].Value)
}

// TestE2E_Mmap_Windows_Persistence tests that Windows file-backed approach persists data.
func TestE2E_Mmap_Windows_Persistence(t *testing.T) {
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
	
	// Close - should sync data via WriteAt + Sync
	err = fr1.Close()
	require.NoError(t, err)
	
	// Reopen - should recover data from file
	fr2, err := NewFlightRecorder(path, 10)
	require.NoError(t, err)
	defer fr2.Close()
	
	snapshots, err := fr2.ReadAll()
	require.NoError(t, err)
	assert.Equal(t, 1, len(snapshots))
	assert.Equal(t, 100.0, snapshots[0].RawMetrics[0].Value)
}

// TestE2E_Mmap_Windows_MultipleWrites tests multiple writes with sync.
func TestE2E_Mmap_Windows_MultipleWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mmap_multiple_test.bin")
	fr, err := NewFlightRecorder(path, 10)
	require.NoError(t, err)
	defer fr.Close()
	
	// Write multiple snapshots - each Record() calls msync (WriteAt + Sync)
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

// TestE2E_Mmap_Windows_FileMapping tests Windows file mapping tracking.
func TestE2E_Mmap_Windows_FileMapping(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mmap_mapping_test.bin")
	
	// Create recorder (creates file mapping)
	fr1, err := NewFlightRecorder(path, 10)
	require.NoError(t, err)
	
	// Record something
	snapshot := poller.MetricsSnapshot{
		Timestamp: time.Now(),
		RawMetrics: []metric.RawMetric{
			{Name: "mapping_test", Value: 42.0},
		},
		Histograms: make(map[string]metric.Histogram),
		Errors:     []string{},
	}
	err = fr1.Record(snapshot)
	require.NoError(t, err)
	
	// Close should remove from mapping
	err = fr1.Close()
	require.NoError(t, err)
	
	// Create new recorder - should create new mapping
	fr2, err := NewFlightRecorder(path, 10)
	require.NoError(t, err)
	defer fr2.Close()
	
	snapshots, err := fr2.ReadAll()
	require.NoError(t, err)
	assert.Equal(t, 1, len(snapshots))
}

// TestE2E_Mmap_Windows_ConcurrentAccess tests concurrent file-backed access.
func TestE2E_Mmap_Windows_ConcurrentAccess(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mmap_concurrent_test.bin")
	fr, err := NewFlightRecorder(path, 100)
	require.NoError(t, err)
	defer fr.Close()
	
	// Concurrent writes - each uses file-backed approach with mutex
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

// TestE2E_Mmap_Windows_EmptyFile tests handling of empty file.
func TestE2E_Mmap_Windows_EmptyFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mmap_empty_test.bin")
	
	// Create empty file
	file, err := os.Create(path)
	require.NoError(t, err)
	file.Close()
	
	// Should handle empty file correctly
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
	
	snapshots, err := fr.ReadAll()
	require.NoError(t, err)
	assert.Equal(t, 1, len(snapshots))
}

// TestE2E_Mmap_Windows_LargeFile tests file-backed approach with large files.
func TestE2E_Mmap_Windows_LargeFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mmap_large_test.bin")
	// Use larger buffer size
	fr, err := NewFlightRecorder(path, 100)
	require.NoError(t, err)
	defer fr.Close()
	
	// Record many snapshots
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
	
	// Verify large file handling works
	snapshots, err := fr.ReadAll()
	require.NoError(t, err)
	assert.Equal(t, 50, len(snapshots))
}

// TestE2E_Mmap_Windows_Munmap tests munmap functionality (removes from map).
func TestE2E_Mmap_Windows_Munmap(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mmap_munmap_test.bin")
	
	// Create recorder (adds to windowsFileMap)
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
	
	// Close should remove from windowsFileMap
	err = fr.Close()
	require.NoError(t, err)
	
	// Verify file still exists and has data
	info, err := os.Stat(path)
	require.NoError(t, err)
	assert.Greater(t, info.Size(), int64(0))
	
	// Reopen should work (new mapping)
	fr2, err := NewFlightRecorder(path, 10)
	require.NoError(t, err)
	defer fr2.Close()
	
	snapshots, err := fr2.ReadAll()
	require.NoError(t, err)
	assert.Equal(t, 1, len(snapshots))
}

// TestE2E_Mmap_Windows_MultipleSyncs tests multiple sync calls.
func TestE2E_Mmap_Windows_MultipleSyncs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mmap_multisync_test.bin")
	fr, err := NewFlightRecorder(path, 10)
	require.NoError(t, err)
	defer fr.Close()
	
	// Each Record() calls msync (WriteAt + Sync) internally
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

// TestE2E_Mmap_Windows_RealWorldScenario tests realistic usage scenario.
func TestE2E_Mmap_Windows_RealWorldScenario(t *testing.T) {
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

// TestE2E_Mmap_Windows_FileResize tests file-backed approach with file resize.
func TestE2E_Mmap_Windows_FileResize(t *testing.T) {
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

// TestE2E_Mmap_Windows_PartialRead tests handling of partial file reads.
func TestE2E_Mmap_Windows_PartialRead(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mmap_partial_test.bin")
	
	// Create file with some initial data
	file, err := os.Create(path)
	require.NoError(t, err)
	_, err = file.Write([]byte("initial data"))
	require.NoError(t, err)
	file.Close()
	
	// Should handle partial read correctly (file smaller than expected size)
	fr, err := NewFlightRecorder(path, 10)
	require.NoError(t, err)
	defer fr.Close()
	
	// Should still work
	snapshot := poller.MetricsSnapshot{
		Timestamp: time.Now(),
		RawMetrics: []metric.RawMetric{
			{Name: "partial_test", Value: 42.0},
		},
		Histograms: make(map[string]metric.Histogram),
		Errors:     []string{},
	}
	
	err = fr.Record(snapshot)
	require.NoError(t, err)
}

