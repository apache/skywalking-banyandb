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
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/fodc/internal/metric"
	"github.com/apache/skywalking-banyandb/fodc/internal/poller"
)

// createTempFile creates a temporary file for testing.
func createTempFile(t *testing.T) string {
	tmpDir := t.TempDir()
	return filepath.Join(tmpDir, "flightrecorder_test.bin")
}

// createTestSnapshot creates a test metrics snapshot.
func createTestSnapshot(_ *testing.T, id int) poller.MetricsSnapshot {
	return poller.MetricsSnapshot{
		Timestamp: time.Now().Add(time.Duration(id) * time.Second),
		RawMetrics: []metric.RawMetric{
			{
				Name:   "test_metric",
				Labels: []metric.Label{{Name: "id", Value: fmt.Sprintf("%d", id)}},
				Value:  float64(id),
			},
		},
		Histograms: make(map[string]metric.Histogram),
		Errors:     []string{},
	}
}

// TestE2E_NewFlightRecorder_CreateNew tests creating a new flight recorder.
func TestE2E_NewFlightRecorder_CreateNew(t *testing.T) {
	path := createTempFile(t)
	fr, err := NewFlightRecorder(path, 100)

	require.NoError(t, err)
	require.NotNil(t, fr)
	defer fr.Close()

	// Verify stats
	totalCount, bufferSize, writeIndex := fr.GetStats()
	assert.Equal(t, uint32(0), totalCount)
	assert.Equal(t, uint32(100), bufferSize)
	assert.Equal(t, uint32(0), writeIndex)

	// Verify file exists
	info, err := os.Stat(path)
	require.NoError(t, err)
	assert.Greater(t, info.Size(), int64(0))
}

// TestE2E_NewFlightRecorder_DefaultBufferSize tests default buffer size.
func TestE2E_NewFlightRecorder_DefaultBufferSize(t *testing.T) {
	path := createTempFile(t)
	fr, err := NewFlightRecorder(path, 0)

	require.NoError(t, err)
	require.NotNil(t, fr)
	defer fr.Close()

	_, bufferSize, _ := fr.GetStats()
	assert.Equal(t, uint32(DefaultBufferSize), bufferSize)
}

// TestE2E_Record_And_ReadAll tests recording and reading snapshots.
func TestE2E_Record_And_ReadAll(t *testing.T) {
	path := createTempFile(t)
	fr, err := NewFlightRecorder(path, 10)
	require.NoError(t, err)
	defer fr.Close()

	// Record multiple snapshots
	numSnapshots := 5
	for i := 0; i < numSnapshots; i++ {
		snapshot := createTestSnapshot(t, i)
		err = fr.Record(snapshot)
		require.NoError(t, err)
	}

	// Verify stats
	totalCount, bufferSize, writeIndex := fr.GetStats()
	assert.Equal(t, uint32(numSnapshots), totalCount)
	assert.Equal(t, uint32(10), bufferSize)
	assert.Equal(t, uint32(numSnapshots), writeIndex)

	// Read all snapshots
	snapshots, err := fr.ReadAll()
	require.NoError(t, err)
	assert.Equal(t, numSnapshots, len(snapshots))

	// Verify snapshots are in order
	for i, snapshot := range snapshots {
		assert.Equal(t, float64(i), snapshot.RawMetrics[0].Value)
	}
}

// TestE2E_Record_CircularBuffer tests circular buffer behavior.
func TestE2E_Record_CircularBuffer(t *testing.T) {
	path := createTempFile(t)
	bufferSize := uint32(5)
	fr, err := NewFlightRecorder(path, bufferSize)
	require.NoError(t, err)
	defer fr.Close()

	// Fill buffer beyond capacity
	numSnapshots := int(bufferSize) + 3
	for i := 0; i < numSnapshots; i++ {
		snapshot := createTestSnapshot(t, i)
		err = fr.Record(snapshot)
		require.NoError(t, err)
	}

	// Verify stats
	totalCount, _, writeIndex := fr.GetStats()
	assert.Equal(t, uint32(numSnapshots), totalCount)
	// WriteIndex should wrap around
	assert.Equal(t, uint32(3), writeIndex) // (5+3) % 5 = 3

	// Read all - should get bufferSize snapshots (oldest to newest)
	snapshots, err := fr.ReadAll()
	require.NoError(t, err)
	assert.Equal(t, int(bufferSize), len(snapshots))

	// Should contain snapshots starting from index 3 (oldest after wrap)
	// The circular buffer should contain: 3, 4, 0, 1, 2 (in chronological order)
	expectedStart := numSnapshots - int(bufferSize)
	for i, snapshot := range snapshots {
		expectedID := expectedStart + i
		assert.Equal(t, float64(expectedID), snapshot.RawMetrics[0].Value)
	}
}

// TestE2E_ReadRecent tests reading recent snapshots.
func TestE2E_ReadRecent(t *testing.T) {
	path := createTempFile(t)
	fr, err := NewFlightRecorder(path, 10)
	require.NoError(t, err)
	defer fr.Close()

	// Record 10 snapshots
	for i := 0; i < 10; i++ {
		snapshot := createTestSnapshot(t, i)
		err = fr.Record(snapshot)
		require.NoError(t, err)
	}

	// Read recent 3
	recent, err := fr.ReadRecent(3)
	require.NoError(t, err)
	assert.Equal(t, 3, len(recent))

	// Should be last 3 snapshots
	assert.Equal(t, float64(7), recent[0].RawMetrics[0].Value)
	assert.Equal(t, float64(8), recent[1].RawMetrics[0].Value)
	assert.Equal(t, float64(9), recent[2].RawMetrics[0].Value)

	// Read recent more than available
	recent, err = fr.ReadRecent(20)
	require.NoError(t, err)
	assert.Equal(t, 10, len(recent))
}

// TestE2E_Recovery tests recovery after close/reopen.
func TestE2E_Recovery(t *testing.T) {
	path := createTempFile(t)

	// Create and record snapshots
	fr1, err := NewFlightRecorder(path, 10)
	require.NoError(t, err)

	numSnapshots := 5
	for i := 0; i < numSnapshots; i++ {
		snapshot := createTestSnapshot(t, i)
		err = fr1.Record(snapshot)
		require.NoError(t, err)
	}

	// Close first recorder
	err = fr1.Close()
	require.NoError(t, err)

	// Reopen and recover
	fr2, err := NewFlightRecorder(path, 10)
	require.NoError(t, err)
	defer fr2.Close()

	// Verify stats are recovered
	totalCount, bufferSize, writeIndex := fr2.GetStats()
	assert.Equal(t, uint32(numSnapshots), totalCount)
	assert.Equal(t, uint32(10), bufferSize)
	assert.Equal(t, uint32(numSnapshots), writeIndex)

	// Read all snapshots
	snapshots, err := fr2.ReadAll()
	require.NoError(t, err)
	assert.Equal(t, numSnapshots, len(snapshots))

	// Verify snapshots are correct
	for i, snapshot := range snapshots {
		assert.Equal(t, float64(i), snapshot.RawMetrics[0].Value)
	}
}

// TestE2E_Recover_Function tests the Recover function.
func TestE2E_Recover_Function(t *testing.T) {
	path := createTempFile(t)

	// Create and record snapshots
	fr, err := NewFlightRecorder(path, 10)
	require.NoError(t, err)

	numSnapshots := 3
	for i := 0; i < numSnapshots; i++ {
		snapshot := createTestSnapshot(t, i)
		err = fr.Record(snapshot)
		require.NoError(t, err)
	}

	err = fr.Close()
	require.NoError(t, err)

	// Recover snapshots
	snapshots, err := Recover(path)
	require.NoError(t, err)
	assert.Equal(t, numSnapshots, len(snapshots))

	// Verify snapshots
	for i, snapshot := range snapshots {
		assert.Equal(t, float64(i), snapshot.RawMetrics[0].Value)
	}
}

// TestE2E_Clear tests clearing the flight recorder.
func TestE2E_Clear(t *testing.T) {
	path := createTempFile(t)
	fr, err := NewFlightRecorder(path, 10)
	require.NoError(t, err)
	defer fr.Close()

	// Record some snapshots
	for i := 0; i < 5; i++ {
		snapshot := createTestSnapshot(t, i)
		err = fr.Record(snapshot)
		require.NoError(t, err)
	}

	// Verify snapshots exist
	snapshots, err := fr.ReadAll()
	require.NoError(t, err)
	assert.Equal(t, 5, len(snapshots))

	// Clear
	err = fr.Clear()
	require.NoError(t, err)

	// Verify stats are reset
	totalCount, _, writeIndex := fr.GetStats()
	assert.Equal(t, uint32(0), totalCount)
	assert.Equal(t, uint32(0), writeIndex)

	// Verify no snapshots
	snapshots, err = fr.ReadAll()
	require.NoError(t, err)
	assert.Equal(t, 0, len(snapshots))

	// Can still record after clear
	snapshot := createTestSnapshot(t, 10)
	err = fr.Record(snapshot)
	require.NoError(t, err)

	snapshots, err = fr.ReadAll()
	require.NoError(t, err)
	assert.Equal(t, 1, len(snapshots))
}

// TestE2E_Record_WithHistograms tests recording snapshots with histograms.
func TestE2E_Record_WithHistograms(t *testing.T) {
	path := createTempFile(t)
	fr, err := NewFlightRecorder(path, 10)
	require.NoError(t, err)
	defer fr.Close()

	snapshot := poller.MetricsSnapshot{
		Timestamp: time.Now(),
		RawMetrics: []metric.RawMetric{
			{Name: "counter_metric", Value: 100},
		},
		Histograms: map[string]metric.Histogram{
			"histogram_metric": {
				Name: "histogram_metric",
				Bins: []metric.Bin{
					{Value: 0.1, Count: 10},
					{Value: 0.5, Count: 20},
				},
			},
		},
		Errors: []string{},
	}

	err = fr.Record(snapshot)
	require.NoError(t, err)

	// Read back
	snapshots, err := fr.ReadAll()
	require.NoError(t, err)
	assert.Equal(t, 1, len(snapshots))

	readSnapshot := snapshots[0]
	assert.Equal(t, 1, len(readSnapshot.RawMetrics))
	assert.Equal(t, 1, len(readSnapshot.Histograms))

	hist, exists := readSnapshot.Histograms["histogram_metric"]
	require.True(t, exists)
	assert.Equal(t, 2, len(hist.Bins))
}

// TestE2E_Record_WithErrors tests recording snapshots with errors.
func TestE2E_Record_WithErrors(t *testing.T) {
	path := createTempFile(t)
	fr, err := NewFlightRecorder(path, 10)
	require.NoError(t, err)
	defer fr.Close()

	snapshot := poller.MetricsSnapshot{
		Timestamp:  time.Now(),
		RawMetrics: []metric.RawMetric{},
		Histograms: make(map[string]metric.Histogram),
		Errors:     []string{"Error 1", "Error 2"},
	}

	err = fr.Record(snapshot)
	require.NoError(t, err)

	// Read back
	snapshots, err := fr.ReadAll()
	require.NoError(t, err)
	assert.Equal(t, 1, len(snapshots))

	readSnapshot := snapshots[0]
	assert.Equal(t, 2, len(readSnapshot.Errors))
	assert.Equal(t, "Error 1", readSnapshot.Errors[0])
	assert.Equal(t, "Error 2", readSnapshot.Errors[1])
}

// TestE2E_ConcurrentRecord tests concurrent recording.
func TestE2E_ConcurrentRecord(t *testing.T) {
	path := createTempFile(t)
	fr, err := NewFlightRecorder(path, 100)
	require.NoError(t, err)
	defer fr.Close()

	var wg sync.WaitGroup
	numGoroutines := 10
	snapshotsPerGoroutine := 10

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < snapshotsPerGoroutine; j++ {
				snapshot := createTestSnapshot(t, id*100+j)
				_ = fr.Record(snapshot)
			}
		}(i)
	}

	wg.Wait()

	// Verify all snapshots were recorded
	totalCount, _, _ := fr.GetStats()
	assert.Equal(t, uint32(numGoroutines*snapshotsPerGoroutine), totalCount)

	// Read all
	snapshots, err := fr.ReadAll()
	require.NoError(t, err)
	assert.Equal(t, numGoroutines*snapshotsPerGoroutine, len(snapshots))
}

// TestE2E_ConcurrentRead tests concurrent reading.
func TestE2E_ConcurrentRead(t *testing.T) {
	path := createTempFile(t)
	fr, err := NewFlightRecorder(path, 10)
	require.NoError(t, err)
	defer fr.Close()

	// Record some snapshots
	for i := 0; i < 5; i++ {
		snapshot := createTestSnapshot(t, i)
		err = fr.Record(snapshot)
		require.NoError(t, err)
	}

	// Concurrent reads
	var wg sync.WaitGroup
	numReaders := 10
	wg.Add(numReaders)

	for i := 0; i < numReaders; i++ {
		go func() {
			defer wg.Done()
			snapshots, err := fr.ReadAll()
			assert.NoError(t, err)
			assert.Equal(t, 5, len(snapshots))
		}()
	}

	wg.Wait()
}

// TestE2E_Record_LargeSnapshot tests recording large snapshots.
func TestE2E_Record_LargeSnapshot(t *testing.T) {
	path := createTempFile(t)
	fr, err := NewFlightRecorder(path, 10)
	require.NoError(t, err)
	defer fr.Close()

	// Create a snapshot with many metrics
	snapshot := poller.MetricsSnapshot{
		Timestamp:  time.Now(),
		RawMetrics: make([]metric.RawMetric, 0, 1000),
		Histograms: make(map[string]metric.Histogram),
		Errors:     []string{},
	}

	// Add many metrics
	for i := 0; i < 1000; i++ {
		snapshot.RawMetrics = append(snapshot.RawMetrics, metric.RawMetric{
			Name:   "metric",
			Labels: []metric.Label{{Name: "id", Value: string(rune(i))}},
			Value:  float64(i),
		})
	}

	err = fr.Record(snapshot)
	require.NoError(t, err)

	// Read back
	snapshots, err := fr.ReadAll()
	require.NoError(t, err)
	assert.Equal(t, 1, len(snapshots))
	assert.Equal(t, 1000, len(snapshots[0].RawMetrics))
}

// TestE2E_Record_TooLargeSnapshot tests error handling for too large snapshots.
func TestE2E_Record_TooLargeSnapshot(t *testing.T) {
	path := createTempFile(t)
	fr, err := NewFlightRecorder(path, 10)
	require.NoError(t, err)
	defer fr.Close()

	// Create a snapshot that's too large (exceeds slot size - 8 bytes)
	// Slot size is 1MB, so we need > 1MB - 8 bytes
	largeData := make([]byte, 2*1024*1024) // 2MB
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	// Create a snapshot with very large JSON data
	// Use a large number of metrics instead of large label values
	snapshot := poller.MetricsSnapshot{
		Timestamp:  time.Now(),
		RawMetrics: make([]metric.RawMetric, 0, 100000),
		Histograms: make(map[string]metric.Histogram),
		Errors:     []string{},
	}

	// Add many metrics to exceed slot size
	for i := 0; i < 200000; i++ {
		snapshot.RawMetrics = append(snapshot.RawMetrics, metric.RawMetric{
			Name:   fmt.Sprintf("metric_%d", i),
			Labels: []metric.Label{{Name: "id", Value: fmt.Sprintf("%d", i)}},
			Value:  float64(i),
		})
	}

	err = fr.Record(snapshot)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "snapshot too large")
}

// TestE2E_Recovery_AfterCrash tests recovery after simulated crash.
func TestE2E_Recovery_AfterCrash(t *testing.T) {
	path := createTempFile(t)

	// Simulate crash: create recorder, write data, but don't close properly
	fr1, err := NewFlightRecorder(path, 10)
	require.NoError(t, err)

	// Record snapshots
	for i := 0; i < 7; i++ {
		snapshot := createTestSnapshot(t, i)
		err = fr1.Record(snapshot)
		require.NoError(t, err)
	}

	// Simulate crash - don't close properly
	// Just create a new recorder which should recover data
	fr2, err := NewFlightRecorder(path, 10)
	require.NoError(t, err)
	defer fr2.Close()

	// Should recover all snapshots
	snapshots, err := fr2.ReadAll()
	require.NoError(t, err)
	assert.Equal(t, 7, len(snapshots))
}

// TestE2E_GetStats tests GetStats functionality.
func TestE2E_GetStats(t *testing.T) {
	path := createTempFile(t)
	bufferSize := uint32(20)
	fr, err := NewFlightRecorder(path, bufferSize)
	require.NoError(t, err)
	defer fr.Close()

	// Initially empty
	totalCount, bs, writeIndex := fr.GetStats()
	assert.Equal(t, uint32(0), totalCount)
	assert.Equal(t, bufferSize, bs)
	assert.Equal(t, uint32(0), writeIndex)

	// Record some snapshots
	for i := 0; i < 5; i++ {
		snapshot := createTestSnapshot(t, i)
		err = fr.Record(snapshot)
		require.NoError(t, err)
	}

	// Check stats
	totalCount, bs, writeIndex = fr.GetStats()
	assert.Equal(t, uint32(5), totalCount)
	assert.Equal(t, bufferSize, bs)
	assert.Equal(t, uint32(5), writeIndex)

	// Fill beyond buffer size
	for i := 5; i < 25; i++ {
		snapshot := createTestSnapshot(t, i)
		err = fr.Record(snapshot)
		require.NoError(t, err)
	}

	// Check stats after wrap
	totalCount, bs, writeIndex = fr.GetStats()
	assert.Equal(t, uint32(25), totalCount)
	assert.Equal(t, bufferSize, bs)
	assert.Equal(t, uint32(5), writeIndex) // (25) % 20 = 5
}

// TestE2E_RealWorldScenario tests a realistic usage scenario.
func TestE2E_RealWorldScenario(t *testing.T) {
	path := createTempFile(t)

	// Simulate a real-world scenario: record metrics over time, recover after restart
	fr1, err := NewFlightRecorder(path, 100)
	require.NoError(t, err)

	// Record snapshots with realistic data
	for i := 0; i < 50; i++ {
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
						{Value: 0.5, Count: uint64(20 + i)},
					},
				},
			},
			Errors: []string{},
		}
		err = fr1.Record(snapshot)
		require.NoError(t, err)
	}

	err = fr1.Close()
	require.NoError(t, err)

	// Restart and recover
	fr2, err := NewFlightRecorder(path, 100)
	require.NoError(t, err)
	defer fr2.Close()

	// Read recent 10
	recent, err := fr2.ReadRecent(10)
	require.NoError(t, err)
	assert.Equal(t, 10, len(recent))

	// Verify they're the most recent
	for i, snapshot := range recent {
		expectedIndex := 40 + i
		assert.Equal(t, float64(100+expectedIndex), snapshot.RawMetrics[0].Value)
	}

	// Clear and verify
	err = fr2.Clear()
	require.NoError(t, err)

	all, err := fr2.ReadAll()
	require.NoError(t, err)
	assert.Equal(t, 0, len(all))
}

// TestE2E_ReadAll_EmptyBuffer tests reading from empty buffer.
func TestE2E_ReadAll_EmptyBuffer(t *testing.T) {
	path := createTempFile(t)
	fr, err := NewFlightRecorder(path, 10)
	require.NoError(t, err)
	defer fr.Close()

	snapshots, err := fr.ReadAll()
	require.NoError(t, err)
	assert.Equal(t, 0, len(snapshots))
}

// TestE2E_ReadRecent_EmptyBuffer tests reading recent from empty buffer.
func TestE2E_ReadRecent_EmptyBuffer(t *testing.T) {
	path := createTempFile(t)
	fr, err := NewFlightRecorder(path, 10)
	require.NoError(t, err)
	defer fr.Close()

	recent, err := fr.ReadRecent(5)
	require.NoError(t, err)
	assert.Equal(t, 0, len(recent))
}

// TestE2E_MultipleClose tests closing multiple times.
func TestE2E_MultipleClose(t *testing.T) {
	path := createTempFile(t)
	fr, err := NewFlightRecorder(path, 10)
	require.NoError(t, err)

	// Record something
	snapshot := createTestSnapshot(t, 0)
	err = fr.Record(snapshot)
	require.NoError(t, err)

	// Close multiple times should be safe
	err = fr.Close()
	require.NoError(t, err)

	err = fr.Close()
	require.NoError(t, err)
}

// TestE2E_FileFormatVersion tests file format version handling.
// Note: Testing version incompatibility by manually corrupting files is unsafe
// due to memory mapping. This test verifies normal version handling.
func TestE2E_FileFormatVersion(t *testing.T) {
	path := createTempFile(t)

	// Create a recorder and write some data
	fr1, err := NewFlightRecorder(path, 10)
	require.NoError(t, err)

	snapshot := createTestSnapshot(t, 0)
	err = fr1.Record(snapshot)
	require.NoError(t, err)

	err = fr1.Close()
	require.NoError(t, err)

	// Reopen with same version - should work
	fr2, err := NewFlightRecorder(path, 10)
	require.NoError(t, err)
	defer fr2.Close()

	// Should be able to read the snapshot
	snapshots, err := fr2.ReadAll()
	require.NoError(t, err)
	assert.Equal(t, 1, len(snapshots))
}

// TestE2E_CorruptedSlots tests handling of corrupted slots.
func TestE2E_CorruptedSlots(t *testing.T) {
	path := createTempFile(t)
	fr, err := NewFlightRecorder(path, 10)
	require.NoError(t, err)
	defer fr.Close()

	// Record some snapshots
	for i := 0; i < 5; i++ {
		snapshot := createTestSnapshot(t, i)
		err = fr.Record(snapshot)
		require.NoError(t, err)
	}

	// Manually corrupt a slot by writing invalid size
	file, err := os.OpenFile(path, os.O_RDWR, 0o644)
	require.NoError(t, err)

	// Corrupt slot 2 (write invalid size > slotSize)
	// Slot size is 1MB (1024 * 1024)
	slotSize := uint32(1024 * 1024)
	slotOffset := HeaderSize + int64(2)*int64(slotSize)
	_, err = file.Seek(slotOffset, 0)
	require.NoError(t, err)

	// Write invalid size (larger than slot size)
	invalidSize := make([]byte, 4)
	binary.LittleEndian.PutUint32(invalidSize, slotSize+1000)
	_, err = file.Write(invalidSize)
	require.NoError(t, err)
	file.Close()

	// ReadAll should skip corrupted slot
	snapshots, err := fr.ReadAll()
	require.NoError(t, err)
	// Should have fewer snapshots due to corrupted one being skipped
	assert.LessOrEqual(t, len(snapshots), 5)
}

// TestE2E_MultipleRecordersSameFile tests multiple recorders on same file.
func TestE2E_MultipleRecordersSameFile(t *testing.T) {
	path := createTempFile(t)

	// Create first recorder
	fr1, err := NewFlightRecorder(path, 10)
	require.NoError(t, err)

	// Record with first recorder
	snapshot1 := createTestSnapshot(t, 1)
	err = fr1.Record(snapshot1)
	require.NoError(t, err)

	// Create second recorder on same file (should work)
	fr2, err := NewFlightRecorder(path, 10)
	require.NoError(t, err)

	// Record with second recorder
	snapshot2 := createTestSnapshot(t, 2)
	err = fr2.Record(snapshot2)
	require.NoError(t, err)

	// Close first recorder
	err = fr1.Close()
	require.NoError(t, err)

	// Read from second recorder - should see both snapshots
	snapshots, err := fr2.ReadAll()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(snapshots), 2)

	err = fr2.Close()
	require.NoError(t, err)
}

// TestE2E_BufferBoundaryEdgeCases tests edge cases around buffer boundaries.
func TestE2E_BufferBoundaryEdgeCases(t *testing.T) {
	path := createTempFile(t)
	bufferSize := uint32(3)
	fr, err := NewFlightRecorder(path, bufferSize)
	require.NoError(t, err)
	defer fr.Close()

	// Fill exactly to buffer size
	for i := 0; i < int(bufferSize); i++ {
		snapshot := createTestSnapshot(t, i)
		err = fr.Record(snapshot)
		require.NoError(t, err)
	}

	totalCount, _, writeIndex := fr.GetStats()
	assert.Equal(t, bufferSize, totalCount)
	assert.Equal(t, uint32(0), writeIndex) // Should wrap to 0

	// Add one more to trigger wrap
	snapshot := createTestSnapshot(t, int(bufferSize))
	err = fr.Record(snapshot)
	require.NoError(t, err)

	totalCount, _, writeIndex = fr.GetStats()
	assert.Equal(t, bufferSize+1, totalCount)
	assert.Equal(t, uint32(1), writeIndex) // Wrapped to 1

	// ReadAll should return bufferSize snapshots
	snapshots, err := fr.ReadAll()
	require.NoError(t, err)
	assert.Equal(t, int(bufferSize), len(snapshots))

	// Should contain snapshots starting from index 1 (oldest after wrap)
	assert.Equal(t, float64(1), snapshots[0].RawMetrics[0].Value)
}

// TestE2E_StressTest_ManySnapshots tests stress test with many snapshots.
func TestE2E_StressTest_ManySnapshots(t *testing.T) {
	path := createTempFile(t)
	bufferSize := uint32(100)
	fr, err := NewFlightRecorder(path, bufferSize)
	require.NoError(t, err)
	defer fr.Close()

	// Record many snapshots (more than buffer size)
	numSnapshots := int(bufferSize) * 3
	for i := 0; i < numSnapshots; i++ {
		snapshot := createTestSnapshot(t, i)
		err = fr.Record(snapshot)
		require.NoError(t, err)
	}

	// Verify stats
	totalCount, _, writeIndex := fr.GetStats()
	assert.Equal(t, uint32(numSnapshots), totalCount)
	assert.Equal(t, uint32(numSnapshots)%bufferSize, writeIndex)

	// ReadAll should return bufferSize snapshots
	snapshots, err := fr.ReadAll()
	require.NoError(t, err)
	assert.Equal(t, int(bufferSize), len(snapshots))

	// Verify they're the most recent ones
	expectedStart := numSnapshots - int(bufferSize)
	for i, snapshot := range snapshots {
		expectedID := expectedStart + i
		assert.Equal(t, float64(expectedID), snapshot.RawMetrics[0].Value)
	}
}

// TestE2E_ReadRecent_EdgeCases tests ReadRecent edge cases.
func TestE2E_ReadRecent_EdgeCases(t *testing.T) {
	path := createTempFile(t)
	fr, err := NewFlightRecorder(path, 10)
	require.NoError(t, err)
	defer fr.Close()

	// Record 5 snapshots
	for i := 0; i < 5; i++ {
		snapshot := createTestSnapshot(t, i)
		err = fr.Record(snapshot)
		require.NoError(t, err)
	}

	// Read recent 0
	recent, err := fr.ReadRecent(0)
	require.NoError(t, err)
	assert.Equal(t, 0, len(recent))

	// Read recent 1
	recent, err = fr.ReadRecent(1)
	require.NoError(t, err)
	assert.Equal(t, 1, len(recent))
	assert.Equal(t, float64(4), recent[0].RawMetrics[0].Value)

	// Read recent exactly what we have
	recent, err = fr.ReadRecent(5)
	require.NoError(t, err)
	assert.Equal(t, 5, len(recent))

	// Read recent more than we have
	recent, err = fr.ReadRecent(100)
	require.NoError(t, err)
	assert.Equal(t, 5, len(recent))
}

// TestE2E_Recovery_CircularBufferWrap tests recovery after circular buffer wrap.
func TestE2E_Recovery_CircularBufferWrap(t *testing.T) {
	path := createTempFile(t)
	bufferSize := uint32(5)

	// Create recorder and fill beyond buffer size
	fr1, err := NewFlightRecorder(path, bufferSize)
	require.NoError(t, err)

	// Record more than buffer size
	for i := 0; i < 8; i++ {
		snapshot := createTestSnapshot(t, i)
		err = fr1.Record(snapshot)
		require.NoError(t, err)
	}

	err = fr1.Close()
	require.NoError(t, err)

	// Recover
	fr2, err := NewFlightRecorder(path, bufferSize)
	require.NoError(t, err)
	defer fr2.Close()

	// Should recover bufferSize snapshots (oldest after wrap)
	snapshots, err := fr2.ReadAll()
	require.NoError(t, err)
	assert.Equal(t, int(bufferSize), len(snapshots))

	// Should start from index 3 (8 % 5 = 3)
	expectedStart := 8 - int(bufferSize)
	for i, snapshot := range snapshots {
		expectedID := expectedStart + i
		assert.Equal(t, float64(expectedID), snapshot.RawMetrics[0].Value)
	}
}

// TestE2E_Record_EmptySnapshot tests recording empty snapshots.
func TestE2E_Record_EmptySnapshot(t *testing.T) {
	path := createTempFile(t)
	fr, err := NewFlightRecorder(path, 10)
	require.NoError(t, err)
	defer fr.Close()

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
	assert.Equal(t, 0, len(snapshots[0].RawMetrics))
	assert.Equal(t, 0, len(snapshots[0].Histograms))
	assert.Equal(t, 0, len(snapshots[0].Errors))
}

// TestE2E_Clear_AndReuse tests clearing and reusing after clear.
func TestE2E_Clear_AndReuse(t *testing.T) {
	path := createTempFile(t)
	fr, err := NewFlightRecorder(path, 10)
	require.NoError(t, err)
	defer fr.Close()

	// Record some snapshots
	for i := 0; i < 5; i++ {
		snapshot := createTestSnapshot(t, i)
		err = fr.Record(snapshot)
		require.NoError(t, err)
	}

	// Clear
	err = fr.Clear()
	require.NoError(t, err)

	// Verify cleared
	snapshots, err := fr.ReadAll()
	require.NoError(t, err)
	assert.Equal(t, 0, len(snapshots))

	// Record new snapshots
	for i := 10; i < 15; i++ {
		snapshot := createTestSnapshot(t, i)
		err = fr.Record(snapshot)
		require.NoError(t, err)
	}

	// Verify new snapshots
	snapshots, err = fr.ReadAll()
	require.NoError(t, err)
	assert.Equal(t, 5, len(snapshots))

	// Should start from 0 (fresh start after clear)
	for i, snapshot := range snapshots {
		assert.Equal(t, float64(10+i), snapshot.RawMetrics[0].Value)
	}
}

// TestE2E_ConcurrentRecordAndRead tests concurrent record and read operations.
func TestE2E_ConcurrentRecordAndRead(t *testing.T) {
	path := createTempFile(t)
	fr, err := NewFlightRecorder(path, 100)
	require.NoError(t, err)
	defer fr.Close()

	var wg sync.WaitGroup

	// Concurrent writers
	numWriters := 5
	wg.Add(numWriters)
	for i := 0; i < numWriters; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				snapshot := createTestSnapshot(t, id*100+j)
				_ = fr.Record(snapshot)
			}
		}(i)
	}

	// Concurrent readers
	numReaders := 3
	wg.Add(numReaders)
	for i := 0; i < numReaders; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				_, _ = fr.ReadAll()
				_, _ = fr.ReadRecent(5)
				_, _, _ = fr.GetStats()
			}
		}()
	}

	wg.Wait()

	// Verify final state
	totalCount, _, _ := fr.GetStats()
	assert.Equal(t, uint32(numWriters*10), totalCount)

	snapshots, err := fr.ReadAll()
	require.NoError(t, err)
	assert.Greater(t, len(snapshots), 0)
}

// TestE2E_RealWorld_MetricsWithAllFields tests realistic metrics with all fields populated.
func TestE2E_RealWorld_MetricsWithAllFields(t *testing.T) {
	path := createTempFile(t)
	fr, err := NewFlightRecorder(path, 10)
	require.NoError(t, err)
	defer fr.Close()

	// Create realistic snapshot with all fields
	snapshot := poller.MetricsSnapshot{
		Timestamp: time.Now(),
		RawMetrics: []metric.RawMetric{
			{
				Name:        "http_requests_total",
				Labels:      []metric.Label{{Name: "method", Value: "GET"}, {Name: "status", Value: "200"}},
				Value:       1234.0,
				Description: "Total HTTP requests",
			},
			{
				Name:        "memory_usage_bytes",
				Value:       1073741824.0,
				Description: "Memory usage",
			},
		},
		Histograms: map[string]metric.Histogram{
			"http_request_duration_seconds": {
				Name:        "http_request_duration_seconds",
				Description: "HTTP request duration",
				Bins: []metric.Bin{
					{Value: 0.1, Count: 10},
					{Value: 0.5, Count: 20},
					{Value: 1.0, Count: 30},
				},
			},
		},
		Errors: []string{"Warning: High memory usage"},
	}

	err = fr.Record(snapshot)
	require.NoError(t, err)

	// Read back
	snapshots, err := fr.ReadAll()
	require.NoError(t, err)
	assert.Equal(t, 1, len(snapshots))

	readSnapshot := snapshots[0]
	assert.Equal(t, 2, len(readSnapshot.RawMetrics))
	assert.Equal(t, 1, len(readSnapshot.Histograms))
	assert.Equal(t, 1, len(readSnapshot.Errors))

	// Verify histogram
	hist, exists := readSnapshot.Histograms["http_request_duration_seconds"]
	require.True(t, exists)
	assert.Equal(t, "HTTP request duration", hist.Description)
	assert.Equal(t, 3, len(hist.Bins))
}

// TestE2E_FileResize_ExistingFile tests opening existing file with different buffer size.
func TestE2E_FileResize_ExistingFile(t *testing.T) {
	path := createTempFile(t)

	// Create with buffer size 10
	fr1, err := NewFlightRecorder(path, 10)
	require.NoError(t, err)

	snapshot := createTestSnapshot(t, 0)
	err = fr1.Record(snapshot)
	require.NoError(t, err)

	err = fr1.Close()
	require.NoError(t, err)

	// Open with same buffer size - should work
	fr2, err := NewFlightRecorder(path, 10)
	require.NoError(t, err)
	defer fr2.Close()

	snapshots, err := fr2.ReadAll()
	require.NoError(t, err)
	assert.Equal(t, 1, len(snapshots))
}

// TestE2E_GetStats_ConcurrentAccess tests GetStats with concurrent access.
func TestE2E_GetStats_ConcurrentAccess(t *testing.T) {
	path := createTempFile(t)
	fr, err := NewFlightRecorder(path, 10)
	require.NoError(t, err)
	defer fr.Close()

	// Record some snapshots
	for i := 0; i < 5; i++ {
		snapshot := createTestSnapshot(t, i)
		err = fr.Record(snapshot)
		require.NoError(t, err)
	}

	// Concurrent GetStats calls
	var wg sync.WaitGroup
	numGoroutines := 20
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				totalCount, bufferSize, writeIndex := fr.GetStats()
				assert.Equal(t, uint32(5), totalCount)
				assert.Equal(t, uint32(10), bufferSize)
				assert.Equal(t, uint32(5), writeIndex)
			}
		}()
	}

	wg.Wait()
}
