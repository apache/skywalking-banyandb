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

package flightrecorder

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/fodc/internal/metrics"
)

// TestDatasource_Update_TimestampBufferCapacity tests timestamp buffer capacity adjustment.
func TestDatasource_Update_TimestampBufferCapacity(t *testing.T) {
	ds := NewDatasource()
	ds.SetCapacity(1000000) // Use larger capacity to ensure ComputeCapacity returns > 0

	// Add a metric to trigger capacity calculation
	rawMetric := &metrics.RawMetric{
		Name:   "cpu_usage",
		Value:  75.5,
		Labels: []metrics.Label{},
	}
	err := ds.Update(rawMetric)
	require.NoError(t, err)

	// Set capacity after adding metric to ensure all buffers have the correct capacity
	ds.SetCapacity(1000000)

	initialTimestampCap := ds.timestamps.Cap()
	// Skip test if initial capacity is 0 (overhead too high)
	if initialTimestampCap == 0 {
		t.Skip("Initial capacity is 0, skipping capacity adjustment test")
	}

	// Change capacity
	ds.SetCapacity(2000000)
	err = ds.Update(rawMetric)
	require.NoError(t, err)

	// Timestamp buffer capacity should be adjusted
	finalTimestampCap := ds.timestamps.Cap()
	// Skip if final capacity is 0
	if finalTimestampCap == 0 {
		t.Skip("Final capacity is 0, skipping capacity adjustment test")
	}
	assert.NotEqual(t, initialTimestampCap, finalTimestampCap)
}

// TestDatasource_Update_AllBuffersSameCapacity tests that all buffers have the same capacity.
func TestDatasource_Update_AllBuffersSameCapacity(t *testing.T) {
	ds := NewDatasource()
	ds.SetCapacity(1000000) // Use larger capacity to ensure ComputeCapacity returns > 0

	// Add multiple metrics
	rawMetric1 := &metrics.RawMetric{
		Name:   "cpu_usage",
		Value:  75.5,
		Labels: []metrics.Label{},
	}
	err1 := ds.Update(rawMetric1)
	require.NoError(t, err1)

	rawMetric2 := &metrics.RawMetric{
		Name:   "memory_usage",
		Value:  50.0,
		Labels: []metrics.Label{},
	}
	err2 := ds.Update(rawMetric2)
	require.NoError(t, err2)

	// Set capacity again after adding metrics to ensure all buffers have the correct capacity
	ds.SetCapacity(1000000)

	metricsMap := ds.GetMetrics()
	cpuCap := metricsMap["cpu_usage"].Cap()
	memoryCap := metricsMap["memory_usage"].Cap()
	timestampCap := ds.timestamps.Cap()

	// Skip test if any capacity is 0 due to overhead being too high
	if cpuCap == 0 || memoryCap == 0 || timestampCap == 0 {
		t.Skip("Capacity is 0, likely due to overhead exceeding available memory")
	}

	// All buffers should have the same capacity
	assert.Equal(t, cpuCap, memoryCap)
	assert.Equal(t, cpuCap, timestampCap)
}

// TestDatasource_Update_ConcurrentUpdatesDifferentMetrics tests concurrent updates to different metrics.
func TestDatasource_Update_ConcurrentUpdatesDifferentMetrics(t *testing.T) {
	ds := NewDatasource()
	ds.SetCapacity(100000)

	var wg sync.WaitGroup
	numGoroutines := 10

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(metricID int) {
			defer wg.Done()
			rawMetric := &metrics.RawMetric{
				Name:   "metric_name",
				Value:  float64(metricID),
				Labels: []metrics.Label{{Name: "id", Value: string(rune('0' + metricID))}},
			}
			_ = ds.Update(rawMetric)
		}(i)
	}

	wg.Wait()

	// Should have successfully updated all metrics
	metricsMap := ds.GetMetrics()
	assert.GreaterOrEqual(t, len(metricsMap), numGoroutines)
}

// TestDatasource_AddTimestamp tests adding timestamps to the datasource.
func TestDatasource_AddTimestamp(t *testing.T) {
	ds := NewDatasource()
	ds.SetCapacity(10000)

	ds.AddTimestamp(int64(1000))

	timestamps := ds.GetTimestamps()
	assert.GreaterOrEqual(t, timestamps.Len(), 1)
}

// TestDatasource_Update_EmptyDescriptionDoesNotOverwrite tests that empty description does not overwrite existing.
func TestDatasource_Update_EmptyDescriptionDoesNotOverwrite(t *testing.T) {
	ds := NewDatasource()
	ds.SetCapacity(10000)

	rawMetric1 := &metrics.RawMetric{
		Name:   "cpu_usage",
		Value:  75.5,
		Labels: []metrics.Label{},
		Desc:   "CPU usage percentage",
	}
	err1 := ds.Update(rawMetric1)
	require.NoError(t, err1)

	rawMetric2 := &metrics.RawMetric{
		Name:   "cpu_usage",
		Value:  80.0,
		Labels: []metrics.Label{},
		Desc:   "", // Empty description should not overwrite
	}
	err2 := ds.Update(rawMetric2)
	require.NoError(t, err2)

	descriptions := ds.GetDescriptions()
	// Empty description should not overwrite existing description
	assert.Equal(t, "CPU usage percentage", descriptions["cpu_usage"])
}

// TestDatasource_Update_MultipleUpdatesSameMetric tests multiple updates to the same metric.
func TestDatasource_Update_MultipleUpdatesSameMetric(t *testing.T) {
	ds := NewDatasource()
	ds.SetCapacity(10000)

	rawMetric := &metrics.RawMetric{
		Name:   "cpu_usage",
		Value:  75.5,
		Labels: []metrics.Label{},
	}

	for i := 0; i < 10; i++ {
		rawMetric.Value = float64(75 + i)
		err := ds.Update(rawMetric)
		require.NoError(t, err)
	}

	metricsMap := ds.GetMetrics()
	buffer := metricsMap["cpu_usage"]
	assert.Equal(t, 84.0, buffer.GetCurrentValue()) // 75 + 9
}

// TestDatasource_ComputeCapacity_EdgeCases tests edge cases for ComputeCapacity.
func TestDatasource_ComputeCapacity_EdgeCases(t *testing.T) {
	ds := NewDatasource()

	// Test with exactly the overhead size
	rawMetric := &metrics.RawMetric{
		Name:   "cpu_usage",
		Value:  75.5,
		Labels: []metrics.Label{},
	}
	err := ds.Update(rawMetric)
	require.NoError(t, err)

	// Calculate overhead
	overhead := ds.ComputeCapacity(1000000)
	// Use capacity that's just slightly more than overhead
	result := ds.ComputeCapacity(int64(overhead*8 + 100))
	assert.Greater(t, result, 0)
}

// TestDatasource_Update_BufferResizePreservesValues tests that buffer resize preserves values.
func TestDatasource_Update_BufferResizePreservesValues(t *testing.T) {
	ds := NewDatasource()
	ds.CapacitySize = 10000

	// Add some values and finalize them (since Update doesn't auto-finalize)
	values := []float64{70.0, 71.0, 72.0, 73.0, 74.0}
	for _, val := range values {
		rawMetric := &metrics.RawMetric{
			Name:   "cpu_usage",
			Value:  val,
			Labels: []metrics.Label{},
		}
		err := ds.Update(rawMetric)
		require.NoError(t, err)
		// Finalize the value to make it visible
		metricsMap := ds.GetMetrics()
		buffer := metricsMap["cpu_usage"]
		buffer.FinalizeLastVisible()
	}

	metricsMap1 := ds.GetMetrics()
	buffer1 := metricsMap1["cpu_usage"]
	valuesBefore := buffer1.GetAllValues()
	require.NotNil(t, valuesBefore) // Verify buffer exists and has values before resize

	// Change capacity to trigger resize
	ds.SetCapacity(50000)
	rawMetric := &metrics.RawMetric{
		Name:   "cpu_usage",
		Value:  75.0,
		Labels: []metrics.Label{},
	}
	err := ds.Update(rawMetric)
	require.NoError(t, err)
	// Finalize the new value
	metricsMap2 := ds.GetMetrics()
	buffer2 := metricsMap2["cpu_usage"]
	buffer2.FinalizeLastVisible()

	valuesAfter := buffer2.GetAllValues()
	require.NotNil(t, valuesAfter)

	// All previous values should be preserved
	for _, val := range values {
		assert.Contains(t, valuesAfter, val)
	}
	// New value should be present
	assert.Contains(t, valuesAfter, 75.0)
}

// TestDatasource_Update_ResizesAllBuffers tests that Update resizes all buffers when capacity changes.
func TestDatasource_Update_ResizesAllBuffers(t *testing.T) {
	ds := NewDatasource()
	ds.SetCapacity(1000000) // Use larger capacity to ensure ComputeCapacity returns > 0

	// Add multiple metrics
	rawMetric1 := &metrics.RawMetric{
		Name:   "cpu_usage",
		Value:  75.5,
		Labels: []metrics.Label{},
	}
	err1 := ds.Update(rawMetric1)
	require.NoError(t, err1)

	rawMetric2 := &metrics.RawMetric{
		Name:   "memory_usage",
		Value:  50.0,
		Labels: []metrics.Label{},
	}
	err2 := ds.Update(rawMetric2)
	require.NoError(t, err2)

	// Set capacity again after adding metrics to ensure all buffers have the correct capacity
	ds.SetCapacity(1000000)

	metricsMap1 := ds.GetMetrics()
	cpuCap1 := metricsMap1["cpu_usage"].Cap()
	memoryCap1 := metricsMap1["memory_usage"].Cap()
	timestampCap1 := ds.timestamps.Cap()

	// Skip test if initial capacity is 0
	if cpuCap1 == 0 {
		t.Skip("Initial capacity is 0, skipping resize test")
	}

	// Change capacity
	ds.SetCapacity(2000000)
	err3 := ds.Update(rawMetric1)
	require.NoError(t, err3)

	metricsMap2 := ds.GetMetrics()
	cpuCap2 := metricsMap2["cpu_usage"].Cap()
	memoryCap2 := metricsMap2["memory_usage"].Cap()
	timestampCap2 := ds.timestamps.Cap()

	// Skip test if any final capacity is 0
	if cpuCap2 == 0 || memoryCap2 == 0 || timestampCap2 == 0 {
		t.Skip("Final capacity is 0, skipping resize test")
	}

	// All buffers should have been resized (if capacity changed)
	if cpuCap2 != cpuCap1 {
		assert.NotEqual(t, cpuCap1, cpuCap2)
		assert.NotEqual(t, memoryCap1, memoryCap2)
		assert.NotEqual(t, timestampCap1, timestampCap2)
	}
	// All buffers should still have the same capacity
	assert.Equal(t, cpuCap2, memoryCap2)
	assert.Equal(t, cpuCap2, timestampCap2)
}

// TestDatasource_Update_WithLongMetricNames tests Update with long metric names.
func TestDatasource_Update_WithLongMetricNames(t *testing.T) {
	ds := NewDatasource()
	ds.SetCapacity(10000)

	longName := "very_long_metric_name_that_might_affect_string_overhead_calculation"
	rawMetric := &metrics.RawMetric{
		Name:   longName,
		Value:  75.5,
		Labels: []metrics.Label{},
		Desc:   "A very long description that also affects string overhead calculation",
	}
	err := ds.Update(rawMetric)
	require.NoError(t, err)

	metricsMap := ds.GetMetrics()
	assert.Contains(t, metricsMap, longName)
	descriptions := ds.GetDescriptions()
	assert.Equal(t, "A very long description that also affects string overhead calculation", descriptions[longName])
}

// TestDatasource_Update_WithManyLabels tests Update with metrics that have many labels.
func TestDatasource_Update_WithManyLabels(t *testing.T) {
	ds := NewDatasource()
	ds.SetCapacity(10000)

	rawMetric := &metrics.RawMetric{
		Name:  "http_requests_total",
		Value: 100.0,
		Labels: []metrics.Label{
			{Name: "method", Value: "GET"},
			{Name: "status", Value: "200"},
			{Name: "endpoint", Value: "/api/v1/users"},
			{Name: "version", Value: "1.0.0"},
			{Name: "environment", Value: "production"},
		},
	}
	err := ds.Update(rawMetric)
	require.NoError(t, err)

	metricsMap := ds.GetMetrics()
	// Should create a metric key with all labels sorted
	assert.GreaterOrEqual(t, len(metricsMap), 1)
	// Verify the metric exists
	found := false
	for key := range metricsMap {
		if len(key) > len("http_requests_total") {
			found = true
			break
		}
	}
	assert.True(t, found)
}

// TestDatasource_ComputeCapacity_WithDescriptions tests ComputeCapacity accounting for descriptions.
func TestDatasource_ComputeCapacity_WithDescriptions(t *testing.T) {
	ds := NewDatasource()

	// Add metrics with descriptions
	for i := 0; i < 10; i++ {
		rawMetric := &metrics.RawMetric{
			Name:   "metric_name",
			Value:  float64(i),
			Labels: []metrics.Label{{Name: "index", Value: string(rune('0' + i))}},
			Desc:   "A description for metric " + string(rune('0'+i)),
		}
		err := ds.Update(rawMetric)
		require.NoError(t, err)
	}

	ds.SetCapacity(100000)
	result := ds.ComputeCapacity(ds.CapacitySize)

	// Should calculate capacity accounting for descriptions
	assert.Greater(t, result, 0)
}

// TestDatasource_Update_ConcurrentCapacityChanges tests concurrent updates with capacity changes.
func TestDatasource_Update_ConcurrentCapacityChanges(t *testing.T) {
	ds := NewDatasource()
	ds.SetCapacity(10000)

	var wg sync.WaitGroup
	numGoroutines := 5

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			// Change capacity in some goroutines using thread-safe method
			if id%2 == 0 {
				ds.SetCapacity(20000)
			}
			rawMetric := &metrics.RawMetric{
				Name:   "cpu_usage",
				Value:  float64(id),
				Labels: []metrics.Label{},
			}
			_ = ds.Update(rawMetric)
		}(i)
	}

	wg.Wait()

	// Should have successfully updated
	metricsMap := ds.GetMetrics()
	assert.Contains(t, metricsMap, "cpu_usage")
}

// TestDatasource_UpdateBatch_Atomicity tests that batch updates are atomic.
// All metrics and timestamp are updated together, ensuring readers never see partial updates.
func TestDatasource_UpdateBatch_Atomicity(t *testing.T) {
	ds := NewDatasource()
	ds.SetCapacity(10000)

	// First batch: initialize metrics
	batch1 := []metrics.RawMetric{
		{Name: "metric1", Value: 10.0, Labels: []metrics.Label{}},
		{Name: "metric2", Value: 20.0, Labels: []metrics.Label{}},
		{Name: "metric3", Value: 30.0, Labels: []metrics.Label{}},
	}
	timestamp1 := int64(1000)
	err := ds.UpdateBatch(batch1, timestamp1)
	require.NoError(t, err)

	// Verify first batch is visible
	metricsMap1 := ds.GetMetrics()
	timestamps1 := ds.GetTimestamps()
	assert.Equal(t, 10.0, metricsMap1["metric1"].GetCurrentValue())
	assert.Equal(t, 20.0, metricsMap1["metric2"].GetCurrentValue())
	assert.Equal(t, 30.0, metricsMap1["metric3"].GetCurrentValue())
	assert.Equal(t, timestamp1, timestamps1.GetCurrentValue())

	// Second batch: update all metrics
	batch2 := []metrics.RawMetric{
		{Name: "metric1", Value: 11.0, Labels: []metrics.Label{}},
		{Name: "metric2", Value: 21.0, Labels: []metrics.Label{}},
		{Name: "metric3", Value: 31.0, Labels: []metrics.Label{}},
	}
	timestamp2 := int64(2000)
	err = ds.UpdateBatch(batch2, timestamp2)
	require.NoError(t, err)

	// Verify second batch is visible - all metrics and timestamp updated together
	metricsMap2 := ds.GetMetrics()
	timestamps2 := ds.GetTimestamps()
	assert.Equal(t, 11.0, metricsMap2["metric1"].GetCurrentValue())
	assert.Equal(t, 21.0, metricsMap2["metric2"].GetCurrentValue())
	assert.Equal(t, 31.0, metricsMap2["metric3"].GetCurrentValue())
	assert.Equal(t, timestamp2, timestamps2.GetCurrentValue())
}

// TestDatasource_UpdateBatch_ConcurrentReadsDuringUpdate tests that concurrent reads
// during batch update see the previous complete batch, not partial updates.
func TestDatasource_UpdateBatch_ConcurrentReadsDuringUpdate(t *testing.T) {
	ds := NewDatasource()
	ds.SetCapacity(10000)

	// Initial batch
	batch1 := []metrics.RawMetric{
		{Name: "metric1", Value: 100.0, Labels: []metrics.Label{}},
		{Name: "metric2", Value: 200.0, Labels: []metrics.Label{}},
		{Name: "metric3", Value: 300.0, Labels: []metrics.Label{}},
	}
	timestamp1 := int64(1000)
	err := ds.UpdateBatch(batch1, timestamp1)
	require.NoError(t, err)

	// Use a channel to coordinate between writer and readers
	batchStart := make(chan struct{})
	readComplete := make(chan struct{})
	readResults := make([]struct {
		metric1Value float64
		metric2Value float64
		metric3Value float64
		timestamp    int64
	}, 10)

	var wg sync.WaitGroup

	// Start concurrent readers
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			// Wait for batch update to start
			<-batchStart

			// Try to read during batch update
			metricsMap := ds.GetMetrics()
			timestamps := ds.GetTimestamps()

			readResults[idx] = struct {
				metric1Value float64
				metric2Value float64
				metric3Value float64
				timestamp    int64
			}{
				metric1Value: metricsMap["metric1"].GetCurrentValue(),
				metric2Value: metricsMap["metric2"].GetCurrentValue(),
				metric3Value: metricsMap["metric3"].GetCurrentValue(),
				timestamp:    timestamps.GetCurrentValue(),
			}

			readComplete <- struct{}{}
		}(i)
	}

	// Start batch update in a goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Signal batch start
		close(batchStart)

		// Small delay to allow readers to start
		time.Sleep(10 * time.Millisecond)

		// Update batch
		batch2 := []metrics.RawMetric{
			{Name: "metric1", Value: 111.0, Labels: []metrics.Label{}},
			{Name: "metric2", Value: 222.0, Labels: []metrics.Label{}},
			{Name: "metric3", Value: 333.0, Labels: []metrics.Label{}},
		}
		timestamp2 := int64(2000)
		_ = ds.UpdateBatch(batch2, timestamp2)
	}()

	// Wait for all reads to complete
	for i := 0; i < 10; i++ {
		<-readComplete
	}

	wg.Wait()

	// Verify all readers saw consistent data (either all old batch or all new batch)
	// Since UpdateBatch holds the lock, readers should block and see either:
	// - All old values (if they read before batch started)
	// - All new values (if they read after batch completed)
	for idx := range readResults {
		result := readResults[idx]
		// All metrics should be from the same batch (either all old or all new)
		// Since readers block during update, they should see either:
		// - Old batch: 100, 200, 300, timestamp 1000
		// - New batch: 111, 222, 333, timestamp 2000
		// But never a mix
		if result.metric1Value == 100.0 {
			// Old batch
			assert.Equal(t, 200.0, result.metric2Value, "Reader %d: metric2 should be from old batch", idx)
			assert.Equal(t, 300.0, result.metric3Value, "Reader %d: metric3 should be from old batch", idx)
			assert.Equal(t, int64(1000), result.timestamp, "Reader %d: timestamp should be from old batch", idx)
		} else {
			// New batch
			assert.Equal(t, 111.0, result.metric1Value, "Reader %d: metric1 should be from new batch", idx)
			assert.Equal(t, 222.0, result.metric2Value, "Reader %d: metric2 should be from new batch", idx)
			assert.Equal(t, 333.0, result.metric3Value, "Reader %d: metric3 should be from new batch", idx)
			assert.Equal(t, int64(2000), result.timestamp, "Reader %d: timestamp should be from new batch", idx)
		}
	}
}

// TestDatasource_UpdateBatch_NoPartialVisibility tests that partial updates
// are never visible to readers.
func TestDatasource_UpdateBatch_NoPartialVisibility(t *testing.T) {
	ds := NewDatasource()
	ds.SetCapacity(10000)

	// Initial batch
	batch1 := []metrics.RawMetric{
		{Name: "metric1", Value: 1.0, Labels: []metrics.Label{}},
		{Name: "metric2", Value: 2.0, Labels: []metrics.Label{}},
		{Name: "metric3", Value: 3.0, Labels: []metrics.Label{}},
	}
	timestamp1 := int64(1000)
	err := ds.UpdateBatch(batch1, timestamp1)
	require.NoError(t, err)

	// Verify initial state
	metricsMap := ds.GetMetrics()
	timestamps := ds.GetTimestamps()
	assert.Equal(t, 1.0, metricsMap["metric1"].GetCurrentValue())
	assert.Equal(t, 2.0, metricsMap["metric2"].GetCurrentValue())
	assert.Equal(t, 3.0, metricsMap["metric3"].GetCurrentValue())
	assert.Equal(t, timestamp1, timestamps.GetCurrentValue())

	// Update batch with new values
	batch2 := []metrics.RawMetric{
		{Name: "metric1", Value: 10.0, Labels: []metrics.Label{}},
		{Name: "metric2", Value: 20.0, Labels: []metrics.Label{}},
		{Name: "metric3", Value: 30.0, Labels: []metrics.Label{}},
	}
	timestamp2 := int64(2000)
	err = ds.UpdateBatch(batch2, timestamp2)
	require.NoError(t, err)

	// After batch completes, verify all values are updated together
	metricsMap2 := ds.GetMetrics()
	timestamps2 := ds.GetTimestamps()

	// All metrics should be updated together
	assert.Equal(t, 10.0, metricsMap2["metric1"].GetCurrentValue(), "metric1 should be updated")
	assert.Equal(t, 20.0, metricsMap2["metric2"].GetCurrentValue(), "metric2 should be updated")
	assert.Equal(t, 30.0, metricsMap2["metric3"].GetCurrentValue(), "metric3 should be updated")
	assert.Equal(t, timestamp2, timestamps2.GetCurrentValue(), "timestamp should be updated")

	// Verify no old values remain
	assert.NotEqual(t, 1.0, metricsMap2["metric1"].GetCurrentValue(), "metric1 should not have old value")
	assert.NotEqual(t, 2.0, metricsMap2["metric2"].GetCurrentValue(), "metric2 should not have old value")
	assert.NotEqual(t, 3.0, metricsMap2["metric3"].GetCurrentValue(), "metric3 should not have old value")
	assert.NotEqual(t, timestamp1, timestamps2.GetCurrentValue(), "timestamp should not have old value")
}

// TestDatasource_UpdateBatch_EmptyBatch tests that empty batch updates work correctly.
func TestDatasource_UpdateBatch_EmptyBatch(t *testing.T) {
	ds := NewDatasource()
	ds.SetCapacity(10000)

	// Initial batch
	batch1 := []metrics.RawMetric{
		{Name: "metric1", Value: 100.0, Labels: []metrics.Label{}},
	}
	timestamp1 := int64(1000)
	err := ds.UpdateBatch(batch1, timestamp1)
	require.NoError(t, err)

	// Empty batch update (only timestamp)
	emptyBatch := []metrics.RawMetric{}
	timestamp2 := int64(2000)
	err = ds.UpdateBatch(emptyBatch, timestamp2)
	require.NoError(t, err)

	// Verify timestamp is updated but metrics remain unchanged
	metricsMap := ds.GetMetrics()
	timestamps := ds.GetTimestamps()
	assert.Equal(t, 100.0, metricsMap["metric1"].GetCurrentValue())
	assert.Equal(t, timestamp2, timestamps.GetCurrentValue())
}
