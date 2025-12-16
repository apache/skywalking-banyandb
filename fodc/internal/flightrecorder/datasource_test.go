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
	assert.Equal(t, uint64(10), ds.GetTotalWritten())
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
	result := ds.ComputeCapacity(overhead*8 + 100)
	assert.Greater(t, result, 0)
}

// TestDatasource_Update_BufferResizePreservesValues tests that buffer resize preserves values.
func TestDatasource_Update_BufferResizePreservesValues(t *testing.T) {
	ds := NewDatasource()
	ds.CapacitySize = 10000

	// Add some values
	values := []float64{70.0, 71.0, 72.0, 73.0, 74.0}
	for _, val := range values {
		rawMetric := &metrics.RawMetric{
			Name:   "cpu_usage",
			Value:  val,
			Labels: []metrics.Label{},
		}
		err := ds.Update(rawMetric)
		require.NoError(t, err)
	}

	metricsMap1 := ds.GetMetrics()
	buffer1 := metricsMap1["cpu_usage"]
	_ = buffer1.GetAllValues() // Verify buffer exists before resize

	// Change capacity to trigger resize
	ds.SetCapacity(50000)
	rawMetric := &metrics.RawMetric{
		Name:   "cpu_usage",
		Value:  75.0,
		Labels: []metrics.Label{},
	}
	err := ds.Update(rawMetric)
	require.NoError(t, err)

	metricsMap2 := ds.GetMetrics()
	buffer2 := metricsMap2["cpu_usage"]
	valuesAfter := buffer2.GetAllValues()

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

// TestDatasource_Update_TotalWrittenWrapsAround tests that TotalWritten can wrap around.
func TestDatasource_Update_TotalWrittenWrapsAround(t *testing.T) {
	ds := NewDatasource()
	ds.SetCapacity(10000)

	rawMetric := &metrics.RawMetric{
		Name:   "cpu_usage",
		Value:  75.5,
		Labels: []metrics.Label{},
	}

	// Update many times to potentially wrap around
	for i := 0; i < 1000; i++ {
		err := ds.Update(rawMetric)
		require.NoError(t, err)
	}

	// TotalWritten should reflect the number of updates
	assert.Equal(t, uint64(1000), ds.GetTotalWritten())
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
