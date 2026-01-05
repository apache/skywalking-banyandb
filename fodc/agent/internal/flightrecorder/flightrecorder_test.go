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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/fodc/agent/internal/metrics"
)

// TestNewFlightRecorder tests the creation of a new FlightRecorder instance.
func TestNewFlightRecorder(t *testing.T) {
	capacitySize := int64(1000000)
	fr := NewFlightRecorder(capacitySize)

	require.NotNil(t, fr)
	assert.Equal(t, capacitySize, fr.GetCapacitySize())
	assert.Empty(t, fr.GetDatasources())
}

// TestFlightRecorder_Update_EmptyMetrics tests Update with empty metrics slice.
func TestFlightRecorder_Update_EmptyMetrics(t *testing.T) {
	fr := NewFlightRecorder(1000000)

	err := fr.Update([]metrics.RawMetric{})

	assert.NoError(t, err)
	assert.Empty(t, fr.GetDatasources())
}

// TestFlightRecorder_Update_SingleMetric tests Update with a single metric.
func TestFlightRecorder_Update_SingleMetric(t *testing.T) {
	fr := NewFlightRecorder(1000000)

	rawMetrics := []metrics.RawMetric{
		{
			Name:   "cpu_usage",
			Value:  75.5,
			Labels: []metrics.Label{},
		},
	}

	err := fr.Update(rawMetrics)

	require.NoError(t, err)
	datasources := fr.GetDatasources()
	require.Len(t, datasources, 1)

	ds := datasources[0]
	metricsMap := ds.GetMetrics()
	assert.Contains(t, metricsMap, "cpu_usage")
	buffer := metricsMap["cpu_usage"]
	assert.Equal(t, 75.5, buffer.GetCurrentValue())

	timestamps := ds.GetTimestamps()
	assert.GreaterOrEqual(t, timestamps.Len(), 1)
}

// TestFlightRecorder_Update_MultipleMetrics tests Update with multiple metrics.
func TestFlightRecorder_Update_MultipleMetrics(t *testing.T) {
	fr := NewFlightRecorder(1000000)

	rawMetrics := []metrics.RawMetric{
		{
			Name:   "cpu_usage",
			Value:  75.5,
			Labels: []metrics.Label{},
		},
		{
			Name:   "memory_usage",
			Value:  50.0,
			Labels: []metrics.Label{},
		},
		{
			Name:   "disk_usage",
			Value:  80.0,
			Labels: []metrics.Label{},
		},
	}

	err := fr.Update(rawMetrics)

	require.NoError(t, err)
	datasources := fr.GetDatasources()
	require.Len(t, datasources, 1)

	ds := datasources[0]
	metricsMap := ds.GetMetrics()
	assert.Contains(t, metricsMap, "cpu_usage")
	assert.Contains(t, metricsMap, "memory_usage")
	assert.Contains(t, metricsMap, "disk_usage")

	assert.Equal(t, 75.5, metricsMap["cpu_usage"].GetCurrentValue())
	assert.Equal(t, 50.0, metricsMap["memory_usage"].GetCurrentValue())
	assert.Equal(t, 80.0, metricsMap["disk_usage"].GetCurrentValue())
}

// TestFlightRecorder_Update_MultipleUpdates tests multiple Update calls.
func TestFlightRecorder_Update_MultipleUpdates(t *testing.T) {
	fr := NewFlightRecorder(1000000)

	// First update
	rawMetrics1 := []metrics.RawMetric{
		{
			Name:   "cpu_usage",
			Value:  75.5,
			Labels: []metrics.Label{},
		},
	}
	err1 := fr.Update(rawMetrics1)
	require.NoError(t, err1)

	// Second update
	rawMetrics2 := []metrics.RawMetric{
		{
			Name:   "cpu_usage",
			Value:  80.0,
			Labels: []metrics.Label{},
		},
		{
			Name:   "memory_usage",
			Value:  60.0,
			Labels: []metrics.Label{},
		},
	}
	err2 := fr.Update(rawMetrics2)
	require.NoError(t, err2)

	datasources := fr.GetDatasources()
	require.Len(t, datasources, 1)

	ds := datasources[0]

	// ComputeCapacity now always returns >= 1, so buffers should work correctly
	computedCap := ds.ComputeCapacity(fr.GetCapacitySize())
	require.GreaterOrEqual(t, computedCap, 1, "ComputeCapacity should return at least 1")

	metricsMap := ds.GetMetrics()
	assert.Contains(t, metricsMap, "cpu_usage")
	assert.Contains(t, metricsMap, "memory_usage")

	// Try GetCurrentValue first (most recent value)
	cpuBuffer := metricsMap["cpu_usage"]
	memoryBuffer := metricsMap["memory_usage"]

	lastCPUValue := cpuBuffer.GetCurrentValue()
	lastMemoryValue := memoryBuffer.GetCurrentValue()

	// If GetCurrentValue returns zero, try finding the last non-zero value in GetAllValues

	// CPU usage should be updated to the latest value
	// Since ComputeCapacity always returns >= 1, values should be found
	require.NotEqual(t, float64(0), lastCPUValue, "CPU value should not be zero")
	require.NotEqual(t, float64(0), lastMemoryValue, "Memory value should not be zero")
	assert.Equal(t, 80.0, lastCPUValue)
	assert.Equal(t, 60.0, lastMemoryValue)

	// Should have timestamps for both updates
	timestamps := ds.GetTimestamps()
	assert.GreaterOrEqual(t, timestamps.Len(), 2)
}

// TestFlightRecorder_Update_WithLabels tests Update with metrics that have labels.
func TestFlightRecorder_Update_WithLabels(t *testing.T) {
	fr := NewFlightRecorder(int64(1000000))

	rawMetrics := []metrics.RawMetric{
		{
			Name:  "http_requests_total",
			Value: 100.0,
			Labels: []metrics.Label{
				{Name: "method", Value: "GET"},
				{Name: "status", Value: "200"},
			},
		},
		{
			Name:  "http_requests_total",
			Value: 50.0,
			Labels: []metrics.Label{
				{Name: "method", Value: "POST"},
				{Name: "status", Value: "201"},
			},
		},
	}

	err := fr.Update(rawMetrics)

	require.NoError(t, err)
	datasources := fr.GetDatasources()
	require.Len(t, datasources, 1)

	ds := datasources[0]
	metricsMap := ds.GetMetrics()
	// Should create separate metric keys for different label combinations
	assert.GreaterOrEqual(t, len(metricsMap), 2)
}

// TestFlightRecorder_Update_WithDescriptions tests Update with metrics that have descriptions.
func TestFlightRecorder_Update_WithDescriptions(t *testing.T) {
	fr := NewFlightRecorder(int64(1000000))

	rawMetrics := []metrics.RawMetric{
		{
			Name:   "cpu_usage",
			Value:  75.5,
			Labels: []metrics.Label{},
			Desc:   "CPU usage percentage",
		},
	}

	err := fr.Update(rawMetrics)

	require.NoError(t, err)
	datasources := fr.GetDatasources()
	require.Len(t, datasources, 1)

	ds := datasources[0]
	descriptions := ds.GetDescriptions()
	assert.Equal(t, "CPU usage percentage", descriptions["cpu_usage"])
}

// TestFlightRecorder_GetDatasources_ReturnsCopy tests that GetDatasources returns a copy.
func TestFlightRecorder_GetDatasources_ReturnsCopy(t *testing.T) {
	fr := NewFlightRecorder(int64(1000000))

	rawMetrics := []metrics.RawMetric{
		{
			Name:   "cpu_usage",
			Value:  75.5,
			Labels: []metrics.Label{},
		},
	}

	err := fr.Update(rawMetrics)
	require.NoError(t, err)

	datasources1 := fr.GetDatasources()
	datasources2 := fr.GetDatasources()

	// Should return copies (different slice instances)
	// Check that they are different slice instances by verifying they don't share the same underlying array
	require.Len(t, datasources1, 1)
	require.Len(t, datasources2, 1)
	// Modify one and verify the other is not affected
	datasources1 = append(datasources1, NewDatasource())
	datasources3 := fr.GetDatasources()
	// datasources3 should still have only 1 element (original wasn't modified)
	require.Len(t, datasources3, 1)
	// But should have the same content for the first element
	assert.Equal(t, datasources1[0], datasources3[0])
}

// TestFlightRecorder_SetCapacitySize tests setting capacity size.
func TestFlightRecorder_SetCapacitySize(t *testing.T) {
	fr := NewFlightRecorder(int64(1000000))

	rawMetrics := []metrics.RawMetric{
		{
			Name:   "cpu_usage",
			Value:  75.5,
			Labels: []metrics.Label{},
		},
	}

	err := fr.Update(rawMetrics)
	require.NoError(t, err)

	// Get initial capacity
	initialCapacity := fr.GetCapacitySize()
	assert.Equal(t, int64(1000000), initialCapacity)

	// Set new capacity
	fr.SetCapacitySize(int64(2000000))
	newCapacity := fr.GetCapacitySize()
	assert.Equal(t, int64(2000000), newCapacity)

	// Verify datasource capacity was updated
	datasources := fr.GetDatasources()
	require.Len(t, datasources, 1)
	assert.Equal(t, int64(2000000), datasources[0].CapacitySize)
}

// TestFlightRecorder_SetCapacitySize_MultipleDatasources tests SetCapacitySize with multiple datasources.
func TestFlightRecorder_SetCapacitySize_MultipleDatasources(t *testing.T) {
	fr := NewFlightRecorder(int64(1000000))

	// Create multiple updates to potentially create multiple datasources
	// (though current implementation uses only the first datasource)
	rawMetrics1 := []metrics.RawMetric{
		{
			Name:   "cpu_usage",
			Value:  75.5,
			Labels: []metrics.Label{},
		},
	}
	err1 := fr.Update(rawMetrics1)
	require.NoError(t, err1)

	rawMetrics2 := []metrics.RawMetric{
		{
			Name:   "memory_usage",
			Value:  50.0,
			Labels: []metrics.Label{},
		},
	}
	err2 := fr.Update(rawMetrics2)
	require.NoError(t, err2)

	// Set new capacity
	fr.SetCapacitySize(int64(3000000))

	// Verify all datasources have updated capacity
	datasources := fr.GetDatasources()
	for _, ds := range datasources {
		assert.Equal(t, int64(3000000), ds.CapacitySize)
	}
}

// TestFlightRecorder_GetCapacitySize tests getting capacity size.
func TestFlightRecorder_GetCapacitySize(t *testing.T) {
	fr := NewFlightRecorder(int64(5000000))

	capacity := fr.GetCapacitySize()
	assert.Equal(t, int64(5000000), capacity)

	// Change capacity
	fr.SetCapacitySize(int64(10000000))
	newCapacity := fr.GetCapacitySize()
	assert.Equal(t, int64(10000000), newCapacity)
}

// TestFlightRecorder_Update_ConcurrentUpdates tests concurrent Update calls.
func TestFlightRecorder_Update_ConcurrentUpdates(t *testing.T) {
	fr := NewFlightRecorder(int64(1000000))

	var wg sync.WaitGroup
	numGoroutines := 10

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(metricID int) {
			defer wg.Done()
			rawMetrics := []metrics.RawMetric{
				{
					Name:   "cpu_usage",
					Value:  float64(metricID),
					Labels: []metrics.Label{},
				},
			}
			_ = fr.Update(rawMetrics)
		}(i)
	}

	wg.Wait()

	// Should have successfully updated
	datasources := fr.GetDatasources()
	assert.GreaterOrEqual(t, len(datasources), 1)

	ds := datasources[0]
	metricsMap := ds.GetMetrics()
	assert.Contains(t, metricsMap, "cpu_usage")
}

// TestFlightRecorder_Update_ConcurrentCapacityChanges tests concurrent updates with capacity changes.
func TestFlightRecorder_Update_ConcurrentCapacityChanges(t *testing.T) {
	fr := NewFlightRecorder(int64(1000000))

	var wg sync.WaitGroup
	numGoroutines := 5

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			// Change capacity in some goroutines
			if id%2 == 0 {
				fr.SetCapacitySize(int64(2000000))
			}
			rawMetrics := []metrics.RawMetric{
				{
					Name:   "cpu_usage",
					Value:  float64(id),
					Labels: []metrics.Label{},
				},
			}
			_ = fr.Update(rawMetrics)
		}(i)
	}

	wg.Wait()

	// Should have successfully updated
	datasources := fr.GetDatasources()
	assert.GreaterOrEqual(t, len(datasources), 1)

	ds := datasources[0]
	metricsMap := ds.GetMetrics()
	assert.Contains(t, metricsMap, "cpu_usage")
}

// TestFlightRecorder_Update_TimestampAdded tests that timestamps are added correctly.
func TestFlightRecorder_Update_TimestampAdded(t *testing.T) {
	fr := NewFlightRecorder(int64(1000000))

	rawMetrics := []metrics.RawMetric{
		{
			Name:   "cpu_usage",
			Value:  75.5,
			Labels: []metrics.Label{},
		},
	}

	beforeTime := time.Now().Unix()
	err := fr.Update(rawMetrics)
	require.NoError(t, err)
	afterTime := time.Now().Unix()

	datasources := fr.GetDatasources()
	require.Len(t, datasources, 1)

	ds := datasources[0]

	// ComputeCapacity always returns >= 1, so buffers should work correctly
	computedCap := ds.ComputeCapacity(fr.GetCapacitySize())
	require.GreaterOrEqual(t, computedCap, 1, "ComputeCapacity should return at least 1")

	// Get timestamps - this returns a copy
	timestamps := ds.GetTimestamps()
	require.NotNil(t, timestamps)
	require.GreaterOrEqual(t, timestamps.Len(), 1)

	// Get all timestamp values and filter out zeros
	// GetAllValues() returns all values in buffer array, including zeros
	allTimestamps := timestamps.GetAllValues()
	require.NotEmpty(t, allTimestamps)

	// Filter out zero values to get actual timestamps
	nonZeroTimestamps := make([]int64, 0)
	for _, ts := range allTimestamps {
		if ts != 0 {
			nonZeroTimestamps = append(nonZeroTimestamps, ts)
		}
	}

	// Since ComputeCapacity always returns >= 1, we should have at least one timestamp
	require.NotEmpty(t, nonZeroTimestamps, "should have at least one non-zero timestamp")

	// Get the most recent timestamp (last in the filtered array)
	lastTimestamp := nonZeroTimestamps[len(nonZeroTimestamps)-1]

	// Timestamp should be in valid range
	assert.GreaterOrEqual(t, lastTimestamp, beforeTime)
	assert.LessOrEqual(t, lastTimestamp, afterTime)
}

// TestFlightRecorder_Update_CapacityApplied tests that capacity is applied after metrics are added.
func TestFlightRecorder_Update_CapacityApplied(t *testing.T) {
	fr := NewFlightRecorder(int64(1000000))

	rawMetrics := []metrics.RawMetric{
		{
			Name:   "cpu_usage",
			Value:  75.5,
			Labels: []metrics.Label{},
		},
		{
			Name:   "memory_usage",
			Value:  50.0,
			Labels: []metrics.Label{},
		},
	}

	err := fr.Update(rawMetrics)
	require.NoError(t, err)

	datasources := fr.GetDatasources()
	require.Len(t, datasources, 1)

	ds := datasources[0]
	// Capacity should be set
	assert.Equal(t, int64(1000000), ds.CapacitySize)

	// Metrics should exist
	metricsMap := ds.GetMetrics()
	assert.Contains(t, metricsMap, "cpu_usage")
	assert.Contains(t, metricsMap, "memory_usage")
}

// TestFlightRecorder_Update_ManyMetrics tests Update with many metrics.
func TestFlightRecorder_Update_ManyMetrics(t *testing.T) {
	fr := NewFlightRecorder(int64(1000000))

	rawMetrics := make([]metrics.RawMetric, 100)
	for i := 0; i < 100; i++ {
		rawMetrics[i] = metrics.RawMetric{
			Name:   fmt.Sprintf("metric_%d", i%10),
			Value:  float64(i),
			Labels: []metrics.Label{},
		}
	}

	err := fr.Update(rawMetrics)

	require.NoError(t, err)
	datasources := fr.GetDatasources()
	require.Len(t, datasources, 1)

	ds := datasources[0]
	metricsMap := ds.GetMetrics()
	assert.GreaterOrEqual(t, len(metricsMap), 10) // At least 10 different metric names
}

// TestFlightRecorder_Update_ThreadSafety tests thread safety of Update and GetDatasources.
func TestFlightRecorder_Update_ThreadSafety(t *testing.T) {
	fr := NewFlightRecorder(int64(1000000))

	var wg sync.WaitGroup
	numGoroutines := 20

	// Concurrent updates
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			rawMetrics := []metrics.RawMetric{
				{
					Name:   "cpu_usage",
					Value:  float64(id),
					Labels: []metrics.Label{},
				},
			}
			_ = fr.Update(rawMetrics)
		}(i)
	}

	// Concurrent reads
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = fr.GetDatasources()
			_ = fr.GetCapacitySize()
		}()
	}

	wg.Wait()

	// Should have successfully updated
	datasources := fr.GetDatasources()
	assert.GreaterOrEqual(t, len(datasources), 1)
}

// TestFlightRecorder_Update_ZeroCapacity tests Update with zero capacity.
func TestFlightRecorder_Update_ZeroCapacity(t *testing.T) {
	fr := NewFlightRecorder(0)

	rawMetrics := []metrics.RawMetric{
		{
			Name:   "cpu_usage",
			Value:  75.5,
			Labels: []metrics.Label{},
		},
	}

	err := fr.Update(rawMetrics)

	// Should not error, but capacity will be 0
	assert.NoError(t, err)
	datasources := fr.GetDatasources()
	require.Len(t, datasources, 1)
	assert.Equal(t, int64(0), datasources[0].CapacitySize)
}

// TestFlightRecorder_Update_ReusesFirstDatasource tests that Update reuses the first datasource.
func TestFlightRecorder_Update_ReusesFirstDatasource(t *testing.T) {
	fr := NewFlightRecorder(int64(1000000))

	rawMetrics1 := []metrics.RawMetric{
		{
			Name:   "cpu_usage",
			Value:  75.5,
			Labels: []metrics.Label{},
		},
	}
	err1 := fr.Update(rawMetrics1)
	require.NoError(t, err1)

	datasources1 := fr.GetDatasources()
	require.Len(t, datasources1, 1)
	firstDS := datasources1[0]

	rawMetrics2 := []metrics.RawMetric{
		{
			Name:   "memory_usage",
			Value:  50.0,
			Labels: []metrics.Label{},
		},
	}
	err2 := fr.Update(rawMetrics2)
	require.NoError(t, err2)

	datasources2 := fr.GetDatasources()
	require.Len(t, datasources2, 1)

	// Should reuse the same datasource
	assert.Equal(t, firstDS, datasources2[0])
}
