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

func TestNewFlightRecorder(t *testing.T) {
	capacitySize := 1000
	fr := NewFlightRecorder(capacitySize)

	assert.NotNil(t, fr)
	assert.Equal(t, capacitySize, fr.GetCapacitySize())
	assert.Empty(t, fr.GetDatasources())
}

func TestFlightRecorder_Update_EmptyMetrics(t *testing.T) {
	fr := NewFlightRecorder(1000)

	err := fr.Update([]metrics.RawMetric{})

	require.NoError(t, err)
	assert.Empty(t, fr.GetDatasources())
}

func TestFlightRecorder_Update_NewMetrics(t *testing.T) {
	fr := NewFlightRecorder(1000000)

	rawMetrics := []metrics.RawMetric{
		{
			Name:   "cpu_usage",
			Value:  75.5,
			Labels: []metrics.Label{},
			Desc:   "CPU usage percentage",
		},
		{
			Name:   "memory_usage",
			Value:  50.0,
			Labels: []metrics.Label{},
			Desc:   "Memory usage percentage",
		},
	}

	err := fr.Update(rawMetrics)

	require.NoError(t, err)
	datasources := fr.GetDatasources()
	require.Len(t, datasources, 1)
	assert.NotNil(t, datasources[0])
}

func TestFlightRecorder_Update_ExistingMetrics(t *testing.T) {
	fr := NewFlightRecorder(1000000)

	rawMetrics1 := []metrics.RawMetric{
		{
			Name:   "cpu_usage",
			Value:  75.5,
			Labels: []metrics.Label{},
			Desc:   "CPU usage percentage",
		},
	}

	err1 := fr.Update(rawMetrics1)
	require.NoError(t, err1)

	rawMetrics2 := []metrics.RawMetric{
		{
			Name:   "cpu_usage",
			Value:  80.0,
			Labels: []metrics.Label{},
			Desc:   "CPU usage percentage",
		},
	}

	err2 := fr.Update(rawMetrics2)
	require.NoError(t, err2)

	datasources := fr.GetDatasources()
	require.Len(t, datasources, 1)
	metricsMap := datasources[0].GetMetrics()
	assert.Contains(t, metricsMap, "cpu_usage")
}

func TestFlightRecorder_Update_MetricsWithLabels(t *testing.T) {
	fr := NewFlightRecorder(1000000)

	rawMetrics := []metrics.RawMetric{
		{
			Name:   "http_requests_total",
			Value:  100.0,
			Labels: []metrics.Label{{Name: "method", Value: "GET"}},
			Desc:   "Total HTTP requests",
		},
		{
			Name:   "http_requests_total",
			Value:  200.0,
			Labels: []metrics.Label{{Name: "method", Value: "POST"}},
			Desc:   "Total HTTP requests",
		},
	}

	err := fr.Update(rawMetrics)

	require.NoError(t, err)
	datasources := fr.GetDatasources()
	require.Len(t, datasources, 1)
	metricsMap := datasources[0].GetMetrics()
	assert.GreaterOrEqual(t, len(metricsMap), 2)
}

func TestFlightRecorder_Update_TimestampAdded(t *testing.T) {
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
	timestamps := datasources[0].GetTimestamps()
	assert.Greater(t, timestamps.Len(), 0)
}

func TestFlightRecorder_SetCapacitySize(t *testing.T) {
	fr := NewFlightRecorder(1000)

	fr.SetCapacitySize(2000)

	assert.Equal(t, 2000, fr.GetCapacitySize())
}

func TestFlightRecorder_GetCapacitySize(t *testing.T) {
	capacitySize := 5000
	fr := NewFlightRecorder(capacitySize)

	result := fr.GetCapacitySize()

	assert.Equal(t, capacitySize, result)
}

func TestFlightRecorder_GetDatasources(t *testing.T) {
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
	assert.NotNil(t, datasources[0])
}

func TestFlightRecorder_ConcurrentUpdates(t *testing.T) {
	fr := NewFlightRecorder(1000000)

	var wg sync.WaitGroup
	numGoroutines := 10
	metricsPerGoroutine := 10

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			rawMetrics := make([]metrics.RawMetric, metricsPerGoroutine)
			for j := 0; j < metricsPerGoroutine; j++ {
				rawMetrics[j] = metrics.RawMetric{
					Name:   "cpu_usage",
					Value:  float64(goroutineID*metricsPerGoroutine + j),
					Labels: []metrics.Label{},
				}
			}
			_ = fr.Update(rawMetrics)
		}(i)
	}

	wg.Wait()

	datasources := fr.GetDatasources()
	require.Len(t, datasources, 1)
	// Verify that updates were successful
	assert.Greater(t, datasources[0].GetTotalWritten(), uint64(0))
}

func TestFlightRecorder_HistogramStorage(t *testing.T) {
	fr := NewFlightRecorder(1000000)

	histogramMetrics := []metrics.RawMetric{
		{
			Name:   "http_request_duration_seconds_bucket",
			Value:  100.0,
			Labels: []metrics.Label{{Name: "le", Value: "0.1"}},
			Desc:   "Request duration histogram",
		},
		{
			Name:   "http_request_duration_seconds_bucket",
			Value:  200.0,
			Labels: []metrics.Label{{Name: "le", Value: "0.5"}},
			Desc:   "Request duration histogram",
		},
		{
			Name:   "http_request_duration_seconds_bucket",
			Value:  300.0,
			Labels: []metrics.Label{{Name: "le", Value: "+Inf"}},
			Desc:   "Request duration histogram",
		},
		{
			Name:   "http_request_duration_seconds_count",
			Value:  300.0,
			Labels: []metrics.Label{},
			Desc:   "Request duration histogram",
		},
		{
			Name:   "http_request_duration_seconds_sum",
			Value:  45.2,
			Labels: []metrics.Label{},
			Desc:   "Request duration histogram",
		},
	}

	err := fr.Update(histogramMetrics)
	require.NoError(t, err)

	datasources := fr.GetDatasources()
	require.Len(t, datasources, 1)
	metricsMap := datasources[0].GetMetrics()
	assert.GreaterOrEqual(t, len(metricsMap), 5)
}

func TestFlightRecorder_HistogramRetrieval(t *testing.T) {
	fr := NewFlightRecorder(1000000)

	histogramMetrics := []metrics.RawMetric{
		{
			Name:   "http_request_duration_seconds_bucket",
			Value:  100.0,
			Labels: []metrics.Label{{Name: "le", Value: "0.1"}},
		},
		{
			Name:   "http_request_duration_seconds_bucket",
			Value:  200.0,
			Labels: []metrics.Label{{Name: "le", Value: "0.5"}},
		},
	}

	err := fr.Update(histogramMetrics)
	require.NoError(t, err)

	datasources := fr.GetDatasources()
	require.Len(t, datasources, 1)
	metricsMap := datasources[0].GetMetrics()

	// Verify histogram buckets can be retrieved
	foundBuckets := 0
	for key, buffer := range metricsMap {
		if len(key) > 0 && buffer.Len() > 0 {
			foundBuckets++
			currentValue := buffer.GetCurrentValue()
			assert.Greater(t, currentValue, 0.0)
		}
	}
	assert.GreaterOrEqual(t, foundBuckets, 2)
}

func TestFlightRecorder_ErrorHandling_NilMetric(t *testing.T) {
	fr := NewFlightRecorder(1000)

	// This test verifies that the FlightRecorder handles errors from Datasource.Update
	// Since Update takes []RawMetric, we can't pass nil directly, but we can test
	// with empty slice which should be handled gracefully
	err := fr.Update([]metrics.RawMetric{})

	require.NoError(t, err)
}

func TestFlightRecorder_MultipleUpdateCycles(t *testing.T) {
	fr := NewFlightRecorder(1000000)

	for cycle := 0; cycle < 5; cycle++ {
		rawMetrics := []metrics.RawMetric{
			{
				Name:   "cpu_usage",
				Value:  float64(75 + cycle),
				Labels: []metrics.Label{},
			},
		}
		err := fr.Update(rawMetrics)
		require.NoError(t, err)
		time.Sleep(10 * time.Millisecond) // Small delay to ensure different timestamps
	}

	datasources := fr.GetDatasources()
	require.Len(t, datasources, 1)
	timestamps := datasources[0].GetTimestamps()
	assert.GreaterOrEqual(t, timestamps.Len(), 1)
}

func TestFlightRecorder_CapacityPropagation(t *testing.T) {
	fr := NewFlightRecorder(1000)

	rawMetrics := []metrics.RawMetric{
		{
			Name:   "cpu_usage",
			Value:  75.5,
			Labels: []metrics.Label{},
		},
	}

	err := fr.Update(rawMetrics)
	require.NoError(t, err)

	// Change capacity
	fr.SetCapacitySize(2000)

	datasources := fr.GetDatasources()
	require.Len(t, datasources, 1)
	assert.Equal(t, 2000, datasources[0].CapacitySize)
}

func TestFlightRecorder_DescriptionStorage(t *testing.T) {
	fr := NewFlightRecorder(1000000)

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
	descriptions := datasources[0].GetDescriptions()
	assert.Equal(t, "CPU usage percentage", descriptions["cpu_usage"])
}

func TestFlightRecorder_MultipleMetricsSameNameDifferentLabels(t *testing.T) {
	fr := NewFlightRecorder(1000000)

	rawMetrics := []metrics.RawMetric{
		{
			Name:   "http_requests_total",
			Value:  100.0,
			Labels: []metrics.Label{{Name: "method", Value: "GET"}},
		},
		{
			Name:   "http_requests_total",
			Value:  200.0,
			Labels: []metrics.Label{{Name: "method", Value: "POST"}},
		},
		{
			Name:   "http_requests_total",
			Value:  150.0,
			Labels: []metrics.Label{{Name: "method", Value: "PUT"}},
		},
	}

	err := fr.Update(rawMetrics)
	require.NoError(t, err)

	datasources := fr.GetDatasources()
	require.Len(t, datasources, 1)
	metricsMap := datasources[0].GetMetrics()
	assert.GreaterOrEqual(t, len(metricsMap), 3)
}

func TestFlightRecorder_LargeNumberOfMetrics(t *testing.T) {
	fr := NewFlightRecorder(10000000) // Large capacity

	rawMetrics := make([]metrics.RawMetric, 100)
	for i := 0; i < 100; i++ {
		rawMetrics[i] = metrics.RawMetric{
			Name:   "metric_name",
			Value:  float64(i),
			Labels: []metrics.Label{{Name: "index", Value: string(rune('0' + i%10))}},
		}
	}

	err := fr.Update(rawMetrics)
	require.NoError(t, err)

	datasources := fr.GetDatasources()
	require.Len(t, datasources, 1)
	metricsMap := datasources[0].GetMetrics()
	assert.GreaterOrEqual(t, len(metricsMap), 10) // At least 10 different label combinations
}

func TestFlightRecorder_ZeroCapacity(t *testing.T) {
	fr := NewFlightRecorder(0)

	rawMetrics := []metrics.RawMetric{
		{
			Name:   "cpu_usage",
			Value:  75.5,
			Labels: []metrics.Label{},
		},
	}

	err := fr.Update(rawMetrics)
	// Should handle zero capacity size gracefully
	// The behavior depends on implementation, but should not panic
	if err != nil {
		assert.Contains(t, err.Error(), "capacity size")
	}
}

func TestFlightRecorder_ReuseSameDatasource(t *testing.T) {
	fr := NewFlightRecorder(1000000)

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
	ds1 := datasources1[0]

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
	ds2 := datasources2[0]

	// Should reuse the same datasource
	assert.Equal(t, ds1, ds2)
}

// RingBuffer Tests.
func TestRingBuffer_NewRingBuffer(t *testing.T) {
	rb := NewRingBuffer[float64]()

	assert.NotNil(t, rb)
	assert.Equal(t, 0, rb.Len())
	assert.Equal(t, 0, rb.Cap())
	assert.Equal(t, 0, rb.next)
}

func TestRingBuffer_Add_Float64(t *testing.T) {
	rb := NewRingBuffer[float64]()
	capacity := 5

	// Set capacity first, then add values
	rb.SetCapacity(capacity)
	rb.Add(1.5)
	rb.Add(2.5)
	rb.Add(3.5)

	// Len() returns the slice length (capacity), not the number of values added
	assert.Equal(t, capacity, rb.Len())
	assert.Equal(t, capacity, rb.Cap())
	// Verify values are stored correctly
	assert.Equal(t, 1.5, rb.Get(0))
	assert.Equal(t, 2.5, rb.Get(1))
	assert.Equal(t, 3.5, rb.Get(2))
	// Verify current value is the most recent
	assert.Equal(t, 3.5, rb.GetCurrentValue())
}

func TestRingBuffer_Add_Int64(t *testing.T) {
	rb := NewRingBuffer[int64]()
	capacity := 5

	// Set capacity first, then add values
	rb.SetCapacity(capacity)
	rb.Add(100)
	rb.Add(200)
	rb.Add(300)

	// Len() returns the slice length (capacity), not the number of values added
	assert.Equal(t, capacity, rb.Len())
	assert.Equal(t, capacity, rb.Cap())
	// Verify values are stored correctly
	assert.Equal(t, int64(100), rb.Get(0))
	assert.Equal(t, int64(200), rb.Get(1))
	assert.Equal(t, int64(300), rb.Get(2))
	// Verify current value is the most recent
	assert.Equal(t, int64(300), rb.GetCurrentValue())
}

func TestRingBuffer_CircularOverwrite(t *testing.T) {
	rb := NewRingBuffer[float64]()
	capacity := 3
	rb.SetCapacity(capacity)

	// Fill buffer to capacity
	rb.Add(1.0)
	rb.Add(2.0)
	rb.Add(3.0)

	assert.Equal(t, capacity, rb.Len())
	assert.Equal(t, 1.0, rb.Get(0))
	assert.Equal(t, 2.0, rb.Get(1))
	assert.Equal(t, 3.0, rb.Get(2))

	// Add one more - should overwrite the oldest (FIFO)
	rb.Add(4.0)

	assert.Equal(t, capacity, rb.Len())
	// Oldest value (1.0) should be overwritten
	allValues := rb.GetAllValues()
	assert.Contains(t, allValues, 2.0)
	assert.Contains(t, allValues, 3.0)
	assert.Contains(t, allValues, 4.0)
	assert.NotContains(t, allValues, 1.0)
}

func TestRingBuffer_CircularOverwrite_Multiple(t *testing.T) {
	rb := NewRingBuffer[int64]()
	capacity := 3
	rb.SetCapacity(capacity)

	// Fill buffer
	rb.Add(10)
	rb.Add(20)
	rb.Add(30)

	// Add multiple values that exceed capacity
	rb.Add(40)
	rb.Add(50)
	rb.Add(60)

	allValues := rb.GetAllValues()
	assert.Equal(t, capacity, len(allValues))
	// Should contain the most recent values
	assert.Contains(t, allValues, int64(40))
	assert.Contains(t, allValues, int64(50))
	assert.Contains(t, allValues, int64(60))
	// Oldest values should be gone
	assert.NotContains(t, allValues, int64(10))
	assert.NotContains(t, allValues, int64(20))
	assert.NotContains(t, allValues, int64(30))
}

func TestRingBuffer_GetCurrentValue(t *testing.T) {
	rb := NewRingBuffer[float64]()
	rb.SetCapacity(10)
	rb.Add(1.0)
	assert.Equal(t, 1.0, rb.GetCurrentValue())

	rb.Add(2.0)
	assert.Equal(t, 2.0, rb.GetCurrentValue())

	rb.Add(3.0)
	assert.Equal(t, 3.0, rb.GetCurrentValue())
}

func TestRingBuffer_GetCurrentValue_Empty(t *testing.T) {
	rb := NewRingBuffer[float64]()

	result := rb.GetCurrentValue()

	var zero float64
	assert.Equal(t, zero, result)
}

func TestRingBuffer_GetCurrentValue_AfterOverwrite(t *testing.T) {
	rb := NewRingBuffer[float64]()
	rb.SetCapacity(3)
	rb.Add(1.0)
	rb.Add(2.0)
	rb.Add(3.0)
	rb.Add(4.0) // Overwrites 1.0

	assert.Equal(t, 4.0, rb.GetCurrentValue())
}

func TestRingBuffer_GetAllValues(t *testing.T) {
	rb := NewRingBuffer[float64]()
	capacity := 5
	rb.SetCapacity(capacity)

	rb.Add(1.0)
	rb.Add(2.0)
	rb.Add(3.0)

	allValues := rb.GetAllValues()

	// GetAllValues returns all values in the buffer (capacity length)
	require.Len(t, allValues, capacity)
	// GetAllValues returns values in circular order starting from next position
	// Verify the values we added are present (they may be at different indices)
	assert.Contains(t, allValues, 1.0)
	assert.Contains(t, allValues, 2.0)
	assert.Contains(t, allValues, 3.0)
	// Should contain zero values for remaining slots
	var zero float64
	zeroCount := 0
	for _, val := range allValues {
		if val == zero {
			zeroCount++
		}
	}
	assert.Equal(t, 2, zeroCount) // Two zero slots
}

func TestRingBuffer_GetAllValues_Empty(t *testing.T) {
	rb := NewRingBuffer[float64]()

	allValues := rb.GetAllValues()

	assert.Nil(t, allValues)
}

func TestRingBuffer_GetAllValues_OrderAfterOverwrite(t *testing.T) {
	rb := NewRingBuffer[int64]()
	capacity := 3
	rb.SetCapacity(capacity)

	rb.Add(10)
	rb.Add(20)
	rb.Add(30)
	rb.Add(40) // Overwrites 10

	allValues := rb.GetAllValues()

	// GetAllValues returns all values in the buffer (capacity length)
	require.Len(t, allValues, capacity)
	// Should be in order: oldest to newest (circular order)
	// After overwrite, the order depends on the circular buffer implementation
	// The most recent 3 values should be present
	assert.Contains(t, allValues, int64(20))
	assert.Contains(t, allValues, int64(30))
	assert.Contains(t, allValues, int64(40))
	// Oldest value should be overwritten
	assert.NotContains(t, allValues, int64(10))
}

func TestRingBuffer_Get_OutOfBounds(t *testing.T) {
	rb := NewRingBuffer[float64]()
	rb.SetCapacity(10)
	rb.Add(1.0)

	var zero float64
	assert.Equal(t, zero, rb.Get(-1))
	assert.Equal(t, zero, rb.Get(10))
}

func TestRingBuffer_Add_ZeroCapacity(t *testing.T) {
	rb := NewRingBuffer[float64]()

	rb.Add(1.0)

	assert.Equal(t, 0, rb.Len())
	assert.Equal(t, 0, rb.Cap())
}

func TestRingBuffer_Add_NegativeCapacity(t *testing.T) {
	rb := NewRingBuffer[float64]()

	rb.Add(1.0)

	assert.Equal(t, 0, rb.Len())
}

func TestRingBuffer_Resize_Grow(t *testing.T) {
	rb := NewRingBuffer[float64]()
	initialCapacity := 3

	rb.SetCapacity(initialCapacity)
	rb.Add(1.0)
	rb.Add(2.0)
	rb.Add(3.0)

	assert.Equal(t, initialCapacity, rb.Cap())

	// Grow capacity
	newCapacity := 5
	rb.SetCapacity(newCapacity)
	rb.Add(4.0)

	assert.Equal(t, newCapacity, rb.Cap())
	// Len() returns the slice length (capacity), not the number of values added
	assert.Equal(t, newCapacity, rb.Len())
	// When growing, values are copied but GetAllValues returns values in circular order
	// The exact values preserved depend on the implementation
	allValues := rb.GetAllValues()
	require.Len(t, allValues, newCapacity)
	// Should contain the most recent value
	assert.Contains(t, allValues, 4.0)
	// Should contain some of the previous values (implementation may not preserve all)
	// At minimum, should contain values 2 and 3 (the most recent before grow)
	assert.Contains(t, allValues, 2.0)
	assert.Contains(t, allValues, 3.0)
	// Verify current value is the most recent
	assert.Equal(t, 4.0, rb.GetCurrentValue())
}

func TestRingBuffer_Resize_Shrink(t *testing.T) {
	rb := NewRingBuffer[int64]()
	initialCapacity := 5

	rb.SetCapacity(initialCapacity)
	rb.Add(10)
	rb.Add(20)
	rb.Add(30)
	rb.Add(40)
	rb.Add(50)

	assert.Equal(t, initialCapacity, rb.Cap())
	assert.Equal(t, initialCapacity, rb.Len())

	// Shrink capacity - should keep most recent values (FIFO)
	newCapacity := 3
	rb.SetCapacity(newCapacity)
	rb.Add(60)

	assert.Equal(t, newCapacity, rb.Cap())
	assert.Equal(t, newCapacity, rb.Len())
	allValues := rb.GetAllValues()
	// Should contain the most recent values
	assert.Contains(t, allValues, int64(40))
	assert.Contains(t, allValues, int64(50))
	assert.Contains(t, allValues, int64(60))
	// Oldest values should be removed
	assert.NotContains(t, allValues, int64(10))
	assert.NotContains(t, allValues, int64(20))
	assert.NotContains(t, allValues, int64(30))
}

func TestRingBuffer_Resize_SameCapacity(t *testing.T) {
	rb := NewRingBuffer[float64]()
	capacity := 10

	rb.SetCapacity(capacity)
	rb.Add(1.0)
	rb.Add(2.0)
	rb.Add(3.0)

	initialLen := rb.Len()
	initialCap := rb.Cap()

	rb.Add(4.0)

	// Capacity and length should remain the same when capacity doesn't change
	assert.Equal(t, initialCap, rb.Cap())
	assert.Equal(t, initialLen, rb.Len())
	// Verify the new value was added
	assert.Equal(t, 4.0, rb.GetCurrentValue())
}

// MetricRingBuffer and TimestampRingBuffer Tests.

func TestNewMetricRingBuffer(t *testing.T) {
	mrb := NewMetricRingBuffer()

	assert.NotNil(t, mrb)
	assert.Equal(t, 0, mrb.Len())
}

func TestNewTimestampRingBuffer(t *testing.T) {
	trb := NewTimestampRingBuffer()

	assert.NotNil(t, trb)
	assert.Equal(t, 0, trb.Len())
}

func TestUpdateMetricRingBuffer(t *testing.T) {
	mrb := NewMetricRingBuffer()
	capacity := 5

	// Set capacity first, then add values
	mrb.SetCapacity(capacity)
	UpdateMetricRingBuffer(mrb, 75.5)
	UpdateMetricRingBuffer(mrb, 80.0)

	// Len() returns the slice length (capacity), not the number of values added
	assert.Equal(t, capacity, mrb.Len())
	assert.Equal(t, capacity, mrb.Cap())
	// Verify values are stored correctly
	assert.Equal(t, 75.5, mrb.Get(0))
	assert.Equal(t, 80.0, mrb.Get(1))
	// Verify current value is the most recent
	assert.Equal(t, 80.0, mrb.GetCurrentValue())
}

func TestUpdateTimestampRingBuffer(t *testing.T) {
	trb := NewTimestampRingBuffer()
	capacity := 5

	timestamp1 := int64(1000)
	timestamp2 := int64(2000)

	// Set capacity first, then add values
	trb.SetCapacity(capacity)
	UpdateTimestampRingBuffer(trb, timestamp1)
	UpdateTimestampRingBuffer(trb, timestamp2)

	// Len() returns the slice length (capacity), not the number of values added
	assert.Equal(t, capacity, trb.Len())
	assert.Equal(t, capacity, trb.Cap())
	// Verify values are stored correctly
	assert.Equal(t, timestamp1, trb.Get(0))
	assert.Equal(t, timestamp2, trb.Get(1))
	// Verify current value is the most recent
	assert.Equal(t, timestamp2, trb.GetCurrentValue())
}

// Datasource.ComputeCapacity Tests.

func TestDatasource_ComputeCapacity_ZeroCapacity(t *testing.T) {
	ds := NewDatasource()

	result := ds.ComputeCapacity(0)

	assert.Equal(t, 0, result)
}

func TestDatasource_ComputeCapacity_NegativeCapacity(t *testing.T) {
	ds := NewDatasource()

	result := ds.ComputeCapacity(-100)

	assert.Equal(t, 0, result)
}

func TestDatasource_ComputeCapacity_NoMetrics(t *testing.T) {
	ds := NewDatasource()

	result := ds.ComputeCapacity(10000)

	assert.Equal(t, defaultCapacity, result)
}

func TestDatasource_ComputeCapacity_WithMetrics(t *testing.T) {
	ds := NewDatasource()

	// Add a metric to create a metric in the map
	rawMetric := &metrics.RawMetric{
		Name:   "cpu_usage",
		Value:  75.5,
		Labels: []metrics.Label{},
	}
	_ = ds.Update(rawMetric)

	capacitySize := 100000
	result := ds.ComputeCapacity(capacitySize)

	// Should return a positive capacity
	assert.Greater(t, result, 0)
}

func TestDatasource_ComputeCapacity_FixedOverheadExceedsCapacity(t *testing.T) {
	ds := NewDatasource()

	// Add many metrics to increase fixed overhead
	for i := 0; i < 100; i++ {
		rawMetric := &metrics.RawMetric{
			Name:   "metric_name",
			Value:  float64(i),
			Labels: []metrics.Label{{Name: "index", Value: string(rune('0' + i%10))}},
		}
		_ = ds.Update(rawMetric)
	}

	// Use very small capacity that won't fit overhead
	result := ds.ComputeCapacity(100)

	// Should return 0 when overhead exceeds capacity
	assert.Equal(t, 0, result)
}

func TestDatasource_ComputeCapacity_MemoryCalculation(t *testing.T) {
	ds := NewDatasource()

	// Add a few metrics
	rawMetric1 := &metrics.RawMetric{
		Name:   "cpu_usage",
		Value:  75.5,
		Labels: []metrics.Label{},
		Desc:   "CPU usage",
	}
	_ = ds.Update(rawMetric1)

	rawMetric2 := &metrics.RawMetric{
		Name:   "memory_usage",
		Value:  50.0,
		Labels: []metrics.Label{},
		Desc:   "Memory usage",
	}
	_ = ds.Update(rawMetric2)

	capacitySize := 10000
	result := ds.ComputeCapacity(capacitySize)

	// Should calculate based on available memory
	assert.Greater(t, result, 0)
	// Should be reasonable (not exceed available memory)
	assert.Less(t, result, capacitySize)
}

// Datasource Tests.

func TestNewDatasource(t *testing.T) {
	ds := NewDatasource()

	assert.NotNil(t, ds)
	assert.NotNil(t, ds.metrics)
	assert.Empty(t, ds.metrics)
	assert.NotNil(t, ds.descriptions)
	assert.Empty(t, ds.descriptions)
	assert.Equal(t, 0, ds.CapacitySize)
	assert.Equal(t, uint64(0), ds.GetTotalWritten())
	if ds.timestamps != nil {
		assert.Equal(t, 0, ds.timestamps.Len())
	}
}

func TestDatasource_Update_NilMetric(t *testing.T) {
	ds := NewDatasource()

	err := ds.Update(nil)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot be nil")
}

func TestDatasource_Update_CreatesRingBuffer(t *testing.T) {
	ds := NewDatasource()

	rawMetric := &metrics.RawMetric{
		Name:   "cpu_usage",
		Value:  75.5,
		Labels: []metrics.Label{},
	}

	err := ds.Update(rawMetric)

	require.NoError(t, err)
	metricsMap := ds.GetMetrics()
	assert.Contains(t, metricsMap, "cpu_usage")
	assert.NotNil(t, metricsMap["cpu_usage"])
}

func TestDatasource_Update_ReusesExistingRingBuffer(t *testing.T) {
	ds := NewDatasource()

	rawMetric1 := &metrics.RawMetric{
		Name:   "cpu_usage",
		Value:  75.5,
		Labels: []metrics.Label{},
	}
	_ = ds.Update(rawMetric1)

	metricsMap1 := ds.GetMetrics()
	buffer1 := metricsMap1["cpu_usage"]

	rawMetric2 := &metrics.RawMetric{
		Name:   "cpu_usage",
		Value:  80.0,
		Labels: []metrics.Label{},
	}
	_ = ds.Update(rawMetric2)

	metricsMap2 := ds.GetMetrics()
	buffer2 := metricsMap2["cpu_usage"]

	// Should reuse the same buffer
	assert.Equal(t, buffer1, buffer2)
}

func TestDatasource_Update_StoresDescription(t *testing.T) {
	ds := NewDatasource()

	rawMetric := &metrics.RawMetric{
		Name:   "cpu_usage",
		Value:  75.5,
		Labels: []metrics.Label{},
		Desc:   "CPU usage percentage",
	}

	err := ds.Update(rawMetric)

	require.NoError(t, err)
	descriptions := ds.GetDescriptions()
	assert.Equal(t, "CPU usage percentage", descriptions["cpu_usage"])
}

func TestDatasource_Update_UpdatesDescription(t *testing.T) {
	ds := NewDatasource()

	rawMetric1 := &metrics.RawMetric{
		Name:   "cpu_usage",
		Value:  75.5,
		Labels: []metrics.Label{},
		Desc:   "Old description",
	}
	_ = ds.Update(rawMetric1)

	rawMetric2 := &metrics.RawMetric{
		Name:   "cpu_usage",
		Value:  80.0,
		Labels: []metrics.Label{},
		Desc:   "New description",
	}
	_ = ds.Update(rawMetric2)

	descriptions := ds.GetDescriptions()
	assert.Equal(t, "New description", descriptions["cpu_usage"])
}

func TestDatasource_Update_IncrementsTotalWritten(t *testing.T) {
	ds := NewDatasource()

	initialTotal := ds.GetTotalWritten()

	rawMetric := &metrics.RawMetric{
		Name:   "cpu_usage",
		Value:  75.5,
		Labels: []metrics.Label{},
	}
	_ = ds.Update(rawMetric)

	assert.Equal(t, initialTotal+1, ds.GetTotalWritten())
}

func TestDatasource_AddTimestamp(t *testing.T) {
	ds := NewDatasource()
	ds.SetCapacity(10000)

	timestamp1 := int64(1000)
	timestamp2 := int64(2000)

	ds.AddTimestamp(timestamp1)
	ds.AddTimestamp(timestamp2)

	timestamps := ds.GetTimestamps()
	assert.GreaterOrEqual(t, timestamps.Len(), 1)
}

func TestDatasource_AddTimestamp_MultipleCycles(t *testing.T) {
	ds := NewDatasource()
	ds.SetCapacity(10000)

	for i := 0; i < 5; i++ {
		ds.AddTimestamp(int64(i * 1000))
	}

	timestamps := ds.GetTimestamps()
	assert.GreaterOrEqual(t, timestamps.Len(), 1)
}

func TestDatasource_GetMetrics_ThreadSafe(t *testing.T) {
	ds := NewDatasource()
	ds.SetCapacity(10000)

	// Add metrics concurrently
	var wg sync.WaitGroup
	numGoroutines := 10
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			rawMetric := &metrics.RawMetric{
				Name:   "cpu_usage",
				Value:  float64(id),
				Labels: []metrics.Label{},
			}
			_ = ds.Update(rawMetric)
		}(i)
	}

	wg.Wait()

	// Should be able to read metrics safely
	metricsMap := ds.GetMetrics()
	assert.NotNil(t, metricsMap)
}

func TestDatasource_GetTimestamps_ThreadSafe(t *testing.T) {
	ds := NewDatasource()
	ds.SetCapacity(10000)

	// Add timestamps concurrently
	var wg sync.WaitGroup
	numGoroutines := 10
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			ds.AddTimestamp(int64(id * 1000))
		}(i)
	}

	wg.Wait()

	// Should be able to read timestamps safely
	timestamps := ds.GetTimestamps()
	assert.NotNil(t, timestamps)
}

func TestDatasource_Update_MetricKeyWithLabels(t *testing.T) {
	ds := NewDatasource()
	ds.SetCapacity(10000)

	rawMetric1 := &metrics.RawMetric{
		Name:   "http_requests_total",
		Value:  100.0,
		Labels: []metrics.Label{{Name: "method", Value: "GET"}},
	}
	_ = ds.Update(rawMetric1)

	rawMetric2 := &metrics.RawMetric{
		Name:   "http_requests_total",
		Value:  200.0,
		Labels: []metrics.Label{{Name: "method", Value: "POST"}},
	}
	_ = ds.Update(rawMetric2)

	metricsMap := ds.GetMetrics()
	// Should have separate buffers for each label combination
	assert.GreaterOrEqual(t, len(metricsMap), 2)
}

func TestDatasource_Update_CapacityAdjustment(t *testing.T) {
	ds := NewDatasource()
	initialCapacity := 10000
	ds.SetCapacity(initialCapacity)

	rawMetric := &metrics.RawMetric{
		Name:   "cpu_usage",
		Value:  75.5,
		Labels: []metrics.Label{},
	}
	_ = ds.Update(rawMetric)

	metricsMap := ds.GetMetrics()
	buffer := metricsMap["cpu_usage"]
	initialBufferCap := buffer.Cap()

	// Change capacity
	newCapacity := 20000
	ds.SetCapacity(newCapacity)

	// Update again - should adjust buffer capacity
	_ = ds.Update(rawMetric)

	updatedBuffer := ds.GetMetrics()["cpu_usage"]
	// Buffer capacity should be adjusted
	assert.NotEqual(t, initialBufferCap, updatedBuffer.Cap())
}

func TestFlightRecorder_Update_MultipleCyclesWithOverwrite(t *testing.T) {
	fr := NewFlightRecorder(10000)

	// Add metrics for multiple cycles that exceed buffer capacity
	for cycle := 0; cycle < 20; cycle++ {
		rawMetrics := []metrics.RawMetric{
			{
				Name:   "cpu_usage",
				Value:  float64(75 + cycle),
				Labels: []metrics.Label{},
			},
		}
		err := fr.Update(rawMetrics)
		require.NoError(t, err)
		time.Sleep(1 * time.Millisecond) // Ensure different timestamps
	}

	datasources := fr.GetDatasources()
	require.Len(t, datasources, 1)
	metricsMap := datasources[0].GetMetrics()
	buffer := metricsMap["cpu_usage"]

	// Buffer should have values, but may have overwritten oldest ones
	assert.Greater(t, buffer.Len(), 0)
	// Current value should be from the most recent cycle
	currentValue := buffer.GetCurrentValue()
	assert.GreaterOrEqual(t, currentValue, 75.0)
}

func TestFlightRecorder_Update_ErrorPropagation(t *testing.T) {
	fr := NewFlightRecorder(1000)

	// This should not cause an error, but if Datasource.Update returns an error,
	// it should be propagated
	rawMetrics := []metrics.RawMetric{
		{
			Name:   "cpu_usage",
			Value:  75.5,
			Labels: []metrics.Label{},
		},
	}

	err := fr.Update(rawMetrics)
	require.NoError(t, err)
}

func TestRingBuffer_TypeSafety_Float64(t *testing.T) {
	rb := NewRingBuffer[float64]()

	rb.Add(1.5)
	rb.Add(2.5)

	value := rb.Get(0)
	// Type assertion should work
	_, ok := interface{}(value).(float64)
	assert.True(t, ok)
	assert.Equal(t, 1.5, value)
}

func TestRingBuffer_TypeSafety_Int64(t *testing.T) {
	rb := NewRingBuffer[int64]()

	rb.Add(100)
	rb.Add(200)

	value := rb.Get(0)
	// Type assertion should work
	_, ok := interface{}(value).(int64)
	assert.True(t, ok)
	assert.Equal(t, int64(100), value)
}

func TestRingBuffer_TypeSafety_String(t *testing.T) {
	rb := NewRingBuffer[string]()

	rb.Add("test1")
	rb.Add("test2")

	value := rb.Get(0)
	assert.Equal(t, "test1", value)
}

// Additional Datasource Tests.

func TestDatasource_Update_LabelSorting(t *testing.T) {
	ds := NewDatasource()
	ds.SetCapacity(10000)

	// Add metric with labels in one order
	rawMetric1 := &metrics.RawMetric{
		Name:  "http_requests_total",
		Value: 100.0,
		Labels: []metrics.Label{
			{Name: "status", Value: "200"},
			{Name: "method", Value: "GET"},
		},
	}
	_ = ds.Update(rawMetric1)

	// Add same metric with labels in different order
	rawMetric2 := &metrics.RawMetric{
		Name:  "http_requests_total",
		Value: 200.0,
		Labels: []metrics.Label{
			{Name: "method", Value: "GET"},
			{Name: "status", Value: "200"},
		},
	}
	_ = ds.Update(rawMetric2)

	metricsMap := ds.GetMetrics()
	// Should use the same MetricKey (labels are sorted) and reuse the same buffer
	assert.Len(t, metricsMap, 1)
	// Both updates should be in the same buffer
	buffer := metricsMap["http_requests_total{method=\"GET\",status=\"200\"}"]
	assert.NotNil(t, buffer)
	assert.Equal(t, 200.0, buffer.GetCurrentValue())
}

func TestDatasource_Update_MetricKeyGeneration(t *testing.T) {
	ds := NewDatasource()
	ds.SetCapacity(10000)

	rawMetric := &metrics.RawMetric{
		Name:   "cpu_usage",
		Value:  75.5,
		Labels: []metrics.Label{},
	}
	_ = ds.Update(rawMetric)

	metricsMap := ds.GetMetrics()
	// MetricKey for metric without labels should be just the name
	assert.Contains(t, metricsMap, "cpu_usage")
}

func TestDatasource_Update_MetricKeyWithSortedLabels(t *testing.T) {
	ds := NewDatasource()
	ds.SetCapacity(10000)

	rawMetric := &metrics.RawMetric{
		Name:  "http_requests_total",
		Value: 100.0,
		Labels: []metrics.Label{
			{Name: "method", Value: "GET"},
			{Name: "status", Value: "200"},
		},
	}
	_ = ds.Update(rawMetric)

	metricsMap := ds.GetMetrics()
	// MetricKey should include sorted labels
	expectedKey := "http_requests_total{method=\"GET\",status=\"200\"}"
	assert.Contains(t, metricsMap, expectedKey)
}

func TestDatasource_Update_EmptyDescription(t *testing.T) {
	ds := NewDatasource()
	ds.SetCapacity(10000)

	rawMetric1 := &metrics.RawMetric{
		Name:   "cpu_usage",
		Value:  75.5,
		Labels: []metrics.Label{},
		Desc:   "CPU usage percentage",
	}
	_ = ds.Update(rawMetric1)

	rawMetric2 := &metrics.RawMetric{
		Name:   "cpu_usage",
		Value:  80.0,
		Labels: []metrics.Label{},
		Desc:   "", // Empty description
	}
	_ = ds.Update(rawMetric2)

	descriptions := ds.GetDescriptions()
	// Empty description should not overwrite existing description
	assert.Equal(t, "CPU usage percentage", descriptions["cpu_usage"])
}

func TestDatasource_Update_FIFOStrategyOnShrink(t *testing.T) {
	ds := NewDatasource()
	initialCapacity := 50000
	ds.SetCapacity(initialCapacity)

	// Add multiple values to fill buffer
	for i := 0; i < 10; i++ {
		rawMetric := &metrics.RawMetric{
			Name:   "cpu_usage",
			Value:  float64(70 + i),
			Labels: []metrics.Label{},
		}
		_ = ds.Update(rawMetric)
	}

	metricsMap := ds.GetMetrics()
	buffer := metricsMap["cpu_usage"]
	initialCurrentValue := buffer.GetCurrentValue()

	// Reduce capacity significantly
	ds.SetCapacity(10000)
	// Update again to trigger capacity recalculation
	rawMetric := &metrics.RawMetric{
		Name:   "cpu_usage",
		Value:  85.0,
		Labels: []metrics.Label{},
	}
	_ = ds.Update(rawMetric)

	updatedBuffer := ds.GetMetrics()["cpu_usage"]
	updatedValues := updatedBuffer.GetAllValues()

	// Should preserve most recent values (FIFO strategy)
	// The most recent value should still be present
	assert.Equal(t, 85.0, updatedBuffer.GetCurrentValue())
	// Should contain some of the recent values from before
	assert.Contains(t, updatedValues, initialCurrentValue)
}

func TestDatasource_Update_CircularOverwrite(t *testing.T) {
	ds := NewDatasource()
	ds.SetCapacity(10000)

	// Add many values that exceed buffer capacity
	for i := 0; i < 50; i++ {
		rawMetric := &metrics.RawMetric{
			Name:   "cpu_usage",
			Value:  float64(70 + i),
			Labels: []metrics.Label{},
		}
		_ = ds.Update(rawMetric)
	}

	metricsMap := ds.GetMetrics()
	buffer := metricsMap["cpu_usage"]

	// Skip test if buffer capacity is 0
	if buffer.Cap() == 0 {
		t.Skip("Buffer capacity is 0, skipping circular overwrite test")
	}

	// Buffer should have values
	assert.Greater(t, buffer.Len(), 0)
	// Current value should be the most recent
	currentValue := buffer.GetCurrentValue()
	assert.Equal(t, 119.0, currentValue) // 70 + 49

	// Oldest values should have been overwritten if buffer capacity is smaller than number of values added
	allValues := buffer.GetAllValues()
	bufferCapacity := buffer.Cap()
	// Only check for overwrite if we added more values than buffer capacity
	if 50 > bufferCapacity {
		// If buffer wrapped around, earliest values should be gone
		// But we can't guarantee which specific values are gone due to circular nature
		// So we just verify the buffer has the correct capacity and current value
		assert.Equal(t, bufferCapacity, len(allValues))
		// Verify that not all early values are present (some should have been overwritten)
		earlyValueCount := 0
		for i := 0; i < 10; i++ {
			if contains(allValues, float64(70+i)) {
				earlyValueCount++
			}
		}
		// Should have fewer than 10 early values if buffer wrapped around
		assert.Less(t, earlyValueCount, 10)
	}
}

// contains checks if a slice contains a specific value.
func contains[T comparable](slice []T, value T) bool {
	for _, v := range slice {
		if v == value {
			return true
		}
	}
	return false
}

func TestDatasource_Update_MultipleMetricsCircularOverwrite(t *testing.T) {
	ds := NewDatasource()
	ds.SetCapacity(20000)

	// Add multiple metrics with many updates
	for cycle := 0; cycle < 30; cycle++ {
		rawMetrics := []*metrics.RawMetric{
			{
				Name:   "cpu_usage",
				Value:  float64(70 + cycle),
				Labels: []metrics.Label{},
			},
			{
				Name:   "memory_usage",
				Value:  float64(50 + cycle),
				Labels: []metrics.Label{},
			},
		}

		for _, rawMetric := range rawMetrics {
			_ = ds.Update(rawMetric)
		}
	}

	metricsMap := ds.GetMetrics()
	cpuBuffer := metricsMap["cpu_usage"]
	memoryBuffer := metricsMap["memory_usage"]

	// Both buffers should have values
	assert.Greater(t, cpuBuffer.Len(), 0)
	assert.Greater(t, memoryBuffer.Len(), 0)

	// Current values should be from the most recent cycle
	assert.Equal(t, 99.0, cpuBuffer.GetCurrentValue())    // 70 + 29
	assert.Equal(t, 79.0, memoryBuffer.GetCurrentValue()) // 50 + 29
}

func TestDatasource_Update_BufferPreservationOnCapacityChange(t *testing.T) {
	ds := NewDatasource()
	ds.SetCapacity(10000)

	// Add some values
	for i := 0; i < 5; i++ {
		rawMetric := &metrics.RawMetric{
			Name:   "cpu_usage",
			Value:  float64(70 + i),
			Labels: []metrics.Label{},
		}
		_ = ds.Update(rawMetric)
	}

	metricsMap1 := ds.GetMetrics()
	buffer1 := metricsMap1["cpu_usage"]
	valuesBefore := buffer1.GetAllValues()

	// Increase capacity
	ds.SetCapacity(50000)
	rawMetric := &metrics.RawMetric{
		Name:   "cpu_usage",
		Value:  80.0,
		Labels: []metrics.Label{},
	}
	_ = ds.Update(rawMetric)

	metricsMap2 := ds.GetMetrics()
	buffer2 := metricsMap2["cpu_usage"]
	valuesAfter := buffer2.GetAllValues()

	// Previous values should be preserved
	for _, val := range valuesBefore {
		if val != 0.0 { // Skip zero values
			assert.Contains(t, valuesAfter, val)
		}
	}
	// New value should be present
	assert.Contains(t, valuesAfter, 80.0)
}

func TestDatasource_Update_DifferentLabelCombinations(t *testing.T) {
	ds := NewDatasource()
	ds.SetCapacity(20000)

	// Add metrics with different label combinations
	labelCombinations := []struct {
		labels []metrics.Label
		value  float64
	}{
		{[]metrics.Label{{Name: "method", Value: "GET"}}, 100.0},
		{[]metrics.Label{{Name: "method", Value: "POST"}}, 200.0},
		{[]metrics.Label{{Name: "method", Value: "GET"}, {Name: "status", Value: "200"}}, 150.0},
		{[]metrics.Label{{Name: "method", Value: "GET"}, {Name: "status", Value: "404"}}, 50.0},
	}

	for _, combo := range labelCombinations {
		rawMetric := &metrics.RawMetric{
			Name:   "http_requests_total",
			Value:  combo.value,
			Labels: combo.labels,
		}
		_ = ds.Update(rawMetric)
	}

	metricsMap := ds.GetMetrics()
	// Should have separate buffers for each label combination
	assert.GreaterOrEqual(t, len(metricsMap), 4)

	// Verify each combination has its own buffer
	for _, combo := range labelCombinations {
		mk := metrics.MetricKey{
			Name:   "http_requests_total",
			Labels: combo.labels,
		}
		key := mk.String()
		assert.Contains(t, metricsMap, key)
		assert.Equal(t, combo.value, metricsMap[key].GetCurrentValue())
	}
}

func TestDatasource_Update_SameMetricNameDifferentLabels(t *testing.T) {
	ds := NewDatasource()
	ds.SetCapacity(20000)

	rawMetric1 := &metrics.RawMetric{
		Name:   "http_requests_total",
		Value:  100.0,
		Labels: []metrics.Label{{Name: "method", Value: "GET"}},
		Desc:   "Total HTTP requests",
	}
	_ = ds.Update(rawMetric1)

	rawMetric2 := &metrics.RawMetric{
		Name:   "http_requests_total",
		Value:  200.0,
		Labels: []metrics.Label{{Name: "method", Value: "POST"}},
		Desc:   "Total HTTP requests",
	}
	_ = ds.Update(rawMetric2)

	metricsMap := ds.GetMetrics()
	// Should have 2 separate buffers
	assert.Len(t, metricsMap, 2)

	// Both should share the same description (by metric name, not key)
	descriptions := ds.GetDescriptions()
	assert.Equal(t, "Total HTTP requests", descriptions["http_requests_total"])
}

func TestDatasource_GetDescriptions_ThreadSafe(t *testing.T) {
	ds := NewDatasource()
	ds.SetCapacity(10000)

	// Add metrics concurrently
	var wg sync.WaitGroup
	numGoroutines := 10
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			rawMetric := &metrics.RawMetric{
				Name:   "cpu_usage",
				Value:  float64(id),
				Labels: []metrics.Label{},
				Desc:   "CPU usage percentage",
			}
			_ = ds.Update(rawMetric)
		}(i)
	}

	wg.Wait()

	// Should be able to read descriptions safely
	descriptions := ds.GetDescriptions()
	assert.NotNil(t, descriptions)
	assert.Equal(t, "CPU usage percentage", descriptions["cpu_usage"])
}

func TestDatasource_Update_ZeroCapacity(t *testing.T) {
	ds := NewDatasource()
	ds.SetCapacity(0)

	rawMetric := &metrics.RawMetric{
		Name:   "cpu_usage",
		Value:  75.5,
		Labels: []metrics.Label{},
	}

	err := ds.Update(rawMetric)

	// Should handle zero capacity gracefully
	// ComputeCapacity returns defaultCapacity when no metrics exist
	require.NoError(t, err)
	metricsMap := ds.GetMetrics()
	assert.Contains(t, metricsMap, "cpu_usage")
}

func TestDatasource_Update_VerySmallCapacity(t *testing.T) {
	ds := NewDatasource()
	ds.SetCapacity(100) // Very small capacity

	rawMetric := &metrics.RawMetric{
		Name:   "cpu_usage",
		Value:  75.5,
		Labels: []metrics.Label{},
	}

	err := ds.Update(rawMetric)

	// Should handle very small capacity
	// May result in capacity 0 if overhead exceeds available memory
	if err == nil {
		metricsMap := ds.GetMetrics()
		// If capacity is too small, ComputeCapacity may return 0
		// In that case, the buffer might not store values
		if len(metricsMap) > 0 {
			buffer := metricsMap["cpu_usage"]
			// Buffer might be empty if capacity is 0
			assert.NotNil(t, buffer)
		}
	}
}

func TestDatasource_AddTimestamp_OnePerPollingCycle(t *testing.T) {
	ds := NewDatasource()
	ds.SetCapacity(10000)

	// Simulate multiple polling cycles
	for cycle := 0; cycle < 5; cycle++ {
		// Add multiple metrics in one cycle
		for i := 0; i < 3; i++ {
			rawMetric := &metrics.RawMetric{
				Name:   "metric_name",
				Value:  float64(i),
				Labels: []metrics.Label{},
			}
			_ = ds.Update(rawMetric)
		}
		// Add timestamp once per cycle
		ds.AddTimestamp(int64(cycle * 1000))
	}

	timestamps := ds.GetTimestamps()
	// Should have timestamps for each polling cycle
	assert.GreaterOrEqual(t, timestamps.Len(), 1)
}

func TestDatasource_ComputeCapacity_WithManyMetrics(t *testing.T) {
	ds := NewDatasource()

	// Add many metrics to increase overhead
	for i := 0; i < 50; i++ {
		rawMetric := &metrics.RawMetric{
			Name:   "metric_name",
			Value:  float64(i),
			Labels: []metrics.Label{{Name: "index", Value: string(rune('0' + i%10))}},
		}
		_ = ds.Update(rawMetric)
	}

	capacitySize := 100000
	result := ds.ComputeCapacity(capacitySize)

	// Should calculate capacity accounting for all metrics
	assert.Greater(t, result, 0)
	// With many metrics, capacity per entry will be lower
	assert.Less(t, result, capacitySize/8) // Rough check
}

func TestDatasource_Update_ConcurrentUpdatesSameMetric(t *testing.T) {
	ds := NewDatasource()
	ds.SetCapacity(100000)

	var wg sync.WaitGroup
	numGoroutines := 20
	updatesPerGoroutine := 10

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < updatesPerGoroutine; j++ {
				rawMetric := &metrics.RawMetric{
					Name:   "cpu_usage",
					Value:  float64(goroutineID*updatesPerGoroutine + j),
					Labels: []metrics.Label{},
				}
				_ = ds.Update(rawMetric)
			}
		}(i)
	}

	wg.Wait()

	// Should have successfully updated
	metricsMap := ds.GetMetrics()
	assert.Contains(t, metricsMap, "cpu_usage")
	assert.Equal(t, uint64(numGoroutines*updatesPerGoroutine), ds.GetTotalWritten())
}

func TestDatasource_GetMetrics_ReturnsCopy(t *testing.T) {
	ds := NewDatasource()
	ds.SetCapacity(10000)

	rawMetric := &metrics.RawMetric{
		Name:   "cpu_usage",
		Value:  75.5,
		Labels: []metrics.Label{},
	}
	_ = ds.Update(rawMetric)

	metricsMap1 := ds.GetMetrics()
	metricsMap2 := ds.GetMetrics()

	// Should return different map instances (copies)
	// Check by modifying one and verifying the other is not affected
	metricsMap1["test_key"] = NewMetricRingBuffer()
	// The second map should not have the test key
	_, exists := metricsMap2["test_key"]
	assert.False(t, exists)
	// But should contain the same original data
	assert.Equal(t, len(metricsMap1)-1, len(metricsMap2)) // -1 because we added test_key to map1
	assert.Contains(t, metricsMap2, "cpu_usage")
}

func TestDatasource_GetDescriptions_ReturnsCopy(t *testing.T) {
	ds := NewDatasource()
	ds.SetCapacity(10000)

	rawMetric := &metrics.RawMetric{
		Name:   "cpu_usage",
		Value:  75.5,
		Labels: []metrics.Label{},
		Desc:   "CPU usage",
	}
	_ = ds.Update(rawMetric)

	descriptions1 := ds.GetDescriptions()
	descriptions2 := ds.GetDescriptions()

	// Should return different map instances (copies)
	// Check by modifying one and verifying the other is not affected
	descriptions1["test_key"] = "test_value"
	// The second map should not have the test key
	_, exists := descriptions2["test_key"]
	assert.False(t, exists)
	// But should contain the same original data
	assert.Equal(t, len(descriptions1)-1, len(descriptions2)) // -1 because we added test_key to map1
	assert.Equal(t, "CPU usage", descriptions2["cpu_usage"])
}
