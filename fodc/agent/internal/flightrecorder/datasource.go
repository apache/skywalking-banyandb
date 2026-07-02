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

// Package flightrecorder implements a flight recorder for metrics data.
package flightrecorder

import (
	"fmt"
	"sync"

	"github.com/apache/skywalking-banyandb/fodc/agent/internal/metrics"
)

const (
	defaultCapacity      = 1000
	intSize              = 8  // Size of int on 64-bit systems
	sliceHeaderSize      = 24 // Size of slice header (pointer + length + capacity)
	mapBaseOverhead      = 48 // Base map structure overhead
	mapEntryOverhead     = 16 // Per-entry overhead in map
	descMapEntryOverhead = 24 // Per-entry overhead for description map
	stringHeaderSize     = 16 // String header size
)

// MetricRingBuffer is a type alias for RingBuffer[float64].
type MetricRingBuffer = RingBuffer[float64]

// TimestampRingBuffer is a type alias for RingBuffer[int64].
type TimestampRingBuffer = RingBuffer[int64]

// NewMetricRingBuffer creates a new MetricRingBuffer.
func NewMetricRingBuffer() *MetricRingBuffer {
	return NewRingBuffer[float64]()
}

// NewTimestampRingBuffer creates a new TimestampRingBuffer.
func NewTimestampRingBuffer() *TimestampRingBuffer {
	return NewRingBuffer[int64]()
}

// UpdateMetricRingBuffer adds a metric value to the metric ring buffer.
func UpdateMetricRingBuffer(mrb *MetricRingBuffer, v float64) {
	mrb.Add(v)
}

// UpdateTimestampRingBuffer adds a timestamp value to the timestamp ring buffer.
func UpdateTimestampRingBuffer(trb *TimestampRingBuffer, v int64) {
	if trb != nil {
		trb.Add(v)
	}
}

// Datasource stores metrics data with ring buffers.
type Datasource struct {
	metrics      map[string]*MetricRingBuffer // Map from metric name+labels to RingBuffer storing metric values
	descriptions map[string]string            // Map from metric name to HELP content descriptions
	types        map[string]string            // Map from metric name to Prometheus type string
	timestamps   *TimestampRingBuffer         // RingBuffer storing timestamps for each polling cycle
	mu           sync.RWMutex
	CapacitySize int64 // Memory limit in bytes
}

// NewDatasource creates a new Datasource.
func NewDatasource() *Datasource {
	return &Datasource{
		metrics:      make(map[string]*MetricRingBuffer),
		timestamps:   NewTimestampRingBuffer(),
		descriptions: make(map[string]string),
		types:        make(map[string]string),
		CapacitySize: 0,
	}
}

// Update records a metric in the datasource.
func (ds *Datasource) Update(m *metrics.RawMetric) error {
	if m == nil {
		return fmt.Errorf("metric cannot be nil")
	}

	ds.mu.Lock()
	defer ds.mu.Unlock()

	mk := metrics.MetricKey{
		Name:   m.Name,
		Labels: m.Labels,
	}
	metricKey := mk.String()

	if _, exists := ds.metrics[metricKey]; !exists {
		ds.metrics[metricKey] = NewMetricRingBuffer()
	}
	if m.Desc != "" {
		ds.descriptions[m.Name] = m.Desc
	}
	if m.Type != "" {
		ds.types[m.Name] = m.Type
	}
	UpdateMetricRingBuffer(ds.metrics[metricKey], m.Value)

	return nil
}

// AddTimestamp adds a timestamp for the current polling cycle.
func (ds *Datasource) AddTimestamp(timestamp int64) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	UpdateTimestampRingBuffer(ds.timestamps, timestamp)
}

// UpdateBatch atomically updates a batch of metrics and timestamp.
func (ds *Datasource) UpdateBatch(rawMetrics []metrics.RawMetric, timestamp int64) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	UpdateTimestampRingBuffer(ds.timestamps, timestamp)

	// Track which RingBuffers were updated in this batch
	updatedBuffers := make([]*MetricRingBuffer, 0, len(rawMetrics))

	for idx := range rawMetrics {
		m := &rawMetrics[idx]

		mk := metrics.MetricKey{
			Name:   m.Name,
			Labels: m.Labels,
		}
		metricKey := mk.String()

		if _, exists := ds.metrics[metricKey]; !exists {
			ds.metrics[metricKey] = NewMetricRingBuffer()
		}
		if m.Desc != "" {
			ds.descriptions[m.Name] = m.Desc
		}
		if m.Type != "" {
			ds.types[m.Name] = m.Type
		}
		buffer := ds.metrics[metricKey]
		UpdateMetricRingBuffer(buffer, m.Value)
		updatedBuffers = append(updatedBuffers, buffer)
	}

	for _, buffer := range updatedBuffers {
		buffer.FinalizeLastVisible()
	}

	if ds.timestamps != nil {
		ds.timestamps.FinalizeLastVisible()
	}

	return nil
}

// SetCapacity sets the capacity for the datasource and updates all ring buffer capacities.
func (ds *Datasource) SetCapacity(capacity int64) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	ds.CapacitySize = capacity

	computedCapacity := ds.ComputeCapacity(capacity)

	for _, metricBuffer := range ds.metrics {
		metricBuffer.SetCapacity(computedCapacity)
	}

	if ds.timestamps != nil {
		ds.timestamps.SetCapacity(computedCapacity)
	}
}

// ComputeCapacity computes the maximum capacity for ring buffers based on available memory constraints.
func (ds *Datasource) ComputeCapacity(capacitySize int64) int {
	if capacitySize <= 0 {
		return 1
	}
	numMetrics := len(ds.metrics)
	if numMetrics == 0 {
		return defaultCapacity
	}

	// Fixed Overheads
	// Metrics Map Overhead
	metricsMapOverhead := mapBaseOverhead + (numMetrics * mapEntryOverhead)

	// Descriptions Map Overhead
	descriptionsMapOverhead := mapBaseOverhead + (numMetrics * descMapEntryOverhead)

	// String Storage Overhead
	stringOverhead := 0
	for key := range ds.metrics {
		stringOverhead += stringHeaderSize + len(key)
	}
	for _, desc := range ds.descriptions {
		stringOverhead += stringHeaderSize + len(desc)
	}

	// For each metric RingBuffer: mutex (24 bytes) + lastVisibleMu (24 bytes) + slice header (24 bytes) +
	// next field (8 bytes) + size field (8 bytes) + visibleSize field (8 bytes) + lastVisibleVal (8 bytes)
	metricBufferOverhead := numMetrics * (24 + 24 + sliceHeaderSize + intSize + intSize + intSize + intSize)
	// For timestamp RingBuffer: pointer (8 bytes) + mutex (24 bytes) + lastVisibleMu (24 bytes) +
	// slice header (24 bytes) + next (8 bytes) + size (8 bytes) + visibleSize (8 bytes) + lastVisibleVal (8 bytes)
	timestampBufferOverhead := 8 + 24 + 24 + sliceHeaderSize + intSize + intSize + intSize + intSize

	totalFixedOverhead := int64(metricsMapOverhead + descriptionsMapOverhead + stringOverhead +
		metricBufferOverhead + timestampBufferOverhead)

	if totalFixedOverhead >= capacitySize {
		return 1
	}

	// Variable Costs per entry
	// Float64 values: numMetrics * 8 bytes
	// Timestamp: 8 bytes
	bytesPerEntry := int64((numMetrics * 8) + 8)

	availableMemory := capacitySize - totalFixedOverhead

	maxCapacity := availableMemory / bytesPerEntry

	if maxCapacity < 1 {
		return 1 // Minimum capacity of 1 to avoid division by zero
	}

	return int(maxCapacity)
}

// GetMetrics returns a copy of the metrics map.
func (ds *Datasource) GetMetrics() map[string]*MetricRingBuffer {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	result := make(map[string]*MetricRingBuffer)
	for k, v := range ds.metrics {
		result[k] = v
	}
	return result
}

// GetTimestamps returns the timestamps ring buffer.
func (ds *Datasource) GetTimestamps() *TimestampRingBuffer {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	return ds.timestamps
}

// GetCurrentSnapshot returns the latest value of every metric together with the
// latest timestamp, all captured under a single lock. UpdateBatch mutates under
// the same lock, so the returned values and timestamp always belong to the same
// batch: callers never observe a mix of an old metric value and a new timestamp
// (or vice versa) while an UpdateBatch is in flight. The map is keyed by the same
// metric key as GetMetrics.
func (ds *Datasource) GetCurrentSnapshot() (map[string]float64, int64) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	values := make(map[string]float64, len(ds.metrics))
	for k, buffer := range ds.metrics {
		values[k] = buffer.GetCurrentValue()
	}
	var timestamp int64
	if ds.timestamps != nil {
		timestamp = ds.timestamps.GetCurrentValue()
	}
	return values, timestamp
}

// latestValues fills out[name] with the current value of each metric whose key matches one
// of names (with the optional label filter). It scans the metric keys directly under the read
// lock and reads only the matched buffers, avoiding the full-map copy of GetCurrentSnapshot on
// the watchdog poll path. Names already present in out are skipped.
func (ds *Datasource) latestValues(out map[string]float64, names []string, labelFilter map[string]string) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	for key, buffer := range ds.metrics {
		for _, name := range names {
			if _, done := out[name]; done {
				continue
			}
			if metricKeyMatches(key, name, labelFilter) {
				out[name] = buffer.GetCurrentValue()
			}
		}
	}
}

// GetDescriptions returns a copy of the descriptions map.
func (ds *Datasource) GetDescriptions() map[string]string {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	result := make(map[string]string)
	for k, v := range ds.descriptions {
		result[k] = v
	}
	return result
}

// GetTypes returns a copy of the types map.
func (ds *Datasource) GetTypes() map[string]string {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	result := make(map[string]string)
	for k, v := range ds.types {
		result[k] = v
	}
	return result
}
