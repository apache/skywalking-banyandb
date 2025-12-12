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
	"time"

	"github.com/apache/skywalking-banyandb/fodc/internal/metrics"
)

// FlightRecorder buffers metrics data using fixed-size circular buffers with in-memory storage.
type FlightRecorder struct {
	mu           sync.RWMutex
	datasources  []*Datasource
	capacitySize int // Memory limit in bytes
}

// NewFlightRecorder creates a new FlightRecorder instance.
func NewFlightRecorder(capacitySize int) *FlightRecorder {
	return &FlightRecorder{
		datasources:  make([]*Datasource, 0),
		capacitySize: capacitySize,
	}
}

// Update records metrics from a polling cycle.
// This implements the MetricsRecorder interface.
func (fr *FlightRecorder) Update(rawMetrics []metrics.RawMetric) error {
	if len(rawMetrics) == 0 {
		return nil
	}

	fr.mu.Lock()
	defer fr.mu.Unlock()

	// Use the first datasource, or create one if none exists
	var ds *Datasource
	if len(fr.datasources) == 0 {
		ds = NewDatasource()
		ds.Capacity = fr.capacitySize
		fr.datasources = append(fr.datasources, ds)
	} else {
		ds = fr.datasources[0]
		ds.Capacity = fr.capacitySize
	}

	// Add timestamp once per polling cycle (not per metric)
	timestamp := time.Now().Unix()
	ds.AddTimestamp(timestamp)

	// Update each metric
	for idx := range rawMetrics {
		if updateErr := ds.Update(&rawMetrics[idx]); updateErr != nil {
			return fmt.Errorf("failed to update metric %s: %w", rawMetrics[idx].Name, updateErr)
		}
	}

	return nil
}

// GetDatasources returns a copy of the datasources slice.
func (fr *FlightRecorder) GetDatasources() []*Datasource {
	fr.mu.RLock()
	defer fr.mu.RUnlock()

	result := make([]*Datasource, len(fr.datasources))
	copy(result, fr.datasources)
	return result
}

// SetCapacitySize sets the memory limit for the flight recorder.
func (fr *FlightRecorder) SetCapacitySize(capacitySize int) {
	fr.mu.Lock()
	defer fr.mu.Unlock()
	fr.capacitySize = capacitySize

	// Update capacity for all datasources
	for _, ds := range fr.datasources {
		ds.Capacity = capacitySize
	}
}

// GetCapacitySize returns the current memory limit.
func (fr *FlightRecorder) GetCapacitySize() int {
	fr.mu.RLock()
	defer fr.mu.RUnlock()
	return fr.capacitySize
}
