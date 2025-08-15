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

package metrics

import (
	"sync"
	"time"
)

// MetricType represents the type of metric
type MetricType string

const (
	// MetricTypeCounter represents a counter metric
	MetricTypeCounter MetricType = "counter"
	// MetricTypeGauge represents a gauge metric
	MetricTypeGauge MetricType = "gauge"
	// MetricTypeHistogram represents a histogram metric
	MetricTypeHistogram MetricType = "histogram"
)

// Metric represents a single metric
type Metric struct {
	Name      string
	Type      MetricType
	Value     float64
	Labels    map[string]string
	Timestamp time.Time
	Help      string
}

// MetricSet represents a collection of metrics
type MetricSet struct {
	metrics []Metric
	mu      sync.RWMutex
}

// NewMetricSet creates a new metric set
func NewMetricSet() *MetricSet {
	return &MetricSet{
		metrics: make([]Metric, 0),
	}
}

// AddCounter adds a counter metric
func (ms *MetricSet) AddCounter(name string, value float64, labels map[string]string) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	
	ms.metrics = append(ms.metrics, Metric{
		Name:      name,
		Type:      MetricTypeCounter,
		Value:     value,
		Labels:    labels,
		Timestamp: time.Now(),
	})
}

// AddGauge adds a gauge metric
func (ms *MetricSet) AddGauge(name string, value float64, labels map[string]string) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	
	ms.metrics = append(ms.metrics, Metric{
		Name:      name,
		Type:      MetricTypeGauge,
		Value:     value,
		Labels:    labels,
		Timestamp: time.Now(),
	})
}

// GetMetrics returns all metrics
func (ms *MetricSet) GetMetrics() []Metric {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	
	result := make([]Metric, len(ms.metrics))
	copy(result, ms.metrics)
	return result
}

// Count returns the number of metrics
func (ms *MetricSet) Count() int {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return len(ms.metrics)
}

// Store manages metrics from multiple modules
type Store struct {
	data map[string]*MetricSet
	mu   sync.RWMutex
}

// NewStore creates a new metrics store
func NewStore() *Store {
	return &Store{
		data: make(map[string]*MetricSet),
	}
}

// Update updates metrics for a module
func (s *Store) Update(module string, metrics *MetricSet) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[module] = metrics
}

// Get returns metrics for a specific module
func (s *Store) Get(module string) *MetricSet {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.data[module]
}

// GetAll returns all metrics
func (s *Store) GetAll() map[string]*MetricSet {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	result := make(map[string]*MetricSet)
	for k, v := range s.data {
		result[k] = v
	}
	return result
}