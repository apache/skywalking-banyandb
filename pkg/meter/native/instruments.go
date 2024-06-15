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

// Package native provides a simple meter system for metrics. The metrics are aggregated by the meter provider.
package native

// Counter is the native implementation of meter.Counter.
type Counter struct {
	*metricVec
}

// Gauge is the native implementation of meter.Gauge.
type Gauge struct {
	*metricVec
}

// Add Metric Value in Gauge.
func (g *Gauge) Add(delta float64, labelValues ...string) {
	g.metricVec.Inc(delta, labelValues...)
}

// Set Metric Value in Gauge.
func (g *Gauge) Set(value float64, labelValues ...string) {
	g.mutex.Lock()
	defer g.mutex.Unlock()
	tagValues := buildTagValues(g.scope, labelValues...)
	hash := seriesHash(tagValues)
	key := string(hash)
	g.metrics[key] = metricWithLabelValues{
		metricValue: value,
		labelValues: tagValues,
		seriesHash:  hash,
	}
}

// Histogram is the native implementation of meter.Histogram.
type Histogram struct {
	*metricVec
}

// Observe to be implemented.
func (h *Histogram) Observe(_ float64, _ ...string) {}
