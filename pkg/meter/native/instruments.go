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

import (
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/meter"
)

type counter struct {
	*metricVec
}

func newCounter(measureName string, pipeline queue.Client, scope meter.Scope) *counter {
	return &counter{
		newMetricVec(measureName, pipeline, scope),
	}
}

func (c *counter) Inc(delta float64, labelValues ...string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	key := generateKey(labelValues)
	v, exist := c.metrics[key]
	if !exist {
		v = metricWithLabelValues{
			labelValues: labelValues,
		}
	}
	v.metric += delta
	c.metrics[key] = v
}

type gauge struct {
	*metricVec
}

func newGauge(measureName string, pipeline queue.Client, scope meter.Scope) *gauge {
	return &gauge{
		newMetricVec(measureName, pipeline, scope),
	}
}

func (g *gauge) Set(value float64, labelValues ...string) {
	g.mutex.Lock()
	defer g.mutex.Unlock()
	g.metrics[generateKey(labelValues)] = metricWithLabelValues{
		metric:      value,
		labelValues: labelValues,
	}
}

func (g *gauge) Add(delta float64, labelValues ...string) {
	g.mutex.Lock()
	defer g.mutex.Unlock()
	key := generateKey(labelValues)
	v, exist := g.metrics[key]
	if !exist {
		v = metricWithLabelValues{
			labelValues: labelValues,
		}
	}
	v.metric += delta
	g.metrics[key] = v
}

type histogram struct {
	*metricVec
}

func newHistogram(measureName string, pipeline queue.Client, scope meter.Scope) *histogram {
	return &histogram{
		newMetricVec(measureName, pipeline, scope),
	}
}

func (h *histogram) Observe(_ float64, _ ...string) {}
