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

// Package prom provides a prometheus implementation for the meter system.
package prom

import "github.com/prometheus/client_golang/prometheus"

type counter struct {
	counter *prometheus.CounterVec
}

func (c *counter) Inc(delta float64, labelValues ...string) {
	c.counter.WithLabelValues(labelValues...).Add(delta)
}

func (c *counter) Delete(labelValues ...string) bool {
	return c.counter.DeleteLabelValues(labelValues...)
}

type gauge struct {
	gauge *prometheus.GaugeVec
}

func (g *gauge) Set(value float64, labelValues ...string) {
	g.gauge.WithLabelValues(labelValues...).Set(value)
}

func (g *gauge) Add(delta float64, labelValues ...string) {
	g.gauge.WithLabelValues(labelValues...).Add(delta)
}

func (g *gauge) Delete(labelValues ...string) bool {
	return g.gauge.DeleteLabelValues(labelValues...)
}

type histogram struct {
	histogram *prometheus.HistogramVec
}

func (h *histogram) Observe(value float64, labelValues ...string) {
	h.histogram.WithLabelValues(labelValues...).Observe(value)
}

func (h *histogram) Delete(labelValues ...string) bool {
	return h.histogram.DeleteLabelValues(labelValues...)
}
