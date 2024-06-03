// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package observability

import "github.com/apache/skywalking-banyandb/pkg/meter"

type counterCollection struct {
	counters []meter.Counter
}

// NewCounter init and return the counterCollection.
func NewCounter(providers []meter.Provider, name string, labelNames ...string) meter.Counter {
	var counters []meter.Counter
	for _, provider := range providers {
		counters = append(counters, provider.Counter(name, labelNames...))
	}
	return &counterCollection{
		counters: counters,
	}
}

func (c *counterCollection) Inc(delta float64, labelValues ...string) {
	for _, counter := range c.counters {
		counter.Inc(delta, labelValues...)
	}
}

func (c *counterCollection) Delete(labelValues ...string) bool {
	success := true
	for _, counter := range c.counters {
		success = success && counter.Delete(labelValues...)
	}
	return success
}

type gaugeCollection struct {
	gauges []meter.Gauge
}

// NewGauge init and return the gaugeCollection.
func NewGauge(providers []meter.Provider, name string, labelNames ...string) meter.Gauge {
	var gauges []meter.Gauge
	for _, provider := range providers {
		gauges = append(gauges, provider.Gauge(name, labelNames...))
	}
	return &gaugeCollection{
		gauges: gauges,
	}
}

func (g *gaugeCollection) Set(value float64, labelValues ...string) {
	for _, gauge := range g.gauges {
		gauge.Set(value, labelValues...)
	}
}

func (g *gaugeCollection) Add(delta float64, labelValues ...string) {
	for _, gauge := range g.gauges {
		gauge.Add(delta, labelValues...)
	}
}

func (g *gaugeCollection) Delete(labelValues ...string) bool {
	success := true
	for _, gauge := range g.gauges {
		success = success && gauge.Delete(labelValues...)
	}
	return success
}

type histogramCollection struct {
	histograms []meter.Histogram
}

// NewHistogram init and return the histogramCollection.
func NewHistogram(providers []meter.Provider, name string, buckets meter.Buckets, labelNames ...string) meter.Histogram {
	var histograms []meter.Histogram
	for _, provider := range providers {
		histograms = append(histograms, provider.Histogram(name, buckets, labelNames...))
	}
	return &histogramCollection{
		histograms: histograms,
	}
}

func (h *histogramCollection) Observe(value float64, labelValues ...string) {
	for _, histogram := range h.histograms {
		histogram.Observe(value, labelValues...)
	}
}

func (h *histogramCollection) Delete(labelValues ...string) bool {
	success := true
	for _, histogram := range h.histograms {
		success = success && histogram.Delete(labelValues...)
	}
	return success
}
