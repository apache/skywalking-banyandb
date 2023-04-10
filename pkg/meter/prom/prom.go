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

package prom

import (
	"unsafe"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/apache/skywalking-banyandb/pkg/meter"
)

// Provider is a prometheus provider.
type provider struct {
	scope meter.Scope
}

// NewProvider creates a new prometheus provider with given meter.Scope.
func NewProvider(scope meter.Scope) meter.Provider {
	return &provider{
		scope: scope,
	}
}

// Counter returns a prometheus counter.
func (p *provider) Counter(name string, labels ...string) meter.Counter {
	return &counter{
		counter: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        name,
			Help:        name,
			ConstLabels: convertLabels(p.scope.GetLabels()),
		}, labels),
	}
}

// Gauge returns a prometheus gauge.
func (p *provider) Gauge(name string, labels ...string) meter.Gauge {
	return &gauge{
		gauge: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        name,
			Help:        name,
			ConstLabels: convertLabels(p.scope.GetLabels()),
		}, labels),
	}
}

// Histogram returns a prometheus histogram.
func (p *provider) Histogram(name string, buckets meter.Buckets, labels ...string) meter.Histogram {
	return &histogram{
		histogram: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:        name,
			Help:        name,
			ConstLabels: convertLabels(p.scope.GetLabels()),
			Buckets:     buckets,
		}, labels),
	}
}

// convertLabels converts a map of labels to a prometheus.Labels.
func convertLabels(labels meter.LabelPairs) prometheus.Labels {
	if labels == nil {
		return nil
	}
	return *(*prometheus.Labels)(unsafe.Pointer(&labels))
}
