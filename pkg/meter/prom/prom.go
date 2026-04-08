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
	"errors"
	"unsafe"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/apache/skywalking-banyandb/pkg/meter"
)

// Provider is a prometheus provider.
type provider struct {
	scope      meter.Scope
	reg        prometheus.Registerer
	collectors []prometheus.Collector
}

// NewProvider creates a new prometheus provider with given meter.Scope.
func NewProvider(scope meter.Scope, reg prometheus.Registerer) meter.Provider {
	return &provider{
		scope: scope,
		reg:   reg,
	}
}

// Counter returns a prometheus counter.
func (p *provider) Counter(name string, labels ...string) meter.Counter {
	vec := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        p.scope.GetNamespace() + "_" + name,
		Help:        p.scope.GetNamespace() + "_" + name,
		ConstLabels: convertLabels(p.scope.GetLabels()),
	}, labels)
	collected := registerCollector(p.reg, vec)
	p.collectors = append(p.collectors, collected)
	return &counter{counter: collected}
}

// Gauge returns a prometheus gauge.
func (p *provider) Gauge(name string, labels ...string) meter.Gauge {
	vec := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:        p.scope.GetNamespace() + "_" + name,
		Help:        p.scope.GetNamespace() + "_" + name,
		ConstLabels: convertLabels(p.scope.GetLabels()),
	}, labels)
	collected := registerCollector(p.reg, vec)
	p.collectors = append(p.collectors, collected)
	return &gauge{gauge: collected}
}

// Histogram returns a prometheus histogram.
func (p *provider) Histogram(name string, buckets meter.Buckets, labels ...string) meter.Histogram {
	vec := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:        p.scope.GetNamespace() + "_" + name,
		Help:        p.scope.GetNamespace() + "_" + name,
		ConstLabels: convertLabels(p.scope.GetLabels()),
		Buckets:     buckets,
	}, labels)
	collected := registerCollector(p.reg, vec)
	p.collectors = append(p.collectors, collected)
	return &histogram{histogram: collected}
}

func registerCollector[T prometheus.Collector](reg prometheus.Registerer, c T) T {
	if regErr := reg.Register(c); regErr != nil {
		var are prometheus.AlreadyRegisteredError
		if errors.As(regErr, &are) {
			if existing, ok := are.ExistingCollector.(T); ok {
				return existing
			}
		}
		panic(regErr)
	}
	return c
}

// Close unregisters all collectors from the prometheus registry.
func (p *provider) Close() {
	for _, c := range p.collectors {
		p.reg.Unregister(c)
	}
	p.collectors = nil
}

// convertLabels converts a map of labels to a prometheus.Labels.
func convertLabels(labels meter.LabelPairs) prometheus.Labels {
	if labels == nil {
		return nil
	}
	return *(*prometheus.Labels)(unsafe.Pointer(&labels))
}
