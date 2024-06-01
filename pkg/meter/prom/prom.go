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
	"sync"
	"unsafe"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/apache/skywalking-banyandb/pkg/meter"
)

// Provider is a prometheus provider.
type provider struct {
	scope        meter.Scope
	reg          prometheus.Registerer
	counterMap   map[string]meter.Counter
	gaugeMap     map[string]meter.Gauge
	histogramMap map[string]meter.Histogram
	mutex        sync.Mutex
}

// NewProvider creates a new prometheus provider with given meter.Scope.
func NewProvider(scope meter.Scope, reg prometheus.Registerer) meter.Provider {
	return &provider{
		scope: scope,
		reg:   reg,
	}
}

// RegisterCounter register a prometheus counter.
func (p *provider) RegisterCounter(name string, labels ...string) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.counterMap[meter.GenerateKey(name, labels)] = &counter{
		counter: promauto.With(p.reg).NewCounterVec(prometheus.CounterOpts{
			Name:        p.scope.GetNamespace() + "_" + name,
			Help:        p.scope.GetNamespace() + "_" + name,
			ConstLabels: convertLabels(p.scope.GetLabels()),
		}, labels),
	}
}

// RegisterGauge register a prometheus gauge.
func (p *provider) RegisterGauge(name string, labels ...string) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.gaugeMap[meter.GenerateKey(name, labels)] = &gauge{
		gauge: promauto.With(p.reg).NewGaugeVec(prometheus.GaugeOpts{
			Name:        p.scope.GetNamespace() + "_" + name,
			Help:        p.scope.GetNamespace() + "_" + name,
			ConstLabels: convertLabels(p.scope.GetLabels()),
		}, labels),
	}
}

// RegisterHistogram register a prometheus histogram.
func (p *provider) RegisterHistogram(name string, buckets meter.Buckets, labels ...string) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.histogramMap[meter.GenerateKey(name, labels)] = &histogram{
		histogram: promauto.With(p.reg).NewHistogramVec(prometheus.HistogramOpts{
			Name:        p.scope.GetNamespace() + "_" + name,
			Help:        p.scope.GetNamespace() + "_" + name,
			ConstLabels: convertLabels(p.scope.GetLabels()),
			Buckets:     buckets,
		}, labels),
	}
}

// GetCounter retrieves a registered prometheus counter.
func (p *provider) GetCounter(name string, labels ...string) meter.Counter {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	return p.counterMap[meter.GenerateKey(name, labels)]
}

// GetGauge retrieves a registered prometheus gauge.
func (p *provider) GetGauge(name string, labels ...string) meter.Gauge {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	return p.gaugeMap[meter.GenerateKey(name, labels)]
}

// GetHistogram retrieves a registered prometheus histogram.
func (p *provider) GetHistogram(name string, _ meter.Buckets, labels ...string) meter.Histogram {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	return p.histogramMap[meter.GenerateKey(name, labels)]
}

// convertLabels converts a map of labels to a prometheus.Labels.
func convertLabels(labels meter.LabelPairs) prometheus.Labels {
	if labels == nil {
		return nil
	}
	return *(*prometheus.Labels)(unsafe.Pointer(&labels))
}
