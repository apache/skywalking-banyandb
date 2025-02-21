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

// Package meter provides a simple meter system for metrics. The metrics are aggregated by the meter provider.
package meter

type (
	// Buckets is a slice of bucket boundaries.
	Buckets []float64

	// LabelPairs is a map of label names to label values, which is used to identify a metric.
	LabelPairs map[string]string
)

// DefBuckets is the default buckets for histograms.
var DefBuckets = Buckets{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10}

// Merge merges the given label pairs with the current label pairs.
func (p LabelPairs) Merge(other LabelPairs) LabelPairs {
	result := make(LabelPairs, len(p)+len(other))
	for k, v := range p {
		result[k] = v
	}
	for k, v := range other {
		result[k] = v
	}
	return result
}

// Provider is the interface for a metrics provider, which is responsible for creating metrics.
type Provider interface {
	Counter(name string, labelNames ...string) Counter
	Gauge(name string, labelNames ...string) Gauge
	Histogram(name string, buckets Buckets, labelNames ...string) Histogram
}

// Scope is a namespace wrapper for metrics.
type Scope interface {
	ConstLabels(labels LabelPairs) Scope
	SubScope(name string) Scope
	GetNamespace() string
	GetLabels() LabelPairs
}

// Instrument is the interface for a metric.
type Instrument interface {
	// Delete the metric with the given label values.
	Delete(labelValues ...string) bool
}

// Counter is a metric that represents a single numerical value that only ever goes up.
type Counter interface {
	Instrument
	Inc(delta float64, labelValues ...string)
}

// Gauge is a metric that represents a single numerical value that can arbitrarily go up and down.
type Gauge interface {
	Instrument
	Set(value float64, labelValues ...string)
	Add(delta float64, labelValues ...string)
}

// Histogram is a metric that represents the statistical distribution of a set of values.
type Histogram interface {
	Instrument
	Observe(value float64, labelValues ...string)
}

// ToLabelPairs converts the given label names and label values to a map of label names to label values.
func ToLabelPairs(labelNames, labelValues []string) LabelPairs {
	labelPairs := make(LabelPairs, len(labelNames))
	for i := range labelNames {
		labelPairs[labelNames[i]] = labelValues[i]
	}
	return labelPairs
}
