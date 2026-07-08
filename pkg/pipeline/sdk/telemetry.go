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

// Freeze note: this file defines the plugin-facing telemetry façade. It is
// FROZEN: adding or removing methods on any interface declared here is a
// breaking .so ABI change and requires an ABIVersion bump. The boundary
// deliberately crosses only stdlib types so that no zerolog, zap, or
// banyand-internal meter type leaks across the .so boundary.

package sdk

// Meter is the bounded metric surface handed to a plugin via Host. The host
// enforces naming, labels, cardinality, and lifecycle; the plugin only requests.
// Methods are safe to call from multiple goroutines concurrently.
type Meter interface {
	// Counter returns or creates a counter with the given name and label names.
	Counter(name string, labelNames ...string) Counter
	// Gauge returns or creates a gauge with the given name and label names.
	Gauge(name string, labelNames ...string) Gauge
	// Histogram returns or creates a histogram with the given name, explicit
	// bucket boundaries, and label names.
	Histogram(name string, buckets []float64, labelNames ...string) Histogram
}

// Counter is a monotonically increasing metric. It must not be decremented.
type Counter interface {
	// Inc adds delta to the counter for the given label values. Label values
	// must align positionally with the label names declared in Meter.Counter.
	// Delta must be non-negative.
	Inc(delta float64, labelValues ...string)
}

// Gauge is a metric whose value can go up or down freely.
type Gauge interface {
	// Set replaces the gauge value for the given label values.
	Set(value float64, labelValues ...string)
	// Add adds delta (positive or negative) to the gauge for the given label values.
	Add(delta float64, labelValues ...string)
}

// Histogram records observations into pre-declared buckets. Use it to track
// distributions such as latency or payload size.
type Histogram interface {
	// Observe records a single observation for the given label values.
	Observe(value float64, labelValues ...string)
}

// Logger is the bounded logging surface handed to a plugin via Host. It exposes
// no third-party types (e.g. zerolog, zap) — only a message plus alternating
// key/value pairs so no external logger dependency crosses the .so boundary.
// Implementations are expected to be safe for concurrent use.
type Logger interface {
	// Debug logs a debug-level message with optional structured key/value pairs.
	Debug(msg string, keysAndValues ...any)
	// Info logs an info-level message with optional structured key/value pairs.
	Info(msg string, keysAndValues ...any)
	// Warn logs a warning-level message with optional structured key/value pairs.
	Warn(msg string, keysAndValues ...any)
	// Error logs an error-level message with optional structured key/value pairs.
	Error(msg string, keysAndValues ...any)
}

// Host is the telemetry context the engine injects into a HostAware plugin.
// It provides scoped metric and logging surfaces that the engine owns; the
// plugin must not cache a Host beyond the lifetime of the pipeline config that
// created it. Instrument handles obtained from Meter() are valid for the
// lifetime of that Host: do not cache a Counter, Gauge, or Histogram across
// distinct UseHost calls, because the engine may reclaim and rebuild a group's
// metric series on reconcile or teardown — a handle from a prior Host would
// emit into a reclaimed series.
type Host interface {
	// Meter returns the metric surface scoped to this plugin's group.
	Meter() Meter
	// Logger returns the logging surface scoped to this plugin's group.
	Logger() Logger
}

// HostAware is the optional interface a plugin implements to receive a Host.
// The engine type-asserts the constructed sampler to HostAware and, if present,
// calls UseHost exactly once before the plugin's first Decide call. Plugins
// that do not implement HostAware are unaffected — there is no ABI change.
// A plugin may cache the Host and use it for the duration of Decide; the
// engine guarantees a distinct Host per group so metrics and logs are
// automatically namespaced.
type HostAware interface {
	// UseHost delivers the engine-owned telemetry context to the plugin. It is
	// called once, before the first Decide, with a Host scoped to the plugin's
	// group.
	UseHost(Host)
}
