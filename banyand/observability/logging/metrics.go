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

package logging

import (
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/meter"
)

// logScope names the instruments. Raw metric names have to be globally unique,
// not merely unique within a scope: the native meter provider uses the bare
// name as the measure name, unlike the Prometheus provider which prefixes it.
var logScope = observability.RootScope.SubScope("logging")

// allReasons is the closed set a drop can name. It is also what NewSink
// allocates a counter for, so a reason added here is countable without a second
// edit. Publishing every one of them, including the zeroes, means an operator
// can tell "no drops" from "that reason never fires here".
var allReasons = []string{
	reasonBufferFull, reasonMemoryReserve, reasonOversizeEvent,
	reasonEncodeFailed, reasonPublishFailed, reasonSchemaMissing,
	reasonSchemaIncompatible, reasonShutdown,
}

// metrics reports what the sink lost and what it wrote. They are gauges rather
// than counters because the sink already holds the running totals; the meter
// only needs to publish them.
type metrics struct {
	dropped      meter.Gauge
	written      meter.Gauge
	bufferBytes  meter.Gauge
	bufferBudget meter.Gauge
}

func newMetrics(omr observability.MetricsRegistry) *metrics {
	if omr == nil {
		return nil
	}
	factory := omr.With(logScope)
	return &metrics{
		dropped:      factory.NewGauge("native_log_dropped_total", "reason"),
		written:      factory.NewGauge("native_log_written_total"),
		bufferBytes:  factory.NewGauge("native_log_buffer_bytes"),
		bufferBudget: factory.NewGauge("native_log_buffer_budget_bytes"),
	}
}

func (m *metrics) observe(s *Sink) {
	if m == nil || s == nil {
		return
	}
	for _, reason := range allReasons {
		m.dropped.Set(float64(s.Dropped(reason)), reason)
	}
	m.written.Set(float64(s.Written()))
	m.bufferBytes.Set(float64(s.QueuedBytes()))
	m.bufferBudget.Set(float64(s.budgetBytes()))
}
