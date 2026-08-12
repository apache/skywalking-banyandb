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

package services

import (
	"sync"

	"github.com/apache/skywalking-banyandb/api/data"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/meter"
)

// vecFrameScope holds the query-layer wire-format metrics. They answer a question
// an operator cannot otherwise ask: is this node actually speaking the native
// columnar frame, or has it silently fallen back to protobuf?
//
// That distinction is load-bearing. The frame is only emitted by a DISTRIBUTED data
// node with the engine's vectorized path enabled, so a standalone server always
// reports zero encodes however the flag is set — and a rolling upgrade that brings
// data nodes up before liaisons produces frames that the older liaisons cannot
// decode (see docs/operation/upgrade.md). These series make both situations visible
// instead of leaving them to be inferred from configuration.
var vecFrameScope = observability.RootScope.SubScope("vec_frame")

var (
	vecFrameGaugesMu   sync.RWMutex
	vecFrameEncodedGge meter.Gauge
	vecFrameDecodedGge meter.Gauge
)

func init() {
	MetricsCollector.Register("vec_frame", collectVecFrame)
}

func (p *metricService) initVecFrameMetrics() {
	factory := p.With(vecFrameScope)
	vecFrameGaugesMu.Lock()
	defer vecFrameGaugesMu.Unlock()
	// Cumulative counts published as gauges: the values live in api/data as
	// process-global atomics (that package must stay free of an observability
	// dependency), so they are sampled here rather than incremented through a
	// counter handle at the call site.
	vecFrameEncodedGge = factory.NewGauge("encoded", "engine")
	vecFrameDecodedGge = factory.NewGauge("decoded", "engine")
}

func collectVecFrame() {
	vecFrameGaugesMu.RLock()
	defer vecFrameGaugesMu.RUnlock()
	if vecFrameEncodedGge == nil || vecFrameDecodedGge == nil {
		return
	}
	vecFrameEncodedGge.Set(float64(data.MeasureFrameEncodedCount()), "measure")
	vecFrameDecodedGge.Set(float64(data.MeasureFrameDecodedCount()), "measure")
	vecFrameEncodedGge.Set(float64(data.StreamFrameEncodedCount()), "stream")
	vecFrameDecodedGge.Set(float64(data.StreamFrameDecodedCount()), "stream")
	vecFrameEncodedGge.Set(float64(data.TraceFrameEncodedCount()), "trace")
	vecFrameDecodedGge.Set(float64(data.TraceFrameDecodedCount()), "trace")
}
