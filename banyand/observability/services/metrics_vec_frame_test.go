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
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/data"
)

// gatherVecFrame returns the sample values of one vec_frame metric family keyed by
// the engine label, reading them the way a scraper would rather than through the
// gauge handles the code under test holds.
func gatherVecFrame(t *testing.T, reg *prometheus.Registry, name string) map[string]float64 {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	out := map[string]float64{}
	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			engine := ""
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "engine" {
					engine = lp.GetValue()
				}
			}
			out[engine] = m.GetGauge().GetValue()
		}
	}
	return out
}

// TestVecFrameMetricsArePublished is the guard on the guard. The frame counters
// exist so a soak can assert the columnar wire format is genuinely in use, but that
// assertion is only worth anything if the values actually reach a scrape endpoint —
// a silent registration failure would look exactly like a cluster emitting no
// frames, which is the confusion the counters were added to end.
func TestVecFrameMetricsArePublished(t *testing.T) {
	svc := newTestPromService(t, true)
	svc.promReg = prometheus.NewRegistry()
	svc.initVecFrameMetrics()

	// A labeled gauge has no child series until something Sets it, so the family
	// is absent from a scrape until the first collection tick. A soak that probes
	// for these series immediately after startup must allow for that.
	require.Empty(t, gatherVecFrame(t, svc.promReg, "banyandb_vec_frame_encoded"),
		"no series should exist before the first collect")

	collectVecFrame()
	before := gatherVecFrame(t, svc.promReg, "banyandb_vec_frame_encoded")
	require.Contains(t, before, "stream", "the encoded family must carry a per-engine series")
	require.Contains(t, before, "measure")
	require.Contains(t, before, "trace")

	// Move the underlying process-global counter the way the send path does, then
	// run the collector the metrics loop drives.
	data.IncrFrameEncoded(data.TopicStreamQuery)
	collectVecFrame()

	after := gatherVecFrame(t, svc.promReg, "banyandb_vec_frame_encoded")
	require.Equal(t, before["stream"]+1, after["stream"],
		"an encoded frame must be visible on the scrape endpoint")
	require.Equal(t, before["trace"], after["trace"], "a stream frame must not move the trace series")
	require.Equal(t, before["measure"], after["measure"], "a stream frame must not move the measure series")

	decoded := gatherVecFrame(t, svc.promReg, "banyandb_vec_frame_decoded")
	require.Contains(t, decoded, "stream", "the decoded family must be published too")
}

// TestCollectVecFrameBeforeInit asserts the collector is safe before the gauges are
// built. It is registered from an init() but the gauges are created during Serve,
// so the metrics loop can reach it first — and a metric service that panics on
// startup would take the node with it.
func TestCollectVecFrameBeforeInit(t *testing.T) {
	vecFrameGaugesMu.Lock()
	savedEnc, savedDec := vecFrameEncodedGge, vecFrameDecodedGge
	vecFrameEncodedGge, vecFrameDecodedGge = nil, nil
	vecFrameGaugesMu.Unlock()
	defer func() {
		vecFrameGaugesMu.Lock()
		vecFrameEncodedGge, vecFrameDecodedGge = savedEnc, savedDec
		vecFrameGaugesMu.Unlock()
	}()

	require.NotPanics(t, collectVecFrame)
}
