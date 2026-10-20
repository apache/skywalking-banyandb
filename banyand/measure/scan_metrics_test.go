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

package measure

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	obsservice "github.com/apache/skywalking-banyandb/banyand/observability/services"
	"github.com/apache/skywalking-banyandb/pkg/meter"
	"github.com/apache/skywalking-banyandb/pkg/meter/prom"
)

// promRegistry is the part of observability.MetricsRegistry the supplier uses, backed
// by a real Prometheus registry so the exposition can be read back.
type promRegistry struct {
	observability.MetricsRegistry
	reg *prometheus.Registry
}

func (r *promRegistry) With(scope meter.Scope) observability.Factory {
	return obsservice.NewFactory(prom.NewProvider(scope, r.reg), nil, nil)
}

// scanMetricNames is every metric the scan path v2 added to the measure query metrics.
var scanMetricNames = []string{
	"total_scan_windows", "total_scan_windows_overlapped", "total_scan_segments_fast", "total_scan_segments_overlap",
	"total_scan_merge_rows", "total_scan_merge_cursors", "scan_merge_peak_live_cursors",
	"scan_prefetch_depth", "scan_decode_queue_wait_seconds", "total_scan_reservations_waiting", "total_scan_reservations_failed",
}

// observeAllScanMetrics touches every scan metric once.
func observeAllScanMetrics(qm *queryMetrics) {
	qm.scanWindows.Inc(1)
	qm.scanWindowsOverlapped.Inc(1)
	qm.scanSegmentsFast.Inc(1)
	qm.scanSegmentsOverlap.Inc(1)
	qm.scanMergeRows.Inc(1)
	qm.scanMergeCursors.Inc(1)
	qm.scanMergePeakLive.Observe(1)
	qm.scanPrefetchDepth.Observe(1)
	qm.scanDecodeQueueWait.Observe(0.001)
	qm.scanReservationsWaiting.Inc(1)
	qm.scanReservationsFailed.Inc(1)
}

func exposedNames(t *testing.T, reg *prometheus.Registry) map[string]int {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	out := map[string]int{}
	for _, mf := range families {
		out[mf.GetName()] = len(mf.GetMetric())
	}
	return out
}

func TestScanMetricsAreRegisteredOnceAndExposedWithTheQueryMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	s := &supplier{omr: &promRegistry{reg: reg}}
	// newMetrics panics on a duplicate registration, so a metric registered twice by
	// the same call fails here.
	s.newMetrics(common.Position{Module: "measure", Database: "g1"})
	qm := s.queryMetricsOf("g1")
	require.NotNil(t, qm)
	observeAllScanMetrics(qm)
	qm.totalQueryResultStarted.Inc(1)

	got := exposedNames(t, reg)
	for _, name := range scanMetricNames {
		require.Contains(t, got, "banyandb_measure_query_"+name, "%s is exposed with the other measure query metrics", name)
	}
	// They share the scope of the metrics scan v2 builds on.
	require.Contains(t, got, "banyandb_measure_query_total_query_result_started")
}

// TestScanMetricsOfAnEarlierGroupSurviveALaterGroupOpening guards the query metrics of
// a node with several groups: opening a second group must not unregister the first
// group's collectors.
func TestScanMetricsOfAnEarlierGroupSurviveALaterGroupOpening(t *testing.T) {
	reg := prometheus.NewRegistry()
	s := &supplier{omr: &promRegistry{reg: reg}}
	s.newMetrics(common.Position{Module: "measure", Database: "g1"})
	first := s.queryMetricsOf("g1")
	observeAllScanMetrics(first)
	s.newMetrics(common.Position{Module: "measure", Database: "g2"})
	observeAllScanMetrics(first) // a measure opened in g1 keeps its own pointer
	observeAllScanMetrics(s.queryMetricsOf("g2"))
	got := exposedNames(t, reg)
	require.Equal(t, 2, got["banyandb_measure_query_total_scan_windows"], "one series per group")
}

func TestReopeningAGroupReplacesOnlyItsOwnQueryMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	s := &supplier{omr: &promRegistry{reg: reg}}
	s.newMetrics(common.Position{Module: "measure", Database: "g1"})
	s.newMetrics(common.Position{Module: "measure", Database: "g2"})
	s.newMetrics(common.Position{Module: "measure", Database: "g1"})
	observeAllScanMetrics(s.queryMetricsOf("g1"))
	observeAllScanMetrics(s.queryMetricsOf("g2"))
	require.Equal(t, 2, exposedNames(t, reg)["banyandb_measure_query_total_scan_windows"])
	require.Nil(t, s.queryMetricsOf("g3"))
}
