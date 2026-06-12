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

package lifecycle

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/observability"
)

// TestBuildLocalNodeMetadataHasDataRole guards the C1 failure mode: the
// co-located data node MUST be registered with ROLE_DATA, or the pub client's
// role gate silently drops it and native metrics never reach the data node.
func TestBuildLocalNodeMetadataHasDataRole(t *testing.T) {
	md := buildLocalNodeMD("127.0.0.1:17912")
	node, ok := md.Spec.(*databasev1.Node)
	require.True(t, ok)
	require.Equal(t, metricsLocalNodeName, node.GetMetadata().GetName())
	require.Equal(t, "127.0.0.1:17912", node.GetGrpcAddress())
	require.Contains(t, node.GetRoles(), databasev1.Role_ROLE_DATA,
		"local node must carry ROLE_DATA or the pub role gate silently drops it")
}

// TestNativeNodeContextSetsIdentity asserts the native node identity is injected
// with a non-empty NodeID so per-pod _monitoring series stay distinct.
func TestNativeNodeContextSetsIdentity(t *testing.T) {
	ctx := nativeNodeContext(context.Background())
	value := ctx.Value(common.ContextNodeKey)
	require.NotNil(t, value)
	n, ok := value.(common.Node)
	require.True(t, ok)
	require.NotEmpty(t, n.NodeID)
}

// stubPromRegistry is a MetricsRegistry that also exposes a Prometheus handler,
// used to exercise buildHTTPRouter's /metrics mounting without a real registry.
type stubPromRegistry struct {
	observability.MetricsRegistry
	handler http.Handler
}

func (s stubPromRegistry) PrometheusHandler() http.Handler { return s.handler }

// TestBuildHTTPRouterServesMetricsAndAPI asserts /metrics is mounted on the same
// router as /api (port reuse, requirement #1) without the two routes colliding.
func TestBuildHTTPRouterServesMetricsAndAPI(t *testing.T) {
	metricsHit, apiHit := false, false
	l := &lifecycleService{
		omr: stubPromRegistry{
			MetricsRegistry: observability.BypassRegistry,
			handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				metricsHit = true
				w.WriteHeader(http.StatusOK)
			}),
		},
	}
	apiHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		apiHit = true
		w.WriteHeader(http.StatusOK)
	})
	router := l.buildHTTPRouter(apiHandler)

	metricsRec := httptest.NewRecorder()
	router.ServeHTTP(metricsRec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	require.Equal(t, http.StatusOK, metricsRec.Code)
	require.True(t, metricsHit, "/metrics must be routed to the prometheus handler")

	apiRec := httptest.NewRecorder()
	router.ServeHTTP(apiRec, httptest.NewRequest(http.MethodGet, "/api/foo", nil))
	require.Equal(t, http.StatusOK, apiRec.Code)
	require.True(t, apiHit, "/api must still route to the gateway handler")
}

// TestBuildHTTPRouterWithoutPromHandler asserts that when the registry does not
// expose a Prometheus handler (e.g. BypassRegistry), /metrics is simply absent
// and /api keeps working.
func TestBuildHTTPRouterWithoutPromHandler(t *testing.T) {
	l := &lifecycleService{omr: observability.BypassRegistry}
	router := l.buildHTTPRouter(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	require.Equal(t, http.StatusNotFound, rec.Code)
}

// recordingGauge captures the Set() and Delete() calls so a unit test
// can assert the value the lifecycle would have emitted AND the
// staleness-prevention Delete that recordLastRun issues before each
// Set. The lifecycle uses the real prometheus-backed Gauge in
// production; this stub keeps the test hermetic. It also records the
// labels argument so tests can assert the (remote_node, remote_role,
// remote_tier, group) tuple stamped by the per-group recordCycleGroup
// and the cycle-end recordLastRun paths.
type recordingGauge struct {
	lastLabels   []string
	deletedLabel [][]string
	lastValue    float64
	called       int
	deleted      int
}

func (g *recordingGauge) Set(v float64, labels ...string) {
	g.lastValue = v
	g.lastLabels = labels
	g.called++
}

func (g *recordingGauge) Add(_ float64, _ ...string) {}

func (g *recordingGauge) Delete(labels ...string) bool {
	g.deleted++
	g.deletedLabel = append(g.deletedLabel, append([]string{}, labels...))
	return true
}

// recordingCounter captures the last Inc() call's label set.
type recordingCounter struct {
	lastLabels []string
	called     int
}

func (c *recordingCounter) Inc(_ float64, labels ...string) {
	c.lastLabels = labels
	c.called++
}

func (c *recordingCounter) Add(_ float64, _ ...string) {}

func (c *recordingCounter) Delete(_ ...string) bool { return true }

// TestRecordCycleGroupStampsLabeledMetrics asserts the per-group helper
// issues an Inc on cyclesTotal with the
// (remote_node, remote_role, remote_tier, group) tuple and captures
// the cycle's last-seen (group, remote_*) tuple on the service for
// the deferred recordLastRun. lastRunTimestamp and lastRunSuccess are
// intentionally NOT touched by recordCycleGroup — they are stamped
// atomically at cycle end so dashboards see consistent (timestamp,
// success) pairs for the same tuple and the success flag reflects the
// whole-cycle outcome.
func TestRecordCycleGroupStampsLabeledMetrics(t *testing.T) {
	cyc, ts, ok := &recordingCounter{}, &recordingGauge{}, &recordingGauge{}
	l := &lifecycleService{
		cyclesTotal:      cyc,
		lastRunTimestamp: ts,
		lastRunSuccess:   ok,
	}
	l.recordCycleGroup("metrics-day", "data-hot-0:17912", "lifecycle", "hot")

	require.Equal(t, 1, cyc.called)
	require.Equal(t,
		[]string{"data-hot-0:17912", "lifecycle", "hot", "metrics-day"},
		cyc.lastLabels,
		"cyclesTotal must be Inc'd with (remote_node, remote_role, remote_tier, group)")
	require.Equal(t, 0, ts.called,
		"lastRunTimestamp must NOT be Set by recordCycleGroup (deferred recordLastRun's job)")
	require.Equal(t, 0, ok.called,
		"lastRunSuccess must NOT be Set by recordCycleGroup (deferred recordLastRun's job)")
	require.Equal(t, "data-hot-0:17912", l.lastRunNode)
	require.Equal(t, "lifecycle", l.lastRunRole)
	require.Equal(t, "hot", l.lastRunTier)
	require.Equal(t, "metrics-day", l.lastRunGroup,
		"lastRunGroup/Node/Role/Tier are the inputs to the deferred recordLastRun")
}

// TestRecordLastRunResetsTupleAtActionStart asserts action() resets the
// cycle's last-seen (group, remote_*) tuple to empty strings so an
// empty cycle (no parseGroup succeeded) doesn't inherit the previous
// cycle's labels. Scheduler-driven consecutive cycles would otherwise
// see a stale group label on last_run_*.
func TestRecordLastRunResetsTupleAtActionStart(t *testing.T) {
	ts, ok := &recordingGauge{}, &recordingGauge{}
	l := &lifecycleService{
		lastRunTimestamp: ts,
		lastRunSuccess:   ok,
		// Stale labels from a previous cycle; action() must clear them.
		lastRunGroup: "stale-group",
		lastRunNode:  "stale-node:17912",
		lastRunRole:  "lifecycle",
		lastRunTier:  "stale",
	}
	// Simulate the action() prelude: reset, then call recordLastRun
	// without any recordCycleGroup in between (empty cycle).
	l.lastRunGroup = ""
	l.lastRunNode = ""
	l.lastRunRole = ""
	l.lastRunTier = ""
	l.recordLastRun(time.Unix(1717929900, 0), nil)

	require.Equal(t, 1, ts.called)
	require.Equal(t, []string{"", "", "", ""}, ts.lastLabels,
		"empty-cycle path must stamp gauges with empty (group, remote_*) labels")
	require.Equal(t, []string{"", "", "", ""}, ok.lastLabels)
}

// TestRecordLastRunSuccess stamps the gauges with the start time (epoch
// seconds) and success=1 when the action returned nil. Asserts both the
// integer epoch shape and the 0/1 success signal so dashboards can
// distinguish a healthy last run from a failed one. The label set is
// sourced from the cycle's last-seen tuple (set by recordCycleGroup),
// so we wire one in before the call.
func TestRecordLastRunSuccess(t *testing.T) {
	tsGauge, okGauge := &recordingGauge{}, &recordingGauge{}
	l := &lifecycleService{
		lastRunTimestamp: tsGauge,
		lastRunSuccess:   okGauge,
		lastRunGroup:     "metrics-day",
		lastRunNode:      "data-hot-0:17912",
		lastRunRole:      "lifecycle",
		lastRunTier:      "hot",
	}
	start := time.Unix(1717929600, 0) // 2024-06-09T00:00:00Z, deterministic
	l.recordLastRun(start, nil)

	require.Equal(t, 1, tsGauge.called)
	require.Equal(t, 1717929600.0, tsGauge.lastValue,
		"lastRunTimestamp must record the start time as epoch seconds")
	require.Equal(t,
		[]string{"data-hot-0:17912", "lifecycle", "hot", "metrics-day"},
		tsGauge.lastLabels,
		"lastRunTimestamp must be Set with the cycle's (remote_node, remote_role, remote_tier, group) tuple")
	require.Equal(t, 0, tsGauge.deleted,
		"first-ever recordLastRun must NOT issue a Delete (no previous tuple to clean up)")
	require.Equal(t, 1, okGauge.called)
	require.Equal(t, 1.0, okGauge.lastValue,
		"lastRunSuccess must be 1 on a nil error")
	require.Equal(t,
		[]string{"data-hot-0:17912", "lifecycle", "hot", "metrics-day"},
		okGauge.lastLabels,
		"lastRunSuccess must be Set with the cycle's (remote_node, remote_role, remote_tier, group) tuple")
	require.Equal(t, 0, okGauge.deleted)
}

// TestRecordLastRunTwoCycleReplaceStaleSeries is the regression test
// for the staleness issue: two consecutive recordLastRun calls with
// DIFFERENT (group, remote_*) tuples. The first call stamps
// ("metrics-day", "data-hot-0", "lifecycle", "hot") and updates
// emittedLastRun*. The second call must Delete that tuple before
// stamping the new ("metrics-hour", "data-warm-0", "lifecycle",
// "warm") tuple, so Prometheus doesn't accumulate a stale series
// shadowing the new one. Without the emittedLastRun* tracking and
// the Delete-before-Set, cycle B's series would coexist with cycle
// A's and dashboards could read either as "current".
func TestRecordLastRunTwoCycleReplaceStaleSeries(t *testing.T) {
	tsGauge, okGauge := &recordingGauge{}, &recordingGauge{}
	l := &lifecycleService{
		lastRunTimestamp: tsGauge,
		lastRunSuccess:   okGauge,
		lastRunGroup:     "metrics-day",
		lastRunNode:      "data-hot-0:17912",
		lastRunRole:      "lifecycle",
		lastRunTier:      "hot",
	}

	// Cycle A: stamp the hot path.
	l.recordLastRun(time.Unix(1717929600, 0), nil)
	require.Equal(t, 1, tsGauge.called)
	require.Equal(t, 1, okGauge.called)
	require.Equal(t, 0, tsGauge.deleted)
	require.Equal(t, 0, okGauge.deleted)
	require.Equal(t,
		[]string{"data-hot-0:17912", "lifecycle", "hot", "metrics-day"},
		tsGauge.lastLabels,
		"cycle A must stamp the hot-path tuple")

	// Cycle B: action() resets lastRun* and the new cycle's
	// recordCycleGroup overwrites them with the warm-path tuple.
	l.lastRunGroup = ""
	l.lastRunNode = ""
	l.lastRunRole = ""
	l.lastRunTier = ""
	l.lastRunGroup = "metrics-hour"
	l.lastRunNode = "data-warm-0:17912"
	l.lastRunRole = "lifecycle"
	l.lastRunTier = "warm"
	l.recordLastRun(time.Unix(1717929700, 0), nil)

	// Cycle B must Delete cycle A's tuple before stamping the new one.
	require.Equal(t, 1, tsGauge.deleted,
		"second recordLastRun must Delete the previous tuple to prevent stale-series shadowing")
	require.Equal(t,
		[]string{"data-hot-0:17912", "lifecycle", "hot", "metrics-day"},
		tsGauge.deletedLabel[0],
		"Delete must target the cycle A tuple (the previously-emitted one)")
	require.Equal(t, 2, tsGauge.called)
	require.Equal(t, 1717929700.0, tsGauge.lastValue)
	require.Equal(t,
		[]string{"data-warm-0:17912", "lifecycle", "warm", "metrics-hour"},
		tsGauge.lastLabels,
		"cycle B must stamp the warm-path tuple after deleting the hot-path one")
	require.Equal(t, 1, okGauge.deleted)
}

// TestRecordLastRunFailure stamps the gauges with success=0 when the action
// returned an error. The timestamp is still set — operators want to know
// "when did the last attempt happen, and did it succeed?".
func TestRecordLastRunFailure(t *testing.T) {
	tsGauge, okGauge := &recordingGauge{}, &recordingGauge{}
	l := &lifecycleService{
		lastRunTimestamp: tsGauge,
		lastRunSuccess:   okGauge,
		lastRunGroup:     "metrics-day",
		lastRunNode:      "data-warm-1:17912",
		lastRunRole:      "lifecycle",
		lastRunTier:      "warm",
	}
	start := time.Unix(1717929700, 0)
	l.recordLastRun(start, errors.New("snapshot dir unavailable"))

	require.Equal(t, 1717929700.0, tsGauge.lastValue)
	require.Equal(t, 0.0, okGauge.lastValue,
		"lastRunSuccess must be 0 on a non-nil error")
}

// TestRecordLastRunNilGaugesSafe ensures nil observability gauges (e.g. when
// the metrics registry is BypassRegistry and never wired) don't crash
// the deferred bookkeeping. This is the path the no-op PreRun takes.
func TestRecordLastRunNilGaugesSafe(t *testing.T) {
	l := &lifecycleService{}
	require.NotPanics(t, func() {
		l.recordLastRun(time.Now(), nil)
		l.recordLastRun(time.Now(), errors.New("boom"))
	})
}
