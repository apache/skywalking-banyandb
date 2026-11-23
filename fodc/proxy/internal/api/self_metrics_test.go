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

package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// selfMetricsServer is a test server whose own registry holds one gauge, as
// the proxy's native log counters are.
func selfMetricsServer(t *testing.T) *Server {
	t.Helper()
	server := newTestServerOnly(t)
	reg := prometheus.NewRegistry()
	written := prometheus.NewGauge(prometheus.GaugeOpts{Name: "banyandb_logging_native_log_written_total", Help: "written"})
	written.Set(7)
	require.NoError(t, reg.Register(written))
	server.SetSelfMetrics(reg)
	return server
}

// TestHandleMetricsServesTheProxysOwnMetrics covers SetSelfMetrics: the
// proxy's own metrics, such as its native log counters, are served on the
// unfiltered scrape, after the metrics collected from the agents.
func TestHandleMetricsServesTheProxysOwnMetrics(t *testing.T) {
	server := selfMetricsServer(t)
	w := httptest.NewRecorder()
	server.handleMetrics(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	require.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "banyandb_logging_native_log_written_total 7")
}

// TestHandleMetricsLeavesOwnMetricsOutOfAFilteredScrape covers a scrape
// filtered by role or pod: the proxy's own metrics belong to neither, so they
// are not in it.
func TestHandleMetricsLeavesOwnMetricsOutOfAFilteredScrape(t *testing.T) {
	server := selfMetricsServer(t)
	for _, query := range []string{"?role=ROLE_DATA", "?pod_name=data-0"} {
		w := httptest.NewRecorder()
		server.handleMetrics(w, httptest.NewRequest(http.MethodGet, "/metrics"+query, nil))
		require.Equal(t, http.StatusOK, w.Code)
		assert.NotContains(t, w.Body.String(), "banyandb_logging_native_log_written_total", "a scrape filtered by %s", query)
	}
}
