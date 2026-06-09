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
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/banyand/observability"
)

func newTestPromService(t *testing.T, listenerDisabled bool) *metricService {
	t.Helper()
	var reg observability.MetricsRegistry
	if listenerDisabled {
		reg = NewMetricServiceWithoutListener(nil, nil, "test", nil)
	} else {
		reg = NewMetricService(nil, nil, "test", nil)
	}
	svc, ok := reg.(*metricService)
	require.True(t, ok)
	// FlagSet is not parsed in unit scope, so set the fields the validators and
	// Serve rely on directly.
	svc.modes = []string{flagPromethusMode}
	svc.metricsInterval = time.Second
	svc.nativeFlushInterval = time.Second
	return svc
}

// TestMetricServiceValidateFailFast asserts the default constructor still rejects
// an empty listener address (M2: fail-fast preserved for standalone/data/liaison),
// while the listener-disabled constructor permits it.
func TestMetricServiceValidateFailFast(t *testing.T) {
	normal := newTestPromService(t, false)
	require.ErrorIs(t, normal.Validate(), errNoAddr)

	disabled := newTestPromService(t, true)
	require.NoError(t, disabled.Validate(), "listener-disabled service must not require an addr")
}

// TestPrometheusHandlerServesRegisteredSeries asserts the exposed handler serves a
// counter registered through With(scope) - the path the lifecycle uses for its
// banyandb_lifecycle_cycles_total proof series.
func TestPrometheusHandlerServesRegisteredSeries(t *testing.T) {
	svc := newTestPromService(t, true)
	require.NoError(t, svc.PreRun(context.Background()))

	counter := svc.With(observability.RootScope.SubScope("lifecycle")).NewCounter("cycles_total")
	counter.Inc(1)

	rec := httptest.NewRecorder()
	svc.PrometheusHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), "banyandb_lifecycle_cycles_total")
}

// TestListenerDisabledServeBindsNoSocket asserts the listener-disabled service
// neither creates its own http.Server nor blocks on GracefulStop (the closer slot
// must be released even though the listener goroutine never starts).
func TestListenerDisabledServeBindsNoSocket(t *testing.T) {
	svc := newTestPromService(t, true)
	require.NoError(t, svc.PreRun(context.Background()))

	require.NotNil(t, svc.Serve())
	require.Nil(t, svc.svr, "listener-disabled service must not create its own http.Server")

	done := make(chan struct{})
	go func() {
		svc.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("GracefulStop blocked for a listener-disabled service")
	}
}
