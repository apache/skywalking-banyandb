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
	"net/http"
	"net/http/httptest"
	"testing"

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
