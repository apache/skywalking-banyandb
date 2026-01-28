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
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	fodcv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/fodc/v1"
	"github.com/apache/skywalking-banyandb/fodc/proxy/internal/cluster"
	"github.com/apache/skywalking-banyandb/fodc/proxy/internal/metrics"
	"github.com/apache/skywalking-banyandb/fodc/proxy/internal/registry"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

func initTestLogger(t *testing.T) {
	t.Helper()
	initErr := logger.Init(logger.Logging{Env: "dev", Level: "debug"})
	require.NoError(t, initErr)
}

// mockRequestSender is a mock implementation of metrics.RequestSender for testing.
type mockRequestSender struct {
	requestErr error
}

func (m *mockRequestSender) RequestMetrics(_ string, _, _ *time.Time) error {
	return m.requestErr
}

// mockClusterDataRequester is a mock implementation of cluster.RequestSender for testing.
type mockClusterDataRequester struct {
	requestErr error
}

func (m *mockClusterDataRequester) RequestClusterData(_ string) error {
	return m.requestErr
}

func newTestServer(t *testing.T) (*Server, *registry.AgentRegistry, *cluster.Manager) {
	t.Helper()
	initTestLogger(t)
	testLogger := logger.GetLogger("test", "api")
	testRegistry := registry.NewAgentRegistry(testLogger, 5*time.Second, 10*time.Second, 100)
	mockSender := &mockRequestSender{}
	aggregator := metrics.NewAggregator(testRegistry, mockSender, testLogger)
	mockRequester := &mockClusterDataRequester{}
	clusterMgr := cluster.NewManager(testRegistry, mockRequester, testLogger)
	server := NewServer(aggregator, clusterMgr, testRegistry, testLogger)
	return server, testRegistry, clusterMgr
}

func TestHandleClusterTopology_Success(t *testing.T) {
	server, testRegistry, clusterMgr := newTestServer(t)
	ctx := context.Background()
	identity := registry.AgentIdentity{
		PodName:        "test-pod",
		Role:           "datanode",
		ContainerNames: []string{"banyandb"},
	}
	agentID, registerErr := testRegistry.RegisterAgent(ctx, identity)
	require.NoError(t, registerErr)
	topology := &fodcv1.ClusterTopology{
		Nodes: []*databasev1.Node{
			{
				Metadata: &commonv1.Metadata{
					Name: "node1",
				},
			},
			{
				Metadata: &commonv1.Metadata{
					Name: "test-node",
				},
			},
		},
		Calls: []*fodcv1.ClusterCall{
			{
				Id:     "call-1",
				Target: "test-node",
				Source: "node1",
			},
		},
	}
	clusterMgr.UpdateClusterTopology(agentID, topology)
	req := httptest.NewRequest(http.MethodGet, "/cluster/topology", nil)
	resp := httptest.NewRecorder()
	server.handleClusterTopology(resp, req)
	assert.Equal(t, http.StatusOK, resp.Code)
	var result fodcv1.ClusterTopology
	decodeErr := json.NewDecoder(resp.Body).Decode(&result)
	require.NoError(t, decodeErr)
	assert.Equal(t, 2, len(result.Nodes))
	assert.Equal(t, 1, len(result.Calls))
	assert.Equal(t, "call-1", result.Calls[0].Id)
}

func TestHandleClusterTopology_NoClusterStateCollector(t *testing.T) {
	initTestLogger(t)
	testLogger := logger.GetLogger("test", "api")
	testRegistry := registry.NewAgentRegistry(testLogger, 5*time.Second, 10*time.Second, 100)
	mockSender := &mockRequestSender{}
	aggregator := metrics.NewAggregator(testRegistry, mockSender, testLogger)
	server := NewServer(aggregator, nil, testRegistry, testLogger)
	req := httptest.NewRequest(http.MethodGet, "/cluster/topology", nil)
	resp := httptest.NewRecorder()
	server.handleClusterTopology(resp, req)
	assert.Equal(t, http.StatusServiceUnavailable, resp.Code)
}

func TestHandleClusterTopology_RequestClusterDataError(t *testing.T) {
	initTestLogger(t)
	testLogger := logger.GetLogger("test", "api")
	testRegistry := registry.NewAgentRegistry(testLogger, 5*time.Second, 10*time.Second, 100)
	mockSender := &mockRequestSender{}
	aggregator := metrics.NewAggregator(testRegistry, mockSender, testLogger)
	mockRequester := &mockClusterDataRequester{requestErr: assert.AnError}
	clusterMgr := cluster.NewManager(testRegistry, mockRequester, testLogger)
	server := NewServer(aggregator, clusterMgr, testRegistry, testLogger)
	ctx := context.Background()
	identity := registry.AgentIdentity{
		PodName:        "test-pod",
		Role:           "datanode",
		ContainerNames: []string{"banyandb"},
	}
	_, registerErr := testRegistry.RegisterAgent(ctx, identity)
	require.NoError(t, registerErr)
	req := httptest.NewRequest(http.MethodGet, "/cluster/topology", nil)
	resp := httptest.NewRecorder()
	server.handleClusterTopology(resp, req)
	assert.Equal(t, http.StatusOK, resp.Code)
}
