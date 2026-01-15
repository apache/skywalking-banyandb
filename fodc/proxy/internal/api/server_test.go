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
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

func newTestServer(t *testing.T) (*Server, *registry.AgentRegistry) {
	t.Helper()
	initTestLogger(t)
	testLogger := logger.GetLogger("test", "api")
	testRegistry := registry.NewAgentRegistry(testLogger, 5*time.Second, 10*time.Second, 100)
	mockSender := &mockRequestSender{}
	aggregator := metrics.NewAggregator(testRegistry, mockSender, testLogger)
	server := NewServer(aggregator, testRegistry, testLogger)
	return server, testRegistry
}

func createTestMetric(name string, value float64, labels map[string]string, timestamp time.Time) *metrics.AggregatedMetric {
	if labels == nil {
		labels = make(map[string]string)
	}
	return &metrics.AggregatedMetric{
		Name:        name,
		Value:       value,
		Labels:      labels,
		Timestamp:   timestamp,
		AgentID:     "test-agent-1",
		NodeRole:    "worker",
		Description: "Test metric description",
	}
}

func TestNewServer(t *testing.T) {
	initTestLogger(t)
	testLogger := logger.GetLogger("test", "api")
	testRegistry := registry.NewAgentRegistry(testLogger, 5*time.Second, 10*time.Second, 100)
	mockSender := &mockRequestSender{}
	aggregator := metrics.NewAggregator(testRegistry, mockSender, testLogger)

	server := NewServer(aggregator, testRegistry, testLogger)

	assert.NotNil(t, server)
	assert.Equal(t, aggregator, server.metricsAggregator)
	assert.Equal(t, testRegistry, server.registry)
	assert.WithinDuration(t, time.Now(), server.startTime, time.Second)
}

func TestServer_StartStop(t *testing.T) {
	server, _ := newTestServer(t)

	err := server.Start("localhost:0", 5*time.Second, 5*time.Second)
	require.NoError(t, err)
	assert.NotNil(t, server.server)

	time.Sleep(50 * time.Millisecond)

	stopErr := server.Stop()
	assert.NoError(t, stopErr)
}

func TestServer_Stop_NotStarted(t *testing.T) {
	server, _ := newTestServer(t)

	err := server.Stop()
	assert.NoError(t, err)
}

func TestHandleMetrics_Success(t *testing.T) {
	server, _ := newTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()

	server.handleMetrics(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Header().Get("Content-Type"), "text/plain")
}

func TestHandleMetrics_MethodNotAllowed(t *testing.T) {
	server, _ := newTestServer(t)

	req := httptest.NewRequest(http.MethodPost, "/metrics", nil)
	w := httptest.NewRecorder()

	server.handleMetrics(w, req)

	assert.Equal(t, http.StatusMethodNotAllowed, w.Code)
}

func TestHandleMetrics_WithRoleFilter(t *testing.T) {
	server, _ := newTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/metrics?role=worker", nil)
	w := httptest.NewRecorder()

	server.handleMetrics(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestHandleMetrics_WithAddressFilter(t *testing.T) {
	server, _ := newTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/metrics?address=192.168.1.1", nil)
	w := httptest.NewRecorder()

	server.handleMetrics(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestHandleMetrics_CollectionError(t *testing.T) {
	server, _ := newTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req = req.WithContext(ctx)
	w := httptest.NewRecorder()

	server.handleMetrics(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestHandleMetricsWindows_Success(t *testing.T) {
	server, _ := newTestServer(t)

	now := time.Now()
	startTime := now.Add(-1 * time.Hour).Format(time.RFC3339)
	endTime := now.Format(time.RFC3339)

	u, urlErr := url.Parse("/metrics-windows")
	require.NoError(t, urlErr)
	q := u.Query()
	q.Set("start_time", startTime)
	q.Set("end_time", endTime)
	u.RawQuery = q.Encode()

	req := httptest.NewRequest(http.MethodGet, u.String(), nil)
	w := httptest.NewRecorder()

	server.handleMetricsWindows(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))

	var response []map[string]interface{}
	decodeErr := json.NewDecoder(w.Body).Decode(&response)
	require.NoError(t, decodeErr)
}

func TestHandleMetricsWindows_InvalidStartTime(t *testing.T) {
	server, _ := newTestServer(t)

	u, urlErr := url.Parse("/metrics-windows")
	require.NoError(t, urlErr)
	q := u.Query()
	q.Set("start_time", "invalid")
	u.RawQuery = q.Encode()

	req := httptest.NewRequest(http.MethodGet, u.String(), nil)
	w := httptest.NewRecorder()

	server.handleMetricsWindows(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "Invalid start_time format")
}

func TestHandleMetricsWindows_InvalidEndTime(t *testing.T) {
	server, _ := newTestServer(t)

	now := time.Now()
	startTime := now.Add(-1 * time.Hour).Format(time.RFC3339)

	u, urlErr := url.Parse("/metrics-windows")
	require.NoError(t, urlErr)
	q := u.Query()
	q.Set("start_time", startTime)
	q.Set("end_time", "invalid")
	u.RawQuery = q.Encode()

	req := httptest.NewRequest(http.MethodGet, u.String(), nil)
	w := httptest.NewRecorder()

	server.handleMetricsWindows(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "Invalid end_time format")
}

func TestHandleMetricsWindows_MethodNotAllowed(t *testing.T) {
	server, _ := newTestServer(t)

	req := httptest.NewRequest(http.MethodPost, "/metrics-windows", nil)
	w := httptest.NewRecorder()

	server.handleMetricsWindows(w, req)

	assert.Equal(t, http.StatusMethodNotAllowed, w.Code)
}

func TestHandleMetricsWindows_WithoutTimeWindow(t *testing.T) {
	server, _ := newTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/metrics-windows", nil)
	w := httptest.NewRecorder()

	server.handleMetricsWindows(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var response []map[string]interface{}
	decodeErr := json.NewDecoder(w.Body).Decode(&response)
	require.NoError(t, decodeErr)
}

func TestHandleHealth_Success(t *testing.T) {
	server, testRegistry := newTestServer(t)

	ctx := context.Background()
	identity1 := registry.AgentIdentity{IP: "192.168.1.1", Port: 8080, Role: "worker", PodName: "test", ContainerName: "worker"}
	identity2 := registry.AgentIdentity{IP: "192.168.1.2", Port: 8080, Role: "master", PodName: "test", ContainerName: "master"}

	_, err1 := testRegistry.RegisterAgent(ctx, identity1, registry.Address{IP: "192.168.1.1", Port: 8080})
	require.NoError(t, err1)
	_, err2 := testRegistry.RegisterAgent(ctx, identity2, registry.Address{IP: "192.168.1.2", Port: 8080})
	require.NoError(t, err2)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()

	server.handleHealth(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))

	var response map[string]interface{}
	decodeErr := json.NewDecoder(w.Body).Decode(&response)
	require.NoError(t, decodeErr)
	assert.Equal(t, "healthy", response["status"])
	assert.Equal(t, float64(2), response["agents_total"])
	assert.GreaterOrEqual(t, response["uptime_seconds"], float64(0))
}

func TestHandleHealth_MethodNotAllowed(t *testing.T) {
	server, _ := newTestServer(t)

	req := httptest.NewRequest(http.MethodPost, "/health", nil)
	w := httptest.NewRecorder()

	server.handleHealth(w, req)

	assert.Equal(t, http.StatusMethodNotAllowed, w.Code)
}

func TestFormatPrometheusText_Empty(t *testing.T) {
	server, _ := newTestServer(t)

	result := server.formatPrometheusText([]*metrics.AggregatedMetric{})

	assert.Empty(t, result)
}

func TestFormatPrometheusText_SingleMetric(t *testing.T) {
	server, _ := newTestServer(t)

	now := time.Now()
	metricsList := []*metrics.AggregatedMetric{
		createTestMetric("cpu_usage", 75.5, map[string]string{"cpu": "0", "ip": "192.168.1.1", "port": "8080"}, now),
	}

	result := server.formatPrometheusText(metricsList)

	assert.Contains(t, result, "# HELP cpu_usage")
	assert.Contains(t, result, "# TYPE cpu_usage gauge")
	assert.Contains(t, result, "cpu_usage")
	assert.Contains(t, result, "75.5")
}

func TestFormatPrometheusText_MultipleMetrics(t *testing.T) {
	server, _ := newTestServer(t)

	now := time.Now()
	metricsList := []*metrics.AggregatedMetric{
		createTestMetric("cpu_usage", 75.5, map[string]string{"cpu": "0", "ip": "192.168.1.1", "port": "8080"}, now),
		createTestMetric("cpu_usage", 80.0, map[string]string{"cpu": "1", "ip": "192.168.1.1", "port": "8080"}, now),
		createTestMetric("memory_usage", 50.0, map[string]string{"type": "heap", "ip": "192.168.1.1", "port": "8080"}, now),
	}

	result := server.formatPrometheusText(metricsList)

	assert.NotEmpty(t, result)
	assert.Contains(t, result, "cpu_usage")
	assert.Contains(t, result, "memory_usage")
}

func TestFormatPrometheusText_WithLabels(t *testing.T) {
	server, _ := newTestServer(t)

	now := time.Now()
	metricsList := []*metrics.AggregatedMetric{
		createTestMetric("cpu_usage", 75.5, map[string]string{"cpu": "0", "env": "test", "ip": "192.168.1.1", "port": "8080"}, now),
	}

	result := server.formatPrometheusText(metricsList)

	assert.Contains(t, result, "cpu_usage{")
	assert.Contains(t, result, "cpu=\"0\"")
	assert.Contains(t, result, "env=\"test\"")
}

func TestFormatMetricsWindowJSON_Empty(t *testing.T) {
	server, _ := newTestServer(t)

	result := server.formatMetricsWindowJSON([]*metrics.AggregatedMetric{})

	assert.Equal(t, 0, len(result))
}

func TestFormatMetricsWindowJSON_SingleMetric(t *testing.T) {
	server, _ := newTestServer(t)

	now := time.Now()
	metricsList := []*metrics.AggregatedMetric{
		createTestMetric("cpu_usage", 75.5, map[string]string{"cpu": "0", "ip": "192.168.1.1", "port": "8080"}, now),
	}

	result := server.formatMetricsWindowJSON(metricsList)

	assert.Equal(t, 1, len(result))
	item := result[0]
	assert.Equal(t, "cpu_usage", item["name"])
	assert.Equal(t, "test-agent-1", item["agent_id"])
	assert.Equal(t, "192.168.1.1", item["ip"])
	assert.Equal(t, 8080, item["port"])
	assert.Contains(t, item, "data")
}

func TestFormatMetricsWindowJSON_MultipleMetrics(t *testing.T) {
	server, _ := newTestServer(t)

	now := time.Now()
	metricsList := []*metrics.AggregatedMetric{
		createTestMetric("cpu_usage", 75.5, map[string]string{"cpu": "0", "ip": "192.168.1.1", "port": "8080"}, now),
		createTestMetric("memory_usage", 50.0, map[string]string{"type": "heap", "ip": "192.168.1.1", "port": "8080"}, now),
	}

	result := server.formatMetricsWindowJSON(metricsList)

	assert.Equal(t, 2, len(result))
	metricNames := make(map[string]bool)
	for _, item := range result {
		metricNames[item["name"].(string)] = true
	}
	assert.True(t, metricNames["cpu_usage"])
	assert.True(t, metricNames["memory_usage"])
}

func TestGetMetricKey(t *testing.T) {
	server, _ := newTestServer(t)

	metric := createTestMetric("cpu_usage", 75.5, map[string]string{"cpu": "0", "env": "test", "ip": "192.168.1.1", "port": "8080"}, time.Now())

	key := server.getMetricKey(metric)

	assert.Contains(t, key, "cpu_usage")
	assert.Contains(t, key, "test-agent-1")
	assert.Contains(t, key, "cpu=0")
	assert.Contains(t, key, "env=test")
	assert.NotContains(t, key, "ip")
	assert.NotContains(t, key, "port")
}

func TestFormatFloat(t *testing.T) {
	tests := []struct {
		name     string
		expected string
		value    float64
	}{
		{"integer", "100", 100.0},
		{"decimal", "75.5", 75.5},
		{"small", "0.001", 0.001},
		{"large", "1000000", 1000000.0},
		{"negative", "-50.5", -50.5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatFloat(tt.value)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestServer_Integration(t *testing.T) {
	server, _ := newTestServer(t)

	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", server.handleMetrics)
	mux.HandleFunc("/metrics-windows", server.handleMetricsWindows)
	mux.HandleFunc("/health", server.handleHealth)

	testServer := httptest.NewServer(mux)
	defer testServer.Close()

	client := &http.Client{Timeout: 500 * time.Millisecond}

	healthResp, healthErr := client.Get(testServer.URL + "/health")
	require.NoError(t, healthErr)
	assert.Equal(t, http.StatusOK, healthResp.StatusCode)
	healthResp.Body.Close()

	metricsResp, metricsErr := client.Get(testServer.URL + "/metrics")
	if metricsErr == nil {
		assert.Equal(t, http.StatusOK, metricsResp.StatusCode)
		metricsResp.Body.Close()
	}
}

func TestHandleMetricsWindows_WithFilters(t *testing.T) {
	server, _ := newTestServer(t)

	now := time.Now()
	startTime := now.Add(-1 * time.Hour).Format(time.RFC3339)
	endTime := now.Format(time.RFC3339)

	u, urlErr := url.Parse("/metrics-windows")
	require.NoError(t, urlErr)
	q := u.Query()
	q.Set("start_time", startTime)
	q.Set("end_time", endTime)
	q.Set("role", "worker")
	q.Set("address", "192.168.1.1")
	u.RawQuery = q.Encode()

	req := httptest.NewRequest(http.MethodGet, u.String(), nil)
	w := httptest.NewRecorder()

	server.handleMetricsWindows(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var response []map[string]interface{}
	decodeErr := json.NewDecoder(w.Body).Decode(&response)
	require.NoError(t, decodeErr)
}

func TestFormatPrometheusText_SortedMetrics(t *testing.T) {
	server, _ := newTestServer(t)

	now := time.Now()
	metricsList := []*metrics.AggregatedMetric{
		createTestMetric("z_metric", 1.0, map[string]string{"ip": "192.168.1.1", "port": "8080"}, now),
		createTestMetric("a_metric", 2.0, map[string]string{"ip": "192.168.1.1", "port": "8080"}, now),
		createTestMetric("m_metric", 3.0, map[string]string{"ip": "192.168.1.1", "port": "8080"}, now),
	}

	result := server.formatPrometheusText(metricsList)

	aIndex := strings.Index(result, "a_metric")
	mIndex := strings.Index(result, "m_metric")
	zIndex := strings.Index(result, "z_metric")

	assert.True(t, aIndex < mIndex && mIndex < zIndex, "Metrics should be sorted alphabetically")
}
