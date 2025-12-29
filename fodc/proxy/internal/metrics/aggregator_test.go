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

package metrics

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	fodcv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/fodc/v1"
	"github.com/apache/skywalking-banyandb/fodc/proxy/internal/registry"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

func initTestLogger(t *testing.T) {
	t.Helper()
	initErr := logger.Init(logger.Logging{Env: "dev", Level: "debug"})
	require.NoError(t, initErr)
}

// mockRequestSender is a mock implementation of RequestSender for testing.
type mockRequestSender struct {
	requestErrors     map[string]error
	requestedAgentIDs []string
	mu                sync.Mutex
	requestCallCount  int
}

func newMockRequestSender() *mockRequestSender {
	return &mockRequestSender{
		requestErrors:     make(map[string]error),
		requestedAgentIDs: make([]string, 0),
	}
}

func (m *mockRequestSender) RequestMetrics(_ context.Context, agentID string, _ *time.Time, _ *time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.requestCallCount++
	m.requestedAgentIDs = append(m.requestedAgentIDs, agentID)
	if err, exists := m.requestErrors[agentID]; exists {
		return err
	}
	return nil
}

func (m *mockRequestSender) GetRequestCallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.requestCallCount
}

func (m *mockRequestSender) GetRequestedAgentIDs() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]string, len(m.requestedAgentIDs))
	copy(result, m.requestedAgentIDs)
	return result
}

func (m *mockRequestSender) SetRequestError(agentID string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.requestErrors[agentID] = err
}

func (m *mockRequestSender) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.requestCallCount = 0
	m.requestedAgentIDs = make([]string, 0)
	m.requestErrors = make(map[string]error)
}

func newTestAggregator(t *testing.T) (*Aggregator, *registry.AgentRegistry, *mockRequestSender) {
	t.Helper()
	initTestLogger(t)
	testLogger := logger.GetLogger("test", "metrics")
	testRegistry := registry.NewAgentRegistry(testLogger, 5*time.Second, 10*time.Second, 100)
	mockSender := newMockRequestSender()
	aggregator := NewAggregator(testRegistry, mockSender, testLogger)
	return aggregator, testRegistry, mockSender
}

//nolint:unparam // port parameter kept for flexibility in future tests
func createTestAgent(t *testing.T, reg *registry.AgentRegistry, ip string, port int, role string, labels map[string]string) string {
	t.Helper()
	ctx := context.Background()
	identity := registry.AgentIdentity{
		IP:     ip,
		Port:   port,
		Role:   role,
		Labels: labels,
	}
	primaryAddr := registry.Address{IP: ip, Port: port}
	agentID, registerErr := reg.RegisterAgent(ctx, identity, primaryAddr)
	require.NoError(t, registerErr)
	return agentID
}

func createTestStreamMetricsRequest(metricName string, metricValue float64, labels map[string]string, timestamp *time.Time) *fodcv1.StreamMetricsRequest {
	req := &fodcv1.StreamMetricsRequest{
		Metrics: []*fodcv1.Metric{
			{
				Name:        metricName,
				Labels:      labels,
				Value:       metricValue,
				Description: "Test metric description",
			},
		},
	}
	if timestamp != nil {
		req.Timestamp = timestamppb.New(*timestamp)
	}
	return req
}

func TestNewAggregator(t *testing.T) {
	initTestLogger(t)
	testLogger := logger.GetLogger("test", "metrics")
	testRegistry := registry.NewAgentRegistry(testLogger, 5*time.Second, 10*time.Second, 100)
	mockSender := newMockRequestSender()

	aggregator := NewAggregator(testRegistry, mockSender, testLogger)

	assert.NotNil(t, aggregator)
	assert.Equal(t, testRegistry, aggregator.registry)
	assert.Equal(t, mockSender, aggregator.grpcService)
	assert.NotNil(t, aggregator.metricsCh)
	assert.NotNil(t, aggregator.collecting)
	assert.Equal(t, 0, len(aggregator.collecting))
}

func TestSetGRPCService(t *testing.T) {
	aggregator, _, _ := newTestAggregator(t)

	newSender := newMockRequestSender()
	aggregator.SetGRPCService(newSender)

	assert.Equal(t, newSender, aggregator.grpcService)
}

func TestProcessMetricsFromAgent_Success(t *testing.T) {
	aggregator, testRegistry, _ := newTestAggregator(t)

	agentID := createTestAgent(t, testRegistry, "192.168.1.1", 8080, "worker", map[string]string{"env": "test"})
	agentInfo, getErr := testRegistry.GetAgentByID(agentID)
	require.NoError(t, getErr)

	ctx := context.Background()
	now := time.Now()
	req := createTestStreamMetricsRequest("cpu_usage", 75.5, map[string]string{"cpu": "0"}, &now)

	err := aggregator.ProcessMetricsFromAgent(ctx, agentID, agentInfo, req)

	require.NoError(t, err)
	aggregator.collectingMu.RLock()
	_, exists := aggregator.collecting[agentID]
	aggregator.collectingMu.RUnlock()
	assert.False(t, exists, "Collection channel should not exist when not collecting")
}

func TestProcessMetricsFromAgent_WithCollectionChannel(t *testing.T) {
	aggregator, testRegistry, _ := newTestAggregator(t)

	agentID := createTestAgent(t, testRegistry, "192.168.1.1", 8080, "worker", nil)
	agentInfo, getErr := testRegistry.GetAgentByID(agentID)
	require.NoError(t, getErr)

	aggregator.collectingMu.Lock()
	collectCh := make(chan []*AggregatedMetric, 1)
	aggregator.collecting[agentID] = collectCh
	aggregator.collectingMu.Unlock()

	ctx := context.Background()
	now := time.Now()
	req := createTestStreamMetricsRequest("cpu_usage", 75.5, map[string]string{"cpu": "0"}, &now)

	err := aggregator.ProcessMetricsFromAgent(ctx, agentID, agentInfo, req)
	require.NoError(t, err)

	select {
	case metrics := <-collectCh:
		require.Equal(t, 1, len(metrics))
		metric := metrics[0]
		assert.Equal(t, "cpu_usage", metric.Name)
		assert.Equal(t, 75.5, metric.Value)
		assert.Equal(t, agentID, metric.AgentID)
		assert.Equal(t, "worker", metric.NodeRole)
		assert.Equal(t, "192.168.1.1", metric.Labels["ip"])
		assert.Equal(t, "8080", metric.Labels["port"])
		assert.Equal(t, agentID, metric.Labels["agent_id"])
		assert.Equal(t, "worker", metric.Labels["node_role"])
		assert.Equal(t, "0", metric.Labels["cpu"])
		assert.WithinDuration(t, now, metric.Timestamp, time.Second)
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for metrics")
	}
}

func TestProcessMetricsFromAgent_WithAgentLabels(t *testing.T) {
	aggregator, testRegistry, _ := newTestAggregator(t)

	agentLabels := map[string]string{"env": "prod", "zone": "us-east"}
	agentID := createTestAgent(t, testRegistry, "192.168.1.1", 8080, "master", agentLabels)
	agentInfo, getErr := testRegistry.GetAgentByID(agentID)
	require.NoError(t, getErr)

	aggregator.collectingMu.Lock()
	collectCh := make(chan []*AggregatedMetric, 1)
	aggregator.collecting[agentID] = collectCh
	aggregator.collectingMu.Unlock()

	ctx := context.Background()
	now := time.Now()
	req := createTestStreamMetricsRequest("memory_usage", 50.0, map[string]string{"type": "heap"}, &now)

	err := aggregator.ProcessMetricsFromAgent(ctx, agentID, agentInfo, req)
	require.NoError(t, err)

	select {
	case metrics := <-collectCh:
		require.Equal(t, 1, len(metrics))
		metric := metrics[0]
		assert.Equal(t, "prod", metric.Labels["env"])
		assert.Equal(t, "us-east", metric.Labels["zone"])
		assert.Equal(t, "master", metric.NodeRole)
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for metrics")
	}
}

func TestProcessMetricsFromAgent_WithoutTimestamp(t *testing.T) {
	aggregator, testRegistry, _ := newTestAggregator(t)

	agentID := createTestAgent(t, testRegistry, "192.168.1.1", 8080, "worker", nil)
	agentInfo, getErr := testRegistry.GetAgentByID(agentID)
	require.NoError(t, getErr)

	aggregator.collectingMu.Lock()
	collectCh := make(chan []*AggregatedMetric, 1)
	aggregator.collecting[agentID] = collectCh
	aggregator.collectingMu.Unlock()

	ctx := context.Background()
	req := createTestStreamMetricsRequest("cpu_usage", 75.5, nil, nil)

	err := aggregator.ProcessMetricsFromAgent(ctx, agentID, agentInfo, req)
	require.NoError(t, err)

	select {
	case metrics := <-collectCh:
		require.Equal(t, 1, len(metrics))
		metric := metrics[0]
		assert.WithinDuration(t, time.Now(), metric.Timestamp, time.Second)
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for metrics")
	}
}

func TestProcessMetricsFromAgent_MultipleMetrics(t *testing.T) {
	aggregator, testRegistry, _ := newTestAggregator(t)

	agentID := createTestAgent(t, testRegistry, "192.168.1.1", 8080, "worker", nil)
	agentInfo, getErr := testRegistry.GetAgentByID(agentID)
	require.NoError(t, getErr)

	aggregator.collectingMu.Lock()
	collectCh := make(chan []*AggregatedMetric, 1)
	aggregator.collecting[agentID] = collectCh
	aggregator.collectingMu.Unlock()

	ctx := context.Background()
	now := time.Now()
	req := &fodcv1.StreamMetricsRequest{
		Metrics: []*fodcv1.Metric{
			{Name: "cpu_usage", Value: 75.5, Labels: map[string]string{"cpu": "0"}},
			{Name: "memory_usage", Value: 50.0, Labels: map[string]string{"type": "heap"}},
			{Name: "disk_usage", Value: 30.0, Labels: map[string]string{"device": "sda"}},
		},
		Timestamp: timestamppb.New(now),
	}

	err := aggregator.ProcessMetricsFromAgent(ctx, agentID, agentInfo, req)
	require.NoError(t, err)

	select {
	case metrics := <-collectCh:
		assert.Equal(t, 3, len(metrics))
		metricNames := make(map[string]bool)
		for _, metric := range metrics {
			metricNames[metric.Name] = true
		}
		assert.True(t, metricNames["cpu_usage"])
		assert.True(t, metricNames["memory_usage"])
		assert.True(t, metricNames["disk_usage"])
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for metrics")
	}
}

func TestProcessMetricsFromAgent_ContextCancelled(t *testing.T) {
	aggregator, testRegistry, _ := newTestAggregator(t)

	agentID := createTestAgent(t, testRegistry, "192.168.1.1", 8080, "worker", nil)
	agentInfo, getErr := testRegistry.GetAgentByID(agentID)
	require.NoError(t, getErr)

	aggregator.collectingMu.Lock()
	collectCh := make(chan []*AggregatedMetric)
	aggregator.collecting[agentID] = collectCh
	aggregator.collectingMu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	now := time.Now()
	req := createTestStreamMetricsRequest("cpu_usage", 75.5, nil, &now)

	err := aggregator.ProcessMetricsFromAgent(ctx, agentID, agentInfo, req)

	assert.Error(t, err)
	assert.Equal(t, context.Canceled, err)
}

func TestCollectMetricsFromAgents_NoAgents(t *testing.T) {
	aggregator, _, _ := newTestAggregator(t)

	ctx := context.Background()
	filter := &Filter{}

	metrics, err := aggregator.CollectMetricsFromAgents(ctx, filter)

	require.NoError(t, err)
	assert.Equal(t, 0, len(metrics))
}

func TestCollectMetricsFromAgents_Success(t *testing.T) {
	aggregator, testRegistry, mockSender := newTestAggregator(t)

	agentID1 := createTestAgent(t, testRegistry, "192.168.1.1", 8080, "worker", nil)
	agentID2 := createTestAgent(t, testRegistry, "192.168.1.2", 8080, "worker", nil)

	ctx := context.Background()
	filter := &Filter{}

	go func() {
		time.Sleep(50 * time.Millisecond)
		agentInfo1, getErr1 := testRegistry.GetAgentByID(agentID1)
		if getErr1 == nil {
			req1 := createTestStreamMetricsRequest("cpu_usage", 75.5, nil, nil)
			processErr1 := aggregator.ProcessMetricsFromAgent(ctx, agentID1, agentInfo1, req1)
			if processErr1 != nil {
				t.Errorf("Failed to process metrics: %v", processErr1)
			}
		}
		agentInfo2, getErr2 := testRegistry.GetAgentByID(agentID2)
		if getErr2 == nil {
			req2 := createTestStreamMetricsRequest("memory_usage", 50.0, nil, nil)
			processErr2 := aggregator.ProcessMetricsFromAgent(ctx, agentID2, agentInfo2, req2)
			if processErr2 != nil {
				t.Errorf("Failed to process metrics: %v", processErr2)
			}
		}
	}()

	metrics, err := aggregator.CollectMetricsFromAgents(ctx, filter)

	require.NoError(t, err)
	assert.Equal(t, 2, len(metrics))
	assert.Equal(t, 2, mockSender.GetRequestCallCount())
	requestedIDs := mockSender.GetRequestedAgentIDs()
	assert.Contains(t, requestedIDs, agentID1)
	assert.Contains(t, requestedIDs, agentID2)
}

func TestCollectMetricsFromAgents_FilterByAgentIDs(t *testing.T) {
	aggregator, testRegistry, mockSender := newTestAggregator(t)

	agentID1 := createTestAgent(t, testRegistry, "192.168.1.1", 8080, "worker", nil)
	agentID2 := createTestAgent(t, testRegistry, "192.168.1.2", 8080, "worker", nil)
	agentID3 := createTestAgent(t, testRegistry, "192.168.1.3", 8080, "master", nil)

	ctx := context.Background()
	filter := &Filter{
		AgentIDs: []string{agentID1, agentID3},
	}

	go func() {
		time.Sleep(50 * time.Millisecond)
		agentInfo1, getErr1 := testRegistry.GetAgentByID(agentID1)
		if getErr1 == nil {
			req1 := createTestStreamMetricsRequest("cpu_usage", 75.5, nil, nil)
			processErr1 := aggregator.ProcessMetricsFromAgent(ctx, agentID1, agentInfo1, req1)
			if processErr1 != nil {
				t.Errorf("Failed to process metrics: %v", processErr1)
			}
		}
		agentInfo3, getErr3 := testRegistry.GetAgentByID(agentID3)
		if getErr3 == nil {
			req3 := createTestStreamMetricsRequest("memory_usage", 50.0, nil, nil)
			processErr3 := aggregator.ProcessMetricsFromAgent(ctx, agentID3, agentInfo3, req3)
			if processErr3 != nil {
				t.Errorf("Failed to process metrics: %v", processErr3)
			}
		}
	}()

	metrics, err := aggregator.CollectMetricsFromAgents(ctx, filter)

	require.NoError(t, err)
	assert.Equal(t, 2, len(metrics))
	assert.Equal(t, 2, mockSender.GetRequestCallCount())
	requestedIDs := mockSender.GetRequestedAgentIDs()
	assert.Contains(t, requestedIDs, agentID1)
	assert.Contains(t, requestedIDs, agentID3)
	assert.NotContains(t, requestedIDs, agentID2)
}

func TestCollectMetricsFromAgents_FilterByRole(t *testing.T) {
	aggregator, testRegistry, mockSender := newTestAggregator(t)

	agentID1 := createTestAgent(t, testRegistry, "192.168.1.1", 8080, "worker", nil)
	agentID2 := createTestAgent(t, testRegistry, "192.168.1.2", 8080, "worker", nil)
	agentID3 := createTestAgent(t, testRegistry, "192.168.1.3", 8080, "master", nil)

	ctx := context.Background()
	filter := &Filter{
		Role: "worker",
	}

	go func() {
		time.Sleep(50 * time.Millisecond)
		agentInfo1, getErr1 := testRegistry.GetAgentByID(agentID1)
		if getErr1 == nil {
			req1 := createTestStreamMetricsRequest("cpu_usage", 75.5, nil, nil)
			processErr1 := aggregator.ProcessMetricsFromAgent(ctx, agentID1, agentInfo1, req1)
			if processErr1 != nil {
				t.Errorf("Failed to process metrics: %v", processErr1)
			}
		}
		agentInfo2, getErr2 := testRegistry.GetAgentByID(agentID2)
		if getErr2 == nil {
			req2 := createTestStreamMetricsRequest("memory_usage", 50.0, nil, nil)
			processErr2 := aggregator.ProcessMetricsFromAgent(ctx, agentID2, agentInfo2, req2)
			if processErr2 != nil {
				t.Errorf("Failed to process metrics: %v", processErr2)
			}
		}
	}()

	metrics, err := aggregator.CollectMetricsFromAgents(ctx, filter)

	require.NoError(t, err)
	assert.Equal(t, 2, len(metrics))
	assert.Equal(t, 2, mockSender.GetRequestCallCount())
	requestedIDs := mockSender.GetRequestedAgentIDs()
	assert.Contains(t, requestedIDs, agentID1)
	assert.Contains(t, requestedIDs, agentID2)
	assert.NotContains(t, requestedIDs, agentID3)
}

func TestCollectMetricsFromAgents_FilterByAddress(t *testing.T) {
	aggregator, testRegistry, mockSender := newTestAggregator(t)

	agentID1 := createTestAgent(t, testRegistry, "192.168.1.1", 8080, "worker", nil)
	agentID2 := createTestAgent(t, testRegistry, "192.168.1.2", 8080, "worker", nil)

	ctx := context.Background()
	filter := &Filter{
		Address: "192.168.1.1:8080",
	}

	go func() {
		time.Sleep(50 * time.Millisecond)
		agentInfo1, getErr1 := testRegistry.GetAgentByID(agentID1)
		if getErr1 == nil {
			req1 := createTestStreamMetricsRequest("cpu_usage", 75.5, nil, nil)
			processErr1 := aggregator.ProcessMetricsFromAgent(ctx, agentID1, agentInfo1, req1)
			if processErr1 != nil {
				t.Errorf("Failed to process metrics: %v", processErr1)
			}
		}
	}()

	metrics, err := aggregator.CollectMetricsFromAgents(ctx, filter)

	require.NoError(t, err)
	assert.Equal(t, 1, len(metrics))
	assert.Equal(t, 1, mockSender.GetRequestCallCount())
	requestedIDs := mockSender.GetRequestedAgentIDs()
	assert.Contains(t, requestedIDs, agentID1)
	assert.NotContains(t, requestedIDs, agentID2)
}

func TestCollectMetricsFromAgents_FilterByIP(t *testing.T) {
	aggregator, testRegistry, mockSender := newTestAggregator(t)

	agentID1 := createTestAgent(t, testRegistry, "192.168.1.1", 8080, "worker", nil)
	agentID2 := createTestAgent(t, testRegistry, "192.168.1.2", 8080, "worker", nil)

	ctx := context.Background()
	filter := &Filter{
		Address: "192.168.1.1",
	}

	go func() {
		time.Sleep(50 * time.Millisecond)
		agentInfo1, getErr1 := testRegistry.GetAgentByID(agentID1)
		if getErr1 == nil {
			req1 := createTestStreamMetricsRequest("cpu_usage", 75.5, nil, nil)
			processErr1 := aggregator.ProcessMetricsFromAgent(ctx, agentID1, agentInfo1, req1)
			if processErr1 != nil {
				t.Errorf("Failed to process metrics: %v", processErr1)
			}
		}
	}()

	metrics, err := aggregator.CollectMetricsFromAgents(ctx, filter)

	require.NoError(t, err)
	assert.Equal(t, 1, len(metrics))
	assert.Equal(t, 1, mockSender.GetRequestCallCount())
	requestedIDs := mockSender.GetRequestedAgentIDs()
	assert.Contains(t, requestedIDs, agentID1)
	assert.NotContains(t, requestedIDs, agentID2)
}

func TestCollectMetricsFromAgents_RequestError(t *testing.T) {
	aggregator, testRegistry, mockSender := newTestAggregator(t)

	agentID1 := createTestAgent(t, testRegistry, "192.168.1.1", 8080, "worker", nil)
	agentID2 := createTestAgent(t, testRegistry, "192.168.1.2", 8080, "worker", nil)

	mockSender.SetRequestError(agentID1, assert.AnError)

	ctx := context.Background()
	filter := &Filter{}

	go func() {
		time.Sleep(50 * time.Millisecond)
		agentInfo2, getErr2 := testRegistry.GetAgentByID(agentID2)
		if getErr2 == nil {
			req2 := createTestStreamMetricsRequest("memory_usage", 50.0, nil, nil)
			processErr2 := aggregator.ProcessMetricsFromAgent(ctx, agentID2, agentInfo2, req2)
			if processErr2 != nil {
				t.Errorf("Failed to process metrics: %v", processErr2)
			}
		}
	}()

	metrics, err := aggregator.CollectMetricsFromAgents(ctx, filter)

	require.NoError(t, err)
	assert.Equal(t, 1, len(metrics))
	assert.Equal(t, 2, mockSender.GetRequestCallCount())
}

func TestCollectMetricsFromAgents_Timeout(t *testing.T) {
	aggregator, testRegistry, _ := newTestAggregator(t)

	createTestAgent(t, testRegistry, "192.168.1.1", 8080, "worker", nil)

	ctx := context.Background()
	filter := &Filter{}

	metrics, err := aggregator.CollectMetricsFromAgents(ctx, filter)

	require.NoError(t, err)
	assert.Equal(t, 0, len(metrics))
}

func TestCollectMetricsFromAgents_WithTimeWindow(t *testing.T) {
	aggregator, testRegistry, mockSender := newTestAggregator(t)

	agentID := createTestAgent(t, testRegistry, "192.168.1.1", 8080, "worker", nil)

	ctx := context.Background()
	startTime := time.Now().Add(-1 * time.Hour)
	endTime := time.Now()
	filter := &Filter{
		StartTime: &startTime,
		EndTime:   &endTime,
	}

	go func() {
		time.Sleep(50 * time.Millisecond)
		agentInfo, getErr := testRegistry.GetAgentByID(agentID)
		if getErr == nil {
			req := createTestStreamMetricsRequest("cpu_usage", 75.5, nil, nil)
			processErr := aggregator.ProcessMetricsFromAgent(ctx, agentID, agentInfo, req)
			if processErr != nil {
				t.Errorf("Failed to process metrics: %v", processErr)
			}
		}
	}()

	metrics, err := aggregator.CollectMetricsFromAgents(ctx, filter)

	require.NoError(t, err)
	assert.Equal(t, 1, len(metrics))
	assert.Equal(t, 1, mockSender.GetRequestCallCount())
}

func TestGetLatestMetrics(t *testing.T) {
	aggregator, testRegistry, mockSender := newTestAggregator(t)

	agentID := createTestAgent(t, testRegistry, "192.168.1.1", 8080, "worker", nil)

	ctx := context.Background()

	go func() {
		time.Sleep(50 * time.Millisecond)
		agentInfo, getErr := testRegistry.GetAgentByID(agentID)
		if getErr == nil {
			req := createTestStreamMetricsRequest("cpu_usage", 75.5, nil, nil)
			processErr := aggregator.ProcessMetricsFromAgent(ctx, agentID, agentInfo, req)
			if processErr != nil {
				t.Errorf("Failed to process metrics: %v", processErr)
			}
		}
	}()

	metrics, err := aggregator.GetLatestMetrics(ctx)

	require.NoError(t, err)
	assert.Equal(t, 1, len(metrics))
	assert.Equal(t, 1, mockSender.GetRequestCallCount())
}

func TestGetMetricsWindow(t *testing.T) {
	aggregator, testRegistry, mockSender := newTestAggregator(t)

	agentID := createTestAgent(t, testRegistry, "192.168.1.1", 8080, "worker", nil)

	ctx := context.Background()
	startTime := time.Now().Add(-1 * time.Hour)
	endTime := time.Now()
	filter := &Filter{Role: "worker"}

	go func() {
		time.Sleep(50 * time.Millisecond)
		agentInfo, getErr := testRegistry.GetAgentByID(agentID)
		if getErr == nil {
			req := createTestStreamMetricsRequest("cpu_usage", 75.5, nil, nil)
			processErr := aggregator.ProcessMetricsFromAgent(ctx, agentID, agentInfo, req)
			if processErr != nil {
				t.Errorf("Failed to process metrics: %v", processErr)
			}
		}
	}()

	metrics, err := aggregator.GetMetricsWindow(ctx, startTime, endTime, filter)

	require.NoError(t, err)
	assert.Equal(t, 1, len(metrics))
	assert.Equal(t, 1, mockSender.GetRequestCallCount())
}

func TestGetMetricsWindow_NilFilter(t *testing.T) {
	aggregator, testRegistry, mockSender := newTestAggregator(t)

	agentID := createTestAgent(t, testRegistry, "192.168.1.1", 8080, "worker", nil)

	ctx := context.Background()
	startTime := time.Now().Add(-1 * time.Hour)
	endTime := time.Now()

	go func() {
		time.Sleep(50 * time.Millisecond)
		agentInfo, getErr := testRegistry.GetAgentByID(agentID)
		if getErr == nil {
			req := createTestStreamMetricsRequest("cpu_usage", 75.5, nil, nil)
			processErr := aggregator.ProcessMetricsFromAgent(ctx, agentID, agentInfo, req)
			if processErr != nil {
				t.Errorf("Failed to process metrics: %v", processErr)
			}
		}
	}()

	metrics, err := aggregator.GetMetricsWindow(ctx, startTime, endTime, nil)

	require.NoError(t, err)
	assert.Equal(t, 1, len(metrics))
	assert.Equal(t, 1, mockSender.GetRequestCallCount())
}

func TestGetFilteredAgents_NilFilter(t *testing.T) {
	aggregator, testRegistry, _ := newTestAggregator(t)

	createTestAgent(t, testRegistry, "192.168.1.1", 8080, "worker", nil)
	createTestAgent(t, testRegistry, "192.168.1.2", 8080, "master", nil)

	agents := aggregator.getFilteredAgents(nil)

	assert.Equal(t, 2, len(agents))
}

func TestGetFilteredAgents_ByAgentIDs(t *testing.T) {
	aggregator, testRegistry, _ := newTestAggregator(t)

	agentID1 := createTestAgent(t, testRegistry, "192.168.1.1", 8080, "worker", nil)
	agentID2 := createTestAgent(t, testRegistry, "192.168.1.2", 8080, "master", nil)
	agentID3 := createTestAgent(t, testRegistry, "192.168.1.3", 8080, "worker", nil)

	filter := &Filter{
		AgentIDs: []string{agentID1, agentID3},
	}

	agents := aggregator.getFilteredAgents(filter)

	assert.Equal(t, 2, len(agents))
	agentIDs := make(map[string]bool)
	for _, agentInfo := range agents {
		agentIDs[agentInfo.AgentID] = true
	}
	assert.True(t, agentIDs[agentID1])
	assert.True(t, agentIDs[agentID3])
	assert.False(t, agentIDs[agentID2])
}

func TestGetFilteredAgents_ByRole(t *testing.T) {
	aggregator, testRegistry, _ := newTestAggregator(t)

	agentID1 := createTestAgent(t, testRegistry, "192.168.1.1", 8080, "worker", nil)
	agentID2 := createTestAgent(t, testRegistry, "192.168.1.2", 8080, "master", nil)
	agentID3 := createTestAgent(t, testRegistry, "192.168.1.3", 8080, "worker", nil)

	filter := &Filter{
		Role: "worker",
	}

	agents := aggregator.getFilteredAgents(filter)

	assert.Equal(t, 2, len(agents))
	agentIDs := make(map[string]bool)
	for _, agentInfo := range agents {
		agentIDs[agentInfo.AgentID] = true
		assert.Equal(t, "worker", agentInfo.NodeRole)
	}
	assert.True(t, agentIDs[agentID1])
	assert.True(t, agentIDs[agentID3])
	assert.False(t, agentIDs[agentID2])
}

func TestGetFilteredAgents_ByAddress(t *testing.T) {
	aggregator, testRegistry, _ := newTestAggregator(t)

	agentID1 := createTestAgent(t, testRegistry, "192.168.1.1", 8080, "worker", nil)
	_ = createTestAgent(t, testRegistry, "192.168.1.2", 8080, "worker", nil)

	filter := &Filter{
		Address: "192.168.1.1:8080",
	}

	agents := aggregator.getFilteredAgents(filter)

	assert.Equal(t, 1, len(agents))
	assert.Equal(t, agentID1, agents[0].AgentID)
}

func TestGetFilteredAgents_ByIP(t *testing.T) {
	aggregator, testRegistry, _ := newTestAggregator(t)

	agentID1 := createTestAgent(t, testRegistry, "192.168.1.1", 8080, "worker", nil)
	_ = createTestAgent(t, testRegistry, "192.168.1.2", 8080, "worker", nil)

	filter := &Filter{
		Address: "192.168.1.1",
	}

	agents := aggregator.getFilteredAgents(filter)

	assert.Equal(t, 1, len(agents))
	assert.Equal(t, agentID1, agents[0].AgentID)
}

func TestGetFilteredAgents_InvalidAgentID(t *testing.T) {
	aggregator, testRegistry, _ := newTestAggregator(t)

	agentID1 := createTestAgent(t, testRegistry, "192.168.1.1", 8080, "worker", nil)

	filter := &Filter{
		AgentIDs: []string{agentID1, "invalid-id"},
	}

	agents := aggregator.getFilteredAgents(filter)

	assert.Equal(t, 1, len(agents))
	assert.Equal(t, agentID1, agents[0].AgentID)
}

func TestMatchesAddress_ExactMatch(t *testing.T) {
	aggregator, testRegistry, _ := newTestAggregator(t)

	agentID := createTestAgent(t, testRegistry, "192.168.1.1", 8080, "worker", nil)
	agentInfo, getErr := testRegistry.GetAgentByID(agentID)
	require.NoError(t, getErr)

	result := aggregator.matchesAddress(agentInfo, "192.168.1.1:8080")

	assert.True(t, result)
}

func TestMatchesAddress_IPMatch(t *testing.T) {
	aggregator, testRegistry, _ := newTestAggregator(t)

	agentID := createTestAgent(t, testRegistry, "192.168.1.1", 8080, "worker", nil)
	agentInfo, getErr := testRegistry.GetAgentByID(agentID)
	require.NoError(t, getErr)

	result := aggregator.matchesAddress(agentInfo, "192.168.1.1")

	assert.True(t, result)
}

func TestMatchesAddress_NoMatch(t *testing.T) {
	aggregator, testRegistry, _ := newTestAggregator(t)

	agentID := createTestAgent(t, testRegistry, "192.168.1.1", 8080, "worker", nil)
	agentInfo, getErr := testRegistry.GetAgentByID(agentID)
	require.NoError(t, getErr)

	result := aggregator.matchesAddress(agentInfo, "192.168.1.2:8080")

	assert.False(t, result)
}

func TestMatchesAddress_PortMismatch(t *testing.T) {
	aggregator, testRegistry, _ := newTestAggregator(t)

	agentID := createTestAgent(t, testRegistry, "192.168.1.1", 8080, "worker", nil)
	agentInfo, getErr := testRegistry.GetAgentByID(agentID)
	require.NoError(t, getErr)

	result := aggregator.matchesAddress(agentInfo, "192.168.1.1:8081")

	assert.False(t, result)
}
