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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	fodcv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/fodc/v1"
	"github.com/apache/skywalking-banyandb/fodc/proxy/internal/registry"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

type mockRequestSender struct {
	requestErrors   map[string]error
	requestedAgents []string
	mu              sync.Mutex
}

func newMockRequestSender() *mockRequestSender {
	return &mockRequestSender{
		requestErrors:   make(map[string]error),
		requestedAgents: make([]string, 0),
	}
}

func (m *mockRequestSender) RequestLifecycleData(agentID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.requestedAgents = append(m.requestedAgents, agentID)
	if err, exists := m.requestErrors[agentID]; exists {
		return err
	}
	return nil
}

func (m *mockRequestSender) GetRequestedAgents() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]string, len(m.requestedAgents))
	copy(result, m.requestedAgents)
	return result
}

func (m *mockRequestSender) SetRequestError(agentID string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.requestErrors[agentID] = err
}

func initTestLogger(t *testing.T) *logger.Logger {
	t.Helper()
	err := logger.Init(logger.Logging{Env: "dev", Level: "debug"})
	require.NoError(t, err)
	return logger.GetLogger("test", "lifecycle")
}

func TestNewManager(t *testing.T) {
	log := initTestLogger(t)
	testRegistry := registry.NewAgentRegistry(log, 5*time.Second, 10*time.Second, 100)
	mgr := NewManager(testRegistry, nil, log)

	require.NotNil(t, mgr)
	assert.Equal(t, log, mgr.log)
	assert.NotNil(t, mgr.collecting)
	assert.Empty(t, mgr.collecting)
}

func TestManager_SetGRPCService(t *testing.T) {
	log := initTestLogger(t)
	testRegistry := registry.NewAgentRegistry(log, 5*time.Second, 10*time.Second, 100)
	mgr := NewManager(testRegistry, nil, log)

	mockSender := newMockRequestSender()
	mgr.SetGRPCService(mockSender)
	assert.Equal(t, mockSender, mgr.grpcService)
}

func TestManager_UpdateLifecycle_NoActiveCollection(t *testing.T) {
	log := initTestLogger(t)
	testRegistry := registry.NewAgentRegistry(log, 5*time.Second, 10*time.Second, 100)
	mgr := NewManager(testRegistry, nil, log)

	data := &fodcv1.LifecycleData{
		Reports: []*fodcv1.LifecycleReport{
			{Filename: "2026-03-26.json", ReportJson: "{}"},
		},
	}

	// Should not panic when no collection is active
	mgr.UpdateLifecycle("agent1", "pod-1", data)
}

func TestManager_CollectLifecycle_NoAgents(t *testing.T) {
	log := initTestLogger(t)
	testRegistry := registry.NewAgentRegistry(log, 5*time.Second, 10*time.Second, 100)
	mgr := NewManager(testRegistry, nil, log)

	result := mgr.CollectLifecycle(context.Background())
	require.NotNil(t, result)
	assert.Empty(t, result.Groups)
	assert.Empty(t, result.LifecycleStatuses)
}

func TestManager_CollectLifecycle_NilRegistry(t *testing.T) {
	log := initTestLogger(t)
	mgr := NewManager(nil, nil, log)

	result := mgr.CollectLifecycle(context.Background())
	require.NotNil(t, result)
	assert.Empty(t, result.Groups)
}

func TestManager_CollectLifecycle_MultipleAgents(t *testing.T) {
	log := initTestLogger(t)
	testRegistry := registry.NewAgentRegistry(log, 5*time.Second, 10*time.Second, 100)
	defer testRegistry.Stop()
	mockSender := newMockRequestSender()
	mgr := NewManager(testRegistry, mockSender, log)

	ctx := context.Background()
	identity1 := registry.AgentIdentity{
		Role:           "ROLE_DATA",
		PodName:        "pod-1",
		ContainerNames: []string{"banyandb"},
		Labels:         map[string]string{"env": "test"},
	}
	identity2 := registry.AgentIdentity{
		Role:           "ROLE_DATA",
		PodName:        "pod-2",
		ContainerNames: []string{"banyandb"},
		Labels:         map[string]string{"env": "test"},
	}
	agentID1, err := testRegistry.RegisterAgent(ctx, identity1)
	require.NoError(t, err)
	agentID2, err := testRegistry.RegisterAgent(ctx, identity2)
	require.NoError(t, err)

	data1 := &fodcv1.LifecycleData{
		Reports: []*fodcv1.LifecycleReport{
			{Filename: "2026-03-25.json", ReportJson: "{}"},
		},
	}

	data2 := &fodcv1.LifecycleData{
		Reports: []*fodcv1.LifecycleReport{
			{Filename: "2026-03-24.json", ReportJson: "{}"},
		},
	}

	done := make(chan *InspectionResult)
	go func() {
		done <- mgr.CollectLifecycle(ctx)
	}()

	time.Sleep(50 * time.Millisecond)

	requestedAgents := mockSender.GetRequestedAgents()
	assert.Contains(t, requestedAgents, agentID1)
	assert.Contains(t, requestedAgents, agentID2)

	mgr.UpdateLifecycle(agentID1, "pod-1", data1)
	mgr.UpdateLifecycle(agentID2, "pod-2", data2)

	select {
	case result := <-done:
		require.NotNil(t, result)
		// Groups are collected from liaison (nil in this test), so should be empty
		assert.Empty(t, result.Groups)
		// Should have 2 lifecycle statuses (one per pod)
		assert.Equal(t, 2, len(result.LifecycleStatuses))
		// Verify pod names
		podNames := []string{result.LifecycleStatuses[0].PodName, result.LifecycleStatuses[1].PodName}
		assert.Contains(t, podNames, "pod-1")
		assert.Contains(t, podNames, "pod-2")

	case <-time.After(5 * time.Second):
		t.Fatal("Collection timed out")
	}
}

func TestManager_CollectLifecycle_RequestError(t *testing.T) {
	log := initTestLogger(t)
	testRegistry := registry.NewAgentRegistry(log, 5*time.Second, 10*time.Second, 100)
	defer testRegistry.Stop()
	mockSender := newMockRequestSender()
	mgr := NewManager(testRegistry, mockSender, log)

	ctx := context.Background()
	identity := registry.AgentIdentity{
		Role:           "ROLE_DATA",
		PodName:        "pod-1",
		ContainerNames: []string{"banyandb"},
		Labels:         map[string]string{"env": "test"},
	}
	agentID, err := testRegistry.RegisterAgent(ctx, identity)
	require.NoError(t, err)

	mockSender.SetRequestError(agentID, fmt.Errorf("connection refused"))

	result := mgr.CollectLifecycle(ctx)
	require.NotNil(t, result)
	// Should return empty data since request failed
	assert.Empty(t, result.Groups)
	assert.Empty(t, result.LifecycleStatuses)
}

func TestManager_AggregateLifecycle_Empty(t *testing.T) {
	log := initTestLogger(t)
	mgr := NewManager(nil, nil, log)

	result := mgr.aggregateLifecycle(nil)
	assert.Empty(t, result)

	result = mgr.aggregateLifecycle([]*agentLifecycleData{})
	assert.Empty(t, result)
}

func TestManager_AggregateLifecycle_StatusesConcatenated(t *testing.T) {
	log := initTestLogger(t)
	mgr := NewManager(nil, nil, log)

	ad1 := &agentLifecycleData{
		PodName: "pod-1",
		Data: &fodcv1.LifecycleData{
			Reports: []*fodcv1.LifecycleReport{{Filename: "2026-03-23.json"}},
		},
	}
	ad2 := &agentLifecycleData{
		PodName: "pod-2",
		Data: &fodcv1.LifecycleData{
			Reports: []*fodcv1.LifecycleReport{{Filename: "2026-03-22.json"}},
		},
	}

	result := mgr.aggregateLifecycle([]*agentLifecycleData{ad1, ad2})
	assert.Equal(t, 2, len(result))
	assert.Equal(t, "pod-1", result[0].PodName)
	assert.Equal(t, "pod-2", result[1].PodName)
}

func TestManager_AggregateLifecycle_NilData(t *testing.T) {
	log := initTestLogger(t)
	mgr := NewManager(nil, nil, log)

	result := mgr.aggregateLifecycle([]*agentLifecycleData{nil, nil})
	assert.Empty(t, result)
}

func TestManager_AggregateLifecycle_OnlyReports(t *testing.T) {
	log := initTestLogger(t)
	mgr := NewManager(nil, nil, log)

	ad := &agentLifecycleData{
		PodName: "pod-1",
		Data: &fodcv1.LifecycleData{
			Reports: []*fodcv1.LifecycleReport{{Filename: "2026-03-23.json"}},
		},
	}

	result := mgr.aggregateLifecycle([]*agentLifecycleData{ad})
	assert.Equal(t, 1, len(result))
	assert.Equal(t, "pod-1", result[0].PodName)
}

func TestManager_CollectLifecycle_WithGroups(t *testing.T) {
	log := initTestLogger(t)
	testRegistry := registry.NewAgentRegistry(log, 5*time.Second, 10*time.Second, 100)
	defer testRegistry.Stop()
	mockSender := newMockRequestSender()
	mgr := NewManager(testRegistry, mockSender, log)

	ctx := context.Background()
	agentID, err := testRegistry.RegisterAgent(ctx, registry.AgentIdentity{
		Role: "ROLE_LIAISON", PodName: "liaison-pod", ContainerNames: []string{"banyandb"},
	})
	require.NoError(t, err)

	done := make(chan *InspectionResult)
	go func() { done <- mgr.CollectLifecycle(ctx) }()
	time.Sleep(50 * time.Millisecond)

	mgr.UpdateLifecycle(agentID, "liaison-pod", &fodcv1.LifecycleData{
		Groups: []*fodcv1.GroupLifecycleInfo{
			{Name: "sw_metric", Catalog: "CATALOG_MEASURE"},
			{Name: "sw_record", Catalog: "CATALOG_STREAM"},
		},
	})

	select {
	case result := <-done:
		require.NotNil(t, result)
		assert.Equal(t, 2, len(result.Groups))
		groupNames := []string{result.Groups[0].Name, result.Groups[1].Name}
		assert.Contains(t, groupNames, "sw_metric")
		assert.Contains(t, groupNames, "sw_record")
		assert.Empty(t, result.LifecycleStatuses)
	case <-time.After(5 * time.Second):
		t.Fatal("Collection timed out")
	}
}

func TestManager_CollectLifecycle_GroupsFromFirstAgent(t *testing.T) {
	log := initTestLogger(t)
	testRegistry := registry.NewAgentRegistry(log, 5*time.Second, 10*time.Second, 100)
	defer testRegistry.Stop()
	mockSender := newMockRequestSender()
	mgr := NewManager(testRegistry, mockSender, log)

	ctx := context.Background()
	agentID1, err := testRegistry.RegisterAgent(ctx, registry.AgentIdentity{
		Role: "ROLE_LIAISON", PodName: "liaison-1", ContainerNames: []string{"banyandb"},
	})
	require.NoError(t, err)
	agentID2, err := testRegistry.RegisterAgent(ctx, registry.AgentIdentity{
		Role: "ROLE_DATA", PodName: "data-1", ContainerNames: []string{"banyandb"},
	})
	require.NoError(t, err)

	done := make(chan *InspectionResult)
	go func() { done <- mgr.CollectLifecycle(ctx) }()
	time.Sleep(50 * time.Millisecond)

	mgr.UpdateLifecycle(agentID1, "liaison-1", &fodcv1.LifecycleData{
		Groups: []*fodcv1.GroupLifecycleInfo{
			{Name: "sw_metric", Catalog: "CATALOG_MEASURE"},
		},
	})
	mgr.UpdateLifecycle(agentID2, "data-1", &fodcv1.LifecycleData{
		Reports: []*fodcv1.LifecycleReport{
			{Filename: "2026-03-26.json", ReportJson: "{}"},
		},
	})

	select {
	case result := <-done:
		require.NotNil(t, result)
		assert.Equal(t, 1, len(result.Groups))
		assert.Equal(t, "sw_metric", result.Groups[0].Name)
		assert.Equal(t, 1, len(result.LifecycleStatuses))
		assert.Equal(t, "data-1", result.LifecycleStatuses[0].PodName)
	case <-time.After(5 * time.Second):
		t.Fatal("Collection timed out")
	}
}

func TestManager_CollectLifecycle_ContextCanceled(t *testing.T) {
	log := initTestLogger(t)
	testRegistry := registry.NewAgentRegistry(log, 5*time.Second, 10*time.Second, 100)
	defer testRegistry.Stop()
	mgr := NewManager(testRegistry, nil, log)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	result := mgr.CollectLifecycle(ctx)
	require.NotNil(t, result)
}
