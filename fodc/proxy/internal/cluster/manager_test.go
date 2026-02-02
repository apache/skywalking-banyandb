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

package cluster

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
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

func (m *mockRequestSender) RequestClusterData(agentID string) error {
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
	initErr := logger.Init(logger.Logging{Env: "dev", Level: "debug"})
	require.NoError(t, initErr)
	return logger.GetLogger("test", "cluster")
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

func TestManager_UpdateClusterTopology(t *testing.T) {
	log := initTestLogger(t)
	testRegistry := registry.NewAgentRegistry(log, 5*time.Second, 10*time.Second, 100)
	mgr := NewManager(testRegistry, nil, log)

	node := &databasev1.Node{
		Metadata: &commonv1.Metadata{
			Name: "test-node",
		},
	}
	topology := &fodcv1.Topology{
		Nodes: []*databasev1.Node{node},
		Calls: []*fodcv1.Call{
			{
				Id:     "call-1",
				Target: "target-node",
				Source: "source-node",
			},
		},
	}

	// Update topology without active collection - should not error
	mgr.UpdateClusterTopology("agent1", topology)

	// With no agents registered, CollectClusterTopology should return empty
	aggregatedTopology := mgr.CollectClusterTopology(context.Background())
	require.NotNil(t, aggregatedTopology)
	assert.Empty(t, aggregatedTopology.Nodes)
	assert.Empty(t, aggregatedTopology.Calls)
}

func TestManager_RemoveTopology(t *testing.T) {
	log := initTestLogger(t)
	testRegistry := registry.NewAgentRegistry(log, 5*time.Second, 10*time.Second, 100)
	mgr := NewManager(testRegistry, nil, log)

	// RemoveTopology should not error even if no collection is active
	mgr.RemoveTopology("agent1")

	// With no agents registered, CollectClusterTopology should return empty
	aggregatedTopology := mgr.CollectClusterTopology(context.Background())
	require.NotNil(t, aggregatedTopology)
	assert.Empty(t, aggregatedTopology.Nodes)
	assert.Empty(t, aggregatedTopology.Calls)
}

func TestManager_CollectClusterTopology_MultipleAgents(t *testing.T) {
	log := initTestLogger(t)
	testRegistry := registry.NewAgentRegistry(log, 5*time.Second, 10*time.Second, 100)
	defer testRegistry.Stop()
	mockSender := newMockRequestSender()
	mgr := NewManager(testRegistry, mockSender, log)

	// Register agents
	ctx := context.Background()
	identity1 := registry.AgentIdentity{
		Role:           "test-role",
		PodName:        "test-pod-1",
		ContainerNames: []string{"container1"},
		Labels:         map[string]string{"env": "test"},
	}
	identity2 := registry.AgentIdentity{
		Role:           "test-role",
		PodName:        "test-pod-2",
		ContainerNames: []string{"container2"},
		Labels:         map[string]string{"env": "test"},
	}
	agentID1, err1 := testRegistry.RegisterAgent(ctx, identity1)
	require.NoError(t, err1)
	agentID2, err2 := testRegistry.RegisterAgent(ctx, identity2)
	require.NoError(t, err2)

	topology1 := &fodcv1.Topology{
		Nodes: []*databasev1.Node{
			{
				Metadata: &commonv1.Metadata{Name: "node1"},
			},
		},
		Calls: []*fodcv1.Call{
			{
				Id:     "call-1",
				Target: "node1",
				Source: "node2",
			},
		},
	}

	topology2 := &fodcv1.Topology{
		Nodes: []*databasev1.Node{
			{
				Metadata: &commonv1.Metadata{Name: "node2"},
			},
		},
		Calls: []*fodcv1.Call{
			{
				Id:     "call-2",
				Target: "node2",
				Source: "node3",
			},
		},
	}

	// Start collection in a goroutine
	done := make(chan *TopologyMap)
	go func() {
		done <- mgr.CollectClusterTopology(context.Background())
	}()

	// Give it a moment to set up channels and request data
	time.Sleep(50 * time.Millisecond)

	// Verify requests were made
	requestedAgents := mockSender.GetRequestedAgents()
	assert.Contains(t, requestedAgents, agentID1)
	assert.Contains(t, requestedAgents, agentID2)

	// Update topology - this should send to the collection channels
	mgr.UpdateClusterTopology(agentID1, topology1)
	mgr.UpdateClusterTopology(agentID2, topology2)

	// Wait for collection to complete (with timeout)
	select {
	case aggregatedTopology := <-done:
		require.NotNil(t, aggregatedTopology)
		// Should have 2 unique nodes (node1 and node2)
		assert.Equal(t, 2, len(aggregatedTopology.Nodes))
		// Should have 2 unique calls
		assert.Equal(t, 2, len(aggregatedTopology.Calls))
	case <-time.After(2 * time.Second):
		t.Fatal("Collection timed out")
	}
}

func TestManager_AggregateTopologies_WithPodNameAndAgentStatus(t *testing.T) {
	log := initTestLogger(t)
	testRegistry := registry.NewAgentRegistry(log, 5*time.Second, 10*time.Second, 100)
	defer testRegistry.Stop()
	mgr := NewManager(testRegistry, nil, log)

	// Register an agent
	ctx := context.Background()
	identity := registry.AgentIdentity{
		Role:           "ROLE_DATA",
		PodName:        "test-pod-1",
		ContainerNames: []string{"container1"},
		Labels:         map[string]string{"env": "test"},
	}
	agentID, err := testRegistry.RegisterAgent(ctx, identity)
	require.NoError(t, err)

	// Update heartbeat
	testRegistry.UpdateHeartbeat(agentID)
	agentInfo, getErr := testRegistry.GetAgentByID(agentID)
	require.NoError(t, getErr)

	// Create topology with nodes that have pod_name labels
	node1 := &databasev1.Node{
		Metadata: &commonv1.Metadata{Name: "node1"},
		Labels:   map[string]string{"pod_name": "test-pod-1"},
		Roles:    []databasev1.Role{databasev1.Role_ROLE_DATA},
	}
	node2 := &databasev1.Node{
		Metadata: &commonv1.Metadata{Name: "node2"},
		Labels:   map[string]string{"pod_name": "test-pod-2"}, // No matching agent
		Roles:    []databasev1.Role{databasev1.Role_ROLE_DATA},
	}
	node3 := &databasev1.Node{
		Metadata: &commonv1.Metadata{Name: "node3"},
		// No pod_name label
		Roles: []databasev1.Role{databasev1.Role_ROLE_DATA},
	}

	topology := &fodcv1.Topology{
		Nodes: []*databasev1.Node{node1, node2, node3},
		Calls: []*fodcv1.Call{},
	}

	topologyMap := convertTopologyToMap(topology)
	aggregatedTopology := mgr.aggregateTopologies([]*TopologyMap{topologyMap})

	require.NotNil(t, aggregatedTopology)
	assert.Equal(t, 3, len(aggregatedTopology.Nodes))

	// Find nodes by name
	nodeMap := make(map[string]*NodeWithStringRoles)
	for _, node := range aggregatedTopology.Nodes {
		if node.Metadata != nil {
			nodeMap[node.Metadata.Name] = node
		}
	}

	// node1 should have Status and LastHeartbeat from matching agent
	node1Result := nodeMap["node1"]
	require.NotNil(t, node1Result)
	assert.NotNil(t, node1Result.Status)
	assert.Equal(t, agentInfo.Status, *node1Result.Status)
	assert.NotNil(t, node1Result.LastHeartbeat)
	assert.WithinDuration(t, agentInfo.LastHeartbeat, *node1Result.LastHeartbeat, time.Second)

	// node2 should not have Status/LastHeartbeat (no matching agent)
	node2Result := nodeMap["node2"]
	require.NotNil(t, node2Result)
	assert.Nil(t, node2Result.Status)
	assert.Nil(t, node2Result.LastHeartbeat)

	// node3 should not have Status/LastHeartbeat (no pod_name label)
	node3Result := nodeMap["node3"]
	require.NotNil(t, node3Result)
	assert.Nil(t, node3Result.Status)
	assert.Nil(t, node3Result.LastHeartbeat)
}

func TestManager_AggregateTopologies_AgentStatusOffline(t *testing.T) {
	log := initTestLogger(t)
	testRegistry := registry.NewAgentRegistry(log, 5*time.Second, 10*time.Second, 100)
	defer testRegistry.Stop()
	mgr := NewManager(testRegistry, nil, log)

	// Register an agent
	ctx := context.Background()
	identity := registry.AgentIdentity{
		Role:           "ROLE_DATA",
		PodName:        "offline-pod",
		ContainerNames: []string{"container1"},
		Labels:         map[string]string{"env": "test"},
	}
	agentID, err := testRegistry.RegisterAgent(ctx, identity)
	require.NoError(t, err)

	// Manually set agent to offline status by manipulating heartbeat timeout
	// We'll wait for health check to mark it offline
	time.Sleep(6 * time.Second) // Wait for health check to run

	// Create topology with node that has matching pod_name
	node := &databasev1.Node{
		Metadata: &commonv1.Metadata{Name: "node1"},
		Labels:   map[string]string{"pod_name": "offline-pod"},
		Roles:    []databasev1.Role{databasev1.Role_ROLE_DATA},
	}

	topology := &fodcv1.Topology{
		Nodes: []*databasev1.Node{node},
		Calls: []*fodcv1.Call{},
	}

	topologyMap := convertTopologyToMap(topology)
	aggregatedTopology := mgr.aggregateTopologies([]*TopologyMap{topologyMap})

	require.NotNil(t, aggregatedTopology)
	assert.Equal(t, 1, len(aggregatedTopology.Nodes))

	nodeResult := aggregatedTopology.Nodes[0]
	require.NotNil(t, nodeResult)
	// Even if agent is offline, Status should still be populated
	assert.NotNil(t, nodeResult.Status)
	// Status should reflect the agent's current status (may be offline)
	agentInfo, getErr := testRegistry.GetAgentByID(agentID)
	if getErr == nil {
		assert.Equal(t, agentInfo.Status, *nodeResult.Status)
	}
}
