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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	fodcv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/fodc/v1"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

func initTestLogger(t *testing.T) *logger.Logger {
	t.Helper()
	initErr := logger.Init(logger.Logging{Env: "dev", Level: "debug"})
	require.NoError(t, initErr)
	return logger.GetLogger("test", "cluster")
}

func TestNewCollector(t *testing.T) {
	log := initTestLogger(t)
	collector := NewCollector(log, []string{"localhost:17914"}, 10*time.Second)
	require.NotNil(t, collector)
	assert.Equal(t, []string{"localhost:17914"}, collector.addrs)
	assert.Equal(t, 10*time.Second, collector.interval)
	assert.NotNil(t, collector.closer)
	assert.False(t, collector.closer.Closed())
}

func TestNewCollector_MultiplePorts(t *testing.T) {
	log := initTestLogger(t)
	collector := NewCollector(log, []string{"localhost:17914", "localhost:17915"}, 10*time.Second)
	require.NotNil(t, collector)
	assert.Equal(t, []string{"localhost:17914", "localhost:17915"}, collector.addrs)
	assert.Equal(t, 10*time.Second, collector.interval)
	assert.NotNil(t, collector.closer)
	assert.False(t, collector.closer.Closed())
}

func TestCollector_Stop_NotStarted(t *testing.T) {
	log := initTestLogger(t)
	collector := NewCollector(log, []string{"localhost:17914"}, 10*time.Second)
	collector.Stop()
	assert.True(t, collector.closer.Closed())
}

func TestCollector_Stop_MultipleCalls(t *testing.T) {
	log := initTestLogger(t)
	collector := NewCollector(log, []string{"localhost:17914"}, 10*time.Second)
	collector.Stop()
	collector.Stop()
	collector.Stop()
	assert.True(t, collector.closer.Closed())
}

func TestCollector_Start_AfterStop(t *testing.T) {
	log := initTestLogger(t)
	collector := NewCollector(log, []string{"localhost:17914"}, 10*time.Second)
	collector.Stop()
	ctx := context.Background()
	err := collector.Start(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "stopped and cannot be restarted")
	assert.True(t, collector.closer.Closed())
}

func TestCollector_FetchClusterStates_NoClient(t *testing.T) {
	log := initTestLogger(t)
	collector := NewCollector(log, []string{"localhost:17914"}, 10*time.Second)
	ctx := context.Background()
	states := collector.fetchClusterStates(ctx)
	assert.Nil(t, states)
}

func TestCollector_FetchCurrentNodes_NoClient(t *testing.T) {
	log := initTestLogger(t)
	collector := NewCollector(log, []string{"localhost:17914"}, 10*time.Second)
	ctx := context.Background()
	nodes := collector.fetchCurrentNodes(ctx)
	assert.Nil(t, nodes)
}

func TestCollector_GetCurrentNode_InitialNil(t *testing.T) {
	log := initTestLogger(t)
	collector := NewCollector(log, []string{"localhost:17914"}, 10*time.Second)
	nodes := collector.GetCurrentNodes()
	assert.Empty(t, nodes)
}

func TestCollector_GetClusterState_InitialNil(t *testing.T) {
	log := initTestLogger(t)
	collector := NewCollector(log, []string{"localhost:17914"}, 10*time.Second)
	topology := collector.GetClusterTopology()
	assert.Empty(t, topology.Nodes)
	assert.Empty(t, topology.Calls)
}

func TestCollector_UpdateCurrentNodes(t *testing.T) {
	log := initTestLogger(t)
	collector := NewCollector(log, []string{"localhost:17914"}, 10*time.Second)
	node := &databasev1.Node{
		Metadata:    &commonv1.Metadata{Name: "test-node"},
		GrpcAddress: "localhost:17913",
		Roles:       []databasev1.Role{databasev1.Role_ROLE_DATA},
		Labels:      map[string]string{"tier": "hot"},
	}
	nodes := map[string]*databasev1.Node{
		"localhost:17914": node,
	}
	collector.updateCurrentNodes(nodes)
	retrievedNodes := collector.GetCurrentNodes()
	assert.Equal(t, node, retrievedNodes["localhost:17914"])
	assert.Equal(t, node, collector.GetCurrentNodes()["localhost:17914"])
}

func TestCollector_UpdateClusterStates(t *testing.T) {
	log := initTestLogger(t)
	collector := NewCollector(log, []string{"localhost:17914"}, 10*time.Second)
	state := &databasev1.GetClusterStateResponse{
		RouteTables: map[string]*databasev1.RouteTable{
			"test": {
				Registered: []*databasev1.Node{},
				Active:     []string{"node1"},
				Evictable:  []string{},
			},
		},
	}
	states := map[string]*databasev1.GetClusterStateResponse{
		"localhost:17914": state,
	}
	collector.updateClusterStates(states)
	topology := collector.GetClusterTopology()
	assert.NotNil(t, topology)
}

func TestCollector_MultipleEndpoints(t *testing.T) {
	log := initTestLogger(t)
	collector := NewCollector(log, []string{"localhost:17914", "localhost:17915"}, 10*time.Second)
	node1 := &databasev1.Node{
		Metadata:    &commonv1.Metadata{Name: "test-node-1"},
		GrpcAddress: "localhost:17913",
		Roles:       []databasev1.Role{databasev1.Role_ROLE_DATA},
		Labels:      map[string]string{"tier": "hot"},
	}
	node2 := &databasev1.Node{
		Metadata:    &commonv1.Metadata{Name: "test-node-2"},
		GrpcAddress: "localhost:17916",
		Roles:       []databasev1.Role{databasev1.Role_ROLE_LIAISON},
		Labels:      map[string]string{"zone": "us-west"},
	}
	nodes := map[string]*databasev1.Node{
		"localhost:17914": node1,
		"localhost:17915": node2,
	}
	collector.updateCurrentNodes(nodes)
	retrievedNodes := collector.GetCurrentNodes()
	assert.Equal(t, 2, len(retrievedNodes))
	assert.Equal(t, node1, retrievedNodes["localhost:17914"])
	assert.Equal(t, node2, retrievedNodes["localhost:17915"])
}

func TestCollector_ProcessClusterData(t *testing.T) {
	log := initTestLogger(t)
	collector := NewCollector(log, []string{"localhost:17914"}, 10*time.Second)
	currentNode := &databasev1.Node{
		Metadata:    &commonv1.Metadata{Name: "current-node"},
		GrpcAddress: "localhost:17913",
		Roles:       []databasev1.Role{databasev1.Role_ROLE_DATA},
		Labels:      map[string]string{"tier": "hot"},
	}
	registeredNode1 := &databasev1.Node{
		Metadata:    &commonv1.Metadata{Name: "registered-node-1"},
		GrpcAddress: "localhost:17920",
		Roles:       []databasev1.Role{databasev1.Role_ROLE_DATA},
	}
	registeredNode2 := &databasev1.Node{
		Metadata:    &commonv1.Metadata{Name: "registered-node-2"},
		GrpcAddress: "localhost:17921",
		Roles:       []databasev1.Role{databasev1.Role_ROLE_DATA},
	}
	clusterState := &databasev1.GetClusterStateResponse{
		RouteTables: map[string]*databasev1.RouteTable{
			"test": {
				Registered: []*databasev1.Node{registeredNode1, registeredNode2},
				Active:     []string{"registered-node-1", "registered-node-2"},
				Evictable:  []string{},
			},
		},
	}
	nodes := map[string]*databasev1.Node{
		"localhost:17914": currentNode,
	}
	states := map[string]*databasev1.GetClusterStateResponse{
		"localhost:17914": clusterState,
	}
	collector.updateCurrentNodes(nodes)
	collector.updateClusterStates(states)
	topology := collector.GetClusterTopology()
	assert.NotNil(t, topology)
	assert.Equal(t, 3, len(topology.Nodes))
	assert.Equal(t, 2, len(topology.Calls))
	assert.Contains(t, topology.Nodes, currentNode)
	assert.Contains(t, topology.Nodes, registeredNode1)
	assert.Contains(t, topology.Nodes, registeredNode2)
	topologyCallMap := make(map[string]*fodcv1.Call)
	for _, call := range topology.Calls {
		topologyCallMap[call.Id] = call
	}
	assert.Contains(t, topologyCallMap, "current-node-registered-node-1")
	assert.Contains(t, topologyCallMap, "current-node-registered-node-2")
}

func TestCollector_MergeMultipleEndpoints(t *testing.T) {
	log := initTestLogger(t)
	collector := NewCollector(log, []string{"localhost:17914", "localhost:17915"}, 10*time.Second)
	currentNode1 := &databasev1.Node{
		Metadata:    &commonv1.Metadata{Name: "node-1"},
		GrpcAddress: "localhost:17913",
		Roles:       []databasev1.Role{databasev1.Role_ROLE_DATA},
	}
	currentNode2 := &databasev1.Node{
		Metadata:    &commonv1.Metadata{Name: "node-2"},
		GrpcAddress: "localhost:17916",
		Roles:       []databasev1.Role{databasev1.Role_ROLE_DATA},
	}
	registeredNode1 := &databasev1.Node{
		Metadata:    &commonv1.Metadata{Name: "registered-1"},
		GrpcAddress: "localhost:17920",
		Roles:       []databasev1.Role{databasev1.Role_ROLE_DATA},
	}
	registeredNode2 := &databasev1.Node{
		Metadata:    &commonv1.Metadata{Name: "registered-2"},
		GrpcAddress: "localhost:17921",
		Roles:       []databasev1.Role{databasev1.Role_ROLE_DATA},
	}
	clusterState1 := &databasev1.GetClusterStateResponse{
		RouteTables: map[string]*databasev1.RouteTable{
			"test": {
				Registered: []*databasev1.Node{registeredNode1},
				Active:     []string{"registered-1"},
				Evictable:  []string{},
			},
		},
	}
	clusterState2 := &databasev1.GetClusterStateResponse{
		RouteTables: map[string]*databasev1.RouteTable{
			"test": {
				Registered: []*databasev1.Node{registeredNode2},
				Active:     []string{"registered-2"},
				Evictable:  []string{},
			},
		},
	}
	nodes := map[string]*databasev1.Node{
		"localhost:17914": currentNode1,
		"localhost:17915": currentNode2,
	}
	states := map[string]*databasev1.GetClusterStateResponse{
		"localhost:17914": clusterState1,
		"localhost:17915": clusterState2,
	}
	collector.updateCurrentNodes(nodes)
	collector.updateClusterStates(states)
	topology := collector.GetClusterTopology()
	assert.NotNil(t, topology)
	assert.Equal(t, 4, len(topology.Nodes))
	assert.Equal(t, 2, len(topology.Calls))
	nodeNames := make(map[string]bool)
	for _, node := range topology.Nodes {
		if node != nil && node.Metadata != nil {
			nodeNames[node.Metadata.Name] = true
		}
	}
	assert.True(t, nodeNames["node-1"])
	assert.True(t, nodeNames["node-2"])
	assert.True(t, nodeNames["registered-1"])
	assert.True(t, nodeNames["registered-2"])
	callMap := make(map[string]*fodcv1.Call)
	for _, call := range topology.Calls {
		callMap[call.Id] = call
	}
	assert.Contains(t, callMap, "node-1-registered-1")
	assert.Contains(t, callMap, "node-2-registered-2")
}

func TestCollector_GetNodeInfo(t *testing.T) {
	log := initTestLogger(t)
	collector := NewCollector(log, []string{"localhost:17914"}, 10*time.Second)
	node := &databasev1.Node{
		Metadata: &commonv1.Metadata{Name: "test-node"},
		Roles:    []databasev1.Role{databasev1.Role_ROLE_DATA},
		Labels:   map[string]string{"tier": "hot", "zone": "us-west"},
	}
	nodes := map[string]*databasev1.Node{
		"localhost:17914": node,
	}
	collector.updateCurrentNodes(nodes)
	nodeRole, nodeLabels := collector.GetNodeInfo()
	assert.Equal(t, "DATA_HOT", nodeRole)
	assert.Equal(t, map[string]string{"tier": "hot", "zone": "us-west"}, nodeLabels)
}

func TestCollector_GetNodeInfo_NoNode(t *testing.T) {
	log := initTestLogger(t)
	collector := NewCollector(log, []string{"localhost:17914"}, 10*time.Second)
	nodeRole, nodeLabels := collector.GetNodeInfo()
	assert.Equal(t, "", nodeRole)
	assert.Nil(t, nodeLabels)
}

func TestGenerateLifecycleAddrs(t *testing.T) {
	tests := []struct {
		name     string
		ports    []string
		expected []string
	}{
		{"empty ports", []string{}, []string{}},
		{"single port", []string{"17914"}, []string{"localhost:17914"}},
		{"multiple ports", []string{"17914", "17915", "17916"}, []string{"localhost:17914", "localhost:17915", "localhost:17916"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GenerateClusterStateAddrs(tt.ports)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestNodeRoleFromNode(t *testing.T) {
	tests := []struct {
		name     string
		node     *databasev1.Node
		expected string
	}{
		{"nil node", nil, "DATA_HOT"},
		{"empty roles", &databasev1.Node{}, "DATA_HOT"},
		{"liaison", &databasev1.Node{Roles: []databasev1.Role{databasev1.Role_ROLE_LIAISON}}, "LIAISON"},
		{"meta", &databasev1.Node{Roles: []databasev1.Role{databasev1.Role_ROLE_META}}, "UNKNOWN"},
		{"data without tier", &databasev1.Node{Roles: []databasev1.Role{databasev1.Role_ROLE_DATA}}, "DATA"},
		{"data with hot tier", &databasev1.Node{Roles: []databasev1.Role{databasev1.Role_ROLE_DATA}, Labels: map[string]string{"tier": "hot"}}, "DATA_HOT"},
		{"data with warm tier", &databasev1.Node{Roles: []databasev1.Role{databasev1.Role_ROLE_DATA}, Labels: map[string]string{"tier": "warm"}}, "DATA_WARM"},
		{"data with cold tier", &databasev1.Node{Roles: []databasev1.Role{databasev1.Role_ROLE_DATA}, Labels: map[string]string{"tier": "cold"}}, "DATA_COLD"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := NodeRoleFromNode(tt.node)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestProcessClusterStates_EmptyInputs(t *testing.T) {
	log := initTestLogger(t)
	collector := NewCollector(log, []string{}, 10*time.Second)

	// Test with empty inputs
	collector.updateClusterStates(map[string]*databasev1.GetClusterStateResponse{})
	topology := collector.GetClusterTopology()
	assert.Empty(t, topology.Nodes)
	assert.Empty(t, topology.Calls)
}

func TestProcessClusterStates_SingleEndpoint(t *testing.T) {
	log := initTestLogger(t)
	collector := NewCollector(log, []string{"localhost:17914"}, 10*time.Second)

	currentNode := &databasev1.Node{
		Metadata:    &commonv1.Metadata{Name: "node-1"},
		GrpcAddress: "localhost:17913",
		Roles:       []databasev1.Role{databasev1.Role_ROLE_DATA},
	}

	registeredNode := &databasev1.Node{
		Metadata:    &commonv1.Metadata{Name: "node-2"},
		GrpcAddress: "localhost:17920",
		Roles:       []databasev1.Role{databasev1.Role_ROLE_DATA},
	}

	clusterState := &databasev1.GetClusterStateResponse{
		RouteTables: map[string]*databasev1.RouteTable{
			"test": {
				Registered: []*databasev1.Node{registeredNode},
				Active:     []string{"node-2"},
				Evictable:  []string{},
			},
		},
	}

	// Set up current nodes first
	nodes := map[string]*databasev1.Node{
		"localhost:17914": currentNode,
	}
	collector.updateCurrentNodes(nodes)

	// Process cluster states
	states := map[string]*databasev1.GetClusterStateResponse{
		"localhost:17914": clusterState,
	}
	collector.updateClusterStates(states)

	topology := collector.GetClusterTopology()
	assert.Equal(t, 2, len(topology.Nodes))
	assert.Equal(t, 1, len(topology.Calls))

	// Check nodes are present
	nodeNames := make(map[string]bool)
	for _, node := range topology.Nodes {
		nodeNames[node.Metadata.Name] = true
	}
	assert.True(t, nodeNames["node-1"])
	assert.True(t, nodeNames["node-2"])

	// Check call is generated
	assert.Equal(t, "node-1-node-2", topology.Calls[0].Id)
	assert.Equal(t, "node-1", topology.Calls[0].Source)
	assert.Equal(t, "node-2", topology.Calls[0].Target)
}

func TestProcessClusterStates_MultipleEndpoints(t *testing.T) {
	log := initTestLogger(t)
	collector := NewCollector(log, []string{"localhost:17914", "localhost:17915"}, 10*time.Second)

	currentNode1 := &databasev1.Node{
		Metadata:    &commonv1.Metadata{Name: "node-1"},
		GrpcAddress: "localhost:17913",
		Roles:       []databasev1.Role{databasev1.Role_ROLE_DATA},
	}

	currentNode2 := &databasev1.Node{
		Metadata:    &commonv1.Metadata{Name: "node-2"},
		GrpcAddress: "localhost:17916",
		Roles:       []databasev1.Role{databasev1.Role_ROLE_DATA},
	}

	registeredNode1 := &databasev1.Node{
		Metadata:    &commonv1.Metadata{Name: "shared-node"},
		GrpcAddress: "localhost:17920",
		Roles:       []databasev1.Role{databasev1.Role_ROLE_DATA},
	}

	registeredNode2 := &databasev1.Node{
		Metadata:    &commonv1.Metadata{Name: "unique-node"},
		GrpcAddress: "localhost:17921",
		Roles:       []databasev1.Role{databasev1.Role_ROLE_DATA},
	}

	clusterState1 := &databasev1.GetClusterStateResponse{
		RouteTables: map[string]*databasev1.RouteTable{
			"test": {
				Registered: []*databasev1.Node{registeredNode1},
				Active:     []string{"shared-node"},
				Evictable:  []string{},
			},
		},
	}

	clusterState2 := &databasev1.GetClusterStateResponse{
		RouteTables: map[string]*databasev1.RouteTable{
			"test": {
				Registered: []*databasev1.Node{registeredNode1, registeredNode2},
				Active:     []string{"shared-node", "unique-node"},
				Evictable:  []string{},
			},
		},
	}

	// Set up current nodes
	nodes := map[string]*databasev1.Node{
		"localhost:17914": currentNode1,
		"localhost:17915": currentNode2,
	}
	collector.updateCurrentNodes(nodes)

	// Process cluster states
	states := map[string]*databasev1.GetClusterStateResponse{
		"localhost:17914": clusterState1,
		"localhost:17915": clusterState2,
	}
	collector.updateClusterStates(states)

	topology := collector.GetClusterTopology()
	assert.Equal(t, 4, len(topology.Nodes)) // node-1, node-2, shared-node, unique-node
	assert.Equal(t, 3, len(topology.Calls)) // node-1->shared-node, node-2->shared-node, node-2->unique-node

	// Check all nodes are present
	nodeNames := make(map[string]bool)
	for _, node := range topology.Nodes {
		nodeNames[node.Metadata.Name] = true
	}
	assert.True(t, nodeNames["node-1"])
	assert.True(t, nodeNames["node-2"])
	assert.True(t, nodeNames["shared-node"])
	assert.True(t, nodeNames["unique-node"])

	// Check calls are generated correctly
	callMap := make(map[string]*fodcv1.Call)
	for _, call := range topology.Calls {
		callMap[call.Id] = call
	}
	assert.Contains(t, callMap, "node-1-shared-node")
	assert.Contains(t, callMap, "node-2-shared-node")
	assert.Contains(t, callMap, "node-2-unique-node")
}

func TestProcessClusterStates_NoCurrentNodeForEndpoint(t *testing.T) {
	log := initTestLogger(t)
	collector := NewCollector(log, []string{"localhost:17914"}, 10*time.Second)

	// Only set cluster state, no current node for this endpoint
	registeredNode := &databasev1.Node{
		Metadata:    &commonv1.Metadata{Name: "node-1"},
		GrpcAddress: "localhost:17920",
		Roles:       []databasev1.Role{databasev1.Role_ROLE_DATA},
	}

	clusterState := &databasev1.GetClusterStateResponse{
		RouteTables: map[string]*databasev1.RouteTable{
			"test": {
				Registered: []*databasev1.Node{registeredNode},
				Active:     []string{"node-1"},
				Evictable:  []string{},
			},
		},
	}

	// Don't set current nodes - this endpoint won't be processed
	states := map[string]*databasev1.GetClusterStateResponse{
		"localhost:17914": clusterState,
	}
	collector.updateClusterStates(states)

	topology := collector.GetClusterTopology()
	// Should be empty since no endpoint has both current node and cluster state
	assert.Empty(t, topology.Nodes)
	assert.Empty(t, topology.Calls)
}

func TestProcessClusterStates_MalformedNodeNames(t *testing.T) {
	log := initTestLogger(t)
	collector := NewCollector(log, []string{"localhost:17914"}, 10*time.Second)

	currentNode := &databasev1.Node{
		Metadata:    &commonv1.Metadata{Name: "node-1"},
		GrpcAddress: "localhost:17913",
		Roles:       []databasev1.Role{databasev1.Role_ROLE_DATA},
	}

	// Test with malformed node names (currently allowed due to TODO in code)
	malformedNode := &databasev1.Node{
		Metadata:    &commonv1.Metadata{Name: ":17914"}, // malformed name
		GrpcAddress: "localhost:17920",
		Roles:       []databasev1.Role{databasev1.Role_ROLE_DATA},
	}

	clusterState := &databasev1.GetClusterStateResponse{
		RouteTables: map[string]*databasev1.RouteTable{
			"test": {
				Registered: []*databasev1.Node{malformedNode},
				Active:     []string{":17914"},
				Evictable:  []string{},
			},
		},
	}

	nodes := map[string]*databasev1.Node{
		"localhost:17914": currentNode,
	}
	collector.updateCurrentNodes(nodes)

	states := map[string]*databasev1.GetClusterStateResponse{
		"localhost:17914": clusterState,
	}
	collector.updateClusterStates(states)

	topology := collector.GetClusterTopology()
	// Should include malformed node names due to current implementation
	assert.Equal(t, 2, len(topology.Nodes))
	assert.Equal(t, 1, len(topology.Calls))

	nodeNames := make(map[string]bool)
	for _, node := range topology.Nodes {
		nodeNames[node.Metadata.Name] = true
	}
	assert.True(t, nodeNames["node-1"])
	assert.True(t, nodeNames[":17914"])
}

func TestProcessClusterStates_NoSelfCalls(t *testing.T) {
	log := initTestLogger(t)
	collector := NewCollector(log, []string{"localhost:17914"}, 10*time.Second)

	currentNode := &databasev1.Node{
		Metadata:    &commonv1.Metadata{Name: "node-1"},
		GrpcAddress: "localhost:17913",
		Roles:       []databasev1.Role{databasev1.Role_ROLE_DATA},
	}

	registeredNode := &databasev1.Node{
		Metadata:    &commonv1.Metadata{Name: "node-2"},
		GrpcAddress: "localhost:17920",
		Roles:       []databasev1.Role{databasev1.Role_ROLE_DATA},
	}

	clusterState := &databasev1.GetClusterStateResponse{
		RouteTables: map[string]*databasev1.RouteTable{
			"test": {
				Registered: []*databasev1.Node{registeredNode},
				Active:     []string{"node-1", "node-2"}, // node-1 is current node, should not create self-call
				Evictable:  []string{},
			},
		},
	}

	nodes := map[string]*databasev1.Node{
		"localhost:17914": currentNode,
	}
	collector.updateCurrentNodes(nodes)

	states := map[string]*databasev1.GetClusterStateResponse{
		"localhost:17914": clusterState,
	}
	collector.updateClusterStates(states)

	topology := collector.GetClusterTopology()
	assert.Equal(t, 2, len(topology.Nodes))
	assert.Equal(t, 1, len(topology.Calls)) // Only node-1 -> node-2, no self-call

	assert.Equal(t, "node-1-node-2", topology.Calls[0].Id)
	assert.Equal(t, "node-1", topology.Calls[0].Source)
	assert.Equal(t, "node-2", topology.Calls[0].Target)
}

func TestProcessClusterStates_ComplexTopology(t *testing.T) {
	log := initTestLogger(t)
	collector := NewCollector(log, []string{"localhost:17914", "localhost:17915", "localhost:17916"}, 10*time.Second)

	// Three current nodes
	currentNode1 := &databasev1.Node{
		Metadata:    &commonv1.Metadata{Name: "liaison-1"},
		GrpcAddress: "localhost:17913",
		Roles:       []databasev1.Role{databasev1.Role_ROLE_LIAISON},
	}

	currentNode2 := &databasev1.Node{
		Metadata:    &commonv1.Metadata{Name: "data-hot-1"},
		GrpcAddress: "localhost:17916",
		Roles:       []databasev1.Role{databasev1.Role_ROLE_DATA},
		Labels:      map[string]string{"tier": "hot"},
	}

	currentNode3 := &databasev1.Node{
		Metadata:    &commonv1.Metadata{Name: "data-warm-1"},
		GrpcAddress: "localhost:17919",
		Roles:       []databasev1.Role{databasev1.Role_ROLE_DATA},
		Labels:      map[string]string{"tier": "warm"},
	}

	// Various registered nodes
	dataHot2 := &databasev1.Node{
		Metadata:    &commonv1.Metadata{Name: "data-hot-2"},
		GrpcAddress: "localhost:17920",
		Roles:       []databasev1.Role{databasev1.Role_ROLE_DATA},
		Labels:      map[string]string{"tier": "hot"},
	}

	dataWarm2 := &databasev1.Node{
		Metadata:    &commonv1.Metadata{Name: "data-warm-2"},
		GrpcAddress: "localhost:17921",
		Roles:       []databasev1.Role{databasev1.Role_ROLE_DATA},
		Labels:      map[string]string{"tier": "warm"},
	}

	liaison2 := &databasev1.Node{
		Metadata:    &commonv1.Metadata{Name: "liaison-2"},
		GrpcAddress: "localhost:17922",
		Roles:       []databasev1.Role{databasev1.Role_ROLE_LIAISON},
	}

	// Different cluster states for each endpoint
	clusterState1 := &databasev1.GetClusterStateResponse{
		RouteTables: map[string]*databasev1.RouteTable{
			"data": {
				Registered: []*databasev1.Node{dataHot2, dataWarm2},
				Active:     []string{"data-hot-2", "data-warm-2"},
				Evictable:  []string{},
			},
		},
	}

	clusterState2 := &databasev1.GetClusterStateResponse{
		RouteTables: map[string]*databasev1.RouteTable{
			"liaison": {
				Registered: []*databasev1.Node{liaison2},
				Active:     []string{"liaison-2"},
				Evictable:  []string{},
			},
		},
	}

	clusterState3 := &databasev1.GetClusterStateResponse{
		RouteTables: map[string]*databasev1.RouteTable{
			"data": {
				Registered: []*databasev1.Node{dataHot2}, // Overlapping node
				Active:     []string{"data-hot-2"},
				Evictable:  []string{},
			},
		},
	}

	nodes := map[string]*databasev1.Node{
		"localhost:17914": currentNode1,
		"localhost:17915": currentNode2,
		"localhost:17916": currentNode3,
	}
	collector.updateCurrentNodes(nodes)

	states := map[string]*databasev1.GetClusterStateResponse{
		"localhost:17914": clusterState1,
		"localhost:17915": clusterState2,
		"localhost:17916": clusterState3,
	}
	collector.updateClusterStates(states)

	topology := collector.GetClusterTopology()

	// Should have 6 unique nodes (3 current + 3 registered)
	assert.Equal(t, 6, len(topology.Nodes))

	// Should have 5 calls:
	// liaison-1 -> data-hot-2, liaison-1 -> data-warm-2
	// data-hot-1 -> liaison-2
	// data-warm-1 -> data-hot-2
	assert.Equal(t, 4, len(topology.Calls))

	// Verify all expected nodes are present
	nodeNames := make(map[string]bool)
	for _, node := range topology.Nodes {
		nodeNames[node.Metadata.Name] = true
	}
	expectedNodes := []string{"liaison-1", "data-hot-1", "data-warm-1", "data-hot-2", "data-warm-2", "liaison-2"}
	for _, expected := range expectedNodes {
		assert.True(t, nodeNames[expected], "Expected node %s to be present", expected)
	}

	// Verify calls are generated correctly
	callMap := make(map[string]*fodcv1.Call)
	for _, call := range topology.Calls {
		callMap[call.Id] = call
	}

	expectedCalls := []string{
		"liaison-1-data-hot-2",
		"liaison-1-data-warm-2",
		"data-hot-1-liaison-2",
		"data-warm-1-data-hot-2",
	}

	for _, expectedCall := range expectedCalls {
		assert.Contains(t, callMap, expectedCall, "Expected call %s to be present", expectedCall)
	}
}
