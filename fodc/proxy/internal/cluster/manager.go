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

// Package cluster provides cluster state management for FODC proxy.
package cluster

import (
	"sync"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/fodc/proxy/internal/registry"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// RequestSender is an interface for sending cluster data requests to agents.
type RequestSender interface {
	RequestClusterData(agentID string) error
}

// Manager manages cluster state from multiple agents.
type Manager struct {
	states      map[string]*agentClusterState
	log         *logger.Logger
	registry    *registry.AgentRegistry
	grpcService RequestSender
	statesMu    sync.RWMutex
}

type agentClusterState struct {
	currentNode  *databasev1.Node
	clusterState *databasev1.GetClusterStateResponse
	agentID      string
}

// NewManager creates a new cluster state manager.
func NewManager(registry *registry.AgentRegistry, grpcService RequestSender, log *logger.Logger) *Manager {
	return &Manager{
		registry:    registry,
		grpcService: grpcService,
		log:         log,
		states:      make(map[string]*agentClusterState),
	}
}

// SetGRPCService sets the gRPC service for sending cluster data requests.
func (m *Manager) SetGRPCService(grpcService RequestSender) {
	m.statesMu.Lock()
	defer m.statesMu.Unlock()
	m.grpcService = grpcService
}

// UpdateClusterState updates cluster state for a specific agent.
func (m *Manager) UpdateClusterState(agentID string, currentNode *databasev1.Node, clusterState *databasev1.GetClusterStateResponse) {
	m.statesMu.Lock()
	defer m.statesMu.Unlock()
	m.states[agentID] = &agentClusterState{
		currentNode:  currentNode,
		clusterState: clusterState,
		agentID:      agentID,
	}
	if m.log != nil {
		nodeName := ""
		if currentNode != nil && currentNode.Metadata != nil {
			nodeName = currentNode.Metadata.Name
		}
		routeTableCount := 0
		if clusterState != nil {
			routeTableCount = len(clusterState.RouteTables)
		}
		m.log.Debug().
			Str("agent_id", agentID).
			Str("node_name", nodeName).
			Int("route_tables_count", routeTableCount).
			Msg("Updated cluster state from agent")
	}
}

// RemoveAgentState removes cluster state for a specific agent.
func (m *Manager) RemoveAgentState(agentID string) {
	m.statesMu.Lock()
	defer m.statesMu.Unlock()

	delete(m.states, agentID)

	if m.log != nil {
		m.log.Debug().
			Str("agent_id", agentID).
			Msg("Removed cluster state for agent")
	}
}

// CollectClusterState requests and collects cluster state from all agents.
func (m *Manager) CollectClusterState() *databasev1.GetClusterStateResponse {
	if m.registry == nil {
		return &databasev1.GetClusterStateResponse{
			RouteTables: make(map[string]*databasev1.RouteTable),
		}
	}
	agents := m.registry.ListAgents()
	if len(agents) == 0 {
		return &databasev1.GetClusterStateResponse{
			RouteTables: make(map[string]*databasev1.RouteTable),
		}
	}
	if m.grpcService != nil {
		for _, agentInfo := range agents {
			if requestErr := m.grpcService.RequestClusterData(agentInfo.AgentID); requestErr != nil {
				if m.log != nil {
					m.log.Warn().Err(requestErr).Str("agent_id", agentInfo.AgentID).Msg("Failed to request cluster data from agent")
				}
			}
		}
	}
	clusterState := m.GetClusterState()
	for _, agentInfo := range agents {
		m.RemoveAgentState(agentInfo.AgentID)
	}
	return clusterState
}

// GetClusterState returns aggregated cluster state from all agents.
func (m *Manager) GetClusterState() *databasev1.GetClusterStateResponse {
	m.statesMu.RLock()
	defer m.statesMu.RUnlock()

	if len(m.states) == 0 {
		return &databasev1.GetClusterStateResponse{
			RouteTables: make(map[string]*databasev1.RouteTable),
		}
	}

	aggregatedRouteTables := make(map[string]*databasev1.RouteTable)
	for agentID, agentState := range m.states {
		if agentState.clusterState == nil {
			continue
		}
		for routeKey, routeTable := range agentState.clusterState.RouteTables {
			aggregatedKey := routeKey
			if len(m.states) > 1 {
				aggregatedKey = agentID + ":" + routeKey
			}
			if existingTable, exists := aggregatedRouteTables[aggregatedKey]; exists {
				aggregatedRouteTables[aggregatedKey] = m.mergeRouteTables(existingTable, routeTable)
			} else {
				aggregatedRouteTables[aggregatedKey] = m.copyRouteTable(routeTable)
			}
		}
	}

	return &databasev1.GetClusterStateResponse{
		RouteTables: aggregatedRouteTables,
	}
}

// mergeRouteTables merges two route tables.
func (m *Manager) mergeRouteTables(table1, table2 *databasev1.RouteTable) *databasev1.RouteTable {
	merged := &databasev1.RouteTable{
		Registered: make([]*databasev1.Node, 0),
		Active:     make([]string, 0),
		Evictable:  make([]string, 0),
	}

	// Merge registered nodes (deduplicate by node name)
	nodeMap := make(map[string]*databasev1.Node)
	for _, node := range table1.Registered {
		if node != nil && node.Metadata != nil {
			nodeMap[node.Metadata.Name] = node
		}
	}
	for _, node := range table2.Registered {
		if node != nil && node.Metadata != nil {
			nodeMap[node.Metadata.Name] = node
		}
	}
	for _, node := range nodeMap {
		merged.Registered = append(merged.Registered, node)
	}

	// Merge active nodes (deduplicate)
	activeSet := make(map[string]bool)
	for _, name := range table1.Active {
		activeSet[name] = true
	}
	for _, name := range table2.Active {
		activeSet[name] = true
	}
	for name := range activeSet {
		merged.Active = append(merged.Active, name)
	}

	// Merge evictable nodes (deduplicate)
	evictableSet := make(map[string]bool)
	for _, name := range table1.Evictable {
		evictableSet[name] = true
	}
	for _, name := range table2.Evictable {
		evictableSet[name] = true
	}
	for name := range evictableSet {
		merged.Evictable = append(merged.Evictable, name)
	}

	return merged
}

// copyRouteTable creates a deep copy of a route table.
func (m *Manager) copyRouteTable(table *databasev1.RouteTable) *databasev1.RouteTable {
	if table == nil {
		return &databasev1.RouteTable{
			Registered: []*databasev1.Node{},
			Active:     []string{},
			Evictable:  []string{},
		}
	}

	copied := &databasev1.RouteTable{
		Registered: make([]*databasev1.Node, len(table.Registered)),
		Active:     make([]string, len(table.Active)),
		Evictable:  make([]string, len(table.Evictable)),
	}

	copy(copied.Registered, table.Registered)
	copy(copied.Active, table.Active)
	copy(copied.Evictable, table.Evictable)

	return copied
}
