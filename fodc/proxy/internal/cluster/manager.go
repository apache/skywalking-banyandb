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
	"context"
	"fmt"
	"sync"
	"time"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	fodcv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/fodc/v1"
	"github.com/apache/skywalking-banyandb/fodc/proxy/internal/registry"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

const (
	defaultCollectionTimeout = 10 * time.Second
)

// NodeWithStringRoles represents a node with roles as strings instead of numeric enum values.
type NodeWithStringRoles struct {
	*databasev1.Node

	Status        *registry.AgentStatus `json:"status,omitempty"`
	LastHeartbeat *time.Time            `json:"last_heartbeat,omitempty"`
	Roles         []string              `json:"roles"`
}

// TopologyMap represents processed cluster data with string roles.
type TopologyMap struct {
	Nodes []*NodeWithStringRoles `json:"nodes"`
	Calls []*fodcv1.Call         `json:"calls"`
}

// RequestSender is an interface for sending cluster data requests to agents.
type RequestSender interface {
	RequestClusterData(agentID string) error
}

// Manager manages cluster state from multiple agents.
type Manager struct {
	log          *logger.Logger
	registry     *registry.AgentRegistry
	grpcService  RequestSender
	collecting   map[string]chan *TopologyMap
	mu           sync.RWMutex
	collectingMu sync.RWMutex
	collectingOp sync.Mutex
}

// NewManager creates a new cluster state manager.
func NewManager(registry *registry.AgentRegistry, grpcService RequestSender, log *logger.Logger) *Manager {
	return &Manager{
		registry:    registry,
		grpcService: grpcService,
		log:         log,
		collecting:  make(map[string]chan *TopologyMap),
	}
}

// SetGRPCService sets the gRPC service for sending cluster data requests.
func (m *Manager) SetGRPCService(grpcService RequestSender) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.grpcService = grpcService
}

// UpdateClusterTopology updates cluster topology for a specific agent.
func (m *Manager) UpdateClusterTopology(agentID string, topology *fodcv1.Topology) {
	topologyMap := convertTopologyToMap(topology)
	m.collectingMu.RLock()
	collectCh, exists := m.collecting[agentID]
	m.collectingMu.RUnlock()
	if exists {
		select {
		case collectCh <- topologyMap:
		default:
			m.log.Warn().Str("agent_id", agentID).Msg("Topology collection channel full, dropping topology")
		}
	}
}

// RemoveTopology removes cluster topology for a specific agent.
func (m *Manager) RemoveTopology(agentID string) {
	m.collectingMu.Lock()
	defer m.collectingMu.Unlock()
	if collectCh, exists := m.collecting[agentID]; exists {
		close(collectCh)
		delete(m.collecting, agentID)
	}
}

// CollectClusterTopology requests and collects cluster topology from all agents with context.
func (m *Manager) CollectClusterTopology(ctx context.Context) *TopologyMap {
	m.collectingOp.Lock()
	defer m.collectingOp.Unlock()
	if m.registry == nil {
		return &TopologyMap{
			Nodes: make([]*NodeWithStringRoles, 0),
			Calls: make([]*fodcv1.Call, 0),
		}
	}
	agents := m.registry.ListAgents()
	if len(agents) == 0 {
		return &TopologyMap{
			Nodes: make([]*NodeWithStringRoles, 0),
			Calls: make([]*fodcv1.Call, 0),
		}
	}
	collectChs := make(map[string]chan *TopologyMap)
	agentIDs := make([]string, 0, len(agents))
	m.collectingMu.Lock()
	for _, agentInfo := range agents {
		collectCh := make(chan *TopologyMap, 1)
		collectChs[agentInfo.AgentID] = collectCh
		m.collecting[agentInfo.AgentID] = collectCh
		agentIDs = append(agentIDs, agentInfo.AgentID)
	}
	m.collectingMu.Unlock()
	defer func() {
		m.collectingMu.Lock()
		for _, agentID := range agentIDs {
			if collectCh, exists := m.collecting[agentID]; exists {
				close(collectCh)
				delete(m.collecting, agentID)
			}
		}
		m.collectingMu.Unlock()
	}()
	for _, agentInfo := range agents {
		select {
		case <-ctx.Done():
			return &TopologyMap{
				Nodes: make([]*NodeWithStringRoles, 0),
				Calls: make([]*fodcv1.Call, 0),
			}
		default:
		}
		if m.grpcService != nil {
			if requestErr := m.grpcService.RequestClusterData(agentInfo.AgentID); requestErr != nil {
				m.log.Warn().
					Err(requestErr).
					Str("agent_id", agentInfo.AgentID).
					Msg("Failed to request cluster data from agent")
				m.collectingMu.Lock()
				if collectCh, exists := m.collecting[agentInfo.AgentID]; exists {
					close(collectCh)
					delete(m.collecting, agentInfo.AgentID)
				}
				m.collectingMu.Unlock()
				delete(collectChs, agentInfo.AgentID)
			}
		}
	}
	timeout := defaultCollectionTimeout
	allTopologies := make([]*TopologyMap, 0)
	var topologiesMu sync.Mutex
	var wg sync.WaitGroup
	for agentID, collectCh := range collectChs {
		wg.Add(1)
		go func(id string, ch chan *TopologyMap) {
			defer wg.Done()
			agentCtx, agentCancel := context.WithTimeout(ctx, timeout)
			defer agentCancel()
			select {
			case <-agentCtx.Done():
				m.log.Warn().
					Str("agent_id", id).
					Msg("Timeout waiting for topology from agent")
			case topologyMap := <-ch:
				if topologyMap != nil {
					topologiesMu.Lock()
					allTopologies = append(allTopologies, topologyMap)
					topologiesMu.Unlock()
				}
			}
		}(agentID, collectCh)
	}
	wg.Wait()
	return m.aggregateTopologies(allTopologies)
}

// aggregateTopologies aggregates topology from multiple agents.
func (m *Manager) aggregateTopologies(topologies []*TopologyMap) *TopologyMap {
	if len(topologies) == 0 {
		return &TopologyMap{
			Nodes: make([]*NodeWithStringRoles, 0),
			Calls: make([]*fodcv1.Call, 0),
		}
	}
	nodeMap := make(map[string]*NodeWithStringRoles)
	callMap := make(map[string]*fodcv1.Call)
	for _, topologyMap := range topologies {
		if topologyMap == nil {
			continue
		}
		for _, node := range topologyMap.Nodes {
			if node != nil && node.Metadata != nil && node.Metadata.Name != "" {
				nodeMap[node.Metadata.Name] = node
			}
		}
		for _, call := range topologyMap.Calls {
			if call != nil && call.Id != "" {
				callMap[call.Id] = call
			}
		}
	}
	if m.registry != nil {
		allAgents := m.registry.ListAgents()
		for _, node := range nodeMap {
			if node.Labels != nil {
				if podName, hasPodName := node.Labels["pod_name"]; hasPodName && podName != "" {
					for _, agentInfo := range allAgents {
						if agentInfo.AgentIdentity.PodName == podName {
							status := agentInfo.Status
							lastHeartbeat := agentInfo.LastHeartbeat
							node.Status = &status
							node.LastHeartbeat = &lastHeartbeat
							break
						}
					}
				}
			}
		}
	}
	aggregatedNodes := make([]*NodeWithStringRoles, 0, len(nodeMap))
	for _, node := range nodeMap {
		aggregatedNodes = append(aggregatedNodes, node)
	}
	aggregatedCalls := make([]*fodcv1.Call, 0, len(callMap))
	for _, call := range callMap {
		aggregatedCalls = append(aggregatedCalls, call)
	}
	return &TopologyMap{
		Nodes: aggregatedNodes,
		Calls: aggregatedCalls,
	}
}

// convertRolesToStrings converts numeric role values to their string names using Role_name map.
func convertRolesToStrings(roles []databasev1.Role) []string {
	if len(roles) == 0 {
		return []string{}
	}
	roleStrings := make([]string, 0, len(roles))
	for _, role := range roles {
		roleName, exists := databasev1.Role_name[int32(role)]
		if exists {
			roleStrings = append(roleStrings, roleName)
		} else {
			roleStrings = append(roleStrings, fmt.Sprintf("UNKNOWN_ROLE_%d", role))
		}
	}
	return roleStrings
}

// convertNodeToStringRoles converts a databasev1.Node to NodeWithStringRoles.
func convertNodeToStringRoles(node *databasev1.Node) *NodeWithStringRoles {
	if node == nil {
		return nil
	}
	return &NodeWithStringRoles{
		Node:  node,
		Roles: convertRolesToStrings(node.Roles),
	}
}

// convertTopologyToMap converts a fodcv1.Topology to TopologyMap with string roles.
func convertTopologyToMap(topology *fodcv1.Topology) *TopologyMap {
	if topology == nil {
		return &TopologyMap{
			Nodes: make([]*NodeWithStringRoles, 0),
			Calls: make([]*fodcv1.Call, 0),
		}
	}
	nodes := make([]*NodeWithStringRoles, 0, len(topology.Nodes))
	for _, node := range topology.Nodes {
		nodeWithStringRoles := convertNodeToStringRoles(node)
		if nodeWithStringRoles != nil {
			nodes = append(nodes, nodeWithStringRoles)
		}
	}
	return &TopologyMap{
		Nodes: nodes,
		Calls: topology.Calls,
	}
}
