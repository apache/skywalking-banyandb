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
	fodcv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/fodc/v1"
	"github.com/apache/skywalking-banyandb/fodc/proxy/internal/registry"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// RequestSender is an interface for sending cluster data requests to agents.
type RequestSender interface {
	RequestClusterData(agentID string) error
}

// Manager manages cluster state from multiple agents.
type Manager struct {
	clusterTopology   map[string]*agentTopology
	log               *logger.Logger
	registry          *registry.AgentRegistry
	grpcService       RequestSender
	clusterTopologyMu sync.RWMutex
}

type agentTopology struct {
	topology *fodcv1.Topology
	agentID  string
}

// NewManager creates a new cluster state manager.
func NewManager(registry *registry.AgentRegistry, grpcService RequestSender, log *logger.Logger) *Manager {
	if log == nil {
		log = logger.GetLogger("cluster-manager")
	}
	return &Manager{
		registry:        registry,
		grpcService:     grpcService,
		log:             log,
		clusterTopology: make(map[string]*agentTopology),
	}
}

// SetGRPCService sets the gRPC service for sending cluster data requests.
func (m *Manager) SetGRPCService(grpcService RequestSender) {
	m.clusterTopologyMu.Lock()
	defer m.clusterTopologyMu.Unlock()
	m.grpcService = grpcService
}

// UpdateClusterTopology updates cluster topology for a specific agent.
func (m *Manager) UpdateClusterTopology(agentID string, topology *fodcv1.Topology) {
	m.clusterTopologyMu.Lock()
	defer m.clusterTopologyMu.Unlock()
	m.clusterTopology[agentID] = &agentTopology{
		topology: topology,
		agentID:  agentID,
	}
}

// RemoveTopology removes cluster topology for a specific agent.
func (m *Manager) RemoveTopology(agentID string) {
	m.clusterTopologyMu.Lock()
	defer m.clusterTopologyMu.Unlock()

	delete(m.clusterTopology, agentID)
}

// CollectClusterTopology requests and collects cluster topology from all agents.
func (m *Manager) CollectClusterTopology() *fodcv1.Topology {
	if m.registry == nil {
		return &fodcv1.Topology{
			Nodes: make([]*databasev1.Node, 0),
			Calls: make([]*fodcv1.Call, 0),
		}
	}
	agents := m.registry.ListAgents()
	if len(agents) == 0 {
		return &fodcv1.Topology{
			Nodes: make([]*databasev1.Node, 0),
			Calls: make([]*fodcv1.Call, 0),
		}
	}
	if m.grpcService != nil {
		for _, agentInfo := range agents {
			if requestErr := m.grpcService.RequestClusterData(agentInfo.AgentID); requestErr != nil {
				m.log.Warn().Err(requestErr).Str("agent_id", agentInfo.AgentID).Msg("Failed to request cluster data from agent")
			}
		}
	}
	clusterTopology := m.GetClusterTopology()
	for _, agentInfo := range agents {
		m.RemoveTopology(agentInfo.AgentID)
	}
	return clusterTopology
}

// GetClusterTopology returns aggregated cluster topology from all agents.
func (m *Manager) GetClusterTopology() *fodcv1.Topology {
	m.clusterTopologyMu.RLock()
	defer m.clusterTopologyMu.RUnlock()

	if len(m.clusterTopology) == 0 {
		return &fodcv1.Topology{
			Nodes: make([]*databasev1.Node, 0),
			Calls: make([]*fodcv1.Call, 0),
		}
	}

	nodeMap := make(map[string]*databasev1.Node)
	callMap := make(map[string]*fodcv1.Call)

	for _, topology := range m.clusterTopology {
		if topology.topology == nil {
			continue
		}
		for _, node := range topology.topology.Nodes {
			if node != nil && node.Metadata != nil && node.Metadata.Name != "" {
				nodeMap[node.Metadata.Name] = node
			}
		}
		for _, call := range topology.topology.Calls {
			if call != nil && call.Id != "" {
				callMap[call.Id] = call
			}
		}
	}

	aggregatedNodes := make([]*databasev1.Node, 0, len(nodeMap))
	for _, node := range nodeMap {
		aggregatedNodes = append(aggregatedNodes, node)
	}

	aggregatedCalls := make([]*fodcv1.Call, 0, len(callMap))
	for _, call := range callMap {
		aggregatedCalls = append(aggregatedCalls, call)
	}

	return &fodcv1.Topology{
		Nodes: aggregatedNodes,
		Calls: aggregatedCalls,
	}
}
