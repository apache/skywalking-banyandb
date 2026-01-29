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
	"fmt"
	"sync"

	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	fodcv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/fodc/v1"
	"github.com/apache/skywalking-banyandb/fodc/proxy/internal/registry"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// NodeWithStringRoles represents a node with roles as strings instead of numeric enum values.
type NodeWithStringRoles struct {
	Metadata  *commonv1.Metadata     `json:"metadata"`
	CreatedAt *timestamppb.Timestamp `json:"created_at"`
	Roles     []string               `json:"roles"`
	Labels    map[string]string      `json:"labels"`

	GrpcAddress                     string `json:"grpc_address"`
	HttpAddress                     string `json:"http_address"`
	PropertyRepairGossipGrpcAddress string `json:"property_repair_gossip_grpc_address"`
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
	clusterTopology   map[string]*TopologyMap
	log               *logger.Logger
	registry          *registry.AgentRegistry
	grpcService       RequestSender
	clusterTopologyMu sync.RWMutex
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
		clusterTopology: make(map[string]*TopologyMap),
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
	topologyMap := convertTopologyToMap(topology)
	m.clusterTopology[agentID] = topologyMap
}

// RemoveTopology removes cluster topology for a specific agent.
func (m *Manager) RemoveTopology(agentID string) {
	m.clusterTopologyMu.Lock()
	defer m.clusterTopologyMu.Unlock()

	delete(m.clusterTopology, agentID)
}

// CollectClusterTopology requests and collects cluster topology from all agents.
func (m *Manager) CollectClusterTopology() *TopologyMap {
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
	if m.grpcService != nil {
		for _, agentInfo := range agents {
			if requestErr := m.grpcService.RequestClusterData(agentInfo.AgentID); requestErr != nil {
				m.log.Warn().Err(requestErr).Str("agent_id", agentInfo.AgentID).Msg("Failed to request cluster data from agent")
			}
		}
	}
	m.clusterTopologyMu.RLock()
	if len(m.clusterTopology) == 0 {
		m.clusterTopologyMu.RUnlock()
		return &TopologyMap{
			Nodes: make([]*NodeWithStringRoles, 0),
			Calls: make([]*fodcv1.Call, 0),
		}
	}
	nodeMap := make(map[string]*NodeWithStringRoles)
	callMap := make(map[string]*fodcv1.Call)
	for _, topologyMap := range m.clusterTopology {
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
	m.clusterTopologyMu.RUnlock()
	aggregatedNodes := make([]*NodeWithStringRoles, 0, len(nodeMap))
	for _, node := range nodeMap {
		aggregatedNodes = append(aggregatedNodes, node)
	}
	aggregatedCalls := make([]*fodcv1.Call, 0, len(callMap))
	for _, call := range callMap {
		aggregatedCalls = append(aggregatedCalls, call)
	}
	result := &TopologyMap{
		Nodes: aggregatedNodes,
		Calls: aggregatedCalls,
	}
	for _, agentInfo := range agents {
		m.RemoveTopology(agentInfo.AgentID)
	}
	return result
}

// GetClusterTopology returns aggregated cluster topology from all agents.
func (m *Manager) GetClusterTopology() *TopologyMap {
	m.clusterTopologyMu.RLock()
	defer m.clusterTopologyMu.RUnlock()

	if len(m.clusterTopology) == 0 {
		return &TopologyMap{
			Nodes: make([]*NodeWithStringRoles, 0),
			Calls: make([]*fodcv1.Call, 0),
		}
	}

	nodeMap := make(map[string]*NodeWithStringRoles)
	callMap := make(map[string]*fodcv1.Call)

	for _, topologyMap := range m.clusterTopology {
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
		Metadata:                        node.Metadata,
		Roles:                           convertRolesToStrings(node.Roles),
		GrpcAddress:                     node.GrpcAddress,
		HttpAddress:                     node.HttpAddress,
		CreatedAt:                       node.CreatedAt,
		Labels:                          node.Labels,
		PropertyRepairGossipGrpcAddress: node.PropertyRepairGossipGrpcAddress,
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
