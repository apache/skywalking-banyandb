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
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
)

// clusterStateManager manages the aggregated RouteTable snapshot from all lifecycle groups.
type clusterStateManager struct {
	lastUpdateTime  time.Time
	aggregatedTable *databasev1.RouteTable
	mu              sync.RWMutex
}

func (m *clusterStateManager) addRouteTable(rt *databasev1.RouteTable) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.aggregatedTable == nil {
		m.aggregatedTable = &databasev1.RouteTable{
			Registered: []*databasev1.Node{},
			Active:     []string{},
			Evictable:  []string{},
		}
	}

	// deduplicate registered nodes using map keyed by node name
	nodeMap := make(map[string]*databasev1.Node)
	for _, node := range m.aggregatedTable.Registered {
		if node != nil && node.Metadata != nil {
			nodeMap[node.Metadata.Name] = node
		}
	}
	for _, node := range rt.Registered {
		if node != nil && node.Metadata != nil {
			nodeMap[node.Metadata.Name] = node
		}
	}

	// deduplicate active node names
	activeSet := make(map[string]bool)
	for _, name := range m.aggregatedTable.Active {
		activeSet[name] = true
	}
	for _, name := range rt.Active {
		activeSet[name] = true
	}

	// Deduplicate evictable node names
	evictableSet := make(map[string]bool)
	for _, name := range m.aggregatedTable.Evictable {
		evictableSet[name] = true
	}
	for _, name := range rt.Evictable {
		evictableSet[name] = true
	}

	// Convert maps back to slices
	m.aggregatedTable.Registered = make([]*databasev1.Node, 0, len(nodeMap))
	for _, node := range nodeMap {
		m.aggregatedTable.Registered = append(m.aggregatedTable.Registered, node)
	}

	m.aggregatedTable.Active = make([]string, 0, len(activeSet))
	for name := range activeSet {
		m.aggregatedTable.Active = append(m.aggregatedTable.Active, name)
	}

	m.aggregatedTable.Evictable = make([]string, 0, len(evictableSet))
	for name := range evictableSet {
		m.aggregatedTable.Evictable = append(m.aggregatedTable.Evictable, name)
	}

	m.lastUpdateTime = time.Now()
}

func (m *clusterStateManager) getSnapshot() *databasev1.RouteTable {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.aggregatedTable == nil {
		return &databasev1.RouteTable{
			Registered: []*databasev1.Node{},
			Active:     []string{},
			Evictable:  []string{},
		}
	}

	// deep copy to avoid concurrent modification
	return proto.Clone(m.aggregatedTable).(*databasev1.RouteTable)
}

// GetClusterState implements the ClusterStateService.GetClusterState RPC.
// It returns the aggregated cluster state under the "lifecycle" key.
func (l *lifecycleService) GetClusterState(_ context.Context, _ *databasev1.GetClusterStateRequest) (*databasev1.GetClusterStateResponse, error) {
	routeTable := l.clusterStateMgr.getSnapshot()

	routeTables := map[string]*databasev1.RouteTable{
		"lifecycle": routeTable,
	}

	return &databasev1.GetClusterStateResponse{
		RouteTables: routeTables,
	}, nil
}
