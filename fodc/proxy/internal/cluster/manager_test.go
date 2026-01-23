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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/fodc/proxy/internal/registry"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

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
	assert.NotNil(t, mgr.states)
	assert.Empty(t, mgr.states)
}

func TestManager_UpdateClusterState(t *testing.T) {
	log := initTestLogger(t)
	testRegistry := registry.NewAgentRegistry(log, 5*time.Second, 10*time.Second, 100)
	mgr := NewManager(testRegistry, nil, log)

	clusterState := &databasev1.GetClusterStateResponse{
		RouteTables: map[string]*databasev1.RouteTable{
			"test": {
				Registered: []*databasev1.Node{},
				Active:     []string{"node1"},
				Evictable:  []string{},
			},
		},
	}

	mgr.UpdateClusterState("agent1", nil, clusterState)

	// Verify state was stored
	aggregatedState := mgr.GetClusterState()
	require.NotNil(t, aggregatedState)
	assert.Equal(t, 1, len(aggregatedState.RouteTables))
	assert.Equal(t, 1, len(aggregatedState.RouteTables["test"].Active))
}

func TestManager_RemoveAgentState(t *testing.T) {
	log := initTestLogger(t)
	testRegistry := registry.NewAgentRegistry(log, 5*time.Second, 10*time.Second, 100)
	mgr := NewManager(testRegistry, nil, log)

	clusterState := &databasev1.GetClusterStateResponse{
		RouteTables: map[string]*databasev1.RouteTable{
			"test": {
				Registered: []*databasev1.Node{},
				Active:     []string{"node1"},
				Evictable:  []string{},
			},
		},
	}

	mgr.UpdateClusterState("agent1", nil, clusterState)
	mgr.RemoveAgentState("agent1")

	// Verify state was removed
	aggregatedState := mgr.GetClusterState()
	require.NotNil(t, aggregatedState)
	assert.Empty(t, aggregatedState.RouteTables)
}

func TestManager_GetClusterState_Empty(t *testing.T) {
	log := initTestLogger(t)
	testRegistry := registry.NewAgentRegistry(log, 5*time.Second, 10*time.Second, 100)
	mgr := NewManager(testRegistry, nil, log)

	clusterState := mgr.GetClusterState()

	require.NotNil(t, clusterState)
	assert.Empty(t, clusterState.RouteTables)
}

func TestManager_GetClusterState_MultipleAgents(t *testing.T) {
	log := initTestLogger(t)
	testRegistry := registry.NewAgentRegistry(log, 5*time.Second, 10*time.Second, 100)
	mgr := NewManager(testRegistry, nil, log)

	clusterState1 := &databasev1.GetClusterStateResponse{
		RouteTables: map[string]*databasev1.RouteTable{
			"route1": {
				Registered: []*databasev1.Node{},
				Active:     []string{"node1"},
				Evictable:  []string{},
			},
		},
	}

	clusterState2 := &databasev1.GetClusterStateResponse{
		RouteTables: map[string]*databasev1.RouteTable{
			"route2": {
				Registered: []*databasev1.Node{},
				Active:     []string{"node2"},
				Evictable:  []string{},
			},
		},
	}

	mgr.UpdateClusterState("agent1", nil, clusterState1)
	mgr.UpdateClusterState("agent2", nil, clusterState2)

	aggregatedState := mgr.GetClusterState()

	require.NotNil(t, aggregatedState)
	// Should have 2 route tables with agent prefixes
	assert.Equal(t, 2, len(aggregatedState.RouteTables))
}
