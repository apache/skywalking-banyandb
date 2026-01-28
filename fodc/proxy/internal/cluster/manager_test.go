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

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	fodcv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/fodc/v1"
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
	assert.NotNil(t, mgr.clusterTopology)
	assert.Empty(t, mgr.clusterTopology)
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

	mgr.UpdateClusterTopology("agent1", topology)

	// Verify topology was stored
	aggregatedTopology := mgr.GetClusterTopology()
	require.NotNil(t, aggregatedTopology)
	assert.Equal(t, 1, len(aggregatedTopology.Nodes))
	assert.Equal(t, 1, len(aggregatedTopology.Calls))
	assert.Equal(t, "test-node", aggregatedTopology.Nodes[0].Metadata.Name)
	assert.Equal(t, "call-1", aggregatedTopology.Calls[0].Id)
}

func TestManager_RemoveAgentState(t *testing.T) {
	log := initTestLogger(t)
	testRegistry := registry.NewAgentRegistry(log, 5*time.Second, 10*time.Second, 100)
	mgr := NewManager(testRegistry, nil, log)

	topology := &fodcv1.Topology{
		Nodes: []*databasev1.Node{
			{
				Metadata: &commonv1.Metadata{Name: "test-node"},
			},
		},
		Calls: []*fodcv1.Call{},
	}

	mgr.UpdateClusterTopology("agent1", topology)
	mgr.RemoveTopology("agent1")

	// Verify topology was removed
	aggregatedTopology := mgr.GetClusterTopology()
	require.NotNil(t, aggregatedTopology)
	assert.Empty(t, aggregatedTopology.Nodes)
	assert.Empty(t, aggregatedTopology.Calls)
}

func TestManager_GetClusterTopology_MultipleAgents(t *testing.T) {
	log := initTestLogger(t)
	testRegistry := registry.NewAgentRegistry(log, 5*time.Second, 10*time.Second, 100)
	mgr := NewManager(testRegistry, nil, log)

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

	mgr.UpdateClusterTopology("agent1", topology1)
	mgr.UpdateClusterTopology("agent2", topology2)

	aggregatedTopology := mgr.GetClusterTopology()

	require.NotNil(t, aggregatedTopology)
	// Should have 2 unique nodes (node1 and node2)
	assert.Equal(t, 2, len(aggregatedTopology.Nodes))
	// Should have 2 unique calls
	assert.Equal(t, 2, len(aggregatedTopology.Calls))
}
