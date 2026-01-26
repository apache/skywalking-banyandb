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
	collector := NewCollector(log, "localhost:17914", 10*time.Second)
	require.NotNil(t, collector)
	assert.Equal(t, "localhost:17914", collector.lifecycleAddr)
	assert.Equal(t, 10*time.Second, collector.interval)
	assert.NotNil(t, collector.closer)
	assert.False(t, collector.closer.Closed())
}

func TestCollector_Stop_NotStarted(t *testing.T) {
	log := initTestLogger(t)
	collector := NewCollector(log, "localhost:17914", 10*time.Second)
	collector.Stop()
	assert.True(t, collector.closer.Closed())
}

func TestCollector_Stop_MultipleCalls(t *testing.T) {
	log := initTestLogger(t)
	collector := NewCollector(log, "localhost:17914", 10*time.Second)
	collector.Stop()
	collector.Stop()
	collector.Stop()
	assert.True(t, collector.closer.Closed())
}

func TestCollector_Start_AfterStop(t *testing.T) {
	log := initTestLogger(t)
	collector := NewCollector(log, "localhost:17914", 10*time.Second)
	collector.Stop()
	ctx := context.Background()
	err := collector.Start(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "stopped and cannot be restarted")
	assert.True(t, collector.closer.Closed())
}

func TestCollector_FetchClusterState_NoClient(t *testing.T) {
	log := initTestLogger(t)
	collector := NewCollector(log, "localhost:17914", 10*time.Second)
	ctx := context.Background()
	state, err := collector.fetchClusterState(ctx)
	assert.Error(t, err)
	assert.Nil(t, state)
	assert.Contains(t, err.Error(), "gRPC client not initialized")
}

func TestCollector_FetchCurrentNode_NoClient(t *testing.T) {
	log := initTestLogger(t)
	collector := NewCollector(log, "localhost:17914", 10*time.Second)
	ctx := context.Background()
	node, err := collector.fetchCurrentNode(ctx)
	assert.Error(t, err)
	assert.Nil(t, node)
	assert.Contains(t, err.Error(), "node query client not initialized")
}

func TestCollector_GetCurrentNode_InitialNil(t *testing.T) {
	log := initTestLogger(t)
	collector := NewCollector(log, "localhost:17914", 10*time.Second)
	assert.Nil(t, collector.GetCurrentNode())
}

func TestCollector_GetClusterState_InitialNil(t *testing.T) {
	log := initTestLogger(t)
	collector := NewCollector(log, "localhost:17914", 10*time.Second)
	assert.Nil(t, collector.GetClusterState())
}

func TestCollector_UpdateCurrentNode(t *testing.T) {
	log := initTestLogger(t)
	collector := NewCollector(log, "localhost:17914", 10*time.Second)
	node := &databasev1.Node{
		Metadata:    &commonv1.Metadata{Name: "test-node"},
		GrpcAddress: "localhost:17913",
		Roles:       []databasev1.Role{databasev1.Role_ROLE_DATA},
		Labels:      map[string]string{"tier": "hot"},
	}
	collector.updateCurrentNode(node)
	assert.Equal(t, node, collector.GetCurrentNode())
}

func TestCollector_UpdateClusterState(t *testing.T) {
	log := initTestLogger(t)
	collector := NewCollector(log, "localhost:17914", 10*time.Second)
	state := &databasev1.GetClusterStateResponse{
		RouteTables: map[string]*databasev1.RouteTable{
			"test": {
				Registered: []*databasev1.Node{},
				Active:     []string{"node1"},
				Evictable:  []string{},
			},
		},
	}
	collector.updateClusterState(state)
	assert.Equal(t, state, collector.GetClusterState())
}

func TestCollector_GetNodeInfo(t *testing.T) {
	log := initTestLogger(t)
	collector := NewCollector(log, "localhost:17914", 10*time.Second)
	node := &databasev1.Node{
		Metadata: &commonv1.Metadata{Name: "test-node"},
		Roles:    []databasev1.Role{databasev1.Role_ROLE_DATA},
		Labels:   map[string]string{"tier": "hot", "zone": "us-west"},
	}
	collector.updateCurrentNode(node)
	nodeRole, nodeLabels := collector.GetNodeInfo()
	assert.Equal(t, "hot", nodeRole)
	assert.Equal(t, map[string]string{"tier": "hot", "zone": "us-west"}, nodeLabels)
}

func TestCollector_GetNodeInfo_NoNode(t *testing.T) {
	log := initTestLogger(t)
	collector := NewCollector(log, "localhost:17914", 10*time.Second)
	nodeRole, nodeLabels := collector.GetNodeInfo()
	assert.Equal(t, "", nodeRole)
	assert.Nil(t, nodeLabels)
}

func TestNodeRoleFromNode(t *testing.T) {
	tests := []struct {
		name     string
		node     *databasev1.Node
		expected string
	}{
		{"nil node", nil, "UNKNOWN"},
		{"empty roles", &databasev1.Node{}, "UNKNOWN"},
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
