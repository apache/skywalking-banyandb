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

package schemaserver

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/property/db"
	"github.com/apache/skywalking-banyandb/banyand/property/gossip"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

// mockPropagationMessenger records Propagation calls for unit testing.
type mockPropagationMessenger struct {
	gossip.Messenger
	propagationCall *propagationCall
	mu              sync.Mutex
}

type propagationCall struct {
	group   string
	nodes   []string
	shardID uint32
}

func (m *mockPropagationMessenger) Propagation(nodes []string, group string, shardID uint32) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.propagationCall = &propagationCall{nodes: nodes, group: group, shardID: shardID}
	return nil
}

func (m *mockPropagationMessenger) getPropagationCall() *propagationCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.propagationCall
}

func TestRepairServiceNodeTracking(t *testing.T) {
	t.Run("add node with address", func(t *testing.T) {
		rs := &repairService{metaNodes: make(map[string]struct{}), l: logger.GetLogger("test-repair")}
		rs.OnAddOrUpdate(schema.Metadata{
			TypeMeta: schema.TypeMeta{Name: "node-1", Kind: schema.KindNode},
			Spec: &databasev1.Node{
				Metadata:                        &commonv1.Metadata{Name: "node-1"},
				Roles:                           []databasev1.Role{databasev1.Role_ROLE_META},
				PropertySchemaGossipGrpcAddress: "127.0.0.1:9999",
			},
		})
		rs.mu.RLock()
		defer rs.mu.RUnlock()
		_, exists := rs.metaNodes["node-1"]
		assert.True(t, exists, "node should be tracked")
	})

	t.Run("add node without address", func(t *testing.T) {
		rs := &repairService{metaNodes: make(map[string]struct{}), l: logger.GetLogger("test-repair")}
		rs.OnAddOrUpdate(schema.Metadata{
			TypeMeta: schema.TypeMeta{Name: "node-1", Kind: schema.KindNode},
			Spec: &databasev1.Node{
				Metadata:                        &commonv1.Metadata{Name: "node-1"},
				PropertySchemaGossipGrpcAddress: "",
			},
		})
		rs.mu.RLock()
		defer rs.mu.RUnlock()
		assert.Empty(t, rs.metaNodes, "node without address should not be tracked")
	})

	t.Run("add non-node kind", func(t *testing.T) {
		rs := &repairService{metaNodes: make(map[string]struct{}), l: logger.GetLogger("test-repair")}
		rs.OnAddOrUpdate(schema.Metadata{
			TypeMeta: schema.TypeMeta{Name: "group-1", Kind: schema.KindGroup},
			Spec:     &commonv1.Group{Metadata: &commonv1.Metadata{Name: "group-1"}},
		})
		rs.mu.RLock()
		defer rs.mu.RUnlock()
		assert.Empty(t, rs.metaNodes, "non-node kind should not be tracked")
	})

	t.Run("delete node", func(t *testing.T) {
		rs := &repairService{metaNodes: make(map[string]struct{}), l: logger.GetLogger("test-repair")}
		rs.metaNodes["node-1"] = struct{}{}
		rs.OnDelete(schema.Metadata{
			TypeMeta: schema.TypeMeta{Name: "node-1", Kind: schema.KindNode},
			Spec: &databasev1.Node{
				Metadata: &commonv1.Metadata{Name: "node-1"},
			},
		})
		rs.mu.RLock()
		defer rs.mu.RUnlock()
		_, exists := rs.metaNodes["node-1"]
		assert.False(t, exists, "deleted node should be removed")
	})

	t.Run("delete non-existing node", func(t *testing.T) {
		rs := &repairService{metaNodes: make(map[string]struct{}), l: logger.GetLogger("test-repair")}
		rs.OnDelete(schema.Metadata{
			TypeMeta: schema.TypeMeta{Name: "unknown", Kind: schema.KindNode},
			Spec: &databasev1.Node{
				Metadata: &commonv1.Metadata{Name: "unknown"},
			},
		})
		rs.mu.RLock()
		defer rs.mu.RUnlock()
		assert.Empty(t, rs.metaNodes, "deleting non-existing node should not panic")
	})
}

func TestRepairServiceDoRepairGossip(t *testing.T) {
	t.Run("zero nodes", func(t *testing.T) {
		mock := &mockPropagationMessenger{}
		rs := &repairService{metaNodes: make(map[string]struct{}), messenger: mock, l: logger.GetLogger("test-repair")}
		repairErr := rs.doRepairGossip()
		require.NoError(t, repairErr)
		assert.Nil(t, mock.getPropagationCall(), "should not propagate with zero nodes")
	})

	t.Run("one node", func(t *testing.T) {
		mock := &mockPropagationMessenger{}
		rs := &repairService{metaNodes: map[string]struct{}{"node-1": {}}, messenger: mock, l: logger.GetLogger("test-repair")}
		repairErr := rs.doRepairGossip()
		require.NoError(t, repairErr)
		assert.Nil(t, mock.getPropagationCall(), "should not propagate with one node")
	})

	t.Run("two nodes", func(t *testing.T) {
		mock := &mockPropagationMessenger{}
		rs := &repairService{
			metaNodes: map[string]struct{}{"node-1": {}, "node-2": {}},
			messenger: mock,
			l:         logger.GetLogger("test-repair"),
		}
		repairErr := rs.doRepairGossip()
		require.NoError(t, repairErr)
		call := mock.getPropagationCall()
		require.NotNil(t, call, "should propagate with two nodes")
		assert.Len(t, call.nodes, 2)
		assert.Equal(t, schemaGroup, call.group)
		assert.Equal(t, uint32(0), call.shardID)
	})

	t.Run("three nodes", func(t *testing.T) {
		mock := &mockPropagationMessenger{}
		rs := &repairService{
			metaNodes: map[string]struct{}{"node-1": {}, "node-2": {}, "node-3": {}},
			messenger: mock,
			l:         logger.GetLogger("test-repair"),
		}
		repairErr := rs.doRepairGossip()
		require.NoError(t, repairErr)
		call := mock.getPropagationCall()
		require.NotNil(t, call, "should propagate with three nodes")
		assert.Len(t, call.nodes, 3)
		assert.Equal(t, schemaGroup, call.group)
		assert.Equal(t, uint32(0), call.shardID)
	})
}

// testNode holds resources for one gossip-enabled node in integration tests.
type testNode struct {
	srv       *server
	messenger gossip.Messenger
	nodeID    string
}

// setupTestNode creates a schema server (which owns the DB) and a gossip messenger, wired together.
func setupTestNode(t *testing.T) *testNode {
	t.Helper()

	// Use NewServer from service.go — it handles DB creation, snapshot config, repair config, etc.
	srv := NewServer(observability.BypassRegistry).(*server)
	flagSet := srv.FlagSet()
	require.NoError(t, flagSet.Parse([]string{
		"--schema-server-root-path", t.TempDir(),
		"--schema-server-grpc-host", "127.0.0.1",
		"--schema-server-grpc-port", fmt.Sprintf("%d", getFreePort(t)),
		"--schema-server-repair-build-tree-cron", "@every 10m",
		"--schema-server-repair-quick-build-tree-time", "6s",
	}))
	require.NoError(t, srv.Validate())
	require.NoError(t, srv.PreRun(context.Background()))
	srv.Serve()
	t.Cleanup(func() { srv.GracefulStop() })

	// Create gossip messenger for the repair protocol.
	gossipPort := getFreePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", gossipPort)
	messenger := gossip.NewMessengerWithoutMetadata("schema-repair",
		func(n *databasev1.Node) string { return n.PropertySchemaGossipGrpcAddress },
		observability.NewBypassRegistry(), int(gossipPort))
	require.NoError(t, messenger.Validate())
	ctx := context.WithValue(context.Background(), common.ContextNodeKey, common.Node{
		NodeID:                          addr,
		PropertySchemaGossipGrpcAddress: addr,
	})
	require.NoError(t, messenger.PreRun(ctx))
	// RegisterGossip must happen after PreRun because PreRun resets listeners.
	srv.RegisterGossip(messenger)
	messenger.Serve(run.NewCloser(0))
	t.Cleanup(messenger.GracefulStop)

	require.Eventually(t, func() bool {
		conn, dialErr := net.DialTimeout("tcp", addr, time.Second)
		if dialErr != nil {
			return false
		}
		_ = conn.Close()
		return true
	}, 10*time.Second, 100*time.Millisecond, "gossip server did not start")

	return &testNode{srv: srv, messenger: messenger, nodeID: addr}
}

func registerNodes(nodes []*testNode) {
	for _, m := range nodes {
		for _, n := range nodes {
			m.messenger.(schema.EventHandler).OnAddOrUpdate(schema.Metadata{
				TypeMeta: schema.TypeMeta{
					Name: n.nodeID,
					Kind: schema.KindNode,
				},
				Spec: &databasev1.Node{
					Metadata:                        &commonv1.Metadata{Name: n.nodeID},
					Roles:                           []databasev1.Role{databasev1.Role_ROLE_DATA},
					PropertySchemaGossipGrpcAddress: n.nodeID,
				},
			})
		}
	}
}

func writeProperty(t *testing.T, d db.Database, name, id string, version int64) {
	t.Helper()
	prop := &propertyv1.Property{
		Metadata: &commonv1.Metadata{
			Group:       schemaGroup,
			Name:        name,
			ModRevision: version,
		},
		Id: id,
		Tags: []*modelv1.Tag{
			{Key: "version", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{
				Str: &modelv1.Str{Value: fmt.Sprintf("%d", version)},
			}}},
		},
	}
	require.NoError(t, d.Update(context.Background(), common.ShardID(0), db.GetPropertyID(prop), prop))
}

func ensureShard(t *testing.T, nodes []*testNode) {
	t.Helper()
	for _, n := range nodes {
		writeProperty(t, n.srv.db, "seed", "seed-init", 0)
	}
}

func propagateAndVerify(t *testing.T, propagator gossip.Messenger, nodeIDs []string, verifyFn func() bool) {
	t.Helper()
	require.Eventually(t, func() bool {
		_ = propagator.Propagation(nodeIDs, schemaGroup, 0)
		return verifyFn()
	}, 30*time.Second, time.Second)
}

func queryHasAllIDs(d db.Database, expectedIDs []string) bool {
	results, queryErr := d.Query(context.Background(), &propertyv1.QueryRequest{
		Groups: []string{schemaGroup},
		Name:   "test",
	})
	if queryErr != nil {
		return false
	}
	foundIDs := make(map[string]bool)
	for _, r := range results {
		if r.DeleteTime() == 0 {
			foundIDs[string(r.ID())] = true
		}
	}
	for _, expectedID := range expectedIDs {
		if !foundIDs[expectedID] {
			return false
		}
	}
	return true
}

func queryLatestVersion(d db.Database, group, name, id string) int64 {
	results, queryErr := d.Query(context.Background(), &propertyv1.QueryRequest{
		Groups: []string{group},
		Name:   name,
		Ids:    []string{id},
	})
	if queryErr != nil {
		return -1
	}
	var latest int64
	for _, r := range results {
		if r.DeleteTime() == 0 && r.Timestamp() > latest {
			latest = r.Timestamp()
		}
	}
	return latest
}

func verifyPropertyData(t *testing.T, d db.Database, id string, expectedVersion int64) {
	t.Helper()
	results, queryErr := d.Query(context.Background(), &propertyv1.QueryRequest{
		Groups: []string{schemaGroup},
		Name:   "test",
		Ids:    []string{id},
	})
	require.NoError(t, queryErr)
	var found bool
	for _, r := range results {
		if r.DeleteTime() != 0 {
			continue
		}
		var prop propertyv1.Property
		require.NoError(t, protojson.Unmarshal(r.Source(), &prop))
		if prop.Id != id {
			continue
		}
		found = true
		assert.Equal(t, schemaGroup, prop.Metadata.Group, "group mismatch for %s", id)
		assert.Equal(t, "test", prop.Metadata.Name, "name mismatch for %s", id)
		assert.Equal(t, expectedVersion, prop.Metadata.ModRevision, "version mismatch for %s", id)
		require.NotEmpty(t, prop.Tags, "tags should not be empty for %s", id)
		assert.Equal(t, "version", prop.Tags[0].Key, "tag key mismatch for %s", id)
		assert.Equal(t, fmt.Sprintf("%d", expectedVersion), prop.Tags[0].Value.GetStr().Value,
			"tag value mismatch for %s", id)
	}
	assert.True(t, found, "property %s/test/%s not found", schemaGroup, id)
}

func TestRepairServiceDataSyncTwoNodes(t *testing.T) {
	node0 := setupTestNode(t)
	node1 := setupTestNode(t)
	nodes := []*testNode{node0, node1}
	registerNodes(nodes)
	ensureShard(t, nodes)

	writeProperty(t, node0.srv.db, "test", "entity-0", 1)

	nodeIDs := []string{node0.nodeID, node1.nodeID}
	expectedIDs := []string{schemaGroup + "/test/entity-0/1"}
	propagateAndVerify(t, node0.messenger, nodeIDs, func() bool {
		return queryHasAllIDs(node1.srv.db, expectedIDs)
	})
	verifyPropertyData(t, node1.srv.db, "entity-0", 1)
}

func TestRepairServiceDataSyncThreeNodes(t *testing.T) {
	node0 := setupTestNode(t)
	node1 := setupTestNode(t)
	node2 := setupTestNode(t)
	nodes := []*testNode{node0, node1, node2}
	registerNodes(nodes)
	ensureShard(t, nodes)

	writeProperty(t, node0.srv.db, "test", "entity-0", 1)
	writeProperty(t, node1.srv.db, "test", "entity-1", 2)

	allNodeIDs := []string{node0.nodeID, node1.nodeID, node2.nodeID}
	expectedIDs := []string{
		schemaGroup + "/test/entity-0/1",
		schemaGroup + "/test/entity-1/2",
	}
	propagateAndVerify(t, node0.messenger, allNodeIDs, func() bool {
		return queryHasAllIDs(node0.srv.db, expectedIDs) &&
			queryHasAllIDs(node1.srv.db, expectedIDs) &&
			queryHasAllIDs(node2.srv.db, expectedIDs)
	})
	for _, n := range nodes {
		verifyPropertyData(t, n.srv.db, "entity-0", 1)
		verifyPropertyData(t, n.srv.db, "entity-1", 2)
	}
}

func TestRepairServiceDataSyncVersionConflict(t *testing.T) {
	node0 := setupTestNode(t)
	node1 := setupTestNode(t)
	nodes := []*testNode{node0, node1}
	registerNodes(nodes)
	ensureShard(t, nodes)

	writeProperty(t, node0.srv.db, "test", "entity-0", 1)
	writeProperty(t, node1.srv.db, "test", "entity-0", 2)

	nodeIDs := []string{node0.nodeID, node1.nodeID}
	propagateAndVerify(t, node0.messenger, nodeIDs, func() bool {
		return queryLatestVersion(node0.srv.db, schemaGroup, "test", "entity-0") == 2
	})
	verifyPropertyData(t, node0.srv.db, "entity-0", 2)
}

func TestRepairServiceDataSyncMissingToFull(t *testing.T) {
	node0 := setupTestNode(t)
	node1 := setupTestNode(t)
	nodes := []*testNode{node0, node1}
	registerNodes(nodes)
	ensureShard(t, nodes)

	writeProperty(t, node0.srv.db, "test", "entity-0", 1)
	writeProperty(t, node0.srv.db, "test", "entity-1", 2)
	writeProperty(t, node0.srv.db, "test", "entity-2", 3)

	nodeIDs := []string{node0.nodeID, node1.nodeID}
	expectedIDs := []string{
		schemaGroup + "/test/entity-0/1",
		schemaGroup + "/test/entity-1/2",
		schemaGroup + "/test/entity-2/3",
	}
	propagateAndVerify(t, node0.messenger, nodeIDs, func() bool {
		return queryHasAllIDs(node1.srv.db, expectedIDs)
	})
	verifyPropertyData(t, node1.srv.db, "entity-0", 1)
	verifyPropertyData(t, node1.srv.db, "entity-1", 2)
	verifyPropertyData(t, node1.srv.db, "entity-2", 3)
}
