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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
)

type mockClusterStateHandler struct {
	currentNode *databasev1.Node
	states      []*databasev1.GetClusterStateResponse
	mu          sync.RWMutex
}

func (m *mockClusterStateHandler) OnClusterStateUpdate(state *databasev1.GetClusterStateResponse) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.states = append(m.states, state)
}

func (m *mockClusterStateHandler) OnCurrentNodeUpdate(node *databasev1.Node) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.currentNode = node
}

func (m *mockClusterStateHandler) GetStates() []*databasev1.GetClusterStateResponse {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.states
}

func (m *mockClusterStateHandler) GetCurrentNode() *databasev1.Node {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.currentNode
}

func TestNewCollector(t *testing.T) {
	handler := &mockClusterStateHandler{}
	collector := NewCollector(handler, "localhost:17914", 10*time.Second)

	require.NotNil(t, collector)
	assert.Equal(t, "localhost:17914", collector.lifecycleAddr)
	assert.Equal(t, 10*time.Second, collector.interval)
	assert.Equal(t, handler, collector.handler)
	assert.NotNil(t, collector.closer)
	assert.False(t, collector.closer.Closed())
}

func TestCollector_Stop_NotStarted(t *testing.T) {
	handler := &mockClusterStateHandler{}
	collector := NewCollector(handler, "localhost:17914", 10*time.Second)

	// Should not panic when stopping a collector that hasn't started
	collector.Stop()

	assert.True(t, collector.closer.Closed())
}

func TestCollector_Stop_MultipleCalls(t *testing.T) {
	handler := &mockClusterStateHandler{}
	collector := NewCollector(handler, "localhost:17914", 10*time.Second)

	// Multiple Stop calls should be safe
	collector.Stop()
	collector.Stop()
	collector.Stop()

	assert.True(t, collector.closer.Closed())
}

func TestCollector_Start_AfterStop(t *testing.T) {
	handler := &mockClusterStateHandler{}
	collector := NewCollector(handler, "localhost:17914", 10*time.Second)

	// Stop before starting
	collector.Stop()

	// Attempt to start should fail
	ctx := context.Background()
	err := collector.Start(ctx)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "stopped and cannot be restarted")
	assert.True(t, collector.closer.Closed())
}

func TestCollector_FetchClusterState_NoClient(t *testing.T) {
	handler := &mockClusterStateHandler{}
	collector := NewCollector(handler, "localhost:17914", 10*time.Second)

	ctx := context.Background()
	state, err := collector.fetchClusterState(ctx)

	assert.Error(t, err)
	assert.Nil(t, state)
	assert.Contains(t, err.Error(), "gRPC client not initialized")
}
