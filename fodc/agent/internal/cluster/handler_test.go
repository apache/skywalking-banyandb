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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

type mockProxySender struct {
	states []*databasev1.GetClusterStateResponse
	err    error
}

func (m *mockProxySender) SendClusterState(_ context.Context, state *databasev1.GetClusterStateResponse) error {
	if m.err != nil {
		return m.err
	}
	m.states = append(m.states, state)
	return nil
}

func initTestLogger(t *testing.T) *logger.Logger {
	t.Helper()
	initErr := logger.Init(logger.Logging{Env: "dev", Level: "debug"})
	require.NoError(t, initErr)
	return logger.GetLogger("test", "cluster")
}

func TestNewHandler(t *testing.T) {
	ctx := context.Background()
	proxySender := &mockProxySender{}
	log := initTestLogger(t)

	handler := NewHandler(ctx, proxySender, log)

	require.NotNil(t, handler)
	assert.Equal(t, proxySender, handler.proxySender)
	assert.Equal(t, ctx, handler.ctx)
	assert.Equal(t, log, handler.log)
}

func TestHandler_OnClusterStateUpdate_Success(t *testing.T) {
	ctx := context.Background()
	proxySender := &mockProxySender{}
	log := initTestLogger(t)
	handler := NewHandler(ctx, proxySender, log)

	clusterState := &databasev1.GetClusterStateResponse{
		RouteTables: map[string]*databasev1.RouteTable{
			"test": {
				Registered: []*databasev1.Node{},
				Active:     []string{"node1"},
				Evictable:  []string{},
			},
		},
	}

	handler.OnClusterStateUpdate(clusterState)

	require.Len(t, proxySender.states, 1)
	assert.Equal(t, clusterState, proxySender.states[0])
}

func TestHandler_OnClusterStateUpdate_NilState(t *testing.T) {
	ctx := context.Background()
	proxySender := &mockProxySender{}
	log := initTestLogger(t)
	handler := NewHandler(ctx, proxySender, log)

	handler.OnClusterStateUpdate(nil)

	// Should not send nil state
	assert.Empty(t, proxySender.states)
}

func TestHandler_OnClusterStateUpdate_SendError(t *testing.T) {
	ctx := context.Background()
	proxySender := &mockProxySender{
		err: errors.New("send failed"),
	}
	log := initTestLogger(t)
	handler := NewHandler(ctx, proxySender, log)

	clusterState := &databasev1.GetClusterStateResponse{
		RouteTables: map[string]*databasev1.RouteTable{
			"test": {
				Registered: []*databasev1.Node{},
				Active:     []string{"node1"},
				Evictable:  []string{},
			},
		},
	}

	// Should not panic on error
	handler.OnClusterStateUpdate(clusterState)

	// State was attempted to be sent
	assert.Empty(t, proxySender.states)
}

func TestHandler_OnClusterStateUpdate_NoProxySender(t *testing.T) {
	ctx := context.Background()
	log := initTestLogger(t)
	handler := NewHandler(ctx, nil, log)

	clusterState := &databasev1.GetClusterStateResponse{
		RouteTables: map[string]*databasev1.RouteTable{
			"test": {
				Registered: []*databasev1.Node{},
				Active:     []string{"node1"},
				Evictable:  []string{},
			},
		},
	}

	// Should not panic when proxySender is nil
	handler.OnClusterStateUpdate(clusterState)
}
