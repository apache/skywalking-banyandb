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

package none

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
)

type testHandler struct {
	added  []schema.Metadata
	delete []schema.Metadata
	mu     sync.Mutex
}

func (h *testHandler) OnAddOrUpdate(m schema.Metadata) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.added = append(h.added, m)
}

func (h *testHandler) OnDelete(m schema.Metadata) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.delete = append(h.delete, m)
}

func (h *testHandler) OnInit(_ []schema.Kind) (bool, []int64) { return false, nil }

func newTestContext() context.Context {
	node := common.Node{
		NodeID:      "test-node",
		GrpcAddress: "127.0.0.1:17912",
	}
	roles := []databasev1.Role{databasev1.Role_ROLE_DATA, databasev1.Role_ROLE_LIAISON}
	ctx := context.WithValue(context.Background(), common.ContextNodeKey, node)
	return context.WithValue(ctx, common.ContextNodeRolesKey, roles)
}

func TestNewService(t *testing.T) {
	t.Run("extracts node and roles from context", func(t *testing.T) {
		ctx := newTestContext()
		svc := NewService(ctx)
		nodes, err := svc.ListNode(ctx, databasev1.Role_ROLE_UNSPECIFIED)
		require.NoError(t, err)
		require.Len(t, nodes, 1)
		assert.Equal(t, "test-node", nodes[0].GetMetadata().GetName())
		assert.Equal(t, "127.0.0.1:17912", nodes[0].GetGrpcAddress())
		assert.Equal(t, []databasev1.Role{databasev1.Role_ROLE_DATA, databasev1.Role_ROLE_LIAISON}, nodes[0].GetRoles())
	})

	t.Run("no node in context", func(t *testing.T) {
		svc := NewService(context.Background())
		nodes, err := svc.ListNode(context.Background(), databasev1.Role_ROLE_UNSPECIFIED)
		require.NoError(t, err)
		assert.Empty(t, nodes)
	})
}

func TestListNode(t *testing.T) {
	ctx := newTestContext()

	t.Run("matching role", func(t *testing.T) {
		svc := NewService(ctx)
		nodes, err := svc.ListNode(ctx, databasev1.Role_ROLE_DATA)
		require.NoError(t, err)
		require.Len(t, nodes, 1)
		assert.Equal(t, "test-node", nodes[0].GetMetadata().GetName())
	})

	t.Run("non-matching role", func(t *testing.T) {
		svc := NewService(ctx)
		nodes, err := svc.ListNode(ctx, databasev1.Role_ROLE_META)
		require.NoError(t, err)
		assert.Empty(t, nodes)
	})

	t.Run("ROLE_UNSPECIFIED returns current node", func(t *testing.T) {
		svc := NewService(ctx)
		nodes, err := svc.ListNode(ctx, databasev1.Role_ROLE_UNSPECIFIED)
		require.NoError(t, err)
		require.Len(t, nodes, 1)
		assert.Equal(t, "test-node", nodes[0].GetMetadata().GetName())
	})
}

func TestGetNode(t *testing.T) {
	ctx := newTestContext()
	svc := NewService(ctx)

	t.Run("matching name", func(t *testing.T) {
		result, err := svc.GetNode(ctx, "test-node")
		require.NoError(t, err)
		assert.Equal(t, "test-node", result.GetMetadata().GetName())
	})

	t.Run("non-matching name", func(t *testing.T) {
		_, err := svc.GetNode(ctx, "other-node")
		require.Error(t, err)
	})
}

func TestRegisterNode(t *testing.T) {
	svc := NewService(context.Background())
	err := svc.RegisterNode(context.Background(), &databasev1.Node{}, false)
	require.ErrorIs(t, err, errNotSupported)
}

func TestUpdateNode(t *testing.T) {
	svc := NewService(context.Background())
	err := svc.UpdateNode(context.Background(), &databasev1.Node{})
	require.ErrorIs(t, err, errNotSupported)
}

func TestRegisterHandlerAndStart(t *testing.T) {
	t.Run("handler receives node event on Start", func(t *testing.T) {
		ctx := newTestContext()
		svc := NewService(ctx)

		handler := &testHandler{}
		svc.RegisterHandler("test-handler", schema.KindNode, handler)

		err := svc.Start(ctx)
		require.NoError(t, err)

		handler.mu.Lock()
		defer handler.mu.Unlock()
		require.Len(t, handler.added, 1)
		assert.Equal(t, schema.KindNode, handler.added[0].Kind)
		assert.Equal(t, "test-node", handler.added[0].Name)
	})

	t.Run("multiple handlers receive node events", func(t *testing.T) {
		ctx := newTestContext()
		svc := NewService(ctx)

		handler1 := &testHandler{}
		handler2 := &testHandler{}
		svc.RegisterHandler("handler-1", schema.KindNode, handler1)
		svc.RegisterHandler("handler-2", schema.KindNode, handler2)

		err := svc.Start(ctx)
		require.NoError(t, err)

		handler1.mu.Lock()
		require.Len(t, handler1.added, 1)
		handler1.mu.Unlock()

		handler2.mu.Lock()
		require.Len(t, handler2.added, 1)
		handler2.mu.Unlock()
	})

	t.Run("no node in context - handler receives no events", func(t *testing.T) {
		svc := NewService(context.Background())

		handler := &testHandler{}
		svc.RegisterHandler("test-handler", schema.KindNode, handler)

		err := svc.Start(context.Background())
		require.NoError(t, err)

		handler.mu.Lock()
		defer handler.mu.Unlock()
		assert.Empty(t, handler.added)
	})
}
