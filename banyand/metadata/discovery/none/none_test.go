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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
)

func TestNewService(t *testing.T) {
	t.Run("extracts node and roles from context", func(t *testing.T) {
		node := common.Node{
			NodeID:      "test-node",
			GrpcAddress: "127.0.0.1:17912",
		}
		roles := []databasev1.Role{databasev1.Role_ROLE_DATA, databasev1.Role_ROLE_LIAISON}
		ctx := context.WithValue(context.Background(), common.ContextNodeKey, node)
		ctx = context.WithValue(ctx, common.ContextNodeRolesKey, roles)

		svc := NewService(ctx)
		nd := svc.(*noneDiscovery)
		require.NotNil(t, nd.curNode)
		assert.Equal(t, "test-node", nd.curNode.GetMetadata().GetName())
		assert.Equal(t, "127.0.0.1:17912", nd.curNode.GetGrpcAddress())
		assert.Equal(t, roles, nd.curNode.GetRoles())
	})

	t.Run("no node in context", func(t *testing.T) {
		svc := NewService(context.Background())
		nd := svc.(*noneDiscovery)
		assert.Nil(t, nd.curNode)
	})
}

func TestListNode(t *testing.T) {
	node := common.Node{
		NodeID:      "test-node",
		GrpcAddress: "127.0.0.1:17912",
	}
	roles := []databasev1.Role{databasev1.Role_ROLE_DATA}

	t.Run("matching role", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), common.ContextNodeKey, node)
		ctx = context.WithValue(ctx, common.ContextNodeRolesKey, roles)
		svc := NewService(ctx)

		nodes, err := svc.ListNode(ctx, databasev1.Role_ROLE_DATA)
		require.NoError(t, err)
		require.Len(t, nodes, 1)
		assert.Equal(t, "test-node", nodes[0].GetMetadata().GetName())
	})

	t.Run("non-matching role", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), common.ContextNodeKey, node)
		ctx = context.WithValue(ctx, common.ContextNodeRolesKey, roles)
		svc := NewService(ctx)

		nodes, err := svc.ListNode(ctx, databasev1.Role_ROLE_LIAISON)
		require.NoError(t, err)
		assert.Empty(t, nodes)
	})

	t.Run("ROLE_UNSPECIFIED returns current node", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), common.ContextNodeKey, node)
		ctx = context.WithValue(ctx, common.ContextNodeRolesKey, roles)
		svc := NewService(ctx)

		nodes, err := svc.ListNode(ctx, databasev1.Role_ROLE_UNSPECIFIED)
		require.NoError(t, err)
		require.Len(t, nodes, 1)
		assert.Equal(t, "test-node", nodes[0].GetMetadata().GetName())
	})

	t.Run("curNode is nil", func(t *testing.T) {
		svc := NewService(context.Background())

		nodes, err := svc.ListNode(context.Background(), databasev1.Role_ROLE_UNSPECIFIED)
		require.NoError(t, err)
		assert.Nil(t, nodes)
	})
}

func TestGetNode(t *testing.T) {
	node := common.Node{
		NodeID:      "test-node",
		GrpcAddress: "127.0.0.1:17912",
	}
	roles := []databasev1.Role{databasev1.Role_ROLE_DATA}
	ctx := context.WithValue(context.Background(), common.ContextNodeKey, node)
	ctx = context.WithValue(ctx, common.ContextNodeRolesKey, roles)
	svc := NewService(ctx)

	t.Run("matching name", func(t *testing.T) {
		result, err := svc.GetNode(ctx, "test-node")
		require.NoError(t, err)
		assert.Equal(t, "test-node", result.GetMetadata().GetName())
	})

	t.Run("non-matching name", func(t *testing.T) {
		_, err := svc.GetNode(ctx, "other-node")
		require.ErrorIs(t, err, schema.ErrGRPCResourceNotFound)
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
