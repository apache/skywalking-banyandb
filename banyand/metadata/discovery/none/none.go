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

// Package none implements a no-op node discovery for standalone mode.
package none

import (
	"context"
	"errors"

	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
)

var _ schema.NodeDiscovery = (*noneDiscovery)(nil)

var errNotSupported = errors.New("none discovery does not support node registration or update")

type noneDiscovery struct {
	curNode *databasev1.Node
}

// NewService creates a new none discovery service for standalone mode.
// It extracts the current node information from the context.
func NewService(ctx context.Context) schema.NodeDiscovery {
	n := &noneDiscovery{}
	if val := ctx.Value(common.ContextNodeKey); val != nil {
		node := val.(common.Node)
		var nodeRoles []databasev1.Role
		if rolesVal := ctx.Value(common.ContextNodeRolesKey); rolesVal != nil {
			nodeRoles = rolesVal.([]databasev1.Role)
		}
		n.curNode = node.ToProtoNode(nodeRoles)
	}
	return n
}

// ListNode returns the current node if it matches the given role.
func (n *noneDiscovery) ListNode(_ context.Context, role databasev1.Role) ([]*databasev1.Node, error) {
	if n.curNode == nil {
		return nil, nil
	}
	for _, r := range n.curNode.GetRoles() {
		if r == role {
			return []*databasev1.Node{n.curNode}, nil
		}
	}
	return nil, nil
}

// GetNode returns the current node if the name matches.
func (n *noneDiscovery) GetNode(_ context.Context, name string) (*databasev1.Node, error) {
	if n.curNode != nil && n.curNode.GetMetadata().GetName() == name {
		return n.curNode, nil
	}
	return nil, schema.ErrGRPCResourceNotFound
}

// RegisterNode is not supported in none discovery mode.
func (n *noneDiscovery) RegisterNode(_ context.Context, _ *databasev1.Node, _ bool) error {
	return errNotSupported
}

// UpdateNode is not supported in none discovery mode.
func (n *noneDiscovery) UpdateNode(_ context.Context, _ *databasev1.Node) error {
	return errNotSupported
}

// RegisterHandler is a no-op for none discovery.
func (n *noneDiscovery) RegisterHandler(string, schema.Kind, schema.EventHandler) {}

// Start is a no-op for none discovery.
func (n *noneDiscovery) Start(context.Context) error {
	return nil
}

// Close is a no-op for none discovery.
func (n *noneDiscovery) Close() error {
	return nil
}
