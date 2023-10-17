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

package schema

import (
	"context"
	"path"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
)

var nodeKeyPrefix = "/nodes/"

func (e *etcdSchemaRegistry) ListNode(ctx context.Context, role databasev1.Role) ([]*databasev1.Node, error) {
	if role == databasev1.Role_ROLE_UNSPECIFIED {
		return nil, BadRequest("group", "group should not be empty")
	}
	messages, err := e.listWithPrefix(ctx, nodeKeyPrefix, KindNode)
	if err != nil {
		return nil, err
	}
	entities := make([]*databasev1.Node, 0, len(messages))
	for _, message := range messages {
		node := message.(*databasev1.Node)
		for _, r := range node.Roles {
			if r == role {
				entities = append(entities, node)
				break
			}
		}
	}
	return entities, nil
}

func (e *etcdSchemaRegistry) RegisterNode(ctx context.Context, node *databasev1.Node, forced bool) error {
	return e.register(ctx, Metadata{
		TypeMeta: TypeMeta{
			Kind: KindNode,
			Name: node.Metadata.Name,
		},
		Spec: node,
	}, forced)
}

func formatNodeKey(name string) string {
	return path.Join(nodeKeyPrefix, name)
}
