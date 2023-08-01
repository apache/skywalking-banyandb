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

	"google.golang.org/protobuf/proto"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
)

var nodeKeyPrefix = "/nodes/"

func (e *etcdSchemaRegistry) ListNode(ctx context.Context, role Role) ([]*databasev1.Node, error) {
	if role == "" {
		return nil, BadRequest("group", "group should not be empty")
	}
	messages, err := e.listWithPrefix(ctx, listPrefixesForEntity(string(role), nodeKeyPrefix), func() proto.Message {
		return &databasev1.Node{}
	})
	if err != nil {
		return nil, err
	}
	entities := make([]*databasev1.Node, 0, len(messages))
	for _, message := range messages {
		entities = append(entities, message.(*databasev1.Node))
	}
	return entities, nil
}

func formatNodePrefix(role Role) string {
	return nodeKeyPrefix + string(role)
}

func formatNodeKey(role Role, id string) string {
	return formatNodePrefix(role) + "/" + id
}
