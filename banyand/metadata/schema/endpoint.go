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

var endpointKeyPrefix = "/endpoints/"

func (e *etcdSchemaRegistry) ListEndpoint(ctx context.Context, nodeRole databasev1.Role) ([]*databasev1.Endpoint, error) {
	nn, err := e.ListNode(ctx, nodeRole)
	if err != nil {
		return nil, err
	}
	messages, err := e.listWithPrefix(ctx, endpointKeyPrefix, func() proto.Message {
		return &databasev1.Node{}
	})
	if err != nil {
		return nil, err
	}
	entities := make([]*databasev1.Endpoint, 0, len(messages))
	for _, message := range messages {
		endpoint := message.(*databasev1.Endpoint)
		for _, n := range nn {
			if n.Name == endpoint.Node {
				entities = append(entities, endpoint)
				break
			}
		}
	}
	return entities, nil
}

func (e *etcdSchemaRegistry) RegisterEndpoint(ctx context.Context, endpoint *databasev1.Endpoint) error {
	return e.register(ctx, Metadata{
		TypeMeta: TypeMeta{
			Kind: KindEndpoint,
			Name: endpoint.Name,
		},
		Spec: endpoint,
	})
}

func formatEndpointKey(name string) string {
	return endpointKeyPrefix + name
}
