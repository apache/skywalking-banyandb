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

package pub

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	clusterv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/cluster/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
)

var retryPolicy = `{
	"methodConfig": [{
	  "name": [{"service": "grpc.examples.echo.Echo"}],
	  "waitForReady": true,
	  "retryPolicy": {
		  "MaxAttempts": 4,
		  "InitialBackoff": ".5s",
		  "MaxBackoff": "10s",
		  "BackoffMultiplier": 1.0,
		  "RetryableStatusCodes": [ "UNAVAILABLE" ]
	  }
	}]}`

type client struct {
	client clusterv1.ServiceClient
	conn   *grpc.ClientConn
}

func (p *pub) OnAddOrUpdate(md schema.Metadata) {
	if md.Kind != schema.KindNode {
		return
	}
	node, ok := md.Spec.(*databasev1.Node)
	if !ok {
		p.log.Warn().Msg("failed to cast node spec")
		return
	}
	var hasDataRole bool
	for _, r := range node.Roles {
		if r == databasev1.Role_ROLE_DATA {
			hasDataRole = true
			break
		}
	}
	if !hasDataRole {
		return
	}

	address := node.GrpcAddress
	if address == "" {
		p.log.Warn().Stringer("node", node).Msg("grpc address is empty")
		return
	}
	name := node.Metadata.GetName()
	if name == "" {
		p.log.Warn().Stringer("node", node).Msg("node name is empty")
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	// If the client already exists, just return
	if _, ok := p.clients[name]; ok {
		return
	}
	conn, err := grpc.Dial(address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultServiceConfig(retryPolicy))
	if err != nil {
		p.log.Error().Err(err).Msg("failed to connect to grpc server")
		return
	}
	c := clusterv1.NewServiceClient(conn)
	p.clients[name] = &client{conn: conn, client: c}
	if p.handler != nil {
		p.handler.OnAddOrUpdate(md)
	}
}

func (p *pub) OnDelete(md schema.Metadata) {
	if md.Kind != schema.KindNode {
		return
	}
	node, ok := md.Spec.(*databasev1.Node)
	if !ok {
		p.log.Warn().Msg("failed to cast node spec")
		return
	}
	name := node.Metadata.GetName()
	if name == "" {
		p.log.Warn().Stringer("node", node).Msg("node name is empty")
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	if client, ok := p.clients[name]; ok {
		if client.conn != nil {
			client.conn.Close() // Close the client connection
		}
		delete(p.clients, name)
		if p.handler != nil {
			p.handler.OnDelete(md)
		}
	}
}
