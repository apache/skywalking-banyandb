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
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"

	clusterv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/cluster/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

const rpcTimeout = 2 * time.Second

var (
	// Retry policy for health check.
	initBackoff       = time.Second
	maxBackoff        = 20 * time.Second
	backoffMultiplier = 2.0

	serviceName = clusterv1.Service_ServiceDesc.ServiceName

	// The timeout is set by each RPC.
	retryPolicy = fmt.Sprintf(`{
	"methodConfig": [{
	  "name": [{"service": "%s"}],
	  "waitForReady": true,
	  "retryPolicy": {
	      "MaxAttempts": 4,
	      "InitialBackoff": ".5s",
	      "MaxBackoff": "10s",
	      "BackoffMultiplier": 1.0,
	      "RetryableStatusCodes": [ "UNAVAILABLE" ]
	  }
	}]}`, serviceName)
)

type client struct {
	client clusterv1.ServiceClient
	conn   *grpc.ClientConn
	md     schema.Metadata
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
	if _, ok := p.evictClients[name]; ok {
		return
	}
	conn, err := grpc.Dial(address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultServiceConfig(retryPolicy))
	if err != nil {
		p.log.Error().Err(err).Msg("failed to connect to grpc server")
		return
	}

	if !p.checkClient(conn, md) {
		return
	}

	c := clusterv1.NewServiceClient(conn)
	p.clients[name] = &client{conn: conn, client: c, md: md}
	if p.handler != nil {
		p.handler.OnAddOrUpdate(md)
	}
	p.log.Info().Stringer("node", node).Msg("new node is healthy, add it to active queue")
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
	if en, ok := p.evictClients[name]; ok {
		close(en.c)
		delete(p.evictClients, name)
		p.log.Info().Stringer("node", node).Msg("node is removed from evict queue by delete event")
		return
	}

	if client, ok := p.clients[name]; ok && !p.healthCheck(node, client.conn) {
		_ = client.conn.Close()
		delete(p.clients, name)
		if p.handler != nil {
			p.handler.OnDelete(md)
		}
		p.log.Info().Stringer("node", node).Msg("node is removed from active queue by delete event")
	}
}

func (p *pub) checkClient(conn *grpc.ClientConn, md schema.Metadata) bool {
	node, ok := md.Spec.(*databasev1.Node)
	if !ok {
		logger.Panicf("failed to cast node spec")
		return false
	}
	if p.healthCheck(node, conn) {
		return true
	}
	_ = conn.Close()
	if !p.closer.AddRunning() {
		return false
	}
	p.log.Info().Stringer("node", node).Msg("node is unhealthy, move it to evict queue")
	name := node.Metadata.Name
	p.evictClients[name] = evictNode{n: node, c: make(chan struct{})}
	if p.handler != nil {
		p.handler.OnDelete(md)
	}
	go func(p *pub, name string, en evictNode, md schema.Metadata) {
		defer p.closer.Done()
		backoff := initBackoff
		for {
			select {
			case <-time.After(backoff):
				connEvict, errEvict := grpc.Dial(node.GrpcAddress, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultServiceConfig(retryPolicy))
				if errEvict == nil && p.healthCheck(en.n, connEvict) {
					p.mu.Lock()
					defer p.mu.Unlock()
					if _, ok := p.evictClients[name]; !ok {
						// The client has been removed from evict clients map, just return
						return
					}
					c := clusterv1.NewServiceClient(connEvict)
					p.clients[name] = &client{conn: connEvict, client: c, md: md}
					if p.handler != nil {
						p.handler.OnAddOrUpdate(md)
					}
					delete(p.evictClients, name)
					p.log.Info().Stringer("node", en.n).Msg("node is healthy, move it back to active queue")
					return
				}
				if errEvict != nil {
					_ = connEvict.Close()
				}
				p.log.Error().Err(errEvict).Msgf("failed to re-connect to grpc server after waiting for %s", backoff)
			case <-en.c:
				return
			}
			if backoff < maxBackoff {
				backoff *= time.Duration(backoffMultiplier)
			} else {
				backoff = maxBackoff
			}
		}
	}(p, name, p.evictClients[name], md)
	return false
}

func (p *pub) healthCheck(node fmt.Stringer, conn *grpc.ClientConn) bool {
	var resp *grpc_health_v1.HealthCheckResponse
	if err := grpchelper.Request(context.Background(), rpcTimeout, func(rpcCtx context.Context) (err error) {
		resp, err = grpc_health_v1.NewHealthClient(conn).Check(rpcCtx,
			&grpc_health_v1.HealthCheckRequest{
				Service: "",
			})
		return err
	}); err != nil {
		if e := p.log.Debug(); e.Enabled() {
			e.Err(err).Stringer("node", node).Msg("service unhealthy")
		}
		return false
	}
	if resp.GetStatus() == grpc_health_v1.HealthCheckResponse_SERVING {
		return true
	}
	return false
}

func (p *pub) failover(node string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if en, ok := p.evictClients[node]; ok {
		close(en.c)
		delete(p.evictClients, node)
		p.log.Info().Str("node", node).Msg("node is removed from evict queue by wire event")
		return
	}

	if client, ok := p.clients[node]; ok && !p.checkClient(client.conn, client.md) {
		_ = client.conn.Close()
		delete(p.clients, node)
		if p.handler != nil {
			p.handler.OnDelete(client.md)
		}
	}
}

type evictNode struct {
	n *databasev1.Node
	c chan struct{}
}
