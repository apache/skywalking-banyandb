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

	if n, ok := p.registered[name]; ok {
		if n.GrpcAddress == address {
			return
		}
		if en, ok := p.evictable[name]; ok {
			close(en.c)
			delete(p.evictable, name)
			p.log.Info().Str("node", name).Str("status", p.dump()).Msg("node is removed from evict queue by the new gRPC address updated event")
		}

		if client, ok := p.active[name]; ok {
			_ = client.conn.Close()
			delete(p.active, name)
			if p.handler != nil {
				p.handler.OnDelete(client.md)
			}
			p.log.Info().Str("status", p.dump()).Str("node", name).Msg("node is removed from active queue by the new gRPC address updated event")
		}
	}
	p.registered[name] = node

	if _, ok := p.active[name]; ok {
		return
	}
	if _, ok := p.evictable[name]; ok {
		return
	}
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultServiceConfig(retryPolicy))
	if err != nil {
		p.log.Error().Err(err).Msg("failed to connect to grpc server")
		return
	}

	if !p.checkClientHealthAndReconnect(conn, md) {
		p.log.Info().Str("status", p.dump()).Stringer("node", node).Msg("node is unhealthy, move it to evict queue")
		return
	}

	c := clusterv1.NewServiceClient(conn)
	p.active[name] = &client{conn: conn, client: c, md: md}
	if p.handler != nil {
		p.handler.OnAddOrUpdate(md)
	}
	p.log.Info().Str("status", p.dump()).Stringer("node", node).Msg("new node is healthy, add it to active queue")
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
	delete(p.registered, name)
	if en, ok := p.evictable[name]; ok {
		close(en.c)
		delete(p.evictable, name)
		p.log.Info().Str("status", p.dump()).Stringer("node", node).Msg("node is removed from evict queue by delete event")
		return
	}

	if client, ok := p.active[name]; ok {
		if p.removeNodeIfUnhealthy(md, node, client) {
			p.log.Info().Str("status", p.dump()).Stringer("node", node).Msg("remove node from active queue by delete event")
			return
		}
		if !p.closer.AddRunning() {
			return
		}
		go func() {
			defer p.closer.Done()
			backoff := initBackoff
			var elapsed time.Duration
			for {
				select {
				case <-time.After(backoff):
					if func() bool {
						elapsed += backoff
						p.mu.Lock()
						defer p.mu.Unlock()
						if _, ok := p.registered[name]; ok {
							// The client has been added back to registered clients map, just return
							return true
						}
						if p.removeNodeIfUnhealthy(md, node, client) {
							p.log.Info().Str("status", p.dump()).Stringer("node", node).Dur("after", elapsed).Msg("remove node from active queue by delete event")
							return true
						}
						return false
					}() {
						return
					}
				case <-p.closer.CloseNotify():
					return
				}
				if backoff < maxBackoff {
					backoff *= time.Duration(backoffMultiplier)
				} else {
					backoff = maxBackoff
				}
			}
		}()
	}
}

func (p *pub) removeNodeIfUnhealthy(md schema.Metadata, node *databasev1.Node, client *client) bool {
	if p.healthCheck(node, client.conn) {
		return false
	}
	_ = client.conn.Close()
	name := node.Metadata.GetName()
	delete(p.active, name)
	if p.handler != nil {
		p.handler.OnDelete(md)
	}
	return true
}

func (p *pub) checkClientHealthAndReconnect(conn *grpc.ClientConn, md schema.Metadata) bool {
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
	name := node.Metadata.Name
	p.evictable[name] = evictNode{n: node, c: make(chan struct{})}
	if p.handler != nil {
		p.handler.OnDelete(md)
	}
	go func(p *pub, name string, en evictNode, md schema.Metadata) {
		defer p.closer.Done()
		backoff := initBackoff
		for {
			select {
			case <-time.After(backoff):
				connEvict, errEvict := grpc.NewClient(node.GrpcAddress, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultServiceConfig(retryPolicy))
				if errEvict == nil && p.healthCheck(en.n, connEvict) {
					func() {
						p.mu.Lock()
						defer p.mu.Unlock()
						if _, ok := p.evictable[name]; !ok {
							// The client has been removed from evict clients map, just return
							return
						}
						c := clusterv1.NewServiceClient(connEvict)
						p.active[name] = &client{conn: connEvict, client: c, md: md}
						if p.handler != nil {
							p.handler.OnAddOrUpdate(md)
						}
						delete(p.evictable, name)
						p.log.Info().Str("status", p.dump()).Stringer("node", en.n).Msg("node is healthy, move it back to active queue")
					}()
					return
				}
				if errEvict != nil {
					_ = connEvict.Close()
				}
				if _, ok := p.registered[name]; !ok {
					return
				}
				p.log.Error().Err(errEvict).Msgf("failed to re-connect to grpc server after waiting for %s", backoff)
			case <-en.c:
				return
			case <-p.closer.CloseNotify():
				return
			}
			if backoff < maxBackoff {
				backoff *= time.Duration(backoffMultiplier)
			} else {
				backoff = maxBackoff
			}
		}
	}(p, name, p.evictable[name], md)
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
	if en, ok := p.evictable[node]; ok {
		close(en.c)
		delete(p.evictable, node)
		p.log.Info().Str("node", node).Str("status", p.dump()).Msg("node is removed from evict queue by wire event")
		return
	}

	if client, ok := p.active[node]; ok && !p.checkClientHealthAndReconnect(client.conn, client.md) {
		_ = client.conn.Close()
		delete(p.active, node)
		if p.handler != nil {
			p.handler.OnDelete(client.md)
		}
		p.log.Info().Str("status", p.dump()).Str("node", node).Msg("node is unhealthy, move it to evict queue")
	}
}

func (p *pub) dump() string {
	keysRegistered := make([]string, 0, len(p.registered))
	for k := range p.registered {
		keysRegistered = append(keysRegistered, k)
	}
	keysActive := make([]string, 0, len(p.active))
	for k := range p.active {
		keysActive = append(keysActive, k)
	}
	keysEvictable := make([]string, 0, len(p.evictable))
	for k := range p.evictable {
		keysEvictable = append(keysEvictable, k)
	}
	return fmt.Sprintf("registered: %v, active :%v, evictable :%v", keysRegistered, keysActive, keysEvictable)
}

type evictNode struct {
	n *databasev1.Node
	c chan struct{}
}
