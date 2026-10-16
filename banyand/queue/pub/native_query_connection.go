// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pub

import (
	"fmt"

	"go.uber.org/multierr"
	"google.golang.org/grpc"

	clusterv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/cluster/v1"
)

const (
	nativeQueryReceiveWindow           = 64 << 10
	nativeQueryConnectionReceiveWindow = 16 << 20
)

type nativeQueryConnection struct {
	conn   *grpc.ClientConn
	client clusterv1.ServiceClient
}

// nativeQueryClient isolates measure receive flow control from other queue RPCs.
// The connection follows the same node generation, credentials and TLS reload
// lifecycle as the primary queue connection; its small static window bounds
// pending responses. Connection credit is shared rather than separately resident:
// each guarded stream prepays its window, accepted-message read delta and codec
// materialization, so a larger connection window avoids serializing their credit.
func (p *pub) nativeQueryClient(c *client) (clusterv1.ServiceClient, error) {
	p.nativeQueryMu.Lock()
	defer p.nativeQueryMu.Unlock()
	if p.nativeQueriesClosed || c.nativeInactive.Load() {
		return nil, fmt.Errorf("native measure connection is closed")
	}
	if cached := p.nativeQueries[c]; cached != nil {
		return cached.client, nil
	}
	if c.conn == nil {
		return nil, fmt.Errorf("native measure connection has no primary node connection")
	}
	opts, err := p.getClientTransportCredentials()
	if err != nil {
		return nil, err
	}
	opts = append(opts, grpc.WithStaticStreamWindowSize(nativeQueryReceiveWindow),
		grpc.WithStaticConnWindowSize(nativeQueryConnectionReceiveWindow),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxReceiveMessageSize)))
	if p.retryPolicy != "" {
		opts = append(opts, grpc.WithDefaultServiceConfig(p.retryPolicy))
	}
	conn, err := grpc.NewClient(c.conn.Target(), opts...)
	if err != nil {
		return nil, err
	}
	cached := &nativeQueryConnection{conn: conn, client: clusterv1.NewServiceClient(conn)}
	if p.nativeQueries == nil {
		p.nativeQueries = make(map[*client]*nativeQueryConnection)
	}
	p.nativeQueries[c] = cached
	return cached.client, nil
}

func (p *pub) closeNativeQueryClient(c *client) error {
	c.nativeInactive.Store(true)
	p.nativeQueryMu.Lock()
	defer p.nativeQueryMu.Unlock()
	cached := p.nativeQueries[c]
	delete(p.nativeQueries, c)
	if cached != nil {
		return cached.conn.Close()
	}
	return nil
}

func (p *pub) closeNativeQueryClients() error {
	p.nativeQueryMu.Lock()
	defer p.nativeQueryMu.Unlock()
	p.nativeQueriesClosed = true
	var err error
	for _, cached := range p.nativeQueries {
		err = multierr.Append(err, cached.conn.Close())
	}
	p.nativeQueries = nil
	return err
}
