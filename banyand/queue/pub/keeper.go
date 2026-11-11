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
	"sync"
	"time"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

// DataNode is the metadata that registers the data node at addr under name on
// a client made by NewWithoutMetadata. The ROLE_DATA role is required: the
// client's role gate (allowedRoles=[ROLE_DATA]) silently drops a node whose
// roles do not intersect, which would leave the client with nowhere to send.
func DataNode(name, addr string) schema.Metadata {
	return schema.Metadata{
		TypeMeta: schema.TypeMeta{Kind: schema.KindNode},
		Spec: &databasev1.Node{
			Metadata:    &commonv1.Metadata{Name: name},
			GrpcAddress: addr,
			Roles:       []databasev1.Role{databasev1.Role_ROLE_DATA},
		},
	}
}

// KeepNode registers node on client and keeps it connected until the returned
// function is called. A client made by NewWithoutMetadata runs a synchronous
// health check at registration and does not retry an evicted node
// (healthCheckInterval is 0), so the keeper registers the node again whenever
// it is not healthy -- covering both an initial dial that failed (the node is
// not ready yet) and a later failover eviction.
//
// Stop is idempotent and returns once the keeper has exited.
func KeepNode(client queue.Client, node schema.Metadata, interval time.Duration, l *logger.Logger) (stop func()) {
	// The initial registration dials the node, so writes can go there at once.
	client.OnAddOrUpdate(node)
	done := make(chan struct{})
	exited := make(chan struct{})
	run.GoOrDie(context.Background(), "queue.pub.node-keeper", l, func(_ context.Context) {
		defer close(exited)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				// HealthyNodes reflects the active-connection state directly.
				// connMgr.OnAddOrUpdate fans out OnActive synchronously, so an
				// empty set means the node is not connected; force a fresh dial.
				if len(client.HealthyNodes()) == 0 {
					// connMgr.OnAddOrUpdate short-circuits an evictable node, so
					// drop it first to force a fresh dial. queue.Client does not
					// expose OnDelete, hence the type assertion.
					if deleter, ok := client.(interface{ OnDelete(schema.Metadata) }); ok {
						deleter.OnDelete(node)
					}
					client.OnAddOrUpdate(node)
				}
			}
		}
	})
	var once sync.Once
	return func() {
		once.Do(func() { close(done) })
		<-exited
	}
}
