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
	"time"

	"google.golang.org/grpc"

	"github.com/apache/skywalking-banyandb/api/common"
	clusterv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/cluster/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
)

const (
	rpcTimeout = 2 * time.Second
	// maxReceiveMessageSize sets the maximum message size the client can receive (32MB).
	maxReceiveMessageSize = 32 << 20
)

// The timeout is set by each RPC.
var retryPolicy = `{
	"methodConfig": [
	  {
	    "name": [{"service": "banyandb.cluster.v1.Service"}],
	    "waitForReady": true,
	    "retryPolicy": {
	        "MaxAttempts": 4,
	        "InitialBackoff": ".5s",
	        "MaxBackoff": "10s",
	        "BackoffMultiplier": 2.0,
	        "RetryableStatusCodes": [ "UNAVAILABLE", "DEADLINE_EXCEEDED", "RESOURCE_EXHAUSTED" ]
	    }
	  },
	  {
	    "name": [{"service": "banyandb.cluster.v1.ChunkedSyncService"}],
	    "waitForReady": true,
	    "retryPolicy": {
	        "MaxAttempts": 3,
	        "InitialBackoff": ".5s",
	        "MaxBackoff": "5s",
	        "BackoffMultiplier": 2.0,
	        "RetryableStatusCodes": [ "UNAVAILABLE", "DEADLINE_EXCEEDED", "RESOURCE_EXHAUSTED" ]
	    }
	  },
	  {
	    "name": [{"service": "banyandb.trace.v1.TraceService"}],
	    "waitForReady": true,
	    "retryPolicy": {
	        "MaxAttempts": 4,
	        "InitialBackoff": ".2s",
	        "MaxBackoff": "5s",
	        "BackoffMultiplier": 2.0,
	        "RetryableStatusCodes": [ "UNAVAILABLE", "DEADLINE_EXCEEDED", "RESOURCE_EXHAUSTED" ]
	    }
	  },
	  {
	    "name": [{"service": "banyandb.stream.v1.StreamService"}],
	    "waitForReady": true,
	    "retryPolicy": {
	        "MaxAttempts": 4,
	        "InitialBackoff": ".2s",
	        "MaxBackoff": "5s",
	        "BackoffMultiplier": 2.0,
	        "RetryableStatusCodes": [ "UNAVAILABLE", "DEADLINE_EXCEEDED", "RESOURCE_EXHAUSTED" ]
	    }
	  },
	  {
	    "name": [{"service": "banyandb.measure.v1.MeasureService"}],
	    "waitForReady": true,
	    "retryPolicy": {
	        "MaxAttempts": 4,
	        "InitialBackoff": ".2s",
	        "MaxBackoff": "5s",
	        "BackoffMultiplier": 2.0,
	        "RetryableStatusCodes": [ "UNAVAILABLE", "DEADLINE_EXCEEDED", "RESOURCE_EXHAUSTED" ]
	    }
	  },
	  {
	    "name": [{"service": "banyandb.property.v1.PropertyService"}],
	    "waitForReady": true,
	    "retryPolicy": {
	        "MaxAttempts": 4,
	        "InitialBackoff": ".2s",
	        "MaxBackoff": "5s",
	        "BackoffMultiplier": 2.0,
	        "RetryableStatusCodes": [ "UNAVAILABLE", "DEADLINE_EXCEEDED", "RESOURCE_EXHAUSTED" ]
	    }
	  }
	]}`

type client struct {
	client clusterv1.ServiceClient
	conn   *grpc.ClientConn
	md     schema.Metadata
}

// Close implements grpchelper.Client.
func (*client) Close() error {
	return nil
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
	var okRole bool
	for _, r := range node.Roles {
		for _, allowed := range p.allowedRoles {
			if r == allowed {
				okRole = true
				break
			}
		}
		if okRole {
			break
		}
	}
	if !okRole {
		return
	}
	p.connMgr.OnAddOrUpdate(node)
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
	p.connMgr.OnDelete(node)
}

func (p *pub) checkServiceHealth(svc string, conn *grpc.ClientConn) *common.Error {
	serviceClient := clusterv1.NewServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()
	resp, err := serviceClient.HealthCheck(ctx, &clusterv1.HealthCheckRequest{
		ServiceName: svc,
	})
	if err != nil {
		return common.NewErrorWithStatus(modelv1.Status_STATUS_INTERNAL_ERROR, err.Error())
	}
	if resp.Status == modelv1.Status_STATUS_SUCCEED {
		return nil
	}
	return common.NewErrorWithStatus(resp.Status, resp.Error)
}

func (p *pub) failover(node string, ce *common.Error, topic bus.Topic) {
	if ce.Status() != modelv1.Status_STATUS_INTERNAL_ERROR {
		_, _ = p.checkWritable(node, topic)
		return
	}
	p.connMgr.FailoverNode(node)
}

func (p *pub) checkWritable(n string, topic bus.Topic) (bool, *common.Error) {
	h, ok := p.handlers[topic]
	if !ok {
		return false, nil
	}
	c, ok := p.connMgr.GetClient(n)
	if !ok {
		return false, nil
	}
	topicStr := topic.String()
	err := p.checkServiceHealth(topicStr, c.conn)
	if err == nil {
		return true, nil
	}
	h.OnDelete(c.md)
	if !p.closer.AddRunning() {
		return false, err
	}
	// Deduplicate concurrent probes for the same node/topic.
	p.writableProbeMu.Lock()
	if _, ok := p.writableProbe[n]; !ok {
		p.writableProbe[n] = make(map[string]struct{})
	}
	if _, exists := p.writableProbe[n][topicStr]; exists {
		p.writableProbeMu.Unlock()
		return false, err
	}
	p.writableProbe[n][topicStr] = struct{}{}
	p.writableProbeMu.Unlock()

	go func(nodeName, t string) {
		defer p.closer.Done()
		defer func() {
			p.writableProbeMu.Lock()
			if topics, ok := p.writableProbe[nodeName]; ok {
				delete(topics, t)
				if len(topics) == 0 {
					delete(p.writableProbe, nodeName)
				}
			}
			p.writableProbeMu.Unlock()
		}()
		attempt := 0
		for {
			backoff := grpchelper.JitteredBackoff(grpchelper.InitBackoff, grpchelper.MaxBackoff, attempt, grpchelper.DefaultJitterFactor)
			select {
			case <-time.After(backoff):
				nodeCur, okCur := p.connMgr.GetClient(nodeName)
				if !okCur {
					return
				}
				errInternal := p.checkServiceHealth(t, nodeCur.conn)
				if errInternal == nil {
					// Record success for circuit breaker
					p.connMgr.RecordSuccess(nodeName)
					h.OnAddOrUpdate(nodeCur.md)
					return
				}
				p.log.Warn().Str("topic", t).Err(errInternal).Str("node", nodeName).Dur("backoff", backoff).Msg("data node can not ingest data")
			case <-p.closer.CloseNotify():
				return
			}
			attempt++
		}
	}(n, topicStr)
	return false, err
}
