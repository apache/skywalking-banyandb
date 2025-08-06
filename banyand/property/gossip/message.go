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

package gossip

import (
	"context"

	"github.com/pkg/errors"
	"google.golang.org/grpc"

	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

// ErrAbortPropagation is an error that indicates the gossip propagation of a message should be aborted.
var ErrAbortPropagation = errors.New("abort propagation")

// MessageListener is an interface that defines a method to handle the incoming propagation message.
type MessageListener interface {
	Rev(ctx context.Context, nextNode *grpc.ClientConn, request *propertyv1.PropagationRequest) error
}

// Messenger is an interface that defines methods for message propagation and subscription in a gossip protocol.
type Messenger interface {
	MessageClient
	MessageServer
	run.PreRunner
	run.Config

	// Serve starts the service from parent stop channel.
	Serve(stopCh chan struct{})
	// GracefulStop shuts down and cleans up the service.
	GracefulStop()
}

// MessageClient is an interface that defines methods for propagating messages to other nodes in a gossip protocol.
type MessageClient interface {
	run.Unit
	// Propagation using anti-entropy gossip protocol to propagate messages to the specified nodes.
	Propagation(nodes []string, group string, shardID uint32) error
	// LocateNodes finds nodes in the specified group and shard ID, returning a list of node addresses.
	LocateNodes(group string, shardID, replicasCount uint32) ([]string, error)
}

// MessageServer is an interface that defines methods for subscribing to topics and receiving messages in a gossip protocol.
type MessageServer interface {
	run.Unit
	// Subscribe allows subscribing to a topic to receive messages.
	Subscribe(listener MessageListener)
	// RegisterServices registers the gRPC services with the provided server.
	RegisterServices(func(r *grpc.Server))
	// GetServerPort returns the port number of the server.
	GetServerPort() *uint32
}
