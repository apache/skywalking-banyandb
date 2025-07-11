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
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/apache/skywalking-banyandb/api/data"
	gossipv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/gossip/v1"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bus"
)

func (s *service) Propagation(nodes []string, topic string, message bus.Message) (Future, error) {
	if len(nodes) < 2 {
		return nil, fmt.Errorf("must provide at least 2 node")
	}

	// building propagation context
	ctx := &gossipv1.PropagationContext{
		Nodes:           nodes,
		OriginNode:      s.nodeID,
		OriginMessageId: uint64(message.ID()),
	}
	if len(nodes) == 2 {
		ctx.MaxPropagationCount = 1
	} else {
		// two rounds of all nodes except the lasted node
		// such when there have three nodes A, B, C,
		// the propagation will be A -> B, B -> C, C -> A, A -> B
		ctx.MaxPropagationCount = int32(len(nodes)*2 - 2)
	}

	// building propagation message request
	d, ok := message.Data().(proto.Message)
	if !ok {
		return nil, fmt.Errorf("invalid message type %T", message.Data())
	}
	payload, err := proto.Marshal(d)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal message %T: %w", d, err)
	}
	request := &gossipv1.PropagationMessageRequest{
		Topic:     topic,
		MessageId: uint64(message.ID()),
		Context:   ctx,
		Body:      payload,
	}

	totalTimeout := time.Second * 15 * time.Duration(ctx.MaxPropagationCount)

	var sendMsg bus.Message
	var sendTo queue.Client
	if nodes[0] == s.nodeID {
		sendMsg = bus.NewMessage(message.ID(), request)
		sendTo = s.local
	} else {
		sendMsg = bus.NewMessageWithNode(message.ID(), nodes[0], request)
		sendTo = s.remoteClient
	}

	return s.handleSending(sendTo, sendMsg, ctx.MaxPropagationCount > 1, totalTimeout)
}

func (s *service) handleSending(sendTo queue.Client, msg bus.Message, hasRemote bool, timeout time.Duration) (Future, error) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), timeout)
	defer cancelFunc()
	publish, err := sendTo.Publish(ctx, data.TopicGossipPropagation, msg)
	if err != nil {
		return nil, fmt.Errorf("failed to publish gossip message: %w", err)
	}

	f := newFuture(s, msg, publish, hasRemote, timeout)
	// only add to wait list if there is a remote node to propagate
	if hasRemote {
		s.addToWait(f)
	}
	return f, nil
}
