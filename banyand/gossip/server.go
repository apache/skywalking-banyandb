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
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/apache/skywalking-banyandb/api/data"
	gossipv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/gossip/v1"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/meter"
)

func (s *service) Subscribe(topic string, listener MessageListener) error {
	s.listenersLock.Lock()
	defer s.listenersLock.Unlock()
	if _, ok := s.listeners[topic]; !ok {
		s.listeners[topic] = make([]MessageListener, 0)
	}
	s.listeners[topic] = append(s.listeners[topic], listener)
	return nil
}

func (s *service) getListener(topic string) MessageListener {
	s.listenersLock.RLock()
	defer s.listenersLock.RUnlock()
	listeners, ok := s.listeners[topic]
	if !ok || len(listeners) == 0 {
		return nil
	}
	return listeners[0]
}

type propagationServer struct {
	*bus.UnImplementedHealthyListener
	s *service
}

func (q *propagationServer) Rev(ctx context.Context, message bus.Message) bus.Message {
	n := time.Now()
	now := n.UnixNano()
	request := message.Data().(*gossipv1.PropagationMessageRequest)
	nodes := request.Context.Nodes
	q.s.serverMetrics.totalStarted.Inc(1, request.Topic)
	q.s.log.Debug().Stringer("request", request).Msg("received gossip message for propagation")
	defer func() {
		q.s.serverMetrics.totalFinished.Inc(1, request.Topic)
		q.s.serverMetrics.totalLatency.Inc(n.Sub(time.Unix(0, now)).Seconds(), request.Topic)
	}()
	if len(nodes) == 0 {
		return q.replyError(ctx, now, request, nil, "no nodes available for gossip message propagation")
	}
	nextNodeID, nextNodeConn, err := q.nextNodeConnection(nodes)
	if err != nil {
		return q.replyError(ctx, now, request, err, "failed to get next node connection for gossip message propagation")
	}
	if nextNodeConn == nil {
		return q.replyError(ctx, now, request, nil, "no valid connection found for gossip message propagation")
	}
	listener := q.s.getListener(request.Topic)
	if listener == nil {
		return q.replyError(ctx, now, request, nil, "no listener found for gossip message propagation")
	}
	var msg bus.Message
	if reqSupplier, ok := TopicMessageRequestMap[request.Topic]; ok {
		// If the topic is registered, we can use the request supplier to create a new request.
		req := reqSupplier()
		if errUnmarshal := proto.Unmarshal(request.Body, req); errUnmarshal != nil {
			return q.replyError(ctx, now, request, errUnmarshal, "failed to unmarshal gossip message propagation")
		}
		msg = bus.NewMessage(bus.MessageID(now), req)
	} else {
		return q.replyError(ctx, now, request, nil, "no request supplier for gossip message propagation")
	}
	q.s.serverMetrics.totalStarted.Inc(1, request.Topic)
	// process the message using the listener
	if err = listener.Rev(ctx, nextNodeConn, msg); err != nil {
		q.s.serverMetrics.totalErr.Inc(1, request.Topic)
		return q.replyError(ctx, now, request, err, "failed to process gossip message propagation")
	}

	request.Context.MaxPropagationCount--
	if request.Context.MaxPropagationCount <= 0 {
		return q.replySuccess(ctx, now, request, false)
	}

	// propagate the message to the next node
	q.s.serverMetrics.totalPropagationStarted.Inc(1, request.Topic)
	propagationStart := time.Now()
	q.s.log.Debug().Stringer("request", request).Str("nextNodeID", nextNodeID).
		Msg("propagating gossip message to next node")
	publish, err := q.s.remoteClient.Publish(ctx, data.TopicGossipPropagation, bus.NewMessageWithNode(bus.MessageID(now), nextNodeID,
		request))
	if err != nil {
		q.s.serverMetrics.totalPropagationFinished.Inc(1, request.Topic)
		q.s.serverMetrics.totalPropagationErr.Inc(1, request.Topic)
		q.s.serverMetrics.totalPropagationLatency.Inc(time.Since(propagationStart).Seconds(), request.Topic)
		return q.replyError(ctx, now, request, err, "failed to propagate gossip message")
	}
	go func(nodeID string, future bus.Future) {
		resp, err := future.Get()
		q.s.serverMetrics.totalPropagationFinished.Inc(1, request.Topic)
		q.s.serverMetrics.totalPropagationLatency.Inc(time.Since(propagationStart).Seconds(), request.Topic)
		if err != nil {
			q.s.serverMetrics.totalPropagationErr.Inc(1, request.Topic)
			q.s.log.Error().Err(err).
				Stringer("request", request).
				Str("nodeID", nodeID).
				Msg("failed to send gossip propagation message")
			return
		}
		respData := resp.Data().(*gossipv1.PropagationMessageResponse)
		if !respData.Success {
			q.s.serverMetrics.totalPropagationErr.Inc(1, request.Topic)
			q.s.log.Error().
				Stringer("request", request).
				Str("nodeID", nodeID).
				Msgf("gossip propagation message failed: %s", respData.Error)
			return
		}
	}(nextNodeID, publish)

	return q.replySuccess(ctx, now, request, true)
}

func (q *propagationServer) nextNodeConnection(nodes []string) (string, *grpc.ClientConn, error) {
	if len(nodes) == 0 {
		return "", nil, errors.New("no nodes available for gossip message propagation")
	}
	var nodeID string
	var node queue.Node
	var exist bool
	for i := 0; i < len(nodes); i++ {
		if nodes[i] == q.s.nodeID {
			nodeID, node, exist = q.nextExistNode(nodes, i)
		}
	}
	if !exist {
		return "", nil, errors.New("no valid node found for gossip message propagation")
	}
	original := node.Original()
	if original == nil {
		return "", nil, fmt.Errorf("no original node found for gossip message propagation")
	}
	conn, ok := original.(*grpc.ClientConn)
	if !ok {
		return "", nil, fmt.Errorf("expected grpc.ClientConn, got %T", original)
	}
	return nodeID, conn, nil
}

func (q *propagationServer) nextExistNode(nodes []string, curIndex int) (string, queue.Node, bool) {
	curIndex = (curIndex + 1) % len(nodes)
	node, exist := q.s.remoteClient.ClusterNode(nodes[curIndex])
	if exist {
		return nodes[curIndex], node, true
	}
	return "", nil, false
}

type futureCallbackServer struct {
	*bus.UnImplementedHealthyListener
	s *service
}

func (c *futureCallbackServer) Rev(_ context.Context, message bus.Message) bus.Message {
	t := time.Now()
	now := t.UnixNano()
	req := message.Data().(*gossipv1.FutureCallbackMessageRequest)
	c.s.serverMetrics.totalFutureCallbackRevStarted.Inc(1, req.Topic)
	c.s.log.Debug().Stringer("request", req).Msg("received gossip message future callback")
	defer func() {
		c.s.serverMetrics.totalFutureCallbackRevLatency.Inc(t.Sub(time.Unix(0, now)).Seconds(), req.Topic)
		c.s.serverMetrics.totalFutureCallbackRevFinished.Inc(1, req.Topic)
	}()
	wait := c.s.getFromWait(bus.MessageID(req.MessageId))
	if wait == nil {
		c.s.log.Error().Stringer("request", req).Msg("no future found for gossip message callback")
		c.s.serverMetrics.totalFutureCallbackRevErr.Inc(1, req.Topic)
		return bus.NewMessage(bus.MessageID(now), &gossipv1.FutureCallbackMessageResponse{})
	}
	wait.saveRemoteResponse(bus.NewMessage(bus.MessageID(now), req.Response))
	c.s.removeFromWait(wait)
	return bus.NewMessage(bus.MessageID(now), &gossipv1.FutureCallbackMessageResponse{})
}

func (q *propagationServer) replyError(ctx context.Context, now int64, req *gossipv1.PropagationMessageRequest, err error, msg string) bus.Message {
	q.s.log.Error().Err(err).Stringer("request", req).Msg(msg)
	q.s.serverMetrics.totalErr.Inc(1, req.Topic)
	if msg == "" {
		msg = err.Error()
	}
	// send the future callback to the original node
	resp := &gossipv1.PropagationMessageResponse{
		Success: false,
		Error:   msg,
	}
	go q.sendFutureCallback(ctx, req, resp)
	return bus.NewMessage(bus.MessageID(now), resp)
}

func (q *propagationServer) replySuccess(ctx context.Context, now int64, req *gossipv1.PropagationMessageRequest, stillSend bool) bus.Message {
	q.s.log.Debug().Stringer("request", req).Msg("gossip message propagation succeeded")
	resp := &gossipv1.PropagationMessageResponse{
		Success: true,
	}
	// if the propagation context has no more propagation count, we should not send the future callback
	if !stillSend {
		go q.sendFutureCallback(ctx, req, resp)
	}
	return bus.NewMessage(bus.MessageID(now), resp)
}

func (q *propagationServer) sendFutureCallback(ctx context.Context, req *gossipv1.PropagationMessageRequest, resp *gossipv1.PropagationMessageResponse) {
	now := time.Now()
	q.s.serverMetrics.totalFutureCallbackSendStarted.Inc(1, req.Topic)
	var err error
	defer func() {
		q.s.serverMetrics.totalFutureCallbackSendLatency.Inc(time.Since(now).Seconds(), req.Topic)
		q.s.serverMetrics.totalFutureCallbackSendFinished.Inc(1, req.Topic)
		if err != nil {
			q.s.serverMetrics.totalFutureCallbackSendErr.Inc(1, req.Topic)
		}
	}()
	node := req.Context.OriginNode
	var publishFuture bus.Future
	callbackReq := &gossipv1.FutureCallbackMessageRequest{
		Topic:     req.Topic,
		MessageId: req.Context.OriginMessageId,
		Response:  resp,
	}
	publishFuture, err = q.s.remoteClient.Publish(ctx, data.TopicGossipFutureCallback, bus.NewMessageWithNode(
		bus.MessageID(now.UnixNano()), node, callbackReq))
	if err != nil {
		q.s.log.Error().Err(err).Stringer("request", callbackReq).Msg("failed to send future callback for gossip message propagation")
		return
	}
	_, err = publishFuture.Get()
	if err != nil {
		q.s.log.Error().Err(err).Stringer("request", callbackReq).Msg("failed to get future callback response for gossip message propagation")
		return
	}
}

type serverMetrics struct {
	totalStarted  meter.Counter
	totalFinished meter.Counter
	totalErr      meter.Counter
	totalLatency  meter.Counter

	totalPropagationStarted  meter.Counter
	totalPropagationFinished meter.Counter
	totalPropagationErr      meter.Counter
	totalPropagationLatency  meter.Counter

	totalFutureCallbackRevStarted  meter.Counter
	totalFutureCallbackRevFinished meter.Counter
	totalFutureCallbackRevErr      meter.Counter
	totalFutureCallbackRevLatency  meter.Counter

	totalFutureCallbackSendStarted  meter.Counter
	totalFutureCallbackSendFinished meter.Counter
	totalFutureCallbackSendErr      meter.Counter
	totalFutureCallbackSendLatency  meter.Counter
}

func newServerMetrics(factory *observability.Factory) *serverMetrics {
	return &serverMetrics{
		totalStarted:  factory.NewCounter("total_started", "topic"),
		totalFinished: factory.NewCounter("total_finished", "topic"),
		totalErr:      factory.NewCounter("total_err", "topic"),
		totalLatency:  factory.NewCounter("total_latency", "topic"),

		totalPropagationStarted:  factory.NewCounter("total_msg_received", "topic"),
		totalPropagationFinished: factory.NewCounter("total_msg_received_err", "topic"),
		totalPropagationErr:      factory.NewCounter("total_msg_sent", "topic"),
		totalPropagationLatency:  factory.NewCounter("total_msg_sent_err", "topic"),

		totalFutureCallbackRevStarted:  factory.NewCounter("total_future_callback_rev_started", "topic"),
		totalFutureCallbackRevFinished: factory.NewCounter("total_future_callback_rev_finished", "topic"),
		totalFutureCallbackRevErr:      factory.NewCounter("total_future_callback_rev_err", "topic"),
		totalFutureCallbackRevLatency:  factory.NewCounter("total_future_callback_rev_latency", "topic"),

		totalFutureCallbackSendStarted:  factory.NewCounter("total_future_callback_send_started", "topic"),
		totalFutureCallbackSendFinished: factory.NewCounter("total_future_callback_send_finished", "topic"),
		totalFutureCallbackSendErr:      factory.NewCounter("total_future_callback_send_err", "topic"),
		totalFutureCallbackSendLatency:  factory.NewCounter("total_future_callback_send_latency", "topic"),
	}
}
