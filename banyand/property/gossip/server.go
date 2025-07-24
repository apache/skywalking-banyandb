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

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/meter"
)

var (
	serviceName = propertyv1.GossipService_ServiceDesc.ServiceName

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

func (s *service) Subscribe(listener MessageListener) error {
	s.listenersLock.Lock()
	defer s.listenersLock.Unlock()
	s.listeners = append(s.listeners, listener)
	return nil
}

func (s *service) getListener() MessageListener {
	s.listenersLock.RLock()
	defer s.listenersLock.RUnlock()
	if len(s.listeners) == 0 {
		return nil
	}
	return s.listeners[0]
}

type protocolHandler struct {
	propertyv1.UnimplementedGossipServiceServer
	s *service
}

func (q *protocolHandler) Propagation(ctx context.Context, request *propertyv1.PropagationRequest) (resp *propertyv1.PropagationResponse, err error) {
	n := time.Now()
	now := n.UnixNano()
	nodes := request.Context.Nodes
	q.s.serverMetrics.totalStarted.Inc(1, request.Group)
	q.s.log.Debug().Stringer("request", request).Msg("received property repair gossip message for propagation")
	defer func() {
		if !resp.Success {
			q.s.serverMetrics.totalPropagationErr.Inc(1, request.Group)
		}
		q.s.serverMetrics.totalFinished.Inc(1, request.Group)
		q.s.serverMetrics.totalLatency.Inc(n.Sub(time.Unix(0, now)).Seconds(), request.Group)
	}()
	if len(nodes) == 0 {
		return &propertyv1.PropagationResponse{Success: false, Error: "no nodes needs to propagation"}, nil
	}
	listener := q.s.getListener()
	if listener == nil {
		return &propertyv1.PropagationResponse{Success: false, Error: "no listener found for gossip message propagation"}, nil
	}
	var nextNodeID string
	var nextNodeConn *grpc.ClientConn
	var executeSuccess bool
	for offset := 1; offset < len(nodes); offset++ {
		nextNodeID, nextNodeConn, err = q.nextNodeConnection(nodes, offset)
		if err != nil {
			q.s.log.Warn().Err(err).Msgf("failed to connect to next node with offset: %d", offset)
			continue
		}

		// process the message using the listener
		err = listener.Rev(ctx, nextNodeConn, request)
		if err != nil {
			q.s.log.Warn().Err(err).Msgf("failed to process with next node: %s", nextNodeID)
			_ = nextNodeConn.Close()
			continue
		}

		executeSuccess = true
		break
	}

	// if no node is available or sync error for gossip message propagation, then it should be notified finished
	if !executeSuccess {
		_ = nextNodeConn.Close()
		return q.replyResultWithOriginalNode(request, &propertyv1.PropagationResponse{
			Success: false,
			Error:   "failed to execute gossip message for propagation in all nodes",
		}), nil
	}

	// if the propagation context has no more propagation count, we should not send the future callback
	request.Context.MaxPropagationCount--
	if request.Context.MaxPropagationCount <= 0 {
		_ = nextNodeConn.Close()
		return q.replyResultWithOriginalNode(request, &propertyv1.PropagationResponse{Success: true}), nil
	}

	// propagate the message to the next node
	go func() {
		// using the new context to avoid blocking the original context
		ctx = context.Background()
		q.s.serverMetrics.totalPropagationStarted.Inc(1, request.Group)
		propagationStart := time.Now()
		q.s.log.Debug().Stringer("request", request).Str("nextNodeID", nextNodeID).
			Msg("propagating gossip message to next node")
		propagation, err := propertyv1.NewGossipServiceClient(nextNodeConn).Propagation(ctx, request)
		_ = nextNodeConn.Close()
		q.s.serverMetrics.totalPropagationFinished.Inc(1, request.Group)
		q.s.serverMetrics.totalPropagationLatency.Inc(time.Since(propagationStart).Seconds(), request.Group)
		if err != nil {
			q.s.serverMetrics.totalPropagationErr.Inc(1, request.Group)
			return
		}
		if !propagation.Success {
			q.s.serverMetrics.totalPropagationErr.Inc(1, request.Group)
			q.s.log.Error().
				Stringer("request", request).
				Str("nodeID", nextNodeID).
				Msgf("gossip propagation message failed: %s", propagation.Error)
			return
		}
	}()

	return &propertyv1.PropagationResponse{Success: true}, nil
}

// nolint: contextcheck
func (q *protocolHandler) replyResultWithOriginalNode(
	req *propertyv1.PropagationRequest,
	resp *propertyv1.PropagationResponse,
) *propertyv1.PropagationResponse {
	// async to notify the original node that the propagation is finished
	go q.sendFutureCallback(context.Background(), req, resp)
	return resp
}

func (q *protocolHandler) FutureCallback(_ context.Context, req *propertyv1.FutureCallbackRequest) (*propertyv1.FutureCallbackResponse, error) {
	t := time.Now()
	now := t.UnixNano()
	q.s.serverMetrics.totalFutureCallbackRevStarted.Inc(1, req.Group)
	q.s.log.Debug().Stringer("request", req).Msg("received gossip message future callback")
	defer func() {
		q.s.serverMetrics.totalFutureCallbackRevLatency.Inc(t.Sub(time.Unix(0, now)).Seconds(), req.Group)
		q.s.serverMetrics.totalFutureCallbackRevFinished.Inc(1, req.Group)
	}()
	wait := q.s.getFromWait(req.MessageId)
	if wait == nil {
		q.s.log.Error().Stringer("request", req).Msg("no future found for gossip message callback")
		q.s.serverMetrics.totalFutureCallbackRevErr.Inc(1, req.Group)
		return &propertyv1.FutureCallbackResponse{}, nil
	}
	wait.saveRemoteResponse(req.Response)
	q.s.removeFromWait(wait)
	return &propertyv1.FutureCallbackResponse{}, nil
}

func (q *protocolHandler) nextNodeConnection(nodes []string, offset int) (string, *grpc.ClientConn, error) {
	if len(nodes) == 0 {
		return "", nil, errors.New("no nodes available for gossip message propagation")
	}
	var nodeID string
	var node *databasev1.Node
	var exist bool
	for i := 0; i < len(nodes); i++ {
		if nodes[i] == q.s.nodeID {
			nodeID, node, exist = q.nextExistNode(nodes, i, offset)
			break
		}
	}
	if !exist {
		return "", nil, errors.New("no valid node found for gossip message propagation")
	}
	conn, err := q.s.newConnectionFromNode(node)
	if err != nil {
		return "", nil, err
	}
	return nodeID, conn, nil
}

func (s *service) newConnectionFromNode(n *databasev1.Node) (*grpc.ClientConn, error) {
	credOpts, err := s.getClientTransportCredentials()
	if err != nil {
		return nil, fmt.Errorf("failed to get client transport credentials: %w", err)
	}
	conn, err := grpc.NewClient(n.PropertyRepairGossipGrpcAddress, append(credOpts, grpc.WithDefaultServiceConfig(retryPolicy))...)
	if err != nil {
		return nil, fmt.Errorf("failed to create gRPC client connection to node %s: %w", n.PropertyRepairGossipGrpcAddress, err)
	}
	return conn, nil
}

func (s *service) getClientTransportCredentials() ([]grpc.DialOption, error) {
	opts, err := grpchelper.SecureOptions(nil, s.tls, false, s.caCertPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load TLS config: %w", err)
	}
	return opts, nil
}

func (q *protocolHandler) nextExistNode(nodes []string, curIndex, offset int) (string, *databasev1.Node, bool) {
	curIndex = (curIndex + offset) % len(nodes)
	node, exist := q.s.registered[nodes[curIndex]]
	if exist {
		return nodes[curIndex], node, true
	}
	return "", nil, false
}

func (q *protocolHandler) sendFutureCallback(ctx context.Context, req *propertyv1.PropagationRequest, resp *propertyv1.PropagationResponse) {
	now := time.Now()
	q.s.serverMetrics.totalFutureCallbackSendStarted.Inc(1, req.Group)
	var err error
	var conn *grpc.ClientConn
	defer func() {
		q.s.serverMetrics.totalFutureCallbackSendLatency.Inc(time.Since(now).Seconds(), req.Group)
		q.s.serverMetrics.totalFutureCallbackSendFinished.Inc(1, req.Group)
		if err != nil {
			q.s.serverMetrics.totalFutureCallbackSendErr.Inc(1, req.Group)
		}
		if conn != nil {
			_ = conn.Close()
		}
	}()
	node := req.Context.OriginNode
	callbackReq := &propertyv1.FutureCallbackRequest{
		MessageId: req.Context.OriginMessageId,
		Response:  resp,
	}
	originNode, exist := q.s.registered[node]
	if !exist {
		err = errors.New("origin node not found in gossip registered nodes")
		q.s.log.Error().Err(err).Stringer("request", callbackReq).Msg("failed to send future callback for gossip message propagation")
		return
	}
	conn, err = q.s.newConnectionFromNode(originNode)
	if err != nil {
		q.s.log.Error().Err(err).Stringer("request", callbackReq).Msg("failed to create gRPC client connection for gossip message propagation")
		return
	}
	client := propertyv1.NewGossipServiceClient(conn)
	_, err = client.FutureCallback(ctx, callbackReq)
	if err != nil {
		q.s.log.Error().Err(err).Stringer("request", callbackReq).Msg("failed to send future callback for gossip message propagation")
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
		totalStarted:  factory.NewCounter("total_started", "group"),
		totalFinished: factory.NewCounter("total_finished", "group"),
		totalErr:      factory.NewCounter("total_err", "group"),
		totalLatency:  factory.NewCounter("total_latency", "group"),

		totalPropagationStarted:  factory.NewCounter("total_msg_received", "group"),
		totalPropagationFinished: factory.NewCounter("total_msg_received_err", "group"),
		totalPropagationErr:      factory.NewCounter("total_msg_sent", "group"),
		totalPropagationLatency:  factory.NewCounter("total_msg_sent_err", "group"),

		totalFutureCallbackRevStarted:  factory.NewCounter("total_future_callback_rev_started", "group"),
		totalFutureCallbackRevFinished: factory.NewCounter("total_future_callback_rev_finished", "group"),
		totalFutureCallbackRevErr:      factory.NewCounter("total_future_callback_rev_err", "group"),
		totalFutureCallbackRevLatency:  factory.NewCounter("total_future_callback_rev_latency", "group"),

		totalFutureCallbackSendStarted:  factory.NewCounter("total_future_callback_send_started", "group"),
		totalFutureCallbackSendFinished: factory.NewCounter("total_future_callback_send_finished", "group"),
		totalFutureCallbackSendErr:      factory.NewCounter("total_future_callback_send_err", "group"),
		totalFutureCallbackSendLatency:  factory.NewCounter("total_future_callback_send_latency", "group"),
	}
}
