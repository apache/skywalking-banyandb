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
	"sync"
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

type groupPropagation struct {
	latestTime     time.Time
	channel        chan *propertyv1.PropagationRequest
	originalNodeID string
}

type protocolHandler struct {
	propertyv1.UnimplementedGossipServiceServer
	s           *service
	groups      map[string]*groupPropagation
	groupNotify chan struct{}
	mu          sync.RWMutex
}

func newProtocolHandler(s *service) *protocolHandler {
	return &protocolHandler{
		s:           s,
		groups:      make(map[string]*groupPropagation),
		groupNotify: make(chan struct{}, 10),
	}
}

// nolint: contextcheck
func (q *protocolHandler) processPropagation() {
	for {
		select {
		case <-q.groupNotify:
			request := q.findUnProcessRequest()
			if request == nil {
				continue
			}
			err := q.handle(q.s.closer.Ctx(), request)
			if err != nil {
				q.s.log.Warn().Err(err).Stringer("request", request).
					Msgf("handle propagation request failure")
			}
		case <-q.s.closer.CloseNotify():
			return
		}
	}
}

func (q *protocolHandler) findUnProcessRequest() *propertyv1.PropagationRequest {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, g := range q.groups {
		select {
		case d := <-g.channel:
			return d
		default:
			continue
		}
	}
	return nil
}

func (q *protocolHandler) Propagation(_ context.Context, request *propertyv1.PropagationRequest) (resp *propertyv1.PropagationResponse, err error) {
	q.s.serverMetrics.totalReceived.Inc(1, request.Group)
	q.s.log.Debug().Stringer("request", request).Msg("received property repair gossip message for propagation")
	if q.addToProcess(request) {
		q.s.serverMetrics.totalAddProcessed.Inc(1, request.Group)
		q.s.log.Debug().Msgf("add the propagation request to the process")
	} else {
		q.s.serverMetrics.totalSkipProcess.Inc(1, request.Group)
		q.s.log.Debug().Msgf("propagation request discarded")
	}
	return &propertyv1.PropagationResponse{}, nil
}

func (q *protocolHandler) handle(ctx context.Context, request *propertyv1.PropagationRequest) (err error) {
	n := time.Now()
	now := n.UnixNano()
	nodes := request.Context.Nodes
	q.s.serverMetrics.totalStarted.Inc(1, request.Group)
	var needsKeepPropagation bool
	defer func() {
		if err != nil {
			q.s.serverMetrics.totalSendToNextErr.Inc(1, request.Group)
		}
		q.s.serverMetrics.totalFinished.Inc(1, request.Group)
		q.s.serverMetrics.totalLatency.Inc(n.Sub(time.Unix(0, now)).Seconds(), request.Group)
		if !needsKeepPropagation {
			q.s.serverMetrics.totalPropagationCount.Inc(float64(request.Context.CurrentPropagationCount),
				request.Group, request.Context.OriginNode)
			q.s.serverMetrics.totalPropagationPercent.Observe(
				float64(request.Context.CurrentPropagationCount)/float64(request.Context.MaxPropagationCount), request.Group)
		}
	}()
	if len(nodes) == 0 {
		return nil
	}
	listener := q.s.getListener()
	if listener == nil {
		return fmt.Errorf("no listener")
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
		if nextNodeConn != nil {
			_ = nextNodeConn.Close()
		}
		return fmt.Errorf("failed to execute gossip message for propagation in all nodes")
	}

	// if the propagation context has no more propagation count, we should not send the future callback
	request.Context.CurrentPropagationCount++
	if request.Context.CurrentPropagationCount >= request.Context.MaxPropagationCount {
		_ = nextNodeConn.Close()
		return nil
	}

	// propagate the message to the next node
	needsKeepPropagation = true
	q.s.serverMetrics.totalSendToNextStarted.Inc(1, request.Group)
	propagationStart := time.Now()
	q.s.log.Debug().Stringer("request", request).Str("nextNodeID", nextNodeID).
		Msg("propagating gossip message to next node")
	_, err = propertyv1.NewGossipServiceClient(nextNodeConn).Propagation(ctx, request)
	_ = nextNodeConn.Close()
	q.s.serverMetrics.totalSendToNextFinished.Inc(1, request.Group)
	q.s.serverMetrics.totalSendToNextLatency.Inc(time.Since(propagationStart).Seconds(), request.Group)
	if err != nil {
		q.s.serverMetrics.totalSendToNextErr.Inc(1, request.Group)
		return fmt.Errorf("failed to propagate gossip message to next node: %w", err)
	}

	return nil
}

func (q *protocolHandler) addToProcess(request *propertyv1.PropagationRequest) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	group, exist := q.groups[request.Group]
	if !exist {
		group = &groupPropagation{
			channel:        make(chan *propertyv1.PropagationRequest, 1),
			originalNodeID: request.Context.OriginNode,
			latestTime:     time.Now(),
		}
		group.channel <- request
		q.groups[request.Group] = group
		q.notifyNewRequest()
		return true
	}

	// if the latest round is out of ttl, then needs to change to current node to executing
	if time.Since(group.latestTime) > q.s.scheduleInterval/2 {
		group.originalNodeID = request.Context.OriginNode
		select {
		case group.channel <- request:
			q.notifyNewRequest()
		default:
			q.s.log.Error().Msgf("ready to added propagation into group %s in a new round, but it's full", request.Group)
		}
		return true
	}

	// if the original node ID are a same node, means which from the same round
	if group.originalNodeID == request.Context.OriginNode {
		select {
		case group.channel <- request:
			q.notifyNewRequest()
		default:
			q.s.log.Error().Msgf("ready to added propagation into group %s in a same round, but it's full", request.Group)
		}
		return true
	}

	// otherwise, it should ignore
	return false
}

func (q *protocolHandler) notifyNewRequest() {
	select {
	case q.groupNotify <- struct{}{}:
	default:
		q.s.log.Warn().Msgf("notify a new request to gossip failure, the queue is full")
	}
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
	node, exist := q.s.getRegisteredNode(nodes[curIndex])
	if exist {
		return nodes[curIndex], node, true
	}
	return "", nil, false
}

type serverMetrics struct {
	totalReceived     meter.Counter
	totalAddProcessed meter.Counter
	totalSkipProcess  meter.Counter

	totalStarted  meter.Counter
	totalFinished meter.Counter
	totalErr      meter.Counter
	totalLatency  meter.Counter

	totalPropagationCount   meter.Counter
	totalPropagationPercent meter.Histogram

	totalSendToNextStarted  meter.Counter
	totalSendToNextFinished meter.Counter
	totalSendToNextErr      meter.Counter
	totalSendToNextLatency  meter.Counter
}

func newServerMetrics(factory *observability.Factory) *serverMetrics {
	return &serverMetrics{
		totalReceived:     factory.NewCounter("total_received", "group"),
		totalAddProcessed: factory.NewCounter("total_add_processed", "group"),
		totalSkipProcess:  factory.NewCounter("total_skip_process", "group"),

		totalStarted:  factory.NewCounter("total_started", "group"),
		totalFinished: factory.NewCounter("total_finished", "group"),
		totalErr:      factory.NewCounter("total_err", "group"),
		totalLatency:  factory.NewCounter("total_latency", "group"),

		totalPropagationCount: factory.NewCounter("total_propagation_count", "group", "original_node"),
		totalPropagationPercent: factory.NewHistogram("total_propagation_percent",
			meter.Buckets{0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1}, "group"),

		totalSendToNextStarted:  factory.NewCounter("total_send_next_started", "group"),
		totalSendToNextFinished: factory.NewCounter("total_send_next_finished", "group"),
		totalSendToNextErr:      factory.NewCounter("total_send_next_err", "group"),
		totalSendToNextLatency:  factory.NewCounter("total_send_next_latency", "group"),
	}
}
