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

	// perNodeSyncTimeout is the timeout for each node to sync the property data.
	perNodeSyncTimeout = time.Hour * 1
)

func (s *service) Subscribe(listener MessageListener) {
	s.listenersLock.Lock()
	defer s.listenersLock.Unlock()
	s.listeners = append(s.listeners, listener)
}

func (s *service) RegisterServices(f func(r *grpc.Server)) {
	s.serviceRegisterLock.Lock()
	defer s.serviceRegisterLock.Unlock()
	s.serviceRegister = append(s.serviceRegister, f)
}

func (s *service) getListener() MessageListener {
	s.listenersLock.RLock()
	defer s.listenersLock.RUnlock()
	if len(s.listeners) == 0 {
		return nil
	}
	return s.listeners[0]
}

func (s *service) getServiceRegisters() []func(server *grpc.Server) {
	s.serviceRegisterLock.RLock()
	defer s.serviceRegisterLock.RUnlock()
	if len(s.serviceRegister) == 0 {
		return nil
	}
	return s.serviceRegister
}

type groupWithShardPropagation struct {
	latestTime     time.Time
	channel        chan *handlingRequest
	originalNodeID string
}

type handlingRequest struct {
	*propertyv1.PropagationRequest
	tracer     Trace
	parentSpan Span
}

type protocolHandler struct {
	propertyv1.UnimplementedGossipServiceServer
	s               *service
	groupWithShards map[string]*groupWithShardPropagation
	groupNotify     chan struct{}
	mu              sync.RWMutex
}

func newProtocolHandler(s *service) *protocolHandler {
	return &protocolHandler{
		s:               s,
		groupWithShards: make(map[string]*groupWithShardPropagation),
		groupNotify:     make(chan struct{}, 10),
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
			timeoutCtx, cancelFunc := context.WithTimeout(q.s.closer.Ctx(), perNodeSyncTimeout)
			err := q.handle(timeoutCtx, request)
			cancelFunc()
			if err != nil {
				q.s.log.Warn().Err(err).Stringer("request", request.PropagationRequest).
					Msgf("handle propagation request failure")
			}
		case <-q.s.closer.CloseNotify():
			return
		}
	}
}

func (q *protocolHandler) findUnProcessRequest() *handlingRequest {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, g := range q.groupWithShards {
		select {
		case d := <-g.channel:
			return d
		default:
			continue
		}
	}
	return nil
}

// nolint: contextcheck
func (q *protocolHandler) Propagation(_ context.Context, request *propertyv1.PropagationRequest) (resp *propertyv1.PropagationResponse, err error) {
	tracer := q.s.createTraceForRequest(request)
	return q.propagation0(context.TODO(), request, tracer)
}

func (q *protocolHandler) propagation0(_ context.Context, request *propertyv1.PropagationRequest, tracer Trace) (resp *propertyv1.PropagationResponse, err error) {
	span := tracer.CreateSpan(tracer.ActivateSpan(), "receive gossip message")
	defer span.End()
	span.Tag(TraceTagGroupName, request.Group)
	span.Tag(TraceTagShardID, fmt.Sprintf("%d", request.ShardId))
	span.Tag(TraceTagOperateType, TraceTagOperateReceive)
	q.s.serverMetrics.totalReceived.Inc(1, request.Group)
	q.s.log.Debug().Stringer("request", request).Msg("received property repair gossip message for propagation")

	if q.addToProcess(request, tracer) {
		span.Tag("added_to_process", "true")
		q.s.serverMetrics.totalAddProcessed.Inc(1, request.Group)
		q.s.log.Debug().Msgf("add the propagation request to the process")
	} else {
		span.Tag("added_to_process", "false")
		q.s.serverMetrics.totalSkipProcess.Inc(1, request.Group)
		q.s.log.Debug().Msgf("propagation request discarded")
	}
	return &propertyv1.PropagationResponse{}, nil
}

func (q *protocolHandler) handle(ctx context.Context, request *handlingRequest) (err error) {
	handlingSpan := request.tracer.CreateSpan(request.parentSpan, "handling gossip message")
	handlingSpan.Tag(TraceTagOperateType, TraceTagOperateHandle)
	n := time.Now()
	now := n.UnixNano()
	nodes := request.Context.Nodes
	q.s.serverMetrics.totalStarted.Inc(1, request.Group)
	q.s.log.Debug().Stringer("request", request).Msgf("handling gossip message for propagation")
	var needsKeepPropagation bool
	defer func() {
		if err != nil {
			q.s.serverMetrics.totalSendToNextErr.Inc(1, request.Group)
			handlingSpan.Error(err.Error())
			q.s.log.Warn().Err(err).Stringer("request", request).
				Msgf("failed to handle gossip message for propagation")
		}
		q.s.serverMetrics.totalFinished.Inc(1, request.Group)
		q.s.serverMetrics.totalLatency.Inc(n.Sub(time.Unix(0, now)).Seconds(), request.Group)
		if !needsKeepPropagation {
			q.s.log.Info().Str("group", request.Group).Uint32("shardNum", request.ShardId).
				Msgf("propagation message for propagation is finished")
			q.s.serverMetrics.totalPropagationCount.Inc(1,
				request.Group, request.Context.OriginNode)
			q.s.serverMetrics.totalPropagationPercent.Observe(
				float64(request.Context.CurrentPropagationCount)/float64(request.Context.MaxPropagationCount), request.Group)
		}
		handlingSpan.End()
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
		selectNodeSpan := request.tracer.CreateSpan(handlingSpan, "selecting next node")
		selectNodeSpan.Tag(TraceTagOperateType, TraceTagOperateSelectNode)
		nextNodeID, nextNodeConn, err = q.nextNodeConnection(nodes, offset, selectNodeSpan)
		if err != nil {
			selectNodeSpan.Error(err.Error())
			selectNodeSpan.End()
			q.s.log.Warn().Err(err).Msgf("failed to connect to next node with offset: %d", offset)
			continue
		}
		selectNodeSpan.Tag(TraceTagTargetNode, nextNodeID)
		selectNodeSpan.End()

		// process the message using the listener
		listenerProcessSpan := request.tracer.CreateSpan(handlingSpan, "listener processing sync")
		listenerProcessSpan.Tag(TraceTagOperateType, TraceTagOperateListenerReceive)
		err = listener.Rev(ctx, request.tracer, nextNodeConn, request.PropagationRequest)
		if err != nil {
			listenerProcessSpan.Error(err.Error())
			listenerProcessSpan.End()
			if errors.Is(err, ErrAbortPropagation) {
				q.s.log.Warn().Err(err).Msgf("propagation aborted by listener for node: %s", nextNodeID)
				_ = nextNodeConn.Close()
				return nil // Abort propagation, no need to continue
			}
			q.s.log.Warn().Err(err).Msgf("failed to process with next node: %s", nextNodeID)
			_ = nextNodeConn.Close()
			continue
		}
		listenerProcessSpan.End()

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

	if q.contextIsDone(ctx) {
		if nextNodeConn != nil {
			_ = nextNodeConn.Close()
		}
		q.s.log.Debug().Msgf("context is done, no need to propagate further")
		return nil
	}

	// propagate the message to the next node
	toNextSpan := request.tracer.CreateSpan(handlingSpan, "propagation to next node")
	toNextSpan.Tag(TraceTagTargetNode, nextNodeID)
	toNextSpan.Tag(TraceTagOperateType, TraceTagOperateSendToNext)
	needsKeepPropagation = true
	q.s.serverMetrics.totalSendToNextStarted.Inc(1, request.Group)
	propagationStart := time.Now()
	q.s.log.Debug().Stringer("request", request).Str("nextNodeID", nextNodeID).
		Msg("propagating gossip message to next node")
	_, err = propertyv1.NewGossipServiceClient(nextNodeConn).Propagation(ctx, request.PropagationRequest)
	_ = nextNodeConn.Close()
	q.s.serverMetrics.totalSendToNextFinished.Inc(1, request.Group)
	q.s.serverMetrics.totalSendToNextLatency.Inc(time.Since(propagationStart).Seconds(), request.Group)
	if err != nil {
		toNextSpan.Error(err.Error())
		toNextSpan.End()
		q.s.serverMetrics.totalSendToNextErr.Inc(1, request.Group)
		return fmt.Errorf("failed to propagate gossip message to next node: %w", err)
	}
	toNextSpan.End()

	return nil
}

func (q *protocolHandler) contextIsDone(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

func (q *protocolHandler) addToProcess(request *propertyv1.PropagationRequest, tracer Trace) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	shardKey := fmt.Sprintf("%s_%d", request.Group, request.ShardId)
	groupShard, exist := q.groupWithShards[shardKey]
	handlingRequestData := &handlingRequest{
		PropagationRequest: request,
		tracer:             tracer,
		parentSpan:         tracer.ActivateSpan(),
	}
	if !exist {
		groupShard = &groupWithShardPropagation{
			channel:        make(chan *handlingRequest, 1),
			originalNodeID: request.Context.OriginNode,
			latestTime:     time.Now(),
		}
		groupShard.channel <- handlingRequestData
		q.groupWithShards[shardKey] = groupShard
		q.notifyNewRequest()
		return true
	}

	// if the latest round is out of ttl, then needs to change to current node to executing
	if time.Since(groupShard.latestTime) > q.s.scheduleInterval/2 {
		groupShard.originalNodeID = request.Context.OriginNode
		select {
		case groupShard.channel <- handlingRequestData:
			q.notifyNewRequest()
		default:
			q.s.log.Error().Msgf("ready to added propagation into group shard %s(%d) in a new round, but it's full", request.Group, request.ShardId)
		}
		return true
	}

	// if the original node ID are a same node, means which from the same round
	if groupShard.originalNodeID == request.Context.OriginNode {
		select {
		case groupShard.channel <- handlingRequestData:
			q.notifyNewRequest()
		default:
			q.s.log.Error().Msgf("ready to added propagation into group shard %s(%d) in a same round, but it's full", request.Group, request.ShardId)
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

func (q *protocolHandler) nextNodeConnection(nodes []string, offset int, span Span) (string, *grpc.ClientConn, error) {
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
	span.Tag(TraceTagTargetNode, nodeID)
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
	s.log.Debug().Str("address", n.PropertyRepairGossipGrpcAddress).Msg("starting to create gRPC client connection to node")
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
