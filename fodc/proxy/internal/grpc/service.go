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

package grpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	fodcv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/fodc/v1"
	"github.com/apache/skywalking-banyandb/fodc/proxy/internal/cluster"
	"github.com/apache/skywalking-banyandb/fodc/proxy/internal/diagnostics"
	"github.com/apache/skywalking-banyandb/fodc/proxy/internal/lifecycle"
	"github.com/apache/skywalking-banyandb/fodc/proxy/internal/metrics"
	"github.com/apache/skywalking-banyandb/fodc/proxy/internal/pressure"
	"github.com/apache/skywalking-banyandb/fodc/proxy/internal/registry"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// agentConnection represents a connection to an agent.
type agentConnection struct {
	metricsStream          fodcv1.FODCService_StreamMetricsServer
	clusterStateStream     fodcv1.FODCService_StreamClusterTopologyServer
	lifecycleStream        fodcv1.FODCService_StreamLifecycleServer
	crashDiagnosticsStream fodcv1.FODCService_StreamCrashDiagnosticsServer
	pressureProfilesStream fodcv1.FODCService_StreamPressureProfilesServer
	pendingFetches         map[string]*fetchWaiter
	lastActivity           time.Time
	agentID                string
	mu                     sync.RWMutex
	fetchMu                sync.Mutex
}

// fetchWaiter delivers the chunks of one in-flight download to the HTTP handler that
// issued the fetch. done is closed when that handler stops reading so deliverChunk drops
// late chunks instead of blocking; the chunk channel itself is never closed.
type fetchWaiter struct {
	ch        chan *fodcv1.PressureProfileChunk
	done      chan struct{}
	closeOnce sync.Once
}

// close signals the fetch is over; safe to call from both unregisterFetch and a
// disconnect-time cancellation without double-closing the channel.
func (w *fetchWaiter) close() {
	w.closeOnce.Do(func() { close(w.done) })
}

// updateActivity updates the last activity time.
func (ac *agentConnection) updateActivity() {
	ac.mu.Lock()
	defer ac.mu.Unlock()
	ac.lastActivity = time.Now()
}

// getLastActivity returns the last activity time.
func (ac *agentConnection) getLastActivity() time.Time {
	ac.mu.RLock()
	defer ac.mu.RUnlock()
	return ac.lastActivity
}

// setMetricsStream sets the metrics stream.
func (ac *agentConnection) setMetricsStream(stream fodcv1.FODCService_StreamMetricsServer) {
	ac.mu.Lock()
	defer ac.mu.Unlock()
	ac.metricsStream = stream
}

// setClusterStateStream sets the cluster state stream.
func (ac *agentConnection) setClusterStateStream(stream fodcv1.FODCService_StreamClusterTopologyServer) {
	ac.mu.Lock()
	defer ac.mu.Unlock()
	ac.clusterStateStream = stream
}

// hasClusterStateStream reports whether the cluster topology stream is established.
func (ac *agentConnection) hasClusterStateStream() bool {
	ac.mu.RLock()
	defer ac.mu.RUnlock()
	return ac.clusterStateStream != nil
}

// sendMetricsRequest sends a metrics request to the agent via the metrics stream.
func (ac *agentConnection) sendMetricsRequest(resp *fodcv1.StreamMetricsResponse) error {
	ac.mu.RLock()
	defer ac.mu.RUnlock()
	if ac.metricsStream == nil {
		return fmt.Errorf("metrics stream not established for agent ID: %s", ac.agentID)
	}
	if sendErr := ac.metricsStream.Send(resp); sendErr != nil {
		return fmt.Errorf("failed to send metrics request: %w", sendErr)
	}
	return nil
}

// sendClusterDataRequest sends a cluster data request to the agent via the cluster state stream.
func (ac *agentConnection) sendClusterDataRequest() error {
	ac.mu.RLock()
	defer ac.mu.RUnlock()
	if ac.clusterStateStream == nil {
		return fmt.Errorf("cluster state stream not established for agent ID: %s", ac.agentID)
	}
	resp := &fodcv1.StreamClusterTopologyResponse{
		RequestTopology: true,
	}
	if sendErr := ac.clusterStateStream.Send(resp); sendErr != nil {
		return fmt.Errorf("failed to send cluster data request: %w", sendErr)
	}
	return nil
}

// setLifecycleStream sets the lifecycle stream.
func (ac *agentConnection) setLifecycleStream(stream fodcv1.FODCService_StreamLifecycleServer) {
	ac.mu.Lock()
	defer ac.mu.Unlock()
	ac.lifecycleStream = stream
}

// hasLifecycleStream reports whether the lifecycle stream is established.
func (ac *agentConnection) hasLifecycleStream() bool {
	ac.mu.RLock()
	defer ac.mu.RUnlock()
	return ac.lifecycleStream != nil
}

// sendLifecycleDataRequest sends a lifecycle data request to the agent via the lifecycle stream.
func (ac *agentConnection) sendLifecycleDataRequest() error {
	ac.mu.RLock()
	defer ac.mu.RUnlock()
	if ac.lifecycleStream == nil {
		return fmt.Errorf("lifecycle stream not established for agent ID: %s", ac.agentID)
	}
	resp := &fodcv1.StreamLifecycleResponse{
		RequestLifecycle: true,
	}
	if err := ac.lifecycleStream.Send(resp); err != nil {
		return fmt.Errorf("failed to send lifecycle data request: %w", err)
	}
	return nil
}

// setCrashDiagnosticsStream sets the crash diagnostics stream.
func (ac *agentConnection) setCrashDiagnosticsStream(stream fodcv1.FODCService_StreamCrashDiagnosticsServer) {
	ac.mu.Lock()
	defer ac.mu.Unlock()
	ac.crashDiagnosticsStream = stream
}

// hasCrashDiagnosticsStream reports whether the crash diagnostics stream is established.
func (ac *agentConnection) hasCrashDiagnosticsStream() bool {
	ac.mu.RLock()
	defer ac.mu.RUnlock()
	return ac.crashDiagnosticsStream != nil
}

// sendDiagnosticsRequest sends a crash diagnostics request to the agent.
func (ac *agentConnection) sendDiagnosticsRequest() error {
	ac.mu.RLock()
	defer ac.mu.RUnlock()
	if ac.crashDiagnosticsStream == nil {
		return fmt.Errorf("crash diagnostics stream not established for agent ID: %s", ac.agentID)
	}
	resp := &fodcv1.StreamCrashDiagnosticsResponse{
		RequestDiagnostics: true,
	}
	if sendErr := ac.crashDiagnosticsStream.Send(resp); sendErr != nil {
		return fmt.Errorf("failed to send crash diagnostics request: %w", sendErr)
	}
	return nil
}

// setPressureProfilesStream sets the pressure profiles stream.
func (ac *agentConnection) setPressureProfilesStream(stream fodcv1.FODCService_StreamPressureProfilesServer) {
	ac.mu.Lock()
	defer ac.mu.Unlock()
	ac.pressureProfilesStream = stream
}

// hasPressureProfilesStream reports whether the pressure profiles stream is established.
func (ac *agentConnection) hasPressureProfilesStream() bool {
	ac.mu.RLock()
	defer ac.mu.RUnlock()
	return ac.pressureProfilesStream != nil
}

// sendListProfiles asks the agent to stream its capture-event metadata.
func (ac *agentConnection) sendListProfiles(requestID string) error {
	ac.mu.RLock()
	defer ac.mu.RUnlock()
	if ac.pressureProfilesStream == nil {
		return fmt.Errorf("pressure profiles stream not established for agent ID: %s", ac.agentID)
	}
	resp := &fodcv1.StreamPressureProfilesResponse{
		Command: &fodcv1.StreamPressureProfilesResponse_ListProfiles{
			ListProfiles: &fodcv1.ListProfiles{RequestId: requestID},
		},
	}
	if sendErr := ac.pressureProfilesStream.Send(resp); sendErr != nil {
		return fmt.Errorf("failed to send list profiles request: %w", sendErr)
	}
	return nil
}

// sendFetchProfile asks the agent to stream one profile's bytes.
func (ac *agentConnection) sendFetchProfile(fetch *fodcv1.FetchPressureProfile) error {
	ac.mu.RLock()
	defer ac.mu.RUnlock()
	if ac.pressureProfilesStream == nil {
		return fmt.Errorf("pressure profiles stream not established for agent ID: %s", ac.agentID)
	}
	resp := &fodcv1.StreamPressureProfilesResponse{
		Command: &fodcv1.StreamPressureProfilesResponse_FetchProfile{FetchProfile: fetch},
	}
	if sendErr := ac.pressureProfilesStream.Send(resp); sendErr != nil {
		return fmt.Errorf("failed to send fetch profile request: %w", sendErr)
	}
	return nil
}

// registerFetch creates a waiter for the chunks of one download, keyed by request id.
func (ac *agentConnection) registerFetch(requestID string) *fetchWaiter {
	ac.mu.Lock()
	defer ac.mu.Unlock()
	if ac.pendingFetches == nil {
		ac.pendingFetches = make(map[string]*fetchWaiter)
	}
	waiter := &fetchWaiter{
		ch:   make(chan *fodcv1.PressureProfileChunk, 4),
		done: make(chan struct{}),
	}
	ac.pendingFetches[requestID] = waiter
	return waiter
}

// unregisterFetch removes the waiter and signals deliverChunk to stop delivering.
func (ac *agentConnection) unregisterFetch(requestID string) {
	ac.mu.Lock()
	defer ac.mu.Unlock()
	if waiter, ok := ac.pendingFetches[requestID]; ok {
		waiter.close()
		delete(ac.pendingFetches, requestID)
	}
}

// cancelPendingFetches closes every in-flight fetch waiter so blocked FetchPressureProfile
// calls return immediately instead of hanging until the chunk timeout; called on disconnect.
func (ac *agentConnection) cancelPendingFetches() {
	ac.mu.Lock()
	defer ac.mu.Unlock()
	for _, waiter := range ac.pendingFetches {
		waiter.close()
	}
}

// deliverChunk routes one received chunk to the waiting download handler.
func (ac *agentConnection) deliverChunk(chunk *fodcv1.PressureProfileChunk) {
	ac.mu.RLock()
	waiter := ac.pendingFetches[chunk.RequestId]
	ac.mu.RUnlock()
	if waiter == nil {
		return
	}
	select {
	case waiter.ch <- chunk:
	case <-waiter.done:
	}
}

// FODCService implements the FODC gRPC service.
type FODCService struct {
	fodcv1.UnimplementedFODCServiceServer
	registry                   *registry.AgentRegistry
	metricsAggregator          *metrics.Aggregator
	clusterStateManager        *cluster.Manager
	lifecycleManager           *lifecycle.Manager
	crashDiagnosticsAggregator *diagnostics.Aggregator
	pressureAggregator         *pressure.Aggregator
	logger                     *logger.Logger
	connections                map[string]*agentConnection
	listWaiters                map[string]*listWaiter
	connectionsMu              sync.RWMutex
	listMu                     sync.Mutex
	heartbeatInterval          time.Duration
}

// listWaiter tracks one CollectList request: it closes done once every agent that was
// successfully sent the list command has reported ListComplete (or been dropped on a send
// failure / disconnect), so the aggregator returns without waiting out the full timeout.
type listWaiter struct {
	pending map[string]struct{}
	done    chan struct{}
	closed  bool
	mu      sync.Mutex
}

// ack removes one agent from the pending set, closing done when the set empties.
func (w *listWaiter) ack(agentID string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.pending, agentID)
	if len(w.pending) == 0 {
		w.closeLocked()
	}
}

// forceDone closes done unconditionally. It is the TTL backstop that releases the waiter (and
// any caller still parked on it) even if some agent never acks; in normal operation the
// aggregator's own listTimeout caps latency well before the TTL fires.
func (w *listWaiter) forceDone() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closeLocked()
}

// closeLocked closes done exactly once. Callers must hold w.mu.
func (w *listWaiter) closeLocked() {
	if !w.closed {
		w.closed = true
		close(w.done)
	}
}

// NewFODCService creates a new FODCService instance.
func NewFODCService(
	registry *registry.AgentRegistry,
	metricsAggregator *metrics.Aggregator,
	clusterStateManager *cluster.Manager,
	lifecycleManager *lifecycle.Manager,
	crashDiagnosticsAggregator *diagnostics.Aggregator,
	pressureAggregator *pressure.Aggregator,
	logger *logger.Logger,
	heartbeatInterval time.Duration,
) *FODCService {
	return &FODCService{
		registry:                   registry,
		metricsAggregator:          metricsAggregator,
		clusterStateManager:        clusterStateManager,
		lifecycleManager:           lifecycleManager,
		crashDiagnosticsAggregator: crashDiagnosticsAggregator,
		pressureAggregator:         pressureAggregator,
		logger:                     logger,
		connections:                make(map[string]*agentConnection),
		listWaiters:                make(map[string]*listWaiter),
		heartbeatInterval:          heartbeatInterval,
	}
}

// HasClusterStateStream reports whether the given agent currently has a cluster topology stream.
func (s *FODCService) HasClusterStateStream(agentID string) bool {
	s.connectionsMu.RLock()
	defer s.connectionsMu.RUnlock()
	conn, exists := s.connections[agentID]
	if !exists || conn == nil {
		return false
	}
	return conn.hasClusterStateStream()
}

// HasLifecycleStream reports whether the given agent currently has a lifecycle stream.
func (s *FODCService) HasLifecycleStream(agentID string) bool {
	s.connectionsMu.RLock()
	defer s.connectionsMu.RUnlock()
	conn, exists := s.connections[agentID]
	if !exists || conn == nil {
		return false
	}
	return conn.hasLifecycleStream()
}

// RegisterAgent handles bi-directional agent registration stream.
func (s *FODCService) RegisterAgent(stream fodcv1.FODCService_RegisterAgentServer) error {
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	var agentID string
	var agentConn *agentConnection
	initialRegistration := true
	defer func() {
		s.cleanupConnection(agentID)
	}()

	for {
		req, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			s.logger.Debug().Str("agent_id", agentID).Msg("Registration stream closed by agent")
			break
		}
		if recvErr != nil {
			s.logger.Error().Err(recvErr).Str("agent_id", agentID).Msg("Error receiving registration request")
			return recvErr
		}

		if initialRegistration {
			identity := registry.AgentIdentity{
				Role:           req.NodeRole,
				Labels:         req.Labels,
				PodName:        req.PodName,
				ContainerNames: req.ContainerNames,
			}

			registeredAgentID, registerErr := s.registry.RegisterAgent(ctx, identity)
			if registerErr != nil {
				resp := &fodcv1.RegisterAgentResponse{
					Success: false,
					Message: registerErr.Error(),
				}
				if sendErr := stream.Send(resp); sendErr != nil {
					s.logger.Error().Err(sendErr).Msg("Failed to send registration error response")
				}
				return registerErr
			}

			agentID = registeredAgentID

			agentConn = &agentConnection{
				agentID:      agentID,
				lastActivity: time.Now(),
			}

			s.connectionsMu.Lock()
			s.connections[agentID] = agentConn
			s.connectionsMu.Unlock()

			resp := &fodcv1.RegisterAgentResponse{
				Success:                  true,
				Message:                  "Agent registered successfully",
				HeartbeatIntervalSeconds: int64(s.heartbeatInterval.Seconds()),
				AgentId:                  agentID,
			}

			if sendErr := stream.Send(resp); sendErr != nil {
				s.logger.Error().Err(sendErr).Str("agent_id", agentID).Msg("Failed to send registration response")
				// Unregister agent since we couldn't send confirmation
				if unregisterErr := s.registry.UnregisterAgent(agentID); unregisterErr != nil {
					s.logger.Error().Err(unregisterErr).Str("agent_id", agentID).Msg("Failed to unregister agent after send error")
				}
				return sendErr
			}

			initialRegistration = false
			logFields := s.logger.Info().
				Str("agent_id", agentID).
				Str("role", identity.Role).
				Str("pod_name", identity.PodName)
			if len(identity.ContainerNames) > 0 {
				logFields = logFields.Strs("container_names", identity.ContainerNames)
			}
			logFields.Msg("Agent registration stream established")
		} else {
			if updateErr := s.registry.UpdateHeartbeat(agentID); updateErr != nil {
				s.logger.Error().Err(updateErr).Str("agent_id", agentID).Msg("Failed to update heartbeat")
				return updateErr
			}

			if agentConn != nil {
				agentConn.updateActivity()
			}
		}
	}

	return nil
}

// StreamMetrics handles bi-directional metrics streaming.
func (s *FODCService) StreamMetrics(stream fodcv1.FODCService_StreamMetricsServer) error {
	ctx := stream.Context()

	agentID := s.getAgentIDFromContext(ctx)
	if agentID == "" {
		agentID = s.getAgentIDFromPeer(ctx)
		if agentID != "" {
			s.logger.Warn().
				Str("agent_id", agentID).
				Msg("Agent ID not found in metadata, using peer address fallback (this may be unreliable)")
		}
	}

	if agentID == "" {
		s.logger.Error().Msg("Agent ID not found in context metadata or peer address")
		return status.Errorf(codes.Unauthenticated, "agent ID not found in context or peer address")
	}

	s.connectionsMu.Lock()
	existingConn, exists := s.connections[agentID]
	if exists {
		existingConn.setMetricsStream(stream)
		existingConn.updateActivity()
	} else {
		agentConn := &agentConnection{
			agentID:       agentID,
			metricsStream: stream,
			lastActivity:  time.Now(),
		}
		s.connections[agentID] = agentConn
	}
	s.connectionsMu.Unlock()

	for {
		req, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			s.logger.Debug().Str("agent_id", agentID).Msg("Metrics stream closed by agent")
			return nil
		}
		if recvErr != nil {
			if errors.Is(recvErr, context.Canceled) || errors.Is(recvErr, context.DeadlineExceeded) {
				s.logger.Debug().Err(recvErr).Str("agent_id", agentID).Msg("Metrics stream closed")
			} else if st, ok := status.FromError(recvErr); ok {
				code := st.Code()
				if code == codes.Canceled || code == codes.DeadlineExceeded {
					s.logger.Debug().Err(recvErr).Str("agent_id", agentID).Msg("Metrics stream closed")
				} else {
					s.logger.Error().Err(recvErr).Str("agent_id", agentID).Msg("Error receiving metrics")
				}
			} else {
				s.logger.Error().Err(recvErr).Str("agent_id", agentID).Msg("Error receiving metrics")
			}
			return recvErr
		}

		s.connectionsMu.RLock()
		conn, connExists := s.connections[agentID]
		s.connectionsMu.RUnlock()
		if connExists {
			conn.updateActivity()
		}

		agentInfo, getErr := s.registry.GetAgentByID(agentID)
		if getErr != nil {
			s.logger.Error().Err(getErr).Str("agent_id", agentID).Msg("Failed to get agent info")
			continue
		}

		if processErr := s.metricsAggregator.ProcessMetricsFromAgent(ctx, agentID, agentInfo, req); processErr != nil {
			s.logger.Error().Err(processErr).Str("agent_id", agentID).Msg("Failed to process metrics")
		}
	}
}

// RequestMetrics requests metrics from an agent via the metrics stream.
func (s *FODCService) RequestMetrics(agentID string, startTime, endTime *time.Time) error {
	s.connectionsMu.RLock()
	agentConn, exists := s.connections[agentID]
	s.connectionsMu.RUnlock()

	if !exists {
		return fmt.Errorf("agent connection not found for agent ID: %s", agentID)
	}

	resp := &fodcv1.StreamMetricsResponse{}
	if startTime != nil {
		resp.StartTime = &timestamppb.Timestamp{
			Seconds: startTime.Unix(),
			Nanos:   int32(startTime.Nanosecond()),
		}
	}
	if endTime != nil {
		resp.EndTime = &timestamppb.Timestamp{
			Seconds: endTime.Unix(),
			Nanos:   int32(endTime.Nanosecond()),
		}
	}

	return agentConn.sendMetricsRequest(resp)
}

// cleanupConnection cleans up a connection and unregisters the agent if needed.
func (s *FODCService) cleanupConnection(agentID string) {
	if agentID == "" {
		return
	}

	s.connectionsMu.Lock()
	conn := s.connections[agentID]
	delete(s.connections, agentID)
	s.connectionsMu.Unlock()

	if conn != nil {
		conn.cancelPendingFetches()
	}
	s.ackDepartedAgent(agentID)
	if s.pressureAggregator != nil {
		s.pressureAggregator.RemoveAgent(agentID)
	}

	s.logger.Debug().Str("agent_id", agentID).Msg("Cleaned up agent connection")
}

// getAgentIDFromContext extracts agent ID from context metadata.
func (s *FODCService) getAgentIDFromContext(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}

	agentIDs := md.Get("agent_id")
	if len(agentIDs) == 0 {
		return ""
	}

	return agentIDs[0]
}

// getAgentIDFromPeer extracts agent ID by matching peer address with registered agents.
func (s *FODCService) getAgentIDFromPeer(ctx context.Context) string {
	peerInfo, ok := peer.FromContext(ctx)
	if !ok {
		return ""
	}

	peerAddr := peerInfo.Addr.String()
	agents := s.registry.ListAgents()

	peerIP, _, splitErr := net.SplitHostPort(peerAddr)
	if splitErr != nil {
		return ""
	}

	for _, agentInfo := range agents {
		// Exact match on primary address IP
		if agentInfo.AgentIdentity.PodName == peerIP {
			return agentInfo.AgentID
		}
	}

	return ""
}

// StreamClusterTopology handles bi-directional cluster topology streaming.
func (s *FODCService) StreamClusterTopology(stream fodcv1.FODCService_StreamClusterTopologyServer) error {
	ctx := stream.Context()
	agentID := s.getAgentIDFromContext(ctx)
	if agentID == "" {
		agentID = s.getAgentIDFromPeer(ctx)
		if agentID != "" {
			s.logger.Warn().
				Str("agent_id", agentID).
				Msg("Agent ID not found in metadata, using peer address fallback (this may be unreliable)")
		}
	}
	if agentID == "" {
		s.logger.Error().Msg("Agent ID not found in context metadata or peer address")
		return status.Errorf(codes.Unauthenticated, "agent ID not found in context or peer address")
	}

	s.connectionsMu.Lock()
	existingConn, exists := s.connections[agentID]
	if exists {
		existingConn.setClusterStateStream(stream)
		existingConn.updateActivity()
	} else {
		agentConn := &agentConnection{
			agentID:            agentID,
			clusterStateStream: stream,
			lastActivity:       time.Now(),
		}
		s.connections[agentID] = agentConn
	}
	s.connectionsMu.Unlock()
	for {
		req, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			s.logger.Debug().Str("agent_id", agentID).Msg("Cluster state stream closed by agent")
			return nil
		}
		if recvErr != nil {
			if errors.Is(recvErr, context.Canceled) || errors.Is(recvErr, context.DeadlineExceeded) {
				s.logger.Debug().Err(recvErr).Str("agent_id", agentID).Msg("Cluster state stream closed")
			} else if st, ok := status.FromError(recvErr); ok {
				code := st.Code()
				if code == codes.Canceled || code == codes.DeadlineExceeded {
					s.logger.Debug().Err(recvErr).Str("agent_id", agentID).Msg("Cluster state stream closed")
				} else {
					s.logger.Error().Err(recvErr).Str("agent_id", agentID).Msg("Error receiving cluster state")
				}
			} else {
				s.logger.Error().Err(recvErr).Str("agent_id", agentID).Msg("Error receiving cluster state")
			}
			return recvErr
		}
		if s.clusterStateManager != nil {
			nodeCount := 0
			callCount := 0
			if req.Topology != nil {
				nodeCount = len(req.Topology.Nodes)
				callCount = len(req.Topology.Calls)
			}
			s.clusterStateManager.UpdateClusterTopology(agentID, req.Topology)
			s.logger.Debug().
				Str("agent_id", agentID).
				Int("nodes_count", nodeCount).
				Int("calls_count", callCount).
				Msg("Received cluster topology from agent")
		}
	}
}

// RequestClusterData requests cluster data from an agent via the cluster state stream.
func (s *FODCService) RequestClusterData(agentID string) error {
	s.connectionsMu.RLock()
	agentConn, exists := s.connections[agentID]
	s.connectionsMu.RUnlock()
	if !exists {
		return fmt.Errorf("agent connection not found for agent ID: %s", agentID)
	}
	return agentConn.sendClusterDataRequest()
}

// StreamLifecycle handles bi-directional lifecycle data streaming.
func (s *FODCService) StreamLifecycle(stream fodcv1.FODCService_StreamLifecycleServer) error {
	ctx := stream.Context()
	agentID := s.getAgentIDFromContext(ctx)
	if agentID == "" {
		agentID = s.getAgentIDFromPeer(ctx)
		if agentID != "" {
			s.logger.Warn().
				Str("agent_id", agentID).
				Msg("Agent ID not found in metadata, using peer address fallback (this may be unreliable)")
		}
	}
	if agentID == "" {
		s.logger.Error().Msg("Agent ID not found in context metadata or peer address for lifecycle stream")
		return status.Errorf(codes.Unauthenticated, "agent ID not found in context or peer address")
	}

	s.connectionsMu.Lock()
	existingConn, exists := s.connections[agentID]
	if exists {
		existingConn.setLifecycleStream(stream)
		existingConn.updateActivity()
	} else {
		agentConn := &agentConnection{
			agentID:         agentID,
			lifecycleStream: stream,
			lastActivity:    time.Now(),
		}
		s.connections[agentID] = agentConn
	}
	s.connectionsMu.Unlock()

	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			s.logger.Debug().Str("agent_id", agentID).Msg("Lifecycle stream closed by agent")
			return nil
		}
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				s.logger.Debug().Err(err).Str("agent_id", agentID).Msg("Lifecycle stream closed")
			} else if st, ok := status.FromError(err); ok {
				code := st.Code()
				if code == codes.Canceled || code == codes.DeadlineExceeded {
					s.logger.Debug().Err(err).Str("agent_id", agentID).Msg("Lifecycle stream closed")
				} else {
					s.logger.Error().Err(err).Str("agent_id", agentID).Msg("Error receiving lifecycle data")
				}
			} else {
				s.logger.Error().Err(err).Str("agent_id", agentID).Msg("Error receiving lifecycle data")
			}
			return err
		}
		if s.lifecycleManager != nil {
			data := req.LifecycleData
			if data == nil {
				data = &fodcv1.LifecycleData{}
			}
			s.lifecycleManager.UpdateLifecycle(agentID, req.PodName, data)
			s.logger.Debug().
				Str("agent_id", agentID).
				Str("pod_name", req.PodName).
				Int("reports_count", len(data.Reports)).
				Msg("Received lifecycle data from agent")
		}
	}
}

// RequestLifecycleData requests lifecycle data from an agent via the lifecycle stream.
func (s *FODCService) RequestLifecycleData(agentID string) error {
	s.connectionsMu.RLock()
	agentConn, exists := s.connections[agentID]
	s.connectionsMu.RUnlock()
	if !exists {
		return fmt.Errorf("agent connection not found for agent ID: %s", agentID)
	}
	return agentConn.sendLifecycleDataRequest()
}

// HasCrashDiagnosticsStream reports whether the given agent has a crash diagnostics stream.
func (s *FODCService) HasCrashDiagnosticsStream(agentID string) bool {
	s.connectionsMu.RLock()
	defer s.connectionsMu.RUnlock()
	conn, exists := s.connections[agentID]
	if !exists || conn == nil {
		return false
	}
	return conn.hasCrashDiagnosticsStream()
}

// HasPressureProfilesStream reports whether the given agent currently has a pressure profiles stream.
func (s *FODCService) HasPressureProfilesStream(agentID string) bool {
	s.connectionsMu.RLock()
	defer s.connectionsMu.RUnlock()
	conn, exists := s.connections[agentID]
	if !exists || conn == nil {
		return false
	}
	return conn.hasPressureProfilesStream()
}

// StreamCrashDiagnostics handles bi-directional crash diagnostics streaming.
func (s *FODCService) StreamCrashDiagnostics(stream fodcv1.FODCService_StreamCrashDiagnosticsServer) error {
	ctx := stream.Context()
	agentID := s.getAgentIDFromContext(ctx)
	if agentID == "" {
		agentID = s.getAgentIDFromPeer(ctx)
		if agentID != "" {
			s.logger.Warn().
				Str("agent_id", agentID).
				Msg("Agent ID not found in metadata, using peer address fallback (this may be unreliable)")
		}
	}
	if agentID == "" {
		s.logger.Error().Msg("Agent ID not found in context metadata or peer address for crash diagnostics stream")
		return status.Errorf(codes.Unauthenticated, "agent ID not found in context or peer address")
	}

	s.connectionsMu.Lock()
	existingConn, exists := s.connections[agentID]
	if exists {
		existingConn.setCrashDiagnosticsStream(stream)
		existingConn.updateActivity()
	} else {
		agentConn := &agentConnection{
			agentID:                agentID,
			crashDiagnosticsStream: stream,
			lastActivity:           time.Now(),
		}
		s.connections[agentID] = agentConn
	}
	s.connectionsMu.Unlock()

	for {
		req, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			s.logger.Debug().Str("agent_id", agentID).Msg("Crash diagnostics stream closed by agent")
			return nil
		}
		if recvErr != nil {
			if errors.Is(recvErr, context.Canceled) || errors.Is(recvErr, context.DeadlineExceeded) {
				s.logger.Debug().Err(recvErr).Str("agent_id", agentID).Msg("Crash diagnostics stream closed")
			} else if st, ok := status.FromError(recvErr); ok {
				code := st.Code()
				if code == codes.Canceled || code == codes.DeadlineExceeded {
					s.logger.Debug().Err(recvErr).Str("agent_id", agentID).Msg("Crash diagnostics stream closed")
				} else {
					s.logger.Error().Err(recvErr).Str("agent_id", agentID).Msg("Error receiving crash diagnostics")
				}
			} else {
				s.logger.Error().Err(recvErr).Str("agent_id", agentID).Msg("Error receiving crash diagnostics")
			}
			return recvErr
		}

		if s.crashDiagnosticsAggregator != nil {
			agentInfo, getErr := s.registry.GetAgentByID(agentID)
			if getErr != nil {
				s.logger.Error().Err(getErr).Str("agent_id", agentID).Msg("Failed to get agent info for crash record")
				continue
			}
			s.crashDiagnosticsAggregator.ProcessCrashFromAgent(agentID, agentInfo, req)
			s.logger.Debug().
				Str("agent_id", agentID).
				Str("artifact_dir", req.ArtifactDir).
				Msg("Received crash diagnostic record from agent")
		}
	}
}

// RequestDiagnostics requests crash diagnostics from an agent via the crash diagnostics stream.
func (s *FODCService) RequestDiagnostics(agentID string) error {
	s.connectionsMu.RLock()
	agentConn, exists := s.connections[agentID]
	s.connectionsMu.RUnlock()
	if !exists {
		return fmt.Errorf("agent connection not found for agent ID: %s", agentID)
	}
	return agentConn.sendDiagnosticsRequest()
}

// fetchChunkTimeout bounds the wait for each download chunk; it resets per chunk, so a
// large but steadily-streaming profile never trips it.
const fetchChunkTimeout = 30 * time.Second

// StreamPressureProfiles handles the bi-directional memory-pressure pprof stream: agents
// reply to list/fetch commands with metadata records or binary chunks.
func (s *FODCService) StreamPressureProfiles(stream fodcv1.FODCService_StreamPressureProfilesServer) error {
	ctx := stream.Context()
	agentID := s.getAgentIDFromContext(ctx)
	if agentID == "" {
		agentID = s.getAgentIDFromPeer(ctx)
		if agentID != "" {
			s.logger.Warn().Str("agent_id", agentID).
				Msg("Agent ID not found in metadata, using peer address fallback (this may be unreliable)")
		}
	}
	if agentID == "" {
		s.logger.Error().Msg("Agent ID not found in context metadata or peer address for pressure profiles stream")
		return status.Errorf(codes.Unauthenticated, "agent ID not found in context or peer address")
	}

	s.connectionsMu.Lock()
	existingConn, exists := s.connections[agentID]
	if exists {
		existingConn.setPressureProfilesStream(stream)
		existingConn.updateActivity()
	} else {
		s.connections[agentID] = &agentConnection{
			agentID:                agentID,
			pressureProfilesStream: stream,
			lastActivity:           time.Now(),
		}
	}
	s.connectionsMu.Unlock()

	for {
		req, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			s.logger.Debug().Str("agent_id", agentID).Msg("Pressure profiles stream closed by agent")
			return nil
		}
		if recvErr != nil {
			if errors.Is(recvErr, context.Canceled) || errors.Is(recvErr, context.DeadlineExceeded) {
				s.logger.Debug().Err(recvErr).Str("agent_id", agentID).Msg("Pressure profiles stream closed")
			} else if st, ok := status.FromError(recvErr); ok && (st.Code() == codes.Canceled || st.Code() == codes.DeadlineExceeded) {
				s.logger.Debug().Err(recvErr).Str("agent_id", agentID).Msg("Pressure profiles stream closed")
			} else {
				s.logger.Error().Err(recvErr).Str("agent_id", agentID).Msg("Error receiving pressure profiles")
			}
			return recvErr
		}
		s.handlePressurePayload(agentID, req)
	}
}

// handlePressurePayload routes one stream message: a metadata record to the aggregator,
// or a download chunk to the waiting fetch handler.
func (s *FODCService) handlePressurePayload(agentID string, req *fodcv1.StreamPressureProfilesRequest) {
	switch payload := req.Payload.(type) {
	case *fodcv1.StreamPressureProfilesRequest_Record:
		if s.pressureAggregator == nil {
			return
		}
		agentInfo, getErr := s.registry.GetAgentByID(agentID)
		if getErr != nil {
			s.logger.Error().Err(getErr).Str("agent_id", agentID).Msg("Failed to get agent info for pressure profile record")
			return
		}
		s.pressureAggregator.ProcessProfileFromAgent(agentID, agentInfo, payload.Record)
	case *fodcv1.StreamPressureProfilesRequest_Chunk:
		s.connectionsMu.RLock()
		conn := s.connections[agentID]
		s.connectionsMu.RUnlock()
		if conn != nil {
			conn.deliverChunk(payload.Chunk)
		}
	case *fodcv1.StreamPressureProfilesRequest_ListComplete:
		// Promote the agent's staged list into the cache before waking the waiting
		// CollectProfiles, so the snapshot it returns reflects this round's full set
		// (and drops events the agent has evicted).
		if s.pressureAggregator != nil {
			s.pressureAggregator.FinalizeAgentList(agentID)
		}
		s.ackList(payload.ListComplete.RequestId, agentID)
	}
}

// collectListTTL bounds how long a list waiter lingers in the registry, independent of the
// aggregator's own wait timeout, so a request whose agents never all ack does not leak.
const collectListTTL = 30 * time.Second

// CollectList sends a list_profiles command (under one request id) to every agent and returns a
// channel closed once each successfully-contacted agent has reported ListComplete. Agents that
// cannot be reached are dropped immediately so they never hold the channel open.
func (s *FODCService) CollectList(agentIDs []string) <-chan struct{} {
	if len(agentIDs) == 0 {
		done := make(chan struct{})
		close(done)
		return done
	}
	requestID := uuid.NewString()
	waiter := &listWaiter{pending: make(map[string]struct{}), done: make(chan struct{})}
	for _, id := range agentIDs {
		waiter.pending[id] = struct{}{}
	}

	s.listMu.Lock()
	s.listWaiters[requestID] = waiter
	s.listMu.Unlock()
	// Remove the registry entry as soon as the waiter completes (all agents acked), and fall
	// back to collectListTTL to force-unblock and clean up if some agent never acks. This keeps
	// waiters/timers from lingering the full TTL under frequent list calls.
	timer := time.NewTimer(collectListTTL)
	go func() {
		select {
		case <-waiter.done:
			timer.Stop()
		case <-timer.C:
			waiter.forceDone()
		}
		s.listMu.Lock()
		delete(s.listWaiters, requestID)
		s.listMu.Unlock()
	}()

	for _, id := range agentIDs {
		s.connectionsMu.RLock()
		conn, exists := s.connections[id]
		s.connectionsMu.RUnlock()
		if !exists {
			waiter.ack(id)
			continue
		}
		if sendErr := conn.sendListProfiles(requestID); sendErr != nil {
			s.logger.Info().Err(sendErr).Str("agent_id", id).Msg("Failed to send list profiles request, skipping")
			waiter.ack(id)
		}
	}
	return waiter.done
}

// ackList records that an agent finished streaming its list for the given request id.
func (s *FODCService) ackList(requestID, agentID string) {
	s.listMu.Lock()
	waiter := s.listWaiters[requestID]
	s.listMu.Unlock()
	if waiter != nil {
		waiter.ack(agentID)
	}
}

// ackDepartedAgent acks a disconnecting agent in every in-flight list waiter: it will never
// send ListComplete, so without this CollectProfiles would stall the full listTimeout on each
// disconnect.
func (s *FODCService) ackDepartedAgent(agentID string) {
	s.listMu.Lock()
	defer s.listMu.Unlock()
	for _, waiter := range s.listWaiters {
		waiter.ack(agentID)
	}
}

// FetchPressureProfile streams one profile's bytes from an agent, invoking onChunk for
// each data slice. Only one fetch per agent runs at a time so a slow download never head-
// of-line blocks list metadata.
func (s *FODCService) FetchPressureProfile(ctx context.Context, agentID string, fetch *fodcv1.FetchPressureProfile, onChunk func([]byte) error) error {
	s.connectionsMu.RLock()
	agentConn, exists := s.connections[agentID]
	s.connectionsMu.RUnlock()
	if !exists {
		return fmt.Errorf("agent connection not found for agent ID: %s", agentID)
	}

	agentConn.fetchMu.Lock()
	defer agentConn.fetchMu.Unlock()

	waiter := agentConn.registerFetch(fetch.RequestId)
	defer agentConn.unregisterFetch(fetch.RequestId)

	if sendErr := agentConn.sendFetchProfile(fetch); sendErr != nil {
		return sendErr
	}

	// One timer reused across chunks (Reset per chunk) rather than a fresh time.After each
	// iteration, which would leak a timer per chunk for the duration of a large download.
	timer := time.NewTimer(fetchChunkTimeout)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-waiter.done:
			return fmt.Errorf("agent %s disconnected during profile fetch", agentID)
		case <-timer.C:
			return fmt.Errorf("timed out waiting for profile chunk from agent ID: %s", agentID)
		case chunk := <-waiter.ch:
			if chunk.Error != "" {
				return fmt.Errorf("agent failed to serve profile: %s", chunk.Error)
			}
			if len(chunk.Data) > 0 {
				if writeErr := onChunk(chunk.Data); writeErr != nil {
					return writeErr
				}
			}
			if chunk.Last {
				return nil
			}
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(fetchChunkTimeout)
		}
	}
}
