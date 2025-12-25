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
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	fodcv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/fodc/v1"
	"github.com/apache/skywalking-banyandb/fodc/proxy/internal/metrics"
	"github.com/apache/skywalking-banyandb/fodc/proxy/internal/registry"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// AgentConnection represents a connection to an agent.
type AgentConnection struct {
	MetricsStream fodcv1.FODCService_StreamMetricsServer
	Context       context.Context
	Stream        grpc.ServerStream
	Cancel        context.CancelFunc
	LastActivity  time.Time
	AgentID       string
	Identity      registry.AgentIdentity
	mu            sync.RWMutex
}

// UpdateActivity updates the last activity time.
func (ac *AgentConnection) UpdateActivity() {
	ac.mu.Lock()
	defer ac.mu.Unlock()
	ac.LastActivity = time.Now()
}

// GetLastActivity returns the last activity time.
func (ac *AgentConnection) GetLastActivity() time.Time {
	ac.mu.RLock()
	defer ac.mu.RUnlock()
	return ac.LastActivity
}

// FODCService implements the FODC gRPC service.
type FODCService struct {
	fodcv1.UnimplementedFODCServiceServer
	registry          *registry.AgentRegistry
	metricsAggregator *metrics.Aggregator
	logger            *logger.Logger
	connections       map[string]*AgentConnection
	connectionsMu     sync.RWMutex
	heartbeatInterval time.Duration
}

// NewFODCService creates a new FODCService instance.
func NewFODCService(registry *registry.AgentRegistry, metricsAggregator *metrics.Aggregator, logger *logger.Logger, heartbeatInterval time.Duration) *FODCService {
	return &FODCService{
		registry:          registry,
		metricsAggregator: metricsAggregator,
		logger:            logger,
		connections:       make(map[string]*AgentConnection),
		heartbeatInterval: heartbeatInterval,
	}
}

// RegisterAgent handles bi-directional agent registration stream.
func (s *FODCService) RegisterAgent(stream fodcv1.FODCService_RegisterAgentServer) error {
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	var agentID string
	var agentConn *AgentConnection
	initialRegistration := true

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
				IP:     req.PrimaryAddress.Ip,
				Port:   int(req.PrimaryAddress.Port),
				Role:   req.NodeRole,
				Labels: req.Labels,
			}

			primaryAddr := registry.Address{
				IP:   req.PrimaryAddress.Ip,
				Port: int(req.PrimaryAddress.Port),
			}

			secondaryAddrs := make(map[string]registry.Address)
			for name, addr := range req.SecondaryAddresses {
				secondaryAddrs[name] = registry.Address{
					IP:   addr.Ip,
					Port: int(addr.Port),
				}
			}

			registeredAgentID, registerErr := s.registry.RegisterAgent(ctx, identity, primaryAddr, secondaryAddrs)
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
			agentConn = &AgentConnection{
				AgentID:      agentID,
				Identity:     identity,
				Stream:       stream,
				Context:      ctx,
				Cancel:       cancel,
				LastActivity: time.Now(),
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
				s.cleanupConnection(agentID)
				return sendErr
			}

			initialRegistration = false
			s.logger.Info().
				Str("agent_id", agentID).
				Str("ip", identity.IP).
				Int("port", identity.Port).
				Str("role", identity.Role).
				Msg("Agent registration stream established")
		} else {
			if updateErr := s.registry.UpdateHeartbeat(agentID); updateErr != nil {
				s.logger.Error().Err(updateErr).Str("agent_id", agentID).Msg("Failed to update heartbeat")
				s.cleanupConnection(agentID)
				return updateErr
			}

			if agentConn != nil {
				agentConn.UpdateActivity()
			}
		}
	}

	s.cleanupConnection(agentID)
	return nil
}

// StreamMetrics handles bi-directional metrics streaming.
func (s *FODCService) StreamMetrics(stream fodcv1.FODCService_StreamMetricsServer) error {
	ctx := stream.Context()

	agentID := s.getAgentIDFromContext(ctx)
	if agentID == "" {
		agentID = s.getAgentIDFromPeer(ctx)
	}

	if agentID == "" {
		return status.Errorf(codes.Unauthenticated, "agent ID not found in context or peer address")
	}

	agentConn := &AgentConnection{
		AgentID:       agentID,
		Stream:        stream,
		MetricsStream: stream,
		Context:       ctx,
		LastActivity:  time.Now(),
	}

	s.connectionsMu.Lock()
	existingConn, exists := s.connections[agentID]
	if exists {
		existingConn.MetricsStream = stream
	} else {
		s.connections[agentID] = agentConn
	}
	s.connectionsMu.Unlock()

	defer s.cleanupConnection(agentID)

	recvCh := make(chan *fodcv1.StreamMetricsRequest, 1)
	recvErrCh := make(chan error, 1)

	go func() {
		for {
			req, recvErr := stream.Recv()
			if errors.Is(recvErr, io.EOF) {
				recvErrCh <- nil
				return
			}
			if recvErr != nil {
				recvErrCh <- recvErr
				return
			}
			select {
			case recvCh <- req:
			case <-ctx.Done():
				return
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case recvErr := <-recvErrCh:
			if recvErr != nil {
				s.logger.Error().Err(recvErr).Str("agent_id", agentID).Msg("Error receiving metrics")
				return recvErr
			}
			return nil
		case req := <-recvCh:
			if req != nil {
				agentConn.UpdateActivity()

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
	}
}

// RequestMetrics requests metrics from an agent via the metrics stream.
func (s *FODCService) RequestMetrics(_ context.Context, agentID string, startTime, endTime *time.Time) error {
	s.connectionsMu.RLock()
	agentConn, exists := s.connections[agentID]
	s.connectionsMu.RUnlock()

	if !exists {
		return fmt.Errorf("agent connection not found for agent ID: %s", agentID)
	}

	if agentConn.MetricsStream == nil {
		return fmt.Errorf("metrics stream not established for agent ID: %s", agentID)
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

	if sendErr := agentConn.MetricsStream.Send(resp); sendErr != nil {
		return fmt.Errorf("failed to send metrics request: %w", sendErr)
	}

	return nil
}

// cleanupConnection cleans up a connection and unregisters the agent if needed.
func (s *FODCService) cleanupConnection(agentID string) {
	if agentID == "" {
		return
	}

	s.connectionsMu.Lock()
	delete(s.connections, agentID)
	s.connectionsMu.Unlock()

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
	peer, ok := peer.FromContext(ctx)
	if !ok {
		return ""
	}

	peerAddr := peer.Addr.String()
	agents := s.registry.ListAgents()

	for _, agentInfo := range agents {
		if strings.Contains(peerAddr, agentInfo.PrimaryAddress.IP) {
			return agentInfo.AgentID
		}
		for _, secondaryAddr := range agentInfo.SecondaryAddresses {
			if strings.Contains(peerAddr, secondaryAddr.IP) {
				return agentInfo.AgentID
			}
		}
	}

	return ""
}
