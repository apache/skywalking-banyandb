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

// agentConnection represents a connection to an agent.
type agentConnection struct {
	metricsStream fodcv1.FODCService_StreamMetricsServer
	lastActivity  time.Time
	agentID       string
	mu            sync.RWMutex
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

// FODCService implements the FODC gRPC service.
type FODCService struct {
	fodcv1.UnimplementedFODCServiceServer
	registry          *registry.AgentRegistry
	metricsAggregator *metrics.Aggregator
	logger            *logger.Logger
	connections       map[string]*agentConnection
	connectionsMu     sync.RWMutex
	heartbeatInterval time.Duration
}

// NewFODCService creates a new FODCService instance.
func NewFODCService(registry *registry.AgentRegistry, metricsAggregator *metrics.Aggregator, logger *logger.Logger, heartbeatInterval time.Duration) *FODCService {
	return &FODCService{
		registry:          registry,
		metricsAggregator: metricsAggregator,
		logger:            logger,
		connections:       make(map[string]*agentConnection),
		heartbeatInterval: heartbeatInterval,
	}
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

	defer s.cleanupConnection(agentID)

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
	defer s.connectionsMu.Unlock()
	delete(s.connections, agentID)

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
