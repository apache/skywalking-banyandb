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

// Package proxy provides a client for communicating with the FODC Proxy.
package proxy

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
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	fodcv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/fodc/v1"
	flightrecorder "github.com/apache/skywalking-banyandb/fodc/agent/internal/flightrecorder"
	"github.com/apache/skywalking-banyandb/fodc/agent/internal/metrics"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// MetricsRequestFilter defines filters for metrics requests.
type MetricsRequestFilter struct {
	StartTime *time.Time
	EndTime   *time.Time
}

// Client manages connection and communication with the FODC Proxy.
type Client struct {
	conn               *grpc.ClientConn
	heartbeatTicker    *time.Ticker
	flightRecorder     *flightrecorder.FlightRecorder
	logger             *logger.Logger
	stopCh             chan struct{}
	labels             map[string]string
	client             fodcv1.FODCServiceClient
	registrationStream fodcv1.FODCService_RegisterAgentClient
	metricsStream      fodcv1.FODCService_StreamMetricsClient

	proxyAddr string
	nodeIP    string
	nodeRole  string
	agentID   string

	nodePort          int
	heartbeatInterval time.Duration
	reconnectInterval time.Duration
	disconnected      bool
	mu                sync.RWMutex
}

// NewClient creates a new Client instance.
func NewClient(
	proxyAddr string,
	nodeIP string,
	nodePort int,
	nodeRole string,
	labels map[string]string,
	heartbeatInterval time.Duration,
	reconnectInterval time.Duration,
	flightRecorder *flightrecorder.FlightRecorder,
	logger *logger.Logger,
) *Client {
	return &Client{
		proxyAddr:         proxyAddr,
		nodeIP:            nodeIP,
		nodePort:          nodePort,
		nodeRole:          nodeRole,
		labels:            labels,
		heartbeatInterval: heartbeatInterval,
		reconnectInterval: reconnectInterval,
		flightRecorder:    flightRecorder,
		logger:            logger,
		stopCh:            make(chan struct{}),
	}
}

// Connect establishes a gRPC connection to Proxy.
func (c *Client) Connect(_ context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		return nil
	}

	// Reset disconnected state and recreate stopCh for reconnection
	if c.disconnected {
		c.disconnected = false
		c.stopCh = make(chan struct{})
	}

	conn, dialErr := grpc.NewClient(c.proxyAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if dialErr != nil {
		return fmt.Errorf("failed to create proxy client: %w", dialErr)
	}

	c.conn = conn
	c.client = fodcv1.NewFODCServiceClient(conn)

	c.logger.Info().
		Str("proxy_addr", c.proxyAddr).
		Msg("Connected to FODC Proxy")

	return nil
}

// StartRegistrationStream establishes bi-directional registration stream with Proxy.
func (c *Client) StartRegistrationStream(ctx context.Context) error {
	c.mu.Lock()
	if c.client == nil {
		c.mu.Unlock()
		return fmt.Errorf("client not connected, call Connect() first")
	}
	client := c.client
	c.mu.Unlock()

	stream, streamErr := client.RegisterAgent(ctx)
	if streamErr != nil {
		return fmt.Errorf("failed to create registration stream: %w", streamErr)
	}

	c.mu.Lock()
	c.registrationStream = stream
	c.mu.Unlock()

	req := &fodcv1.RegisterAgentRequest{
		NodeRole: c.nodeRole,
		Labels:   c.labels,
		PrimaryAddress: &fodcv1.Address{
			Ip:   c.nodeIP,
			Port: int32(c.nodePort),
		},
	}

	if sendErr := stream.Send(req); sendErr != nil {
		return fmt.Errorf("failed to send registration request: %w", sendErr)
	}

	resp, recvErr := stream.Recv()
	if recvErr != nil {
		return fmt.Errorf("failed to receive registration response: %w", recvErr)
	}

	if !resp.Success {
		return fmt.Errorf("registration failed: %s", resp.Message)
	}

	if resp.AgentId == "" {
		return fmt.Errorf("registration response missing agent ID")
	}

	c.mu.Lock()
	c.agentID = resp.AgentId
	if resp.HeartbeatIntervalSeconds > 0 {
		c.heartbeatInterval = time.Duration(resp.HeartbeatIntervalSeconds) * time.Second
	}
	c.mu.Unlock()

	c.logger.Info().
		Str("proxy_addr", c.proxyAddr).
		Str("agent_id", resp.AgentId).
		Dur("heartbeat_interval", c.heartbeatInterval).
		Msg("Agent registered with Proxy")

	c.startHeartbeat(ctx)

	go c.handleRegistrationStream(ctx, stream)

	return nil
}

// StartMetricsStream establishes bi-directional metrics stream with Proxy.
func (c *Client) StartMetricsStream(ctx context.Context) error {
	c.mu.RLock()
	if c.client == nil {
		c.mu.RUnlock()
		return fmt.Errorf("client not connected, call Connect() first")
	}
	client := c.client
	agentID := c.agentID
	c.mu.RUnlock()

	if agentID == "" {
		return fmt.Errorf("agent ID not available, register agent first")
	}

	md := metadata.New(map[string]string{"agent_id": agentID})
	ctxWithMetadata := metadata.NewOutgoingContext(ctx, md)

	stream, streamErr := client.StreamMetrics(ctxWithMetadata)
	if streamErr != nil {
		return fmt.Errorf("failed to create metrics stream: %w", streamErr)
	}

	c.mu.Lock()
	c.metricsStream = stream
	c.mu.Unlock()

	go c.handleMetricsStream(ctx, stream)

	c.logger.Info().
		Str("agent_id", agentID).
		Msg("Metrics stream established with Proxy")

	return nil
}

// RetrieveAndSendMetrics retrieves metrics from Flight Recorder when requested by Proxy.
func (c *Client) RetrieveAndSendMetrics(_ context.Context, filter *MetricsRequestFilter) error {
	c.mu.RLock()
	metricsStream := c.metricsStream
	c.mu.RUnlock()

	if metricsStream == nil {
		return fmt.Errorf("metrics stream not established")
	}

	datasources := c.flightRecorder.GetDatasources()
	if len(datasources) == 0 {
		// Always send a response even if no datasources exist
		req := &fodcv1.StreamMetricsRequest{
			Metrics:   []*fodcv1.Metric{},
			Timestamp: timestamppb.Now(),
		}
		if sendErr := metricsStream.Send(req); sendErr != nil {
			return fmt.Errorf("failed to send empty metrics response: %w", sendErr)
		}
		return nil
	}

	ds := datasources[0]
	allMetrics := ds.GetMetrics()
	timestamps := ds.GetTimestamps()
	descriptions := ds.GetDescriptions()

	// If filtering by time window is requested, we need timestamps
	if filter != nil && (filter.StartTime != nil || filter.EndTime != nil) {
		if timestamps == nil {
			// Send empty response if timestamps are required but not available
			req := &fodcv1.StreamMetricsRequest{
				Metrics:   []*fodcv1.Metric{},
				Timestamp: timestamppb.Now(),
			}
			if sendErr := metricsStream.Send(req); sendErr != nil {
				return fmt.Errorf("failed to send empty metrics response: %w", sendErr)
			}
			return nil
		}

		timestampValues := timestamps.GetAllValues()
		if len(timestampValues) == 0 {
			// Send empty response if no timestamps available for filtering
			req := &fodcv1.StreamMetricsRequest{
				Metrics:   []*fodcv1.Metric{},
				Timestamp: timestamppb.Now(),
			}
			if sendErr := metricsStream.Send(req); sendErr != nil {
				return fmt.Errorf("failed to send empty metrics response: %w", sendErr)
			}
			return nil
		}

		return c.sendFilteredMetrics(metricsStream, allMetrics, timestampValues, descriptions, filter)
	}

	// For latest metrics (no time filter), we can send even without timestamps
	return c.sendLatestMetrics(metricsStream, allMetrics, descriptions)
}

// sendLatestMetrics sends the latest metrics (most recent values).
func (c *Client) sendLatestMetrics(
	stream fodcv1.FODCService_StreamMetricsClient,
	allMetrics map[string]*flightrecorder.MetricRingBuffer,
	descriptions map[string]string,
) error {
	protoMetrics := make([]*fodcv1.Metric, 0)

	for metricKey, metricBuffer := range allMetrics {
		// Get the most recent value (may include unfinalized values)
		metricValue := metricBuffer.GetCurrentValue()
		allValues := metricBuffer.GetAllValues()

		// For latest metrics, we want to include metrics if:
		// 1. There are finalized values (GetAllValues() has entries), OR
		// 2. GetCurrentValue() returns a non-zero value (even if not finalized)
		// We only skip if both GetAllValues() is empty AND GetCurrentValue() is zero
		// This ensures we include metrics that were just added but not yet finalized
		var zero float64
		if len(allValues) == 0 {
			// No finalized values - check if GetCurrentValue() has a value
			if metricValue == zero {
				// Both are empty/zero, skip this metric
				continue
			}
			// GetCurrentValue() has a non-zero value (might be unfinalized), include it
		}

		parsedKey, parseErr := c.parseMetricKey(metricKey)
		if parseErr != nil {
			c.logger.Warn().Err(parseErr).Str("metric_key", metricKey).Msg("Failed to parse metric key")
			continue
		}

		labelsMap := make(map[string]string)
		for _, label := range parsedKey.Labels {
			labelsMap[label.Name] = label.Value
		}

		protoMetric := &fodcv1.Metric{
			Name:        parsedKey.Name,
			Labels:      labelsMap,
			Value:       metricValue,
			Description: descriptions[parsedKey.Name],
		}

		protoMetrics = append(protoMetrics, protoMetric)
	}

	req := &fodcv1.StreamMetricsRequest{
		Metrics:   protoMetrics,
		Timestamp: timestamppb.Now(),
	}

	if sendErr := stream.Send(req); sendErr != nil {
		return fmt.Errorf("failed to send metrics: %w", sendErr)
	}

	return nil
}

// sendFilteredMetrics sends metrics filtered by time window.
func (c *Client) sendFilteredMetrics(
	stream fodcv1.FODCService_StreamMetricsClient,
	allMetrics map[string]*flightrecorder.MetricRingBuffer,
	timestampValues []int64,
	descriptions map[string]string,
	filter *MetricsRequestFilter,
) error {
	protoMetrics := make([]*fodcv1.Metric, 0)

	for metricKey, metricBuffer := range allMetrics {
		metricValues := metricBuffer.GetAllValues()
		if len(metricValues) == 0 {
			continue
		}

		parsedKey, parseErr := c.parseMetricKey(metricKey)
		if parseErr != nil {
			c.logger.Warn().Err(parseErr).Str("metric_key", metricKey).Msg("Failed to parse metric key")
			continue
		}

		description := descriptions[parsedKey.Name]

		minLen := len(metricValues)
		if len(timestampValues) < minLen {
			minLen = len(timestampValues)
		}

		labelsMap := make(map[string]string)
		for _, label := range parsedKey.Labels {
			labelsMap[label.Name] = label.Value
		}

		for idx := 0; idx < minLen; idx++ {
			timestampUnix := timestampValues[idx]
			timestamp := time.Unix(timestampUnix, 0)

			if filter.StartTime != nil && timestamp.Before(*filter.StartTime) {
				continue
			}
			if filter.EndTime != nil && timestamp.After(*filter.EndTime) {
				continue
			}

			protoMetric := &fodcv1.Metric{
				Name:        parsedKey.Name,
				Labels:      labelsMap,
				Value:       metricValues[idx],
				Description: description,
			}

			protoMetrics = append(protoMetrics, protoMetric)
		}
	}

	req := &fodcv1.StreamMetricsRequest{
		Metrics:   protoMetrics,
		Timestamp: timestamppb.Now(),
	}

	if sendErr := stream.Send(req); sendErr != nil {
		return fmt.Errorf("failed to send metrics: %w", sendErr)
	}

	return nil
}

// SendHeartbeat sends heartbeat to Proxy.
func (c *Client) SendHeartbeat(_ context.Context) error {
	c.mu.RLock()
	stream := c.registrationStream
	c.mu.RUnlock()

	if stream == nil {
		return fmt.Errorf("registration stream not established")
	}

	req := &fodcv1.RegisterAgentRequest{
		NodeRole: c.nodeRole,
		Labels:   c.labels,
		PrimaryAddress: &fodcv1.Address{
			Ip:   c.nodeIP,
			Port: int32(c.nodePort),
		},
	}

	if sendErr := stream.Send(req); sendErr != nil {
		return fmt.Errorf("failed to send heartbeat: %w", sendErr)
	}

	return nil
}

// Disconnect closes connection to Proxy.
func (c *Client) Disconnect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Make Disconnect idempotent - check if already disconnected
	if c.disconnected {
		return nil
	}

	c.disconnected = true
	close(c.stopCh)

	if c.heartbeatTicker != nil {
		c.heartbeatTicker.Stop()
		c.heartbeatTicker = nil
	}

	if c.registrationStream != nil {
		if closeErr := c.registrationStream.CloseSend(); closeErr != nil {
			c.logger.Warn().Err(closeErr).Msg("Error closing registration stream")
		}
		c.registrationStream = nil
	}

	if c.metricsStream != nil {
		if closeErr := c.metricsStream.CloseSend(); closeErr != nil {
			c.logger.Warn().Err(closeErr).Msg("Error closing metrics stream")
		}
		c.metricsStream = nil
	}

	if c.conn != nil {
		if closeErr := c.conn.Close(); closeErr != nil {
			c.logger.Warn().Err(closeErr).Msg("Error closing connection")
		}
		c.conn = nil
		c.client = nil
	}

	c.logger.Info().Msg("Disconnected from FODC Proxy")

	return nil
}

// Start starts the proxy client with automatic reconnection.
func (c *Client) Start(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.stopCh:
			return nil
		default:
		}

		if connectErr := c.Connect(ctx); connectErr != nil {
			c.logger.Error().Err(connectErr).Msg("Failed to connect to Proxy, retrying...")
			time.Sleep(c.reconnectInterval)
			continue
		}

		if regErr := c.StartRegistrationStream(ctx); regErr != nil {
			c.logger.Error().Err(regErr).Msg("Failed to start registration stream, reconnecting...")
			if disconnectErr := c.Disconnect(); disconnectErr != nil {
				c.logger.Warn().Err(disconnectErr).Msg("Failed to disconnect before retry")
			}
			time.Sleep(c.reconnectInterval)
			continue
		}

		if metricsErr := c.StartMetricsStream(ctx); metricsErr != nil {
			c.logger.Error().Err(metricsErr).Msg("Failed to start metrics stream, reconnecting...")
			if disconnectErr := c.Disconnect(); disconnectErr != nil {
				c.logger.Warn().Err(disconnectErr).Msg("Failed to disconnect before retry")
			}
			time.Sleep(c.reconnectInterval)
			continue
		}

		c.startHeartbeat(ctx)

		c.logger.Info().Msg("Proxy client started successfully")

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.stopCh:
			return nil
		}
	}
}

// handleRegistrationStream handles the registration stream.
func (c *Client) handleRegistrationStream(ctx context.Context, stream fodcv1.FODCService_RegisterAgentClient) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopCh:
			return
		default:
		}

		resp, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			c.logger.Info().Msg("Registration stream closed by Proxy")
			return
		}
		if recvErr != nil {
			// Check if it's a context cancellation or deadline exceeded (expected errors during cleanup)
			if errors.Is(recvErr, context.Canceled) || errors.Is(recvErr, context.DeadlineExceeded) {
				c.logger.Debug().Err(recvErr).Msg("Registration stream closed")
				return
			}
			if st, ok := status.FromError(recvErr); ok {
				// Check if it's a gRPC status error with expected codes
				code := st.Code()
				if code == codes.Canceled || code == codes.DeadlineExceeded {
					c.logger.Debug().Err(recvErr).Msg("Registration stream closed")
					return
				}
			}
			c.logger.Error().Err(recvErr).Msg("Error receiving from registration stream")
			return
		}

		if resp.HeartbeatIntervalSeconds > 0 {
			c.mu.Lock()
			c.heartbeatInterval = time.Duration(resp.HeartbeatIntervalSeconds) * time.Second
			c.mu.Unlock()

			if c.heartbeatTicker != nil {
				c.heartbeatTicker.Stop()
				c.startHeartbeat(ctx)
			}
		}
	}
}

// handleMetricsStream handles the metrics stream.
func (c *Client) handleMetricsStream(ctx context.Context, stream fodcv1.FODCService_StreamMetricsClient) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopCh:
			return
		default:
		}

		resp, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			c.logger.Info().Msg("Metrics stream closed by Proxy")
			return
		}
		if recvErr != nil {
			// Check if it's a context cancellation or deadline exceeded (expected errors during cleanup)
			if errors.Is(recvErr, context.Canceled) || errors.Is(recvErr, context.DeadlineExceeded) {
				c.logger.Debug().Err(recvErr).Msg("Metrics stream closed")
				return
			}
			if st, ok := status.FromError(recvErr); ok {
				// Check if it's a gRPC status error with expected codes
				code := st.Code()
				if code == codes.Canceled || code == codes.DeadlineExceeded {
					c.logger.Debug().Err(recvErr).Msg("Metrics stream closed")
					return
				}
			}
			c.logger.Error().Err(recvErr).Msg("Error receiving from metrics stream")
			return
		}

		filter := &MetricsRequestFilter{}
		if resp.StartTime != nil {
			startTime := resp.StartTime.AsTime()
			filter.StartTime = &startTime
		}
		if resp.EndTime != nil {
			endTime := resp.EndTime.AsTime()
			filter.EndTime = &endTime
		}

		if retrieveErr := c.RetrieveAndSendMetrics(ctx, filter); retrieveErr != nil {
			c.logger.Error().Err(retrieveErr).Msg("Failed to retrieve and send metrics")
		}
	}
}

// startHeartbeat starts the heartbeat ticker.
func (c *Client) startHeartbeat(ctx context.Context) {
	c.mu.Lock()
	if c.heartbeatTicker != nil {
		c.heartbeatTicker.Stop()
	}

	c.heartbeatTicker = time.NewTicker(c.heartbeatInterval)
	tickerCh := c.heartbeatTicker.C
	c.mu.Unlock()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-c.stopCh:
				return
			case <-tickerCh:
				if heartbeatErr := c.SendHeartbeat(ctx); heartbeatErr != nil {
					c.logger.Error().Err(heartbeatErr).Msg("Failed to send heartbeat")
				}
			}
		}
	}()
}

// parseMetricKey parses a metric key string into a MetricKey struct.
func (c *Client) parseMetricKey(key string) (metrics.MetricKey, error) {
	if !strings.Contains(key, "{") {
		return metrics.MetricKey{
			Name:   key,
			Labels: []metrics.Label{},
		}, nil
	}

	parts := strings.SplitN(key, "{", 2)
	if len(parts) != 2 {
		return metrics.MetricKey{}, fmt.Errorf("invalid metric key format: %s", key)
	}

	name := parts[0]
	labelStr := strings.TrimSuffix(parts[1], "}")
	if !strings.HasSuffix(parts[1], "}") {
		return metrics.MetricKey{}, fmt.Errorf("invalid metric key format: %s", key)
	}

	labelParts := strings.Split(labelStr, ",")
	labels := make([]metrics.Label, 0, len(labelParts))

	for _, part := range labelParts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		kvParts := strings.SplitN(part, "=", 2)
		if len(kvParts) != 2 {
			continue
		}

		keyName := strings.TrimSpace(kvParts[0])
		keyValue := strings.Trim(strings.TrimSpace(kvParts[1]), `"`)

		labels = append(labels, metrics.Label{
			Name:  keyName,
			Value: keyValue,
		})
	}

	return metrics.MetricKey{
		Name:   name,
		Labels: labels,
	}, nil
}
