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

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	fodcv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/fodc/v1"
	"github.com/apache/skywalking-banyandb/fodc/agent/internal/cluster"
	"github.com/apache/skywalking-banyandb/fodc/agent/internal/flightrecorder"
	"github.com/apache/skywalking-banyandb/fodc/agent/internal/metrics"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// MetricsRequestFilter defines filters for metrics requests.
type MetricsRequestFilter struct {
	StartTime *time.Time
	EndTime   *time.Time
}

// ClusterStateCollector represents a cluster state collector interface.
type ClusterStateCollector interface {
	Start() error
	Stop()
}

// Client manages connection and communication with the FODC Proxy.
type Client struct {
	logger             *logger.Logger
	heartbeatTicker    *time.Ticker
	flightRecorder     *flightrecorder.FlightRecorder
	connManager        *connManager
	clusterCollector   ClusterStateCollector
	clusterHandler     *cluster.Handler
	stopCh             chan struct{}
	reconnectCh        chan struct{}
	labels             map[string]string
	metricsStream      fodcv1.FODCService_StreamMetricsClient
	registrationStream fodcv1.FODCService_RegisterAgentClient
	clusterStateStream fodcv1.FODCService_StreamClusterStateClient
	client             fodcv1.FODCServiceClient

	proxyAddr      string
	nodeRole       string
	podName        string
	agentID        string
	containerNames []string

	heartbeatInterval time.Duration
	reconnectInterval time.Duration
	streamsMu         sync.RWMutex   // Protects streams only
	heartbeatWg       sync.WaitGroup // Tracks heartbeat goroutine
	disconnected      bool
}

// NewClient creates a new Client instance.
func NewClient(
	proxyAddr string,
	nodeRole string,
	podName string,
	containerNames []string,
	labels map[string]string,
	heartbeatInterval time.Duration,
	reconnectInterval time.Duration,
	flightRecorder *flightrecorder.FlightRecorder,
	logger *logger.Logger,
) *Client {
	connMgr := newConnManager(proxyAddr, reconnectInterval, logger)
	client := &Client{
		connManager:       connMgr,
		proxyAddr:         proxyAddr,
		nodeRole:          nodeRole,
		podName:           podName,
		containerNames:    containerNames,
		labels:            labels,
		heartbeatInterval: heartbeatInterval,
		reconnectInterval: reconnectInterval,
		flightRecorder:    flightRecorder,
		logger:            logger,
		stopCh:            make(chan struct{}),
		reconnectCh:       make(chan struct{}, 1),
	}

	connMgr.setHeartbeatChecker(client.SendHeartbeat)
	return client
}

// StartConnManager is useful for tests or scenarios where you want to manually control connection lifecycle.
func (c *Client) StartConnManager(ctx context.Context) {
	c.connManager.start(ctx)
}

// Connect establishes a gRPC connection to Proxy.
func (c *Client) Connect(ctx context.Context) error {
	resultCh := c.connManager.RequestConnect(ctx)
	result := <-resultCh
	if result.err != nil {
		return result.err
	}

	c.streamsMu.Lock()
	wasDisconnected := c.disconnected
	c.streamsMu.Unlock()

	if wasDisconnected {
		c.heartbeatWg.Wait()
	}

	c.streamsMu.Lock()
	c.client = fodcv1.NewFODCServiceClient(result.conn)
	if wasDisconnected {
		c.disconnected = false
		c.stopCh = make(chan struct{})
	}
	c.streamsMu.Unlock()

	return nil
}

// StartRegistrationStream establishes bi-directional registration stream with Proxy.
func (c *Client) StartRegistrationStream(ctx context.Context) error {
	c.streamsMu.Lock()
	if c.client == nil {
		c.streamsMu.Unlock()
		return fmt.Errorf("client not connected, call Connect() first")
	}
	client := c.client
	c.streamsMu.Unlock()

	stream, streamErr := client.RegisterAgent(ctx)
	if streamErr != nil {
		return fmt.Errorf("failed to create registration stream: %w", streamErr)
	}

	req := &fodcv1.RegisterAgentRequest{
		NodeRole:       c.nodeRole,
		Labels:         c.labels,
		PodName:        c.podName,
		ContainerNames: c.containerNames,
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

	c.streamsMu.Lock()
	c.registrationStream = stream
	c.agentID = resp.AgentId
	if resp.HeartbeatIntervalSeconds > 0 {
		c.heartbeatInterval = time.Duration(resp.HeartbeatIntervalSeconds) * time.Second
	}
	c.streamsMu.Unlock()

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
	c.streamsMu.Lock()
	if c.client == nil {
		c.streamsMu.Unlock()
		return fmt.Errorf("client not connected, call Connect() first")
	}
	client := c.client
	agentID := c.agentID
	c.streamsMu.Unlock()

	if agentID == "" {
		return fmt.Errorf("agent ID not available, register agent first")
	}

	md := metadata.New(map[string]string{"agent_id": agentID})
	ctxWithMetadata := metadata.NewOutgoingContext(ctx, md)

	stream, streamErr := client.StreamMetrics(ctxWithMetadata)
	if streamErr != nil {
		return fmt.Errorf("failed to create metrics stream: %w", streamErr)
	}

	c.streamsMu.Lock()
	c.metricsStream = stream
	c.streamsMu.Unlock()

	go c.handleMetricsStream(ctx, stream)

	c.logger.Info().
		Str("agent_id", agentID).
		Msg("Metrics stream established with Proxy")

	return nil
}

// StartClusterStateStream establishes bi-directional cluster state stream with Proxy.
func (c *Client) StartClusterStateStream(ctx context.Context) error {
	c.streamsMu.Lock()
	if c.client == nil {
		c.streamsMu.Unlock()
		return fmt.Errorf("client not connected, call Connect() first")
	}
	client := c.client
	agentID := c.agentID
	c.streamsMu.Unlock()

	if agentID == "" {
		return fmt.Errorf("agent ID not available, register agent first")
	}

	md := metadata.New(map[string]string{"agent_id": agentID})
	ctxWithMetadata := metadata.NewOutgoingContext(ctx, md)

	stream, streamErr := client.StreamClusterState(ctxWithMetadata)
	if streamErr != nil {
		return fmt.Errorf("failed to create cluster state stream: %w", streamErr)
	}

	c.streamsMu.Lock()
	c.clusterStateStream = stream
	c.streamsMu.Unlock()

	go c.handleClusterStateStream(ctx, stream)

	c.logger.Info().
		Str("agent_id", agentID).
		Msg("Cluster state stream established with Proxy")

	return nil
}

// sendClusterData sends cluster data to Proxy.
func (c *Client) sendClusterData() error {
	c.streamsMu.RLock()
	if c.disconnected || c.clusterStateStream == nil {
		c.streamsMu.RUnlock()
		return fmt.Errorf("cluster state stream not established")
	}
	clusterStateStream := c.clusterStateStream
	handler := c.clusterHandler
	c.streamsMu.RUnlock()
	if handler == nil {
		return fmt.Errorf("cluster handler not available")
	}
	currentNode, clusterState := handler.GetClusterData()
	req := &fodcv1.StreamClusterStateRequest{
		CurrentNode:  currentNode,
		ClusterState: clusterState,
		Timestamp:    timestamppb.Now(),
	}
	if sendErr := clusterStateStream.Send(req); sendErr != nil {
		return fmt.Errorf("failed to send cluster data: %w", sendErr)
	}
	return nil
}

// handleClusterStateStream handles the cluster state stream.
func (c *Client) handleClusterStateStream(ctx context.Context, stream fodcv1.FODCService_StreamClusterStateClient) {
	c.streamsMu.RLock()
	stopCh := c.stopCh
	c.streamsMu.RUnlock()
	for {
		select {
		case <-ctx.Done():
			return
		case <-stopCh:
			return
		default:
		}
		resp, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			c.logger.Warn().Msg("Cluster state stream closed by Proxy, reconnecting...")
			go c.reconnect(ctx)
			return
		}
		if recvErr != nil {
			c.streamsMu.RLock()
			disconnected := c.disconnected
			c.streamsMu.RUnlock()
			if disconnected {
				c.logger.Debug().Err(recvErr).Msg("Cluster state stream closed")
				return
			}
			c.logger.Error().Err(recvErr).Msg("Error receiving from cluster state stream, reconnecting...")
			go c.reconnect(ctx)
			return
		}
		if resp != nil && resp.RequestClusterData {
			c.logger.Debug().Msg("Received cluster data request from proxy")
			if sendErr := c.sendClusterData(); sendErr != nil {
				c.logger.Error().Err(sendErr).Msg("Failed to send cluster data to proxy")
			}
		}
	}
}

// RetrieveAndSendMetrics retrieves metrics from Flight Recorder when requested by Proxy.
func (c *Client) RetrieveAndSendMetrics(_ context.Context, filter *MetricsRequestFilter) error {
	c.streamsMu.RLock()
	if c.disconnected || c.metricsStream == nil {
		c.streamsMu.RUnlock()
		return fmt.Errorf("metrics stream not established")
	}
	metricsStream := c.metricsStream
	c.streamsMu.RUnlock()

	datasources := c.flightRecorder.GetDatasources()
	if len(datasources) == 0 {
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

	if filter != nil && (filter.StartTime != nil || filter.EndTime != nil) {
		if timestamps == nil {
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
		metricValue := metricBuffer.GetCurrentValue()
		allValues := metricBuffer.GetAllValues()

		if len(allValues) == 0 && metricValue == 0 {
			continue
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
				Timestamp:   timestamppb.New(timestamp),
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
	c.streamsMu.RLock()
	if c.disconnected || c.registrationStream == nil {
		c.streamsMu.RUnlock()
		return fmt.Errorf("registration stream not established")
	}
	registrationStream := c.registrationStream
	c.streamsMu.RUnlock()

	req := &fodcv1.RegisterAgentRequest{
		NodeRole:       c.nodeRole,
		Labels:         c.labels,
		PodName:        c.podName,
		ContainerNames: c.containerNames,
	}

	if sendErr := registrationStream.Send(req); sendErr != nil {
		// Check if error is due to stream being closed/disconnected
		if errors.Is(sendErr, io.EOF) || errors.Is(sendErr, context.Canceled) {
			return fmt.Errorf("registration stream closed")
		}
		if st, ok := status.FromError(sendErr); ok {
			if st.Code() == codes.Canceled {
				return fmt.Errorf("registration stream closed")
			}
		}
		return fmt.Errorf("failed to send heartbeat: %w", sendErr)
	}

	return nil
}

// cleanupStreams cleans up streams without stopping the connection manager.
func (c *Client) cleanupStreams() {
	c.streamsMu.Lock()
	if c.heartbeatTicker != nil {
		c.heartbeatTicker.Stop()
		c.heartbeatTicker = nil
	}

	oldStopCh := c.stopCh
	c.stopCh = make(chan struct{})
	c.streamsMu.Unlock()

	if oldStopCh != nil {
		select {
		case <-oldStopCh:
		default:
			close(oldStopCh)
		}
	}

	c.heartbeatWg.Wait()

	c.streamsMu.Lock()
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

	if c.clusterStateStream != nil {
		if closeErr := c.clusterStateStream.CloseSend(); closeErr != nil {
			c.logger.Warn().Err(closeErr).Msg("Error closing cluster state stream")
		}
		c.clusterStateStream = nil
	}
	c.client = nil
	c.streamsMu.Unlock()
}

// StartClusterStateCollector starts the cluster state collector if configured.
func (c *Client) StartClusterStateCollector(ctx context.Context, lifecycleAddr string, pollInterval time.Duration) error {
	if lifecycleAddr == "" {
		return nil
	}
	clusterHandler := cluster.NewHandler(c.logger)
	clusterCollector := cluster.NewCollector(clusterHandler, lifecycleAddr, pollInterval)
	if startErr := clusterCollector.Start(); startErr != nil {
		return fmt.Errorf("failed to start cluster state collector: %w", startErr)
	}
	c.streamsMu.Lock()
	c.clusterCollector = clusterCollector
	c.clusterHandler = clusterHandler
	c.streamsMu.Unlock()
	c.logger.Info().
		Str("lifecycle_addr", lifecycleAddr).
		Dur("poll_interval", pollInterval).
		Msg("Cluster state collector started")
	return nil
}

// Disconnect closes connection to Proxy.
func (c *Client) Disconnect() error {
	c.streamsMu.Lock()
	if c.disconnected {
		c.streamsMu.Unlock()
		return nil
	}

	c.disconnected = true
	close(c.stopCh)

	if c.heartbeatTicker != nil {
		c.heartbeatTicker.Stop()
		c.heartbeatTicker = nil
	}

	clusterCollector := c.clusterCollector
	c.streamsMu.Unlock()

	// Stop cluster state collector if running
	if clusterCollector != nil {
		clusterCollector.Stop()
	}

	// Wait for heartbeat goroutine to exit before closing streams
	c.heartbeatWg.Wait()

	c.streamsMu.Lock()
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

	if c.clusterStateStream != nil {
		if closeErr := c.clusterStateStream.CloseSend(); closeErr != nil {
			c.logger.Warn().Err(closeErr).Msg("Error closing cluster state stream")
		}
		c.clusterStateStream = nil
	}
	c.streamsMu.Unlock()

	c.connManager.stop()

	c.streamsMu.Lock()
	c.client = nil
	c.streamsMu.Unlock()

	c.logger.Info().Msg("Disconnected from FODC Proxy")

	return nil
}

// Start starts the proxy client with automatic reconnection.
func (c *Client) Start(ctx context.Context) error {
	c.connManager.start(ctx)

	for {
		c.streamsMu.RLock()
		stopCh := c.stopCh
		c.streamsMu.RUnlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-stopCh:
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
			c.cleanupStreams()
			time.Sleep(c.reconnectInterval)
			continue
		}

		if metricsErr := c.StartMetricsStream(ctx); metricsErr != nil {
			c.logger.Error().Err(metricsErr).Msg("Failed to start metrics stream, reconnecting...")
			c.cleanupStreams()
			time.Sleep(c.reconnectInterval)
			continue
		}

		if clusterStateErr := c.StartClusterStateStream(ctx); clusterStateErr != nil {
			c.logger.Error().Err(clusterStateErr).Msg("Failed to start cluster state stream, reconnecting...")
			c.cleanupStreams()
			time.Sleep(c.reconnectInterval)
			continue
		}

		c.logger.Info().Msg("Proxy client started successfully")
		return nil
	}
}

// handleRegistrationStream handles the registration stream.
func (c *Client) handleRegistrationStream(ctx context.Context, stream fodcv1.FODCService_RegisterAgentClient) {
	c.streamsMu.RLock()
	stopCh := c.stopCh
	c.streamsMu.RUnlock()

	for {
		select {
		case <-ctx.Done():
			return
		case <-stopCh:
			return
		default:
		}

		_, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			c.logger.Warn().Msg("Registration stream closed by Proxy, reconnecting...")
			go c.reconnect(ctx)
			return
		}
		if recvErr != nil {
			c.streamsMu.RLock()
			disconnected := c.disconnected
			c.streamsMu.RUnlock()
			if disconnected {
				c.logger.Debug().Err(recvErr).Msg("Registration stream closed")
				return
			}
			c.logger.Error().Err(recvErr).Msg("Error receiving from registration stream, reconnecting...")
			go c.reconnect(ctx)
			return
		}
	}
}

// handleMetricsStream handles the metrics stream.
func (c *Client) handleMetricsStream(ctx context.Context, stream fodcv1.FODCService_StreamMetricsClient) {
	c.streamsMu.RLock()
	stopCh := c.stopCh
	c.streamsMu.RUnlock()

	for {
		select {
		case <-ctx.Done():
			return
		case <-stopCh:
			return
		default:
		}

		resp, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			c.logger.Warn().Msg("Metrics stream closed by Proxy, reconnecting...")
			go c.reconnect(ctx)
			return
		}
		if recvErr != nil {
			c.streamsMu.RLock()
			disconnected := c.disconnected
			c.streamsMu.RUnlock()
			if disconnected {
				c.logger.Debug().Err(recvErr).Msg("Metrics stream closed")
				return
			}
			c.logger.Error().Err(recvErr).Msg("Error receiving from metrics stream, reconnecting...")
			go c.reconnect(ctx)
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

// reconnect uses a buffered channel to ensure only one reconnect goroutine runs at a time.
func (c *Client) reconnect(ctx context.Context) {
	select {
	case <-ctx.Done():
		c.logger.Debug().Msg("Context canceled, skipping reconnection...")
		return
	case c.reconnectCh <- struct{}{}:
		// Acquired the slot, proceed with reconnection
	default:
		c.logger.Debug().Msg("Reconnection already in progress, skipping...")
		return
	}

	defer func() {
		<-c.reconnectCh
	}()

	c.streamsMu.Lock()
	if c.disconnected {
		c.streamsMu.Unlock()
		c.logger.Warn().Msg("Already disconnected intentionally, skipping reconnection...")
		return
	}

	c.logger.Info().Msg("Starting reconnection process...")

	if c.heartbeatTicker != nil {
		c.heartbeatTicker.Stop()
		c.heartbeatTicker = nil
	}

	c.streamsMu.Unlock()

	c.heartbeatWg.Wait()
	c.streamsMu.Lock()

	if !c.disconnected {
		close(c.stopCh)
	}
	c.stopCh = make(chan struct{})
	if c.registrationStream != nil {
		_ = c.registrationStream.CloseSend()
		c.registrationStream = nil
	}
	if c.metricsStream != nil {
		_ = c.metricsStream.CloseSend()
		c.metricsStream = nil
	}
	if c.clusterStateStream != nil {
		_ = c.clusterStateStream.CloseSend()
		c.clusterStateStream = nil
	}
	c.streamsMu.Unlock()

	connResultCh := c.connManager.RequestConnect(ctx)
	var connResult connResult
	select {
	case <-ctx.Done():
		c.logger.Warn().Msg("Context canceled during reconnection")
		return
	case connResult = <-connResultCh:
	}

	if connResult.err != nil {
		c.logger.Error().Err(connResult.err).Msg("Failed to reconnect to Proxy")
		if disconnectErr := c.Disconnect(); disconnectErr != nil {
			c.logger.Warn().Err(disconnectErr).Msg("Failed to disconnect")
		}
		return
	}

	if connResult.conn != nil {
		c.streamsMu.Lock()
		c.client = fodcv1.NewFODCServiceClient(connResult.conn)
		c.streamsMu.Unlock()
	}

	if regErr := c.StartRegistrationStream(ctx); regErr != nil {
		c.logger.Error().Err(regErr).Msg("Failed to restart registration stream")
		return
	}

	if metricsErr := c.StartMetricsStream(ctx); metricsErr != nil {
		c.logger.Error().Err(metricsErr).Msg("Failed to restart metrics stream")
		return
	}

	if clusterStateErr := c.StartClusterStateStream(ctx); clusterStateErr != nil {
		c.logger.Error().Err(clusterStateErr).Msg("Failed to restart cluster state stream")
		return
	}

	c.logger.Info().Msg("Successfully reconnected to Proxy")
}

// startHeartbeat starts the heartbeat ticker.
func (c *Client) startHeartbeat(ctx context.Context) {
	c.streamsMu.Lock()
	if c.heartbeatTicker != nil {
		c.heartbeatTicker.Stop()
	}

	c.heartbeatTicker = time.NewTicker(c.heartbeatInterval)
	tickerCh := c.heartbeatTicker.C
	stopCh := c.stopCh
	c.heartbeatWg.Add(1)
	c.streamsMu.Unlock()

	go func() {
		defer c.heartbeatWg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case <-stopCh:
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
