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

package proxy

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"

	fodcv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/fodc/v1"
	"github.com/apache/skywalking-banyandb/fodc/internal/flightrecorder"
	"github.com/apache/skywalking-banyandb/fodc/internal/metrics"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// MetricsRequestFilter defines filters for metrics requests.
type MetricsRequestFilter struct {
	StartTime *time.Time
	EndTime   *time.Time
}

// ProxyClient manages connection and communication with the FODC Proxy.
type ProxyClient struct {
	proxyAddr          string
	nodeIP             string
	nodePort           int
	nodeRole           string
	labels             map[string]string
	conn               *grpc.ClientConn
	client             fodcv1.FODCServiceClient
	heartbeatTicker    *time.Ticker
	heartbeatInterval  time.Duration
	reconnectInterval  time.Duration
	flightRecorder     *flightrecorder.FlightRecorder
	mu                 sync.RWMutex
	logger             *logger.Logger
	stopCh             chan struct{}
	registrationStream fodcv1.FODCService_RegisterAgentClient
	metricsStream      fodcv1.FODCService_StreamMetricsClient
}

// NewProxyClient creates a new ProxyClient instance.
func NewProxyClient(
	proxyAddr string,
	nodeIP string,
	nodePort int,
	nodeRole string,
	labels map[string]string,
	heartbeatInterval time.Duration,
	reconnectInterval time.Duration,
	flightRecorder *flightrecorder.FlightRecorder,
	logger *logger.Logger,
) *ProxyClient {
	return &ProxyClient{
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
func (pc *ProxyClient) Connect(ctx context.Context) error {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if pc.conn != nil {
		return nil
	}

	conn, dialErr := grpc.NewClient(pc.proxyAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if dialErr != nil {
		return fmt.Errorf("failed to create proxy client: %w", dialErr)
	}

	pc.conn = conn
	pc.client = fodcv1.NewFODCServiceClient(conn)

	pc.logger.Info().
		Str("proxy_addr", pc.proxyAddr).
		Msg("Connected to FODC Proxy")

	return nil
}

// StartRegistrationStream establishes bi-directional registration stream with Proxy.
func (pc *ProxyClient) StartRegistrationStream(ctx context.Context) error {
	pc.mu.Lock()
	if pc.client == nil {
		pc.mu.Unlock()
		return fmt.Errorf("client not connected, call Connect() first")
	}
	client := pc.client
	pc.mu.Unlock()

	stream, streamErr := client.RegisterAgent(ctx)
	if streamErr != nil {
		return fmt.Errorf("failed to create registration stream: %w", streamErr)
	}

	pc.mu.Lock()
	pc.registrationStream = stream
	pc.mu.Unlock()

	req := &fodcv1.RegisterAgentRequest{
		NodeRole: pc.nodeRole,
		Labels:   pc.labels,
		PrimaryAddress: &fodcv1.Address{
			Ip:   pc.nodeIP,
			Port: int32(pc.nodePort),
		},
		SecondaryAddresses: make(map[string]*fodcv1.Address),
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

	if resp.HeartbeatIntervalSeconds > 0 {
		pc.mu.Lock()
		pc.heartbeatInterval = time.Duration(resp.HeartbeatIntervalSeconds) * time.Second
		pc.mu.Unlock()
	}

	pc.logger.Info().
		Str("proxy_addr", pc.proxyAddr).
		Dur("heartbeat_interval", pc.heartbeatInterval).
		Msg("Agent registered with Proxy")

	go pc.handleRegistrationStream(ctx, stream)

	return nil
}

// StartMetricsStream establishes bi-directional metrics stream with Proxy.
func (pc *ProxyClient) StartMetricsStream(ctx context.Context) error {
	pc.mu.Lock()
	if pc.client == nil {
		pc.mu.Unlock()
		return fmt.Errorf("client not connected, call Connect() first")
	}
	client := pc.client
	pc.mu.Unlock()

	stream, streamErr := client.StreamMetrics(ctx)
	if streamErr != nil {
		return fmt.Errorf("failed to create metrics stream: %w", streamErr)
	}

	pc.mu.Lock()
	pc.metricsStream = stream
	pc.mu.Unlock()

	go pc.handleMetricsStream(ctx, stream)

	pc.logger.Info().Msg("Metrics stream established with Proxy")

	return nil
}

// RetrieveAndSendMetrics retrieves metrics from Flight Recorder when requested by Proxy.
func (pc *ProxyClient) RetrieveAndSendMetrics(ctx context.Context, filter *MetricsRequestFilter) error {
	datasources := pc.flightRecorder.GetDatasources()
	if len(datasources) == 0 {
		return nil
	}

	ds := datasources[0]
	allMetrics := ds.GetMetrics()
	timestamps := ds.GetTimestamps()
	descriptions := ds.GetDescriptions()

	if timestamps == nil {
		return nil
	}

	timestampValues := timestamps.GetAllValues()
	if len(timestampValues) == 0 {
		return nil
	}

	pc.mu.RLock()
	metricsStream := pc.metricsStream
	pc.mu.RUnlock()

	if metricsStream == nil {
		return fmt.Errorf("metrics stream not established")
	}

	if filter == nil || (filter.StartTime == nil && filter.EndTime == nil) {
		return pc.sendLatestMetrics(metricsStream, allMetrics, descriptions)
	}

	return pc.sendFilteredMetrics(metricsStream, allMetrics, timestampValues, descriptions, filter)
}

// sendLatestMetrics sends the latest metrics (most recent values).
func (pc *ProxyClient) sendLatestMetrics(
	stream fodcv1.FODCService_StreamMetricsClient,
	allMetrics map[string]*flightrecorder.MetricRingBuffer,
	descriptions map[string]string,
) error {
	protoMetrics := make([]*fodcv1.Metric, 0)

	for metricKey, metricBuffer := range allMetrics {
		metricValue := metricBuffer.GetCurrentValue()
		if metricValue == 0 && len(metricBuffer.GetAllValues()) == 0 {
			continue
		}

		parsedKey, parseErr := pc.parseMetricKey(metricKey)
		if parseErr != nil {
			pc.logger.Warn().Err(parseErr).Str("metric_key", metricKey).Msg("Failed to parse metric key")
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

	if len(protoMetrics) == 0 {
		return nil
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
func (pc *ProxyClient) sendFilteredMetrics(
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

		parsedKey, parseErr := pc.parseMetricKey(metricKey)
		if parseErr != nil {
			pc.logger.Warn().Err(parseErr).Str("metric_key", metricKey).Msg("Failed to parse metric key")
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

	if len(protoMetrics) == 0 {
		return nil
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
func (pc *ProxyClient) SendHeartbeat(ctx context.Context) error {
	pc.mu.RLock()
	stream := pc.registrationStream
	pc.mu.RUnlock()

	if stream == nil {
		return fmt.Errorf("registration stream not established")
	}

	req := &fodcv1.RegisterAgentRequest{
		NodeRole: pc.nodeRole,
		Labels:   pc.labels,
		PrimaryAddress: &fodcv1.Address{
			Ip:   pc.nodeIP,
			Port: int32(pc.nodePort),
		},
	}

	if sendErr := stream.Send(req); sendErr != nil {
		return fmt.Errorf("failed to send heartbeat: %w", sendErr)
	}

	return nil
}

// Disconnect closes connection to Proxy.
func (pc *ProxyClient) Disconnect() error {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	close(pc.stopCh)

	if pc.heartbeatTicker != nil {
		pc.heartbeatTicker.Stop()
		pc.heartbeatTicker = nil
	}

	if pc.registrationStream != nil {
		if closeErr := pc.registrationStream.CloseSend(); closeErr != nil {
			pc.logger.Warn().Err(closeErr).Msg("Error closing registration stream")
		}
		pc.registrationStream = nil
	}

	if pc.metricsStream != nil {
		if closeErr := pc.metricsStream.CloseSend(); closeErr != nil {
			pc.logger.Warn().Err(closeErr).Msg("Error closing metrics stream")
		}
		pc.metricsStream = nil
	}

	if pc.conn != nil {
		if closeErr := pc.conn.Close(); closeErr != nil {
			pc.logger.Warn().Err(closeErr).Msg("Error closing connection")
		}
		pc.conn = nil
		pc.client = nil
	}

	pc.logger.Info().Msg("Disconnected from FODC Proxy")

	return nil
}

// Start starts the proxy client with automatic reconnection.
func (pc *ProxyClient) Start(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-pc.stopCh:
			return nil
		default:
		}

		if connectErr := pc.Connect(ctx); connectErr != nil {
			pc.logger.Error().Err(connectErr).Msg("Failed to connect to Proxy, retrying...")
			time.Sleep(pc.reconnectInterval)
			continue
		}

		if regErr := pc.StartRegistrationStream(ctx); regErr != nil {
			pc.logger.Error().Err(regErr).Msg("Failed to start registration stream, reconnecting...")
			pc.Disconnect()
			time.Sleep(pc.reconnectInterval)
			continue
		}

		if metricsErr := pc.StartMetricsStream(ctx); metricsErr != nil {
			pc.logger.Error().Err(metricsErr).Msg("Failed to start metrics stream, reconnecting...")
			pc.Disconnect()
			time.Sleep(pc.reconnectInterval)
			continue
		}

		pc.startHeartbeat(ctx)

		pc.logger.Info().Msg("Proxy client started successfully")

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-pc.stopCh:
			return nil
		}
	}
}

// handleRegistrationStream handles the registration stream.
func (pc *ProxyClient) handleRegistrationStream(ctx context.Context, stream fodcv1.FODCService_RegisterAgentClient) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-pc.stopCh:
			return
		default:
		}

		resp, recvErr := stream.Recv()
		if recvErr == io.EOF {
			pc.logger.Info().Msg("Registration stream closed by Proxy")
			return
		}
		if recvErr != nil {
			pc.logger.Error().Err(recvErr).Msg("Error receiving from registration stream")
			return
		}

		if resp.HeartbeatIntervalSeconds > 0 {
			pc.mu.Lock()
			pc.heartbeatInterval = time.Duration(resp.HeartbeatIntervalSeconds) * time.Second
			pc.mu.Unlock()

			if pc.heartbeatTicker != nil {
				pc.heartbeatTicker.Stop()
				pc.startHeartbeat(ctx)
			}
		}
	}
}

// handleMetricsStream handles the metrics stream.
func (pc *ProxyClient) handleMetricsStream(ctx context.Context, stream fodcv1.FODCService_StreamMetricsClient) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-pc.stopCh:
			return
		default:
		}

		resp, recvErr := stream.Recv()
		if recvErr == io.EOF {
			pc.logger.Info().Msg("Metrics stream closed by Proxy")
			return
		}
		if recvErr != nil {
			pc.logger.Error().Err(recvErr).Msg("Error receiving from metrics stream")
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

		if retrieveErr := pc.RetrieveAndSendMetrics(ctx, filter); retrieveErr != nil {
			pc.logger.Error().Err(retrieveErr).Msg("Failed to retrieve and send metrics")
		}
	}
}

// startHeartbeat starts the heartbeat ticker.
func (pc *ProxyClient) startHeartbeat(ctx context.Context) {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if pc.heartbeatTicker != nil {
		pc.heartbeatTicker.Stop()
	}

	pc.heartbeatTicker = time.NewTicker(pc.heartbeatInterval)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-pc.stopCh:
				return
			case <-pc.heartbeatTicker.C:
				if heartbeatErr := pc.SendHeartbeat(ctx); heartbeatErr != nil {
					pc.logger.Error().Err(heartbeatErr).Msg("Failed to send heartbeat")
				}
			}
		}
	}()
}

// parseMetricKey parses a metric key string into a MetricKey struct.
func (pc *ProxyClient) parseMetricKey(key string) (metrics.MetricKey, error) {
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

// ParseLabels parses comma-separated key=value pairs into a map.
func ParseLabels(labelsStr string) map[string]string {
	labels := make(map[string]string)
	if labelsStr == "" {
		return labels
	}

	pairs := strings.Split(labelsStr, ",")
	for _, pair := range pairs {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}

		kv := strings.SplitN(pair, "=", 2)
		if len(kv) != 2 {
			continue
		}

		key := strings.TrimSpace(kv[0])
		value := strings.TrimSpace(kv[1])
		if key != "" && value != "" {
			labels[key] = value
		}
	}

	return labels
}
