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

// Package metrics provides functionality for aggregating and enriching metrics from all agents.
package metrics

import (
	"context"
	"fmt"
	"sync"
	"time"

	fodcv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/fodc/v1"
	"github.com/apache/skywalking-banyandb/fodc/proxy/internal/registry"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// AggregatedMetric represents an aggregated metric with node metadata.
type AggregatedMetric struct {
	Labels      map[string]string
	Timestamp   time.Time
	Name        string
	AgentID     string
	NodeRole    string
	Description string
	Value       float64
}

// Filter defines filters for metrics collection.
type Filter struct {
	StartTime *time.Time
	EndTime   *time.Time
	Role      string
	Address   string
	AgentIDs  []string
}

// Aggregator aggregates and enriches metrics from all agents.
type Aggregator struct {
	registry     *registry.AgentRegistry
	logger       *logger.Logger
	grpcService  RequestSender
	metricsCh    chan *AggregatedMetric
	collecting   map[string]chan []*AggregatedMetric
	mu           sync.RWMutex
	collectingMu sync.RWMutex
}

// RequestSender is an interface for sending metrics requests to agents.
type RequestSender interface {
	RequestMetrics(ctx context.Context, agentID string, startTime, endTime *time.Time) error
}

// NewAggregator creates a new MetricsAggregator instance.
func NewAggregator(registry *registry.AgentRegistry, grpcService RequestSender, logger *logger.Logger) *Aggregator {
	return &Aggregator{
		registry:    registry,
		grpcService: grpcService,
		logger:      logger,
		metricsCh:   make(chan *AggregatedMetric, 1000),
		collecting:  make(map[string]chan []*AggregatedMetric),
	}
}

// SetGRPCService sets the gRPC service for sending metrics requests.
func (ma *Aggregator) SetGRPCService(grpcService RequestSender) {
	ma.mu.Lock()
	defer ma.mu.Unlock()
	ma.grpcService = grpcService
}

// ProcessMetricsFromAgent processes metrics received from an agent.
func (ma *Aggregator) ProcessMetricsFromAgent(ctx context.Context, agentID string, agentInfo *registry.AgentInfo, req *fodcv1.StreamMetricsRequest) error {
	aggregatedMetrics := make([]*AggregatedMetric, 0, len(req.Metrics))

	for _, metric := range req.Metrics {
		labels := make(map[string]string)
		for key, value := range metric.Labels {
			labels[key] = value
		}

		labels["agent_id"] = agentID
		labels["node_role"] = agentInfo.NodeRole
		labels["ip"] = agentInfo.PrimaryAddress.IP
		labels["port"] = fmt.Sprintf("%d", agentInfo.PrimaryAddress.Port)

		for key, value := range agentInfo.Labels {
			labels[key] = value
		}

		var timestamp time.Time
		if req.Timestamp != nil {
			timestamp = req.Timestamp.AsTime()
		} else {
			timestamp = time.Now()
		}

		aggregatedMetric := &AggregatedMetric{
			Name:        metric.Name,
			Labels:      labels,
			Value:       metric.Value,
			Timestamp:   timestamp,
			AgentID:     agentID,
			NodeRole:    agentInfo.NodeRole,
			Description: metric.Description,
		}

		aggregatedMetrics = append(aggregatedMetrics, aggregatedMetric)
	}

	ma.collectingMu.RLock()
	collectCh, exists := ma.collecting[agentID]
	ma.collectingMu.RUnlock()

	if exists {
		select {
		case collectCh <- aggregatedMetrics:
		case <-ctx.Done():
			return ctx.Err()
		default:
			ma.logger.Warn().Str("agent_id", agentID).Msg("Metrics collection channel full, dropping metrics")
		}
	}

	return nil
}

// CollectMetricsFromAgents requests metrics from all agents (or filtered agents) when external client queries.
func (ma *Aggregator) CollectMetricsFromAgents(ctx context.Context, filter *Filter) ([]*AggregatedMetric, error) {
	agents := ma.getFilteredAgents(filter)
	if len(agents) == 0 {
		return []*AggregatedMetric{}, nil
	}

	collectChs := make(map[string]chan []*AggregatedMetric)
	ma.collectingMu.Lock()
	for _, agentInfo := range agents {
		collectCh := make(chan []*AggregatedMetric, 1)
		collectChs[agentInfo.AgentID] = collectCh
		ma.collecting[agentInfo.AgentID] = collectCh
	}
	ma.collectingMu.Unlock()

	defer func() {
		ma.collectingMu.Lock()
		for agentID := range collectChs {
			delete(ma.collecting, agentID)
		}
		ma.collectingMu.Unlock()
	}()

	for _, agentInfo := range agents {
		requestErr := ma.grpcService.RequestMetrics(ctx, agentInfo.AgentID, filter.StartTime, filter.EndTime)
		if requestErr != nil {
			ma.logger.Error().
				Err(requestErr).
				Str("agent_id", agentInfo.AgentID).
				Msg("Failed to request metrics from agent")
			delete(collectChs, agentInfo.AgentID)
		}
	}

	allMetrics := make([]*AggregatedMetric, 0)
	timeout := 10 * time.Second
	if filter.StartTime != nil && filter.EndTime != nil {
		timeout = filter.EndTime.Sub(*filter.StartTime) + 5*time.Second
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	for agentID, collectCh := range collectChs {
		select {
		case <-timeoutCtx.Done():
			ma.logger.Warn().
				Str("agent_id", agentID).
				Msg("Timeout waiting for metrics from agent")
		case metrics := <-collectCh:
			allMetrics = append(allMetrics, metrics...)
		}
	}

	return allMetrics, nil
}

// GetLatestMetrics triggers on-demand collection from all agents.
func (ma *Aggregator) GetLatestMetrics(ctx context.Context) ([]*AggregatedMetric, error) {
	filter := &Filter{}
	return ma.CollectMetricsFromAgents(ctx, filter)
}

// GetMetricsWindow triggers on-demand collection from all agents with time window filter.
func (ma *Aggregator) GetMetricsWindow(ctx context.Context, startTime, endTime time.Time, filter *Filter) ([]*AggregatedMetric, error) {
	if filter == nil {
		filter = &Filter{}
	}
	filter.StartTime = &startTime
	filter.EndTime = &endTime
	return ma.CollectMetricsFromAgents(ctx, filter)
}

// getFilteredAgents returns agents filtered by the provided filter.
func (ma *Aggregator) getFilteredAgents(filter *Filter) []*registry.AgentInfo {
	if filter == nil {
		return ma.registry.ListAgents()
	}

	var agents []*registry.AgentInfo

	switch {
	case len(filter.AgentIDs) > 0:
		agents = make([]*registry.AgentInfo, 0, len(filter.AgentIDs))
		for _, agentID := range filter.AgentIDs {
			agentInfo, getErr := ma.registry.GetAgentByID(agentID)
			if getErr == nil {
				agents = append(agents, agentInfo)
			}
		}
	case filter.Role != "":
		agents = ma.registry.ListAgentsByRole(filter.Role)
	default:
		agents = ma.registry.ListAgents()
	}

	if filter.Address != "" {
		filteredAgents := make([]*registry.AgentInfo, 0)
		for _, agentInfo := range agents {
			if ma.matchesAddress(agentInfo, filter.Address) {
				filteredAgents = append(filteredAgents, agentInfo)
			}
		}
		agents = filteredAgents
	}

	return agents
}

// matchesAddress checks if an agent matches the address filter.
func (ma *Aggregator) matchesAddress(agentInfo *registry.AgentInfo, address string) bool {
	ipPort := fmt.Sprintf("%s:%d", agentInfo.PrimaryAddress.IP, agentInfo.PrimaryAddress.Port)
	if ipPort == address {
		return true
	}
	if agentInfo.PrimaryAddress.IP == address {
		return true
	}
	return false
}
