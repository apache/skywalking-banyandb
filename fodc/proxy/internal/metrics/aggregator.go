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
	"maps"
	"strings"
	"sync"
	"time"

	fodcv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/fodc/v1"
	"github.com/apache/skywalking-banyandb/fodc/proxy/internal/registry"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

const (
	// defaultCollectionTimeout is the default timeout for collecting metrics from agents.
	defaultCollectionTimeout = 10 * time.Second
	// maxCollectionTimeout is the maximum timeout allowed for collecting metrics,
	// preventing excessively long waits for wide time windows.
	maxCollectionTimeout = 5 * time.Minute
	// podNameLabelName and containerNameLabelName are first-class node identity labels that are
	// already stamped per metric, so they are not re-applied as namespaced node labels.
	podNameLabelName       = "pod_name"
	containerNameLabelName = "container_name"
	// nodeLabelPrefix namespaces a node's own labels (e.g. the data-node tier "type" becomes
	// "node_type") so they can never collide with a metric-intrinsic label of the same name.
	nodeLabelPrefix = "node_"
)

// AggregatedMetric represents an aggregated metric with node metadata.
type AggregatedMetric struct {
	Labels      map[string]string
	Timestamp   time.Time
	Name        string
	AgentID     string
	Description string
	Type        string // lowercase prometheus type: "counter","gauge","histogram","summary","untyped",""
	Value       float64
}

// Filter defines filters for metrics collection.
type Filter struct {
	StartTime *time.Time
	EndTime   *time.Time
	Role      string
	PodName   string
	AgentIDs  []string
}

// agentSubscription is one collection's claim on an agent's next metrics push.
type agentSubscription struct {
	collectCh chan []*AggregatedMetric
	subID     uint64
}

// Aggregator aggregates and enriches metrics from all agents.
type Aggregator struct {
	registry     *registry.AgentRegistry
	logger       *logger.Logger
	grpcService  RequestSender
	collecting   map[string]map[uint64]chan []*AggregatedMetric
	nextSubID    uint64
	mu           sync.RWMutex
	collectingMu sync.RWMutex
}

// RequestSender is an interface for sending metrics requests to agents.
type RequestSender interface {
	RequestMetrics(agentID string, startTime, endTime *time.Time) error
}

// NewAggregator creates a new MetricsAggregator instance.
func NewAggregator(registry *registry.AgentRegistry, grpcService RequestSender, logger *logger.Logger) *Aggregator {
	return &Aggregator{
		registry:    registry,
		grpcService: grpcService,
		logger:      logger,
		collecting:  make(map[string]map[uint64]chan []*AggregatedMetric),
	}
}

// SetGRPCService sets the gRPC service for sending metrics requests.
func (ma *Aggregator) SetGRPCService(grpcService RequestSender) {
	ma.mu.Lock()
	defer ma.mu.Unlock()
	ma.grpcService = grpcService
}

// protoMetricTypeToString converts a proto MetricType enum to its lowercase string representation.
// UNSPECIFIED maps to "" (unknown, causes proxy to fall back to suffix heuristic).
func protoMetricTypeToString(mt fodcv1.MetricType) string {
	switch mt {
	case fodcv1.MetricType_METRIC_TYPE_COUNTER:
		return "counter"
	case fodcv1.MetricType_METRIC_TYPE_GAUGE:
		return "gauge"
	case fodcv1.MetricType_METRIC_TYPE_HISTOGRAM:
		return "histogram"
	case fodcv1.MetricType_METRIC_TYPE_SUMMARY:
		return "summary"
	case fodcv1.MetricType_METRIC_TYPE_UNTYPED:
		return "untyped"
	default:
		return ""
	}
}

// ProcessMetricsFromAgent processes metrics received from an agent.
func (ma *Aggregator) ProcessMetricsFromAgent(ctx context.Context, agentID string, agentInfo *registry.AgentInfo, req *fodcv1.StreamMetricsRequest) error {
	aggregatedMetrics := make([]*AggregatedMetric, 0, len(req.Metrics))

	for _, metric := range req.Metrics {
		labels := make(map[string]string, len(metric.Labels))
		maps.Copy(labels, metric.Labels)

		// Overlay the agent's node labels under a "node_" prefix so they can never collide
		// with a metric-intrinsic label of the same name (e.g. the merge "type"). pod_name and
		// container_name are already first-class labels and are skipped. A namespaced label
		// already present (stamped per metric by the agent) is left untouched.
		for key, value := range agentInfo.Labels {
			if value == "" || key == podNameLabelName || key == containerNameLabelName {
				continue
			}
			prefixed := nodeLabelPrefix + key
			if _, exists := labels[prefixed]; !exists {
				labels[prefixed] = value
			}
		}

		var timestamp time.Time
		switch {
		case metric.Timestamp != nil:
			timestamp = metric.Timestamp.AsTime()
		case req.Timestamp != nil:
			timestamp = req.Timestamp.AsTime()
		default:
			timestamp = time.Now()
		}

		aggregatedMetric := &AggregatedMetric{
			Name:        metric.Name,
			Labels:      labels,
			Value:       metric.Value,
			Timestamp:   timestamp,
			AgentID:     agentID,
			Description: metric.Description,
			Type:        protoMetricTypeToString(metric.Type),
		}

		aggregatedMetrics = append(aggregatedMetrics, aggregatedMetric)
	}

	// Deliver to every collection waiting on this agent. Holding the read lock keeps
	// unsubscribe (which closes the channel) from running between the lookup and the
	// send, so this can never send on a closed channel.
	ma.collectingMu.RLock()
	defer ma.collectingMu.RUnlock()

	subscribers := ma.collecting[agentID]
	if len(subscribers) == 0 {
		// Expected whenever scrapes overlap: each one asks the agent separately, so the
		// second reply arrives after the first has already satisfied every subscriber.
		ma.logger.Debug().Str("agent_id", agentID).Msg("Metrics collection channel not found, dropping metrics")
		return nil
	}

	// Every subscriber gets the same slice; consumers must treat it as read-only.
	// No send can block - each channel is buffered and the select has a default - so the
	// loop always runs to completion. Bailing out on a canceled context part-way through
	// would strand the subscribers not yet visited, and each of those collections would
	// then wait out its whole timeout for data that had already arrived.
	for _, collectCh := range subscribers {
		select {
		case collectCh <- aggregatedMetrics:
		default:
			ma.logger.Warn().Str("agent_id", agentID).Msg("Metrics collection channel full, dropping metrics")
		}
	}

	return ctx.Err()
}

// subscribe registers a channel to receive this agent's next metrics push and returns
// the subscription ID needed to release it. Each collection subscribes separately, so a
// scrape that starts while another is in flight can never take over - or, on cleanup,
// close - the other one's channel.
func (ma *Aggregator) subscribe(agentID string) (uint64, chan []*AggregatedMetric) {
	collectCh := make(chan []*AggregatedMetric, 1)

	ma.collectingMu.Lock()
	defer ma.collectingMu.Unlock()

	ma.nextSubID++
	subID := ma.nextSubID
	subscribers, exists := ma.collecting[agentID]
	if !exists {
		subscribers = make(map[uint64]chan []*AggregatedMetric)
		ma.collecting[agentID] = subscribers
	}
	subscribers[subID] = collectCh

	return subID, collectCh
}

// unsubscribe releases one subscription and closes its channel. It is a no-op if the
// subscription is already gone.
func (ma *Aggregator) unsubscribe(agentID string, subID uint64) {
	ma.collectingMu.Lock()
	defer ma.collectingMu.Unlock()

	subscribers, exists := ma.collecting[agentID]
	if !exists {
		return
	}
	if collectCh, ok := subscribers[subID]; ok {
		delete(subscribers, subID)
		close(collectCh)
	}
	if len(subscribers) == 0 {
		delete(ma.collecting, agentID)
	}
}

// CollectMetricsFromAgents requests metrics from all agents (or filtered agents) when external client queries.
func (ma *Aggregator) CollectMetricsFromAgents(ctx context.Context, filter *Filter) ([]*AggregatedMetric, error) {
	agents := ma.getFilteredAgents(filter)
	if len(agents) == 0 {
		return []*AggregatedMetric{}, nil
	}

	agents = dedupeAgents(agents)

	subscriptions := make(map[string]agentSubscription, len(agents))
	for _, agentInfo := range agents {
		subID, collectCh := ma.subscribe(agentInfo.AgentID)
		subscriptions[agentInfo.AgentID] = agentSubscription{subID: subID, collectCh: collectCh}
	}

	defer func() {
		for agentID, sub := range subscriptions {
			ma.unsubscribe(agentID, sub.subID)
		}
	}()

	for _, agentInfo := range agents {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		requestErr := ma.grpcService.RequestMetrics(agentInfo.AgentID, filter.StartTime, filter.EndTime)
		if requestErr != nil {
			ma.logger.Error().
				Err(requestErr).
				Str("agent_id", agentInfo.AgentID).
				Msg("Failed to request metrics from agent")
			ma.unsubscribe(agentInfo.AgentID, subscriptions[agentInfo.AgentID].subID)
			delete(subscriptions, agentInfo.AgentID)
		}
	}

	timeout := defaultCollectionTimeout
	if filter.StartTime != nil && filter.EndTime != nil {
		windowDuration := filter.EndTime.Sub(*filter.StartTime) + 5*time.Second
		if windowDuration < maxCollectionTimeout {
			timeout = windowDuration
		} else {
			timeout = maxCollectionTimeout
		}
	}

	allMetrics := make([]*AggregatedMetric, 0)
	var metricsMu sync.Mutex
	var wg sync.WaitGroup

	for agentID, sub := range subscriptions {
		wg.Add(1)
		go func(id string, ch chan []*AggregatedMetric) {
			defer wg.Done()
			agentCtx, agentCancel := context.WithTimeout(ctx, timeout)
			defer agentCancel()

			select {
			case <-agentCtx.Done():
				ma.logger.Warn().
					Str("agent_id", id).
					Msg("Timeout waiting for metrics from agent")
			case metrics := <-ch:
				metricsMu.Lock()
				allMetrics = append(allMetrics, metrics...)
				metricsMu.Unlock()
			}
		}(agentID, sub.collectCh)
	}

	wg.Wait()
	return allMetrics, nil
}

// ActiveCollections returns the number of agents currently being collected.
func (ma *Aggregator) ActiveCollections() int {
	ma.collectingMu.RLock()
	defer ma.collectingMu.RUnlock()
	return len(ma.collecting)
}

// GetLatestMetrics triggers on-demand collection from all agents.
func (ma *Aggregator) GetLatestMetrics(ctx context.Context, filter *Filter) ([]*AggregatedMetric, error) {
	if filter == nil {
		filter = &Filter{}
	}
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

// dedupeAgents drops repeated agents so each one is subscribed to, asked and cleaned up
// exactly once. Filter.AgentIDs is caller-supplied and may name the same agent twice; a
// second subscription for it would be stranded forever, because only one subscription per
// agent is tracked for cleanup.
func dedupeAgents(agents []*registry.AgentInfo) []*registry.AgentInfo {
	seen := make(map[string]struct{}, len(agents))
	deduped := make([]*registry.AgentInfo, 0, len(agents))
	for _, agentInfo := range agents {
		if _, ok := seen[agentInfo.AgentID]; ok {
			continue
		}
		seen[agentInfo.AgentID] = struct{}{}
		deduped = append(deduped, agentInfo)
	}
	return deduped
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

	if filter.PodName != "" {
		filteredAgents := make([]*registry.AgentInfo, 0)
		for _, agentInfo := range agents {
			if strings.EqualFold(agentInfo.AgentIdentity.PodName, filter.PodName) {
				filteredAgents = append(filteredAgents, agentInfo)
			}
		}
		agents = filteredAgents
	}

	return agents
}
