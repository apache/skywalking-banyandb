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

// Package lifecycle provides lifecycle state management for FODC proxy.
package lifecycle

import (
	"context"
	"sync"
	"time"

	fodcv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/fodc/v1"
	"github.com/apache/skywalking-banyandb/fodc/proxy/internal/registry"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

const defaultCollectionTimeout = 10 * time.Second

// PodLifecycleStatus represents lifecycle status for a single pod.
type PodLifecycleStatus struct {
	PodName string                    `json:"pod_name,omitempty"`
	Reports []*fodcv1.LifecycleReport `json:"reports,omitempty"`
}

// InspectionResult is the aggregated result from agents and liaison.
type InspectionResult struct {
	Groups            []*fodcv1.GroupLifecycleInfo `json:"groups"`
	LifecycleStatuses []*PodLifecycleStatus        `json:"lifecycle_statuses"`
}

// AgentSummary describes how many agents the most recent CollectLifecycle invocation saw,
// requested data from, and actually got data back from. It lets callers tell apart "the
// cluster has nothing to report" (cluster-side empty) from "the proxy could not reach any
// agent" (infrastructure-side empty), which look identical when only the InspectionResult
// is observed.
type AgentSummary struct {
	Total        int `json:"total"`
	Requested    int `json:"requested"`
	Responded    int `json:"responded"`
	NotResponded int `json:"not_responded"`
}

func emptyResult() *InspectionResult {
	return &InspectionResult{
		Groups:            make([]*fodcv1.GroupLifecycleInfo, 0),
		LifecycleStatuses: make([]*PodLifecycleStatus, 0),
	}
}

// agentLifecycleData carries pod_name alongside the lifecycle data through the channel.
type agentLifecycleData struct {
	Data    *fodcv1.LifecycleData
	PodName string
}

// RequestSender is an interface for sending lifecycle data requests to agents.
type RequestSender interface {
	RequestLifecycleData(agentID string) error
}

// Manager manages lifecycle data from multiple agents.
type Manager struct {
	log          *logger.Logger
	registry     *registry.AgentRegistry
	grpcService  RequestSender
	collecting   map[string]chan *agentLifecycleData
	mu           sync.RWMutex
	collectingMu sync.RWMutex
	collectingOp sync.Mutex
}

// NewManager creates a new lifecycle manager.
func NewManager(registry *registry.AgentRegistry, grpcService RequestSender, log *logger.Logger) *Manager {
	return &Manager{
		registry:    registry,
		grpcService: grpcService,
		log:         log,
		collecting:  make(map[string]chan *agentLifecycleData),
	}
}

// SetGRPCService sets the gRPC service for sending lifecycle data requests.
func (m *Manager) SetGRPCService(grpcService RequestSender) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.grpcService = grpcService
}

// UpdateLifecycle updates lifecycle data for a specific agent.
func (m *Manager) UpdateLifecycle(agentID, podName string, data *fodcv1.LifecycleData) {
	if data == nil {
		data = &fodcv1.LifecycleData{}
	}
	m.collectingMu.RLock()
	defer m.collectingMu.RUnlock()
	collectCh, exists := m.collecting[agentID]
	if !exists {
		return
	}
	select {
	case collectCh <- &agentLifecycleData{PodName: podName, Data: data}:
	default:
		m.log.Warn().Str("agent_id", agentID).Msg("Lifecycle collection channel full, dropping data")
	}
}

func (m *Manager) registerSession(agentID string, collectChs map[string]chan *agentLifecycleData) {
	collectCh := make(chan *agentLifecycleData, 1)
	collectChs[agentID] = collectCh
	m.collectingMu.Lock()
	m.collecting[agentID] = collectCh
	m.collectingMu.Unlock()
}

func (m *Manager) unregisterSession(agentID string, collectChs map[string]chan *agentLifecycleData) {
	m.collectingMu.Lock()
	if ch, exists := m.collecting[agentID]; exists {
		close(ch)
		delete(m.collecting, agentID)
	}
	m.collectingMu.Unlock()
	delete(collectChs, agentID)
}

// CollectLifecycle requests and collects lifecycle data from all agents and returns
// both the aggregated result and the agent summary captured atomically during the same
// invocation. Returning the summary as a second value (rather than via a separate
// "LastSummary" accessor) avoids a read-after-write race between the result and the
// summary when concurrent HTTP requests trigger overlapping collections.
func (m *Manager) CollectLifecycle(ctx context.Context) (*InspectionResult, AgentSummary) {
	m.collectingOp.Lock()
	defer m.collectingOp.Unlock()

	summary := AgentSummary{}

	if m.registry == nil || m.grpcService == nil {
		m.log.Info().Msg("CollectLifecycle: registry or grpcService is nil, returning empty")
		return emptyResult(), summary
	}

	agents := m.registry.ListAgents()
	summary.Total = len(agents)
	if len(agents) == 0 {
		m.log.Info().Msg("CollectLifecycle: no agents registered, returning empty")
		return emptyResult(), summary
	}

	m.log.Info().Int("agent_count", len(agents)).Msg("CollectLifecycle: starting collection")

	collectChs := make(map[string]chan *agentLifecycleData)
	defer m.cleanupSessions(collectChs)

	summary.Requested = m.requestAllAgents(ctx, agents, collectChs)
	m.log.Info().Int("requested", summary.Requested).Int("waiting_for", len(collectChs)).
		Msg("CollectLifecycle: requests sent, waiting for responses")

	allData := m.waitForResponses(ctx, collectChs)
	summary.Responded = len(allData)
	if summary.Requested >= summary.Responded {
		summary.NotResponded = summary.Requested - summary.Responded
	}
	m.log.Info().Int("responses_with_data", len(allData)).
		Msg("CollectLifecycle: all responses collected, aggregating")

	return m.buildInspectionResult(allData), summary
}

func (m *Manager) requestAllAgents(ctx context.Context, agents []*registry.AgentInfo,
	collectChs map[string]chan *agentLifecycleData,
) int {
	requestedCount := 0
	for _, agentInfo := range agents {
		select {
		case <-ctx.Done():
			m.log.Info().Msg("CollectLifecycle: context canceled during request phase")
			return requestedCount
		default:
		}
		m.registerSession(agentInfo.AgentID, collectChs)
		if err := m.grpcService.RequestLifecycleData(agentInfo.AgentID); err != nil {
			m.log.Info().Err(err).
				Str("agent_id", agentInfo.AgentID).
				Msg("Agent does not support lifecycle stream, skipping")
			m.unregisterSession(agentInfo.AgentID, collectChs)
			continue
		}
		requestedCount++
	}
	return requestedCount
}

func (m *Manager) waitForResponses(ctx context.Context, collectChs map[string]chan *agentLifecycleData) []*agentLifecycleData {
	allData := make([]*agentLifecycleData, 0, len(collectChs))
	var dataMu sync.Mutex
	var wg sync.WaitGroup
	for agentID, collectCh := range collectChs {
		wg.Add(1)
		go func(id string, ch chan *agentLifecycleData) {
			defer wg.Done()
			agentCtx, agentCancel := context.WithTimeout(ctx, defaultCollectionTimeout)
			defer agentCancel()
			select {
			case <-agentCtx.Done():
				m.log.Warn().Str("agent_id", id).Msg("Timeout waiting for lifecycle data from agent")
			case data := <-ch:
				if data != nil {
					m.log.Info().
						Str("agent_id", id).
						Str("pod_name", data.PodName).
						Int("reports", len(data.Data.Reports)).
						Int("groups", len(data.Data.Groups)).
						Msg("CollectLifecycle: received data from agent")
					dataMu.Lock()
					allData = append(allData, data)
					dataMu.Unlock()
				}
			}
		}(agentID, collectCh)
	}
	wg.Wait()
	return allData
}

func (m *Manager) cleanupSessions(collectChs map[string]chan *agentLifecycleData) {
	m.collectingMu.Lock()
	for agentID, collectCh := range collectChs {
		close(collectCh)
		delete(m.collecting, agentID)
	}
	m.collectingMu.Unlock()
}

func (m *Manager) buildInspectionResult(allData []*agentLifecycleData) *InspectionResult {
	return &InspectionResult{
		Groups:            m.mergeGroups(allData),
		LifecycleStatuses: m.aggregateLifecycle(allData),
	}
}

func (m *Manager) mergeGroups(allData []*agentLifecycleData) []*fodcv1.GroupLifecycleInfo {
	groupMap := make(map[string]*fodcv1.GroupLifecycleInfo)
	for _, ad := range allData {
		if ad == nil || ad.Data == nil {
			continue
		}
		for _, g := range ad.Data.Groups {
			groupMap[g.Name] = g
		}
	}
	groups := make([]*fodcv1.GroupLifecycleInfo, 0, len(groupMap))
	for _, g := range groupMap {
		groups = append(groups, g)
	}
	return groups
}

// aggregateLifecycle aggregates lifecycle statuses from multiple agents.
func (m *Manager) aggregateLifecycle(allData []*agentLifecycleData) []*PodLifecycleStatus {
	allStatuses := make([]*PodLifecycleStatus, 0)
	for _, ad := range allData {
		if ad == nil || ad.Data == nil {
			continue
		}
		if len(ad.Data.Reports) == 0 {
			continue
		}
		allStatuses = append(allStatuses, &PodLifecycleStatus{
			PodName: ad.PodName,
			Reports: ad.Data.Reports,
		})
	}
	return allStatuses
}
