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

// CollectLifecycle requests and collects lifecycle data from all agents.
func (m *Manager) CollectLifecycle(ctx context.Context) *InspectionResult {
	m.collectingOp.Lock()
	defer m.collectingOp.Unlock()

	if m.registry == nil {
		m.log.Info().Msg("CollectLifecycle: registry is nil, returning empty")
		return emptyResult()
	}

	agents := m.registry.ListAgents()
	if len(agents) == 0 {
		m.log.Info().Msg("CollectLifecycle: no agents registered, returning empty")
		return emptyResult()
	}

	m.log.Info().Int("agent_count", len(agents)).Msg("CollectLifecycle: starting collection")

	collectChs := make(map[string]chan *agentLifecycleData)

	defer func() {
		m.collectingMu.Lock()
		for agentID, collectCh := range collectChs {
			close(collectCh)
			delete(m.collecting, agentID)
		}
		m.collectingMu.Unlock()
	}()

	requestedCount := 0
	for _, agentInfo := range agents {
		select {
		case <-ctx.Done():
			m.log.Info().Msg("CollectLifecycle: context canceled during request phase")
			return emptyResult()
		default:
		}
		if m.grpcService == nil {
			m.log.Info().Str("agent_id", agentInfo.AgentID).Msg("CollectLifecycle: grpcService is nil, skipping request")
			continue
		}
		if err := m.grpcService.RequestLifecycleData(agentInfo.AgentID); err != nil {
			m.log.Info().Err(err).
				Str("agent_id", agentInfo.AgentID).
				Msg("Agent does not support lifecycle stream, skipping")
			continue
		}
		collectCh := make(chan *agentLifecycleData, 1)
		collectChs[agentInfo.AgentID] = collectCh
		m.collectingMu.Lock()
		m.collecting[agentInfo.AgentID] = collectCh
		m.collectingMu.Unlock()
		requestedCount++
	}

	m.log.Info().Int("requested", requestedCount).Int("waiting_for", len(collectChs)).Msg("CollectLifecycle: requests sent, waiting for responses")

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
				m.log.Warn().
					Str("agent_id", id).
					Msg("Timeout waiting for lifecycle data from agent")
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
				} else {
					m.log.Info().Str("agent_id", id).Msg("CollectLifecycle: received nil data from agent")
				}
			}
		}(agentID, collectCh)
	}

	wg.Wait()
	m.log.Info().Int("responses_with_data", len(allData)).Msg("CollectLifecycle: all responses collected, aggregating")

	return m.buildInspectionResult(allData)
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
