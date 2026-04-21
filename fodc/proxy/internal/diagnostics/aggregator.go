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

// Package diagnostics provides functionality for aggregating crash/panic diagnostic data from all agents.
package diagnostics

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	fodcv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/fodc/v1"
	"github.com/apache/skywalking-banyandb/fodc/proxy/internal/registry"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// refreshWait is the time given to agents to respond after a RequestDiagnostics call.
// The agent sends one gRPC message per crash record synchronously; this window
// collects all messages that arrive before returning the cached view.
const refreshWait = 2 * time.Second

// CrashPanicInfo holds the panic details from a crash record.
type CrashPanicInfo struct {
	OccurredAt     time.Time `json:"occurred_at"`
	Component      string    `json:"component"`
	PanicValue     string    `json:"panic_value"`
	Recovered      bool      `json:"recovered"`
	GoroutineStack string    `json:"goroutine_stack,omitempty"`
}

// AggregatedCrashRecord enriches a crash collection with agent identity.
type AggregatedCrashRecord struct {
	FetchedAt      time.Time       `json:"fetched_at"`
	PanicRecord    *CrashPanicInfo `json:"panic_record,omitempty"`
	AgentID        string          `json:"agent_id"`
	PodName        string          `json:"pod_name"`
	Role           string          `json:"role"`
	SourceEndpoint string          `json:"source_endpoint"`
	ArtifactDir    string          `json:"artifact_dir"`
	Files          []string        `json:"files,omitempty"`
}

// Filter defines filters for diagnostics collection.
type Filter struct {
	Role    string
	PodName string
}

// RequestSender is an interface for requesting crash diagnostics from agents.
type RequestSender interface {
	RequestDiagnostics(agentID string) error
}

// Aggregator aggregates crash diagnostic data from all agents.
//
// Unlike the metrics aggregator (which uses a request/response channel per
// collection cycle), crash records arrive as one gRPC message per record with
// no end-of-batch signal in the current proto.  The aggregator therefore keeps
// a dedup cache keyed by "agentID::artifactDir".  On each HTTP request it
// triggers a fresh pull from every agent, waits a short window for messages to
// arrive, then returns the current cache snapshot.
type Aggregator struct {
	registry    *registry.AgentRegistry
	grpcService RequestSender
	// cache maps "agentID::artifactDir" → record for O(1) dedup.
	cache   map[string]*AggregatedCrashRecord
	cacheMu sync.RWMutex
	log     *logger.Logger
	mu      sync.RWMutex
}

// NewAggregator creates a new Aggregator instance.
func NewAggregator(registry *registry.AgentRegistry, grpcService RequestSender, log *logger.Logger) *Aggregator {
	return &Aggregator{
		registry:    registry,
		grpcService: grpcService,
		log:         log,
		cache:       make(map[string]*AggregatedCrashRecord),
	}
}

// SetGRPCService sets the gRPC service for sending diagnostics requests.
func (a *Aggregator) SetGRPCService(grpcService RequestSender) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.grpcService = grpcService
}

// RemoveAgent removes all cached records for an agent (called on disconnect).
func (a *Aggregator) RemoveAgent(agentID string) {
	a.cacheMu.Lock()
	defer a.cacheMu.Unlock()
	prefix := agentID + "::"
	for key := range a.cache {
		if strings.HasPrefix(key, prefix) {
			delete(a.cache, key)
		}
	}
}

// ProcessCrashFromAgent stores or updates the cached record for this crash artifact.
// Each StreamCrashDiagnosticsRequest carries exactly one artifact; dedup is by artifactDir.
func (a *Aggregator) ProcessCrashFromAgent(agentID string, agentInfo *registry.AgentInfo, req *fodcv1.StreamCrashDiagnosticsRequest) {
	record := &AggregatedCrashRecord{
		AgentID:        agentID,
		PodName:        agentInfo.AgentIdentity.PodName,
		Role:           agentInfo.AgentIdentity.Role,
		SourceEndpoint: req.SourceEndpoint,
		ArtifactDir:    req.ArtifactDir,
		Files:          req.Files,
	}
	if req.FetchedAt != nil {
		record.FetchedAt = req.FetchedAt.AsTime()
	}
	if req.PanicRecord != nil {
		info := &CrashPanicInfo{
			Component:      req.PanicRecord.Component,
			PanicValue:     req.PanicRecord.PanicValue,
			Recovered:      req.PanicRecord.Recovered,
			GoroutineStack: req.PanicRecord.GoroutineStack,
		}
		if req.PanicRecord.OccurredAt != nil {
			info.OccurredAt = req.PanicRecord.OccurredAt.AsTime()
		}
		record.PanicRecord = info
	}

	cacheKey := fmt.Sprintf("%s::%s", agentID, req.ArtifactDir)
	a.cacheMu.Lock()
	a.cache[cacheKey] = record
	a.cacheMu.Unlock()
}

// CollectDiagnostics triggers a refresh from all matching agents, waits a short
// window for their records to arrive, then returns the current cache snapshot.
func (a *Aggregator) CollectDiagnostics(ctx context.Context, filter *Filter) ([]*AggregatedCrashRecord, error) {
	a.mu.RLock()
	grpcService := a.grpcService
	a.mu.RUnlock()

	agents := a.getFilteredAgents(filter)

	// Request a fresh send from each agent.  Failures are non-fatal — the agent
	// may not have established its crash stream yet.
	for _, agentInfo := range agents {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		if requestErr := grpcService.RequestDiagnostics(agentInfo.AgentID); requestErr != nil {
			a.log.Info().Err(requestErr).Str("agent_id", agentInfo.AgentID).
				Msg("Agent does not support crash diagnostics stream, skipping")
		}
	}

	// Give agents time to send all their records.
	select {
	case <-time.After(refreshWait):
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	return a.snapshotCache(filter), nil
}

// snapshotCache returns a copy of all cached records matching the filter.
func (a *Aggregator) snapshotCache(filter *Filter) []*AggregatedCrashRecord {
	a.cacheMu.RLock()
	defer a.cacheMu.RUnlock()

	result := make([]*AggregatedCrashRecord, 0, len(a.cache))
	for _, record := range a.cache {
		if filter != nil {
			if filter.Role != "" && !strings.EqualFold(record.Role, filter.Role) {
				continue
			}
			if filter.PodName != "" && !strings.EqualFold(record.PodName, filter.PodName) {
				continue
			}
		}
		result = append(result, record)
	}
	return result
}

// getFilteredAgents returns agents matching the filter.
func (a *Aggregator) getFilteredAgents(filter *Filter) []*registry.AgentInfo {
	if filter == nil {
		return a.registry.ListAgents()
	}

	var agents []*registry.AgentInfo
	if filter.Role != "" {
		agents = a.registry.ListAgentsByRole(filter.Role)
	} else {
		agents = a.registry.ListAgents()
	}

	if filter.PodName != "" {
		filtered := make([]*registry.AgentInfo, 0)
		for _, agentInfo := range agents {
			if strings.EqualFold(agentInfo.AgentIdentity.PodName, filter.PodName) {
				filtered = append(filtered, agentInfo)
			}
		}
		agents = filtered
	}

	return agents
}
