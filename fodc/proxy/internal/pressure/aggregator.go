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

// Package pressure aggregates memory-pressure pprof profile metadata from all agents.
package pressure

import (
	"context"
	"strings"
	"sync"
	"time"

	fodcv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/fodc/v1"
	"github.com/apache/skywalking-banyandb/fodc/proxy/internal/registry"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// listTimeout bounds how long CollectProfiles waits for agents to finish streaming their
// metadata. It is a fallback only: the collection normally returns as soon as every contacted
// agent signals completion, well before this elapses.
const listTimeout = 10 * time.Second

// ProfileInfo is one pprof profile's metadata in the HTTP list response.
type ProfileInfo struct {
	Type      string `json:"type"`
	Filename  string `json:"filename"`
	Filepath  string `json:"filepath"`
	Format    string `json:"format"`
	SizeBytes int64  `json:"size_bytes"`
}

// AggregatedPressureProfile enriches a capture event with the agent's identity.
type AggregatedPressureProfile struct {
	CapturedAt       time.Time     `json:"captured_at"`
	AgentID          string        `json:"agent_id"`
	PodName          string        `json:"pod_name"`
	Role             string        `json:"role"`
	ProfileID        string        `json:"profile_id"`
	SourceEndpoint   string        `json:"source_endpoint"`
	Profiles         []ProfileInfo `json:"profiles"`
	RSSBytes         uint64        `json:"rss_bytes"`
	CgroupLimitBytes uint64        `json:"cgroup_limit_bytes"`
	ThresholdBytes   uint64        `json:"threshold_bytes"`
	TriggerPercent   uint32        `json:"trigger_percent"`
}

// Filter narrows the listed/collected profiles by role and pod name.
type Filter struct {
	Role    string
	PodName string
}

// RequestSender drives the agents to stream their profile metadata. CollectList sends a
// list command to every given agent under one request and returns a channel closed once all
// of them have signaled completion.
type RequestSender interface {
	CollectList(agentIDs []string) <-chan struct{}
}

// Aggregator keeps a dedup cache of capture-event metadata nested by agentID -> profileID.
// On each list request it pulls fresh metadata from every matching agent, waits a short
// window, then returns the cache snapshot. The authoritative copy lives on the agent disk.
type Aggregator struct {
	registry    *registry.AgentRegistry
	grpcService RequestSender
	// cache is the live, queryable set: agentID -> profileID -> record. Each agent's whole set
	// is replaced in one shot on ListComplete (ReplaceAgentProfiles), so events the agent has
	// evicted from its disk drop out instead of lingering as unservable entries. The per-round
	// accumulation lives in the pressure stream handler's local state, not here.
	cache   map[string]map[string]*AggregatedPressureProfile
	log     *logger.Logger
	cacheMu sync.RWMutex
	mu      sync.RWMutex
}

// NewAggregator creates a new Aggregator instance.
func NewAggregator(reg *registry.AgentRegistry, grpcService RequestSender, log *logger.Logger) *Aggregator {
	return &Aggregator{
		registry:    reg,
		grpcService: grpcService,
		log:         log,
		cache:       make(map[string]map[string]*AggregatedPressureProfile),
	}
}

// SetGRPCService sets the gRPC service used to request profiles from agents.
func (a *Aggregator) SetGRPCService(grpcService RequestSender) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.grpcService = grpcService
}

// RemoveAgent drops all cached events for an agent (called on disconnect).
func (a *Aggregator) RemoveAgent(agentID string) {
	a.cacheMu.Lock()
	defer a.cacheMu.Unlock()
	delete(a.cache, agentID)
}

// ReplaceAgentProfiles replaces an agent's whole cached set with the records of its just-completed
// list round, enriched with the agent's pod name and role. An empty round drops the agent's entry.
// Called on ListComplete with the records accumulated by the pressure stream handler for that round,
// so events the agent has evicted from its disk drop out instead of lingering as unservable entries.
func (a *Aggregator) ReplaceAgentProfiles(agentID string, agentInfo *registry.AgentInfo, records []*fodcv1.PressureProfileRecord) {
	byProfile := make(map[string]*AggregatedPressureProfile, len(records))
	for _, record := range records {
		if record == nil {
			continue
		}
		byProfile[record.ProfileId] = buildAggregated(agentID, agentInfo, record)
	}

	a.cacheMu.Lock()
	defer a.cacheMu.Unlock()
	if len(byProfile) == 0 {
		delete(a.cache, agentID)
		return
	}
	a.cache[agentID] = byProfile
}

// buildAggregated converts one wire record into a cache entry enriched with the agent's identity.
func buildAggregated(agentID string, agentInfo *registry.AgentInfo, record *fodcv1.PressureProfileRecord) *AggregatedPressureProfile {
	profiles := make([]ProfileInfo, 0, len(record.Profiles))
	for _, p := range record.Profiles {
		profiles = append(profiles, ProfileInfo{
			Type:      p.Type,
			Filename:  p.Filename,
			Filepath:  p.Filepath,
			Format:    p.Format,
			SizeBytes: p.SizeBytes,
		})
	}
	agg := &AggregatedPressureProfile{
		AgentID:          agentID,
		PodName:          agentInfo.AgentIdentity.PodName,
		Role:             agentInfo.AgentIdentity.Role,
		ProfileID:        record.ProfileId,
		SourceEndpoint:   record.SourceEndpoint,
		RSSBytes:         record.RssBytes,
		CgroupLimitBytes: record.CgroupLimitBytes,
		TriggerPercent:   record.TriggerPercent,
		ThresholdBytes:   record.ThresholdBytes,
		Profiles:         profiles,
	}
	if record.CapturedAt != nil {
		agg.CapturedAt = record.CapturedAt.AsTime()
	}
	return agg
}

// CollectProfiles triggers a refresh from all matching agents, waits a short window for
// their metadata to arrive, then returns the current cache snapshot.
func (a *Aggregator) CollectProfiles(ctx context.Context, filter *Filter) ([]*AggregatedPressureProfile, error) {
	a.mu.RLock()
	grpcService := a.grpcService
	a.mu.RUnlock()
	if grpcService == nil {
		a.log.Warn().Msg("Pressure gRPC service not configured yet, returning cached snapshot")
		return a.snapshotCache(filter), nil
	}

	agents := a.getFilteredAgents(filter)
	// Nothing to request when no agent matched, so nothing new will arrive: skip the wait.
	if len(agents) == 0 {
		return a.snapshotCache(filter), nil
	}
	agentIDs := make([]string, 0, len(agents))
	for _, agentInfo := range agents {
		agentIDs = append(agentIDs, agentInfo.AgentID)
	}

	// Return as soon as every contacted agent has finished streaming its metadata; listTimeout
	// is only a fallback for an agent that never reports completion (e.g. it disconnected).
	select {
	case <-grpcService.CollectList(agentIDs):
	case <-time.After(listTimeout):
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	return a.snapshotCache(filter), nil
}

// LookupForDownload resolves a download target: the agent currently online for podName
// (download routes by the stable pod name, not the volatile agent id), and the on-disk
// filepath of the requested profile from the cache.
func (a *Aggregator) LookupForDownload(podName, profileID, profType string) (agentID, filepath, role string, ok bool) {
	agents := a.getFilteredAgents(&Filter{PodName: podName})
	if len(agents) == 0 {
		return "", "", "", false
	}
	agentID = agents[0].AgentID
	role = agents[0].AgentIdentity.Role

	a.cacheMu.RLock()
	defer a.cacheMu.RUnlock()
	// Keyed by the currently-online agent_id, so a stale entry from a previous connection (the
	// agent gets a fresh agent_id on reconnect) can never be served.
	rec := a.cache[agentID][profileID]
	if rec == nil || !strings.EqualFold(rec.PodName, podName) {
		return "", "", "", false
	}
	for _, p := range rec.Profiles {
		if p.Type == profType {
			return agentID, p.Filepath, role, true
		}
	}
	return "", "", "", false
}

// snapshotCache returns a copy of all cached events matching the filter.
func (a *Aggregator) snapshotCache(filter *Filter) []*AggregatedPressureProfile {
	a.cacheMu.RLock()
	defer a.cacheMu.RUnlock()

	result := make([]*AggregatedPressureProfile, 0)
	for _, byProfile := range a.cache {
		for _, rec := range byProfile {
			if filter != nil {
				if filter.Role != "" && !strings.EqualFold(rec.Role, filter.Role) {
					continue
				}
				if filter.PodName != "" && !strings.EqualFold(rec.PodName, filter.PodName) {
					continue
				}
			}
			result = append(result, rec)
		}
	}
	return result
}

// getFilteredAgents returns the registered agents matching the filter.
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
