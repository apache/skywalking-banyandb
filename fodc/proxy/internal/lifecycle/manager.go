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
	"fmt"
	"sort"
	"sync"

	fodcv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/fodc/v1"
	"github.com/apache/skywalking-banyandb/fodc/internal/timeouts"
	"github.com/apache/skywalking-banyandb/fodc/proxy/internal/registry"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/panicdiag"
	"github.com/apache/skywalking-banyandb/pkg/schema/consistency"
)

// defaultCollectionTimeout bounds how long the proxy waits for each agent to push back
// its lifecycle data. It is derived from the agent-side InspectAll timeout plus a fixed
// slack so that this deadline is strictly greater than the agent's own deadline; the
// proxy must always outlast the agent and never give up while a still-progressing
// InspectAll call is in flight on the agent side.
const defaultCollectionTimeout = timeouts.AgentInspectAll + timeouts.ProxySlack

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
	ctx = panicdiag.WithBreadcrumb(ctx, "collect lifecycle from agents", "fodc-proxy-lifecycle", map[string]string{
		"agent_count": fmt.Sprintf("%d", len(agents)),
	})

	m.log.Info().Int("agent_count", len(agents)).Msg("CollectLifecycle: starting collection")

	collectChs := make(map[string]chan *agentLifecycleData)
	defer m.cleanupSessions(collectChs)

	summary.Requested = m.requestAllAgents(ctx, agents, collectChs)
	ctx = panicdiag.WithBreadcrumb(ctx, "requested lifecycle reports", "fodc-proxy-lifecycle", map[string]string{
		"requested_count": fmt.Sprintf("%d", summary.Requested),
		"waiting_for":     fmt.Sprintf("%d", len(collectChs)),
	})
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
	// Errors are unioned across agents because each agent may observe a
	// different subset of per-node failures (e.g. liaison-0 sees cold-0
	// time out while liaison-1 sees cold-0 panic). Last-wins on the rest
	// of the GroupLifecycleInfo is fine -- name/catalog/resource_opts are
	// agent-invariant -- but errors must be the deduped union.
	mergedErrors := make(map[string]map[string]struct{})
	// schema_consistency is merged rather than last-wins for the same reason as
	// errors: one liaison reporting a healthy group must never mask another
	// liaison that found a real divergence.
	mergedConsistency := make(map[string]*fodcv1.SchemaConsistency)
	for _, ad := range allData {
		if ad == nil || ad.Data == nil {
			continue
		}
		for _, g := range ad.Data.Groups {
			groupMap[g.Name] = g
			mergedConsistency[g.Name] = mergeSchemaConsistency(mergedConsistency[g.Name], g.SchemaConsistency)
			if len(g.Errors) == 0 {
				continue
			}
			set, ok := mergedErrors[g.Name]
			if !ok {
				set = make(map[string]struct{})
				mergedErrors[g.Name] = set
			}
			for _, e := range g.Errors {
				set[e] = struct{}{}
			}
		}
	}
	groups := make([]*fodcv1.GroupLifecycleInfo, 0, len(groupMap))
	for name, g := range groupMap {
		// g is this round's freshly received data: the gRPC stream re-unmarshals
		// every LifecycleData on arrival and nothing caches it, so overwriting the
		// two merged fields in place cannot leak into a later round. This keeps
		// every other field (and any future field) without a copy that must be
		// kept in lockstep with the message definition.
		g.SchemaConsistency = mergedConsistency[name]
		if set := mergedErrors[name]; len(set) > 0 {
			errs := make([]string, 0, len(set))
			for e := range set {
				errs = append(errs, e)
			}
			sort.Strings(errs)
			g.Errors = errs
		}
		groups = append(groups, g)
	}
	// Range over groupMap is nondeterministic; sort so the payload order is
	// stable across requests (the checker sorts its own slices for the same
	// reason).
	sort.Slice(groups, func(i, j int) bool { return groups[i].GetName() < groups[j].GetName() })
	return groups
}

// mergeSchemaConsistency folds one liaison's verdict into the accumulated one.
// The status takes the most severe of the two and the issues are the deduped
// union, so a finding survives no matter which liaison reported it or in what
// order the agents answered.
//
// The result is always a fresh message: the inputs belong to the agents' cached
// lifecycle data, and mutating them would let one collection round leak into the
// next.
func mergeSchemaConsistency(acc, incoming *fodcv1.SchemaConsistency) *fodcv1.SchemaConsistency {
	if acc == nil && incoming == nil {
		return nil
	}
	merged := &fodcv1.SchemaConsistency{Status: fodcv1.ConsistencyStatus_CONSISTENCY_STATUS_UNSPECIFIED}
	seenIssue := make(map[string]struct{})
	// Every liaison's InspectAll produces the full object table for the group, so
	// dedup by kind+name and union each object's node fingerprints by node id;
	// the accumulation belongs on a fresh message, never on an agent's cache.
	objects := make(map[string]*fodcv1.ObjectConsistency)
	for _, src := range []*fodcv1.SchemaConsistency{acc, incoming} {
		if src == nil {
			continue
		}
		if consistencySeverity(src.GetStatus()) > consistencySeverity(merged.GetStatus()) {
			merged.Status = src.GetStatus()
		}
		for _, obj := range src.GetObjects() {
			mergeObject(objects, obj)
		}
		for _, issue := range src.GetIssues() {
			key := schemaIssueKey(issue)
			if _, dup := seenIssue[key]; dup {
				continue
			}
			seenIssue[key] = struct{}{}
			merged.Issues = append(merged.Issues, issue)
		}
	}
	// acc/incoming order follows goroutine-completion order; sort so the merged
	// payload is deterministic regardless of which liaison answered first.
	merged.Objects = sortedObjects(objects)
	sort.Slice(merged.Issues, func(i, j int) bool {
		return schemaIssueKey(merged.Issues[i]) < schemaIssueKey(merged.Issues[j])
	})
	return merged
}

// mergeObject folds one object's record into the accumulator on a fresh
// ObjectConsistency, unioning node fingerprints by node id.
func mergeObject(dst map[string]*fodcv1.ObjectConsistency, obj *fodcv1.ObjectConsistency) {
	if obj == nil {
		return
	}
	key := obj.GetKind() + "|" + obj.GetName()
	oc, ok := dst[key]
	if !ok {
		oc = &fodcv1.ObjectConsistency{
			Kind: obj.GetKind(), Name: obj.GetName(), RegistryFingerprint: obj.GetRegistryFingerprint(),
		}
		dst[key] = oc
	} else if oc.GetRegistryFingerprint() == 0 && obj.GetRegistryFingerprint() != 0 {
		// The entry was created from a record that did not know the registry truth
		// (an orphan view, or a partial payload during a rolling upgrade); adopt a
		// non-zero fingerprint once any agent reports it so the merged output keeps
		// the registry truth rather than a stale zero.
		oc.RegistryFingerprint = obj.GetRegistryFingerprint()
	}
	seen := make(map[string]struct{}, len(oc.NodeFingerprints))
	for _, nf := range oc.NodeFingerprints {
		seen[nf.GetNode()] = struct{}{}
	}
	for _, nf := range obj.GetNodeFingerprints() {
		if _, dup := seen[nf.GetNode()]; dup {
			continue
		}
		seen[nf.GetNode()] = struct{}{}
		oc.NodeFingerprints = append(oc.NodeFingerprints, nf)
	}
}

// sortedObjects flattens the accumulator and orders it deterministically via the
// shared sorter, the same ordering the checker emits.
func sortedObjects(m map[string]*fodcv1.ObjectConsistency) []*fodcv1.ObjectConsistency {
	out := make([]*fodcv1.ObjectConsistency, 0, len(m))
	for _, oc := range m {
		out = append(out, oc)
	}
	consistency.SortObjectConsistencies(out)
	return out
}

// schemaIssueKey identifies a finding: the same object on the same node with the
// same failure mode is one finding however many liaisons saw it.
func schemaIssueKey(issue *fodcv1.SchemaIssue) string {
	return fmt.Sprintf("%s|%s|%s|%d",
		issue.GetKind(), issue.GetName(), issue.GetNode(), issue.GetType())
}

// consistencySeverity orders verdicts so the merge keeps the most alarming one.
func consistencySeverity(status fodcv1.ConsistencyStatus) int {
	switch status {
	case fodcv1.ConsistencyStatus_CONSISTENCY_STATUS_INCONSISTENT:
		return 3
	case fodcv1.ConsistencyStatus_CONSISTENCY_STATUS_UNKNOWN:
		return 2
	case fodcv1.ConsistencyStatus_CONSISTENCY_STATUS_CONSISTENT:
		return 1
	case fodcv1.ConsistencyStatus_CONSISTENCY_STATUS_UNSPECIFIED:
		return 0
	default:
		return 0
	}
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
