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
	"errors"
	"fmt"
	"sort"
	"sync"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	fodcv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/fodc/v1"
	"github.com/apache/skywalking-banyandb/fodc/internal/consistency"
	"github.com/apache/skywalking-banyandb/fodc/internal/timeouts"
	"github.com/apache/skywalking-banyandb/fodc/proxy/internal/registry"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/panicdiag"
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

// RequestSender is an interface for sending requests to agents. Beyond driving
// the lifecycle stream, it reads the authoritative registry from a single
// schema-serving agent the manager selects by role.
type RequestSender interface {
	RequestLifecycleData(agentID string) error
	FetchSchemaRegistry(ctx context.Context, agentID string) ([]*fodcv1.SchemaRegistryGroup, error)
}

// Manager manages lifecycle data from multiple agents.
type Manager struct {
	grpcService  RequestSender
	log          *logger.Logger
	registry     *registry.AgentRegistry
	collecting   map[string]chan *agentLifecycleData
	checker      *consistency.Checker
	schemaStatus map[string]fodcv1.ConsistencyStatus
	mu           sync.RWMutex
	collectingMu sync.RWMutex
	collectingOp sync.Mutex
}

// NewManager creates a new lifecycle manager.
func NewManager(registry *registry.AgentRegistry, grpcService RequestSender, log *logger.Logger) *Manager {
	return &Manager{
		registry:     registry,
		grpcService:  grpcService,
		log:          log,
		collecting:   make(map[string]chan *agentLifecycleData),
		checker:      consistency.NewChecker(),
		schemaStatus: make(map[string]fodcv1.ConsistencyStatus),
	}
}

// SchemaConsistencySnapshot returns the per-group schema consistency status from
// the most recent collection, for the proxy to export as a Prometheus gauge. It
// reflects whatever cadence CollectLifecycle is driven at (the checker needs
// regular collections anyway for its two-round suppression).
func (m *Manager) SchemaConsistencySnapshot() map[string]fodcv1.ConsistencyStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make(map[string]fodcv1.ConsistencyStatus, len(m.schemaStatus))
	for group, status := range m.schemaStatus {
		out[group] = status
	}
	return out
}

// cacheSchemaStatus records the per-group verdict of a completed collection.
func (m *Manager) cacheSchemaStatus(groups []*fodcv1.GroupLifecycleInfo) {
	status := make(map[string]fodcv1.ConsistencyStatus, len(groups))
	for _, g := range groups {
		if sc := g.GetSchemaConsistency(); sc != nil {
			status[g.GetName()] = sc.GetStatus()
		}
	}
	m.mu.Lock()
	m.schemaStatus = status
	m.mu.Unlock()
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

	result := m.buildInspectionResult(ctx, allData, summary.NotResponded > 0)
	m.cacheSchemaStatus(result.Groups)
	return result, summary
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

func (m *Manager) buildInspectionResult(ctx context.Context, allData []*agentLifecycleData, shortfall bool) *InspectionResult {
	registryByGroup, registryGroupErrs, registryErr := m.fetchRegistry(ctx)
	verdicts := assembleSchemaConsistency(allData, m.checker, shortfall, registryByGroup)
	schemaErrors := m.registrySchemaErrors(verdicts, registryGroupErrs, registryErr)
	return &InspectionResult{
		Groups:            m.mergeGroups(allData, verdicts, schemaErrors),
		LifecycleStatuses: m.aggregateLifecycle(allData),
	}
}

// registrySchemaErrors turns the registry-fetch failures into per-group error
// strings so mergeGroups surfaces them on GroupLifecycleInfo.errors -- the same
// channel the user already reads -- instead of the reason living only in a log
// line. A fatal fetch failure is attached to every group the checker had to
// downgrade, so each UNKNOWN carries its cause; per-group read failures attach to
// just their group.
func (m *Manager) registrySchemaErrors(
	verdicts map[string]*fodcv1.SchemaConsistency, registryGroupErrs map[string]string, registryErr error,
) map[string][]string {
	schemaErrors := make(map[string][]string)
	for group, e := range registryGroupErrs {
		schemaErrors[group] = append(schemaErrors[group], "registry read failed: "+e)
	}
	if registryErr != nil {
		m.log.Warn().Err(registryErr).Msg("schema registry fetch failed; schema consistency degrades to UNKNOWN")
		for group := range verdicts {
			schemaErrors[group] = append(schemaErrors[group], "registry unavailable: "+registryErr.Error())
		}
	}
	return schemaErrors
}

// fetchRegistry reads the authoritative registry truth once per collection by
// asking a single schema-serving agent, selected by its ROLE_LIAISON node role,
// to read its local registry and report the fingerprints. It tries each liaison
// agent in a deterministic order until one succeeds, so a single unhealthy
// liaison does not blank the schema check. It returns the per-group fingerprints,
// per-group read errors, and a fatal error when no liaison could be reached; the
// caller surfaces these on the groups so the user learns why rather than only
// seeing a log line.
func (m *Manager) fetchRegistry(
	ctx context.Context,
) (map[string]map[consistency.ObjectKey]uint64, map[string]string, error) {
	m.mu.RLock()
	reg := m.registry
	svc := m.grpcService
	m.mu.RUnlock()
	if reg == nil || svc == nil {
		return nil, nil, errors.New("registry or grpc service not available for schema registry read")
	}
	liaisons := reg.ListAgentsByRole(databasev1.Role_ROLE_LIAISON.String())
	if len(liaisons) == 0 {
		return nil, nil, errors.New("no schema-serving (liaison) agent available to read the registry")
	}
	sort.Slice(liaisons, func(i, j int) bool { return liaisons[i].AgentID < liaisons[j].AgentID })
	var lastErr error
	for _, agentInfo := range liaisons {
		// Bound each read so a liaison that dies mid-stream (never sending its done
		// marker) cannot hang the collection past this deadline.
		fetchCtx, cancel := context.WithTimeout(ctx, defaultCollectionTimeout)
		groups, err := svc.FetchSchemaRegistry(fetchCtx, agentInfo.AgentID)
		cancel()
		if err != nil {
			lastErr = err
			continue
		}
		registryByGroup, groupErrs := schemaRegistryToFingerprints(groups)
		return registryByGroup, groupErrs, nil
	}
	return nil, nil, fmt.Errorf("schema registry read failed on all %d liaison agent(s): %w", len(liaisons), lastErr)
}

// schemaRegistryToFingerprints converts the agent's reported registry message
// into the per-group object fingerprint map the checker consumes, splitting out
// per-group read errors (a group whose read failed carries no objects and its
// reason is returned so the caller can surface it).
func schemaRegistryToFingerprints(
	groups []*fodcv1.SchemaRegistryGroup,
) (map[string]map[consistency.ObjectKey]uint64, map[string]string) {
	byGroup := make(map[string]map[consistency.ObjectKey]uint64, len(groups))
	groupErrs := make(map[string]string)
	for _, g := range groups {
		if e := g.GetError(); e != "" {
			groupErrs[g.GetGroup()] = e
			continue
		}
		objects := make(map[consistency.ObjectKey]uint64, len(g.GetObjects()))
		for _, o := range g.GetObjects() {
			objects[consistency.ObjectKey{Kind: o.GetKind(), Name: o.GetName()}] = o.GetFingerprint()
		}
		byGroup[g.GetGroup()] = objects
	}
	return byGroup, groupErrs
}

func (m *Manager) mergeGroups(
	allData []*agentLifecycleData,
	verdicts map[string]*fodcv1.SchemaConsistency,
	schemaErrors map[string][]string,
) []*fodcv1.GroupLifecycleInfo {
	groupMap := make(map[string]*fodcv1.GroupLifecycleInfo)
	// Errors are unioned across agents because each agent may observe a
	// different subset of per-node failures (e.g. liaison-0 sees cold-0
	// time out while liaison-1 sees cold-0 panic). Last-wins on the rest
	// of the GroupLifecycleInfo is fine -- name/catalog/resource_opts are
	// agent-invariant -- but errors must be the deduped union.
	mergedErrors := make(map[string]map[string]struct{})
	addErr := func(group string, errs ...string) {
		if len(errs) == 0 {
			return
		}
		set, ok := mergedErrors[group]
		if !ok {
			set = make(map[string]struct{})
			mergedErrors[group] = set
		}
		for _, e := range errs {
			set[e] = struct{}{}
		}
	}
	for _, ad := range allData {
		if ad == nil || ad.Data == nil {
			continue
		}
		for _, g := range ad.Data.Groups {
			groupMap[g.Name] = g
			// g.Errors already carries this agent's schema collection errors (the
			// agent folds them in before sending), so the union covers them too.
			addErr(g.Name, g.Errors...)
		}
	}
	// The proxy's own registry-fetch failures (fatal or per-group) surface here so
	// each affected group tells the user why its verdict is UNKNOWN.
	for group, errs := range schemaErrors {
		addErr(group, errs...)
	}
	groups := make([]*fodcv1.GroupLifecycleInfo, 0, len(groupMap))
	for name, g := range groupMap {
		// g is this round's freshly received data: the gRPC stream re-unmarshals
		// every LifecycleData on arrival and nothing caches it, so overwriting the
		// two merged fields in place cannot leak into a later round. This keeps
		// every other field (and any future field) without a copy that must be
		// kept in lockstep with the message definition.
		g.SchemaConsistency = verdicts[name]
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
