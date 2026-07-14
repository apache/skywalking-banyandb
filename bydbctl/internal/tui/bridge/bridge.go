// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package bridge provides the private, bydbctl-owned tool set exposed to ACP agents.
package bridge

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/agent"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/approval"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/planner"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/tools"
)

const (
	eventBufferSize       = 64
	maxSchemaDescriptions = 3
	maxProbePreviewRows   = 10
	ToolListGroupsSchemas = "list_groups_schemas"
	ToolDescribeSchema    = "describe_schema"
	ToolProposeQueryPlan  = "propose_query_plan"
	ToolValidateBydbQL    = "validate_bydbql"
	ToolProbeBydbQL       = "probe_bydbql"
	ToolExecuteBydbQL     = "execute_bydbql"
)

// Validator checks a BYDBQL query without executing it.
type Validator interface {
	Validate(ctx context.Context, query string, schema *session.SchemaSnapshot) (session.ValidationReport, error)
}

// Config creates a private tool bridge.
type Config struct {
	Approvals *approval.Controller
	Executor  tools.Executor
	Validator Validator
}

// Call is one structured MCP tool request.
type Call struct {
	Arguments map[string]any
	Name      string
}

// Result is the compact, provider-safe result of a tool request.
type Result struct {
	Content string
	Err     error
}

// ToolBridge holds all tool policy, execution, and visible lifecycle events behind a small interface.
type ToolBridge struct {
	approvals *approval.Controller
	executor  tools.Executor
	validator Validator
	now       func() time.Time
	events    chan agent.Event

	mu                 sync.RWMutex
	callMu             sync.Mutex
	querySession       *session.QuerySession
	activePolicy       approval.ExecutionPolicy
	planAttempts       int
	schemaDescriptions int
	rankedCandidates   []session.CatalogEntry
	executionMu        sync.Mutex
	cancelQuery        context.CancelFunc
}

// New creates a private tool bridge. The bridge never receives server credentials.
func New(config Config) *ToolBridge {
	approvals := config.Approvals
	if approvals == nil {
		approvals = approval.NewController()
	}
	return &ToolBridge{
		approvals: approvals,
		executor:  config.Executor,
		validator: config.Validator,
		now:       time.Now,
		events:    make(chan agent.Event, eventBufferSize),
	}
}

// SetExecutionPolicy stores the active execution policy for tool calls.
func (toolBridge *ToolBridge) SetExecutionPolicy(policy approval.ExecutionPolicy) {
	if toolBridge == nil {
		return
	}
	normalizedPolicy := approval.NormalizeExecutionPolicy(string(policy))
	toolBridge.mu.Lock()
	toolBridge.activePolicy = normalizedPolicy
	toolBridge.mu.Unlock()
	toolBridge.approvals.SetPolicy(normalizedPolicy)
}

// SetSession copies the current workspace session for subsequent tool calls.
func (toolBridge *ToolBridge) SetSession(querySession *session.QuerySession) {
	toolBridge.callMu.Lock()
	defer toolBridge.callMu.Unlock()
	toolBridge.mu.Lock()
	toolBridge.querySession = cloneQuerySession(querySession)
	toolBridge.planAttempts = 0
	toolBridge.schemaDescriptions = 0
	toolBridge.rankedCandidates = nil
	toolBridge.mu.Unlock()
}

// SessionSnapshot returns a copy of the workspace state produced by controlled tool calls.
func (toolBridge *ToolBridge) SessionSnapshot() *session.QuerySession {
	if toolBridge == nil {
		return nil
	}
	toolBridge.callMu.Lock()
	defer toolBridge.callMu.Unlock()
	return cloneQuerySession(toolBridge.session())
}

// SetRankedCandidates pins the catalog shortlist used by describe_schema and propose_query_plan.
func (toolBridge *ToolBridge) SetRankedCandidates(candidates []session.CatalogEntry) {
	toolBridge.mu.Lock()
	toolBridge.rankedCandidates = append([]session.CatalogEntry(nil), candidates...)
	toolBridge.mu.Unlock()
}

// Events returns visible tool lifecycle updates for the TUI.
func (toolBridge *ToolBridge) Events() <-chan agent.Event {
	return toolBridge.events
}

// Cancel rejects pending approvals and makes a best effort to cancel the active query request.
func (toolBridge *ToolBridge) Cancel() {
	if toolBridge == nil {
		return
	}
	toolBridge.approvals.Cancel()
	toolBridge.executionMu.Lock()
	cancelQuery := toolBridge.cancelQuery
	toolBridge.executionMu.Unlock()
	if cancelQuery != nil {
		cancelQuery()
	}
}

// Call dispatches only the closed, registered bydbctl tool set.
func (toolBridge *ToolBridge) Call(ctx context.Context, call Call) Result {
	toolBridge.callMu.Lock()
	defer toolBridge.callMu.Unlock()
	toolName := strings.TrimSpace(call.Name)
	callID := uuid.NewString()
	toolBridge.emit(agent.Event{
		ID:           callID,
		Kind:         agent.EventKindToolCall,
		ToolName:     toolName,
		InputSummary: summarizeArguments(call.Arguments),
		InputDetail:  formatArgumentsDetail(call.Arguments),
		Status:       agent.EventStatusRunning,
		StartedAt:    toolBridge.now(),
	})
	var result Result
	switch toolName {
	case ToolListGroupsSchemas:
		result = toolBridge.listGroupsSchemas(ctx)
	case ToolDescribeSchema:
		result = toolBridge.describeSchema(ctx, call.Arguments)
	case ToolProposeQueryPlan:
		result = toolBridge.proposeQueryPlan(ctx, callID, call.Arguments)
	case ToolValidateBydbQL:
		result = toolBridge.validateBydbQL(ctx, callID, call.Arguments)
	case ToolProbeBydbQL:
		result = toolBridge.probeBydbQL(ctx, callID, call.Arguments)
	case ToolExecuteBydbQL:
		result = toolBridge.executeBydbQL(ctx, callID, call.Arguments)
	default:
		result = Result{Err: fmt.Errorf("tool %q is not registered", toolName)}
	}
	toolBridge.emitResult(callID, toolName, result)
	return result
}

func (toolBridge *ToolBridge) listGroupsSchemas(ctx context.Context) Result {
	if toolBridge.executor == nil {
		return Result{Err: fmt.Errorf("schema executor is not configured")}
	}
	catalog, catalogErr := toolBridge.executor.DiscoverCatalog(ctx)
	if catalogErr != nil {
		return Result{Err: fmt.Errorf("group and schema discovery failed")}
	}
	goal := ""
	if querySession := toolBridge.session(); querySession != nil {
		goal = strings.TrimSpace(querySession.DiscoveryGoal)
		if goal == "" {
			goal = querySession.UserGoal
		}
	}
	candidates := toolBridge.rankedCatalogCandidates()
	if len(candidates) == 0 {
		candidates = rankCatalogCandidates(goal, catalog.Entries)
		toolBridge.setRankedCandidates(candidates)
	}
	return jsonResult(map[string]any{
		"groups":          catalog.Groups,
		"candidate_limit": maxCatalogCandidates,
		"resources":       candidates,
	})
}

func (toolBridge *ToolBridge) describeSchema(ctx context.Context, arguments map[string]any) Result {
	if toolBridge.executor == nil {
		return Result{Err: fmt.Errorf("schema executor is not configured")}
	}
	querySession := toolBridge.session()
	resourceType := session.NormalizeResourceType(stringArgument(arguments, "type"))
	resourceName := stringArgument(arguments, "name")
	groups := stringSliceArgument(arguments, "groups")
	if querySession != nil {
		if resourceName == "" {
			resourceName = querySession.ResourceName
		}
		if len(groups) == 0 {
			groups = append([]string(nil), querySession.Groups...)
		}
		if stringArgument(arguments, "type") == "" {
			resourceType = querySession.ResourceType
		}
	}
	if !resourceIsRanked(toolBridge.rankedCatalogCandidates(), resourceType, resourceName, groups) {
		return Result{Err: fmt.Errorf("schema description requires a resource from the top five catalog candidates")}
	}
	if !toolBridge.reserveSchemaDescription() {
		return Result{Err: fmt.Errorf("schema discovery limit reached after three detailed schema inspections")}
	}
	snapshot, schemaErr := toolBridge.executor.DiscoverSchema(ctx, tools.SchemaRequest{
		Type:   resourceType,
		Name:   resourceName,
		Groups: groups,
	})
	if schemaErr != nil {
		return Result{Err: fmt.Errorf("schema description failed")}
	}
	if querySession != nil {
		setSessionSchema(querySession, snapshot)
	}
	toolBridge.mu.Lock()
	toolBridge.planAttempts = 0
	toolBridge.mu.Unlock()
	response := map[string]any{
		"type":           snapshot.Type,
		"name":           snapshot.Name,
		"groups":         snapshot.Groups,
		"tags":           snapshot.Tags,
		"fields":         snapshot.Fields,
		"columns":        columnsForProvider(snapshot.Columns),
		"indexed_fields": snapshot.IndexedFields,
	}
	if planExample := buildDescribePlanExample(snapshot); planExample != nil {
		response["plan_example"] = planExample
	}
	return jsonResult(response)
}

const maxCatalogCandidates = 5

func (toolBridge *ToolBridge) setRankedCandidates(candidates []session.CatalogEntry) {
	toolBridge.mu.Lock()
	toolBridge.rankedCandidates = append([]session.CatalogEntry(nil), candidates...)
	toolBridge.mu.Unlock()
}

func (toolBridge *ToolBridge) rankedCatalogCandidates() []session.CatalogEntry {
	toolBridge.mu.RLock()
	defer toolBridge.mu.RUnlock()
	return append([]session.CatalogEntry(nil), toolBridge.rankedCandidates...)
}

func rankCatalogCandidates(goal string, entries []session.CatalogEntry) []session.CatalogEntry {
	goalTokens := strings.FieldsFunc(strings.ToLower(strings.ReplaceAll(goal, "_", " ")), func(r rune) bool {
		return r < 'a' || r > 'z'
	})
	normalizedGoal := strings.ToLower(goal)
	type rankedEntry struct {
		entry session.CatalogEntry
		score int
	}
	ranked := make([]rankedEntry, 0, len(entries))
	for _, entry := range entries {
		if strings.TrimSpace(entry.Name) == "" || strings.HasPrefix(entry.Group, "_") {
			continue
		}
		name := strings.ToLower(entry.Name)
		group := strings.ToLower(entry.Group)
		score := 0
		for _, token := range goalTokens {
			if strings.Contains(name, token) {
				score += 3
			}
			if strings.Contains(group, token) {
				score += 2
			}
		}
		if strings.Contains(normalizedGoal, name) {
			score += 20
		}
		if strings.Contains(normalizedGoal, group) {
			score += 10
		}
		if entry.Type == session.ResourceTypeMeasure && (strings.Contains(normalizedGoal, "metric") || strings.Contains(normalizedGoal, "latency") || strings.Contains(normalizedGoal, "endpoint")) {
			score += 4
		}
		if entry.Type == session.ResourceTypeStream && (strings.Contains(normalizedGoal, "log") || strings.Contains(normalizedGoal, "stream")) {
			score += 2
		}
		if entry.Type == session.ResourceTypeTrace && (strings.Contains(normalizedGoal, "trace") || strings.Contains(normalizedGoal, "span")) {
			score += 2
		}
		if strings.Contains(group, "metric") && (strings.Contains(normalizedGoal, "metric") || strings.Contains(normalizedGoal, "endpoint") || strings.Contains(normalizedGoal, "latency")) {
			score += 6
		}
		if strings.Contains(name, "latency") && strings.Contains(normalizedGoal, "slow") {
			score += 8
		}
		if strings.Contains(name, "endpoint") && (strings.Contains(normalizedGoal, "endpoint") || strings.Contains(normalizedGoal, "payment")) {
			score += 8
		}
		if strings.Contains(name, "cpu") && strings.Contains(normalizedGoal, "cpu") {
			score += 12
		}
		if group == "default" && len(entries) > 20 {
			score -= 8
		}
		ranked = append(ranked, rankedEntry{entry: entry, score: score})
	}
	sort.SliceStable(ranked, func(leftIndex, rightIndex int) bool {
		if ranked[leftIndex].score != ranked[rightIndex].score {
			return ranked[leftIndex].score > ranked[rightIndex].score
		}
		if ranked[leftIndex].entry.Group != ranked[rightIndex].entry.Group {
			return ranked[leftIndex].entry.Group < ranked[rightIndex].entry.Group
		}
		if ranked[leftIndex].entry.Type != ranked[rightIndex].entry.Type {
			return ranked[leftIndex].entry.Type < ranked[rightIndex].entry.Type
		}
		return ranked[leftIndex].entry.Name < ranked[rightIndex].entry.Name
	})
	if len(ranked) > maxCatalogCandidates {
		ranked = ranked[:maxCatalogCandidates]
	}
	candidates := make([]session.CatalogEntry, 0, len(ranked))
	for _, entry := range ranked {
		candidates = append(candidates, entry.entry)
	}
	return candidates
}

func resourceIsRanked(candidates []session.CatalogEntry, resourceType session.ResourceType, resourceName string, groups []string) bool {
	if len(candidates) == 0 {
		return false
	}
	for _, group := range groups {
		found := false
		for _, entry := range candidates {
			if catalogTypesCompatible(resourceType, entry.Type) &&
				strings.EqualFold(entry.Name, resourceName) &&
				strings.EqualFold(entry.Group, group) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func catalogTypesCompatible(planType, catalogType session.ResourceType) bool {
	if planType == catalogType {
		return true
	}
	return planType == session.ResourceTypeTopN && catalogType == session.ResourceTypeMeasure
}

func (toolBridge *ToolBridge) reserveSchemaDescription() bool {
	toolBridge.mu.Lock()
	defer toolBridge.mu.Unlock()
	if toolBridge.schemaDescriptions >= maxSchemaDescriptions {
		return false
	}
	toolBridge.schemaDescriptions++
	return true
}

func columnsForProvider(columns []session.SchemaColumn) []map[string]any {
	if len(columns) == 0 {
		return nil
	}
	result := make([]map[string]any, 0, len(columns))
	for _, column := range columns {
		result = append(result, map[string]any{
			"name":    column.Name,
			"kind":    column.Kind,
			"type":    column.Type,
			"indexed": column.Indexed,
		})
	}
	return result
}

func setSessionSchema(querySession *session.QuerySession, schemaSnapshot session.SchemaSnapshot) {
	if len(schemaSnapshot.AvailableGroups) == 0 {
		schemaSnapshot.AvailableGroups = append([]string(nil), querySession.SchemaSnapshot.AvailableGroups...)
	}
	if len(schemaSnapshot.Catalog) == 0 {
		schemaSnapshot.Catalog = append([]session.CatalogEntry(nil), querySession.SchemaSnapshot.Catalog...)
	}
	querySession.ResourceType = schemaSnapshot.Type
	querySession.ResourceName = schemaSnapshot.Name
	querySession.Groups = append([]string(nil), schemaSnapshot.Groups...)
	querySession.SchemaSnapshot = schemaSnapshot
}

func (toolBridge *ToolBridge) proposeQueryPlan(ctx context.Context, callID string, arguments map[string]any) Result {
	if toolBridge.executor == nil || toolBridge.validator == nil {
		return Result{Err: fmt.Errorf("query plan bridge is not configured")}
	}
	querySession := toolBridge.session()
	if querySession == nil {
		return Result{Err: fmt.Errorf("query session is not configured")}
	}
	plans, planErr := plannedQueries(arguments)
	if planErr != nil {
		return jsonResult(map[string]any{
			"valid":       false,
			"message":     planErr.Error(),
			"schema_hint": queryPlanSchemaHint(),
		})
	}
	for planIndex, plan := range plans {
		if rankedCandidates := toolBridge.rankedCatalogCandidates(); len(rankedCandidates) != 0 &&
			!resourceIsRanked(rankedCandidates, plan.Resource.Type, plan.Resource.Name, plan.Resource.Groups) {
			return Result{Err: fmt.Errorf("query plan step %d selects a resource outside the top five catalog candidates", planIndex+1)}
		}
		if !schemaReadyForPlan(querySession, plan.Resource) {
			return jsonResult(planFailurePayload(querySession, schemaNotReadyMessage(planIndex+1, plan.Resource), planIndex+1, 0, ""))
		}
	}
	attempt, allowed := toolBridge.reservePlanAttempt()
	if !allowed {
		return jsonResult(map[string]any{
			"valid":   false,
			"message": planRepairLimitMessage(),
		})
	}
	if attempt > 1 {
		toolBridge.emit(agent.Event{
			ID:        callID,
			Kind:      agent.EventKindPlanUpdate,
			ToolName:  ToolProposeQueryPlan,
			Message:   fmt.Sprintf("repairing query plan (%d of %d attempts)", attempt, MaxPlanRepairAttempts),
			Status:    agent.EventStatusRunning,
			StartedAt: toolBridge.now(),
		})
	}
	compiledQueries := make([]planner.CompiledQuery, 0, len(plans))
	plannedQueries := make([]session.PlannedQuery, 0, len(plans))
	var selectedSnapshot session.SchemaSnapshot
	for planIndex, plan := range plans {
		if !resourceIsDiscoverable(querySession.SchemaSnapshot.Catalog, plan.Resource) {
			return Result{Err: fmt.Errorf("query plan step %d selects a resource outside the discovered catalog", planIndex+1)}
		}
		snapshot, schemaErr := toolBridge.executor.DiscoverSchema(ctx, schemaRequestForPlan(plan))
		if schemaErr != nil {
			return Result{Err: fmt.Errorf("failed to discover schema for plan step %d: %w", planIndex+1, schemaErr)}
		}
		compiled, compileErr := planner.Compile(plan, snapshot)
		if compileErr != nil {
			draftQuery := planner.CompileDisplayDraft(plan)
			toolBridge.emitProposeCandidate(callID, draftQuery, true, compileErr.Error())
			return jsonResult(planFailurePayload(querySession, compileErr.Error(), planIndex+1, attempt, draftQuery))
		}
		validation, validationErr := toolBridge.validator.Validate(ctx, compiled.Query, &snapshot)
		if validationErr != nil {
			return Result{Err: fmt.Errorf("failed to validate query plan step %d: %w", planIndex+1, validationErr)}
		}
		if !validation.Valid {
			toolBridge.emitProposeCandidate(callID, compiled.Query, true, validation.Message)
			return jsonResult(planFailurePayload(querySession, validation.Message, planIndex+1, attempt, compiled.Query))
		}
		if planIndex == 0 {
			selectedSnapshot = snapshot
		}
		compiledQueries = append(compiledQueries, compiled)
		plannedQueries = append(plannedQueries, session.PlannedQuery{
			ID:           compiled.ID,
			Query:        compiled.Query,
			ResourceType: compiled.Resource.Type,
			Name:         compiled.Resource.Name,
			Groups:       append([]string(nil), compiled.Resource.Groups...),
		})
	}
	setSessionSchema(querySession, selectedSnapshot)
	querySession.SetPlannedQueries(plannedQueries)
	firstQuery := compiledQueries[0]
	response := map[string]any{
		"valid":        true,
		"query":        firstQuery.Query,
		"step_count":   len(compiledQueries),
		"resource":     firstQuery.Resource,
		"next_step_id": firstQuery.ID,
	}
	if toolBridge.shouldAutoProbeAfterPlan() {
		probeSummary := toolBridge.probePlannedQuery(ctx, querySession, firstQuery.Query, plannedQueries[0])
		if probeSummary != nil {
			querySession.SetPendingProbe(probeSummary)
			response["probe"] = probeSummaryPayload(probeSummary)
			if probeSummary.Error != "" {
				response["valid"] = false
				response["message"] = probeSummary.Error
				toolBridge.emitProposeCandidate(callID, firstQuery.Query, true, probeSummary.Error)
				return jsonResult(response)
			}
			toolBridge.emit(agent.Event{
				ID:            uuid.NewString(),
				Kind:          agent.EventKindToolResult,
				ToolName:      ToolProbeBydbQL,
				Message:       fmt.Sprintf("workflow probe returned %d rows", probeSummary.Rows),
				OutputSummary: probeOutputSummary(probeSummary),
				Status:        agent.EventStatusSucceeded,
				CompletedAt:   toolBridge.now(),
			})
		}
	}
	toolBridge.emitProposeCandidate(callID, firstQuery.Query, false, "query plan compiled through controlled tool")
	return jsonResult(response)
}

func schemaRequestForPlan(plan planner.QueryPlan) tools.SchemaRequest {
	resourceType := plan.Resource.Type
	if resourceType == session.ResourceTypeTopN {
		resourceType = session.ResourceTypeMeasure
	}
	return tools.SchemaRequest{
		Type:   resourceType,
		Name:   plan.Resource.Name,
		Groups: plan.Resource.Groups,
	}
}

func resourceIsDiscoverable(catalog []session.CatalogEntry, resource planner.Resource) bool {
	if len(catalog) == 0 {
		return true
	}
	for _, group := range resource.Groups {
		found := false
		for _, entry := range catalog {
			if catalogTypesCompatible(resource.Type, entry.Type) &&
				strings.EqualFold(entry.Name, resource.Name) &&
				strings.EqualFold(entry.Group, group) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func (toolBridge *ToolBridge) reservePlanAttempt() (int, bool) {
	toolBridge.mu.Lock()
	defer toolBridge.mu.Unlock()
	if toolBridge.planAttempts >= MaxPlanRepairAttempts {
		return toolBridge.planAttempts, false
	}
	toolBridge.planAttempts++
	return toolBridge.planAttempts, true
}

func plannedQueries(arguments map[string]any) ([]planner.QueryPlan, error) {
	planValue, hasPlan := arguments["plan"]
	workflowValue, hasWorkflow := arguments["workflow"]
	if hasPlan == hasWorkflow {
		return nil, fmt.Errorf("propose_query_plan requires exactly one of plan or workflow")
	}
	if hasPlan {
		var plan planner.QueryPlan
		if decodeErr := decodePlanArgument(normalizePlanArgument(planValue), &plan); decodeErr != nil {
			return nil, fmt.Errorf("invalid query plan: %w", decodeErr)
		}
		return []planner.QueryPlan{plan}, nil
	}
	var workflow planner.WorkflowPlan
	if decodeErr := decodePlanArgument(normalizePlanArgument(workflowValue), &workflow); decodeErr != nil {
		return nil, fmt.Errorf("invalid query workflow: %w", decodeErr)
	}
	if len(workflow.Steps) == 0 {
		return nil, fmt.Errorf("query workflow requires at least one step")
	}
	return workflow.Steps, nil
}

func decodePlanArgument(value any, target any) error {
	encodedValue, marshalErr := json.Marshal(value)
	if marshalErr != nil {
		return fmt.Errorf("failed to encode plan input: %w", marshalErr)
	}
	decoder := json.NewDecoder(bytes.NewReader(encodedValue))
	decoder.UseNumber()
	if decodeErr := decoder.Decode(target); decodeErr != nil {
		return fmt.Errorf("failed to decode plan input: %w", decodeErr)
	}
	return nil
}

func (toolBridge *ToolBridge) validateBydbQL(ctx context.Context, _ string, arguments map[string]any) Result {
	if toolBridge.validator == nil {
		return Result{Err: fmt.Errorf("BYDBQL validator is not configured")}
	}
	query := stringArgument(arguments, "query")
	querySession := toolBridge.session()
	var schemaSnapshot *session.SchemaSnapshot
	if querySession != nil {
		schemaSnapshot = &querySession.SchemaSnapshot
	}
	validation, validateErr := toolBridge.validator.Validate(ctx, query, schemaSnapshot)
	if validateErr != nil {
		return Result{Err: fmt.Errorf("failed to validate BYDBQL: %w", validateErr)}
	}
	return jsonResult(map[string]any{
		"valid":      validation.Valid,
		"message":    validation.Message,
		"query_type": validation.QueryType,
	})
}

func (toolBridge *ToolBridge) probeBydbQL(ctx context.Context, callID string, arguments map[string]any) Result {
	if toolBridge.validator == nil || toolBridge.executor == nil {
		return Result{Err: fmt.Errorf("BYDBQL probe bridge is not configured")}
	}
	querySession := toolBridge.session()
	if querySession == nil {
		return Result{Err: fmt.Errorf("query session is not configured")}
	}
	query := stringArgument(arguments, "query")
	plannedQuery := querySession.CurrentPlannedQuery()
	if plannedQuery == nil || plannedQuery.Query != query {
		return Result{Err: fmt.Errorf("probe_bydbql requires propose_query_plan to return valid=true first; validate_bydbql alone does not register a candidate")}
	}
	schemaSnapshot, schemaErr := toolBridge.executor.DiscoverSchema(ctx, tools.SchemaRequest{
		Type:   plannedQuery.ResourceType,
		Name:   plannedQuery.Name,
		Groups: plannedQuery.Groups,
	})
	if schemaErr != nil {
		return Result{Err: fmt.Errorf("failed to refresh planned query schema: %w", schemaErr)}
	}
	setSessionSchema(querySession, schemaSnapshot)
	validation, validationErr := toolBridge.validator.Validate(ctx, query, &schemaSnapshot)
	if validationErr != nil {
		return Result{Err: fmt.Errorf("failed to validate probe query: %w", validationErr)}
	}
	if !validation.Valid {
		return jsonResult(map[string]any{"valid": false, "message": validation.Message})
	}
	if !toolBridge.executionPolicy().AutoApprove(approval.SourceAgentProbe, true, query) {
		toolBridge.emit(agent.Event{
			ID:           callID,
			Kind:         agent.EventKindApproval,
			ToolName:     ToolProbeBydbQL,
			Message:      "probe approval required",
			InputSummary: "read-only probe awaiting user decision",
			Status:       agent.EventStatusWaiting,
			StartedAt:    toolBridge.now(),
		})
	}
	probeSummary, probeErr := toolBridge.runApprovedProbe(ctx, querySession, query, plannedQuery)
	if probeErr != nil {
		return Result{Err: probeErr}
	}
	return jsonResult(map[string]any{
		"valid":   true,
		"rows":    probeSummary.Rows,
		"columns": probeSummary.Columns,
		"preview": probeSummary.Preview,
		"error":   probeSummary.Error,
		"summary": probeOutputSummary(probeSummary),
	})
}

func (toolBridge *ToolBridge) executeBydbQL(ctx context.Context, callID string, arguments map[string]any) Result {
	if toolBridge.validator == nil || toolBridge.executor == nil {
		return Result{Err: fmt.Errorf("BYDBQL execution bridge is not configured")}
	}
	querySession := toolBridge.session()
	if querySession == nil {
		return Result{Err: fmt.Errorf("query session is not configured")}
	}
	query := stringArgument(arguments, "query")
	plannedQuery := querySession.CurrentPlannedQuery()
	if plannedQuery == nil || plannedQuery.Query != query {
		return Result{Err: fmt.Errorf("execute_bydbql requires propose_query_plan to return valid=true first; validate_bydbql alone does not register a candidate")}
	}
	schemaSnapshot, schemaErr := toolBridge.executor.DiscoverSchema(ctx, tools.SchemaRequest{
		Type:   plannedQuery.ResourceType,
		Name:   plannedQuery.Name,
		Groups: plannedQuery.Groups,
	})
	if schemaErr != nil {
		return Result{Err: fmt.Errorf("failed to refresh planned query schema: %w", schemaErr)}
	}
	setSessionSchema(querySession, schemaSnapshot)
	validation, validationErr := toolBridge.validator.Validate(ctx, query, &schemaSnapshot)
	if validationErr != nil {
		return Result{Err: fmt.Errorf("failed to validate execution query: %w", validationErr)}
	}
	if !validation.Valid {
		return jsonResult(map[string]any{"valid": false, "message": validation.Message})
	}
	if !toolBridge.executionPolicy().AutoApprove(approval.SourceAgentTool, false, query) {
		toolBridge.emit(agent.Event{
			ID:           callID,
			Kind:         agent.EventKindApproval,
			ToolName:     ToolExecuteBydbQL,
			Message:      "execution approval required",
			InputSummary: "exact BYDBQL statement awaiting user decision",
			Status:       agent.EventStatusWaiting,
			StartedAt:    toolBridge.now(),
		})
	}
	limits := tools.Limits(toolBridge.executor)
	request := approval.WithLimits(
		approval.NewRequest(
			query,
			fmt.Sprintf("%s/%s", querySession.ResourceType, querySession.ResourceName),
			querySession.Groups,
			approval.SourceAgentTool,
		),
		limits.Timeout,
		limits.PreviewRows,
	)
	decision, approvalErr := toolBridge.approvals.Request(ctx, request)
	if approvalErr != nil {
		return Result{Err: fmt.Errorf("execution approval did not complete: %w", approvalErr)}
	}
	if !decision.Approved {
		toolBridge.emit(agent.Event{
			ID:          callID,
			Kind:        agent.EventKindCancelled,
			ToolName:    ToolExecuteBydbQL,
			Message:     "execution rejected",
			Status:      agent.EventStatusCancelled,
			CompletedAt: toolBridge.now(),
		})
		return Result{Err: fmt.Errorf("execution rejected")}
	}
	validation, validationErr = toolBridge.validator.Validate(ctx, query, &querySession.SchemaSnapshot)
	if validationErr != nil {
		return Result{Err: fmt.Errorf("failed to revalidate approved query: %w", validationErr)}
	}
	if !validation.Valid {
		return Result{Err: fmt.Errorf("approved query failed revalidation: %s", validation.Message)}
	}
	executionCtx, cancelQuery := context.WithCancel(ctx)
	toolBridge.setExecutionCancel(cancelQuery)
	executionResult, executeErr := toolBridge.executor.Execute(executionCtx, querySession, query)
	executionCancelled := executionCtx.Err() != nil
	cancelQuery()
	toolBridge.clearExecutionCancel()
	querySession.ExecutionResult = executionResult
	if executeErr != nil {
		if executionCancelled {
			return Result{Err: fmt.Errorf("BYDBQL execution failed")}
		}
		return jsonResult(map[string]any{
			"rows":    executionResult.Rows,
			"summary": "BYDBQL execution failed",
			"error":   "BYDBQL execution failed",
		})
	}
	nextPlannedQuery := querySession.CompletePlannedQuery(query)
	response := map[string]any{
		"rows":      executionResult.Rows,
		"summary":   executionResult.Summary,
		"error":     providerError(executionResult.Error),
		"columns":   executionResult.Columns,
		"preview":   executionResult.Preview,
		"truncated": executionResult.Truncated,
	}
	if nextPlannedQuery != nil {
		response["next_query"] = nextPlannedQuery.Query
		toolBridge.emit(agent.Event{
			ID:          uuid.NewString(),
			Kind:        agent.EventKindCandidate,
			ToolName:    ToolProposeQueryPlan,
			Candidate:   nextPlannedQuery.Query,
			Message:     "next planned query ready for individual approval",
			Status:      agent.EventStatusSucceeded,
			CompletedAt: toolBridge.now(),
		})
	}
	return jsonResult(response)
}

func providerError(executionError string) string {
	if strings.TrimSpace(executionError) == "" {
		return ""
	}
	return "BYDBQL execution failed"
}

func (toolBridge *ToolBridge) setExecutionCancel(cancelQuery context.CancelFunc) {
	toolBridge.executionMu.Lock()
	toolBridge.cancelQuery = cancelQuery
	toolBridge.executionMu.Unlock()
}

func (toolBridge *ToolBridge) clearExecutionCancel() {
	toolBridge.executionMu.Lock()
	toolBridge.cancelQuery = nil
	toolBridge.executionMu.Unlock()
}

func (toolBridge *ToolBridge) session() *session.QuerySession {
	toolBridge.mu.RLock()
	defer toolBridge.mu.RUnlock()
	return toolBridge.querySession
}

func cloneQuerySession(querySession *session.QuerySession) *session.QuerySession {
	if querySession == nil {
		return nil
	}
	clonedSession := *querySession
	clonedSession.Groups = append([]string(nil), querySession.Groups...)
	clonedSession.SchemaSnapshot = cloneSchemaSnapshot(querySession.SchemaSnapshot)
	clonedSession.Conversation = append([]session.ConversationTurn(nil), querySession.Conversation...)
	clonedSession.Candidates = cloneCandidates(querySession.Candidates)
	clonedSession.PlannedQueries = clonePlannedQueries(querySession.PlannedQueries)
	clonedSession.ExecutionResult = cloneExecutionResult(querySession.ExecutionResult)
	clonedSession.Transcript = append([]session.TranscriptEntry(nil), querySession.Transcript...)
	clonedSession.ChatMessages = cloneChatMessages(querySession.ChatMessages)
	clonedSession.PendingProbe = cloneProbeSummary(querySession.PendingProbe)
	return &clonedSession
}

func cloneSchemaSnapshot(schemaSnapshot session.SchemaSnapshot) session.SchemaSnapshot {
	clonedSnapshot := schemaSnapshot
	clonedSnapshot.Groups = append([]string(nil), schemaSnapshot.Groups...)
	clonedSnapshot.Tags = append([]string(nil), schemaSnapshot.Tags...)
	clonedSnapshot.EntityTags = append([]string(nil), schemaSnapshot.EntityTags...)
	clonedSnapshot.Fields = append([]string(nil), schemaSnapshot.Fields...)
	clonedSnapshot.Columns = append([]session.SchemaColumn(nil), schemaSnapshot.Columns...)
	clonedSnapshot.IndexedFields = append([]string(nil), schemaSnapshot.IndexedFields...)
	clonedSnapshot.ResourceNames = append([]string(nil), schemaSnapshot.ResourceNames...)
	clonedSnapshot.AvailableGroups = append([]string(nil), schemaSnapshot.AvailableGroups...)
	clonedSnapshot.Catalog = append([]session.CatalogEntry(nil), schemaSnapshot.Catalog...)
	return clonedSnapshot
}

func cloneCandidates(candidates []session.BydbqlCandidate) []session.BydbqlCandidate {
	clonedCandidates := append([]session.BydbqlCandidate(nil), candidates...)
	for candidateIdx := range clonedCandidates {
		clonedCandidates[candidateIdx].Probe = cloneProbeSummary(candidates[candidateIdx].Probe)
	}
	return clonedCandidates
}

func clonePlannedQueries(queries []session.PlannedQuery) []session.PlannedQuery {
	clonedQueries := append([]session.PlannedQuery(nil), queries...)
	for queryIdx := range clonedQueries {
		clonedQueries[queryIdx].Groups = append([]string(nil), queries[queryIdx].Groups...)
	}
	return clonedQueries
}

func cloneExecutionResult(executionResult session.ExecutionResult) session.ExecutionResult {
	clonedResult := executionResult
	clonedResult.Columns = append([]string(nil), executionResult.Columns...)
	clonedResult.Preview = clonePreview(executionResult.Preview)
	return clonedResult
}

func cloneChatMessages(messages []session.ChatMessage) []session.ChatMessage {
	clonedMessages := append([]session.ChatMessage(nil), messages...)
	for messageIdx := range clonedMessages {
		if messages[messageIdx].Validation == nil {
			continue
		}
		clonedValidation := *messages[messageIdx].Validation
		clonedMessages[messageIdx].Validation = &clonedValidation
	}
	return clonedMessages
}

func cloneProbeSummary(probe *session.ProbeSummary) *session.ProbeSummary {
	if probe == nil {
		return nil
	}
	clonedProbe := *probe
	clonedProbe.Columns = append([]string(nil), probe.Columns...)
	clonedProbe.Preview = clonePreview(probe.Preview)
	return &clonedProbe
}

func clonePreview(preview [][]string) [][]string {
	clonedPreview := make([][]string, 0, len(preview))
	for _, row := range preview {
		clonedPreview = append(clonedPreview, append([]string(nil), row...))
	}
	return clonedPreview
}

func (toolBridge *ToolBridge) emitResult(callID, toolName string, result Result) {
	status := agent.EventStatusSucceeded
	message := "tool completed"
	if result.Err != nil {
		status = agent.EventStatusFailed
		message = result.Err.Error()
	}
	toolBridge.emit(agent.Event{
		ID:            callID,
		Kind:          agent.EventKindToolResult,
		ToolName:      toolName,
		Message:       message,
		OutputSummary: summarizeResult(result),
		Status:        status,
		CompletedAt:   toolBridge.now(),
		Err:           result.Err,
	})
}

func (toolBridge *ToolBridge) emit(event agent.Event) {
	event.Origin = agent.EventOriginToolBridge
	select {
	case toolBridge.events <- event:
	default:
	}
}

func jsonResult(value any) Result {
	encodedValue, marshalErr := json.Marshal(value)
	if marshalErr != nil {
		return Result{Err: fmt.Errorf("failed to encode tool result: %w", marshalErr)}
	}
	return Result{Content: string(encodedValue)}
}

func stringArgument(arguments map[string]any, name string) string {
	if arguments == nil {
		return ""
	}
	value, ok := arguments[name].(string)
	if !ok {
		return ""
	}
	return strings.TrimSpace(value)
}

func stringSliceArgument(arguments map[string]any, name string) []string {
	if arguments == nil {
		return nil
	}
	switch value := arguments[name].(type) {
	case string:
		return compactStrings(strings.Split(value, ","))
	case []string:
		return compactStrings(value)
	case []any:
		groups := make([]string, 0, len(value))
		for _, item := range value {
			if group, ok := item.(string); ok {
				groups = append(groups, group)
			}
		}
		return compactStrings(groups)
	default:
		return nil
	}
}

func compactStrings(values []string) []string {
	compactedValues := make([]string, 0, len(values))
	for _, value := range values {
		if trimmedValue := strings.TrimSpace(value); trimmedValue != "" {
			compactedValues = append(compactedValues, trimmedValue)
		}
	}
	return compactedValues
}

func summarizeArguments(arguments map[string]any) string {
	if len(arguments) == 0 {
		return "no parameters"
	}
	if query := stringArgument(arguments, "query"); query != "" {
		trimmedQuery := strings.Join(strings.Fields(query), " ")
		if len(trimmedQuery) > 120 {
			return "query=" + trimmedQuery[:120] + "..."
		}
		return "query=" + trimmedQuery
	}
	if planValue, hasPlan := arguments["plan"]; hasPlan {
		return "plan=" + summarizePlanArgument(planValue)
	}
	if workflowValue, hasWorkflow := arguments["workflow"]; hasWorkflow {
		return "workflow=" + summarizePlanArgument(workflowValue)
	}
	keys := make([]string, 0, len(arguments))
	for key := range arguments {
		keys = append(keys, key)
	}
	return "parameters=" + strings.Join(keys, ",")
}

func formatArgumentsDetail(arguments map[string]any) string {
	if len(arguments) == 0 {
		return ""
	}
	if query := stringArgument(arguments, "query"); query != "" {
		return "query:\n" + strings.TrimSpace(query)
	}
	if planValue, hasPlan := arguments["plan"]; hasPlan {
		return formatJSONDetailSection("plan", planValue)
	}
	if workflowValue, hasWorkflow := arguments["workflow"]; hasWorkflow {
		return formatJSONDetailSection("workflow", workflowValue)
	}
	return formatJSONDetailSection("parameters", arguments)
}

func formatJSONDetailSection(label string, value any) string {
	encodedValue, marshalErr := json.MarshalIndent(value, "", "  ")
	if marshalErr != nil {
		return label + ":\n" + fmt.Sprint(value)
	}
	return label + ":\n" + string(encodedValue)
}

func summarizePlanArgument(value any) string {
	encodedValue, marshalErr := json.Marshal(value)
	if marshalErr != nil {
		return "structured plan"
	}
	trimmedValue := strings.TrimSpace(string(encodedValue))
	if len(trimmedValue) > 120 {
		return trimmedValue[:120] + "..."
	}
	return trimmedValue
}

func summarizeResult(result Result) string {
	if result.Err != nil {
		return result.Err.Error()
	}
	if result.Content == "" {
		return "completed"
	}
	return fmt.Sprintf("result=%d characters", len([]rune(result.Content)))
}

func (toolBridge *ToolBridge) executionPolicy() approval.ExecutionPolicy {
	toolBridge.mu.RLock()
	defer toolBridge.mu.RUnlock()
	if toolBridge.activePolicy == "" {
		return approval.PolicyAskEveryTime
	}
	return toolBridge.activePolicy
}

func (toolBridge *ToolBridge) shouldAutoProbeAfterPlan() bool {
	return toolBridge.executor != nil && toolBridge.validator != nil
}

func probeSummaryPayload(probeSummary *session.ProbeSummary) map[string]any {
	if probeSummary == nil {
		return nil
	}
	return map[string]any{
		"rows":    probeSummary.Rows,
		"columns": probeSummary.Columns,
		"preview": probeSummary.Preview,
		"error":   probeSummary.Error,
		"summary": probeOutputSummary(probeSummary),
	}
}

func (toolBridge *ToolBridge) probePlannedQuery(
	ctx context.Context,
	querySession *session.QuerySession,
	query string,
	plannedQuery session.PlannedQuery,
) *session.ProbeSummary {
	probeSummary, probeErr := toolBridge.runWorkflowProbe(ctx, querySession, query, &plannedQuery)
	if probeErr != nil {
		return &session.ProbeSummary{Query: query, Error: agent.SanitizeExecutionErrorForProvider(probeErr.Error())}
	}
	return probeSummary
}

func (toolBridge *ToolBridge) runWorkflowProbe(
	ctx context.Context,
	querySession *session.QuerySession,
	query string,
	plannedQuery *session.PlannedQuery,
) (*session.ProbeSummary, error) {
	if toolBridge.validator == nil || toolBridge.executor == nil || querySession == nil || plannedQuery == nil {
		return nil, fmt.Errorf("probe bridge is not configured")
	}
	schemaSnapshot, schemaErr := toolBridge.executor.DiscoverSchema(ctx, tools.SchemaRequest{
		Type:   plannedQuery.ResourceType,
		Name:   plannedQuery.Name,
		Groups: plannedQuery.Groups,
	})
	if schemaErr != nil {
		return nil, fmt.Errorf("failed to refresh planned query schema: %w", schemaErr)
	}
	setSessionSchema(querySession, schemaSnapshot)
	validation, validationErr := toolBridge.validator.Validate(ctx, query, &schemaSnapshot)
	if validationErr != nil {
		return nil, fmt.Errorf("failed to validate probe query: %w", validationErr)
	}
	if !validation.Valid {
		return nil, fmt.Errorf("probe query failed validation: %s", validation.Message)
	}
	executionCtx, cancelQuery := context.WithCancel(ctx)
	toolBridge.setExecutionCancel(cancelQuery)
	executionResult, executeErr := toolBridge.executor.Execute(executionCtx, querySession, query)
	cancelQuery()
	toolBridge.clearExecutionCancel()
	probeSummary := executionResultToProbe(query, executionResult, executeErr)
	if executeErr != nil {
		return &probeSummary, fmt.Errorf("%s", probeSummary.Error)
	}
	if probeSummary.Error != "" {
		return &probeSummary, fmt.Errorf("%s", probeSummary.Error)
	}
	return &probeSummary, nil
}

func (toolBridge *ToolBridge) runApprovedProbe(
	ctx context.Context,
	querySession *session.QuerySession,
	query string,
	plannedQuery *session.PlannedQuery,
) (*session.ProbeSummary, error) {
	if toolBridge.validator == nil || toolBridge.executor == nil || querySession == nil || plannedQuery == nil {
		return nil, fmt.Errorf("probe bridge is not configured")
	}
	limits := tools.Limits(toolBridge.executor)
	request := approval.WithLimits(
		approval.NewRequest(
			query,
			fmt.Sprintf("%s/%s", plannedQuery.ResourceType, plannedQuery.Name),
			plannedQuery.Groups,
			approval.SourceAgentProbe,
		),
		limits.Timeout,
		maxProbePreviewRows,
	)
	decision, approvalErr := toolBridge.approvals.Request(ctx, request)
	if approvalErr != nil {
		return nil, fmt.Errorf("probe approval did not complete: %w", approvalErr)
	}
	if !decision.Approved {
		return nil, fmt.Errorf("probe rejected")
	}
	validation, validationErr := toolBridge.validator.Validate(ctx, query, &querySession.SchemaSnapshot)
	if validationErr != nil {
		return nil, fmt.Errorf("failed to revalidate probe query: %w", validationErr)
	}
	if !validation.Valid {
		return nil, fmt.Errorf("probe query failed revalidation: %s", validation.Message)
	}
	executionCtx, cancelQuery := context.WithCancel(ctx)
	toolBridge.setExecutionCancel(cancelQuery)
	executionResult, executeErr := toolBridge.executor.Execute(executionCtx, querySession, query)
	cancelQuery()
	toolBridge.clearExecutionCancel()
	probeSummary := executionResultToProbe(query, executionResult, executeErr)
	return &probeSummary, nil
}

func executionResultToProbe(query string, executionResult session.ExecutionResult, executeErr error) session.ProbeSummary {
	probeSummary := session.ProbeSummary{
		Query:   query,
		Rows:    executionResult.Rows,
		Columns: append([]string(nil), executionResult.Columns...),
	}
	previewLength := len(executionResult.Preview)
	if previewLength > maxProbePreviewRows {
		previewLength = maxProbePreviewRows
	}
	for _, row := range executionResult.Preview[:previewLength] {
		probeSummary.Preview = append(probeSummary.Preview, append([]string(nil), row...))
	}
	if executeErr != nil || executionResult.Error != "" {
		rawError := executionResult.Error
		if executeErr != nil {
			rawError = executeErr.Error()
		}
		probeSummary.Error = agent.SanitizeExecutionErrorForProvider(rawError)
		if probeSummary.Error == "" {
			probeSummary.Error = "BYDBQL probe failed"
		}
	}
	return probeSummary
}

func schemaReadyForPlan(querySession *session.QuerySession, resource planner.Resource) bool {
	if querySession == nil {
		return false
	}
	snapshot := querySession.SchemaSnapshot
	if !snapshot.Loaded || len(snapshot.Columns) == 0 {
		return false
	}
	return planResourceMatchesSnapshot(resource, snapshot)
}

func planResourceMatchesSnapshot(resource planner.Resource, snapshot session.SchemaSnapshot) bool {
	if !strings.EqualFold(strings.TrimSpace(snapshot.Name), strings.TrimSpace(resource.Name)) {
		return false
	}
	if snapshot.Type != "" && resource.Type != "" && !catalogTypesCompatible(resource.Type, snapshot.Type) {
		return false
	}
	return planGroupsMatchSnapshot(resource.Groups, snapshot.Groups)
}

func planGroupsMatchSnapshot(planGroups, snapshotGroups []string) bool {
	if len(planGroups) == 0 {
		return len(snapshotGroups) > 0
	}
	for _, planGroup := range planGroups {
		groupMatched := false
		for _, snapshotGroup := range snapshotGroups {
			if strings.EqualFold(strings.TrimSpace(planGroup), strings.TrimSpace(snapshotGroup)) {
				groupMatched = true
				break
			}
		}
		if !groupMatched {
			return false
		}
	}
	return true
}

func schemaNotReadyMessage(step int, resource planner.Resource) string {
	groupLabel := strings.Join(resource.Groups, ", ")
	if groupLabel == "" {
		groupLabel = "<group>"
	}
	return fmt.Sprintf(
		"query plan step %d: call describe_schema for %s %s in %s before propose_query_plan; use only typed columns from describe_schema",
		step,
		resource.Type,
		resource.Name,
		groupLabel,
	)
}

func probeOutputSummary(probeSummary *session.ProbeSummary) string {
	if probeSummary == nil {
		return "no probe result"
	}
	if probeSummary.Error != "" {
		return probeSummary.Error
	}
	return fmt.Sprintf("rows=%d columns=%d", probeSummary.Rows, len(probeSummary.Columns))
}
