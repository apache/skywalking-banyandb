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

// Package bridge provides the private, bydbctl-owned tool set exposed to agents.
package bridge

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/agent"
	tuibydbql "github.com/apache/skywalking-banyandb/bydbctl/internal/tui/bydbql"
	tuicatalog "github.com/apache/skywalking-banyandb/bydbctl/internal/tui/catalog"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/planner"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/tools"
)

const (
	eventBufferSize       = 64
	maxSchemaDescriptions = agent.DefaultMaxSchemaDescriptions
	ToolListGroupsSchemas = "list_groups_schemas"
	ToolDescribeSchema    = "describe_schema"
	ToolProposeQueryPlan  = "propose_query_plan"
	ToolValidateBydbQL    = "validate_bydbql"
	ToolExecuteBydbQL     = "execute_bydbql"
)

// Validator checks a BYDBQL query without executing it.
type Validator interface {
	Validate(ctx context.Context, query string, schema *session.SchemaSnapshot) (session.ValidationReport, error)
}

// Config creates a private tool bridge.
type Config struct {
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
	Err     error
	Content string
}

// ToolBridge holds all tool execution and visible lifecycle events behind a small interface.
type ToolBridge struct {
	executor  tools.Executor
	validator Validator
	now       func() time.Time
	events    chan agent.Event

	querySession       *session.QuerySession
	cancelQuery        context.CancelFunc
	rankedCandidates   []session.CatalogEntry
	mu                 sync.RWMutex
	callMu             sync.Mutex
	executionMu        sync.Mutex
	planAttempts       int
	schemaDescriptions int
}

// New creates a private tool bridge. The bridge never receives server credentials.
func New(config Config) *ToolBridge {
	return &ToolBridge{
		executor:  config.Executor,
		validator: config.Validator,
		now:       time.Now,
		events:    make(chan agent.Event, eventBufferSize),
	}
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

// Cancel makes a best effort to cancel the active query request.
func (toolBridge *ToolBridge) Cancel() {
	if toolBridge == nil {
		return
	}
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
	case ToolExecuteBydbQL:
		result = toolBridge.executeBydbQL(ctx, call.Arguments)
	default:
		result = Result{Err: fmt.Errorf("tool %q is not registered", toolName)}
	}
	toolBridge.emitResult(callID, toolName, call.Arguments, result)
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
		querySession.SchemaSnapshot.AvailableGroups = append([]string(nil), catalog.Groups...)
		querySession.SchemaSnapshot.Catalog = append([]session.CatalogEntry(nil), catalog.Entries...)
		goal = strings.TrimSpace(querySession.DiscoveryGoal)
		if goal == "" {
			goal = querySession.UserGoal
		}
	}
	candidates := toolBridge.rankedCatalogCandidates()
	if len(candidates) == 0 {
		candidates = tuicatalog.Rank(goal, catalog.Entries, maxCatalogCandidates)
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
	if querySession != nil && !catalogContainsResource(querySession.SchemaSnapshot.Catalog, resourceType, resourceName, groups) {
		return Result{Err: fmt.Errorf("schema description requires a resource from the discovered catalog")}
	}
	if !toolBridge.reserveSchemaDescription() {
		return Result{Err: fmt.Errorf("schema discovery limit reached after %d detailed schema inspections", maxSchemaDescriptions)}
	}
	snapshot, schemaErr := toolBridge.executor.DiscoverSchema(ctx, tools.SchemaRequest{
		Type:   resourceType,
		Name:   resourceName,
		Groups: groups,
	})
	if schemaErr != nil {
		return Result{Err: fmt.Errorf("schema description failed")}
	}
	snapshot.EnsureFingerprint()
	if querySession != nil {
		setSessionSchema(querySession, snapshot)
	}
	toolBridge.mu.Lock()
	toolBridge.planAttempts = 0
	toolBridge.mu.Unlock()
	response := map[string]any{
		"type":               snapshot.Type,
		"name":               snapshot.Name,
		"groups":             snapshot.Groups,
		"tags":               snapshot.Tags,
		"fields":             snapshot.Fields,
		"columns":            columnsForProvider(snapshot.Columns),
		"indexed_fields":     snapshot.IndexedFields,
		"sortable_indexes":   snapshot.SortableIndexes,
		"schema_fingerprint": snapshot.Fingerprint,
		"plan_constraints":   planConstraintsForSnapshot(snapshot),
	}
	if snapshot.Type == session.ResourceTypeTrace {
		response["trace_id_tag"] = snapshot.TraceIDTag
		response["timestamp_tag"] = snapshot.TimestampTag
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

func resourceIsRanked(candidates []session.CatalogEntry, resourceType session.ResourceType, resourceName string, groups []string) bool {
	if len(candidates) == 0 {
		return false
	}
	for _, group := range groups {
		found := false
		for _, entry := range candidates {
			if catalogTypesCompatible(resourceType, entry.Type) &&
				entry.Name == resourceName &&
				entry.Group == group {
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
	return planType == catalogType
}

func catalogContainsResource(entries []session.CatalogEntry, resourceType session.ResourceType, resourceName string, groups []string) bool {
	if len(entries) == 0 {
		return true
	}
	return resourceIsRanked(entries, resourceType, resourceName, groups)
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
	querySession.ActivateSchema(schemaSnapshot)
}

func (toolBridge *ToolBridge) proposeQueryPlan(ctx context.Context, callID string, arguments map[string]any) Result {
	if toolBridge.executor == nil || toolBridge.validator == nil {
		return Result{Err: fmt.Errorf("query plan bridge is not configured")}
	}
	querySession := toolBridge.session()
	if querySession == nil {
		return Result{Err: fmt.Errorf("query session is not configured")}
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
	plans, planErr := plannedQueries(arguments)
	if planErr != nil {
		diagnostic := planner.Diagnostic{
			Code:    "PLAN_DECODE_FAILED",
			Path:    "/",
			Message: planErr.Error(),
		}
		return jsonResult(planFailurePayload(querySession, diagnostic, 0, attempt, ""))
	}
	for planIndex, plan := range plans {
		if !resourceIsDiscoverable(querySession.SchemaSnapshot.Catalog, plan.Resource) {
			return Result{Err: fmt.Errorf("query plan step %d selects a resource outside the discovered catalog", planIndex+1)}
		}
	}
	compiledQueries := make([]planner.CompiledQuery, 0, len(plans))
	plannedQueries := make([]session.PlannedQuery, 0, len(plans))
	var selectedSnapshot session.SchemaSnapshot
	for planIndex, plan := range plans {
		if !resourceIsDiscoverable(querySession.SchemaSnapshot.Catalog, plan.Resource) {
			return Result{Err: fmt.Errorf("query plan step %d selects a resource outside the discovered catalog", planIndex+1)}
		}
		snapshot, cached := querySession.CachedSchema(plan.Resource.Type, plan.Resource.Name, plan.Resource.Groups)
		if !cached {
			var schemaErr error
			snapshot, schemaErr = toolBridge.executor.DiscoverSchema(ctx, schemaRequestForPlan(plan))
			if schemaErr != nil {
				return Result{Err: fmt.Errorf("failed to discover schema for plan step %d: %w", planIndex+1, schemaErr)}
			}
			snapshot = querySession.CacheSchema(snapshot)
		}
		if !snapshot.Loaded || len(snapshot.Columns) == 0 {
			return jsonResult(planFailurePayload(querySession, planner.Diagnostic{
				Code:    "SCHEMA_NOT_READY",
				Path:    "/resource",
				Message: schemaNotReadyMessage(planIndex+1, plan.Resource),
			}, planIndex+1, attempt, ""))
		}
		compiled, compileErr := planner.Compile(plan, snapshot)
		if compileErr != nil {
			draftQuery := planner.CompileDisplayDraft(plan)
			toolBridge.emitProposeCandidate(callID, draftQuery, true, compileErr.Error())
			return jsonResult(planFailurePayload(querySession, planner.DescribeError(compileErr), planIndex+1, attempt, draftQuery))
		}
		validation, validationErr := toolBridge.validator.Validate(ctx, compiled.Query, &snapshot)
		if validationErr != nil {
			return Result{Err: fmt.Errorf("failed to validate query plan step %d: %w", planIndex+1, validationErr)}
		}
		if !validation.Valid {
			toolBridge.emitProposeCandidate(callID, compiled.Query, true, validation.Message)
			return jsonResult(planFailurePayload(querySession, planner.Diagnostic{
				Code:    "BYDBQL_VALIDATION_FAILED",
				Path:    "/",
				Message: validation.Message,
			}, planIndex+1, attempt, compiled.Query))
		}
		if planIndex == 0 {
			selectedSnapshot = snapshot
		}
		compiledQueries = append(compiledQueries, compiled)
		plannedQueries = append(plannedQueries, session.PlannedQuery{
			ID:                compiled.ID,
			Query:             compiled.Query,
			ResourceType:      compiled.Resource.Type,
			Name:              compiled.Resource.Name,
			SchemaFingerprint: snapshot.Fingerprint,
			Groups:            append([]string(nil), compiled.Resource.Groups...),
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
	toolBridge.emitProposeCandidate(callID, firstQuery.Query, false, "query plan compiled through controlled tool")
	return jsonResult(response)
}

func schemaRequestForPlan(plan planner.QueryPlan) tools.SchemaRequest {
	return tools.SchemaRequest{
		Type:   plan.Resource.Type,
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
				entry.Name == resource.Name &&
				entry.Group == group {
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
	if len(arguments) != 1 {
		return nil, fmt.Errorf("propose_query_plan accepts exactly one top-level field: plan or workflow")
	}
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
	decoder.DisallowUnknownFields()
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

func (toolBridge *ToolBridge) executeBydbQL(ctx context.Context, arguments map[string]any) Result {
	if toolBridge.validator == nil || toolBridge.executor == nil {
		return Result{Err: fmt.Errorf("BYDBQL execution bridge is not configured")}
	}
	querySession := toolBridge.session()
	if querySession == nil {
		return Result{Err: fmt.Errorf("query session is not configured")}
	}
	query := stringArgument(arguments, "query")
	if !tuibydbql.IsReadOnly(query) {
		return Result{Err: fmt.Errorf("execute_bydbql accepts only one read-only SELECT or SHOW TOP statement")}
	}
	plannedQuery := querySession.CurrentPlannedQuery()
	if plannedQuery == nil || plannedQuery.Query != query {
		return Result{Err: fmt.Errorf("execute_bydbql requires propose_query_plan to return valid=true first; validate_bydbql alone does not register a candidate")}
	}
	schemaSnapshot, schemaErr := toolBridge.refreshPlannedSchema(ctx, querySession, *plannedQuery)
	if schemaErr != nil {
		return Result{Err: fmt.Errorf("failed to refresh planned query schema: %w", schemaErr)}
	}
	validation, validationErr := toolBridge.validator.Validate(ctx, query, &schemaSnapshot)
	if validationErr != nil {
		return Result{Err: fmt.Errorf("failed to validate execution query: %w", validationErr)}
	}
	if !validation.Valid {
		return jsonResult(map[string]any{"valid": false, "message": validation.Message})
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

func (toolBridge *ToolBridge) refreshPlannedSchema(
	ctx context.Context,
	querySession *session.QuerySession,
	plannedQuery session.PlannedQuery,
) (session.SchemaSnapshot, error) {
	schemaSnapshot, schemaErr := toolBridge.executor.DiscoverSchema(ctx, tools.SchemaRequest{
		Type:   plannedQuery.ResourceType,
		Name:   plannedQuery.Name,
		Groups: plannedQuery.Groups,
	})
	if schemaErr != nil {
		return session.SchemaSnapshot{}, schemaErr
	}
	schemaSnapshot.EnsureFingerprint()
	if plannedQuery.SchemaFingerprint != "" && plannedQuery.SchemaFingerprint != schemaSnapshot.Fingerprint {
		return session.SchemaSnapshot{}, fmt.Errorf("resource schema changed after plan compilation; regenerate the query plan")
	}
	querySession.ActivateSchema(schemaSnapshot)
	return schemaSnapshot, nil
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

func (toolBridge *ToolBridge) emitResult(callID, toolName string, arguments map[string]any, result Result) {
	status := agent.EventStatusSucceeded
	message := "tool completed"
	if result.Err != nil {
		status = agent.EventStatusFailed
		message = result.Err.Error()
	}
	candidate := ""
	if toolName == ToolExecuteBydbQL {
		candidate = stringArgument(arguments, "query")
	}
	toolBridge.emit(agent.Event{
		ID:            callID,
		Candidate:     candidate,
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
