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

// Package workflow owns the deterministic BYDBQL assistant state machine.
package workflow

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/agent"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/bridge"
	tuibysql "github.com/apache/skywalking-banyandb/bydbctl/internal/tui/bydbql"
	tuicatalog "github.com/apache/skywalking-banyandb/bydbctl/internal/tui/catalog"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/execution"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/tools"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/tuirun"
)

const (
	defaultGroupName = "default"
	defaultTimeStart = "-30m"
	defaultLimit     = 10
	defaultTopN      = 10
)

// Runner coordinates deterministic workflow phases and agent turns.
type Runner struct {
	agentGateway agent.Gateway
	validator    execution.Validator
	executor     tools.Executor
	toolBridge   *bridge.ToolBridge
	execution    *execution.Engine
	now          func() time.Time
}

// Config configures a Runner.
type Config struct {
	AgentGateway agent.Gateway
	Validator    execution.Validator
	Executor     tools.Executor
	ToolBridge   *bridge.ToolBridge
}

// NewRunner creates a WorkflowRunner.
func NewRunner(config Config) *Runner {
	validator := config.Validator
	if validator == nil {
		validator = tuibysql.NewSemanticValidator()
	}
	executor := config.Executor
	if executor == nil {
		executor = tools.NewReadOnlyExecutor()
	}
	return &Runner{
		agentGateway: config.AgentGateway,
		validator:    validator,
		executor:     executor,
		toolBridge:   config.ToolBridge,
		execution:    execution.New(executor, validator),
		now:          time.Now,
	}
}

// TurnUpdate is one real-time agent or controlled-tool event, or the completed turn result.
type TurnUpdate struct {
	Err          error
	Event        *agent.Event
	QuerySession *session.QuerySession
	Done         bool
}

// StartOptions contains user-provided session slots.
type StartOptions struct {
	ResourceType   session.ResourceType
	TimeRange      session.TimeRange
	Goal           string
	ResourceName   string
	Groups         []string
	NameProvided   bool
	GroupsProvided bool
	TypeProvided   bool
}

// ReviseWithAgent asks the configured agent to revise the current BYDBQL candidate.
func (runner *Runner) ReviseWithAgent(ctx context.Context, querySession *session.QuerySession) ([]agent.Event, error) {
	return runner.RunAgentTurn(ctx, querySession, "")
}

// RunAgentTurn runs one user-facing agent turn with an optional per-round hint.
func (runner *Runner) RunAgentTurn(ctx context.Context, querySession *session.QuerySession, turnHint string) ([]agent.Event, error) {
	updates, startErr := runner.StartAgentTurn(ctx, querySession, turnHint)
	if startErr != nil {
		return nil, startErr
	}
	var events []agent.Event
	for update := range updates {
		if update.Event != nil {
			events = append(events, *update.Event)
		}
		if update.Done {
			return events, update.Err
		}
	}
	return events, errors.New("agent turn ended without a completion update")
}

// StartAgentTurn starts one agent turn and streams its visible updates as they arrive.
func (runner *Runner) StartAgentTurn(ctx context.Context, querySession *session.QuerySession, turnHint string) (<-chan TurnUpdate, error) {
	if querySession == nil {
		return nil, errors.New("query session is required")
	}
	if runner.agentGateway == nil {
		return nil, errors.New("agent gateway is not configured")
	}
	if bootstrapErr := runner.refreshDiscoveryForTurn(ctx, querySession, strings.TrimSpace(turnHint)); bootstrapErr != nil {
		querySession.AddTranscript("workflow", "schema bootstrap: "+bootstrapErr.Error(), runner.now())
	}
	trimmedTurnHint := strings.TrimSpace(turnHint)
	rankingGoal := CatalogRankingGoal(querySession.UserGoal, trimmedTurnHint)
	querySession.DiscoveryGoal = rankingGoal
	if runner.toolBridge != nil {
		runner.toolBridge.SetSession(querySession)
		runner.toolBridge.SetRankedCandidates(tuicatalog.Rank(rankingGoal, querySession.SchemaSnapshot.Catalog, 5))
	}
	querySession.Phase = session.PhaseAgentDraft
	agentSessionID := strings.TrimSpace(querySession.AgentSessionID)
	providerSessionContinues := agentSessionID != ""
	if agentSessionID == "" {
		agentSession, startErr := runner.agentGateway.Start(ctx, agent.StartRequest{Provider: "bydbctl-agent"})
		if startErr != nil {
			querySession.Phase = session.PhaseError
			return nil, fmt.Errorf("failed to start agent session: %w", startErr)
		}
		agentSessionID = agentSession.ID
		querySession.AgentSessionID = agentSessionID
	}
	if trimmedTurnHint != "" {
		querySession.AddTranscript("user", trimmedTurnHint, runner.now())
		querySession.AddChatMessage(session.ChatMessage{
			Role:      session.ChatRoleUser,
			Content:   trimmedTurnHint,
			CreatedAt: runner.now(),
		})
		if strings.TrimSpace(querySession.UserGoal) == "" {
			querySession.UserGoal = trimmedTurnHint
		}
	}
	templateHint := ""
	if strings.TrimSpace(querySession.ResourceName) != "" {
		templateHint = BuildTemplateQuery(
			querySession.ResourceType,
			querySession.ResourceName,
			querySession.Groups,
			querySession.TimeRange,
		)
	}
	hints := ClassifyIntent(querySession)
	rankedCatalog := tuicatalog.Rank(rankingGoal, querySession.SchemaSnapshot.Catalog, maxPromptCatalogCandidates)
	payload := agent.BuildAgentTurnRequest(querySession, hints, templateHint, trimmedTurnHint)
	payload.Schema.CatalogTotal = len(querySession.SchemaSnapshot.Catalog)
	if len(rankedCatalog) > 0 {
		payload.Schema.RankedCandidates = agent.CatalogEntrySummaries(rankedCatalog)
		payload.Schema.Catalog = payload.Schema.RankedCandidates
	}
	payload.PlanExample = buildStructuredPlanExample(querySession, hints)
	if providerSessionContinues && gatewayMaintainsConversationHistory(runner.agentGateway) {
		payload.Conversation = nil
	}
	agentEvents, sendErr := runner.sendAgentTurn(ctx, agentSessionID, payload)
	if sendErr != nil {
		querySession.Phase = session.PhaseError
		return nil, sendErr
	}
	updates := make(chan TurnUpdate, 16)
	tuirun.Go(ctx, "agent-turn-stream", func(turnCtx context.Context) {
		runner.streamAgentTurn(turnCtx, querySession, trimmedTurnHint, agentEvents, updates)
	})
	return updates, nil
}

// StopAgentTurn asks the provider to interrupt the active turn and cancels in-flight queries.
func (runner *Runner) StopAgentTurn(ctx context.Context, querySession *session.QuerySession) error {
	if runner.toolBridge != nil {
		runner.toolBridge.Cancel()
	}
	if querySession == nil || strings.TrimSpace(querySession.AgentSessionID) == "" || runner.agentGateway == nil {
		return nil
	}
	if interruptErr := runner.agentGateway.Interrupt(ctx, querySession.AgentSessionID); interruptErr != nil {
		return fmt.Errorf("failed to interrupt agent turn: %w", interruptErr)
	}
	querySession.AddTranscript("workflow", "agent turn canceled", runner.now())
	return nil
}

func (runner *Runner) refreshDiscoveryForTurn(ctx context.Context, querySession *session.QuerySession, turnHint string) error {
	if querySession == nil || runner.executor == nil {
		return nil
	}
	if len(querySession.SchemaSnapshot.Catalog) == 0 {
		return nil
	}
	rankingGoal := CatalogRankingGoal(querySession.UserGoal, turnHint)
	if strings.TrimSpace(turnHint) == "" {
		return runner.bootstrapAutonomousSchema(ctx, querySession, rankingGoal)
	}
	var match tuicatalog.Match
	if explicitEntry := FindExplicitResourceMention(rankingGoal, querySession.SchemaSnapshot.Catalog); explicitEntry != nil {
		match = tuicatalog.Match{
			Matched: true,
			Group:   explicitEntry.Group,
			Name:    explicitEntry.Name,
			Type:    explicitEntry.Type,
			Score:   100,
		}
	} else {
		match = tuicatalog.MatchGoal(
			rankingGoal,
			session.SchemaCatalog{Entries: querySession.SchemaSnapshot.Catalog},
			"",
			"",
			nil,
		)
	}
	if !match.Matched {
		return nil
	}
	currentName := strings.TrimSpace(querySession.ResourceName)
	currentGroup := ""
	if len(querySession.Groups) > 0 {
		currentGroup = querySession.Groups[0]
	}
	if currentName != "" &&
		currentName == match.Name &&
		(currentGroup == "" || currentGroup == match.Group) {
		return nil
	}
	schemaSnapshot, schemaErr := runner.executor.DiscoverSchema(ctx, tools.SchemaRequest{
		Type:   match.Type,
		Name:   match.Name,
		Groups: []string{match.Group},
	})
	if schemaErr != nil {
		return fmt.Errorf("failed to refresh matched schema: %w", schemaErr)
	}
	querySession.ActivateSchema(schemaSnapshot)
	querySession.AutoMatched = true
	querySession.CandidateSuperseded = true
	querySession.Validation = session.ValidationReport{}
	querySession.AddTranscript(
		"workflow",
		fmt.Sprintf("re-matched resource %s %s in %s from turn hint", match.Type, match.Name, match.Group),
		runner.now(),
	)
	if runner.toolBridge != nil {
		runner.toolBridge.SetSession(querySession)
		runner.toolBridge.SetRankedCandidates(tuicatalog.Ensure(
			tuicatalog.Rank(rankingGoal, querySession.SchemaSnapshot.Catalog, 5),
			session.CatalogEntry{Group: match.Group, Type: match.Type, Name: match.Name},
			5,
		))
	}
	return nil
}

func (runner *Runner) bootstrapAutonomousSchema(ctx context.Context, querySession *session.QuerySession, rankingGoal string) error {
	if querySession == nil || runner.executor == nil {
		return nil
	}
	if strings.TrimSpace(querySession.ResourceName) != "" {
		return nil
	}
	if len(querySession.SchemaSnapshot.Catalog) == 0 {
		return nil
	}
	if strings.TrimSpace(rankingGoal) == "" {
		rankingGoal = strings.TrimSpace(querySession.UserGoal)
	}
	match := tuicatalog.MatchGoal(
		rankingGoal,
		session.SchemaCatalog{Entries: querySession.SchemaSnapshot.Catalog},
		"",
		"",
		nil,
	)
	if !match.Matched {
		return nil
	}
	schemaSnapshot, schemaErr := runner.executor.DiscoverSchema(ctx, tools.SchemaRequest{
		Type:   match.Type,
		Name:   match.Name,
		Groups: []string{match.Group},
	})
	if schemaErr != nil {
		return fmt.Errorf("failed to preload matched schema: %w", schemaErr)
	}
	querySession.ActivateSchema(schemaSnapshot)
	querySession.AutoMatched = true
	querySession.AddTranscript(
		"workflow",
		fmt.Sprintf("preloaded schema for %s %s in %s", match.Type, match.Name, match.Group),
		runner.now(),
	)
	if runner.toolBridge != nil {
		runner.toolBridge.SetSession(querySession)
		matchedEntry := session.CatalogEntry{Group: match.Group, Type: match.Type, Name: match.Name}
		runner.toolBridge.SetRankedCandidates(tuicatalog.Ensure(
			tuicatalog.Rank(rankingGoal, querySession.SchemaSnapshot.Catalog, 5),
			matchedEntry,
			5,
		))
	}
	return nil
}

func (runner *Runner) streamAgentTurn(
	ctx context.Context,
	querySession *session.QuerySession,
	turnHint string,
	agentEvents <-chan agent.Event,
	updates chan<- TurnUpdate,
) {
	defer close(updates)
	var collectedEvents []agent.Event
	var toolEvents <-chan agent.Event
	if runner.toolBridge != nil {
		toolEvents = runner.toolBridge.Events()
	}
	for agentEvents != nil {
		select {
		case <-ctx.Done():
			runner.reportCancelledTurn(ctx, querySession, updates)
			return
		case event, open := <-agentEvents:
			if !open {
				agentEvents = nil
				collectedEvents = drainBridgeEvents(toolEvents, querySession, updates, collectedEvents)
				continue
			}
			collectedEvents = append(collectedEvents, event)
			updates <- TurnUpdate{Event: &event, QuerySession: querySession}
			if event.Kind == agent.EventKindError {
				runner.syncToolBridgeSession(querySession)
				querySession.Phase = session.PhaseError
				errorValue := event.Err
				if errorValue == nil {
					errorValue = fmt.Errorf("agent error: %s", event.Message)
				}
				updates <- TurnUpdate{Done: true, Err: errorValue, QuerySession: querySession}
				return
			}
		case event := <-toolEvents:
			collectedEvents = append(collectedEvents, event)
			updates <- TurnUpdate{Event: &event, QuerySession: querySession}
		}
	}
	// Canceling the turn also closes the provider stream, so both cases above can be ready at once
	// and select may take the closed stream. The context is rechecked here so a truncated stream is
	// never mistaken for a finished turn and its partial output never reaches the conversation.
	if ctx.Err() != nil {
		runner.reportCancelledTurn(ctx, querySession, updates)
		return
	}
	runner.syncToolBridgeSession(querySession)
	completeErr := runner.completeAgentTurn(ctx, querySession, turnHint, collectedEvents)
	updates <- TurnUpdate{Done: true, Err: completeErr, QuerySession: querySession}
}

// reportCancelledTurn ends a turn the user stopped, leaving the workspace ready for the next one.
func (runner *Runner) reportCancelledTurn(ctx context.Context, querySession *session.QuerySession, updates chan<- TurnUpdate) {
	querySession.Phase = session.PhaseReady
	updates <- TurnUpdate{Done: true, Err: ctx.Err(), QuerySession: querySession}
}

func (runner *Runner) syncToolBridgeSession(querySession *session.QuerySession) {
	if runner.toolBridge == nil || querySession == nil {
		return
	}
	bridgeSession := runner.toolBridge.SessionSnapshot()
	if bridgeSession == nil {
		return
	}
	querySession.ResourceType = bridgeSession.ResourceType
	querySession.ResourceName = bridgeSession.ResourceName
	querySession.Groups = append([]string(nil), bridgeSession.Groups...)
	querySession.SchemaSnapshot = bridgeSession.SchemaSnapshot
	querySession.Schemas = bridgeSession.Schemas
	querySession.PlannedQueries = append([]session.PlannedQuery(nil), bridgeSession.PlannedQueries...)
	querySession.ActivePlanStep = bridgeSession.ActivePlanStep
	querySession.ExecutionResult = bridgeSession.ExecutionResult
}

func drainBridgeEvents(
	toolEvents <-chan agent.Event,
	querySession *session.QuerySession,
	updates chan<- TurnUpdate,
	collectedEvents []agent.Event,
) []agent.Event {
	if toolEvents == nil {
		return collectedEvents
	}
	for {
		select {
		case event := <-toolEvents:
			collectedEvents = append(collectedEvents, event)
			updates <- TurnUpdate{Event: &event, QuerySession: querySession}
		default:
			return collectedEvents
		}
	}
}

func (runner *Runner) completeAgentTurn(ctx context.Context, querySession *session.QuerySession, turnHint string, turnEvents []agent.Event) error {
	candidate := finalCandidate(turnEvents)
	if strings.TrimSpace(candidate) == "" {
		if containsUncontrolledBydbql(turnEvents) {
			querySession.Phase = session.PhaseValidate
			return errors.New("agent embedded BYDBQL outside the controlled query plan tool")
		}
		response := finalClarification(turnEvents)
		phase := session.PhaseClarifying
		if response == "" {
			response = strings.TrimSpace(agentOutputText(turnEvents))
			phase = session.PhaseConversation
		}
		if response == "" {
			return errors.New("agent returned no structured BYDBQL candidate and no readable output")
		}
		runner.recordConversation(querySession, turnHint, response, phase)
		return nil
	}
	validation, validationErr := runner.validator.Validate(ctx, candidate, &querySession.SchemaSnapshot)
	if validationErr != nil {
		querySession.Phase = session.PhaseError
		return fmt.Errorf("failed to validate agent candidate: %w", validationErr)
	}
	explanation := NormalizeAgentDisplayText(finalExplanation(turnEvents))
	querySession.AddCandidateVersion(candidate, explanation, session.CandidateSourceAgent, validation, runner.now())
	querySession.AddConversationTurn(session.ConversationTurn{
		Hint:      turnHint,
		Response:  explanation,
		Candidate: candidate,
		CreatedAt: runner.now(),
	})
	assistantMessage := session.ChatMessage{
		Role:      session.ChatRoleAssistant,
		Content:   explanation,
		Candidate: candidate,
		CreatedAt: runner.now(),
	}
	if validation.Message != "" || validation.Valid {
		copiedValidation := validation
		assistantMessage.Validation = &copiedValidation
	}
	querySession.AddChatMessage(assistantMessage)
	querySession.AddTranscript("agent", explanation, runner.now())
	if validation.Valid {
		querySession.Phase = session.PhaseReady
		return nil
	}
	querySession.Phase = session.PhaseValidate
	return nil
}

// recordConversation stores a turn that answered in words instead of proposing a query.
//
// The message keeps its original line breaks so the conversation panel can format the body, and it
// records whether the turn is finished or waiting on the user.
func (runner *Runner) recordConversation(querySession *session.QuerySession, turnHint, response string, phase session.Phase) {
	displayResponse := NormalizeAgentDisplayText(response)
	querySession.Phase = phase
	querySession.AddConversationTurn(session.ConversationTurn{
		Hint:      turnHint,
		Response:  displayResponse,
		CreatedAt: runner.now(),
	})
	querySession.AddChatMessage(session.ChatMessage{
		Role:      session.ChatRoleAssistant,
		Kind:      chatMessageKindForPhase(phase),
		Content:   displayResponse,
		Detail:    strings.TrimSpace(response),
		CreatedAt: runner.now(),
	})
	querySession.AddTranscript("agent", displayResponse, runner.now())
}

// chatMessageKindForPhase reports what a candidate-free turn is waiting on.
func chatMessageKindForPhase(phase session.Phase) session.ChatMessageKind {
	if phase == session.PhaseClarifying {
		return session.ChatMessageKindClarification
	}
	return session.ChatMessageKindAnswer
}

// ValidateManualQuery validates an edited BYDBQL query and records it as a manual candidate.
func (runner *Runner) ValidateManualQuery(ctx context.Context, querySession *session.QuerySession, query string) error {
	if querySession == nil {
		return errors.New("query session is required")
	}
	validation, validationErr := runner.validator.Validate(ctx, query, &querySession.SchemaSnapshot)
	if validationErr != nil {
		querySession.Phase = session.PhaseError
		return fmt.Errorf("failed to validate manual query: %w", validationErr)
	}
	querySession.SetPlannedQueries(nil)
	querySession.AddCandidateVersion(
		strings.TrimSpace(query),
		"manual edit from TUI",
		session.CandidateSourceManual,
		validation,
		runner.now(),
	)
	querySession.AddTranscript("workflow", "validated manual BYDBQL edit", runner.now())
	if validation.Valid {
		querySession.Phase = session.PhaseReady
		return nil
	}
	querySession.Phase = session.PhaseValidate
	return nil
}

// ExecuteCurrent runs the exact current BYDBQL candidate once.
func (runner *Runner) ExecuteCurrent(ctx context.Context, querySession *session.QuerySession) error {
	outcome, executeErr := runner.execution.ExecuteCurrent(ctx, querySession)
	if querySession != nil && outcome.Phase != "" {
		querySession.Phase = outcome.Phase
	}
	if executeErr != nil {
		return executeErr
	}
	if !outcome.Validation.Valid {
		return fmt.Errorf("query failed revalidation: %s", outcome.Validation.Message)
	}
	if outcome.Result.Hint != "" {
		querySession.AddTranscript("workflow", outcome.Result.Hint, runner.now())
	}
	querySession.AddTranscript("workflow", outcome.Result.Summary, runner.now())
	if outcome.Next != nil {
		return runner.prepareNextPlanStep(ctx, querySession, *outcome.Next)
	}
	querySession.Phase = session.PhaseExecuted
	return nil
}

func (runner *Runner) prepareNextPlanStep(
	ctx context.Context,
	querySession *session.QuerySession,
	nextPlanStep session.PlannedQuery,
) error {
	schemaSnapshot, schemaErr := runner.executor.DiscoverSchema(ctx, tools.SchemaRequest{
		Type:   nextPlanStep.ResourceType,
		Name:   nextPlanStep.Name,
		Groups: nextPlanStep.Groups,
	})
	if schemaErr != nil {
		querySession.Phase = session.PhaseError
		return fmt.Errorf("failed to refresh next workflow schema: %w", schemaErr)
	}
	schemaSnapshot = querySession.CacheSchema(schemaSnapshot)
	if nextPlanStep.SchemaFingerprint != "" && nextPlanStep.SchemaFingerprint != schemaSnapshot.Fingerprint {
		querySession.Phase = session.PhaseValidate
		return errors.New("next workflow resource schema changed after plan compilation; regenerate the query plan")
	}
	querySession.ActivateSchema(schemaSnapshot)
	validation, validationErr := runner.validator.Validate(ctx, nextPlanStep.Query, &querySession.SchemaSnapshot)
	if validationErr != nil {
		querySession.Phase = session.PhaseError
		return fmt.Errorf("failed to validate next workflow statement: %w", validationErr)
	}
	querySession.AddCandidateVersion(
		nextPlanStep.Query,
		"next independently approved workflow statement",
		session.CandidateSourceAgent,
		validation,
		runner.now(),
	)
	querySession.AddTranscript("workflow", "next workflow statement is ready", runner.now())
	if !validation.Valid {
		querySession.Phase = session.PhaseValidate
		return fmt.Errorf("next workflow statement failed validation: %s", validation.Message)
	}
	querySession.Phase = session.PhaseReady
	return nil
}

func buildStructuredPlanExample(querySession *session.QuerySession, hints agent.QueryHints) map[string]any {
	if querySession == nil || strings.TrimSpace(querySession.ResourceName) == "" {
		return nil
	}
	groups := normalizeGroups(querySession.Groups)
	if len(groups) == 0 {
		return nil
	}
	timeStart := strings.TrimSpace(hints.TimeRangeHint)
	if timeStart == "" {
		timeStart = strings.TrimSpace(querySession.TimeRange.Start)
	}
	if timeStart == "" {
		timeStart = "-30m"
	}
	resource := map[string]any{
		"name":   querySession.ResourceName,
		"groups": groups,
	}
	planExample := map[string]any{
		"resource": resource,
	}
	if hints.PreferShowTop && querySession.ResourceType != session.ResourceTypeTopN {
		return nil
	}
	if querySession.ResourceType == session.ResourceTypeTopN {
		resource["type"] = session.ResourceTypeTopN.String()
		topN := hints.LimitHint
		if topN <= 0 {
			topN = defaultTopN
		}
		planExample["time_range"] = map[string]any{"start": timeStart}
		planExample["aggregate"] = map[string]any{"function": "SUM"}
		planExample["order_by"] = map[string]any{"direction": "DESC"}
		planExample["top_n"] = topN
		return map[string]any{"plan": planExample}
	}
	resource["type"] = querySession.ResourceType.String()
	planExample["projection_mode"] = "ALL"
	if querySession.ResourceType != session.ResourceTypeProperty {
		planExample["time_range"] = map[string]any{"start": timeStart}
	}
	limit := hints.LimitHint
	if limit <= 0 {
		limit = defaultLimit
	}
	planExample["limit"] = limit
	return map[string]any{"plan": planExample}
}

// BuildTemplateQuery creates the deterministic starter query for a resource.
func BuildTemplateQuery(resourceType session.ResourceType, resourceName string, groups []string, timeRange session.TimeRange) string {
	groupExpr := strings.Join(normalizeGroups(groups), ", ")
	timeExpr := buildTimeClause(timeRange)
	switch resourceType {
	case session.ResourceTypeStream:
		return fmt.Sprintf("SELECT * FROM STREAM %s IN %s %s LIMIT %d", resourceName, groupExpr, timeExpr, defaultLimit)
	case session.ResourceTypeTrace:
		return fmt.Sprintf("SELECT * FROM TRACE %s IN %s %s LIMIT %d", resourceName, groupExpr, timeExpr, defaultLimit)
	case session.ResourceTypeProperty:
		return fmt.Sprintf("SELECT * FROM PROPERTY %s IN %s LIMIT %d", resourceName, groupExpr, defaultLimit)
	case session.ResourceTypeTopN:
		return ""
	default:
		return fmt.Sprintf("SELECT * FROM MEASURE %s IN %s %s LIMIT %d", resourceName, groupExpr, timeExpr, defaultLimit)
	}
}

func (runner *Runner) sendAgentTurn(ctx context.Context, agentSessionID string, payload agent.RequestPayload) (<-chan agent.Event, error) {
	var taskPrompt string
	switch payload.Intent {
	case agent.TurnIntentRefine:
		taskPrompt = "Refine the current typed query plan according to turn_hint while preserving correct constraints."
	case agent.TurnIntentRepair:
		taskPrompt = "Repair the current typed query plan using the structured validation or execution diagnostic."
	case agent.TurnIntentAnswer:
		taskPrompt = "Answer this schema or usage question in plain language. " +
			"Use list_groups_schemas and describe_schema only. Do not call propose_query_plan or execute_bydbql, and do not read stored rows."
	case agent.TurnIntentNextStep:
		taskPrompt = "Continue the next independently compiled workflow step using prior bounded results as data."
	default:
		taskPrompt = "Handle the new query request from turn_hint. Discover exact schemas and submit a typed plan only when unambiguous."
	}
	events, sendErr := runner.agentGateway.Send(ctx, agentSessionID, agent.TurnRequest{
		Prompt:  taskPrompt,
		Payload: payload,
	})
	if sendErr != nil {
		return nil, fmt.Errorf("failed to send agent turn: %w", sendErr)
	}
	return events, nil
}

func gatewayMaintainsConversationHistory(agentGateway agent.Gateway) bool {
	historyGateway, supportsHistoryMode := agentGateway.(agent.ConversationHistoryGateway)
	return supportsHistoryMode && historyGateway.MaintainsConversationHistory()
}

func normalizeGroups(groups []string) []string {
	normalizedGroups := normalizeGroupsIfProvided(groups)
	if len(normalizedGroups) == 0 {
		return []string{defaultGroupName}
	}
	return normalizedGroups
}

func buildTimeClause(timeRange session.TimeRange) string {
	start := strings.TrimSpace(timeRange.Start)
	end := strings.TrimSpace(timeRange.End)
	if start == "" {
		start = defaultTimeStart
	}
	if end != "" {
		return fmt.Sprintf("TIME BETWEEN '%s' AND '%s'", start, end)
	}
	return fmt.Sprintf("TIME > '%s'", start)
}
