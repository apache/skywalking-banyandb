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
	"regexp"
	"strings"
	"time"
	"unicode"

	"github.com/google/uuid"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/agent"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/approval"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/bridge"
	tuibysql "github.com/apache/skywalking-banyandb/bydbctl/internal/tui/bydbql"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/tools"
)

const (
	defaultGroupName    = "default"
	defaultResourceName = "service_endpoint_latency"
	defaultTimeStart    = "-30m"
	defaultLimit        = 10
	defaultTopN         = 10
	maxDiagnosticLength = 360
)

var fragmentedTimeRangePattern = regexp.MustCompile(`'-\s*(\d+)\s*m\s*'`)

var fragmentedTokenReplacements = []struct {
	old string
	new string
}{
	{old: "by db ql", new: "bydbql"},
	{old: "b yd b ql", new: "bydbql"},
	{old: "SH OW", new: "SHOW"},
	{old: "A GG REG ATE", new: "AGGREGATE"},
	{old: "AGGREGATE BY AV G", new: "AGGREGATE BY AVG"},
	{old: "AGGREGATE BY MA X", new: "AGGREGATE BY MAX"},
	{old: "AGGREGATE BY MI N", new: "AGGREGATE BY MIN"},
	{old: "AV G", new: "AVG"},
	{old: "MA X", new: "MAX"},
	{old: "MI N", new: "MIN"},
	{old: "TOP text 10", new: "TOP 10"},
	{old: "TOP text ", new: "TOP "},
	{old: "ME ASURE", new: "MEASURE"},
	{old: "ME AS URE", new: "MEASURE"},
	{old: "ST REAM", new: "STREAM"},
	{old: "TR ACE", new: "TRACE"},
	{old: "PROP ERTY", new: "PROPERTY"},
	{old: "SER VICE", new: "SERVICE"},
	{old: "LI MIT", new: "LIMIT"},
	{old: "GRO UP", new: "GROUP"},
	{old: "OR DER", new: "ORDER"},
	{old: "WHE RE", new: "WHERE"},
	{old: "service _", new: "service_"},
	{old: "service_end point_l atency", new: "service_endpoint_latency"},
	{old: "endpoint _", new: "endpoint_"},
	{old: "_ ", new: "_"},
	{old: " - ", new: "-"},
	{old: "text 10 text", new: "10"},
	{old: "text 100 text", new: "100"},
	{old: "text SELECT", new: "SELECT"},
	{old: "sche mas", new: "schemas"},
	{old: "sche ma", new: "schema"},
}

// Validator validates a BYDBQL candidate.
type Validator interface {
	Validate(ctx context.Context, query string, schema *session.SchemaSnapshot) (session.ValidationReport, error)
}

// Runner coordinates deterministic workflow phases and agent turns.
type Runner struct {
	agentGateway agent.Gateway
	validator    Validator
	executor     tools.Executor
	approvals    *approval.Controller
	toolBridge   *bridge.ToolBridge
	now          func() time.Time
}

// Config configures a Runner.
type Config struct {
	AgentGateway agent.Gateway
	Validator    Validator
	Executor     tools.Executor
	Approvals    *approval.Controller
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
	approvals := config.Approvals
	if approvals == nil {
		approvals = approval.NewController()
	}
	return &Runner{
		agentGateway: config.AgentGateway,
		validator:    validator,
		executor:     executor,
		approvals:    approvals,
		toolBridge:   config.ToolBridge,
		now:          time.Now,
	}
}

// SetExecutionPolicy stores the execution policy on the runner approval controller and tool bridge.
func (runner *Runner) SetExecutionPolicy(policy approval.ExecutionPolicy) {
	if runner == nil {
		return
	}
	normalizedPolicy := approval.NormalizeExecutionPolicy(string(policy))
	if runner.approvals != nil {
		runner.approvals.SetPolicy(normalizedPolicy)
	}
	if runner.toolBridge != nil {
		runner.toolBridge.SetExecutionPolicy(normalizedPolicy)
	}
}

// ApprovalRequests returns execution approvals that require a user decision.
func (runner *Runner) ApprovalRequests() <-chan approval.Request {
	return runner.approvals.Requests()
}

// ResolveApproval records a one-time user decision for an execution request.
func (runner *Runner) ResolveApproval(requestID string, approved bool) error {
	return runner.approvals.Resolve(requestID, approval.Decision{Approved: approved})
}

// CancelApprovals rejects all pending execution requests.
func (runner *Runner) CancelApprovals() {
	runner.approvals.Cancel()
}

// TurnUpdate is one real-time agent or controlled-tool event, or the completed turn result.
type TurnUpdate struct {
	Event        *agent.Event
	Err          error
	Done         bool
	QuerySession *session.QuerySession
}

// StartOptions contains user-provided session slots.
type StartOptions struct {
	ResourceType    session.ResourceType
	TimeRange       session.TimeRange
	Goal            string
	ResourceName    string
	Groups          []string
	ExecutionPolicy approval.ExecutionPolicy
	NameProvided    bool
	GroupsProvided  bool
	TypeProvided    bool
}

// StartSession creates a session and discovers a schema summary.
func (runner *Runner) StartSession(ctx context.Context, options StartOptions) (*session.QuerySession, error) {
	catalog, catalogErr := runner.executor.DiscoverCatalog(ctx)
	if catalogErr != nil {
		return nil, fmt.Errorf("failed to discover schema catalog: %w", catalogErr)
	}
	if usesAutonomousDiscovery(options) {
		return newAutonomousSession(options, catalog, runner.now()), nil
	}
	resolved := ResolveSessionSlots(options, catalog)
	schemaSnapshot, schemaErr := runner.executor.DiscoverSchema(ctx, tools.SchemaRequest{
		Type:   resolved.ResourceType,
		Name:   resolved.ResourceName,
		Groups: resolved.Groups,
	})
	if schemaErr != nil {
		return nil, fmt.Errorf("failed to discover schema: %w", schemaErr)
	}
	schemaSnapshot.AvailableGroups = append([]string(nil), catalog.Groups...)
	schemaSnapshot.Catalog = append([]session.CatalogEntry(nil), catalog.Entries...)
	querySession := &session.QuerySession{
		ID:              uuid.NewString(),
		Phase:           session.PhaseIntent,
		UserGoal:        resolved.Goal,
		ResourceType:    resolved.ResourceType,
		ResourceName:    resolved.ResourceName,
		Groups:          append([]string(nil), resolved.Groups...),
		TimeRange:       resolved.TimeRange,
		SchemaSnapshot:  schemaSnapshot,
		SlotsPinned:     resolved.SlotsPinned,
		AutoMatched:     resolved.AutoMatched,
		ExecutionPolicy: approval.NormalizeExecutionPolicy(string(options.ExecutionPolicy)),
	}
	querySession.AddTranscript("workflow", "created BYDBQL agent session", runner.now())
	if resolved.AutoMatched {
		querySession.AddTranscript(
			"workflow",
			fmt.Sprintf("auto-matched resource %s %s in %s from goal", resolved.ResourceType, resolved.ResourceName, strings.Join(resolved.Groups, ",")),
			runner.now(),
		)
	}
	return querySession, nil
}

// SyncSession updates session slots and refreshes schema when the TUI inputs change.
func (runner *Runner) SyncSession(ctx context.Context, querySession *session.QuerySession, options StartOptions) (*session.QuerySession, error) {
	if querySession == nil {
		return runner.StartSession(ctx, options)
	}
	if !slotsChanged(querySession, options) {
		return querySession, nil
	}
	catalog, catalogErr := runner.executor.DiscoverCatalog(ctx)
	if catalogErr != nil {
		return nil, fmt.Errorf("failed to discover schema catalog: %w", catalogErr)
	}
	if usesAutonomousDiscovery(options) {
		querySession.UserGoal = strings.TrimSpace(options.Goal)
		querySession.TimeRange = applyTimeDefaults(options.TimeRange)
		querySession.SchemaSnapshot.AvailableGroups = append([]string(nil), catalog.Groups...)
		querySession.SchemaSnapshot.Catalog = append([]session.CatalogEntry(nil), catalog.Entries...)
		querySession.SlotsPinned = false
		querySession.AutoMatched = false
		querySession.AddTranscript("workflow", "refreshed catalog for autonomous schema discovery", runner.now())
		return querySession, nil
	}
	resolved := ResolveSessionSlots(options, catalog)
	schemaSnapshot, schemaErr := runner.executor.DiscoverSchema(ctx, tools.SchemaRequest{
		Type:   resolved.ResourceType,
		Name:   resolved.ResourceName,
		Groups: resolved.Groups,
	})
	if schemaErr != nil {
		return nil, fmt.Errorf("failed to refresh schema: %w", schemaErr)
	}
	schemaSnapshot.AvailableGroups = append([]string(nil), catalog.Groups...)
	schemaSnapshot.Catalog = append([]session.CatalogEntry(nil), catalog.Entries...)
	querySession.UserGoal = resolved.Goal
	querySession.ResourceType = resolved.ResourceType
	querySession.ResourceName = resolved.ResourceName
	querySession.Groups = append([]string(nil), resolved.Groups...)
	querySession.TimeRange = resolved.TimeRange
	querySession.SchemaSnapshot = schemaSnapshot
	querySession.SlotsPinned = resolved.SlotsPinned
	querySession.AutoMatched = resolved.AutoMatched
	querySession.AddTranscript("workflow", "refreshed schema after slot change", runner.now())
	if resolved.AutoMatched {
		querySession.AddTranscript(
			"workflow",
			fmt.Sprintf("auto-matched resource %s %s in %s from goal", resolved.ResourceType, resolved.ResourceName, strings.Join(resolved.Groups, ",")),
			runner.now(),
		)
	}
	return querySession, nil
}

func newAutonomousSession(options StartOptions, catalog session.SchemaCatalog, now time.Time) *session.QuerySession {
	querySession := &session.QuerySession{
		ID:              uuid.NewString(),
		Phase:           session.PhaseIntent,
		UserGoal:        strings.TrimSpace(options.Goal),
		TimeRange:       applyTimeDefaults(options.TimeRange),
		AutoMatched:     false,
		ExecutionPolicy: approval.NormalizeExecutionPolicy(string(options.ExecutionPolicy)),
		SchemaSnapshot: session.SchemaSnapshot{
			UpdatedAt:       catalog.UpdatedAt,
			AvailableGroups: append([]string(nil), catalog.Groups...),
			Catalog:         append([]session.CatalogEntry(nil), catalog.Entries...),
		},
	}
	querySession.AddTranscript("workflow", "created autonomous BYDBQL agent session", now)
	return querySession
}

func usesAutonomousDiscovery(options StartOptions) bool {
	if options.NameProvided || options.GroupsProvided || options.TypeProvided {
		return false
	}
	if strings.TrimSpace(options.ResourceName) != "" {
		return false
	}
	return len(normalizeGroupsIfProvided(options.Groups)) == 0
}

func slotsChanged(querySession *session.QuerySession, options StartOptions) bool {
	if querySession.UserGoal != strings.TrimSpace(options.Goal) {
		return true
	}
	if options.TypeProvided && querySession.ResourceType != options.ResourceType {
		return true
	}
	if options.NameProvided && querySession.ResourceName != strings.TrimSpace(options.ResourceName) {
		return true
	}
	if options.GroupsProvided && !sameGroups(querySession.Groups, normalizeGroupsIfProvided(options.Groups)) {
		return true
	}
	if querySession.TimeRange.Start != strings.TrimSpace(options.TimeRange.Start) || querySession.TimeRange.End != strings.TrimSpace(options.TimeRange.End) {
		return true
	}
	return false
}

func sameGroups(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for idx := range left {
		if left[idx] != right[idx] {
			return false
		}
	}
	return true
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
	if runner.toolBridge != nil {
		runner.toolBridge.SetExecutionPolicy(querySession.ExecutionPolicy)
	}
	if bootstrapErr := runner.refreshDiscoveryForTurn(ctx, querySession, strings.TrimSpace(turnHint)); bootstrapErr != nil {
		querySession.AddTranscript("workflow", "schema bootstrap: "+bootstrapErr.Error(), runner.now())
	}
	trimmedTurnHint := strings.TrimSpace(turnHint)
	rankingGoal := CatalogRankingGoal(querySession.UserGoal, trimmedTurnHint)
	querySession.DiscoveryGoal = rankingGoal
	rankedCatalog := RankCatalogCandidates(rankingGoal, querySession.SchemaSnapshot.Catalog, maxPromptCatalogCandidates)
	if runner.toolBridge != nil {
		runner.toolBridge.SetSession(querySession)
		runner.toolBridge.SetRankedCandidates(RankCatalogCandidates(rankingGoal, querySession.SchemaSnapshot.Catalog, 5))
	}
	querySession.Phase = session.PhaseAgentDraft
	agentSessionID := strings.TrimSpace(querySession.AgentSessionID)
	if agentSessionID == "" {
		agentSession, startErr := runner.agentGateway.Start(ctx, agent.StartRequest{
			Provider: "bydbctl-agent",
			Metadata: map[string]string{
				"query_session_id": querySession.ID,
			},
		})
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
	rankedCatalog = RankCatalogCandidates(rankingGoal, querySession.SchemaSnapshot.Catalog, maxPromptCatalogCandidates)
	payload := agent.BuildAgentTurnRequest(querySession, hints, templateHint, trimmedTurnHint)
	payload.Schema.CatalogTotal = len(querySession.SchemaSnapshot.Catalog)
	if len(rankedCatalog) > 0 {
		payload.Schema.RankedCandidates = agent.CatalogEntrySummaries(rankedCatalog)
		payload.Schema.Catalog = payload.Schema.RankedCandidates
	}
	payload.PlanExample = buildStructuredPlanExample(querySession, hints)
	agentEvents, sendErr := runner.sendAgentTurn(ctx, agentSessionID, payload)
	if sendErr != nil {
		querySession.Phase = session.PhaseError
		return nil, sendErr
	}
	updates := make(chan TurnUpdate, 16)
	go runner.streamAgentTurn(ctx, querySession, trimmedTurnHint, agentEvents, updates)
	return updates, nil
}

// StopAgentTurn cancels approvals and asks the provider to stop the active session.
func (runner *Runner) StopAgentTurn(ctx context.Context, querySession *session.QuerySession) error {
	runner.CancelApprovals()
	if runner.toolBridge != nil {
		runner.toolBridge.Cancel()
	}
	if querySession == nil || strings.TrimSpace(querySession.AgentSessionID) == "" || runner.agentGateway == nil {
		return nil
	}
	if stopErr := runner.agentGateway.Stop(ctx, querySession.AgentSessionID); stopErr != nil {
		return fmt.Errorf("failed to stop agent session: %w", stopErr)
	}
	querySession.AgentSessionID = ""
	querySession.AddTranscript("workflow", "agent turn cancelled", runner.now())
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
	var match catalogMatch
	if explicitEntry := FindExplicitResourceMention(rankingGoal, querySession.SchemaSnapshot.Catalog); explicitEntry != nil {
		match = catalogMatch{
			Matched: true,
			Group:   explicitEntry.Group,
			Name:    explicitEntry.Name,
			Type:    explicitEntry.Type,
			Score:   100,
		}
	} else {
		match = matchResourceFromGoal(
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
		strings.EqualFold(currentName, match.Name) &&
		(currentGroup == "" || strings.EqualFold(currentGroup, match.Group)) {
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
	schemaSnapshot.AvailableGroups = append([]string(nil), querySession.SchemaSnapshot.AvailableGroups...)
	schemaSnapshot.Catalog = append([]session.CatalogEntry(nil), querySession.SchemaSnapshot.Catalog...)
	querySession.ResourceType = match.Type
	querySession.ResourceName = match.Name
	querySession.Groups = []string{match.Group}
	querySession.SchemaSnapshot = schemaSnapshot
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
		runner.toolBridge.SetRankedCandidates(EnsureCatalogEntry(
			RankCatalogCandidates(rankingGoal, querySession.SchemaSnapshot.Catalog, 5),
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
	match := matchResourceFromGoal(
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
	schemaSnapshot.AvailableGroups = append([]string(nil), querySession.SchemaSnapshot.AvailableGroups...)
	schemaSnapshot.Catalog = append([]session.CatalogEntry(nil), querySession.SchemaSnapshot.Catalog...)
	querySession.ResourceType = match.Type
	querySession.ResourceName = match.Name
	querySession.Groups = []string{match.Group}
	querySession.SchemaSnapshot = schemaSnapshot
	querySession.AutoMatched = true
	querySession.AddTranscript(
		"workflow",
		fmt.Sprintf("preloaded schema for %s %s in %s", match.Type, match.Name, match.Group),
		runner.now(),
	)
	if runner.toolBridge != nil {
		runner.toolBridge.SetSession(querySession)
		matchedEntry := session.CatalogEntry{Group: match.Group, Type: match.Type, Name: match.Name}
		runner.toolBridge.SetRankedCandidates(EnsureCatalogEntry(
			RankCatalogCandidates(rankingGoal, querySession.SchemaSnapshot.Catalog, 5),
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
	toolEvents := bridgeEvents(runner.toolBridge)
	for agentEvents != nil {
		select {
		case <-ctx.Done():
			querySession.Phase = session.PhaseReady
			updates <- TurnUpdate{Done: true, Err: ctx.Err(), QuerySession: querySession}
			return
		case event, open := <-agentEvents:
			if !open {
				agentEvents = nil
				collectedEvents = drainBridgeEvents(toolEvents, querySession, updates, collectedEvents)
				continue
			}
			collectedEvents = append(collectedEvents, event)
			if shouldForwardAgentTurnEvent(event) {
				updates <- TurnUpdate{Event: &event, QuerySession: querySession}
			}
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
			if shouldForwardAgentTurnEvent(event) {
				updates <- TurnUpdate{Event: &event, QuerySession: querySession}
			}
		}
	}
	runner.syncToolBridgeSession(querySession)
	completeErr := runner.completeAgentTurn(ctx, querySession, turnHint, collectedEvents)
	updates <- TurnUpdate{Done: true, Err: completeErr, QuerySession: querySession}
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
	querySession.PlannedQueries = append([]session.PlannedQuery(nil), bridgeSession.PlannedQueries...)
	querySession.ActivePlanStep = bridgeSession.ActivePlanStep
	querySession.ExecutionResult = bridgeSession.ExecutionResult
	querySession.SetPendingProbe(bridgeSession.PendingProbe)
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
			if shouldForwardAgentTurnEvent(event) {
				updates <- TurnUpdate{Event: &event, QuerySession: querySession}
			}
		default:
			return collectedEvents
		}
	}
}

func bridgeEvents(toolBridge *bridge.ToolBridge) <-chan agent.Event {
	if toolBridge == nil {
		return nil
	}
	return toolBridge.Events()
}

func shouldForwardAgentTurnEvent(event agent.Event) bool {
	return event.Kind != agent.EventKindMessageDelta
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
	if candidateEvent := finalProposeCandidateEvent(turnEvents); candidateEvent != nil &&
		candidateEvent.Status == agent.EventStatusFailed && strings.TrimSpace(candidateEvent.Message) != "" {
		explanation = NormalizeAgentDisplayText(candidateEvent.Message)
	}
	querySession.AddCandidate(session.BydbqlCandidate{
		ID:          fmt.Sprintf("candidate-%d", len(querySession.Candidates)+1),
		Query:       candidate,
		Explanation: explanation,
		Source:      session.CandidateSourceAgent,
		CreatedAt:   runner.now(),
		Validation:  validation,
		Probe:       querySession.TakePendingProbe(),
	})
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
		Content:   displayResponse,
		CreatedAt: runner.now(),
	})
	querySession.AddTranscript("agent", displayResponse, runner.now())
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
	querySession.AddCandidate(session.BydbqlCandidate{
		ID:          fmt.Sprintf("candidate-%d", len(querySession.Candidates)+1),
		Query:       strings.TrimSpace(query),
		Explanation: "manual edit from TUI",
		Source:      session.CandidateSourceManual,
		CreatedAt:   runner.now(),
		Validation:  validation,
	})
	querySession.AddTranscript("workflow", "validated manual BYDBQL edit", runner.now())
	if validation.Valid {
		querySession.Phase = session.PhaseReady
		return nil
	}
	querySession.Phase = session.PhaseValidate
	return nil
}

// ExecuteCurrent asks for approval and then runs the exact current BYDBQL candidate once.
func (runner *Runner) ExecuteCurrent(ctx context.Context, querySession *session.QuerySession) error {
	if querySession == nil {
		return errors.New("query session is required")
	}
	currentCandidate := querySession.CurrentCandidate()
	if currentCandidate == nil {
		return errors.New("query candidate is required")
	}
	if !currentCandidate.Validation.Valid {
		querySession.Phase = session.PhaseValidate
		return errors.New("only a valid BYDBQL candidate can be executed")
	}
	query := currentCandidate.Query
	plannedQuery := querySession.CurrentPlannedQuery()
	if plannedQuery != nil && plannedQuery.Query != query {
		querySession.Phase = session.PhaseValidate
		return errors.New("only the current compiled workflow statement can be executed")
	}
	if plannedQuery != nil && runner.executor != nil {
		schemaSnapshot, schemaErr := runner.executor.DiscoverSchema(ctx, tools.SchemaRequest{
			Type:   plannedQuery.ResourceType,
			Name:   plannedQuery.Name,
			Groups: plannedQuery.Groups,
		})
		if schemaErr != nil {
			querySession.Phase = session.PhaseError
			return fmt.Errorf("failed to refresh schema before execution: %w", schemaErr)
		}
		querySession.SchemaSnapshot = schemaSnapshot
		querySession.ResourceType = plannedQuery.ResourceType
		querySession.ResourceName = plannedQuery.Name
		querySession.Groups = append([]string(nil), plannedQuery.Groups...)
	}
	decision, approvalErr := runner.approvals.Request(ctx, runner.executionApproval(querySession, query, approval.SourceManual))
	if approvalErr != nil {
		querySession.Phase = session.PhaseReady
		return fmt.Errorf("execution approval did not complete: %w", approvalErr)
	}
	if !decision.Approved {
		querySession.Phase = session.PhaseReady
		querySession.AddTranscript("workflow", "execution rejected", runner.now())
		return errors.New("execution rejected")
	}
	validation, validationErr := runner.validator.Validate(ctx, query, &querySession.SchemaSnapshot)
	if validationErr != nil {
		querySession.Phase = session.PhaseError
		return fmt.Errorf("failed to revalidate approved query: %w", validationErr)
	}
	currentCandidate.Validation = validation
	querySession.Validation = validation
	if !validation.Valid {
		querySession.Phase = session.PhaseValidate
		return fmt.Errorf("approved query failed revalidation: %s", validation.Message)
	}
	executionResult, executeErr := runner.executor.Execute(ctx, querySession, query)
	if executeErr != nil {
		querySession.Phase = session.PhaseError
		executionResult.Error = executeErr.Error()
		if executionResult.Summary == "" {
			executionResult.Summary = executeErr.Error()
		}
		querySession.ExecutionResult = executionResult
		return fmt.Errorf("failed to execute query: %w", executeErr)
	}
	querySession.ExecutionResult = executionResult
	if executionResult.Hint != "" {
		querySession.AddTranscript("workflow", executionResult.Hint, runner.now())
	}
	querySession.AddTranscript("workflow", executionResult.Summary, runner.now())
	if plannedQuery != nil {
		nextPlanStep := querySession.CompletePlannedQuery(query)
		if nextPlanStep != nil {
			return runner.prepareNextPlanStep(ctx, querySession, *nextPlanStep)
		}
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
	preserveDiscoveryContext(&schemaSnapshot, querySession.SchemaSnapshot)
	querySession.ResourceType = schemaSnapshot.Type
	querySession.ResourceName = schemaSnapshot.Name
	querySession.Groups = append([]string(nil), schemaSnapshot.Groups...)
	querySession.SchemaSnapshot = schemaSnapshot
	validation, validationErr := runner.validator.Validate(ctx, nextPlanStep.Query, &querySession.SchemaSnapshot)
	if validationErr != nil {
		querySession.Phase = session.PhaseError
		return fmt.Errorf("failed to validate next workflow statement: %w", validationErr)
	}
	querySession.AddCandidate(session.BydbqlCandidate{
		ID:          fmt.Sprintf("candidate-%d", len(querySession.Candidates)+1),
		Query:       nextPlanStep.Query,
		Explanation: "next independently approved workflow statement",
		Source:      session.CandidateSourceAgent,
		CreatedAt:   runner.now(),
		Validation:  validation,
	})
	querySession.AddTranscript("workflow", "next workflow statement is ready for individual approval", runner.now())
	if !validation.Valid {
		querySession.Phase = session.PhaseValidate
		return fmt.Errorf("next workflow statement failed validation: %s", validation.Message)
	}
	querySession.Phase = session.PhaseReady
	return nil
}

func preserveDiscoveryContext(target *session.SchemaSnapshot, existing session.SchemaSnapshot) {
	if len(target.AvailableGroups) == 0 {
		target.AvailableGroups = append([]string(nil), existing.AvailableGroups...)
	}
	if len(target.Catalog) == 0 {
		target.Catalog = append([]session.CatalogEntry(nil), existing.Catalog...)
	}
}

func (runner *Runner) executionApproval(querySession *session.QuerySession, query string, source approval.Source) approval.Request {
	resource := fmt.Sprintf("%s/%s", querySession.ResourceType, querySession.ResourceName)
	limits := tools.Limits(runner.executor)
	return approval.WithLimits(approval.NewRequest(query, resource, querySession.Groups, source), limits.Timeout, limits.PreviewRows)
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
		"resource":   resource,
		"time_range": map[string]any{"start": timeStart},
		"order_by":   map[string]any{"direction": "DESC"},
		"aggregate":  map[string]any{"function": "MEAN"},
	}
	if hints.PreferShowTop || querySession.ResourceType == session.ResourceTypeTopN {
		resource["type"] = session.ResourceTypeTopN.String()
		topN := hints.LimitHint
		if topN <= 0 {
			topN = defaultTopN
		}
		planExample["top_n"] = topN
		return map[string]any{"plan": planExample}
	}
	resource["type"] = querySession.ResourceType.String()
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
		return fmt.Sprintf("SHOW TOP %d FROM MEASURE %s IN %s %s AGGREGATE BY SUM ORDER BY DESC", defaultTopN, resourceName, groupExpr, timeExpr)
	default:
		return fmt.Sprintf("SELECT * FROM MEASURE %s IN %s %s LIMIT %d", resourceName, groupExpr, timeExpr, defaultLimit)
	}
}

func (runner *Runner) sendAgentTurn(ctx context.Context, agentSessionID string, payload agent.RequestPayload) (<-chan agent.Event, error) {
	taskPrompt := "Continue the conversation using the goal and context JSON. Discover schemas when useful, and submit a typed query plan only when the user request is ready."
	if strings.TrimSpace(payload.Candidate) != "" {
		taskPrompt = "Revise the typed query plan using validation or execution feedback in the context JSON."
	}
	events, sendErr := runner.agentGateway.Send(ctx, agentSessionID, agent.TurnRequest{
		Task:    payload.Task,
		Prompt:  taskPrompt,
		Payload: payload,
	})
	if sendErr != nil {
		return nil, fmt.Errorf("failed to send agent turn: %w", sendErr)
	}
	return events, nil
}

func inferResourceType(goal string) session.ResourceType {
	normalizedGoal := strings.ToLower(goal)
	switch {
	case strings.Contains(normalizedGoal, "trace"):
		return session.ResourceTypeTrace
	case strings.Contains(normalizedGoal, "property"):
		return session.ResourceTypeProperty
	case strings.Contains(normalizedGoal, "stream") || strings.Contains(normalizedGoal, "log"):
		return session.ResourceTypeStream
	case strings.Contains(normalizedGoal, "top"):
		return session.ResourceTypeTopN
	default:
		return session.ResourceTypeMeasure
	}
}

func normalizeGroups(groups []string) []string {
	var normalizedGroups []string
	for _, group := range groups {
		parts := strings.Split(group, ",")
		for _, part := range parts {
			trimmedPart := strings.TrimSpace(part)
			if trimmedPart != "" {
				normalizedGroups = append(normalizedGroups, trimmedPart)
			}
		}
	}
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

func finalCandidate(events []agent.Event) string {
	if candidateEvent := finalProposeCandidateEvent(events); candidateEvent != nil {
		return cleanBydbqlCandidate(candidateEvent.Candidate)
	}
	return ""
}

func finalProposeCandidateEvent(events []agent.Event) *agent.Event {
	for eventIdx := len(events) - 1; eventIdx >= 0; eventIdx-- {
		event := events[eventIdx]
		if event.Kind != agent.EventKindCandidate || event.Origin != agent.EventOriginToolBridge || event.ToolName != bridge.ToolProposeQueryPlan {
			continue
		}
		if candidate := cleanBydbqlCandidate(event.Candidate); candidate != "" {
			copiedEvent := event
			copiedEvent.Candidate = candidate
			return &copiedEvent
		}
	}
	return nil
}

func agentOutputText(events []agent.Event) string {
	for eventIdx := len(events) - 1; eventIdx >= 0; eventIdx-- {
		event := events[eventIdx]
		if event.Origin != agent.EventOriginToolBridge && event.Kind == agent.EventKindFinalResponse && strings.TrimSpace(event.Message) != "" {
			return NormalizeAgentDisplayText(event.Message)
		}
	}
	var messages []string
	for _, event := range events {
		if event.Origin == agent.EventOriginToolBridge || event.Kind == agent.EventKindPlanUpdate {
			continue
		}
		if strings.TrimSpace(event.Message) != "" {
			messages = append(messages, NormalizeAgentDisplayText(event.Message))
		}
	}
	return strings.Join(messages, "\n")
}

func finalExplanation(events []agent.Event) string {
	for eventIdx := len(events) - 1; eventIdx >= 0; eventIdx-- {
		event := events[eventIdx]
		if event.Origin == agent.EventOriginToolBridge {
			continue
		}
		if strings.TrimSpace(event.Explanation) != "" {
			return strings.TrimSpace(event.Explanation)
		}
		if strings.TrimSpace(event.Message) != "" {
			return strings.TrimSpace(event.Message)
		}
	}
	return "agent returned a BYDBQL candidate"
}

func finalClarification(events []agent.Event) string {
	for eventIdx := len(events) - 1; eventIdx >= 0; eventIdx-- {
		event := events[eventIdx]
		if event.Kind == agent.EventKindClarification && strings.TrimSpace(event.Message) != "" {
			return strings.TrimSpace(event.Message)
		}
	}
	return ""
}

func containsUncontrolledBydbql(events []agent.Event) bool {
	var outputParts []string
	for _, event := range events {
		outputParts = append(outputParts, event.Candidate, event.Message, event.Explanation)
	}
	normalizedText := strings.ToUpper(RepairFragmentedQuery(strings.Join(outputParts, " ")))
	if strings.Contains(normalizedText, "SHOW TOP ") && strings.Contains(normalizedText, " FROM MEASURE ") {
		return true
	}
	if !strings.Contains(normalizedText, "SELECT ") {
		return false
	}
	for _, resourceType := range []string{"MEASURE", "STREAM", "TRACE", "PROPERTY"} {
		if strings.Contains(normalizedText, " FROM "+resourceType+" ") {
			return true
		}
	}
	return false
}

func agentOutputSummary(events []agent.Event) string {
	var outputParts []string
	for _, event := range events {
		if event.Origin == agent.EventOriginToolBridge {
			continue
		}
		if strings.TrimSpace(event.Candidate) != "" {
			outputParts = append(outputParts, "candidate="+singleLine(event.Candidate))
		}
		if strings.TrimSpace(event.Message) != "" {
			outputParts = append(outputParts, singleLine(event.Message))
		}
		if strings.TrimSpace(event.Explanation) != "" {
			outputParts = append(outputParts, singleLine(event.Explanation))
		}
	}
	return strings.TrimSpace(strings.Join(outputParts, " | "))
}

func truncateDiagnostic(value string) string {
	trimmedValue := strings.TrimSpace(value)
	if len(trimmedValue) <= maxDiagnosticLength {
		return trimmedValue
	}
	return trimmedValue[:maxDiagnosticLength] + "..."
}

func singleLine(value string) string {
	return strings.Join(strings.Fields(value), " ")
}

// NormalizeAgentDisplayText repairs ACP fragmented natural-language output for UI display.
func NormalizeAgentDisplayText(text string) string {
	normalizedText := singleLine(text)
	if normalizedText == "" {
		return strings.TrimSpace(text)
	}
	if !strings.Contains(normalizedText, "`") {
		return normalizePlainAgentText(normalizedText)
	}
	var builder strings.Builder
	segmentStart := 0
	for segmentStart < len(normalizedText) {
		backtickStart := strings.Index(normalizedText[segmentStart:], "`")
		if backtickStart < 0 {
			builder.WriteString(normalizePlainAgentText(normalizedText[segmentStart:]))
			break
		}
		backtickStart += segmentStart
		builder.WriteString(normalizePlainAgentText(normalizedText[segmentStart:backtickStart]))
		backtickEnd := strings.Index(normalizedText[backtickStart+1:], "`")
		if backtickEnd < 0 {
			builder.WriteString(normalizeFragmentedAgentText(normalizedText[backtickStart:]))
			break
		}
		backtickEnd += backtickStart + 1
		innerText := strings.TrimSpace(normalizedText[backtickStart+1 : backtickEnd])
		builder.WriteString("`")
		builder.WriteString(normalizeFragmentedAgentText(innerText))
		builder.WriteString("`")
		segmentStart = backtickEnd + 1
	}
	return strings.TrimSpace(builder.String())
}

func normalizePlainAgentText(text string) string {
	if text == "" {
		return text
	}
	plainText := collapseCJKSpacing(text)
	plainText = collapseContractionSpacing(plainText)
	plainText = strings.ReplaceAll(plainText, " ,", ",")
	plainText = strings.ReplaceAll(plainText, " .", ".")
	plainText = strings.ReplaceAll(plainText, "( ", "(")
	plainText = strings.ReplaceAll(plainText, " )", ")")
	plainText = strings.ReplaceAll(plainText, " - ", "-")
	plainText = strings.ReplaceAll(plainText, " -", "-")
	plainText = strings.ReplaceAll(plainText, "- ", "-")
	if strings.Contains(plainText, "_") {
		return normalizeFragmentedAgentText(plainText)
	}
	plainText = collapseIdentifierFragments(plainText)
	for _, replacement := range fragmentedTokenReplacements {
		if strings.Contains(plainText, replacement.old) {
			plainText = strings.ReplaceAll(plainText, replacement.old, replacement.new)
		}
	}
	return plainText
}

func collapseContractionSpacing(text string) string {
	replacements := []string{
		" n't", "n't",
		" 't ", "'t ",
		" 's ", "'s ",
		" 're ", "'re ",
		" 've ", "'ve ",
		" 'd ", "'d ",
		" 'll ", "'ll ",
	}
	for idx := 0; idx < len(replacements); idx += 2 {
		text = strings.ReplaceAll(text, replacements[idx], replacements[idx+1])
	}
	return text
}

func collapseACPTextSpacing(text string) string {
	return collapseCJKSpacing(text)
}

func collapseCJKSpacing(text string) string {
	runes := []rune(text)
	if len(runes) == 0 {
		return text
	}
	var builder strings.Builder
	builder.Grow(len(runes))
	for runeIdx := 0; runeIdx < len(runes); runeIdx++ {
		currentRune := runes[runeIdx]
		if currentRune == ' ' && runeIdx > 0 && runeIdx+1 < len(runes) && shouldCollapseACPSpacing(runes[runeIdx-1], runes[runeIdx+1]) {
			continue
		}
		builder.WriteRune(currentRune)
	}
	return builder.String()
}

func shouldCollapseACPSpacing(left, right rune) bool {
	return isACPCompactRune(left) && isACPCompactRune(right)
}

func isACPCompactRune(value rune) bool {
	if unicode.Is(unicode.Han, value) {
		return true
	}
	switch value {
	case '，', '。', '、', '；', '：', '？', '！', '）', '（', '》', '《', '」', '「', '’', '‘', '”', '“':
		return true
	default:
		return false
	}
}

// RepairFragmentedQuery normalizes ACP fragmented BYDBQL text into a single statement.
func RepairFragmentedQuery(query string) string {
	normalizedQuery := normalizeFragmentedAgentText(query)
	if normalizedQuery == "" {
		return strings.TrimSpace(query)
	}
	return normalizedQuery
}

func normalizeFragmentedAgentText(text string) string {
	normalizedText := singleLine(text)
	normalizedText = strings.ReplaceAll(normalizedText, "` ` `", "```")
	normalizedText = strings.ReplaceAll(normalizedText, "`` `", "```")
	normalizedText = strings.ReplaceAll(normalizedText, "` ``", "```")
	normalizedText = collapseIdentifierFragments(normalizedText)
	for _, replacement := range fragmentedTokenReplacements {
		normalizedText = strings.ReplaceAll(normalizedText, replacement.old, replacement.new)
	}
	normalizedText = strings.ReplaceAll(normalizedText, " ,", ",")
	normalizedText = strings.ReplaceAll(normalizedText, " .", ".")
	normalizedText = strings.ReplaceAll(normalizedText, "( ", "(")
	normalizedText = strings.ReplaceAll(normalizedText, " )", ")")
	normalizedText = strings.ReplaceAll(normalizedText, " text ", " ")
	normalizedText = fragmentedTimeRangePattern.ReplaceAllString(normalizedText, "'-${1}m'")
	normalizedText = strings.ReplaceAll(normalizedText, ">'", "> '")
	normalizedText = strings.ReplaceAll(normalizedText, "<'", "< '")
	return strings.TrimSpace(normalizedText)
}

func collapseIdentifierFragments(text string) string {
	fields := strings.Fields(text)
	if len(fields) == 0 {
		return ""
	}
	collapsedFields := make([]string, 0, len(fields))
	for fieldIdx := 0; fieldIdx < len(fields); fieldIdx++ {
		currentField := fields[fieldIdx]
		if fieldIdx+1 < len(fields) && shouldJoinIdentifierFragment(currentField, fields[fieldIdx+1]) {
			collapsedFields = append(collapsedFields, currentField+fields[fieldIdx+1])
			fieldIdx++
			continue
		}
		collapsedFields = append(collapsedFields, currentField)
	}
	return strings.Join(collapsedFields, " ")
}

func shouldJoinIdentifierFragment(left, right string) bool {
	if left == "" || right == "" {
		return false
	}
	if isFragmentJoinStopword(left) || isFragmentJoinStopword(right) {
		return false
	}
	if strings.HasSuffix(left, "_") || strings.HasPrefix(right, "_") {
		return true
	}
	if strings.Contains(left, "_") && len(right) <= 4 && isIdentifierFragment(right) {
		return true
	}
	if len(right) == 1 && isUpperAlpha(right) {
		switch left + right {
		case "AVG", "MAX", "MIN":
			return true
		}
	}
	return isLowerAlpha(left) && isLowerAlpha(right) && len(left) <= 4 && len(right) <= 12 && len(left)+len(right) <= 8
}

func isFragmentJoinStopword(token string) bool {
	_, found := fragmentJoinStopwords[token]
	return found
}

var fragmentJoinStopwords = map[string]struct{}{
	"the": {}, "and": {}, "for": {}, "you": {}, "your": {}, "need": {}, "more": {}, "let": {}, "but": {}, "not": {},
	"see": {}, "ask": {}, "use": {}, "with": {}, "from": {}, "that": {}, "this": {}, "what": {}, "when": {}, "have": {},
	"has": {}, "are": {}, "was": {}, "were": {}, "been": {}, "into": {}, "also": {}, "all": {}, "can": {}, "could": {},
	"would": {}, "should": {}, "will": {}, "after": {}, "before": {}, "about": {}, "than": {}, "then": {}, "them": {},
	"they": {}, "most": {}, "like": {}, "just": {}, "only": {}, "very": {}, "here": {}, "there": {}, "how": {}, "who": {},
	"why": {}, "its": {}, "our": {}, "out": {}, "any": {}, "may": {}, "did": {}, "don": {}, "does": {}, "didn": {},
	"doesn": {}, "isn": {}, "aren": {}, "won": {}, "cant": {}, "couldn": {}, "wouldn": {}, "shouldn": {}, "must": {},
	"still": {}, "even": {}, "over": {}, "such": {}, "once": {}, "each": {}, "both": {}, "me": {}, "by": {}, "to": {},
	"in": {}, "on": {}, "at": {}, "or": {}, "if": {}, "as": {}, "an": {}, "be": {}, "we": {}, "he": {}, "it": {},
	"my": {}, "up": {}, "so": {}, "no": {}, "do": {}, "go": {}, "is": {}, "am": {},
}

func isUpperAlpha(value string) bool {
	for _, valueRune := range value {
		if valueRune < 'A' || valueRune > 'Z' {
			return false
		}
	}
	return value != ""
}

func isLowerAlpha(value string) bool {
	for _, valueRune := range value {
		if valueRune < 'a' || valueRune > 'z' {
			return false
		}
	}
	return true
}

func isIdentifierFragment(value string) bool {
	if value == "" {
		return false
	}
	for _, valueRune := range value {
		if (valueRune < 'a' || valueRune > 'z') && valueRune != '_' {
			return false
		}
	}
	return true
}

func cleanBydbqlCandidate(text string) string {
	candidate := strings.TrimSpace(text)
	if candidate == "" {
		return ""
	}
	if strings.Contains(candidate, ";") {
		return ""
	}
	if !looksLikeBydbql(candidate) {
		return ""
	}
	upperCandidate := strings.ToUpper(candidate)
	if strings.HasPrefix(upperCandidate, "SELECT ") && !strings.Contains(upperCandidate, " FROM ") && !strings.Contains(upperCandidate, "\nFROM ") {
		return ""
	}
	return candidate
}

func looksLikeBydbql(text string) bool {
	upperText := strings.ToUpper(strings.TrimSpace(text))
	return strings.HasPrefix(upperText, "SELECT ") || strings.HasPrefix(upperText, "SHOW TOP ")
}
