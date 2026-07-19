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

// Package agent defines provider-neutral contracts for coding agent integration.
package agent

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/agent/prompt"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/approval"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
)

// Controlled workflow limits shared by agent gateways and the tool bridge.
const (
	DefaultMaxPlanAttempts       = 3
	DefaultMaxSchemaDescriptions = 20
)

// AgentGateway starts sessions and streams provider-neutral agent events.
type AgentGateway interface {
	Start(ctx context.Context, req StartRequest) (Session, error)
	Send(ctx context.Context, sessionID string, req TurnRequest) (<-chan Event, error)
	Stop(ctx context.Context, sessionID string) error
}

// ConversationHistoryGateway reports whether a provider session retains prior turns.
type ConversationHistoryGateway interface {
	MaintainsConversationHistory() bool
}

// Gateway is an alias for AgentGateway.
type Gateway = AgentGateway

// StartRequest contains provider-neutral session startup data.
type StartRequest struct {
	Metadata         map[string]string
	Provider         string
	WorkingDirectory string
}

// AgentStartRequest is an alias for StartRequest.
type AgentStartRequest = StartRequest

// Session identifies a provider session.
type Session struct {
	StartedAt time.Time
	ID        string
	Provider  string
}

// AgentSession is an alias for Session.
type AgentSession = Session

// TurnRequest is a structured prompt sent to an agent.
type TurnRequest struct {
	Payload RequestPayload
	Task    string
	Prompt  string
}

// AgentTurnRequest is an alias for TurnRequest.
type AgentTurnRequest = TurnRequest

// ConversationTurnPayload is one prior user-agent exchange exposed to the agent.
type ConversationTurnPayload struct {
	Hint      string `json:"hint,omitempty"`
	Response  string `json:"response,omitempty"`
	Candidate string `json:"candidate,omitempty"`
}

// TurnIntent identifies how the current turn relates to prior query state.
type TurnIntent string

// Supported turn intents.
const (
	TurnIntentNewQuery TurnIntent = "NEW_QUERY"
	TurnIntentRefine   TurnIntent = "REFINE"
	TurnIntentRepair   TurnIntent = "REPAIR"
	TurnIntentAnswer   TurnIntent = "ANSWER"
	TurnIntentNextStep TurnIntent = "NEXT_STEP"
)

// RequestPayload is the JSON shape sent through ACP/Codex adapters.
type RequestPayload struct {
	Constraints      Constraints               `json:"constraints"`
	Schema           SchemaSummary             `json:"schema"`
	Workflow         WorkflowGuidance          `json:"workflow"`
	QueryHints       QueryHints                `json:"query_hints"`
	TimeRange        TimeRangePayload          `json:"time_range"`
	ExecutionSummary *ExecutionSummary         `json:"execution_summary,omitempty"`
	ProbeSummary     *ProbeSummaryPayload      `json:"probe_summary,omitempty"`
	ValidationError  *string                   `json:"validation_error,omitempty"`
	Conversation     []ConversationTurnPayload `json:"conversation,omitempty"`
	Intent           TurnIntent                `json:"intent"`
	Task             string                    `json:"task"`
	Goal             string                    `json:"goal"`
	TurnHint         string                    `json:"turn_hint,omitempty"`
	Candidate        string                    `json:"candidate"`
	TemplateHint     string                    `json:"template_hint,omitempty"`
	PlanExample      map[string]any            `json:"plan_example,omitempty"`
}

// Constraints are hard safety constraints owned by bydbctl.
type Constraints struct {
	ExecutionPolicy                    string `json:"execution_policy"`
	FinalArtifact                      string `json:"final_artifact"`
	ReadOnly                           bool   `json:"read_only"`
	MustUseSchema                      bool   `json:"must_use_schema"`
	UserMustEditOrConfirmBeforeExecute bool   `json:"user_must_edit_or_confirm_before_execute"`
	MustNotExecuteTools                bool   `json:"must_not_execute_tools"`
	AgentMayProbeData                  bool   `json:"agent_may_probe_data"`
	AgentMayExecuteWithoutPrompt       bool   `json:"agent_may_execute_without_prompt"`
}

// SchemaSummary is the schema subset exposed to an agent.
type SchemaSummary struct {
	Columns            []SchemaColumnSummary  `json:"columns,omitempty"`
	SortableIndexes    []SortableIndexSummary `json:"sortable_indexes,omitempty"`
	Groups             []string               `json:"groups"`
	Tags               []string               `json:"tags"`
	Fields             []string               `json:"fields"`
	IndexedFields      []string               `json:"indexed_fields,omitempty"`
	AvailableResources []string               `json:"available_resources,omitempty"`
	AvailableGroups    []string               `json:"available_groups,omitempty"`
	Catalog            []CatalogEntrySummary  `json:"catalog,omitempty"`
	CatalogTotal       int                    `json:"catalog_total,omitempty"`
	RankedCandidates   []CatalogEntrySummary  `json:"ranked_candidates,omitempty"`
	Type               string                 `json:"type"`
	Name               string                 `json:"name"`
	SourceMeasure      string                 `json:"source_measure,omitempty"`
	SourceMeasureGroup string                 `json:"source_measure_group,omitempty"`
	FieldValueSort     string                 `json:"field_value_sort,omitempty"`
	Fingerprint        string                 `json:"fingerprint,omitempty"`
	TypedColumnsReady  bool                   `json:"typed_columns_ready,omitempty"`
}

// SortableIndexSummary is one exact ORDER BY rule exposed to an agent.
type SortableIndexSummary struct {
	RuleName string   `json:"rule_name"`
	Tags     []string `json:"tags"`
}

// WorkflowGuidance exposes orchestration limits and gates to the agent.
type WorkflowGuidance struct {
	MaxPlanAttempts                    int  `json:"max_plan_attempts"`
	MaxSchemaDescriptions              int  `json:"max_schema_descriptions"`
	RequireDescribeSchemaBeforePropose bool `json:"require_describe_schema_before_propose"`
}

// SchemaColumnSummary is a typed column contract exposed to an ACP provider.
type SchemaColumnSummary struct {
	Name    string `json:"name"`
	Kind    string `json:"kind"`
	Type    string `json:"type"`
	Indexed bool   `json:"indexed,omitempty"`
}

// TimeRangePayload is the BYDBQL-compatible time range from TUI slots.
type TimeRangePayload struct {
	Start string `json:"start"`
	End   string `json:"end"`
}

// QueryHints carries rule-based intent classification for prompt guidance.
type QueryHints struct {
	PreferShowTop bool   `json:"prefer_show_top,omitempty"`
	TimeRangeHint string `json:"time_range_hint,omitempty"`
	LimitHint     int    `json:"limit_hint,omitempty"`
	SlotsPinned   bool   `json:"slots_pinned"`
	AutoMatched   bool   `json:"auto_matched,omitempty"`
	UseSlots      bool   `json:"use_slots"`
}

// CatalogEntrySummary is one discoverable resource exposed to the agent.
type CatalogEntrySummary struct {
	Group string `json:"group"`
	Type  string `json:"type"`
	Name  string `json:"name"`
}

// ExecutionSummary is the compact execution feedback exposed to the agent.
type ExecutionSummary struct {
	Rows         int        `json:"rows"`
	Columns      []string   `json:"columns,omitempty"`
	Preview      [][]string `json:"preview,omitempty"`
	ResourceType string     `json:"resource_type,omitempty"`
	Duration     string     `json:"duration,omitempty"`
	Query        string     `json:"query"`
	Error        string     `json:"error,omitempty"`
}

// ProbeSummaryPayload is the bounded probe feedback exposed to the agent.
type ProbeSummaryPayload struct {
	Rows    int        `json:"rows"`
	Columns []string   `json:"columns,omitempty"`
	Preview [][]string `json:"preview,omitempty"`
	Query   string     `json:"query,omitempty"`
	Error   string     `json:"error,omitempty"`
}

// BuildBydbqlPrompt renders the provider prompt for BYDBQL generation.
func BuildBydbqlPrompt(req TurnRequest) (string, error) {
	parts, partsErr := BuildBydbqlPromptParts(req)
	if partsErr != nil {
		return "", partsErr
	}
	return parts.System + "\n\n" + parts.User, nil
}

// BuildBydbqlPromptParts separates trusted instructions from untrusted context.
func BuildBydbqlPromptParts(req TurnRequest) (prompt.Parts, error) {
	payload, marshalErr := MarshalPayload(req.Payload)
	if marshalErr != nil {
		return prompt.Parts{}, marshalErr
	}
	return prompt.BuildParts(prompt.Input{
		TaskPrompt:      req.Prompt,
		PayloadJSON:     payload,
		Candidate:       req.Payload.Candidate,
		ExecutionPolicy: req.Payload.Constraints.ExecutionPolicy,
	}), nil
}

// EventKind identifies a normalized event emitted by an agent adapter.
type EventKind string

// Normalized agent events.
const (
	EventKindMessageDelta      EventKind = "message_delta"
	EventKindPermissionRequest EventKind = "permission_request"
	EventKindPlanUpdate        EventKind = "plan_update"
	EventKindToolCall          EventKind = "tool_call"
	EventKindToolResult        EventKind = "tool_result"
	EventKindClarification     EventKind = "clarification"
	EventKindCandidate         EventKind = "candidate"
	EventKindApproval          EventKind = "approval"
	EventKindCancelled         EventKind = "cancelled"
	EventKindFinalResponse     EventKind = "final_response"
	EventKindError             EventKind = "error"
)

// EventOrigin identifies whether an event came from the provider or a trusted bydbctl component.
type EventOrigin string

// Event origins.
const (
	EventOriginProvider   EventOrigin = "provider"
	EventOriginToolBridge EventOrigin = "tool_bridge"
)

// EventStatus describes the lifecycle state of a visible agent action.
type EventStatus string

// Event lifecycle statuses.
const (
	EventStatusWaiting   EventStatus = "waiting"
	EventStatusRunning   EventStatus = "running"
	EventStatusSucceeded EventStatus = "succeeded"
	EventStatusFailed    EventStatus = "failed"
	EventStatusCancelled EventStatus = "cancelled"
)

// Event is the provider-neutral stream item consumed by WorkflowRunner and the TUI.
type Event struct {
	StartedAt     time.Time
	CompletedAt   time.Time
	ID            string
	Kind          EventKind
	Message       string
	Candidate     string
	Explanation   string
	Permission    string
	Origin        EventOrigin
	ToolName      string
	InputSummary  string
	InputDetail   string
	OutputSummary string
	Status        EventStatus
	Err           error
}

// AgentEvent is an alias for Event.
type AgentEvent = Event

// IsTerminal reports whether the event ends an agent turn.
func (event Event) IsTerminal() bool {
	return event.Kind == EventKindFinalResponse || event.Kind == EventKindError
}

// BuildAgentTurnRequest builds the structured request for one user-facing agent turn.
func BuildAgentTurnRequest(querySession *session.QuerySession, hints QueryHints, templateHint, turnHint string) RequestPayload {
	payload := buildRequestPayload(querySession, hints, templateHint, querySession.ExecutionPolicy)
	payload.TurnHint = strings.TrimSpace(turnHint)
	payload.Conversation = conversationSummary(querySession.Conversation)
	if querySession.CandidateSuperseded {
		payload.Candidate = ""
		payload.ValidationError = nil
		payload.ProbeSummary = nil
	}
	payload.Intent = classifyTurnIntent(querySession, payload)
	payload.Task = taskForIntent(payload.Intent)
	return payload
}

// BuildReviseRequest builds the structured request used by the BYDBQL refinement workflow.
func BuildReviseRequest(querySession *session.QuerySession, hints QueryHints, templateHint string) RequestPayload {
	payload := buildRequestPayload(querySession, hints, templateHint, querySession.ExecutionPolicy)
	payload.Intent = TurnIntentRefine
	if payload.ValidationError != nil {
		payload.Intent = TurnIntentRepair
	}
	payload.Task = taskForIntent(payload.Intent)
	return payload
}

func classifyTurnIntent(querySession *session.QuerySession, payload RequestPayload) TurnIntent {
	if querySession.CandidateSuperseded {
		return TurnIntentNewQuery
	}
	if payload.ValidationError != nil {
		return TurnIntentRepair
	}
	if strings.TrimSpace(payload.Candidate) == "" {
		return TurnIntentNewQuery
	}
	if strings.TrimSpace(payload.TurnHint) == "" {
		return TurnIntentNextStep
	}
	return TurnIntentRefine
}

func taskForIntent(intent TurnIntent) string {
	switch intent {
	case TurnIntentRefine:
		return "refine_query_plan"
	case TurnIntentRepair:
		return "repair_query_plan"
	case TurnIntentAnswer:
		return "answer_question"
	case TurnIntentNextStep:
		return "continue_workflow"
	default:
		return "new_query"
	}
}

func buildRequestPayload(querySession *session.QuerySession, hints QueryHints, templateHint string, executionPolicy approval.ExecutionPolicy) RequestPayload {
	normalizedPolicy := approval.NormalizeExecutionPolicy(string(executionPolicy))
	var candidate string
	if currentCandidate := querySession.CurrentCandidate(); currentCandidate != nil {
		candidate = currentCandidate.Query
	}
	var validationError *string
	if !querySession.Validation.Valid && querySession.Validation.Message != "" {
		message := querySession.Validation.Message
		validationError = &message
	}
	var executionSummary *ExecutionSummary
	if querySession.ExecutionResult.Query != "" || querySession.ExecutionResult.Error != "" {
		executionSummary = &ExecutionSummary{
			Rows:         querySession.ExecutionResult.Rows,
			Columns:      append([]string(nil), querySession.ExecutionResult.Columns...),
			Preview:      previewForProvider(querySession.ExecutionResult.Preview),
			ResourceType: querySession.ExecutionResult.ResourceType,
			Duration:     querySession.ExecutionResult.Duration.String(),
			Query:        querySession.ExecutionResult.Query,
			Error:        providerExecutionError(querySession.ExecutionResult.Error),
		}
	}
	var probeSummary *ProbeSummaryPayload
	if currentCandidate := querySession.CurrentCandidate(); currentCandidate != nil && currentCandidate.Probe != nil {
		probe := currentCandidate.Probe
		probeSummary = &ProbeSummaryPayload{
			Rows:    probe.Rows,
			Columns: append([]string(nil), probe.Columns...),
			Preview: previewForProvider(probe.Preview),
			Query:   probe.Query,
			Error:   providerExecutionError(probe.Error),
		}
	}
	readOnlyCandidate := approval.IsReadOnlyBYDBQL(candidate)
	typedColumnsReady := querySession.SchemaSnapshot.Loaded && len(querySession.SchemaSnapshot.Columns) > 0
	return RequestPayload{
		Goal:         querySession.UserGoal,
		Candidate:    candidate,
		TemplateHint: strings.TrimSpace(templateHint),
		QueryHints:   hints,
		Workflow: WorkflowGuidance{
			MaxPlanAttempts:                    DefaultMaxPlanAttempts,
			MaxSchemaDescriptions:              DefaultMaxSchemaDescriptions,
			RequireDescribeSchemaBeforePropose: false,
		},
		TimeRange: TimeRangePayload{
			Start: strings.TrimSpace(querySession.TimeRange.Start),
			End:   strings.TrimSpace(querySession.TimeRange.End),
		},
		Constraints: Constraints{
			ExecutionPolicy: string(normalizedPolicy),
			FinalArtifact:   "structured_query_plan",
			ReadOnly:        true,
			MustUseSchema:   true,
			UserMustEditOrConfirmBeforeExecute: candidate != "" &&
				!normalizedPolicy.AutoApprove(approval.SourceAgentTool, false, candidate),
			MustNotExecuteTools: false,
			AgentMayProbeData:   true,
			AgentMayExecuteWithoutPrompt: candidate != "" && readOnlyCandidate &&
				normalizedPolicy.AutoApprove(approval.SourceAgentTool, false, candidate),
		},
		Schema: SchemaSummary{
			Columns:            columnSummary(querySession.SchemaSnapshot.Columns),
			SortableIndexes:    sortableIndexSummary(querySession.SchemaSnapshot.SortableIndexes),
			Type:               querySession.SchemaSnapshot.Type.String(),
			Name:               querySession.SchemaSnapshot.Name,
			Groups:             append([]string(nil), querySession.SchemaSnapshot.Groups...),
			Tags:               append([]string(nil), querySession.SchemaSnapshot.Tags...),
			Fields:             append([]string(nil), querySession.SchemaSnapshot.Fields...),
			IndexedFields:      append([]string(nil), querySession.SchemaSnapshot.IndexedFields...),
			AvailableResources: append([]string(nil), querySession.SchemaSnapshot.ResourceNames...),
			AvailableGroups:    append([]string(nil), querySession.SchemaSnapshot.AvailableGroups...),
			Catalog:            catalogSummary(querySession.SchemaSnapshot.Catalog),
			SourceMeasure:      querySession.SchemaSnapshot.SourceMeasure,
			SourceMeasureGroup: querySession.SchemaSnapshot.SourceMeasureGroup,
			FieldValueSort:     querySession.SchemaSnapshot.FieldValueSort,
			Fingerprint:        querySession.SchemaSnapshot.Fingerprint,
			TypedColumnsReady:  typedColumnsReady,
		},
		ExecutionSummary: executionSummary,
		ProbeSummary:     probeSummary,
		ValidationError:  validationError,
	}
}

func sortableIndexSummary(indexes []session.SortableIndex) []SortableIndexSummary {
	if len(indexes) == 0 {
		return nil
	}
	summary := make([]SortableIndexSummary, 0, len(indexes))
	for _, index := range indexes {
		summary = append(summary, SortableIndexSummary{
			RuleName: index.RuleName,
			Tags:     append([]string(nil), index.Tags...),
		})
	}
	return summary
}

const (
	maxProviderConversationTurns = 6
	maxProviderPreviewRows       = 50
)

func previewForProvider(preview [][]string) [][]string {
	if len(preview) == 0 {
		return nil
	}
	previewLength := len(preview)
	if previewLength > maxProviderPreviewRows {
		previewLength = maxProviderPreviewRows
	}
	sharedPreview := make([][]string, 0, previewLength)
	for _, row := range preview[:previewLength] {
		sharedPreview = append(sharedPreview, append([]string(nil), row...))
	}
	return sharedPreview
}

func columnSummary(columns []session.SchemaColumn) []SchemaColumnSummary {
	if len(columns) == 0 {
		return nil
	}
	summary := make([]SchemaColumnSummary, 0, len(columns))
	for _, column := range columns {
		summary = append(summary, SchemaColumnSummary{
			Name:    column.Name,
			Kind:    string(column.Kind),
			Type:    string(column.Type),
			Indexed: column.Indexed,
		})
	}
	return summary
}

func conversationSummary(turns []session.ConversationTurn) []ConversationTurnPayload {
	if len(turns) == 0 {
		return nil
	}
	startTurn := len(turns) - maxProviderConversationTurns
	if startTurn < 0 {
		startTurn = 0
	}
	recentTurns := turns[startTurn:]
	summary := make([]ConversationTurnPayload, 0, len(recentTurns))
	for _, turn := range recentTurns {
		summary = append(summary, ConversationTurnPayload{
			Hint:      turn.Hint,
			Response:  turn.Response,
			Candidate: turn.Candidate,
		})
	}
	return summary
}

// MarshalPayload renders a structured request for subprocess prompts.
func MarshalPayload(payload RequestPayload) (string, error) {
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func catalogSummary(entries []session.CatalogEntry) []CatalogEntrySummary {
	if len(entries) == 0 {
		return nil
	}
	summary := make([]CatalogEntrySummary, 0, len(entries))
	for _, entry := range entries {
		summary = append(summary, CatalogEntrySummary{
			Group: entry.Group,
			Type:  entry.Type.String(),
			Name:  entry.Name,
		})
	}
	return summary
}

// CatalogEntrySummaries converts catalog entries into provider-safe summaries.
func CatalogEntrySummaries(entries []session.CatalogEntry) []CatalogEntrySummary {
	return catalogSummary(entries)
}
