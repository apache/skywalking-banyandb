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

package workflow

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/agent"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/agent/fake"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/approval"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/bridge"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/tools"
)

func TestSyncSessionUpdatesSlots(t *testing.T) {
	runner := NewRunner(Config{AgentGateway: fake.NewGateway()})
	querySession, startErr := runner.StartSession(context.Background(), StartOptions{
		ResourceType: session.ResourceTypeMeasure,
		ResourceName: "service_latency",
		Groups:       []string{"default"},
		Goal:         "first goal",
	})
	if startErr != nil {
		t.Fatalf("StartSession returned error: %v", startErr)
	}
	updatedSession, syncErr := runner.SyncSession(context.Background(), querySession, StartOptions{
		ResourceType:   session.ResourceTypeStream,
		ResourceName:   "sw",
		Groups:         []string{"production"},
		Goal:           "updated goal",
		TimeRange:      session.TimeRange{Start: "-1h"},
		NameProvided:   true,
		GroupsProvided: true,
		TypeProvided:   true,
	})
	if syncErr != nil {
		t.Fatalf("SyncSession returned error: %v", syncErr)
	}
	if updatedSession.ResourceName != "sw" {
		t.Fatalf("unexpected resource name: %s", updatedSession.ResourceName)
	}
	if updatedSession.ResourceType != session.ResourceTypeStream {
		t.Fatalf("unexpected resource type: %s", updatedSession.ResourceType)
	}
	if updatedSession.UserGoal != "updated goal" {
		t.Fatalf("unexpected goal: %s", updatedSession.UserGoal)
	}
	if len(updatedSession.Transcript) < 2 {
		t.Fatalf("expected refresh transcript entry: %+v", updatedSession.Transcript)
	}
}

func TestStartSessionDiscoversSchemaWithoutCandidate(t *testing.T) {
	runner := NewRunner(Config{AgentGateway: fake.NewGateway()})
	querySession, startErr := runner.StartSession(context.Background(), StartOptions{
		ResourceType: session.ResourceTypeTopN,
		ResourceName: "service_latency",
		Groups:       []string{"production"},
		TimeRange:    session.TimeRange{Start: "-30m"},
		Goal:         "top slow endpoints",
	})
	if startErr != nil {
		t.Fatalf("StartSession returned error: %v", startErr)
	}
	if querySession.CurrentCandidate() != nil {
		t.Fatalf("expected no candidate before agent generation: %+v", querySession.CurrentCandidate())
	}
	if querySession.Phase != session.PhaseIntent {
		t.Fatalf("unexpected phase: %s", querySession.Phase)
	}
	if querySession.SchemaSnapshot.Name != "service_latency" {
		t.Fatalf("unexpected schema snapshot: %+v", querySession.SchemaSnapshot)
	}
}

func TestStartSessionLeavesResourceUnresolvedForAutonomousDiscovery(t *testing.T) {
	executor := &catalogExecutor{catalog: session.SchemaCatalog{
		Groups: []string{"production"},
		Entries: []session.CatalogEntry{{
			Type:  session.ResourceTypeMeasure,
			Name:  "service_latency",
			Group: "production",
		}},
	}}
	runner := NewRunner(Config{AgentGateway: fake.NewGateway(), Executor: executor})
	querySession, startErr := runner.StartSession(context.Background(), StartOptions{Goal: "show slow endpoints"})
	if startErr != nil {
		t.Fatalf("StartSession returned error: %v", startErr)
	}
	if querySession.ResourceName != "" || querySession.ResourceType != "" || len(querySession.Groups) != 0 {
		t.Fatalf("expected no preselected schema, got %+v", querySession)
	}
	if len(querySession.SchemaSnapshot.Catalog) != 1 || executor.discoverSchemaCount != 0 {
		t.Fatalf("expected catalog-only discovery, got session=%+v executor=%+v", querySession.SchemaSnapshot, executor)
	}
}

func TestReviseWithFakeAgentAndExecuteAfterApproval(t *testing.T) {
	runner := NewRunner(Config{AgentGateway: fake.NewGateway()})
	querySession, startErr := runner.StartSession(context.Background(), StartOptions{
		ResourceType: session.ResourceTypeMeasure,
		ResourceName: "service_latency",
		Groups:       []string{"production"},
		Goal:         "average latency",
	})
	if startErr != nil {
		t.Fatalf("StartSession returned error: %v", startErr)
	}
	events, reviseErr := runner.ReviseWithAgent(context.Background(), querySession)
	if reviseErr != nil {
		t.Fatalf("ReviseWithAgent returned error: %v", reviseErr)
	}
	if len(events) == 0 {
		t.Fatal("expected agent events")
	}
	if querySession.Phase != session.PhaseReady {
		t.Fatalf("unexpected phase: %s", querySession.Phase)
	}
	if executeErr := executeAfterApproval(t, runner, querySession); executeErr != nil {
		t.Fatalf("ExecuteCurrent returned error: %v", executeErr)
	}
	if querySession.Phase != session.PhaseExecuted {
		t.Fatalf("expected executed phase, got %s", querySession.Phase)
	}
}

func TestExecuteCurrentRejectsInvalidCandidateBeforeApproval(t *testing.T) {
	runner := NewRunner(Config{AgentGateway: fake.NewGateway()})
	querySession, startErr := runner.StartSession(context.Background(), StartOptions{
		ResourceType: session.ResourceTypeStream,
		ResourceName: "sw",
		Groups:       []string{"default"},
		Goal:         "find logs",
	})
	if startErr != nil {
		t.Fatalf("StartSession returned error: %v", startErr)
	}
	if validateErr := runner.ValidateManualQuery(context.Background(), querySession, "SELECT FROM"); validateErr != nil {
		t.Fatalf("ValidateManualQuery returned error: %v", validateErr)
	}
	if executeErr := runner.ExecuteCurrent(context.Background(), querySession); executeErr == nil {
		t.Fatal("expected invalid candidate to be rejected")
	}
}

func TestExecuteCurrentRevalidatesAfterApproval(t *testing.T) {
	validator := &sequenceValidator{reports: []session.ValidationReport{
		{Valid: true, Message: "initial validation", QueryType: "MEASURE"},
		{Valid: false, Message: "schema changed", QueryType: "MEASURE"},
	}}
	runner := NewRunner(Config{AgentGateway: fake.NewGateway(), Validator: validator})
	querySession, startErr := runner.StartSession(context.Background(), StartOptions{
		ResourceType: session.ResourceTypeMeasure,
		ResourceName: "service_latency",
		Groups:       []string{"production"},
		Goal:         "average latency",
	})
	if startErr != nil {
		t.Fatalf("StartSession returned error: %v", startErr)
	}
	query := "SELECT * FROM MEASURE service_latency IN production TIME > '-30m' LIMIT 10"
	if validateErr := runner.ValidateManualQuery(context.Background(), querySession, query); validateErr != nil {
		t.Fatalf("ValidateManualQuery returned error: %v", validateErr)
	}
	executeErr := executeAfterApproval(t, runner, querySession)
	if executeErr == nil || !strings.Contains(executeErr.Error(), "failed revalidation") {
		t.Fatalf("expected immediate revalidation failure, got %v", executeErr)
	}
	if querySession.ExecutionResult.Query != "" {
		t.Fatalf("query must not execute after failed revalidation: %+v", querySession.ExecutionResult)
	}
}

func TestExecuteCurrentAdvancesOneCompiledWorkflowStep(t *testing.T) {
	firstQuery := "SELECT * FROM STREAM logs IN production TIME > '-30m' WHERE status = 500 LIMIT 10"
	secondQuery := "SELECT * FROM STREAM logs IN production TIME > '-30m' WHERE service = 'payment' LIMIT 10"
	schema := session.SchemaSnapshot{
		Type:   session.ResourceTypeStream,
		Name:   "logs",
		Groups: []string{"production"},
		Columns: []session.SchemaColumn{
			{Name: "service", Kind: session.SchemaColumnTag, Type: session.SchemaValueTypeString},
			{Name: "status", Kind: session.SchemaColumnTag, Type: session.SchemaValueTypeInt},
		},
	}
	executor := &catalogExecutor{schema: schema, result: session.ExecutionResult{Rows: 1, Summary: "one row"}}
	validator := &sequenceValidator{reports: []session.ValidationReport{
		{Valid: true, QueryType: "STREAM"},
		{Valid: true, QueryType: "STREAM"},
	}}
	runner := NewRunner(Config{AgentGateway: fake.NewGateway(), Executor: executor, Validator: validator})
	querySession := &session.QuerySession{
		Phase:          session.PhaseReady,
		ResourceType:   session.ResourceTypeStream,
		ResourceName:   "logs",
		Groups:         []string{"production"},
		SchemaSnapshot: schema,
		PlannedQueries: []session.PlannedQuery{
			{ID: "first", Query: firstQuery, ResourceType: session.ResourceTypeStream, Name: "logs", Groups: []string{"production"}},
			{ID: "second", Query: secondQuery, ResourceType: session.ResourceTypeStream, Name: "logs", Groups: []string{"production"}},
		},
	}
	querySession.AddCandidate(session.BydbqlCandidate{Query: firstQuery, Validation: session.ValidationReport{Valid: true, QueryType: "STREAM"}})
	if executeErr := executeAfterApproval(t, runner, querySession); executeErr != nil {
		t.Fatalf("ExecuteCurrent returned error: %v", executeErr)
	}
	if querySession.Phase != session.PhaseReady {
		t.Fatalf("expected next workflow statement to be ready, got %s", querySession.Phase)
	}
	nextPlanStep := querySession.CurrentPlannedQuery()
	if nextPlanStep == nil || nextPlanStep.ID != "second" {
		t.Fatalf("expected second plan step, got %+v", nextPlanStep)
	}
	nextCandidate := querySession.CurrentCandidate()
	if nextCandidate == nil || nextCandidate.Query != secondQuery || !nextCandidate.Validation.Valid {
		t.Fatalf("expected valid second candidate, got %+v", nextCandidate)
	}
}

func TestReviseWithAgentRejectsCandidateEmbeddedInMessage(t *testing.T) {
	gateway := scriptedGateway{
		events: []agent.Event{
			{
				Kind:    agent.EventKindFinalResponse,
				Message: "```bydbql\nSELECT * FROM STREAM sw IN default TIME > '-30m' LIMIT 10\n```",
			},
		},
	}
	runner := NewRunner(Config{AgentGateway: gateway})
	querySession, startErr := runner.StartSession(context.Background(), StartOptions{
		ResourceType: session.ResourceTypeStream,
		ResourceName: "sw",
		Groups:       []string{"default"},
		Goal:         "find logs",
	})
	if startErr != nil {
		t.Fatalf("StartSession returned error: %v", startErr)
	}
	if _, reviseErr := runner.ReviseWithAgent(context.Background(), querySession); reviseErr == nil {
		t.Fatal("expected unstructured response to be rejected")
	}
}

func TestReviseWithAgentRejectsCandidateEmbeddedInChunkedMessages(t *testing.T) {
	gateway := scriptedGateway{
		events: []agent.Event{
			{
				Kind:    agent.EventKindMessageDelta,
				Message: "Here is the final query:\n```bydbql\nSELECT *",
			},
			{
				Kind:    agent.EventKindMessageDelta,
				Message: "FROM STREAM sw IN default",
			},
			{
				Kind:    agent.EventKindMessageDelta,
				Message: "TIME > '-30m' LIMIT 10\n```",
			},
		},
	}
	runner := NewRunner(Config{AgentGateway: gateway})
	querySession, startErr := runner.StartSession(context.Background(), StartOptions{
		ResourceType: session.ResourceTypeStream,
		ResourceName: "sw",
		Groups:       []string{"default"},
		Goal:         "find logs",
	})
	if startErr != nil {
		t.Fatalf("StartSession returned error: %v", startErr)
	}
	if _, reviseErr := runner.ReviseWithAgent(context.Background(), querySession); reviseErr == nil {
		t.Fatal("expected unstructured chunks to be rejected")
	}
}

func TestReviseWithAgentRejectsCandidateEmbeddedInFragmentedProviderOutput(t *testing.T) {
	gateway := scriptedGateway{
		events: []agent.Event{
			{Kind: agent.EventKindMessageDelta, Message: "```"},
			{Kind: agent.EventKindMessageDelta, Message: "by"},
			{Kind: agent.EventKindMessageDelta, Message: "db"},
			{Kind: agent.EventKindMessageDelta, Message: "ql"},
			{Kind: agent.EventKindMessageDelta, Message: "text"},
			{Kind: agent.EventKindMessageDelta, Message: "SELECT"},
			{Kind: agent.EventKindMessageDelta, Message: "*"},
			{Kind: agent.EventKindMessageDelta, Message: "FROM"},
			{Kind: agent.EventKindMessageDelta, Message: "ME"},
			{Kind: agent.EventKindMessageDelta, Message: "ASURE"},
			{Kind: agent.EventKindMessageDelta, Message: "service"},
			{Kind: agent.EventKindMessageDelta, Message: "_endpoint"},
			{Kind: agent.EventKindMessageDelta, Message: "_latency"},
			{Kind: agent.EventKindMessageDelta, Message: "IN"},
			{Kind: agent.EventKindMessageDelta, Message: "default"},
			{Kind: agent.EventKindMessageDelta, Message: "TIME"},
			{Kind: agent.EventKindMessageDelta, Message: ">"},
			{Kind: agent.EventKindMessageDelta, Message: "'-"},
			{Kind: agent.EventKindMessageDelta, Message: "30"},
			{Kind: agent.EventKindMessageDelta, Message: "m"},
			{Kind: agent.EventKindMessageDelta, Message: "'"},
			{Kind: agent.EventKindMessageDelta, Message: "LIMIT"},
			{Kind: agent.EventKindMessageDelta, Message: "text"},
			{Kind: agent.EventKindMessageDelta, Message: "100"},
			{Kind: agent.EventKindMessageDelta, Message: "text"},
			{Kind: agent.EventKindMessageDelta, Message: "```"},
		},
	}
	runner := NewRunner(Config{AgentGateway: gateway})
	querySession, startErr := runner.StartSession(context.Background(), StartOptions{
		ResourceType: session.ResourceTypeMeasure,
		ResourceName: "service_endpoint_latency",
		Groups:       []string{"default"},
		Goal:         "生成正常的 bydbql, limit100",
	})
	if startErr != nil {
		t.Fatalf("StartSession returned error: %v", startErr)
	}
	if _, reviseErr := runner.ReviseWithAgent(context.Background(), querySession); reviseErr == nil {
		t.Fatal("expected fragmented unstructured output to be rejected")
	}
}

func TestNormalizeAgentDisplayTextPreservesNaturalLanguage(t *testing.T) {
	input := "I need more information before writing a query."
	got := NormalizeAgentDisplayText(input)
	if got != input {
		t.Fatalf("got %q want %q", got, input)
	}
}

func TestNormalizeAgentDisplayTextRepairsProviderFragments(t *testing.T) {
	input := "The top candidate is ` end point _m q _ cons ume _l atency _h our ` in ` sw _ metrics Hour `"
	got := NormalizeAgentDisplayText(input)
	if strings.Contains(got, "topcandidate") || strings.Contains(got, "end point") {
		t.Fatalf("expected natural language and identifier fragments to be repaired, got %q", got)
	}
}

func TestNormalizeFragmentedAgentTextTimeRange(t *testing.T) {
	input := "TIME > '- 30 m '"
	got := normalizeFragmentedAgentText(input)
	want := "TIME > '-30m'"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestNormalizeFragmentedAgentTextAggregateAVG(t *testing.T) {
	input := "SHOW TOP 10 FROM MEASURE latency IN default TIME > '-30m' AGGREGATE BY AV G ORDER BY DESC"
	want := "SHOW TOP 10 FROM MEASURE latency IN default TIME > '-30m' AGGREGATE BY AVG ORDER BY DESC"
	got := RepairFragmentedQuery(input)
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestRepairFragmentedQueryAggregateAVG(t *testing.T) {
	input := "SHOW TOP 10 FROM MEASURE service_endpoint_latency IN default TIME > '-30m' AGGREGATE BY AV G ORDER BY DESC"
	want := "SHOW TOP 10 FROM MEASURE service_endpoint_latency IN default TIME > '-30m' AGGREGATE BY AVG ORDER BY DESC"
	got := RepairFragmentedQuery(input)
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestFinalCandidateDoesNotInferFromProviderMessageFragments(t *testing.T) {
	fragments := []string{
		"```", "b", "yd", "b", "ql", "text", "SH", "OW", "TOP", "text", "10", "FROM", "ME", "AS", "URE",
		"service", "_end", "point", "_l", "at", "ency", "IN", "sw", "_", "metrics", "TIME", ">", "'-", "30", "m", "'",
		"A", "GG", "REG", "ATE", "BY", "SUM", "ORDER", "BY", "DESC", "text", "```",
	}
	var events []agent.Event
	events = append(events, agent.Event{Kind: agent.EventKindPlanUpdate, Message: "available commands updated"})
	var buffered []string
	for _, fragment := range fragments {
		events = append(events, agent.Event{Kind: agent.EventKindMessageDelta, Message: fragment})
		buffered = append(buffered, fragment)
	}
	events = append(events, agent.Event{
		Kind:    agent.EventKindFinalResponse,
		Message: strings.Join(buffered, "\n"),
	})
	candidate := finalCandidate(events)
	if candidate != "" {
		t.Fatalf("unexpected inferred candidate: %s", candidate)
	}
}

func TestFinalCandidateAcceptsOnlyControlledPlanProposals(t *testing.T) {
	query := "SELECT * FROM MEASURE service_latency IN production TIME > '-30m' LIMIT 10"
	if candidate := finalCandidate([]agent.Event{{Kind: agent.EventKindCandidate, Candidate: query, ToolName: "validate_bydbql"}}); candidate != "" {
		t.Fatalf("unexpected raw candidate acceptance: %s", candidate)
	}
	if candidate := finalCandidate([]agent.Event{{Kind: agent.EventKindCandidate, Candidate: query, ToolName: "propose_query_plan"}}); candidate != "" {
		t.Fatalf("unexpected provider proposal spoof acceptance: %s", candidate)
	}
	candidate := finalCandidate([]agent.Event{{
		Kind:      agent.EventKindCandidate,
		Candidate: query,
		Origin:    agent.EventOriginToolBridge,
		ToolName:  "propose_query_plan",
		Status:    agent.EventStatusSucceeded,
	}})
	if candidate != query {
		t.Fatalf("expected controlled proposal candidate, got %q", candidate)
	}
	failedCandidate := finalCandidate([]agent.Event{{
		Kind:      agent.EventKindCandidate,
		Candidate: query,
		Origin:    agent.EventOriginToolBridge,
		ToolName:  "propose_query_plan",
		Status:    agent.EventStatusFailed,
	}})
	if failedCandidate != "" {
		t.Fatalf("failed proposal became a workspace candidate: %q", failedCandidate)
	}
}

func TestDrainBridgeEventsRetainsProposalAfterAgentStreamCloses(t *testing.T) {
	toolEvents := make(chan agent.Event, 1)
	toolEvents <- agent.Event{
		Kind:      agent.EventKindCandidate,
		Origin:    agent.EventOriginToolBridge,
		ToolName:  "propose_query_plan",
		Candidate: "SELECT * FROM MEASURE service_latency IN production TIME > '-30m' LIMIT 10",
		Status:    agent.EventStatusSucceeded,
	}
	updates := make(chan TurnUpdate, 1)
	events := drainBridgeEvents(toolEvents, &session.QuerySession{}, updates, nil)
	if len(events) != 1 || finalCandidate(events) == "" {
		t.Fatalf("expected drained proposal event, got %+v", events)
	}
	if update := <-updates; update.Event == nil || update.Event.ToolName != "propose_query_plan" {
		t.Fatalf("unexpected streamed bridge update: %+v", update)
	}
}

func TestReviseWithAgentRejectsCandidateEmbeddedInJSONMessage(t *testing.T) {
	gateway := scriptedGateway{
		events: []agent.Event{
			{
				Kind: agent.EventKindFinalResponse,
				Message: `{
					"candidate": "SELECT * FROM MEASURE service_latency IN production TIME > '-30m' LIMIT 10"
				}`,
			},
		},
	}
	runner := NewRunner(Config{AgentGateway: gateway})
	querySession, startErr := runner.StartSession(context.Background(), StartOptions{
		ResourceType: session.ResourceTypeMeasure,
		ResourceName: "service_latency",
		Groups:       []string{"production"},
		Goal:         "average latency",
	})
	if startErr != nil {
		t.Fatalf("StartSession returned error: %v", startErr)
	}
	if _, reviseErr := runner.ReviseWithAgent(context.Background(), querySession); reviseErr == nil {
		t.Fatal("expected JSON text without a structured event to be rejected")
	}
}

func TestReviseWithAgentKeepsConversationWithoutCandidate(t *testing.T) {
	gateway := scriptedGateway{
		events: []agent.Event{
			{
				Kind:    agent.EventKindFinalResponse,
				Message: "I need more information before writing a query.",
			},
		},
	}
	runner := NewRunner(Config{AgentGateway: gateway})
	querySession, startErr := runner.StartSession(context.Background(), StartOptions{
		ResourceType: session.ResourceTypeMeasure,
		ResourceName: "service_latency",
		Groups:       []string{"production"},
		Goal:         "average latency",
	})
	if startErr != nil {
		t.Fatalf("StartSession returned error: %v", startErr)
	}
	if _, reviseErr := runner.ReviseWithAgent(context.Background(), querySession); reviseErr != nil {
		t.Fatalf("ReviseWithAgent returned error: %v", reviseErr)
	}
	if querySession.Phase != session.PhaseConversation {
		t.Fatalf("expected conversation phase, got %s", querySession.Phase)
	}
	if querySession.CurrentCandidate() != nil {
		t.Fatalf("unexpected candidate: %+v", querySession.CurrentCandidate())
	}
	if len(querySession.Conversation) != 1 || querySession.Conversation[0].Response != "I need more information before writing a query." {
		t.Fatalf("expected conversation response, got %+v", querySession.Conversation)
	}
}

func TestReviseWithAgentAllowsConversationThatMentionsSelect(t *testing.T) {
	gateway := scriptedGateway{
		events: []agent.Event{{
			Kind:    agent.EventKindFinalResponse,
			Message: "The typed planner can create SELECT queries after you choose a metric.",
		}},
	}
	runner := NewRunner(Config{AgentGateway: gateway})
	querySession, startErr := runner.StartSession(context.Background(), StartOptions{
		ResourceType: session.ResourceTypeMeasure,
		ResourceName: "service_latency",
		Groups:       []string{"production"},
		Goal:         "what queries can I create?",
	})
	if startErr != nil {
		t.Fatalf("StartSession returned error: %v", startErr)
	}
	if _, reviseErr := runner.ReviseWithAgent(context.Background(), querySession); reviseErr != nil {
		t.Fatalf("ReviseWithAgent returned error: %v", reviseErr)
	}
	if querySession.Phase != session.PhaseConversation {
		t.Fatalf("expected conversation phase, got %s", querySession.Phase)
	}
}

func TestCompleteAgentTurnDoesNotPublishDraftFromFailedPropose(t *testing.T) {
	runner := NewRunner(Config{})
	querySession := &session.QuerySession{
		SchemaSnapshot: session.SchemaSnapshot{
			Type:   session.ResourceTypeMeasure,
			Name:   "service_latency",
			Groups: []string{"production"},
			Loaded: true,
		},
	}
	completeErr := runner.completeAgentTurn(context.Background(), querySession, "cpu usage", []agent.Event{{
		Kind:      agent.EventKindCandidate,
		Origin:    agent.EventOriginToolBridge,
		ToolName:  bridge.ToolProposeQueryPlan,
		Candidate: "SELECT * FROM STREAM sw IN default WHERE",
		Status:    agent.EventStatusFailed,
		Message:   `typed schema metadata is required to use column "unknown"`,
	}, {
		Kind:    agent.EventKindFinalResponse,
		Message: "I could not validate the plan yet",
	}})
	if completeErr != nil {
		t.Fatalf("completeAgentTurn returned error: %v", completeErr)
	}
	if querySession.Phase != session.PhaseConversation {
		t.Fatalf("expected conversation phase, got %s", querySession.Phase)
	}
	currentCandidate := querySession.CurrentCandidate()
	if currentCandidate != nil {
		t.Fatalf("failed draft became an executable candidate: %+v", currentCandidate)
	}
	if len(querySession.Conversation) != 1 || !strings.Contains(querySession.Conversation[0].Response, "could not validate") {
		t.Fatalf("expected final explanation without a candidate, got %+v", querySession.Conversation)
	}
}

func TestCompleteAgentTurnDoesNotTreatToolOutputAsConversation(t *testing.T) {
	runner := NewRunner(Config{})
	querySession := &session.QuerySession{}
	completeErr := runner.completeAgentTurn(context.Background(), querySession, "", []agent.Event{{
		Kind:    agent.EventKindToolResult,
		Origin:  agent.EventOriginToolBridge,
		Message: "tool completed",
	}})
	if completeErr == nil {
		t.Fatal("expected tool output without an agent response to be rejected")
	}
	if len(querySession.Conversation) != 0 {
		t.Fatalf("tool output must not become a conversation turn: %+v", querySession.Conversation)
	}
}

func TestReviseWithAgentKeepsInvalidCandidateForNextTurn(t *testing.T) {
	gateway := scriptedGateway{
		events: []agent.Event{
			{
				Kind:      agent.EventKindCandidate,
				Origin:    agent.EventOriginToolBridge,
				ToolName:  "propose_query_plan",
				Candidate: "SELECT * FROM STREAM sw IN default WHERE",
			},
		},
	}
	runner := NewRunner(Config{AgentGateway: gateway})
	querySession, startErr := runner.StartSession(context.Background(), StartOptions{
		ResourceType: session.ResourceTypeStream,
		ResourceName: "sw",
		Groups:       []string{"default"},
		Goal:         "find logs",
	})
	if startErr != nil {
		t.Fatalf("StartSession returned error: %v", startErr)
	}
	events, turnErr := runner.RunAgentTurn(context.Background(), querySession, "draft initial query")
	if turnErr != nil {
		t.Fatalf("RunAgentTurn returned error: %v", turnErr)
	}
	if len(events) == 0 {
		t.Fatal("expected agent events")
	}
	if querySession.Phase != session.PhaseValidate {
		t.Fatalf("expected validate phase, got %s", querySession.Phase)
	}
	if !strings.Contains(querySession.Validation.Message, "syntax error") {
		t.Fatalf("expected validation detail, got: %s", querySession.Validation.Message)
	}
	currentCandidate := querySession.CurrentCandidate()
	if currentCandidate == nil || currentCandidate.Query == "" {
		t.Fatal("expected invalid candidate to remain for next turn")
	}
	if len(querySession.Conversation) != 1 {
		t.Fatalf("expected one conversation turn, got %d", len(querySession.Conversation))
	}
}

func TestStartAgentTurnStreamsEventsBeforeCompletion(t *testing.T) {
	gateway := scriptedGateway{events: []agent.Event{
		{Kind: agent.EventKindPlanUpdate, Message: "inspect schema"},
		{
			Kind:      agent.EventKindCandidate,
			Origin:    agent.EventOriginToolBridge,
			ToolName:  "propose_query_plan",
			Candidate: "SELECT * FROM MEASURE service_latency IN production TIME > '-30m' LIMIT 10",
		},
		{Kind: agent.EventKindFinalResponse, Message: "candidate ready"},
	}}
	runner := NewRunner(Config{AgentGateway: gateway})
	querySession, startErr := runner.StartSession(context.Background(), StartOptions{
		ResourceType: session.ResourceTypeMeasure,
		ResourceName: "service_latency",
		Groups:       []string{"production"},
		Goal:         "average latency",
	})
	if startErr != nil {
		t.Fatalf("StartSession returned error: %v", startErr)
	}
	updates, turnErr := runner.StartAgentTurn(context.Background(), querySession, "")
	if turnErr != nil {
		t.Fatalf("StartAgentTurn returned error: %v", turnErr)
	}
	firstUpdate := <-updates
	if firstUpdate.Event == nil || firstUpdate.Event.Kind != agent.EventKindPlanUpdate || firstUpdate.Done {
		t.Fatalf("expected first plan update before completion, got %+v", firstUpdate)
	}
	for update := range updates {
		if update.Done && update.Err != nil {
			t.Fatalf("unexpected streamed turn error: %v", update.Err)
		}
	}
	currentCandidate := querySession.CurrentCandidate()
	if currentCandidate == nil || currentCandidate.Query == "" {
		t.Fatal("expected stream completion to retain the structured candidate")
	}
}

func TestRunAgentTurnSynchronizesControlledBridgeState(t *testing.T) {
	schema := session.SchemaSnapshot{
		Type:   session.ResourceTypeMeasure,
		Name:   "service_latency",
		Groups: []string{"production"},
		Loaded: true,
		Columns: []session.SchemaColumn{
			{Name: "latency", Kind: session.SchemaColumnField, Type: session.SchemaValueTypeFloat},
		},
	}
	executor := &catalogExecutor{schema: schema}
	validator := &sequenceValidator{reports: []session.ValidationReport{
		{Valid: true, QueryType: "MEASURE"},
		{Valid: true, QueryType: "MEASURE"},
	}}
	toolBridge := bridge.New(bridge.Config{Executor: executor, Validator: validator})
	runner := NewRunner(Config{
		AgentGateway: controlledBridgeGateway{toolBridge: toolBridge},
		Executor:     executor,
		ToolBridge:   toolBridge,
		Validator:    validator,
	})
	querySession, startErr := runner.StartSession(context.Background(), StartOptions{
		ResourceType: session.ResourceTypeMeasure,
		ResourceName: "service_latency",
		Groups:       []string{"production"},
		Goal:         "show service latency",
	})
	if startErr != nil {
		t.Fatalf("StartSession returned error: %v", startErr)
	}
	if _, turnErr := runner.RunAgentTurn(context.Background(), querySession, "create the query"); turnErr != nil {
		t.Fatalf("RunAgentTurn returned error: %v", turnErr)
	}
	if len(querySession.PlannedQueries) != 1 || querySession.CurrentPlannedQuery() == nil {
		t.Fatalf("expected the runner to synchronize the controlled plan: %+v", querySession.PlannedQueries)
	}
	if querySession.CurrentCandidate() == nil || querySession.CurrentCandidate().Query != querySession.CurrentPlannedQuery().Query {
		t.Fatalf("expected the synchronized plan to publish the current candidate: %+v", querySession)
	}
}

func TestCancelledAgentTurnDoesNotPublishPartialConversation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gateway := blockingGateway{started: make(chan struct{})}
	runner := NewRunner(Config{AgentGateway: gateway})
	querySession, startErr := runner.StartSession(context.Background(), StartOptions{
		ResourceType: session.ResourceTypeMeasure,
		ResourceName: "service_latency",
		Groups:       []string{"production"},
		Goal:         "show service latency",
	})
	if startErr != nil {
		t.Fatalf("StartSession returned error: %v", startErr)
	}
	updates, turnErr := runner.StartAgentTurn(ctx, querySession, "create a query")
	if turnErr != nil {
		t.Fatalf("StartAgentTurn returned error: %v", turnErr)
	}
	<-gateway.started
	cancel()
	for update := range updates {
		if !update.Done {
			continue
		}
		if !errors.Is(update.Err, context.Canceled) {
			t.Fatalf("expected cancelled update, got %+v", update)
		}
	}
	if len(querySession.Conversation) != 0 || len(querySession.ChatMessages) != 1 {
		t.Fatalf("expected only the user message after cancellation, got conversation=%+v chat=%+v", querySession.Conversation, querySession.ChatMessages)
	}
}

func TestStopAgentTurnPreservesSessionID(t *testing.T) {
	var interruptedSessionID string
	gateway := scriptedGateway{interruptedSessionID: &interruptedSessionID}
	runner := NewRunner(Config{AgentGateway: gateway})
	querySession, startErr := runner.StartSession(context.Background(), StartOptions{
		ResourceType: session.ResourceTypeMeasure,
		ResourceName: "service_latency",
		Groups:       []string{"production"},
		Goal:         "show service latency",
	})
	if startErr != nil {
		t.Fatalf("StartSession returned error: %v", startErr)
	}
	originalSessionID := querySession.AgentSessionID
	if stopErr := runner.StopAgentTurn(context.Background(), querySession); stopErr != nil {
		t.Fatalf("StopAgentTurn returned error: %v", stopErr)
	}
	if interruptedSessionID != originalSessionID {
		t.Fatalf("interrupted session %q, want %q", interruptedSessionID, originalSessionID)
	}
	if querySession.AgentSessionID != originalSessionID {
		t.Fatalf("StopAgentTurn cleared the reusable session ID: %q", querySession.AgentSessionID)
	}
}

func TestReviseWithAgentIncludesExecutionSummary(t *testing.T) {
	var requests []agent.TurnRequest
	gateway := scriptedGateway{
		events: []agent.Event{
			{
				Kind:      agent.EventKindCandidate,
				Origin:    agent.EventOriginToolBridge,
				ToolName:  "propose_query_plan",
				Candidate: "SELECT * FROM MEASURE service_latency IN production TIME > '-30m' LIMIT 10",
			},
		},
		requests:         &requests,
		maintainsHistory: true,
	}
	runner := NewRunner(Config{AgentGateway: gateway})
	querySession, startErr := runner.StartSession(context.Background(), StartOptions{
		ResourceType: session.ResourceTypeMeasure,
		ResourceName: "service_latency",
		Groups:       []string{"production"},
		Goal:         "average latency",
	})
	if startErr != nil {
		t.Fatalf("StartSession returned error: %v", startErr)
	}
	querySession.AgentSessionID = "scripted"
	querySession.AddConversationTurn(session.ConversationTurn{Hint: "prior request", Response: "prior response"})
	query := "SELECT * FROM MEASURE service_latency IN production TIME > '-30m' LIMIT 10"
	if validateErr := runner.ValidateManualQuery(context.Background(), querySession, query); validateErr != nil {
		t.Fatalf("ValidateManualQuery returned error: %v", validateErr)
	}
	if executeErr := executeAfterApproval(t, runner, querySession); executeErr != nil {
		t.Fatalf("ExecuteCurrent returned error: %v", executeErr)
	}
	if _, reviseErr := runner.ReviseWithAgent(context.Background(), querySession); reviseErr != nil {
		t.Fatalf("ReviseWithAgent returned error: %v", reviseErr)
	}
	if len(requests) == 0 {
		t.Fatal("expected agent request")
	}
	payload := requests[0].Payload
	if payload.ExecutionSummary == nil {
		t.Fatal("expected execution summary in agent payload")
	}
	if len(payload.Conversation) != 0 {
		t.Fatalf("persistent provider received duplicated conversation history: %+v", payload.Conversation)
	}
	if payload.ExecutionSummary.Query != query {
		t.Fatalf("unexpected query: %s", payload.ExecutionSummary.Query)
	}
	if payload.ExecutionSummary.ResourceType != "" || len(payload.ExecutionSummary.Columns) != 0 {
		t.Fatalf("unexpected data-bearing execution summary: %+v", payload.ExecutionSummary)
	}
	if !payload.Constraints.UserMustEditOrConfirmBeforeExecute {
		t.Fatal("ask_every_time should require execution confirmation")
	}
	if payload.Constraints.AgentMayExecuteWithoutPrompt {
		t.Fatal("ask_every_time should not allow agent execution without a prompt")
	}
}

type scriptedGateway struct {
	requests             *[]agent.TurnRequest
	interruptedSessionID *string
	events               []agent.Event
	maintainsHistory     bool
}

type controlledBridgeGateway struct {
	toolBridge *bridge.ToolBridge
}

func (gateway controlledBridgeGateway) Start(_ context.Context, _ agent.StartRequest) (agent.Session, error) {
	return agent.Session{ID: "controlled-bridge", Provider: "controlled-bridge"}, nil
}

func (gateway controlledBridgeGateway) Send(ctx context.Context, _ string, _ agent.TurnRequest) (<-chan agent.Event, error) {
	result := gateway.toolBridge.Call(ctx, bridge.Call{
		Name: bridge.ToolProposeQueryPlan,
		Arguments: map[string]any{"plan": map[string]any{
			"resource": map[string]any{"type": "MEASURE", "name": "service_latency", "groups": []any{"production"}},
			"limit":    10,
		}},
	})
	if result.Err != nil {
		return nil, result.Err
	}
	events := make(chan agent.Event, 1)
	events <- agent.Event{Kind: agent.EventKindFinalResponse, Message: "query plan prepared"}
	close(events)
	return events, nil
}

func (gateway controlledBridgeGateway) Interrupt(_ context.Context, _ string) error {
	return nil
}

func (gateway controlledBridgeGateway) Close() error {
	return nil
}

type blockingGateway struct {
	started chan struct{}
}

func (gateway blockingGateway) Start(_ context.Context, _ agent.StartRequest) (agent.Session, error) {
	return agent.Session{ID: "blocking", Provider: "blocking"}, nil
}

func (gateway blockingGateway) Send(ctx context.Context, _ string, _ agent.TurnRequest) (<-chan agent.Event, error) {
	events := make(chan agent.Event, 1)
	events <- agent.Event{Kind: agent.EventKindMessageDelta, Message: "partial response"}
	close(gateway.started)
	go func() {
		defer close(events)
		<-ctx.Done()
	}()
	return events, nil
}

func (gateway blockingGateway) Interrupt(_ context.Context, _ string) error {
	return nil
}

func (gateway blockingGateway) Close() error {
	return nil
}

func (gateway scriptedGateway) Start(_ context.Context, _ agent.StartRequest) (agent.Session, error) {
	return agent.Session{ID: "scripted", Provider: "scripted"}, nil
}

func (gateway scriptedGateway) Send(ctx context.Context, _ string, req agent.TurnRequest) (<-chan agent.Event, error) {
	if gateway.requests != nil {
		*gateway.requests = append(*gateway.requests, req)
	}
	events := make(chan agent.Event, len(gateway.events))
	go func() {
		defer close(events)
		for _, event := range gateway.events {
			select {
			case <-ctx.Done():
				return
			case events <- event:
			}
		}
	}()
	return events, nil
}

func (gateway scriptedGateway) Interrupt(_ context.Context, sessionID string) error {
	if gateway.interruptedSessionID != nil {
		*gateway.interruptedSessionID = sessionID
	}
	return nil
}

func (gateway scriptedGateway) Close() error {
	return nil
}

func (gateway scriptedGateway) MaintainsConversationHistory() bool {
	return gateway.maintainsHistory
}

func executeAfterApproval(t *testing.T, runner *Runner, querySession *session.QuerySession) error {
	t.Helper()
	executeErrCh := make(chan error, 1)
	go func() {
		executeErrCh <- runner.ExecuteCurrent(context.Background(), querySession)
	}()
	needsInteractiveApproval := true
	if currentCandidate := querySession.CurrentCandidate(); currentCandidate != nil {
		policy := approval.NormalizeExecutionPolicy(string(querySession.ExecutionPolicy))
		needsInteractiveApproval = !policy.AutoApprove(approval.SourceManual, false, currentCandidate.Query)
	}
	if needsInteractiveApproval {
		request := <-runner.ApprovalRequests()
		if resolveErr := runner.ResolveApproval(request.ID, true); resolveErr != nil {
			t.Fatalf("failed to approve execution: %v", resolveErr)
		}
	}
	return <-executeErrCh
}

type catalogExecutor struct {
	catalog             session.SchemaCatalog
	schema              session.SchemaSnapshot
	result              session.ExecutionResult
	discoverSchemaCount int
}

func (executor *catalogExecutor) DiscoverCatalog(_ context.Context) (session.SchemaCatalog, error) {
	return executor.catalog, nil
}

func (executor *catalogExecutor) DiscoverSchema(_ context.Context, _ tools.SchemaRequest) (session.SchemaSnapshot, error) {
	executor.discoverSchemaCount++
	return executor.schema, nil
}

func (executor *catalogExecutor) Execute(_ context.Context, _ *session.QuerySession, _ string) (session.ExecutionResult, error) {
	return executor.result, nil
}

func TestCompleteAgentTurnKeepsClarificationWithoutCandidate(t *testing.T) {
	runner := NewRunner(Config{
		Validator: &sequenceValidator{reports: []session.ValidationReport{{Valid: true, Message: "valid", QueryType: "TOPN"}}},
	})
	querySession := &session.QuerySession{
		UserGoal:     "top 10 slow payment endpoints in last 30 minutes",
		ResourceType: session.ResourceTypeMeasure,
		ResourceName: "service_endpoint_latency",
		Groups:       []string{"sw_metrics"},
		AutoMatched:  true,
		SchemaSnapshot: session.SchemaSnapshot{
			Type:   session.ResourceTypeMeasure,
			Name:   "service_endpoint_latency",
			Groups: []string{"sw_metrics"},
			Loaded: true,
			Columns: []session.SchemaColumn{
				{Name: "endpoint", Kind: session.SchemaColumnTag, Type: session.SchemaValueTypeString, Indexed: true},
				{Name: "latency", Kind: session.SchemaColumnField, Type: session.SchemaValueTypeFloat},
			},
		},
	}
	completeErr := runner.completeAgentTurn(context.Background(), querySession, "", []agent.Event{
		{Kind: agent.EventKindClarification, Message: "Which service group should I use?"},
	})
	if completeErr != nil {
		t.Fatalf("completeAgentTurn returned error: %v", completeErr)
	}
	if querySession.CurrentCandidate() != nil {
		t.Fatalf("unexpected candidate: %+v", querySession.CurrentCandidate())
	}
	if querySession.Phase != session.PhaseClarifying {
		t.Fatalf("expected clarification phase, got %s", querySession.Phase)
	}
	if len(querySession.Conversation) != 1 || querySession.Conversation[0].Response != "Which service group should I use?" {
		t.Fatalf("expected clarification conversation, got %+v", querySession.Conversation)
	}
	if len(querySession.ChatMessages) != 1 || querySession.ChatMessages[0].Role != session.ChatRoleAssistant {
		t.Fatalf("expected assistant clarification message, got %+v", querySession.ChatMessages)
	}
}

type sequenceValidator struct {
	reports []session.ValidationReport
	calls   int
}

func (validator *sequenceValidator) Validate(_ context.Context, _ string, _ *session.SchemaSnapshot) (session.ValidationReport, error) {
	reportIndex := validator.calls
	validator.calls++
	if reportIndex >= len(validator.reports) {
		reportIndex = len(validator.reports) - 1
	}
	return validator.reports[reportIndex], nil
}
