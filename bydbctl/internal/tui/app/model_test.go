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

package app

import (
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/agent"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/applog"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/approval"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/bridge"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/workflow"
)

func TestPendingApprovalStatusSurvivesTurnTimeout(t *testing.T) {
	startedAt := time.Now()
	model := NewModel(Config{Provider: "claude"})
	model.busy = true
	model.turnStartedAt = startedAt
	updatedModel, _ := model.Update(approvalMsg{request: approval.NewRequest(
		"SELECT * FROM MEASURE endpoint_traffic_minute IN sw_metadata TIME > '-30m' LIMIT 10",
		"MEASURE/endpoint_traffic_minute",
		[]string{"sw_metadata"},
		approval.SourceManual,
	)})
	typedModel, ok := updatedModel.(Model)
	if !ok {
		t.Fatalf("unexpected model type: %T", updatedModel)
	}
	timedOutModel, timeoutCmd := typedModel.Update(turnTimeoutMsg{startedAt: startedAt})
	typedTimedOutModel, ok := timedOutModel.(Model)
	if !ok {
		t.Fatalf("unexpected timed out model type: %T", timedOutModel)
	}
	if typedTimedOutModel.status != "execution approval required" {
		t.Fatalf("unexpected approval status after timeout: %q", typedTimedOutModel.status)
	}
	if timeoutCmd == nil {
		t.Fatal("expected timeout monitoring to continue while approval is pending")
	}
	if strings.Contains(typedTimedOutModel.View(), "agent turn in progress") {
		t.Fatalf("approval wait must not be labeled as an agent turn:\n%s", typedTimedOutModel.View())
	}
}

func TestUpdateSyncsSessionAndEventsBeforeError(t *testing.T) {
	sessionLog, createErr := applog.New(t.TempDir())
	if createErr != nil {
		t.Fatalf("failed to create session log: %v", createErr)
	}
	defer func() {
		_ = sessionLog.Close()
	}()
	model := NewModel(Config{SessionLog: sessionLog})
	querySession := &session.QuerySession{}
	querySession.AddCandidate(session.BydbqlCandidate{
		Query:  "SELECT * FROM STREAM sw IN default WHERE",
		Source: session.CandidateSourceAgent,
		Validation: session.ValidationReport{
			Valid:   false,
			Message: "syntax error: expected expression",
		},
	})
	updatedModel, _ := model.Update(workflowMsg{
		querySession: querySession,
		events: []agent.Event{
			{
				Kind:    agent.EventKindMessageDelta,
				Message: "agent raw output",
			},
		},
		err: errors.New("agent candidate failed validation"),
	})
	typedModel, ok := updatedModel.(Model)
	if !ok {
		t.Fatalf("unexpected model type: %T", updatedModel)
	}
	if typedModel.query.Value() != "SELECT * FROM STREAM sw IN default WHERE" {
		t.Fatalf("unexpected query value: %s", typedModel.query.Value())
	}
	events := strings.Join(typedModel.events, "\n")
	for _, expected := range []string{"validation:", "invalid candidate", "error: agent candidate failed validation"} {
		if !strings.Contains(events, expected) {
			t.Fatalf("expected compact event %q in:\n%s", expected, events)
		}
	}
	if strings.Contains(events, "agent raw output") {
		t.Fatalf("message delta should not appear in compact events:\n%s", events)
	}
	logBytes, readErr := os.ReadFile(sessionLog.Path())
	if readErr != nil {
		t.Fatalf("failed to read session log: %v", readErr)
	}
	logContent := string(logBytes)
	for _, expected := range []string{"syntax error: expected expression", "agent candidate failed validation"} {
		if !strings.Contains(logContent, expected) {
			t.Fatalf("expected log to contain %q:\n%s", expected, logContent)
		}
	}
	if strings.Contains(logContent, "agent raw output") {
		t.Fatalf("provider output must not be persisted by default:\n%s", logContent)
	}
}

func TestAgentStartedShowsSentMessageImmediately(t *testing.T) {
	model := NewModel(Config{})
	model.message.SetValue("show payment latency")
	querySession := &session.QuerySession{}
	querySession.AddChatMessage(session.ChatMessage{
		Role:    session.ChatRoleUser,
		Content: "show payment latency",
	})
	updatedModel, _ := model.Update(agentStartedMsg{querySession: querySession})
	typedModel, ok := updatedModel.(Model)
	if !ok {
		t.Fatalf("unexpected model type: %T", updatedModel)
	}
	if typedModel.querySession != querySession {
		t.Fatal("expected the active query session to update before streaming begins")
	}
	if typedModel.message.Value() != "" {
		t.Fatalf("expected sent message to clear immediately, got %q", typedModel.message.Value())
	}
	if !strings.Contains(typedModel.View(), "You › show payment latency") {
		t.Fatalf("expected sent message in conversation:\n%s", typedModel.View())
	}
}

func TestSendShowsMessageBeforeAgentSessionStarts(t *testing.T) {
	model := NewModel(Config{})
	model.message.SetValue("show payment latency")
	updatedModel, _ := model.Update(tea.KeyMsg{Type: tea.KeyEnter})
	typedModel, ok := updatedModel.(Model)
	if !ok {
		t.Fatalf("unexpected model type: %T", updatedModel)
	}
	if typedModel.message.Value() != "" {
		t.Fatalf("expected the composer to clear immediately, got %q", typedModel.message.Value())
	}
	if typedModel.currentGoal() != "show payment latency" {
		t.Fatalf("expected the first message to remain available for session setup, got %q", typedModel.currentGoal())
	}
	if !strings.Contains(typedModel.View(), "You › show payment latency") {
		t.Fatalf("expected the queued user message in conversation:\n%s", typedModel.View())
	}
}

func TestCtrlADoesNotSendComposerMessage(t *testing.T) {
	model := NewModel(Config{})
	model.message.SetValue("show payment latency")
	updatedModel, _ := model.Update(tea.KeyMsg{Type: tea.KeyCtrlA})
	typedModel, ok := updatedModel.(Model)
	if !ok {
		t.Fatalf("unexpected model type: %T", updatedModel)
	}
	if typedModel.busy {
		t.Fatal("Ctrl+A must not start an agent turn")
	}
	if typedModel.message.Value() != "show payment latency" {
		t.Fatalf("Ctrl+A must preserve the composer message, got %q", typedModel.message.Value())
	}
	if strings.Contains(typedModel.View(), "You › show payment latency") {
		t.Fatalf("Ctrl+A must not submit the composer message:\n%s", typedModel.View())
	}
}

func TestCtrlVDoesNotStartManualValidation(t *testing.T) {
	model := NewModel(Config{})
	updatedModel, _ := model.Update(tea.KeyMsg{Type: tea.KeyCtrlV})
	typedModel, ok := updatedModel.(Model)
	if !ok {
		t.Fatalf("unexpected model type: %T", updatedModel)
	}
	if typedModel.busy {
		t.Fatal("Ctrl+V must not start manual validation")
	}
	if typedModel.status == "validating query" {
		t.Fatalf("Ctrl+V must not change validation status, got %q", typedModel.status)
	}
}

func TestCtrlFFocusesDataPreviewWithoutStartingAgent(t *testing.T) {
	model := NewModel(Config{})
	querySession := &session.QuerySession{}
	querySession.AddCandidate(session.BydbqlCandidate{
		Query: "SELECT FROM",
		Validation: session.ValidationReport{
			Message: "syntax error near FROM",
		},
	})
	model.querySession = querySession
	model.syncQuerySession()
	updatedModel, updateCmd := model.Update(tea.KeyMsg{Type: tea.KeyCtrlF})
	if updateCmd != nil {
		t.Fatal("Ctrl+F must focus the preview without starting an agent turn")
	}
	typedModel, ok := updatedModel.(Model)
	if !ok {
		t.Fatalf("unexpected model type: %T", updatedModel)
	}
	if !strings.Contains(typedModel.View(), "Data Preview · focused") {
		t.Fatalf("expected Ctrl+F to visibly focus the Data Preview:\n%s", typedModel.View())
	}
}

func TestCtrlGRepairsInvalidCandidateAndCtrlYRefreshesPreview(t *testing.T) {
	model := NewModel(Config{})
	querySession := &session.QuerySession{}
	querySession.AddCandidate(session.BydbqlCandidate{
		Query: "SELECT FROM",
		Validation: session.ValidationReport{
			Message: "syntax error near FROM",
		},
	})
	model.querySession = querySession
	model.syncQuerySession()

	repairModel, repairCmd := model.Update(tea.KeyMsg{Type: tea.KeyCtrlG})
	typedRepairModel, ok := repairModel.(Model)
	if !ok {
		t.Fatalf("unexpected model type: %T", repairModel)
	}
	if !typedRepairModel.busy || repairCmd == nil {
		t.Fatal("Ctrl+G must start an Agent repair turn for an invalid candidate")
	}
	if !strings.Contains(typedRepairModel.queuedMessage, "Repair") {
		t.Fatalf("unexpected repair request: %q", typedRepairModel.queuedMessage)
	}

	querySession.Candidates[0].Validation = session.ValidationReport{Valid: true}
	model.busy = false
	previewModel, previewCmd := model.Update(tea.KeyMsg{Type: tea.KeyCtrlY})
	typedPreviewModel, ok := previewModel.(Model)
	if !ok {
		t.Fatalf("unexpected model type: %T", previewModel)
	}
	if !typedPreviewModel.busy || previewCmd == nil {
		t.Fatal("Ctrl+Y must start a bounded preview refresh for a valid candidate")
	}
	if typedPreviewModel.status != "refreshing preview" {
		t.Fatalf("unexpected preview status: %q", typedPreviewModel.status)
	}
}

func TestAgentCompletionPrefersOnlyProbeProducedByCurrentTurn(t *testing.T) {
	query := "SELECT * FROM MEASURE service_cpm IN sw_metrics TIME > '-30m' LIMIT 10"
	querySession := &session.QuerySession{ExecutionResult: session.ExecutionResult{Query: query, Summary: "full execution"}}
	querySession.AddCandidate(session.BydbqlCandidate{
		Query:      query,
		Validation: session.ValidationReport{Valid: true},
		Probe:      &session.ProbeSummary{Query: query, Rows: 1},
	})
	model := NewModel(Config{})
	model.querySession = querySession
	model.preferCandidateProbe = false
	model.turnEvents = []agent.Event{{Kind: agent.EventKindFinalResponse, Status: agent.EventStatusSucceeded}}

	completedModel, _ := model.Update(agentTurnUpdateMsg{update: workflow.TurnUpdate{Done: true, QuerySession: querySession}})
	typedCompletedModel, ok := completedModel.(Model)
	if !ok {
		t.Fatalf("unexpected model type: %T", completedModel)
	}
	if typedCompletedModel.preferCandidateProbe {
		t.Fatal("a conversational turn must not restore an older probe over the full execution")
	}

	model = NewModel(Config{})
	model.querySession = querySession
	model.turnEvents = []agent.Event{{
		Kind:     agent.EventKindToolResult,
		ToolName: bridge.ToolProbeBydbQL,
		Status:   agent.EventStatusSucceeded,
	}}
	completedModel, _ = model.Update(agentTurnUpdateMsg{update: workflow.TurnUpdate{Done: true, QuerySession: querySession}})
	typedCompletedModel, ok = completedModel.(Model)
	if !ok {
		t.Fatalf("unexpected model type: %T", completedModel)
	}
	if !typedCompletedModel.preferCandidateProbe {
		t.Fatal("a successful probe in the current turn must become the visible preview")
	}
}

func TestAgentTurnDisplaysLiveResponseWhenEnabled(t *testing.T) {
	model := NewModel(Config{})
	model.busy = true
	event := agent.Event{Kind: agent.EventKindMessageDelta, Message: "Inspecting the schema…"}
	updatedModel, _ := model.Update(agentTurnUpdateMsg{update: workflow.TurnUpdate{Event: &event}})
	typedModel, ok := updatedModel.(Model)
	if !ok {
		t.Fatalf("unexpected model type: %T", updatedModel)
	}
	view := typedModel.View()
	for _, expected := range []string{"live output:", "Inspecting the schema", "agent output streaming"} {
		if !strings.Contains(view, expected) {
			t.Fatalf("expected %q in live update view:\n%s", expected, view)
		}
	}
}

func TestViewOmitsManualValidationShortcut(t *testing.T) {
	model := NewModel(Config{})
	if strings.Contains(model.View(), "Ctrl+V") {
		t.Fatalf("manual validation shortcut must not appear in the TUI:\n%s", model.View())
	}
}

func TestNewModelFocusesConversationComposer(t *testing.T) {
	model := NewModel(Config{})
	if model.focus != focusMessage {
		t.Fatalf("expected conversation composer focus, got %d", model.focus)
	}
	if !model.message.Focused() {
		t.Fatal("expected the conversation composer to be focused")
	}
}

func TestCatalogConnectionErrorIsVisibleInWorkspace(t *testing.T) {
	model := NewModel(Config{Provider: "codex"})
	updatedModel, _ := model.Update(catalogMsg{loadErr: errors.New("failed to list groups: connection refused")})
	typedModel, ok := updatedModel.(Model)
	if !ok {
		t.Fatalf("unexpected model type: %T", updatedModel)
	}
	view := typedModel.View()
	for _, expected := range []string{"provider codex", "BanyanDB connection failed", "connection refused"} {
		if !strings.Contains(view, expected) {
			t.Fatalf("expected %q in workspace:\n%s", expected, view)
		}
	}
}

func TestWorkspaceUsesConversationFirstLayout(t *testing.T) {
	model := NewModel(Config{})
	model.resize(160, 42)
	view := model.View()
	for _, expected := range []string{"Conversation", "Message · Enter to send", "Policy: auto probe", "Time "} {
		if !strings.Contains(view, expected) {
			t.Fatalf("expected %q in workspace:\n%s", expected, view)
		}
	}
	for _, unexpected := range []string{"Autonomous discovery", "Execution policy"} {
		if strings.Contains(view, unexpected) {
			t.Fatalf("did not expect %q in workspace:\n%s", unexpected, view)
		}
	}
	if model.message.Height() < 3 {
		t.Fatalf("expected a compact message composer, got height %d", model.message.Height())
	}
	if model.message.Width() < 70 {
		t.Fatalf("expected the message composer to use the conversation width, got width %d", model.message.Width())
	}
}

func TestChatLinesKeepsToolCallsAndLongMessagesReadable(t *testing.T) {
	querySession := &session.QuerySession{ChatMessages: []session.ChatMessage{
		{Role: session.ChatRoleUser, Content: "show the p99 payment latency grouped by service for the last 30 minutes"},
		{Role: session.ChatRoleTool, ToolName: "describe_schema", Content: "payment_latency"},
	}}
	entries := chatEntries(querySession, false, "", "")
	if len(entries) != 2 {
		t.Fatalf("expected one entry per chat message, got %d: %#v", len(entries), entries)
	}
	if !strings.HasPrefix(entries[0].headline, "You › ") {
		t.Fatalf("expected user label, got %q", entries[0].headline)
	}
	if entries[1].headline != "  ↳ describe_schema: payment_latency" {
		t.Fatalf("unexpected tool line: %q", entries[1].headline)
	}
	if !strings.Contains(wrapText(entries[0].detail, 24), "\n") {
		t.Fatalf("expected a long message to wrap in detail: %q", entries[0].detail)
	}
}
