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

	tea "github.com/charmbracelet/bubbletea"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/agent"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/applog"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
)

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
	_, handled := model.handleKey(tea.KeyMsg{Type: tea.KeyCtrlA})
	if !handled {
		t.Fatal("expected Ctrl+A to start an agent turn")
	}
	if model.message.Value() != "" {
		t.Fatalf("expected the composer to clear immediately, got %q", model.message.Value())
	}
	if model.currentGoal() != "show payment latency" {
		t.Fatalf("expected the first message to remain available for session setup, got %q", model.currentGoal())
	}
	if !strings.Contains(model.View(), "You › show payment latency") {
		t.Fatalf("expected the queued user message in conversation:\n%s", model.View())
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

func TestCatalogConnectionErrorIsVisibleOnQueryTab(t *testing.T) {
	model := NewModel(Config{Provider: "acp"})
	updatedModel, _ := model.Update(catalogMsg{loadErr: errors.New("failed to list groups: connection refused")})
	typedModel, ok := updatedModel.(Model)
	if !ok {
		t.Fatalf("unexpected model type: %T", updatedModel)
	}
	if typedModel.activeTab != tabQuery {
		t.Fatalf("expected Query tab to remain active, got %d", typedModel.activeTab)
	}
	view := typedModel.View()
	for _, expected := range []string{"provider acp", "BanyanDB connection failed", "connection refused"} {
		if !strings.Contains(view, expected) {
			t.Fatalf("expected %q in Query tab:\n%s", expected, view)
		}
	}
}

func TestQueryTabUsesConversationFirstLayout(t *testing.T) {
	model := NewModel(Config{})
	model.resize(160, 42)
	view := model.View()
	for _, expected := range []string{"Conversation", "Message · Ctrl+A to send", "Policy: ask every time", "Time: start"} {
		if !strings.Contains(view, expected) {
			t.Fatalf("expected %q in Query tab:\n%s", expected, view)
		}
	}
	for _, unexpected := range []string{"Autonomous discovery", "Execution policy"} {
		if strings.Contains(view, unexpected) {
			t.Fatalf("did not expect %q in Query tab:\n%s", unexpected, view)
		}
	}
	if model.message.Height() < 3 {
		t.Fatalf("expected a compact message composer, got height %d", model.message.Height())
	}
	if model.message.Width() < 80 {
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
