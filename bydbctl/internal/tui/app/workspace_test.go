// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain a
// copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package app

import (
	"strings"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/approval"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
)

func TestWorkspaceShowsConversationCandidateAndPreviewWithoutTabs(t *testing.T) {
	model := NewModel(Config{})
	model.resize(160, 42)
	querySession := &session.QuerySession{}
	querySession.AddCandidate(session.BydbqlCandidate{
		Query: "SELECT error_rate FROM MEASURE service_cpm IN sw_metrics",
		Validation: session.ValidationReport{
			Valid: true,
		},
		Probe: &session.ProbeSummary{
			Rows:    1240,
			Columns: []string{"time", "service", "error_rate"},
			Preview: [][]string{{"12:01", "checkout", "0.02"}},
		},
	})
	model.querySession = querySession
	model.syncQuerySession()

	view := model.View()
	for _, expected := range []string{"Conversation", "Candidate QL v1", "Data Preview", "1/1,240 rows"} {
		if !strings.Contains(view, expected) {
			t.Fatalf("expected %q in workspace:\n%s", expected, view)
		}
	}
	for _, unexpected := range []string{"F1 Schema", "F2 Query", "F3 Run"} {
		if strings.Contains(view, unexpected) {
			t.Fatalf("did not expect tab label %q in workspace:\n%s", unexpected, view)
		}
	}
}

func TestWorkspaceFitsTerminalWithProviderVisible(t *testing.T) {
	const terminalHeight = 42
	model := NewModel(Config{Provider: "claude"})
	model.resize(160, terminalHeight)

	assertWorkspaceFitsTerminal(
		t,
		model.View(),
		terminalHeight,
		"provider claude",
		"Conversation",
		"Candidate QL",
		"Data Preview",
		"Message · Enter to send",
		"Status: ready",
		"Esc stop/quit",
	)
}

func TestFooterWrapsBetweenShortcutLabels(t *testing.T) {
	model := NewModel(Config{})
	model.resize(160, 42)
	view := model.View()
	for _, shortcut := range []string{"Tab focus", "Esc stop/quit"} {
		if !strings.Contains(view, shortcut) {
			t.Fatalf("expected complete shortcut label %q in workspace:\n%s", shortcut, view)
		}
	}
}

func TestWorkspaceFitsTerminalWithSelectedChatDetail(t *testing.T) {
	const terminalHeight = 42
	model := NewModel(Config{Provider: "claude"})
	model.resize(180, terminalHeight)
	querySession := &session.QuerySession{}
	for messageIndex := 0; messageIndex < 19; messageIndex++ {
		querySession.ChatMessages = append(querySession.ChatMessages, session.ChatMessage{
			Role:    session.ChatRoleTool,
			Content: "catalog lookup",
		})
	}
	querySession.ChatMessages = append(querySession.ChatMessages, session.ChatMessage{
		Role:    session.ChatRoleAssistant,
		Content: "查询结果",
		Detail: "我会先按精确资源读取 schema，再提交只读 typed plan。\n" +
			"受控目录中，没有发现：精确资源；请确认，资源名称或刷新 BanyanDB catalog 后重试。",
	})
	model.querySession = querySession
	model.chatCursor = len(querySession.ChatMessages) - 1

	assertWorkspaceFitsTerminal(t, model.View(), terminalHeight, "provider claude", "Detail · pgup/pgdn scroll", "4/20 messages")
}

func TestWorkspaceFitsTerminalWithExecutionApproval(t *testing.T) {
	const terminalHeight = 42
	model := NewModel(Config{Provider: "claude"})
	model.resize(180, terminalHeight)
	model.busy = true
	model.status = "execution approval required"
	model.pendingApproval = &approval.Request{
		Query:       "SELECT * FROM MEASURE endpoint_traffic_minute IN sw_metadata TIME > '-30m' LIMIT 10",
		Resource:    "MEASURE/endpoint_traffic_minute",
		Groups:      []string{"sw_metadata"},
		TimeRange:   "TIME > '-30m'",
		Limit:       "10",
		Timeout:     3 * time.Second,
		PreviewRows: 50,
		Source:      approval.SourceManual,
	}

	assertWorkspaceFitsTerminal(
		t,
		model.View(),
		terminalHeight,
		"provider claude",
		"Execution approval required",
		"execution waiting for approval",
		"y execute once · n reject · e copy to editor and revise",
		"Esc",
		"stop/quit",
	)
}

func TestWorkspaceFitsTerminalWithSchemaSearchOpen(t *testing.T) {
	const terminalHeight = 42
	model := NewModel(Config{Provider: "claude"})
	model.resize(160, terminalHeight)
	model.catalog.setCatalog(session.SchemaCatalog{Entries: []session.CatalogEntry{
		{Group: "sw_records", Type: session.ResourceTypeStream, Name: "event"},
		{Group: "sw_records", Type: session.ResourceTypeTrace, Name: "segment"},
		{Group: "sw_metrics", Type: session.ResourceTypeMeasure, Name: "service_cpm"},
		{Group: "sw_metrics", Type: session.ResourceTypeMeasure, Name: "service_resp_time"},
		{Group: "sw_top_n", Type: session.ResourceTypeTopN, Name: "endpoint_traffic"},
		{Group: "sw_profile", Type: session.ResourceTypeProperty, Name: "task"},
	}})
	model.message.SetValue("show @")
	model.updateSchemaSearch()

	view := model.View()
	assertWorkspaceFitsTerminal(
		t,
		view,
		terminalHeight,
		"provider claude",
		"Conversation",
		"Candidate QL",
		"@ search · local catalog",
		"Message · Enter to send",
		"Status: ready",
		"Esc stop/quit",
	)
}

func assertWorkspaceFitsTerminal(t *testing.T, view string, terminalHeight int, expectedValues ...string) {
	t.Helper()
	if viewHeight := lipgloss.Height(view); viewHeight > terminalHeight {
		t.Fatalf("workspace height %d exceeds terminal height %d:\n%s", viewHeight, terminalHeight, view)
	}
	for _, expectedValue := range expectedValues {
		if !strings.Contains(view, expectedValue) {
			t.Fatalf("expected %q in visible workspace:\n%s", expectedValue, view)
		}
	}
}

func TestWorkspaceStacksEvidenceBelowConversationOnNarrowTerminals(t *testing.T) {
	model := NewModel(Config{})
	model.resize(80, 42)
	model.querySession = &session.QuerySession{ExecutionResult: session.ExecutionResult{
		Rows:    1,
		Columns: []string{"service"},
		Preview: [][]string{{"checkout"}},
		Summary: "query complete",
	}}

	view := model.View()
	conversationAt := strings.Index(view, "Conversation")
	previewAt := strings.Index(view, "Data Preview")
	if conversationAt < 0 || previewAt < 0 {
		t.Fatalf("expected conversation and preview in narrow workspace:\n%s", view)
	}
	if conversationAt >= previewAt {
		t.Fatalf("expected the evidence panel after the conversation on narrow terminals:\n%s", view)
	}
}

func TestComposerEnterInsertsThenSendsSelectedResourceReference(t *testing.T) {
	model := NewModel(Config{})
	model.catalog.setCatalog(session.SchemaCatalog{Entries: []session.CatalogEntry{
		{Group: "sw_metrics", Type: session.ResourceTypeMeasure, Name: "service_cpm"},
		{Group: "sw_metrics", Type: session.ResourceTypeMeasure, Name: "service_latency"},
	}})
	model.message.SetValue("show @service_cpm")
	model.updateSchemaSearch()

	if !model.schemaSearchOpen() {
		t.Fatal("expected @ search to open")
	}
	if !strings.Contains(model.View(), "@ search · local catalog") {
		t.Fatalf("expected local search in workspace:\n%s", model.View())
	}
	updatedModel, _ := model.Update(tea.KeyMsg{Type: tea.KeyEnter})
	typedModel, ok := updatedModel.(Model)
	if !ok {
		t.Fatalf("unexpected model type: %T", updatedModel)
	}
	if typedModel.busy {
		t.Fatal("first Enter must only insert the selected resource")
	}
	if typedModel.message.Value() != "show @sw_metrics/service_cpm" {
		t.Fatalf("unexpected composer reference: %q", typedModel.message.Value())
	}
	updatedModel, _ = typedModel.Update(tea.KeyMsg{Type: tea.KeyEnter})
	typedModel, ok = updatedModel.(Model)
	if !ok {
		t.Fatalf("unexpected model type: %T", updatedModel)
	}
	if !typedModel.busy {
		t.Fatal("second Enter must start an agent turn")
	}
	if typedModel.message.Value() != "" {
		t.Fatalf("expected second Enter to clear the composer, got %q", typedModel.message.Value())
	}
	if !strings.Contains(typedModel.View(), "You › show @sw_metrics/service_cpm") {
		t.Fatalf("expected second Enter to send the selected resource reference:\n%s", typedModel.View())
	}
}

func TestInvalidCandidateDoesNotOfferManualAgentRepair(t *testing.T) {
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

	if strings.Contains(model.View(), "Ctrl+F") {
		t.Fatalf("manual repair shortcut must not appear for an invalid candidate:\n%s", model.View())
	}
}
