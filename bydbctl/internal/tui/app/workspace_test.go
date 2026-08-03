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
	"slices"
	"strings"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/agent"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/approval"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/bridge"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/workflow"
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

func TestRefreshedProbeTakesPrecedenceOverEarlierFullExecution(t *testing.T) {
	query := "SELECT * FROM MEASURE service_cpm IN sw_metrics TIME > '-30m' LIMIT 10"
	model := NewModel(Config{})
	querySession := &session.QuerySession{
		ResourceName: "service_cpm",
		ExecutionResult: session.ExecutionResult{
			Query:   query,
			Summary: "earlier full execution",
			Columns: []string{"value"},
			Preview: [][]string{{"old"}},
			Rows:    1,
		},
	}
	querySession.AddCandidate(session.BydbqlCandidate{
		Query:      query,
		Validation: session.ValidationReport{Valid: true},
		Probe: &session.ProbeSummary{
			Query:   query,
			Columns: []string{"value"},
			Preview: [][]string{{"refreshed"}},
			Rows:    1,
		},
	})
	model.querySession = querySession
	model.preferCandidateProbe = true

	preview, ok := model.currentPreviewData()
	if !ok || len(preview.preview) != 1 || preview.preview[0][0] != "refreshed" {
		t.Fatalf("expected refreshed probe, got %+v", preview)
	}
	exportResult, ok := model.exportResult()
	if !ok || len(exportResult.Preview) != 1 || exportResult.Preview[0][0] != "refreshed" {
		t.Fatalf("expected export of visible refreshed probe, got %+v", exportResult)
	}
}

func TestCompletedDescribeSchemaShowsSchemaEvidence(t *testing.T) {
	model := NewModel(Config{})
	model.resize(160, 42)
	querySession := &session.QuerySession{SchemaSnapshot: session.SchemaSnapshot{
		Loaded: true,
		Type:   session.ResourceTypeMeasure,
		Name:   "service_cpm",
		Groups: []string{"sw_metrics"},
		Columns: []session.SchemaColumn{
			{Name: "service", Kind: session.SchemaColumnTag, Type: session.SchemaValueTypeString},
			{Name: "value", Kind: session.SchemaColumnField, Type: session.SchemaValueTypeFloat},
		},
	}}
	model.busy = true
	model.focus = focusExecution
	event := agent.Event{
		Kind:     agent.EventKindToolResult,
		ToolName: bridge.ToolDescribeSchema,
		Status:   agent.EventStatusSucceeded,
	}
	updatedModel, _ := model.Update(agentTurnUpdateMsg{update: workflow.TurnUpdate{
		Event:        &event,
		QuerySession: querySession,
	}})
	typedModel, ok := updatedModel.(Model)
	if !ok {
		t.Fatalf("unexpected model type: %T", updatedModel)
	}
	if typedModel.focus == focusExecution {
		t.Fatal("describe_schema must release stale Data Preview focus")
	}
	if interimView := typedModel.View(); !strings.Contains(interimView, "MEASURE service_cpm") ||
		strings.Contains(interimView, "Data Preview") {
		t.Fatalf("describe_schema must switch evidence before turn completion:\n%s", interimView)
	}
	completedModel, _ := typedModel.Update(agentTurnUpdateMsg{update: workflow.TurnUpdate{
		Done:         true,
		QuerySession: querySession,
	}})
	model, ok = completedModel.(Model)
	if !ok {
		t.Fatalf("unexpected model type: %T", completedModel)
	}

	view := model.View()
	for _, expected := range []string{"Schema", "MEASURE service_cpm", "Typed columns", "service"} {
		if !strings.Contains(view, expected) {
			t.Fatalf("expected %q in schema evidence:\n%s", expected, view)
		}
	}
	if strings.Contains(view, "Data Preview") {
		t.Fatalf("describe_schema must select schema evidence, got:\n%s", view)
	}
}

func TestWorkspaceExplainsCandidateActionsAndProgress(t *testing.T) {
	model := NewModel(Config{})
	model.resize(160, 42)
	querySession := &session.QuerySession{}
	querySession.AddCandidate(session.BydbqlCandidate{
		Query: "SELECT FROM",
		Validation: session.ValidationReport{
			Message: "syntax error near FROM",
		},
	})
	model.querySession = querySession
	model.syncQuerySession()
	model.busy = true
	model.turnEvents = []agent.Event{
		{Kind: agent.EventKindToolResult, ToolName: bridge.ToolListGroupsSchemas, Status: agent.EventStatusSucceeded},
		{Kind: agent.EventKindToolCall, ToolName: bridge.ToolDescribeSchema, Status: agent.EventStatusRunning},
	}

	view := model.View()
	for _, expected := range []string{"Ctrl+G let Agent fix", "Ctrl+Y refresh preview", "Ctrl+E full execute", "Steps", "catalog", "describe schema"} {
		if !strings.Contains(view, expected) {
			t.Fatalf("expected %q in workspace:\n%s", expected, view)
		}
	}
}

func TestCandidateProgressRemainsPartOfPlanCompilation(t *testing.T) {
	stageIndex, ok := progressStageForEvent(agent.Event{
		Kind:     agent.EventKindCandidate,
		ToolName: bridge.ToolProposeQueryPlan,
		Status:   agent.EventStatusSucceeded,
	})
	if !ok || stageIndex != 2 {
		t.Fatalf("candidate event must remain in the compile-plan stage, got stage=%d found=%t", stageIndex, ok)
	}
}

func TestWorkspaceShowsColdStartGuidanceAfterCatalogLoads(t *testing.T) {
	model := NewModel(Config{})
	model.resize(160, 42)
	model.catalog.setCatalog(session.SchemaCatalog{
		Groups: []string{"sw_metrics", "sw_trace"},
		Entries: []session.CatalogEntry{{
			Group: "sw_metrics",
			Type:  session.ResourceTypeMeasure,
			Name:  "service_cpm",
		}},
	})

	view := model.View()
	for _, expected := range []string{
		"Welcome to text2bydbQL",
		"Available groups: sw_metrics, sw_trace",
		"@sw_metrics/service_cpm",
		"What fields does",
		"Show the latest 10 rows",
	} {
		if !strings.Contains(view, expected) {
			t.Fatalf("expected %q in cold-start guidance:\n%s", expected, view)
		}
	}
}

func TestDataPreviewScrollsHorizontallyWhenFocused(t *testing.T) {
	model := NewModel(Config{})
	model.resize(120, 42)
	querySession := &session.QuerySession{}
	querySession.AddCandidate(session.BydbqlCandidate{
		Query: "SELECT * FROM TRACE segment IN sw_trace TIME > '-30m' LIMIT 10",
		Validation: session.ValidationReport{
			Valid: true,
		},
		Probe: &session.ProbeSummary{
			Rows:    1,
			Columns: []string{"spans", "traceId"},
			Preview: [][]string{{"spans-begin-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-right-edge", "trace-1"}},
		},
	})
	model.querySession = querySession
	model.syncQuerySession()
	model.focus = focusExecution

	if initialView := model.View(); strings.Contains(initialView, "right-edge") {
		t.Fatalf("expected the right edge to be outside the initial preview viewport:\n%s", initialView)
	}

	updatedModel := tea.Model(model)
	for range 20 {
		updatedModel, _ = updatedModel.Update(tea.KeyMsg{Type: tea.KeyRight})
	}
	scrolledModel := updatedModel.(Model)
	scrolledView := scrolledModel.View()
	if !strings.Contains(scrolledView, "right-edge") {
		t.Fatalf("expected horizontal scrolling to reveal the right edge:\n%s", scrolledView)
	}
	if !strings.Contains(scrolledView, "│ > ") {
		t.Fatalf("expected the selected-row marker to remain visible while scrolling:\n%s", scrolledView)
	}
}

func TestCtrlFShowsDataPreviewWhenSchemaSearchIsOpen(t *testing.T) {
	model := NewModel(Config{})
	model.catalog.setCatalog(session.SchemaCatalog{Entries: []session.CatalogEntry{
		{Group: "sw_trace", Type: session.ResourceTypeTrace, Name: "segment"},
	}})
	model.message.SetValue("@segment")
	model.updateSchemaSearch()
	querySession := &session.QuerySession{}
	querySession.AddCandidate(session.BydbqlCandidate{
		Query: "SELECT * FROM TRACE segment IN sw_trace TIME > '-30m' LIMIT 10",
		Validation: session.ValidationReport{
			Valid: true,
		},
		Probe: &session.ProbeSummary{
			Rows:    1,
			Columns: []string{"traceId"},
			Preview: [][]string{{"trace-1"}},
		},
	})
	model.querySession = querySession
	model.syncQuerySession()

	updatedModel, _ := model.Update(tea.KeyMsg{Type: tea.KeyCtrlF})
	typedModel := updatedModel.(Model)
	if !strings.Contains(typedModel.View(), "Data Preview · focused") {
		t.Fatalf("expected Ctrl+F to show the focused Data Preview:\n%s", typedModel.View())
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

func TestSchemaSearchKeepsAllMatchesAndScrollsVisibleResults(t *testing.T) {
	const group = "sw_metrics"
	model := NewModel(Config{})
	model.catalog.setCatalog(session.SchemaCatalog{Entries: []session.CatalogEntry{
		{Group: group, Type: session.ResourceTypeMeasure, Name: "match_01"},
		{Group: group, Type: session.ResourceTypeMeasure, Name: "match_02"},
		{Group: group, Type: session.ResourceTypeMeasure, Name: "match_03"},
		{Group: group, Type: session.ResourceTypeMeasure, Name: "match_04"},
		{Group: group, Type: session.ResourceTypeMeasure, Name: "match_05"},
		{Group: group, Type: session.ResourceTypeMeasure, Name: "match_06"},
		{Group: group, Type: session.ResourceTypeMeasure, Name: "match_07"},
		{Group: group, Type: session.ResourceTypeMeasure, Name: "match_08"},
	}})
	model.message.SetValue("@match")
	model.updateSchemaSearch()

	if got := len(model.schemaSearchEntries()); got != 8 {
		t.Fatalf("expected all eight matching schemas, got %d", got)
	}
	for index := 0; index < 6; index++ {
		updatedModel, _ := model.Update(tea.KeyMsg{Type: tea.KeyDown})
		var ok bool
		model, ok = updatedModel.(Model)
		if !ok {
			t.Fatalf("unexpected model type: %T", updatedModel)
		}
	}

	view := model.renderSchemaSearch(100, 3)
	for _, expected := range []string{"match_05", "match_06", "match_07", "results 5-7/8"} {
		if !strings.Contains(view, expected) {
			t.Fatalf("expected %q in scrolled schema search:\n%s", expected, view)
		}
	}
	if strings.Contains(view, "match_01") {
		t.Fatalf("did not expect the first result in the scrolled schema search:\n%s", view)
	}
}

func TestSchemaSearchPrioritizesResourceNamesAndRejectsCrossWordSubsequences(t *testing.T) {
	model := NewModel(Config{})
	model.catalog.setCatalog(session.SchemaCatalog{Entries: []session.CatalogEntry{
		{Group: "sw_metadata", Type: session.ResourceTypeMeasure, Name: "ebpf_profiling_schedule_minute"},
		{Group: "sw_metricsDay", Type: session.ResourceTypeMeasure, Name: "instance_jvm_memory_pool_codeheap_profiled_nmethods_day"},
		{Group: "sw_metricsDay", Type: session.ResourceTypeMeasure, Name: "meter_rocketmq_topic_producer_offset_day"},
		{Group: "pprof", Type: session.ResourceTypeMeasure, Name: "cpu"},
		{Group: "sw_profile", Type: session.ResourceTypeMeasure, Name: "pprof_cpu"},
		{Group: "sw_profile", Type: session.ResourceTypeMeasure, Name: "pprof_heap"},
	}})
	model.message.SetValue("@pprof")
	model.updateSchemaSearch()

	entries := model.schemaSearchEntries()
	if len(entries) != 3 {
		t.Fatalf("expected three direct pprof matches, got %#v", entries)
	}
	gotNames := []string{entries[0].Name, entries[1].Name, entries[2].Name}
	wantNames := []string{"pprof_cpu", "pprof_heap", "cpu"}
	if !slices.Equal(gotNames, wantNames) {
		t.Fatalf("unexpected search order: got %v, want %v", gotNames, wantNames)
	}
}

func TestSchemaSearchMatchesExplicitGroupResourceReference(t *testing.T) {
	model := NewModel(Config{})
	model.catalog.setCatalog(session.SchemaCatalog{Entries: []session.CatalogEntry{
		{Group: "sw_metadata", Type: session.ResourceTypeMeasure, Name: "endpoint_traffic_minute"},
		{Group: "sw_metadata", Type: session.ResourceTypeMeasure, Name: "service_traffic_minute"},
		{Group: "sw_metrics", Type: session.ResourceTypeMeasure, Name: "endpoint_traffic_minute"},
	}})
	model.message.SetValue("@sw_metadata/endpoint")
	model.updateSchemaSearch()

	entries := model.schemaSearchEntries()
	if len(entries) != 1 {
		t.Fatalf("expected one explicit group/resource match, got %#v", entries)
	}
	if entries[0].Group != "sw_metadata" || entries[0].Name != "endpoint_traffic_minute" {
		t.Fatalf("unexpected explicit group/resource match: %#v", entries[0])
	}
}

func TestSchemaSearchMatchesMultiTokenResourcePrefix(t *testing.T) {
	model := NewModel(Config{})
	model.catalog.setCatalog(session.SchemaCatalog{Entries: []session.CatalogEntry{
		{Group: "sw_metadata", Type: session.ResourceTypeMeasure, Name: "endpoint_traffic_minute"},
	}})
	model.message.SetValue("@sw_metadata/endpoint_traffic")
	model.updateSchemaSearch()

	entries := model.schemaSearchEntries()
	if len(entries) != 1 || entries[0].Name != "endpoint_traffic_minute" {
		t.Fatalf("expected multi-token resource prefix to match, got %#v", entries)
	}
}

func TestSchemaSearchUsesBestTokenMatch(t *testing.T) {
	model := NewModel(Config{})
	model.catalog.setCatalog(session.SchemaCatalog{Entries: []session.CatalogEntry{
		{Group: "sw_metadata", Type: session.ResourceTypeMeasure, Name: "foobar"},
		{Group: "sw_metadata", Type: session.ResourceTypeMeasure, Name: "xfoo_foo"},
	}})
	model.message.SetValue("@foo")
	model.updateSchemaSearch()

	entries := model.schemaSearchEntries()
	if len(entries) != 2 || entries[0].Name != "xfoo_foo" {
		t.Fatalf("expected exact token match to rank first, got %#v", entries)
	}
}

func TestSchemaSearchViewportLimitUsesAvailableHeight(t *testing.T) {
	if got := schemaSearchViewportLimit(4, 100); got != 1 {
		t.Fatalf("expected one result in a compact workspace, got %d", got)
	}
	if got := schemaSearchViewportLimit(42, 100); got >= 100 {
		t.Fatalf("expected a bounded viewport for a large catalog, got %d", got)
	}
}

func TestSchemaSearchRendersLongSelectedEntryOnOneLine(t *testing.T) {
	const (
		searchWidth           = 80
		schemaSearchFixedRows = 3
	)
	model := NewModel(Config{})
	model.catalog.setCatalog(session.SchemaCatalog{Entries: []session.CatalogEntry{
		{
			Group: "sw_metricsMinute",
			Type:  session.ResourceTypeMeasure,
			Name:  "instance_jvm_memory_pool_codeheap_non_profiled_nmethods_minute",
		},
	}})
	model.message.SetValue("@prof")
	model.updateSchemaSearch()

	view := model.renderSchemaSearch(searchWidth, 1)
	wantHeight := panelStyle.GetVerticalFrameSize() + schemaSearchFixedRows
	if gotHeight := lipgloss.Height(view); gotHeight != wantHeight {
		t.Fatalf("expected one-line selected entry, got height %d want %d:\n%s", gotHeight, wantHeight, view)
	}
	panelWidth := lipgloss.Width(panelStyle.Width(searchWidth).Render(""))
	for _, line := range strings.Split(view, "\n") {
		if lineWidth := lipgloss.Width(line); lineWidth > panelWidth {
			t.Fatalf("rendered line width %d exceeds panel width %d:\n%s", lineWidth, panelWidth, view)
		}
	}
}

func TestTruncateSchemaSearchLabelFitsNarrowWidth(t *testing.T) {
	const maxWidth = 1
	truncatedLabel := truncateSchemaSearchLabel("long schema label", maxWidth)
	if gotWidth := lipgloss.Width(truncatedLabel); gotWidth > maxWidth {
		t.Fatalf("truncated label width %d exceeds maximum width %d", gotWidth, maxWidth)
	}
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
