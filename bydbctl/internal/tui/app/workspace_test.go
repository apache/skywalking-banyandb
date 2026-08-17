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
	"fmt"
	"slices"
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/agent"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/bridge"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/workflow"
)

func TestWorkspaceShowsConversationCandidateAndPreviewWithoutTabs(t *testing.T) {
	model := NewModel(Config{})
	model.resize(160, 42)
	querySession := &session.QuerySession{
		ExecutionResult: session.ExecutionResult{
			Query:   "SELECT error_rate FROM MEASURE service_cpm IN sw_metrics",
			Summary: "execution complete",
			Rows:    1240,
			Columns: []string{"time", "service", "error_rate"},
			Preview: [][]string{{"12:01", "checkout", "0.02"}},
		},
	}
	querySession.AddCandidate(session.BydbqlCandidate{
		Query: "SELECT error_rate FROM MEASURE service_cpm IN sw_metrics",
		Validation: session.ValidationReport{
			Valid: true,
		},
	})
	model.querySession = querySession
	model.syncQuerySession()

	view := model.View()
	for _, expected := range []string{"Conversation", "Candidate QL", "Data Preview", "1/1,240 rows"} {
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

func TestPreviewAndExportUseTheLatestExecutionResult(t *testing.T) {
	query := testMeasureQuery
	model := NewModel(Config{})
	querySession := &session.QuerySession{
		ResourceName: "service_cpm",
		ExecutionResult: session.ExecutionResult{
			Query:   query,
			Summary: "execution complete",
			Columns: []string{"value"},
			Preview: [][]string{{"executed"}},
			Rows:    1,
		},
	}
	querySession.AddCandidate(session.BydbqlCandidate{
		Query:      query,
		Validation: session.ValidationReport{Valid: true},
	})
	model.querySession = querySession

	preview, ok := model.currentPreviewData()
	if !ok || len(preview.preview) != 1 || preview.preview[0][0] != "executed" {
		t.Fatalf("expected the execution result, got %+v", preview)
	}
	exportResult, ok := model.exportResult()
	if !ok || len(exportResult.Preview) != 1 || exportResult.Preview[0][0] != "executed" {
		t.Fatalf("expected export of the execution result, got %+v", exportResult)
	}
}

func TestPreviewIsEmptyBeforeAnyExecution(t *testing.T) {
	model := NewModel(Config{})
	querySession := &session.QuerySession{ResourceName: "service_cpm"}
	querySession.AddCandidate(session.BydbqlCandidate{
		Query:      testMeasureQuery,
		Validation: session.ValidationReport{Valid: true},
	})
	model.querySession = querySession
	if _, ok := model.currentPreviewData(); ok {
		t.Fatal("a compiled candidate must not surface preview rows before execution")
	}
	if _, ok := model.exportResult(); ok {
		t.Fatal("a compiled candidate must not be exportable before execution")
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
	for _, expected := range []string{"Ctrl+G let Agent fix", "Steps", "catalog", "describe schema"} {
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
	querySession := &session.QuerySession{
		ExecutionResult: session.ExecutionResult{
			Query:   "SELECT * FROM TRACE segment IN sw_trace TIME > '-30m' LIMIT 10",
			Summary: "execution complete",
			Rows:    1,
			Columns: []string{"spans", "traceId"},
			Preview: [][]string{{"spans-begin-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-right-edge", "trace-1"}},
		},
	}
	querySession.AddCandidate(session.BydbqlCandidate{
		Query: "SELECT * FROM TRACE segment IN sw_trace TIME > '-30m' LIMIT 10",
		Validation: session.ValidationReport{
			Valid: true,
		},
	})
	model.querySession = querySession
	model.syncQuerySession()
	model.focus = focusExecution

	if initialTable := model.visiblePreviewTable(); strings.Contains(initialTable, "right-edge") {
		t.Fatalf("expected the right edge to be outside the initial table viewport:\n%s", initialTable)
	}

	updatedModel := tea.Model(model)
	for range 20 {
		updatedModel, _ = updatedModel.Update(tea.KeyMsg{Type: tea.KeyRight})
	}
	scrolledModel := updatedModel.(Model)
	scrolledTable := scrolledModel.visiblePreviewTable()
	if !strings.Contains(scrolledTable, "right-edge") {
		t.Fatalf("expected horizontal scrolling to reveal the right edge:\n%s", scrolledTable)
	}
	if !strings.Contains(scrolledModel.View(), "│ > ") {
		t.Fatalf("expected the selected-row marker to remain visible while scrolling:\n%s", scrolledModel.View())
	}
}

func TestDataPreviewDownArrowScrollsSelectedRowIntoView(t *testing.T) {
	model := NewModel(Config{})
	model.resize(120, 18)
	preview := make([][]string, 30)
	for rowIndex := range preview {
		preview[rowIndex] = []string{fmt.Sprintf("row-%d", rowIndex)}
	}
	model.querySession = &session.QuerySession{ExecutionResult: session.ExecutionResult{
		Query:   testMeasureQuery,
		Summary: "execution complete",
		Rows:    len(preview),
		Columns: []string{"value"},
		Preview: preview,
	}}
	model.focus = focusExecution
	model.executionRowCursor = -1

	updatedModel := tea.Model(model)
	for range 20 {
		updatedModel, _ = updatedModel.Update(tea.KeyMsg{Type: tea.KeyDown})
	}
	if view := updatedModel.(Model).View(); !strings.Contains(view, "> row-20") {
		t.Fatalf("Down must scroll the selected row into view:\n%s", view)
	}
}

func TestDataPreviewKeepsSelectedRowDetailVisibleWhenTableOverflows(t *testing.T) {
	model := NewModel(Config{})
	model.resize(120, 24)
	preview := make([][]string, 30)
	for rowIndex := range preview {
		preview[rowIndex] = []string{fmt.Sprintf("row-%d", rowIndex), fmt.Sprintf("detail-%d", rowIndex)}
	}
	model.querySession = &session.QuerySession{ExecutionResult: session.ExecutionResult{
		Query:   testMeasureQuery,
		Summary: "execution complete",
		Rows:    len(preview),
		Columns: []string{"value", "extra"},
		Preview: preview,
	}}
	model.focus = focusExecution
	model.executionRowCursor = -1

	updatedModel := tea.Model(model)
	for range 20 {
		updatedModel, _ = updatedModel.Update(tea.KeyMsg{Type: tea.KeyDown})
	}
	view := updatedModel.(Model).View()
	for _, expected := range []string{"> row-20", "Row detail", "extra: detail-20"} {
		if !strings.Contains(view, expected) {
			t.Fatalf("overflowing previews must keep %q visible for the selected row:\n%s", expected, view)
		}
	}
}

func TestDataPreviewPageKeysOnlyScrollSelectedRowDetail(t *testing.T) {
	model := NewModel(Config{})
	model.resize(120, 60)
	columns := make([]string, 35)
	previewRow := make([]string, len(columns))
	for columnIndex := range columns {
		columns[columnIndex] = fmt.Sprintf("field_%02d", columnIndex)
		previewRow[columnIndex] = fmt.Sprintf("value-%02d", columnIndex)
	}
	model.querySession = &session.QuerySession{ExecutionResult: session.ExecutionResult{
		Query:   testMeasureQuery,
		Summary: "execution complete",
		Rows:    1,
		Columns: columns,
		Preview: [][]string{previewRow},
	}}
	model.focus = focusExecution
	model.executionRowCursor = 0

	updatedModel := tea.Model(model)
	for range 6 {
		updatedModel, _ = updatedModel.Update(tea.KeyMsg{Type: tea.KeyPgDown})
	}
	scrolledModel := updatedModel.(Model)
	if scrolledModel.executionDetailScroll != 0 {
		t.Fatalf("pgdn must not scroll past the selected row detail, got offset %d", scrolledModel.executionDetailScroll)
	}
	if view := scrolledModel.View(); !strings.Contains(view, "field_00: value-00") {
		t.Fatalf("the selected row detail must remain at its first line when it fits:\n%s", view)
	}
}

func TestDataPreviewPageKeysReachLastDetailLineWithCatalogError(t *testing.T) {
	model := NewModel(Config{})
	model.resize(120, 42)
	columns := make([]string, 50)
	previewRow := make([]string, len(columns))
	for columnIndex := range columns {
		columns[columnIndex] = fmt.Sprintf("field_%02d", columnIndex)
		previewRow[columnIndex] = fmt.Sprintf("value-%02d", columnIndex)
	}
	model.querySession = &session.QuerySession{ExecutionResult: session.ExecutionResult{
		Query:   testMeasureQuery,
		Summary: "execution complete",
		Rows:    1,
		Columns: columns,
		Preview: [][]string{previewRow},
	}}
	model.catalog.loadError = "BanyanDB is unavailable"
	model.focus = focusExecution
	model.executionRowCursor = 0

	updatedModel := tea.Model(model)
	for range 20 {
		updatedModel, _ = updatedModel.Update(tea.KeyMsg{Type: tea.KeyPgDown})
	}
	if view := updatedModel.(Model).View(); !strings.Contains(view, "field_49: value-49") {
		t.Fatalf("pgdn must reach the final selected-row detail line with a catalog error:\n%s", view)
	}
}

// visiblePreviewTable returns just the horizontally scrolled table rows of the preview panel.
func (m Model) visiblePreviewTable() string {
	visibleLines := previewTableViewport(m.dataPreviewTableLines(), m.dataPreviewViewportWidth(), m.executionPreviewOffset)
	return strings.Join(visibleLines, "\n")
}

func TestFocusingThePreviewOverridesAnOpenSchemaSearch(t *testing.T) {
	model := NewModel(Config{})
	model.resize(160, 42)
	model.catalog.setCatalog(session.SchemaCatalog{Entries: []session.CatalogEntry{
		{Group: "sw_trace", Type: session.ResourceTypeTrace, Name: "segment"},
	}})
	model.message.SetValue("@segment")
	model.updateSchemaSearch()
	querySession := &session.QuerySession{
		ExecutionResult: session.ExecutionResult{
			Query:   "SELECT * FROM TRACE segment IN sw_trace TIME > '-30m' LIMIT 10",
			Summary: "execution complete",
			Rows:    1,
			Columns: []string{"traceId"},
			Preview: [][]string{{"trace-1"}},
		},
	}
	querySession.AddCandidate(session.BydbqlCandidate{
		Query: "SELECT * FROM TRACE segment IN sw_trace TIME > '-30m' LIMIT 10",
		Validation: session.ValidationReport{
			Valid: true,
		},
	})
	model.querySession = querySession
	model.syncQuerySession()
	model.focus = focusExecution

	if view := model.View(); !strings.Contains(view, "Data Preview") || !strings.Contains(view, "Focus: data preview") {
		t.Fatalf("expected the focused Data Preview to win over the schema search:\n%s", view)
	}
}

func TestWorkspaceFitsTerminalWithProviderVisible(t *testing.T) {
	model := NewModel(Config{Provider: "claude"})
	model.resize(160, testTerminalHeight)
	// The evidence column only appears once a turn has produced schema or result rows.
	model.querySession = &session.QuerySession{ExecutionResult: session.ExecutionResult{
		Query:   testMeasureQuery,
		Columns: []string{"service_id"},
		Preview: [][]string{{"payment"}},
		Rows:    1,
	}}

	assertWorkspaceFitsTerminal(
		t,
		model.View(),
		"provider claude",
		"Conversation",
		"Candidate QL",
		"Data Preview",
		"Message",
		"Status: ready",
		"Esc quit",
	)
}

func TestFooterWrapsBetweenShortcutLabels(t *testing.T) {
	model := NewModel(Config{})
	model.resize(160, 42)
	view := model.View()
	for _, shortcut := range []string{"Tab focus", "Alt+1-4 panel", "? help", "Esc quit"} {
		if !strings.Contains(view, shortcut) {
			t.Fatalf("expected complete shortcut label %q in workspace:\n%s", shortcut, view)
		}
	}
}

func TestWorkspaceFitsTerminalWithSelectedChatDetail(t *testing.T) {
	model := NewModel(Config{Provider: "claude"})
	model.resize(180, testTerminalHeight)
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

	// The visible window must contain the cursor, which sits on the last message.
	assertWorkspaceFitsTerminal(t, model.View(), "provider claude", "Detail · pgup/pgdn scroll", "20/20 messages")
}

func TestPanelRegionsStayInsideTheRenderedViewWhenCompressed(t *testing.T) {
	for _, size := range []struct {
		width  int
		height int
	}{
		{width: 160, height: 42},
		{width: 120, height: 24},
		{width: 90, height: 20},
	} {
		model := NewModel(Config{Provider: "claude"})
		model.resize(size.width, size.height)
		model.refreshPanelRegions()
		view, regions := model.renderView()
		viewHeight := lipgloss.Height(view)
		if len(regions) == 0 {
			t.Fatalf("%dx%d produced no clickable regions", size.width, size.height)
		}
		seen := make(map[int]bool, len(regions))
		for _, region := range regions {
			if region.top < 0 || region.bottom < region.top {
				t.Fatalf("%dx%d produced an inverted region: %+v", size.width, size.height, region)
			}
			if region.bottom >= viewHeight {
				t.Fatalf("%dx%d region %+v falls outside the %d-row view:\n%s",
					size.width, size.height, region, viewHeight, view)
			}
			seen[region.focus] = true
		}
		for _, requiredFocus := range []int{focusChat, focusQuery, focusMessage} {
			if !seen[requiredFocus] {
				t.Fatalf("%dx%d left focus %d unclickable: %+v", size.width, size.height, requiredFocus, regions)
			}
		}
	}
}

func TestPanelRegionsDoNotOverlap(t *testing.T) {
	model := NewModel(Config{Provider: "claude"})
	model.resize(160, 42)
	model.refreshPanelRegions()
	for firstIndex, first := range model.panelRegions {
		for _, second := range model.panelRegions[firstIndex+1:] {
			rowsOverlap := first.top <= second.bottom && second.top <= first.bottom
			columnsOverlap := first.left <= second.right && second.left <= first.right
			if rowsOverlap && columnsOverlap {
				t.Fatalf("regions overlap so a click is ambiguous: %+v and %+v", first, second)
			}
		}
	}
}

func TestWorkspaceFitsTerminalWhileRunning(t *testing.T) {
	model := NewModel(Config{Provider: "claude"})
	model.resize(180, testTerminalHeight)
	model.busy = true
	model.status = "executing full query"

	assertWorkspaceFitsTerminal(
		t,
		model.View(),
		"provider claude",
		"Stop",
		"executing full query",
		"Esc",
	)
}

func TestWorkspaceFitsTerminalWithQuitConfirmation(t *testing.T) {
	model := NewModel(Config{Provider: "claude"})
	model.resize(180, testTerminalHeight)
	model.quitConfirmPending = true

	assertWorkspaceFitsTerminal(
		t,
		model.View(),
		"provider claude",
		"Quit bydbctl agent?",
	)
}

func TestWorkspaceFitsTerminalWithSchemaSearchOpen(t *testing.T) {
	model := NewModel(Config{Provider: "claude"})
	model.resize(160, testTerminalHeight)
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
		"provider claude",
		"Conversation",
		"Candidate QL",
		"@ search · local catalog",
		"Message",
		"Status: ready",
		"Esc quit",
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
	for _, expected := range []string{"match_05", "match_06", "match_07", "5-7/8"} {
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

// testTerminalHeight is the terminal height every workspace-fit assertion renders against.
const testTerminalHeight = 42

func assertWorkspaceFitsTerminal(t *testing.T, view string, expectedValues ...string) {
	t.Helper()
	if viewHeight := lipgloss.Height(view); viewHeight > testTerminalHeight {
		t.Fatalf("workspace height %d exceeds terminal height %d:\n%s", viewHeight, testTerminalHeight, view)
	}
	for _, expectedValue := range expectedValues {
		if !strings.Contains(view, expectedValue) {
			t.Fatalf("expected %q in visible workspace:\n%s", expectedValue, view)
		}
	}
}

func TestNarrowWorkspaceDrillsIntoTheEvidencePanelInsteadOfStackingIt(t *testing.T) {
	model := NewModel(Config{})
	model.resize(80, 30)
	model.querySession = &session.QuerySession{ExecutionResult: session.ExecutionResult{
		Rows:    1,
		Columns: []string{"service"},
		Preview: [][]string{{"checkout"}},
		Summary: "query complete",
	}}

	conversationView := model.View()
	if !strings.Contains(conversationView, "Conversation") {
		t.Fatalf("expected the conversation to own a narrow terminal by default:\n%s", conversationView)
	}
	if strings.Contains(conversationView, "Data Preview") {
		t.Fatalf("a narrow terminal must not stack both columns into one screen:\n%s", conversationView)
	}
	if !strings.Contains(conversationView, "open results") {
		t.Fatalf("expected a hint pointing at the off-screen results:\n%s", conversationView)
	}

	model.focus = focusExecution
	previewView := model.View()
	if !strings.Contains(previewView, "Data Preview") {
		t.Fatalf("focusing the preview must show it full screen:\n%s", previewView)
	}
	if !strings.Contains(previewView, "back to the conversation") {
		t.Fatalf("expected a way back from the full-screen preview:\n%s", previewView)
	}
}

func TestNarrowWorkspaceFitsTheTerminalInBothDrillDownStates(t *testing.T) {
	for _, size := range []struct{ width, height int }{{80, 30}, {70, 24}, {99, 20}, {60, 18}} {
		for _, focusTarget := range []int{focusMessage, focusExecution} {
			model := NewModel(Config{})
			model.resize(size.width, size.height)
			model.querySession = &session.QuerySession{ExecutionResult: session.ExecutionResult{
				Rows:    2,
				Columns: []string{"timestamp", "service"},
				Preview: [][]string{{"2026-08-16T12:00:00Z", "checkout"}, {"2026-08-16T12:01:00Z", "cart"}},
				Summary: "query complete",
			}}
			model.focus = focusTarget
			view := model.View()
			if viewHeight := lipgloss.Height(view); viewHeight > size.height {
				t.Fatalf("%dx%d focus %d rendered %d rows:\n%s", size.width, size.height, focusTarget, viewHeight, view)
			}
			for _, line := range strings.Split(view, "\n") {
				if lineWidth := lipgloss.Width(line); lineWidth > size.width {
					t.Fatalf("%dx%d focus %d rendered a %d-column line:\n%s", size.width, size.height, focusTarget, lineWidth, view)
				}
			}
		}
	}
}

func TestWorkspaceFitsEveryTerminalSizeAndState(t *testing.T) {
	for _, size := range []struct{ width, height int }{{200, 50}, {160, 44}, {120, 36}, {100, 30}, {96, 30}, {80, 24}, {60, 18}} {
		for _, state := range []string{"empty", "loaded", "busy", "help"} {
			model := NewModel(Config{Provider: "claude"})
			model.resize(size.width, size.height)
			if state != "empty" {
				querySession := &session.QuerySession{ResourceName: "service_cpm", ExecutionResult: session.ExecutionResult{
					Rows:    9,
					Columns: []string{"timestamp", "service", "error_rate"},
					Preview: [][]string{{"2026-08-16T12:00:00Z", "checkout", "0.02"}},
					Summary: "query complete",
				}}
				querySession.AddCandidate(session.BydbqlCandidate{
					Query:      "SELECT error_rate FROM MEASURE service_cpm IN sw_metrics TIME > '-30m' LIMIT 10",
					Validation: session.ValidationReport{Valid: true},
				})
				for messageIndex := 0; messageIndex < 12; messageIndex++ {
					querySession.AddChatMessage(session.ChatMessage{Role: session.ChatRoleAssistant, Content: "candidate discussion"})
				}
				model.querySession = querySession
				model.syncQuerySession()
			}
			switch state {
			case "busy":
				model.busy = true
				model.status = "asking agent"
			case "help":
				model.helpVisible = true
			}
			view := model.View()
			if viewHeight := lipgloss.Height(view); viewHeight > size.height {
				t.Fatalf("%dx%d %s rendered %d rows:\n%s", size.width, size.height, state, viewHeight, view)
			}
			for _, line := range strings.Split(view, "\n") {
				if lineWidth := lipgloss.Width(line); lineWidth > size.width {
					t.Fatalf("%dx%d %s rendered a %d-column line:\n%s", size.width, size.height, state, lineWidth, view)
				}
			}
		}
	}
}

func TestTerminalBelowTheMinimumSizeExplainsItselfInsteadOfBreaking(t *testing.T) {
	model := NewModel(Config{})
	model.resize(48, 12)
	view := model.View()
	if !strings.Contains(view, "Terminal too small") {
		t.Fatalf("expected a minimum-size message:\n%s", view)
	}
	if !strings.Contains(view, "Need 60×18") {
		t.Fatalf("expected the required size in the message:\n%s", view)
	}
	if viewHeight := lipgloss.Height(view); viewHeight > 12 {
		t.Fatalf("the minimum-size screen rendered %d rows:\n%s", viewHeight, view)
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
