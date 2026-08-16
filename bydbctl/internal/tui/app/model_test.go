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
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/agent"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/applog"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/workflow"
)

// testMeasureQuery is the valid BYDBQL candidate shared by the workspace tests.
const testMeasureQuery = "SELECT * FROM MEASURE service_cpm IN sw_metrics TIME > '-30m' LIMIT 10"

func TestEscRequiresConfirmationBeforeQuitting(t *testing.T) {
	model := NewModel(Config{Provider: "claude"})
	promptedModel, quitCmd := model.Update(tea.KeyMsg{Type: tea.KeyEsc})
	typedPrompted, ok := promptedModel.(Model)
	if !ok {
		t.Fatalf("unexpected model type: %T", promptedModel)
	}
	if quitCmd != nil {
		t.Fatal("the first Esc must not quit immediately")
	}
	if !typedPrompted.quitConfirmPending {
		t.Fatal("expected a pending quit confirmation after Esc")
	}
	if !strings.Contains(typedPrompted.View(), "Quit bydbctl agent?") {
		t.Fatalf("expected a visible quit confirmation:\n%s", typedPrompted.View())
	}
	declinedModel, declinedCmd := typedPrompted.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'n'}})
	typedDeclined, ok := declinedModel.(Model)
	if !ok {
		t.Fatalf("unexpected declined model type: %T", declinedModel)
	}
	if declinedCmd != nil {
		t.Fatal("declining the confirmation must not quit")
	}
	if typedDeclined.quitConfirmPending {
		t.Fatal("declining must clear the pending quit confirmation")
	}
	if _, confirmCmd := typedPrompted.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'y'}}); confirmCmd == nil {
		t.Fatal("confirming the prompt must quit")
	}
}

func TestEscStopsActiveRunInsteadOfPromptingQuit(t *testing.T) {
	model := NewModel(Config{Provider: "claude"})
	model.busy = true
	model.turnStartedAt = time.Now()
	stoppedModel, stopCmd := model.Update(tea.KeyMsg{Type: tea.KeyEsc})
	typedStopped, ok := stoppedModel.(Model)
	if !ok {
		t.Fatalf("unexpected model type: %T", stoppedModel)
	}
	if stopCmd != nil {
		t.Fatal("stopping a run must not quit the program")
	}
	if typedStopped.quitConfirmPending {
		t.Fatal("Esc during a run must stop the run rather than prompt to quit")
	}
	if typedStopped.busy {
		t.Fatal("expected the active run to stop")
	}
	if typedStopped.status != "stopped" {
		t.Fatalf("unexpected status after stop: %q", typedStopped.status)
	}
}

func TestBusyWorkspaceShowsStopControl(t *testing.T) {
	model := NewModel(Config{Provider: "claude"})
	model.busy = true
	model.status = "asking agent"
	if !strings.Contains(model.View(), "Stop") {
		t.Fatalf("expected a visible stop control while busy:\n%s", model.View())
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
	activityTitles := make([]string, 0, len(typedModel.activityLog))
	for _, entry := range typedModel.activityLog {
		activityTitles = append(activityTitles, entry.title)
	}
	events := strings.Join(activityTitles, "\n")
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

func TestClickFocusesThePanelUnderTheCursor(t *testing.T) {
	model := NewModel(Config{})
	model.resize(160, 42)
	// The evidence panel only takes a column once it has something to show.
	model.selectedSchema = session.SchemaSnapshot{
		Type: session.ResourceTypeMeasure, Name: "service_cpm", Groups: []string{"sw_metrics"},
		Tags: []string{"service_id"}, Loaded: true,
	}
	model.refreshPanelRegions()
	previewRegion, ok := regionForFocus(model.panelRegions, focusExecution)
	if !ok {
		t.Fatalf("expected a clickable data preview region, got %+v", model.panelRegions)
	}
	clickedModel, _ := model.Update(tea.MouseMsg{
		X:      previewRegion.left,
		Y:      previewRegion.top,
		Action: tea.MouseActionPress,
		Button: tea.MouseButtonLeft,
	})
	typedModel, ok := clickedModel.(Model)
	if !ok {
		t.Fatalf("unexpected model type: %T", clickedModel)
	}
	if typedModel.focus != focusExecution {
		t.Fatalf("expected a click to focus the data preview, got focus %d", typedModel.focus)
	}
	composerRegion, ok := regionForFocus(typedModel.panelRegions, focusMessage)
	if !ok {
		t.Fatalf("expected a clickable composer region, got %+v", typedModel.panelRegions)
	}
	backModel, _ := typedModel.Update(tea.MouseMsg{
		X:      composerRegion.left,
		Y:      composerRegion.top,
		Action: tea.MouseActionPress,
		Button: tea.MouseButtonLeft,
	})
	typedBackModel, ok := backModel.(Model)
	if !ok {
		t.Fatalf("unexpected model type: %T", backModel)
	}
	if typedBackModel.focus != focusMessage {
		t.Fatalf("expected a click to focus the composer, got focus %d", typedBackModel.focus)
	}
}

func regionForFocus(regions []panelRegion, focus int) (panelRegion, bool) {
	for _, region := range regions {
		if region.focus == focus {
			return region, true
		}
	}
	return panelRegion{}, false
}

func TestFocusedPanelIsVisiblyMarked(t *testing.T) {
	model := NewModel(Config{})
	model.resize(160, 42)
	model.focus = focusExecution
	if view := model.View(); !strings.Contains(view, "Focus: data preview") {
		t.Fatalf("expected the status line to name the focused panel:\n%s", view)
	}
	model.focus = focusQuery
	if view := model.View(); !strings.Contains(view, "Focus: candidate QL") {
		t.Fatalf("expected the status line to track focus changes:\n%s", view)
	}
}

func TestCtrlGRepairsInvalidCandidate(t *testing.T) {
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
}

func TestCtrlERunsTheCandidateWithoutApproval(t *testing.T) {
	query := testMeasureQuery
	model := NewModel(Config{})
	querySession := &session.QuerySession{}
	querySession.AddCandidate(session.BydbqlCandidate{
		Query:      query,
		Validation: session.ValidationReport{Valid: true},
	})
	model.querySession = querySession
	model.syncQuerySession()

	executeModel, executeCmd := model.Update(tea.KeyMsg{Type: tea.KeyCtrlE})
	typedExecuteModel, ok := executeModel.(Model)
	if !ok {
		t.Fatalf("unexpected model type: %T", executeModel)
	}
	if !typedExecuteModel.busy || executeCmd == nil {
		t.Fatal("Ctrl+E must start the execution immediately")
	}
	if typedExecuteModel.status != "executing full query" {
		t.Fatalf("unexpected execution status: %q", typedExecuteModel.status)
	}
	if view := typedExecuteModel.View(); strings.Contains(view, "approval") {
		t.Fatalf("execution must not wait for an approval step:\n%s", view)
	}
}

func TestManualEditSurvivesAnAgentTurnUpdate(t *testing.T) {
	query := testMeasureQuery
	querySession := &session.QuerySession{}
	querySession.AddCandidate(session.BydbqlCandidate{
		Query:      query,
		Validation: session.ValidationReport{Valid: true},
	})
	model := NewModel(Config{})
	model.querySession = querySession
	model.syncQuerySession()
	model.focus = focusQuery
	model.syncFocus()

	editedModel, _ := model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'X'}})
	typedEditedModel, ok := editedModel.(Model)
	if !ok {
		t.Fatalf("unexpected model type: %T", editedModel)
	}
	if !typedEditedModel.editingQuery {
		t.Fatal("typing in the editor must mark the candidate as locally edited")
	}
	editedQuery := typedEditedModel.query.Value()
	if editedQuery == query {
		t.Fatal("expected the editor contents to change")
	}

	syncedModel, _ := typedEditedModel.Update(workflowMsg{querySession: querySession, status: "validation complete"})
	typedSyncedModel, ok := syncedModel.(Model)
	if !ok {
		t.Fatalf("unexpected model type: %T", syncedModel)
	}
	if typedSyncedModel.query.Value() != editedQuery {
		t.Fatalf("a session sync must not overwrite an in-progress edit: got %q want %q",
			typedSyncedModel.query.Value(), editedQuery)
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
	model.focus = focusQuery
	model.syncFocus()
	view := model.View()
	for _, expected := range []string{"Conversation", "Message", "Focus: candidate QL", "Time "} {
		if !strings.Contains(view, expected) {
			t.Fatalf("expected %q in workspace:\n%s", expected, view)
		}
	}
	for _, unexpected := range []string{"Autonomous discovery", "Execution policy", "Policy:", "Live output"} {
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
	entries := chatEntries(querySession, "", "")
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

func TestNumberKeysJumpStraightToAPanelOutsideAnEditor(t *testing.T) {
	for _, jump := range []struct {
		key   rune
		focus int
	}{{key: '1', focus: focusChat}, {key: '2', focus: focusQuery}, {key: '3', focus: focusMessage}, {key: '4', focus: focusExecution}} {
		model := NewModel(Config{})
		model.resize(160, 42)
		model.focus = focusChat
		updated, _ := model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{jump.key}})
		typedModel, ok := updated.(Model)
		if !ok {
			t.Fatalf("unexpected model type: %T", updated)
		}
		if typedModel.focus != jump.focus {
			t.Fatalf("key %q must focus panel %d, got %d", jump.key, jump.focus, typedModel.focus)
		}
	}
}

func TestAltNumberKeysJumpToAPanelEvenFromAnEditor(t *testing.T) {
	model := NewModel(Config{})
	model.resize(160, 42)
	model.focus = focusQuery
	model.syncFocus()

	updated, _ := model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'4'}, Alt: true})
	typedModel, ok := updated.(Model)
	if !ok {
		t.Fatalf("unexpected model type: %T", updated)
	}
	if typedModel.focus != focusExecution {
		t.Fatalf("alt+4 must reach the preview from the editor, got %d", typedModel.focus)
	}
	if typedModel.query.Value() != "" {
		t.Fatalf("alt+4 must not type into the editor, got %q", typedModel.query.Value())
	}
}

func TestNumberAndVimKeysStayLiteralInsideAnEditor(t *testing.T) {
	model := NewModel(Config{})
	model.resize(160, 42)
	model.focus = focusMessage
	model.syncFocus()

	var updated tea.Model = model
	for _, character := range []rune{'2', 'j', 'k', '/', '?'} {
		updated, _ = updated.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{character}})
	}
	typedModel, ok := updated.(Model)
	if !ok {
		t.Fatalf("unexpected model type: %T", updated)
	}
	if typedModel.focus != focusMessage {
		t.Fatalf("typing in the composer must not move focus, got %d", typedModel.focus)
	}
	if typedModel.helpVisible {
		t.Fatal("a typed question mark must not open the help overlay")
	}
	if got := typedModel.message.Value(); got != "2jk/?" {
		t.Fatalf("expected the characters to reach the composer, got %q", got)
	}
}

func TestVimKeysMoveTheChatCursorWhenNoEditorHasFocus(t *testing.T) {
	model := NewModel(Config{})
	model.resize(160, 42)
	querySession := &session.QuerySession{}
	for messageIndex := 0; messageIndex < 5; messageIndex++ {
		querySession.AddChatMessage(session.ChatMessage{Role: session.ChatRoleAssistant, Content: "message"})
	}
	model.querySession = querySession
	model.focus = focusChat
	model.chatCursor = 4

	updated, _ := model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'k'}})
	typedModel, ok := updated.(Model)
	if !ok {
		t.Fatalf("unexpected model type: %T", updated)
	}
	if typedModel.chatCursor != 3 {
		t.Fatalf("k must move the cursor up, got %d", typedModel.chatCursor)
	}
}

func TestQuestionMarkTogglesTheHelpOverlay(t *testing.T) {
	model := NewModel(Config{})
	model.resize(160, 42)
	model.focus = focusChat

	opened, _ := model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'?'}})
	openedModel, ok := opened.(Model)
	if !ok {
		t.Fatalf("unexpected model type: %T", opened)
	}
	if !openedModel.helpVisible {
		t.Fatal("? must open the help overlay")
	}
	view := openedModel.View()
	for _, expected := range []string{"Keyboard reference", "Move focus", "Ctrl+E"} {
		if !strings.Contains(view, expected) {
			t.Fatalf("expected %q in the help overlay:\n%s", expected, view)
		}
	}

	closed, _ := openedModel.Update(tea.KeyMsg{Type: tea.KeyEsc})
	closedModel, ok := closed.(Model)
	if !ok {
		t.Fatalf("unexpected model type: %T", closed)
	}
	if closedModel.helpVisible {
		t.Fatal("Esc must close the help overlay")
	}
	if closedModel.quitConfirmPending {
		t.Fatal("closing the help overlay must not also prompt to quit")
	}
}

func TestEscClosesTheSchemaSearchBeforeOfferingToQuit(t *testing.T) {
	model := NewModel(Config{})
	model.resize(160, 42)
	model.catalog.setCatalog(session.SchemaCatalog{Entries: []session.CatalogEntry{
		{Group: "sw_metrics", Type: session.ResourceTypeMeasure, Name: "service_cpm"},
	}})
	model.message.SetValue("show @")
	model.updateSchemaSearch()
	if !model.schemaSearchOpen() {
		t.Fatal("expected the schema search to be open")
	}

	closed, _ := model.Update(tea.KeyMsg{Type: tea.KeyEsc})
	closedModel, ok := closed.(Model)
	if !ok {
		t.Fatalf("unexpected model type: %T", closed)
	}
	if closedModel.schemaSearchOpen() {
		t.Fatal("Esc must close the schema search")
	}
	if closedModel.quitConfirmPending {
		t.Fatal("Esc must not prompt to quit while an overlay is open")
	}
}

func TestSlashOpensTheCatalogSearchInTheComposer(t *testing.T) {
	model := NewModel(Config{})
	model.resize(160, 42)
	model.catalog.setCatalog(session.SchemaCatalog{Entries: []session.CatalogEntry{
		{Group: "sw_metrics", Type: session.ResourceTypeMeasure, Name: "service_cpm"},
	}})
	model.focus = focusChat

	opened, _ := model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'/'}})
	openedModel, ok := opened.(Model)
	if !ok {
		t.Fatalf("unexpected model type: %T", opened)
	}
	if openedModel.focus != focusMessage {
		t.Fatalf("/ must focus the composer, got %d", openedModel.focus)
	}
	if !openedModel.schemaSearchOpen() {
		t.Fatalf("/ must open the catalog search, composer=%q", openedModel.message.Value())
	}
}

func TestSelectedRowDetailAppearsInThePreviewPanel(t *testing.T) {
	model := NewModel(Config{})
	model.resize(160, 42)
	model.querySession = &session.QuerySession{ResourceName: "segment", ExecutionResult: session.ExecutionResult{
		Rows:    2,
		Summary: "query complete",
		Columns: []string{"timestamp", "trace_id", "service_id", "endpoint_id", "content", "span_id"},
		Preview: [][]string{
			{"2026-08-16T12:00:00Z", "trace-1", "checkout", "/pay", "listing", "span-1"},
			{"2026-08-16T12:01:00Z", "trace-2", "cart", "/add", "adding", "span-2"},
		},
	}}
	model.focus = focusExecution
	model.executionRowCursor = 1

	view := model.View()
	if !strings.Contains(view, "Row detail") {
		t.Fatalf("expected the selected row detail in the preview panel:\n%s", view)
	}
	if !strings.Contains(view, "row 2/2") {
		t.Fatalf("expected the detail to name the selected row:\n%s", view)
	}
	if !strings.Contains(view, "span_id: span-2") {
		t.Fatalf("expected the detail to show a column dropped from the table:\n%s", view)
	}
}

func TestSchemaEvidenceScrollsWithPageKeys(t *testing.T) {
	model := NewModel(Config{})
	model.resize(120, 30)
	columns := make([]session.SchemaColumn, 0, 40)
	for columnIndex := 0; columnIndex < 40; columnIndex++ {
		columns = append(columns, session.SchemaColumn{Name: fmt.Sprintf("column_%02d", columnIndex), Kind: "tag", Type: "STRING"})
	}
	model.selectedSchema = session.SchemaSnapshot{
		Type: session.ResourceTypeMeasure, Name: "service_cpm", Groups: []string{"sw_metrics"},
		Loaded: true, Columns: columns,
	}
	model.evidenceMode = evidenceModeSchema
	model.focus = focusExecution

	if view := model.View(); strings.Contains(view, "column_39") {
		t.Fatalf("expected the last column to start off screen:\n%s", view)
	}
	scrolled := tea.Model(model)
	for range 6 {
		scrolled, _ = scrolled.Update(tea.KeyMsg{Type: tea.KeyPgDown})
	}
	scrolledModel, ok := scrolled.(Model)
	if !ok {
		t.Fatalf("unexpected model type: %T", scrolled)
	}
	if scrolledModel.schemaDetailScroll == 0 {
		t.Fatal("pgdn must scroll the schema evidence panel")
	}
	if !strings.Contains(scrolledModel.View(), "column_39") {
		t.Fatalf("expected scrolling to reveal the last column:\n%s", scrolledModel.View())
	}
}

func TestFocusMarkersDoNotChangePanelHeights(t *testing.T) {
	baseline := 0
	for _, focusTarget := range []int{focusChat, focusMessage, focusQuery, focusStart, focusEnd, focusLimit, focusExecution} {
		model := NewModel(Config{})
		model.resize(160, 42)
		model.focus = focusTarget
		model.syncFocus()
		viewHeight := lipgloss.Height(model.View())
		if baseline == 0 {
			baseline = viewHeight
			continue
		}
		if viewHeight != baseline {
			t.Fatalf("focus %d changed the rendered height from %d to %d; the focus marker must not add rows",
				focusTarget, baseline, viewHeight)
		}
	}
}

func TestFooterShowsOnlyTheBindingsOfTheFocusedPanel(t *testing.T) {
	model := NewModel(Config{})
	model.resize(160, 42)
	model.focus = focusMessage
	composerView := model.View()
	if !strings.Contains(composerView, "Enter send") {
		t.Fatalf("expected the composer bindings in the footer:\n%s", composerView)
	}
	if strings.Contains(composerView, "Ctrl+O export") {
		t.Fatalf("the composer footer must not advertise preview bindings:\n%s", composerView)
	}

	model.focus = focusExecution
	previewView := model.View()
	if !strings.Contains(previewView, "Ctrl+O export") {
		t.Fatalf("expected the preview bindings in the footer:\n%s", previewView)
	}
	if strings.Contains(previewView, "Enter send") {
		t.Fatalf("the preview footer must not advertise composer bindings:\n%s", previewView)
	}
}

func TestFooterShowsOnlyStopWhileARunIsActive(t *testing.T) {
	model := NewModel(Config{})
	model.resize(160, 42)
	model.busy = true
	model.status = "asking agent"
	view := model.View()
	if !strings.Contains(view, "Esc stop run") {
		t.Fatalf("expected the stop binding while busy:\n%s", view)
	}
	if strings.Contains(view, "Ctrl+E run") {
		t.Fatalf("a running turn must not advertise a second run:\n%s", view)
	}
}

func TestUpdateDoesNotRenderTheViewToTrackClickRegions(t *testing.T) {
	model := NewModel(Config{})
	model.resize(160, 42)
	model.refreshPanelRegions()
	if model.regionsStale {
		t.Fatal("refreshing regions must clear the stale flag")
	}

	updated, _ := model.Update(tea.KeyMsg{Type: tea.KeyTab})
	typedModel, ok := updated.(Model)
	if !ok {
		t.Fatalf("unexpected model type: %T", updated)
	}
	if !typedModel.regionsStale {
		t.Fatal("a keystroke must mark click regions stale instead of re-rendering the view")
	}

	clicked, _ := typedModel.Update(tea.MouseMsg{
		X: 4, Y: 6, Action: tea.MouseActionPress, Button: tea.MouseButtonLeft,
	})
	clickedModel, ok := clicked.(Model)
	if !ok {
		t.Fatalf("unexpected model type: %T", clicked)
	}
	if len(clickedModel.panelRegions) == 0 {
		t.Fatal("a click must refresh the stale click regions before hit testing")
	}
}

func TestHelpOverlayScrollsInsteadOfTruncating(t *testing.T) {
	model := NewModel(Config{})
	model.resize(120, 30)
	model.helpVisible = true

	firstView := model.View()
	if !strings.Contains(firstView, "Keyboard reference") {
		t.Fatalf("expected the help title:\n%s", firstView)
	}
	if !strings.Contains(firstView, "pgup/pgdn scroll") {
		t.Fatalf("expected a scroll hint when the reference overflows:\n%s", firstView)
	}
	if strings.Contains(firstView, "reload the schema catalog") {
		t.Fatalf("expected the last section to start off screen:\n%s", firstView)
	}

	scrolled := tea.Model(model)
	for range 6 {
		scrolled, _ = scrolled.Update(tea.KeyMsg{Type: tea.KeyPgDown})
	}
	scrolledModel, ok := scrolled.(Model)
	if !ok {
		t.Fatalf("unexpected model type: %T", scrolled)
	}
	if !scrolledModel.helpVisible {
		t.Fatal("scrolling must not close the help overlay")
	}
	if !strings.Contains(scrolledModel.View(), "reload the schema catalog") {
		t.Fatalf("expected scrolling to reveal the global bindings:\n%s", scrolledModel.View())
	}

	closed, _ := scrolledModel.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'?'}})
	closedModel, ok := closed.(Model)
	if !ok {
		t.Fatalf("unexpected model type: %T", closed)
	}
	if closedModel.helpScroll != 0 {
		t.Fatalf("closing the help must reset its scroll, got %d", closedModel.helpScroll)
	}
}
