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
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/charmbracelet/bubbles/cursor"
	tea "github.com/charmbracelet/bubbletea"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/agent"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/tools"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/workflow"
)

// schemaLookupEntries is the catalog the direct schema lookup resolves names against.
var schemaLookupEntries = []session.CatalogEntry{
	{Group: "sw_trace", Type: session.ResourceTypeTrace, Name: "segment"},
	{Group: "sw_metrics", Type: session.ResourceTypeMeasure, Name: "service_cpm"},
}

// schemaLookupExecutor serves the trace schema a describe turn reads, and refuses to run queries.
type schemaLookupExecutor struct{}

func (executor *schemaLookupExecutor) DiscoverCatalog(_ context.Context) (session.SchemaCatalog, error) {
	return session.SchemaCatalog{Groups: []string{"sw_trace", "sw_metrics"}, Entries: schemaLookupEntries}, nil
}

func (executor *schemaLookupExecutor) DiscoverSchema(_ context.Context, req tools.SchemaRequest) (session.SchemaSnapshot, error) {
	return session.SchemaSnapshot{
		Loaded: true,
		Type:   req.Type,
		Name:   req.Name,
		Groups: append([]string(nil), req.Groups...),
		Columns: []session.SchemaColumn{
			{Name: "trace_id", Kind: session.SchemaColumnTag, Type: session.SchemaValueTypeString, Indexed: true},
			{Name: "duration", Kind: session.SchemaColumnField, Type: session.SchemaValueTypeInt},
		},
		EntityTags: []string{"trace_id"},
	}, nil
}

func (executor *schemaLookupExecutor) Execute(_ context.Context, _ *session.QuerySession, _ string) (session.ExecutionResult, error) {
	return session.ExecutionResult{}, errors.New("a schema lookup must never execute a query")
}

// refusingGateway fails the test if a turn reaches the agent provider at all.
type refusingGateway struct{ t *testing.T }

func (gateway *refusingGateway) Start(_ context.Context, _ agent.StartRequest) (agent.Session, error) {
	gateway.t.Fatal("a direct schema lookup must not start an agent session")
	return agent.Session{}, nil
}

func (gateway *refusingGateway) Send(_ context.Context, _ string, _ agent.TurnRequest) (<-chan agent.Event, error) {
	gateway.t.Fatal("a direct schema lookup must not send an agent turn")
	return nil, nil
}

func (gateway *refusingGateway) Interrupt(_ context.Context, _ string) error { return nil }

func (gateway *refusingGateway) Close() error { return nil }

// sendSchemaLookup composes one schema question, sends it, and folds the resulting turn in.
func sendSchemaLookup(t *testing.T, question string) Model {
	t.Helper()
	model := NewModel(Config{
		Provider:     "claude",
		Executor:     &schemaLookupExecutor{},
		AgentGateway: &refusingGateway{t: t},
	})
	model.resize(160, 42)
	model.catalog.setCatalog(session.SchemaCatalog{
		Groups:  []string{"sw_trace", "sw_metrics"},
		Entries: schemaLookupEntries,
	})
	model.message.SetValue(question)
	model.updateSchemaSearch()
	model.schemaSearchDismissed = true

	sent, sendCmd := model.Update(tea.KeyMsg{Type: tea.KeyEnter})
	sentModel, ok := sent.(Model)
	if !ok {
		t.Fatalf("unexpected model type: %T", sent)
	}
	if !sentModel.busy {
		t.Fatal("sending a schema question must start a turn")
	}
	if sendCmd == nil {
		t.Fatal("sending a schema question must produce a command")
	}
	turnMsg := collectWorkflowMsg(t, sendCmd)
	applied, _ := sentModel.Update(turnMsg)
	appliedModel, ok := applied.(Model)
	if !ok {
		t.Fatalf("unexpected model type: %T", applied)
	}
	return appliedModel
}

// collectWorkflowMsg runs a batched command until it yields the workflow result of the turn.
func collectWorkflowMsg(t *testing.T, command tea.Cmd) workflowMsg {
	t.Helper()
	pending := []tea.Cmd{command}
	for len(pending) > 0 {
		next := pending[0]
		pending = pending[1:]
		if next == nil {
			continue
		}
		switch typedMsg := next().(type) {
		case workflowMsg:
			return typedMsg
		case tea.BatchMsg:
			pending = append(pending, typedMsg...)
		}
	}
	t.Fatal("the turn produced no workflow result")
	return workflowMsg{}
}

func TestSchemaQuestionIsAnsweredFromTheCatalogWithoutTheAgent(t *testing.T) {
	model := sendSchemaLookup(t, "segment 有哪些字段")

	if model.busy {
		t.Fatal("the schema lookup must complete the turn")
	}
	if model.status != statusSchemaComplete {
		t.Fatalf("unexpected status after a schema lookup: %q", model.status)
	}
	if model.querySession == nil || model.querySession.Phase != session.PhaseSchema {
		t.Fatalf("expected the schema phase, got %+v", model.querySession)
	}
	if len(model.querySession.Candidates) != 0 {
		t.Fatalf("a schema lookup must not publish a candidate: %+v", model.querySession.Candidates)
	}
}

func TestSchemaLookupIsLabelledApartFromAQueryResult(t *testing.T) {
	model := sendSchemaLookup(t, "describe @sw_trace/segment")

	view := model.View()
	for _, expected := range []string{
		// The conversation credits the catalog rather than the agent.
		"Schema › ",
		schemaLookupLabel,
		// The evidence panel says which of its two column views this is.
		"read from the catalog · no query run",
		// The candidate card explains why the editor stayed empty.
		"schema lookup only",
	} {
		if !strings.Contains(view, expected) {
			t.Fatalf("expected %q in the schema lookup view:\n%s", expected, view)
		}
	}
	if strings.Contains(view, "Data Preview") {
		t.Fatalf("a schema lookup must not show a results panel:\n%s", view)
	}
	if strings.Contains(view, "Validation:") {
		t.Fatalf("a schema lookup has no candidate to validate:\n%s", view)
	}
	if got := model.currentPhaseLabel(); got != string(session.PhaseSchema) {
		t.Fatalf("expected the status line to name the schema phase, got %q", got)
	}
}

func TestSchemaLookupShowsTheTypedColumnsItRead(t *testing.T) {
	model := sendSchemaLookup(t, "describe @sw_trace/segment")

	view := model.View()
	for _, expected := range []string{"TRACE segment", "trace_id", "duration"} {
		if !strings.Contains(view, expected) {
			t.Fatalf("expected %q in the schema lookup view:\n%s", expected, view)
		}
	}
	if model.evidenceMode != evidenceModeSchemaPinned {
		t.Fatalf("a schema lookup must pin the schema evidence panel, got mode %s", model.evidenceMode)
	}
	if model.selectedSchema.Name != "segment" {
		t.Fatalf("expected the described schema to be selected, got %q", model.selectedSchema.Name)
	}
}

// The repair that rejoins fragmented agent prose must not touch a schema bydbctl read itself: it
// would rewrite an exact BanyanDB identifier, and a column name is not a guess to be corrected.
func TestSchemaLookupRendersExactColumnNames(t *testing.T) {
	columnNames := []string{"searchable.service_id", "SH OW", "min", "a_b", "端点_id"}
	columns := make([]session.SchemaColumn, 0, len(columnNames))
	for _, columnName := range columnNames {
		columns = append(columns, session.SchemaColumn{
			Name: columnName, Kind: session.SchemaColumnTag, Type: session.SchemaValueTypeString,
		})
	}
	entry := chatEntryFromMessage(session.ChatMessage{
		Role:    session.ChatRoleAssistant,
		Kind:    session.ChatMessageKindSchema,
		Content: "schema MEASURE service_cpm in sw_metrics",
		Detail: workflow.FormatSchemaMarkdown(session.SchemaSnapshot{
			Loaded: true, Type: session.ResourceTypeMeasure, Name: "service_cpm",
			Groups: []string{"sw_metrics"}, Columns: columns,
		}),
	})
	if !entry.exactDetail {
		t.Fatal("a schema message must be rendered as exact text")
	}
	rendered := stripANSI(strings.Join(entry.detailLines(90), "\n"))
	for _, columnName := range columnNames {
		if !strings.Contains(rendered, columnName) {
			t.Fatalf("expected the exact column name %q in:\n%s", columnName, rendered)
		}
	}
}

func TestSchemaLookupDoesNotClaimTheDataPreviewFocus(t *testing.T) {
	model := NewModel(Config{
		Executor:     &schemaLookupExecutor{},
		AgentGateway: &refusingGateway{t: t},
	})
	model.resize(160, 42)
	model.focus = focusExecution
	model.querySession = &session.QuerySession{SchemaSnapshot: session.SchemaSnapshot{
		Loaded: true, Type: session.ResourceTypeTrace, Name: "segment", Groups: []string{"sw_trace"},
	}}
	model.querySession.Phase = session.PhaseSchema

	model.applySchemaAnswer()

	if model.focus == focusExecution {
		t.Fatal("a schema answer must release stale Data Preview focus")
	}
	if model.schemaDetailScroll != 0 {
		t.Fatalf("a fresh schema answer must start at the top, got scroll %d", model.schemaDetailScroll)
	}
}

func TestSchemaLookupReleasesTheComposerReference(t *testing.T) {
	model := sendSchemaLookup(t, "describe @sw_trace/segment")

	if model.composerReference != nil {
		t.Fatalf("the sent reference must not pin the next turn, got %+v", model.composerReference)
	}
}

// The composer keeps blinking its cursor after a turn ends, and every one of those messages reaches
// the schema search. A blink is not a request to close the panel, so the answer has to survive it.
func TestSchemaLookupSurvivesComposerCursorBlinks(t *testing.T) {
	model := sendSchemaLookup(t, "describe @sw_trace/segment")
	if !strings.Contains(model.View(), "read from the catalog") {
		t.Fatalf("expected the schema panel right after the lookup:\n%s", model.View())
	}

	blinked := tea.Model(model)
	for range 4 {
		blinked, _ = blinked.Update(cursor.BlinkMsg{})
	}
	blinkedModel, ok := blinked.(Model)
	if !ok {
		t.Fatalf("unexpected model type: %T", blinked)
	}
	if !blinkedModel.evidenceMode.showsSchema() {
		t.Fatalf("a cursor blink must not close the schema panel, got mode %s", blinkedModel.evidenceMode)
	}
	view := blinkedModel.View()
	for _, expected := range []string{"read from the catalog · no query run", "trace_id", "duration"} {
		if !strings.Contains(view, expected) {
			t.Fatalf("expected %q to survive the cursor blinks:\n%s", expected, view)
		}
	}
	if strings.Contains(view, "Data Preview") {
		t.Fatalf("a blink must not hand the slot back to the results panel:\n%s", view)
	}
}

// Typing continues the conversation rather than dismissing the last answer, so the schema stays until
// the user opens a search, focuses the results panel, or runs a query.
func TestSchemaLookupSurvivesTypingTheNextQuestion(t *testing.T) {
	model := sendSchemaLookup(t, "describe @sw_trace/segment")

	typed := tea.Model(model)
	for _, keyRune := range "now show me rows" {
		typed, _ = typed.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{keyRune}})
	}
	typedModel, ok := typed.(Model)
	if !ok {
		t.Fatalf("unexpected model type: %T", typed)
	}
	if !typedModel.evidenceMode.showsSchema() {
		t.Fatalf("typing must not close the schema panel, got mode %s", typedModel.evidenceMode)
	}
}

// Focusing the evidence slot is how a schema is read and scrolled, so it must show the schema that
// is in the slot rather than replacing it with an empty results panel.
func TestFocusingTheEvidencePanelOpensThePinnedSchema(t *testing.T) {
	model := sendSchemaLookup(t, "describe @sw_trace/segment")

	focused, _ := model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'4'}, Alt: true})
	focusedModel, ok := focused.(Model)
	if !ok {
		t.Fatalf("unexpected model type: %T", focused)
	}
	if !focusedModel.showsSchemaEvidence() {
		t.Fatalf("focusing the slot must open the schema in it, got mode %s", focusedModel.evidenceMode)
	}
	view := focusedModel.View()
	if strings.Contains(view, "No results yet") {
		t.Fatalf("focusing a schema must not show an empty results panel:\n%s", view)
	}
	for _, expected := range []string{"read from the catalog · no query run", "trace_id"} {
		if !strings.Contains(view, expected) {
			t.Fatalf("expected %q after focusing the schema:\n%s", expected, view)
		}
	}
	if got := focusedModel.focusLabel(); got != "schema" {
		t.Fatalf("the status line must name the focused panel, got %q", got)
	}
}

// On a terminal too narrow for two columns the schema is off screen, and the hint is the only route
// to it, so the hint has to name it and the key it offers has to arrive there.
func TestNarrowTerminalHintOpensTheSchema(t *testing.T) {
	model := sendSchemaLookup(t, "describe @sw_trace/segment")
	model.resize(80, 30)
	if workspaceIsStacked(80) != true {
		t.Fatal("this test needs the stacked layout")
	}
	if !strings.Contains(model.View(), "open schema segment") {
		t.Fatalf("expected the narrow layout to point at the described schema:\n%s", model.View())
	}

	focused, _ := model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'4'}, Alt: true})
	focusedModel, ok := focused.(Model)
	if !ok {
		t.Fatalf("unexpected model type: %T", focused)
	}
	view := focusedModel.View()
	for _, expected := range []string{"Schema", "read from the catalog", "trace_id", "duration"} {
		if !strings.Contains(view, expected) {
			t.Fatalf("expected %q after opening the schema on a narrow terminal:\n%s", expected, view)
		}
	}
	// Rows, columns, and export belong to a query result; a schema has none of them.
	for _, unexpected := range []string{"↑↓ row", "Ctrl+O export"} {
		if strings.Contains(view, unexpected) {
			t.Fatalf("a schema panel must not offer %q:\n%s", unexpected, view)
		}
	}
}

// The placeholder that shows the sent message while a turn runs is cleared by the agent path only,
// so the describe path has to clear it too or the answer is buried under a duplicate of the question.
func TestSchemaLookupSelectsTheDescriptionNotAPlaceholder(t *testing.T) {
	model := sendSchemaLookup(t, "describe @sw_trace/segment")

	if model.queuedMessage != "" {
		t.Fatalf("the sent placeholder must be cleared once the answer lands, got %q", model.queuedMessage)
	}
	entries := chatEntries(model.querySession, model.liveResponse, model.queuedMessage)
	if len(entries) != 2 {
		t.Fatalf("expected the question and its answer, got %d entries: %+v", len(entries), entries)
	}
	if model.chatCursor != len(entries)-1 {
		t.Fatalf("expected the cursor on the description, got %d of %d", model.chatCursor, len(entries))
	}
	if entries[model.chatCursor].kind != session.ChatMessageKindSchema {
		t.Fatalf("expected the selected entry to be the schema answer, got %+v", entries[model.chatCursor])
	}
	// The conversation is the left column, so the description has to be readable there too.
	view := model.View()
	for _, expected := range []string{"Detail · pgup/pgdn scroll", "trace_id", "duration"} {
		if !strings.Contains(view, expected) {
			t.Fatalf("expected %q in the conversation:\n%s", expected, view)
		}
	}
}

// An @ search preview is the user steering the panel by hand, so closing that search restores nothing:
// the preview is retracted, and only an explicitly pinned schema stays.
func TestClosingASchemaSearchClearsOnlyThePreview(t *testing.T) {
	model := NewModel(Config{Executor: &schemaLookupExecutor{}})
	model.resize(160, 42)
	model.catalog.setCatalog(session.SchemaCatalog{Entries: schemaLookupEntries})
	model.message.SetValue("@segment")
	model.updateSchemaSearch()
	if model.evidenceMode != evidenceModeSchema {
		t.Fatalf("an open search must preview a schema, got mode %s", model.evidenceMode)
	}

	if _, handled := model.handleEscape(); !handled {
		t.Fatal("Esc must close an open schema search")
	}
	if model.evidenceMode != evidenceModeData {
		t.Fatalf("closing a search must retract its preview, got mode %s", model.evidenceMode)
	}
}

func TestDataRequestStillRoutesThroughTheAgent(t *testing.T) {
	model := NewModel(Config{Executor: &schemaLookupExecutor{}})
	model.resize(160, 42)
	model.catalog.setCatalog(session.SchemaCatalog{Entries: schemaLookupEntries})

	if _, ok := model.resolveDescribeTarget("show the latest 10 rows from segment"); ok {
		t.Fatal("a request for stored rows must not be served as a schema lookup")
	}
	if _, ok := model.resolveDescribeTarget("segment 有哪些字段"); !ok {
		t.Fatal("a schema question naming a catalog resource must be served directly")
	}
}
