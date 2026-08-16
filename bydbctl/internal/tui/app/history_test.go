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
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
)

// typeInto sends each rune of text to the model as its own key press.
func typeInto(t *testing.T, model Model, text string) Model {
	t.Helper()
	current := tea.Model(model)
	for _, character := range text {
		current, _ = current.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{character}})
	}
	typedModel, ok := current.(Model)
	if !ok {
		t.Fatalf("unexpected model type: %T", current)
	}
	return typedModel
}

// pressKey sends one key press and returns the resulting model.
func pressKey(t *testing.T, model Model, keyType tea.KeyType) Model {
	t.Helper()
	updated, _ := model.Update(tea.KeyMsg{Type: keyType})
	typedModel, ok := updated.(Model)
	if !ok {
		t.Fatalf("unexpected model type: %T", updated)
	}
	return typedModel
}

func TestComposerArrowsRecallEarlierMessages(t *testing.T) {
	model := NewModel(Config{})
	model.resize(120, 36)
	model.focus = focusMessage
	model.syncFocus()

	model = typeInto(t, model, "first question")
	model = pressKey(t, model, tea.KeyEnter)
	if model.message.Value() != "" {
		t.Fatalf("sending must clear the composer, got %q", model.message.Value())
	}
	model.busy = false
	model = typeInto(t, model, "second question")
	model = pressKey(t, model, tea.KeyEnter)
	model.busy = false

	model = typeInto(t, model, "draft in progress")
	recalled := pressKey(t, model, tea.KeyUp)
	if recalled.message.Value() != "second question" {
		t.Fatalf("the first Up must recall the newest sent message, got %q", recalled.message.Value())
	}
	recalled = pressKey(t, recalled, tea.KeyUp)
	if recalled.message.Value() != "first question" {
		t.Fatalf("a second Up must reach the older message, got %q", recalled.message.Value())
	}
	if !strings.Contains(recalled.status, "message history 1 of 2") {
		t.Fatalf("unexpected recall status: %q", recalled.status)
	}
	recalled = pressKey(t, recalled, tea.KeyUp)
	if recalled.message.Value() != "first question" {
		t.Fatalf("Up past the oldest entry must stay put, got %q", recalled.message.Value())
	}

	recalled = pressKey(t, recalled, tea.KeyDown)
	if recalled.message.Value() != "second question" {
		t.Fatalf("Down must step back towards the newest entry, got %q", recalled.message.Value())
	}
	recalled = pressKey(t, recalled, tea.KeyDown)
	if recalled.message.Value() != "draft in progress" {
		t.Fatalf("Down past the newest entry must restore the draft, got %q", recalled.message.Value())
	}
}

func TestCandidateEditorArrowsStayWithTheEditor(t *testing.T) {
	model := NewModel(Config{})
	model.resize(120, 36)
	querySession := &session.QuerySession{}
	querySession.AddCandidate(session.BydbqlCandidate{
		Query:      testMeasureQuery,
		Validation: session.ValidationReport{Valid: true},
	})
	model.querySession = querySession
	model.syncQuerySession()
	model.focus = focusQuery
	model.syncFocus()

	// The candidate editor has no recall list: Ctrl+←/→ walks the recorded queries instead.
	for _, keyType := range []tea.KeyType{tea.KeyUp, tea.KeyDown} {
		moved := pressKey(t, model, keyType)
		if moved.query.Value() != testMeasureQuery {
			t.Fatalf("an arrow key must not replace the query in the editor, got %q", moved.query.Value())
		}
		if moved.editingQuery {
			t.Fatal("an arrow key must not mark the candidate as locally edited")
		}
	}
}

func TestArrowsMoveTheCursorBeforeReachingTheHistory(t *testing.T) {
	const multiLineDraft = "which resource holds errors,\nand what tags does it have?"
	model := NewModel(Config{})
	model.resize(120, 36)
	model.messageHistory.record("earlier question")
	model.focus = focusMessage
	model.syncFocus()
	// A pasted question can span lines, and the arrows must walk it before reaching the history.
	model.message.SetValue(multiLineDraft)
	if model.message.Line() != 1 {
		t.Fatalf("expected a two-line draft, got line %d", model.message.Line())
	}

	moved := pressKey(t, model, tea.KeyUp)
	if moved.message.Value() != multiLineDraft {
		t.Fatalf("Up inside a multi-line draft must move the cursor, not recall: %q", moved.message.Value())
	}
	if moved.message.Line() != 0 {
		t.Fatalf("expected the cursor on the first line, got %d", moved.message.Line())
	}

	recalled := pressKey(t, moved, tea.KeyUp)
	if recalled.message.Value() != "earlier question" {
		t.Fatalf("Up on the first line must recall history, got %q", recalled.message.Value())
	}

	restored := pressKey(t, recalled, tea.KeyDown)
	if restored.message.Value() != multiLineDraft {
		t.Fatalf("Down must restore the multi-line draft, got %q", restored.message.Value())
	}
}

func TestHistoryCollapsesAnImmediateRepeatAndBoundsItsLength(t *testing.T) {
	var history editorHistory
	history.record("same")
	history.record("same")
	if len(history.entries) != 1 {
		t.Fatalf("an immediate repeat must not be recorded twice, got %v", history.entries)
	}
	history.record("   ")
	if len(history.entries) != 1 {
		t.Fatalf("a blank submission must not be recorded, got %v", history.entries)
	}
	for entryIndex := range maxEditorHistoryEntries + 10 {
		history.record(string(rune('a'+entryIndex%26)) + strings.Repeat("x", entryIndex%3+1))
	}
	if len(history.entries) > maxEditorHistoryEntries {
		t.Fatalf("history must stay bounded, got %d entries", len(history.entries))
	}
}
