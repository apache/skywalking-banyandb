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

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
)

// answerTurnModel builds a workspace whose last turn answered a schema question in words.
func answerTurnModel(t *testing.T, kind session.ChatMessageKind, phase session.Phase) Model {
	t.Helper()
	model := NewModel(Config{Provider: "claude"})
	model.resize(140, 40)
	querySession := &session.QuerySession{Phase: phase}
	querySession.AddChatMessage(session.ChatMessage{Role: session.ChatRoleUser, Content: "describe the sw_trace schema"})
	querySession.AddChatMessage(session.ChatMessage{
		Role:    session.ChatRoleAssistant,
		Kind:    kind,
		Content: "sw_trace is a TRACE resource in group sw_trace with 12 tags.",
		Detail:  "## Tags\n\n- trace_id (string)\n- span_id (string)\n\n## Sortable index rules\n\n- start_time",
	})
	model.querySession = querySession
	model.status = "agent turn complete"
	model.syncQuerySession()
	return model
}

func TestAnswerTurnMarksItselfAsCarryingNoQuery(t *testing.T) {
	model := answerTurnModel(t, session.ChatMessageKindAnswer, session.PhaseConversation)

	view := model.View()
	if !strings.Contains(view, "answered, no query") {
		t.Fatalf("an answer turn must say it produced no query:\n%s", view)
	}
	if strings.Contains(view, "Validation:") {
		t.Fatalf("a turn without a candidate has nothing to validate:\n%s", view)
	}
	if strings.Contains(view, "Ctrl+E") {
		t.Fatalf("an answer turn must not advertise running a query that does not exist:\n%s", view)
	}
}

func TestClarificationTurnSaysItIsWaitingOnTheUser(t *testing.T) {
	model := answerTurnModel(t, session.ChatMessageKindClarification, session.PhaseClarifying)

	view := model.View()
	if !strings.Contains(view, "needs your reply") {
		t.Fatalf("a clarification must ask for a reply:\n%s", view)
	}
	if !strings.Contains(view, "waiting on your reply") {
		t.Fatalf("the collapsed candidate card must explain the wait:\n%s", view)
	}
}

func TestCandidateCardCollapsesUntilThereIsAQuery(t *testing.T) {
	model := answerTurnModel(t, session.ChatMessageKindAnswer, session.PhaseConversation)

	collapsedView := model.View()
	if !strings.Contains(collapsedView, "no query yet") {
		t.Fatalf("an empty candidate card must collapse to one row:\n%s", collapsedView)
	}
	if strings.Contains(collapsedView, "Time ") {
		t.Fatalf("the collapsed card must not render the time and limit slots:\n%s", collapsedView)
	}

	model.focus = focusQuery
	model.syncFocus()
	expandedView := model.View()
	if !strings.Contains(expandedView, "Time ") {
		t.Fatalf("focusing the card must expand it so a query can be written:\n%s", expandedView)
	}
}

func TestConversationTakesTheFullWidthWhenThereIsNoEvidence(t *testing.T) {
	model := answerTurnModel(t, session.ChatMessageKindAnswer, session.PhaseConversation)

	view := model.View()
	if strings.Contains(view, "Data Preview") {
		t.Fatalf("an empty results panel must not take a column:\n%s", view)
	}
	conversationWidth := 0
	for _, line := range strings.Split(view, "\n") {
		if strings.Contains(line, "Conversation") {
			conversationWidth = len(strings.TrimRight(line, " "))
			break
		}
	}
	if conversationWidth < 100 {
		t.Fatalf("the conversation must widen when no evidence panel is shown, got width %d:\n%s", conversationWidth, view)
	}
}

func TestEvidencePanelReturnsWhenATurnProducesResults(t *testing.T) {
	model := answerTurnModel(t, session.ChatMessageKindAnswer, session.PhaseConversation)
	model.querySession.ExecutionResult = session.ExecutionResult{
		Query:   "SELECT * FROM TRACE sw_trace IN sw_trace TIME > '-30m' ORDER BY start_time DESC LIMIT 10",
		Columns: []string{"trace_id"},
		Preview: [][]string{{"abc123"}},
		Rows:    1,
	}

	if view := model.View(); !strings.Contains(view, "Data Preview") {
		t.Fatalf("a turn with results must show the evidence panel again:\n%s", view)
	}
}

func TestChatDetailKeepsTheStructureOfAnAnsweredTurn(t *testing.T) {
	model := answerTurnModel(t, session.ChatMessageKindAnswer, session.PhaseConversation)

	view := model.View()
	if !strings.Contains(view, "• trace_id (string)") {
		t.Fatalf("list items must survive normalization as bullets:\n%s", view)
	}
	if strings.Contains(view, "-trace_id") {
		t.Fatalf("the bullet must not be absorbed into the tag name:\n%s", view)
	}
	if !strings.Contains(view, "Tags") || !strings.Contains(view, "Sortable index rules") {
		t.Fatalf("both headings must remain separate lines:\n%s", view)
	}
}
