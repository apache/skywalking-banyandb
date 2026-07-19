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

package agent

import (
	"fmt"
	"strings"
	"testing"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
)

func TestBuildBydbqlPromptIncludesOutputContract(t *testing.T) {
	prompt, promptErr := BuildBydbqlPrompt(TurnRequest{
		Prompt: "Generate a query.",
		Payload: RequestPayload{
			Task:      "revise_query_plan",
			Goal:      "top slow endpoints",
			Candidate: "",
			Schema: SchemaSummary{
				Type:          "MEASURE",
				Name:          "service_latency",
				Groups:        []string{"production"},
				Tags:          []string{"endpoint"},
				Fields:        []string{"latency"},
				IndexedFields: []string{"endpoint"},
			},
			TimeRange: TimeRangePayload{Start: "-30m"},
		},
	})
	if promptErr != nil {
		t.Fatalf("BuildBydbqlPrompt returned error: %v", promptErr)
	}
	for _, expected := range []string{
		"query workspace assistant",
		"propose_query_plan",
		"Never publish a raw BYDBQL statement in free text",
		"Controlled workflow",
		"describe_schema",
		"<untrusted_context_json>",
		"top slow endpoints",
		"Use only the provided bydbctl tools",
		"probe_bydbql",
		"time_range",
	} {
		if !strings.Contains(prompt, expected) {
			t.Fatalf("prompt does not contain %q:\n%s", expected, prompt)
		}
	}
}

func TestBuildAgentTurnRequestSharesBoundedPreviewByDefault(t *testing.T) {
	querySession := &session.QuerySession{
		ExecutionResult: session.ExecutionResult{
			Query:        "SELECT * FROM MEASURE latency IN production TIME > '-30m' LIMIT 10",
			Rows:         2,
			ResourceType: "measure",
			Columns:      []string{"service", "latency"},
			Preview:      [][]string{{"payment", "20"}, {"checkout", "50"}},
		},
	}
	payload := BuildAgentTurnRequest(querySession, QueryHints{}, "", "")
	if payload.ExecutionSummary == nil {
		t.Fatal("expected execution summary")
	}
	if len(payload.ExecutionSummary.Preview) != 2 {
		t.Fatalf("expected preview to be shared by default: %+v", payload.ExecutionSummary.Preview)
	}
}

func TestBuildAgentTurnRequestRedactsTransportErrors(t *testing.T) {
	querySession := &session.QuerySession{
		ExecutionResult: session.ExecutionResult{
			Query: "SELECT * FROM MEASURE latency IN production TIME > '-30m' LIMIT 10",
			Error: "Post https://banyandb.internal:17913/api/v1/bydbql/query: dial tcp 10.0.0.1: connect: refused",
		},
	}
	payload := BuildAgentTurnRequest(querySession, QueryHints{}, "", "")
	if payload.ExecutionSummary == nil || payload.ExecutionSummary.Error != "BYDBQL execution failed: transport error" {
		t.Fatalf("unexpected execution error summary: %+v", payload.ExecutionSummary)
	}
	if strings.Contains(payload.ExecutionSummary.Error, "banyandb.internal") {
		t.Fatalf("transport details leaked to provider: %+v", payload.ExecutionSummary)
	}
}

func TestBuildAgentTurnRequestKeepsRecentConversationWindow(t *testing.T) {
	querySession := &session.QuerySession{}
	for turnIdx := 0; turnIdx < 8; turnIdx++ {
		querySession.AddConversationTurn(session.ConversationTurn{
			Hint:      fmt.Sprintf("request-%d", turnIdx),
			Response:  fmt.Sprintf("response-%d", turnIdx),
			Candidate: fmt.Sprintf("candidate-%d", turnIdx),
		})
	}
	payload := BuildAgentTurnRequest(querySession, QueryHints{}, "", "follow up")
	if len(payload.Conversation) != 6 {
		t.Fatalf("expected six recent turns, got %+v", payload.Conversation)
	}
	if payload.Conversation[0].Hint != "request-2" {
		t.Fatalf("expected oldest retained turn, got %+v", payload.Conversation[0])
	}
	if payload.Conversation[5].Hint != "request-7" {
		t.Fatalf("expected latest retained turn, got %+v", payload.Conversation[5])
	}
}

func TestBuildAgentTurnRequestDropsSupersededCandidate(t *testing.T) {
	querySession := &session.QuerySession{
		CandidateSuperseded: true,
		Candidates: []session.BydbqlCandidate{{
			Query: "SELECT * FROM MEASURE old IN production TIME > '-30m' LIMIT 10",
		}},
		SelectedCandidate: 0,
	}
	payload := BuildAgentTurnRequest(querySession, QueryHints{}, "", "use the new logs resource")
	if payload.Candidate != "" || payload.Intent != TurnIntentNewQuery || payload.Task != "new_query" {
		t.Fatalf("superseded candidate leaked into new query turn: %+v", payload)
	}
}

func TestBuildAgentTurnRequestIncludesExactSortableRules(t *testing.T) {
	querySession := &session.QuerySession{SchemaSnapshot: session.SchemaSnapshot{
		Loaded:      true,
		Fingerprint: "schema-v1",
		SortableIndexes: []session.SortableIndex{{
			RuleName: "latency_rule",
			Tags:     []string{"service", "endpoint"},
		}},
	}}
	payload := BuildAgentTurnRequest(querySession, QueryHints{}, "", "show latency")
	if payload.Schema.Fingerprint != "schema-v1" || len(payload.Schema.SortableIndexes) != 1 {
		t.Fatalf("missing schema identity or sortable rules: %+v", payload.Schema)
	}
	if payload.Schema.SortableIndexes[0].RuleName != "latency_rule" {
		t.Fatalf("unexpected sortable rule: %+v", payload.Schema.SortableIndexes)
	}
}

func TestBuildBydbqlPromptAllowsConversationBeforeAPlan(t *testing.T) {
	prompt, promptErr := BuildBydbqlPrompt(TurnRequest{
		Prompt: "Continue the conversation.",
		Payload: RequestPayload{
			Task: "continue_conversation",
			Goal: "Which groups contain payment metrics?",
			Schema: SchemaSummary{
				AvailableGroups: []string{"production", "staging"},
			},
		},
	})
	if promptErr != nil {
		t.Fatalf("BuildBydbqlPrompt returned error: %v", promptErr)
	}
	for _, expected := range []string{
		"A normal conversational response is valid when no query is ready.",
		"Submit a typed query plan only when the user asks for a query and the request is specific enough.",
	} {
		if !strings.Contains(prompt, expected) {
			t.Fatalf("prompt does not contain %q:\n%s", expected, prompt)
		}
	}
	if strings.Contains(prompt, "Do not end the turn until propose_query_plan returns valid=true") {
		t.Fatalf("conversation prompt still requires a plan:\n%s", prompt)
	}
}
