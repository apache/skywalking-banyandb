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
	"testing"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
)

func TestIsInformationalRequestSeparatesSchemaQuestionsFromDataRequests(t *testing.T) {
	informational := []string{
		"what fields does @sw_metrics/service_cpm have?",
		"which resources can I use to inspect errors?",
		"describe the segment trace schema",
		"how do i order by an index rule?",
		"service_cpm 有哪些字段",
		"这个表结构是什么",
		"看下 schema",
		"描述下 sw_trace 下面的 schema",
		"介绍下 sw_trace",
		"讲讲 sw_trace 这个资源",
		"列出所有的 group",
		"sw_trace 是干什么的",
	}
	for _, turnHint := range informational {
		if !IsInformationalRequest(turnHint) {
			t.Fatalf("expected %q to be an informational request", turnHint)
		}
	}
	dataRequests := []string{
		"show the latest 10 rows for the last 30 minutes",
		"top slow payment endpoints",
		"run the query",
		"查一下数据",
		"最近30分钟的错误率",
		"what is the average latency", // asks for an aggregate over stored rows
		// A description verb loses to an explicit data marker in the same turn.
		"描述下 sw_trace 并查一下最近的数据",
	}
	for _, turnHint := range dataRequests {
		if IsInformationalRequest(turnHint) {
			t.Fatalf("expected %q to be treated as a data request", turnHint)
		}
	}
	if IsInformationalRequest("") {
		t.Fatal("an empty turn must not be classified as informational")
	}
}

func TestIsSchemaDescriptionRequestNarrowsToResourceShapeQuestions(t *testing.T) {
	descriptions := []string{
		"what fields does @sw_metrics/service_cpm have?",
		"describe the segment trace schema",
		"show me the schema of service_cpm",
		"service_cpm 有哪些字段",
		"segment 的表结构",
		"描述下 sw_trace",
		"看下 schema",
	}
	for _, turnHint := range descriptions {
		if !IsSchemaDescriptionRequest(turnHint) {
			t.Fatalf("expected %q to ask for a resource description", turnHint)
		}
		// Every description request is also informational, so the two classifiers cannot disagree.
		if !IsInformationalRequest(turnHint) {
			t.Fatalf("expected %q to remain an informational request", turnHint)
		}
	}
	// These stay with the agent: a catalog question, advice, or a request for stored rows.
	notDescriptions := []string{
		"which resources can I use to inspect errors?",
		"how do i order by an index rule?",
		"list all the groups",
		"can i use MATCH here",
		"show the latest 10 rows for the last 30 minutes",
		"描述下 sw_trace 并查一下最近的数据",
		"",
	}
	for _, turnHint := range notDescriptions {
		if IsSchemaDescriptionRequest(turnHint) {
			t.Fatalf("expected %q not to be served as a direct schema description", turnHint)
		}
	}
}

func TestSchemaQuestionsClassifyAsAnswerTurns(t *testing.T) {
	querySession := &session.QuerySession{}
	querySession.AddCandidate(session.BydbqlCandidate{
		Query:      "SELECT value FROM MEASURE service_cpm IN sw_metrics TIME > '-30m' LIMIT 10",
		Validation: session.ValidationReport{Valid: true},
	})
	payload := BuildAgentTurnRequest(querySession, QueryHints{}, "", "what fields does service_cpm have?")
	if payload.Intent != TurnIntentAnswer {
		t.Fatalf("a schema question must not become a query turn, got intent %q", payload.Intent)
	}
	if payload.Task != "answer_question" {
		t.Fatalf("unexpected task for a schema question: %q", payload.Task)
	}
}

func TestDataRequestsStillClassifyAsQueryTurns(t *testing.T) {
	querySession := &session.QuerySession{}
	payload := BuildAgentTurnRequest(querySession, QueryHints{}, "", "show the latest 10 rows from service_cpm")
	if payload.Intent != TurnIntentNewQuery {
		t.Fatalf("a data request must remain a query turn, got intent %q", payload.Intent)
	}
}

func TestRepairStillTakesPrecedenceOverPhrasing(t *testing.T) {
	querySession := &session.QuerySession{
		Validation: session.ValidationReport{Valid: false, Message: "syntax error near FROM"},
	}
	querySession.AddCandidate(session.BydbqlCandidate{
		Query:      "SELECT FROM",
		Validation: session.ValidationReport{Valid: false, Message: "syntax error near FROM"},
	})
	payload := BuildAgentTurnRequest(querySession, QueryHints{}, "", "fix this and show me the data")
	if payload.Intent != TurnIntentRepair {
		t.Fatalf("an invalid candidate must still route to repair, got intent %q", payload.Intent)
	}
}
