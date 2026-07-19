// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package claude

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/packages/ssestream"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/agent"
)

func TestConvertToolsPreservesSchema(t *testing.T) {
	definitions := []map[string]any{
		{
			"name":        "validate_bydbql",
			"description": "validate one statement",
			"inputSchema": map[string]any{
				"type":       "object",
				"required":   []string{"query"},
				"properties": map[string]any{"query": map[string]any{"type": "string"}},
			},
		},
		{
			"name":        "propose_query_plan",
			"description": "submit a plan",
			"inputSchema": map[string]any{
				"type":                 "object",
				"additionalProperties": false,
				"oneOf":                []map[string]any{{"required": []string{"plan"}}},
				"properties":           map[string]any{"plan": map[string]any{"type": "object"}},
				"$defs":                map[string]any{"predicate": map[string]any{"type": "object"}},
			},
		},
	}
	tools := convertTools(definitions)
	if len(tools) != 2 {
		t.Fatalf("expected 2 tools, got %d", len(tools))
	}
	validateSchema := marshalToolSchema(t, tools[0])
	if validateSchema["type"] != "object" {
		t.Fatalf("expected validate schema type object, got %v", validateSchema["type"])
	}
	if query, _ := validateSchema["properties"].(map[string]any)["query"]; query == nil {
		t.Fatal("validate schema lost the query property")
	}
	if required, _ := validateSchema["required"].([]any); len(required) != 1 || required[0] != "query" {
		t.Fatalf("validate schema lost required, got %v", validateSchema["required"])
	}
	planSchema := marshalToolSchema(t, tools[1])
	if planSchema["oneOf"] == nil {
		t.Fatal("propose schema lost the oneOf constraint")
	}
	if planSchema["$defs"] == nil {
		t.Fatal("propose schema lost the $defs")
	}
	if planSchema["additionalProperties"] != false {
		t.Fatalf("propose schema lost additionalProperties, got %v", planSchema["additionalProperties"])
	}
	if planSchema["properties"] == nil {
		t.Fatal("propose schema lost the properties")
	}
}

func marshalToolSchema(t *testing.T, tool any) map[string]any {
	t.Helper()
	encoded, marshalErr := json.Marshal(tool)
	if marshalErr != nil {
		t.Fatalf("failed to marshal tool: %v", marshalErr)
	}
	var decoded map[string]any
	if unmarshalErr := json.Unmarshal(encoded, &decoded); unmarshalErr != nil {
		t.Fatalf("failed to unmarshal tool: %v", unmarshalErr)
	}
	schema, _ := decoded["input_schema"].(map[string]any)
	if schema == nil {
		t.Fatalf("tool is missing input_schema: %s", string(encoded))
	}
	return schema
}

func TestRunTurnToolUseLoop(t *testing.T) {
	tools := &recordingControlledTools{}
	gateway := NewGateway(Config{APIKey: "test-key", Tools: tools})
	var round atomic.Int32
	gateway.newStream = func(_ context.Context, _ anthropic.MessageNewParams) *ssestream.Stream[anthropic.MessageStreamEventUnion] {
		var events []sseEvent
		switch round.Add(1) {
		case 1:
			events = toolUseRound()
		default:
			events = endTurnRound()
		}
		return ssestream.NewStream[anthropic.MessageStreamEventUnion](ssestream.NewDecoder(fakeSSEResponse(events)), nil)
	}
	session, startErr := gateway.Start(context.Background(), agent.StartRequest{Provider: agentProviderClaude})
	if startErr != nil {
		t.Fatalf("start failed: %v", startErr)
	}
	events, sendErr := gateway.Send(context.Background(), session.ID, agent.TurnRequest{
		Prompt: "hello",
		Task:   "new_query",
	})
	if sendErr != nil {
		t.Fatalf("send failed: %v", sendErr)
	}
	var collected []agent.Event
	for event := range events {
		collected = append(collected, event)
	}
	if toolCalls := atomic.LoadInt32(&tools.invocations); toolCalls != 1 {
		for idx, event := range collected {
			t.Logf("event[%d] kind=%s origin=%s msg=%q err=%v", idx, event.Kind, event.Origin, event.Message, event.Err)
		}
		t.Fatalf("expected one InvokeTool call, got %d", toolCalls)
	}
	if tools.lastQuery != "SELECT 1" {
		t.Fatalf("expected tool to receive SELECT 1, got %q", tools.lastQuery)
	}
	var messageDeltas int
	var finalResponses int
	var providerToolCalls int
	for _, event := range collected {
		switch event.Kind {
		case agent.EventKindMessageDelta:
			messageDeltas++
		case agent.EventKindFinalResponse:
			finalResponses++
			if !strings.Contains(event.Message, "done") {
				t.Fatalf("final response missing text, got %q", event.Message)
			}
		case agent.EventKindToolCall:
			if event.Origin == agent.EventOriginProvider {
				providerToolCalls++
			}
		case agent.EventKindError:
			t.Fatalf("unexpected error event: %s (err=%v)", event.Message, event.Err)
		}
	}
	if messageDeltas == 0 {
		t.Fatal("expected at least one message delta")
	}
	if finalResponses != 1 {
		t.Fatalf("expected one final response, got %d", finalResponses)
	}
	if providerToolCalls != 0 {
		t.Fatalf("provider must not emit tool_call events, got %d", providerToolCalls)
	}
}

func fakeSSEResponse(events []sseEvent) *http.Response {
	var buffer bytes.Buffer
	for _, event := range events {
		fmt.Fprintf(&buffer, "event: %s\ndata: %s\n\n", event.kind, event.data)
	}
	resp := &http.Response{Body: io.NopCloser(&buffer), Header: http.Header{}}
	resp.Header.Set("content-type", "text/event-stream")
	return resp
}

type sseEvent struct {
	kind string
	data string
}

func toolUseRound() []sseEvent {
	return []sseEvent{
		{kind: "message_start", data: `{"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","content":[],"model":"claude-sonnet-5","stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":1,"output_tokens":0}}}`},
		{kind: "content_block_start", data: `{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"tool_1","name":"validate_bydbql","input":{}}}`},
		{kind: "content_block_delta", data: `{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"query\":"}}`},
		{kind: "content_block_delta", data: `{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"\"SELECT 1\"}"}}`},
		{kind: "content_block_stop", data: `{"type":"content_block_stop","index":0}`},
		{kind: "message_delta", data: `{"type":"message_delta","delta":{"stop_reason":"tool_use","stop_sequence":null},"usage":{"input_tokens":1,"output_tokens":1}}`},
		{kind: "message_stop", data: `{"type":"message_stop"}`},
	}
}

func endTurnRound() []sseEvent {
	return []sseEvent{
		{kind: "message_start", data: `{"type":"message_start","message":{"id":"msg_2","type":"message","role":"assistant","content":[],"model":"claude-sonnet-5","stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":1,"output_tokens":0}}}`},
		{kind: "content_block_start", data: `{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`},
		{kind: "content_block_delta", data: `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"done"}}`},
		{kind: "content_block_stop", data: `{"type":"content_block_stop","index":0}`},
		{kind: "message_delta", data: `{"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"input_tokens":1,"output_tokens":1}}`},
		{kind: "message_stop", data: `{"type":"message_stop"}`},
	}
}

type recordingControlledTools struct {
	invocations int32
	lastQuery   string
}

func (tools *recordingControlledTools) InvokeTool(_ context.Context, name string, arguments map[string]any) (string, error) {
	atomic.AddInt32(&tools.invocations, 1)
	if name != "validate_bydbql" {
		return "", fmt.Errorf("unexpected tool %q", name)
	}
	query, _ := arguments["query"].(string)
	tools.lastQuery = query
	if query != "SELECT 1" {
		return "", fmt.Errorf("unexpected query %q", query)
	}
	return `{"valid":true}`, nil
}

func (tools *recordingControlledTools) Definitions() []map[string]any {
	return []map[string]any{{
		"name":        "validate_bydbql",
		"description": "validate one statement",
		"inputSchema": map[string]any{
			"type":       "object",
			"required":   []string{"query"},
			"properties": map[string]any{"query": map[string]any{"type": "string"}},
		},
	}}
}

const agentProviderClaude = "claude"
