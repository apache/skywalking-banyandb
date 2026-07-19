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

package builtin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/agent"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/bridge"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/tools"
)

func TestOpenAIChatModelDecodesToolCalls(t *testing.T) {
	var receivedPayload map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/chat/completions" {
			t.Fatalf("unexpected path: %s", request.URL.Path)
		}
		if decodeErr := json.NewDecoder(request.Body).Decode(&receivedPayload); decodeErr != nil {
			t.Fatalf("failed to decode request payload: %v", decodeErr)
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(
			`{"choices":[{"message":{"role":"assistant","tool_calls":[` +
				`{"id":"call-1","type":"function","function":{"name":"describe_schema","arguments":"{\"name\":\"latency\"}"}}]}}]}`,
		))
	}))
	defer server.Close()
	chatModel, modelErr := NewOpenAIChatModel(ModelConfig{APIKey: "test-key", BaseURL: server.URL, Model: "test-model"})
	if modelErr != nil {
		t.Fatalf("NewOpenAIChatModel returned error: %v", modelErr)
	}
	response, chatErr := chatModel.Chat(context.Background(), ChatRequest{
		Messages: []Message{{Role: "user", Content: "hello"}},
		Tools:    bridge.ToolDefinitions(),
	})
	if chatErr != nil {
		t.Fatalf("Chat returned error: %v", chatErr)
	}
	if len(response.Message.ToolCalls) != 1 || response.Message.ToolCalls[0].Name != "describe_schema" {
		t.Fatalf("unexpected tool calls: %+v", response.Message.ToolCalls)
	}
	if receivedPayload["temperature"] != float64(0) || receivedPayload["parallel_tool_calls"] != false {
		t.Fatalf("expected deterministic serialized tool request, got %+v", receivedPayload)
	}
}

func TestGatewayInvokesToolBridgeDirectly(t *testing.T) {
	schema := session.SchemaSnapshot{
		Type:   session.ResourceTypeMeasure,
		Name:   "service_latency",
		Groups: []string{"production"},
		Loaded: true,
		Columns: []session.SchemaColumn{
			{Name: "latency", Kind: session.SchemaColumnField, Type: session.SchemaValueTypeFloat},
		},
	}
	toolBridge := bridge.New(bridge.Config{
		Executor:  &stubExecutor{schema: schema},
		Validator: &stubValidator{report: session.ValidationReport{Valid: true, QueryType: "MEASURE"}},
	})
	toolBridge.SetSession(&session.QuerySession{SchemaSnapshot: schema})
	planArguments, marshalErr := json.Marshal(map[string]any{
		"plan": map[string]any{
			"resource": map[string]any{"type": "MEASURE", "name": "service_latency", "groups": []any{"production"}},
			"projection": []any{
				map[string]any{"column": "latency"},
			},
			"limit": 10,
		},
	})
	if marshalErr != nil {
		t.Fatalf("failed to marshal plan arguments: %v", marshalErr)
	}
	sequenceModel := &scriptedChatModel{responses: []ChatResponse{
		{Message: Message{Role: "assistant", ToolCalls: []ToolCall{{
			ID: "call-1", Name: bridge.ToolProposeQueryPlan, Arguments: string(planArguments),
		}}}},
		{Message: Message{Role: "assistant", Content: "compiled a latency query"}},
	}}
	gateway, gatewayErr := NewGateway(Config{ToolBridge: toolBridge, Model: sequenceModel})
	if gatewayErr != nil {
		t.Fatalf("NewGateway returned error: %v", gatewayErr)
	}
	sessionValue, startErr := gateway.Start(context.Background(), agentStartRequest())
	if startErr != nil {
		t.Fatalf("Start returned error: %v", startErr)
	}
	events, sendErr := gateway.Send(context.Background(), sessionValue.ID, agentTurnRequest())
	if sendErr != nil {
		t.Fatalf("Send returned error: %v", sendErr)
	}
	var finalMessage string
	for event := range events {
		if event.Kind == agent.EventKindFinalResponse {
			finalMessage = event.Message
		}
	}
	if !strings.Contains(finalMessage, "latency query") {
		t.Fatalf("expected final response, got %q", finalMessage)
	}
	if sequenceModel.calls != 2 {
		t.Fatalf("expected two model calls, got %d", sequenceModel.calls)
	}
	if len(sequenceModel.requests) == 0 || len(sequenceModel.requests[0].Messages) < 2 {
		t.Fatalf("expected system and user messages, got %+v", sequenceModel.requests)
	}
	if sequenceModel.requests[0].Messages[0].Role != "system" || sequenceModel.requests[0].Messages[1].Role != "user" {
		t.Fatalf("expected trusted rules to use the system role, got %+v", sequenceModel.requests[0].Messages)
	}
	if sequenceModel.requests[0].Temperature != 0 {
		t.Fatalf("expected deterministic model temperature, got %v", sequenceModel.requests[0].Temperature)
	}
	if len(toolBridge.SessionSnapshot().PlannedQueries) != 1 {
		t.Fatalf("expected compiled plan in bridge session: %+v", toolBridge.SessionSnapshot().PlannedQueries)
	}
}

func TestGatewayRetriesAfterProposeFailure(t *testing.T) {
	schema := session.SchemaSnapshot{
		Type:   session.ResourceTypeMeasure,
		Name:   "service_latency",
		Groups: []string{"production"},
		Loaded: true,
		Columns: []session.SchemaColumn{
			{Name: "latency", Kind: session.SchemaColumnField, Type: session.SchemaValueTypeFloat},
		},
	}
	toolBridge := bridge.New(bridge.Config{
		Executor:  &stubExecutor{schema: schema},
		Validator: &stubValidator{report: session.ValidationReport{Valid: true, QueryType: "MEASURE"}},
	})
	toolBridge.SetSession(&session.QuerySession{SchemaSnapshot: schema})
	invalidPlan, marshalInvalidErr := json.Marshal(map[string]any{
		"plan": map[string]any{
			"resource": map[string]any{"type": "MEASURE", "name": "service_latency", "groups": []any{"production"}},
			"filter":   map[string]any{"column": "unknown", "operator": "=", "value": 1},
			"limit":    10,
		},
	})
	if marshalInvalidErr != nil {
		t.Fatalf("failed to marshal invalid plan: %v", marshalInvalidErr)
	}
	validPlan, marshalValidErr := json.Marshal(map[string]any{
		"plan": map[string]any{
			"resource": map[string]any{"type": "MEASURE", "name": "service_latency", "groups": []any{"production"}},
			"projection": []any{
				map[string]any{"column": "latency"},
			},
			"limit": 10,
		},
	})
	if marshalValidErr != nil {
		t.Fatalf("failed to marshal valid plan: %v", marshalValidErr)
	}
	sequenceModel := &scriptedChatModel{responses: []ChatResponse{
		{Message: Message{Role: "assistant", ToolCalls: []ToolCall{{
			ID: "call-1", Name: bridge.ToolProposeQueryPlan, Arguments: string(invalidPlan),
		}}}},
		{Message: Message{Role: "assistant", ToolCalls: []ToolCall{{
			ID: "call-2", Name: bridge.ToolProposeQueryPlan, Arguments: string(validPlan),
		}}}},
		{Message: Message{Role: "assistant", Content: "repaired the latency query"}},
	}}
	gateway, gatewayErr := NewGateway(Config{ToolBridge: toolBridge, Model: sequenceModel})
	if gatewayErr != nil {
		t.Fatalf("NewGateway returned error: %v", gatewayErr)
	}
	sessionValue, startErr := gateway.Start(context.Background(), agentStartRequest())
	if startErr != nil {
		t.Fatalf("Start returned error: %v", startErr)
	}
	events, sendErr := gateway.Send(context.Background(), sessionValue.ID, agentTurnRequest())
	if sendErr != nil {
		t.Fatalf("Send returned error: %v", sendErr)
	}
	for event := range events {
		if event.Kind == agent.EventKindError {
			t.Fatalf("unexpected gateway error: %s", event.Message)
		}
	}
	if sequenceModel.calls != 3 {
		t.Fatalf("expected three model calls after propose repair, got %d", sequenceModel.calls)
	}
	if len(toolBridge.SessionSnapshot().PlannedQueries) != 1 {
		t.Fatalf("expected compiled plan after repair, got %+v", toolBridge.SessionSnapshot().PlannedQueries)
	}
}

type scriptedChatModel struct {
	responses []ChatResponse
	requests  []ChatRequest
	calls     int
}

func (model *scriptedChatModel) Chat(_ context.Context, request ChatRequest) (ChatResponse, error) {
	model.requests = append(model.requests, request)
	if model.calls >= len(model.responses) {
		return ChatResponse{}, nil
	}
	response := model.responses[model.calls]
	model.calls++
	return response, nil
}

type stubExecutor struct {
	schema session.SchemaSnapshot
}

func (executor *stubExecutor) DiscoverCatalog(context.Context) (session.SchemaCatalog, error) {
	return session.SchemaCatalog{}, nil
}

func (executor *stubExecutor) DiscoverSchema(context.Context, tools.SchemaRequest) (session.SchemaSnapshot, error) {
	return executor.schema, nil
}

func (executor *stubExecutor) Execute(context.Context, *session.QuerySession, string) (session.ExecutionResult, error) {
	return session.ExecutionResult{}, nil
}

type stubValidator struct {
	report session.ValidationReport
}

func (validator *stubValidator) Validate(context.Context, string, *session.SchemaSnapshot) (session.ValidationReport, error) {
	return validator.report, nil
}

func agentStartRequest() agent.StartRequest {
	return agent.StartRequest{Provider: "builtin"}
}

func agentTurnRequest() agent.TurnRequest {
	return agent.TurnRequest{
		Prompt: "draft a query",
		Payload: agent.RequestPayload{
			Task: "continue_conversation",
			Goal: "show latency",
		},
	}
}
