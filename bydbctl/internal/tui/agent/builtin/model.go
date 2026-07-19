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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const defaultHTTPTimeout = 120 * time.Second

// ToolCall is one model-requested controlled tool invocation.
type ToolCall struct {
	ID        string
	Name      string
	Arguments string
}

// Message is one chat message in a model conversation.
type Message struct {
	Role       string
	Content    string
	ToolCalls  []ToolCall
	ToolCallID string
}

// ChatRequest is one chat completion request with optional tools.
type ChatRequest struct {
	Messages    []Message
	Tools       []map[string]any
	Temperature float64
}

// ChatResponse is one assistant message from the model.
type ChatResponse struct {
	Message Message
}

// ChatModel performs one chat completion turn.
type ChatModel interface {
	Chat(ctx context.Context, request ChatRequest) (ChatResponse, error)
}

// ModelConfig configures an OpenAI-compatible chat model client.
type ModelConfig struct {
	APIKey  string
	BaseURL string
	Model   string
	Client  *http.Client
}

// OpenAIChatModel calls an OpenAI-compatible chat completions API.
type OpenAIChatModel struct {
	apiKey  string
	baseURL string
	model   string
	client  *http.Client
}

// NewOpenAIChatModel creates an OpenAI-compatible chat model client.
func NewOpenAIChatModel(config ModelConfig) (*OpenAIChatModel, error) {
	apiKey := strings.TrimSpace(config.APIKey)
	if apiKey == "" {
		return nil, fmt.Errorf("BYDBCTL_AGENT_API_KEY is required for --agent builtin")
	}
	baseURL := strings.TrimRight(strings.TrimSpace(config.BaseURL), "/")
	if baseURL == "" {
		baseURL = "https://api.openai.com/v1"
	}
	modelName := strings.TrimSpace(config.Model)
	if modelName == "" {
		modelName = "gpt-4o-mini"
	}
	httpClient := config.Client
	if httpClient == nil {
		httpClient = &http.Client{Timeout: defaultHTTPTimeout}
	}
	return &OpenAIChatModel{
		apiKey:  apiKey,
		baseURL: baseURL,
		model:   modelName,
		client:  httpClient,
	}, nil
}

// Chat sends one chat completion request.
func (chatModel *OpenAIChatModel) Chat(ctx context.Context, request ChatRequest) (ChatResponse, error) {
	payload := map[string]any{
		"model":       chatModel.model,
		"messages":    encodeMessages(request.Messages),
		"temperature": request.Temperature,
	}
	if len(request.Tools) > 0 {
		payload["tools"] = encodeTools(request.Tools)
		payload["parallel_tool_calls"] = false
	}
	body, marshalErr := json.Marshal(payload)
	if marshalErr != nil {
		return ChatResponse{}, fmt.Errorf("failed to encode chat request: %w", marshalErr)
	}
	httpRequest, requestErr := http.NewRequestWithContext(ctx, http.MethodPost, chatModel.baseURL+"/chat/completions", bytes.NewReader(body))
	if requestErr != nil {
		return ChatResponse{}, fmt.Errorf("failed to create chat request: %w", requestErr)
	}
	httpRequest.Header.Set("Authorization", "Bearer "+chatModel.apiKey)
	httpRequest.Header.Set("Content-Type", "application/json")
	httpResponse, doErr := chatModel.client.Do(httpRequest)
	if doErr != nil {
		return ChatResponse{}, fmt.Errorf("chat completion request failed: %w", doErr)
	}
	defer func() {
		_ = httpResponse.Body.Close()
	}()
	responseBody, readErr := io.ReadAll(httpResponse.Body)
	if readErr != nil {
		return ChatResponse{}, fmt.Errorf("failed to read chat response: %w", readErr)
	}
	if httpResponse.StatusCode < http.StatusOK || httpResponse.StatusCode >= http.StatusMultipleChoices {
		return ChatResponse{}, fmt.Errorf("chat completion returned %s: %s", httpResponse.Status, truncate(string(responseBody), 500))
	}
	message, decodeErr := decodeChatResponse(responseBody)
	if decodeErr != nil {
		return ChatResponse{}, decodeErr
	}
	return ChatResponse{Message: message}, nil
}

func encodeMessages(messages []Message) []map[string]any {
	encoded := make([]map[string]any, 0, len(messages))
	for _, message := range messages {
		item := map[string]any{"role": message.Role}
		if strings.TrimSpace(message.Content) != "" {
			item["content"] = message.Content
		}
		if message.ToolCallID != "" {
			item["tool_call_id"] = message.ToolCallID
		}
		if len(message.ToolCalls) > 0 {
			toolCalls := make([]map[string]any, 0, len(message.ToolCalls))
			for _, toolCall := range message.ToolCalls {
				toolCalls = append(toolCalls, map[string]any{
					"id":   toolCall.ID,
					"type": "function",
					"function": map[string]any{
						"name":      toolCall.Name,
						"arguments": toolCall.Arguments,
					},
				})
			}
			item["tool_calls"] = toolCalls
		}
		encoded = append(encoded, item)
	}
	return encoded
}

func encodeTools(toolDefinitions []map[string]any) []map[string]any {
	tools := make([]map[string]any, 0, len(toolDefinitions))
	for _, definition := range toolDefinitions {
		tools = append(tools, map[string]any{
			"type": "function",
			"function": map[string]any{
				"name":        definition["name"],
				"description": definition["description"],
				"parameters":  definition["inputSchema"],
			},
		})
	}
	return tools
}

func decodeChatResponse(responseBody []byte) (Message, error) {
	var raw map[string]any
	if unmarshalErr := json.Unmarshal(responseBody, &raw); unmarshalErr != nil {
		return Message{}, fmt.Errorf("failed to decode chat response: %w", unmarshalErr)
	}
	choices, _ := raw["choices"].([]any)
	if len(choices) == 0 {
		return Message{}, fmt.Errorf("chat completion returned no choices")
	}
	choice, choiceOK := choices[0].(map[string]any)
	if !choiceOK {
		return Message{}, fmt.Errorf("chat completion choice has unexpected shape")
	}
	messageValue, _ := choice["message"].(map[string]any)
	if messageValue == nil {
		return Message{}, fmt.Errorf("chat completion returned no message")
	}
	message := Message{
		Role:    stringValue(messageValue["role"]),
		Content: stringValue(messageValue["content"]),
	}
	toolCalls, _ := messageValue["tool_calls"].([]any)
	for _, toolCallValue := range toolCalls {
		toolCallMap, toolCallOK := toolCallValue.(map[string]any)
		if !toolCallOK {
			continue
		}
		functionValue, _ := toolCallMap["function"].(map[string]any)
		message.ToolCalls = append(message.ToolCalls, ToolCall{
			ID:        stringValue(toolCallMap["id"]),
			Name:      stringValue(functionValue["name"]),
			Arguments: stringValue(functionValue["arguments"]),
		})
	}
	return message, nil
}

func stringValue(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	default:
		return ""
	}
}

func truncate(value string, limit int) string {
	trimmedValue := strings.Join(strings.Fields(value), " ")
	if len(trimmedValue) <= limit {
		return trimmedValue
	}
	return trimmedValue[:limit] + "..."
}
