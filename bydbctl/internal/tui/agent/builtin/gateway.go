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

// Package builtin runs an in-process agent that calls ToolBridge directly.
package builtin

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/agent"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/bridge"
)

const (
	defaultMaxToolRounds     = 20
	defaultMaxProposeRepairs = 6
	eventBufferSize          = 64
)

// Config creates a builtin agent gateway.
type Config struct {
	ToolBridge        *bridge.ToolBridge
	Model             ChatModel
	MaxToolRounds     int
	MaxProposeRepairs int
}

// Gateway runs controlled tools in-process through ToolBridge.
type Gateway struct {
	toolBridge        *bridge.ToolBridge
	model             ChatModel
	maxToolRounds     int
	maxProposeRepairs int
	now               func() time.Time
	mu                sync.Mutex
	sessions          map[string]struct{}
}

// NewGateway creates a builtin agent gateway.
func NewGateway(config Config) (*Gateway, error) {
	if config.ToolBridge == nil {
		return nil, fmt.Errorf("tool bridge is required")
	}
	if config.Model == nil {
		return nil, fmt.Errorf("chat model is required")
	}
	maxToolRounds := config.MaxToolRounds
	if maxToolRounds <= 0 {
		maxToolRounds = defaultMaxToolRounds
	}
	maxProposeRepairs := config.MaxProposeRepairs
	if maxProposeRepairs <= 0 {
		maxProposeRepairs = defaultMaxProposeRepairs
	}
	return &Gateway{
		toolBridge:        config.ToolBridge,
		model:             config.Model,
		maxToolRounds:     maxToolRounds,
		maxProposeRepairs: maxProposeRepairs,
		now:               time.Now,
		sessions:          make(map[string]struct{}),
	}, nil
}

// Start creates a builtin agent session.
func (gateway *Gateway) Start(_ context.Context, req agent.StartRequest) (agent.Session, error) {
	sessionID := "builtin-" + uuid.NewString()
	gateway.mu.Lock()
	gateway.sessions[sessionID] = struct{}{}
	gateway.mu.Unlock()
	provider := strings.TrimSpace(req.Provider)
	if provider == "" {
		provider = "builtin"
	}
	return agent.Session{
		ID:        sessionID,
		Provider:  provider,
		StartedAt: gateway.now(),
	}, nil
}

// Send runs one prompt turn with direct controlled tool calls.
func (gateway *Gateway) Send(ctx context.Context, sessionID string, req agent.TurnRequest) (<-chan agent.Event, error) {
	if lookupErr := gateway.ensureSession(sessionID); lookupErr != nil {
		return nil, lookupErr
	}
	promptText, promptErr := agent.BuildBydbqlPrompt(req)
	if promptErr != nil {
		return nil, fmt.Errorf("failed to build builtin prompt: %w", promptErr)
	}
	events := make(chan agent.Event, eventBufferSize)
	go gateway.runTurn(ctx, promptText, events)
	return events, nil
}

// Stop ends a builtin agent session.
func (gateway *Gateway) Stop(_ context.Context, sessionID string) error {
	gateway.mu.Lock()
	delete(gateway.sessions, sessionID)
	gateway.mu.Unlock()
	return nil
}

func (gateway *Gateway) ensureSession(sessionID string) error {
	gateway.mu.Lock()
	defer gateway.mu.Unlock()
	if _, found := gateway.sessions[sessionID]; !found {
		return fmt.Errorf("builtin session %q was not found", sessionID)
	}
	return nil
}

func (gateway *Gateway) runTurn(ctx context.Context, promptText string, events chan<- agent.Event) {
	defer close(events)
	messages := []Message{{Role: "user", Content: promptText}}
	tools := bridge.ToolDefinitions()
	pendingProposeRepair := false
	proposeRepairCount := 0
	for roundIdx := 0; roundIdx < gateway.maxToolRounds; roundIdx++ {
		if ctx.Err() != nil {
			_ = sendEvent(ctx, events, agent.Event{
				Kind:    agent.EventKindError,
				Message: ctx.Err().Error(),
				Err:     ctx.Err(),
			})
			return
		}
		response, chatErr := gateway.model.Chat(ctx, ChatRequest{Messages: messages, Tools: tools})
		if chatErr != nil {
			_ = sendEvent(ctx, events, agent.Event{
				Kind:    agent.EventKindError,
				Message: chatErr.Error(),
				Err:     chatErr,
			})
			return
		}
		assistantMessage := response.Message
		if len(assistantMessage.ToolCalls) == 0 {
			if pendingProposeRepair && proposeRepairCount < gateway.maxProposeRepairs {
				messages = append(messages, Message{Role: "user", Content: proposeRepairNudge})
				proposeRepairCount++
				continue
			}
			finalMessage := strings.TrimSpace(assistantMessage.Content)
			if finalMessage == "" {
				finalMessage = "builtin agent completed the turn without a final message"
			}
			_ = sendEvent(ctx, events, agent.Event{
				Kind:    agent.EventKindFinalResponse,
				Message: finalMessage,
			})
			return
		}
		messages = append(messages, assistantMessage)
		roundProposeFailed := false
		roundProposeSucceeded := false
		for _, toolCall := range assistantMessage.ToolCalls {
			toolResult, toolErr := gateway.invokeTool(ctx, toolCall)
			if toolErr != nil {
				toolResult = toolErr.Error()
			}
			if proposeToolSucceeded(toolCall.Name, toolResult) {
				roundProposeSucceeded = true
			}
			if proposeToolFailed(toolCall.Name, toolResult) {
				roundProposeFailed = true
			}
			messages = append(messages, Message{
				Role:       "tool",
				ToolCallID: toolCall.ID,
				Content:    toolResult,
			})
		}
		if roundProposeSucceeded {
			pendingProposeRepair = false
			proposeRepairCount = 0
			continue
		}
		if roundProposeFailed {
			pendingProposeRepair = true
			if proposeRepairCount < gateway.maxProposeRepairs {
				messages = append(messages, Message{Role: "user", Content: proposeRepairNudge})
				proposeRepairCount++
			}
		}
	}
	_ = sendEvent(ctx, events, agent.Event{
		Kind:    agent.EventKindError,
		Message: fmt.Sprintf("builtin agent exceeded %d controlled tool rounds", gateway.maxToolRounds),
		Err:     fmt.Errorf("builtin agent exceeded %d controlled tool rounds", gateway.maxToolRounds),
	})
}

func (gateway *Gateway) invokeTool(ctx context.Context, toolCall ToolCall) (string, error) {
	toolName := strings.TrimSpace(toolCall.Name)
	if toolName == "" {
		return "", fmt.Errorf("tool call is missing a name")
	}
	arguments := map[string]any{}
	if strings.TrimSpace(toolCall.Arguments) != "" {
		if decodeErr := json.Unmarshal([]byte(toolCall.Arguments), &arguments); decodeErr != nil {
			return "", fmt.Errorf("failed to decode tool arguments for %s: %w", toolName, decodeErr)
		}
	}
	result := gateway.toolBridge.Call(ctx, bridge.Call{Name: toolName, Arguments: arguments})
	if result.Err != nil {
		return "", result.Err
	}
	if strings.TrimSpace(result.Content) == "" {
		return "{}", nil
	}
	return result.Content, nil
}

func sendEvent(ctx context.Context, events chan<- agent.Event, event agent.Event) bool {
	select {
	case <-ctx.Done():
		return false
	case events <- event:
		return true
	}
}
