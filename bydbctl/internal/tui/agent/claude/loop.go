// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package claude

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/anthropics/anthropic-sdk-go"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/agent"
)

// maxToolRounds bounds the tool-use loop so a runaway model cannot drive the turn forever.
const maxToolRounds = 12

// pendingToolUse accumulates an in-flight tool_use block across streaming deltas.
type pendingToolUse struct {
	id        string
	name      string
	inputJSON strings.Builder
}

// roundResult captures the model output from one Messages streaming round.
type roundResult struct {
	text       string
	stopReason anthropic.StopReason
	toolUses   []*pendingToolUse
}

// runTurn drives the Anthropic Messages streaming agentic loop for one user turn.
// It emits only provider-side events: message_delta text, a final terminal response,
// or an error. Tool lifecycle events (tool_call/tool_result/candidate/approval) come
// from the bridge when InvokeTool drives ToolBridge.Call, so the runner merges them.
func (gateway *Gateway) runTurn(
	ctx context.Context,
	req agent.TurnRequest,
	events chan agent.Event,
) {
	defer close(events)
	parts, promptErr := agent.BuildBydbqlPromptParts(req)
	if promptErr != nil {
		gateway.emit(ctx, events, errorEvent(fmt.Errorf("failed to build Claude turn input: %w", promptErr)))
		return
	}
	tools := convertTools(gateway.config.Tools.Definitions())
	system := []anthropic.TextBlockParam{{Text: parts.System}}
	messages := []anthropic.MessageParam{
		anthropic.NewUserMessage(anthropic.NewTextBlock(parts.User)),
	}
	for round := 0; round < maxToolRounds; round++ {
		result, streamErr := gateway.streamRound(ctx, system, messages, tools, events)
		if streamErr != nil {
			gateway.emit(ctx, events, gateway.streamErrorEvent(ctx, streamErr))
			return
		}
		if result.stopReason == anthropic.StopReasonToolUse && len(result.toolUses) > 0 {
			assistantBlocks := gateway.assistantBlocks(result.text, result.toolUses)
			toolResults := gateway.dispatchToolCalls(ctx, result.toolUses)
			messages = append(messages, anthropic.NewAssistantMessage(assistantBlocks...))
			messages = append(messages, anthropic.NewUserMessage(toolResults...))
			continue
		}
		gateway.emit(ctx, events, agent.Event{
			Kind:    agent.EventKindFinalResponse,
			Message: result.text,
			Origin:  agent.EventOriginProvider,
		})
		return
	}
	gateway.emit(ctx, events, errorEvent(fmt.Errorf("claude turn exceeded %d tool rounds", maxToolRounds)))
}

// streamRound consumes one Messages streaming response and accumulates text plus
// pending tool_use blocks, emitting message_delta events as text arrives.
func (gateway *Gateway) streamRound(
	ctx context.Context,
	system []anthropic.TextBlockParam,
	messages []anthropic.MessageParam,
	tools []anthropic.ToolUnionParam,
	events chan agent.Event,
) (roundResult, error) {
	var result roundResult
	toolUseByIndex := make(map[int64]*pendingToolUse)
	var toolUseOrder []*pendingToolUse
	var textBuilder strings.Builder
	params := anthropic.MessageNewParams{
		Model:     anthropic.Model(gateway.config.Model),
		MaxTokens: gateway.config.MaxTokens,
		System:    system,
		Messages:  messages,
		Tools:     tools,
	}
	stream := gateway.newStream(ctx, params)
	for stream.Next() {
		if ctx.Err() != nil {
			_ = stream.Close()
			return result, ctx.Err()
		}
		switch variant := stream.Current().AsAny().(type) {
		case anthropic.ContentBlockDeltaEvent:
			delta := variant.Delta
			switch delta.Type {
			case "text_delta":
				textBuilder.WriteString(delta.Text)
				gateway.emit(ctx, events, agent.Event{
					Kind:    agent.EventKindMessageDelta,
					Message: delta.Text,
					Origin:  agent.EventOriginProvider,
				})
			case "input_json_delta":
				if toolUse, hasTool := toolUseByIndex[variant.Index]; hasTool {
					toolUse.inputJSON.WriteString(delta.PartialJSON)
				}
			}
		case anthropic.ContentBlockStartEvent:
			if variant.ContentBlock.Type == "tool_use" {
				toolUse := variant.ContentBlock.AsToolUse()
				pending := &pendingToolUse{id: toolUse.ID, name: toolUse.Name}
				toolUseByIndex[variant.Index] = pending
				toolUseOrder = append(toolUseOrder, pending)
			}
		case anthropic.MessageDeltaEvent:
			result.stopReason = variant.Delta.StopReason
		}
	}
	streamErr := stream.Err()
	_ = stream.Close()
	result.text = textBuilder.String()
	result.toolUses = toolUseOrder
	return result, streamErr
}

// assistantBlocks rebuilds the assistant message content to echo back into the next
// request, mirroring the text and tool_use blocks the model just produced.
func (gateway *Gateway) assistantBlocks(text string, toolUses []*pendingToolUse) []anthropic.ContentBlockParamUnion {
	var blocks []anthropic.ContentBlockParamUnion
	if strings.TrimSpace(text) != "" {
		blocks = append(blocks, anthropic.NewTextBlock(text))
	}
	for _, toolUse := range toolUses {
		rawInput := strings.TrimSpace(toolUse.inputJSON.String())
		if rawInput == "" {
			rawInput = "{}"
		}
		blocks = append(blocks, anthropic.NewToolUseBlock(toolUse.id, json.RawMessage(rawInput), toolUse.name))
	}
	return blocks
}

// dispatchToolCalls invokes each pending tool through the controlled bridge and wraps
// the result as a tool_result content block. The bridge emits its own tool_call and
// tool_result events; this only feeds the result back to the model.
func (gateway *Gateway) dispatchToolCalls(ctx context.Context, toolUses []*pendingToolUse) []anthropic.ContentBlockParamUnion {
	results := make([]anthropic.ContentBlockParamUnion, 0, len(toolUses))
	for _, toolUse := range toolUses {
		arguments := map[string]any{}
		if rawInput := strings.TrimSpace(toolUse.inputJSON.String()); rawInput != "" {
			if parseErr := json.Unmarshal([]byte(rawInput), &arguments); parseErr != nil {
				results = append(results, anthropic.NewToolResultBlock(toolUse.id, "invalid tool input: "+parseErr.Error(), true))
				continue
			}
		}
		content, toolErr := gateway.config.Tools.InvokeTool(ctx, toolUse.name, arguments)
		isError := toolErr != nil
		if isError && strings.TrimSpace(content) == "" {
			content = toolErr.Error()
		}
		results = append(results, anthropic.NewToolResultBlock(toolUse.id, content, isError))
	}
	return results
}

// streamErrorEvent converts a streaming failure into a provider error, distinguishing
// an interruption (context cancellation) from a real API error.
func (gateway *Gateway) streamErrorEvent(ctx context.Context, streamErr error) agent.Event {
	if ctx.Err() != nil {
		return errorEvent(fmt.Errorf("claude turn interrupted: %w", ctx.Err()))
	}
	return errorEvent(fmt.Errorf("claude stream failed: %w", streamErr))
}

// emit sends one event on the turn channel, yielding to the turn context so an
// interruption cannot block the streaming goroutine on a full buffer.
func (gateway *Gateway) emit(ctx context.Context, events chan agent.Event, event agent.Event) {
	if event.StartedAt.IsZero() {
		event.StartedAt = gateway.now()
	}
	select {
	case <-ctx.Done():
	case events <- event:
	}
}
