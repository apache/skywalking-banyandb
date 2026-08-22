// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Package codex integrates the Codex CLI with the TUI agent protocol.
package codex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/agent"
)

// The app-server reports turn progress as notifications. Each handler below folds one notification
// into the turn it belongs to, ignoring anything that names a turn this connection is not running.

func (appConnection *connection) handleNotification(ctx context.Context, method string, params json.RawMessage) {
	switch method {
	case "turn/started":
		appConnection.recordTurnStarted(ctx, params)
	case "item/agentMessage/delta":
		appConnection.recordMessageDelta(ctx, params)
	case "item/started", "item/completed":
		appConnection.validateItemNotification(ctx, params)
	case "turn/completed":
		appConnection.completeTurn(ctx, params)
	case "error":
		appConnection.recordErrorNotification(ctx, params)
	case "turn/diff/updated", "item/commandExecution/outputDelta", "item/fileChange/outputDelta":
		appConnection.failActiveUnsafe(ctx, fmt.Errorf("codex emitted forbidden notification %q", method))
	default:
		// Unknown non-request notifications are ignored for forward compatibility.
	}
}

func (appConnection *connection) recordTurnStarted(ctx context.Context, params json.RawMessage) {
	var notification struct {
		ThreadID string `json:"threadId"`
		Turn     struct {
			ID string `json:"id"`
		} `json:"turn"`
	}
	if unmarshalErr := json.Unmarshal(params, &notification); unmarshalErr != nil {
		appConnection.failActiveUnsafe(ctx, fmt.Errorf("invalid turn/started notification: %w", unmarshalErr))
		return
	}
	turn := appConnection.activeTurn(notification.ThreadID, notification.Turn.ID)
	if turn == nil {
		return
	}
	if setErr := turn.setID(notification.Turn.ID); setErr != nil {
		appConnection.failUnsafeTurn(ctx, turn, setErr)
	}
}

func (appConnection *connection) recordMessageDelta(ctx context.Context, params json.RawMessage) {
	var notification struct {
		Delta    string `json:"delta"`
		ThreadID string `json:"threadId"`
		TurnID   string `json:"turnId"`
	}
	if unmarshalErr := json.Unmarshal(params, &notification); unmarshalErr != nil {
		appConnection.failActiveUnsafe(ctx, fmt.Errorf("invalid agent message delta: %w", unmarshalErr))
		return
	}
	turn := appConnection.activeTurn(notification.ThreadID, notification.TurnID)
	if turn == nil {
		return
	}
	if setErr := turn.setID(notification.TurnID); setErr != nil {
		appConnection.failUnsafeTurn(ctx, turn, setErr)
		return
	}
	turn.appendMessage(notification.Delta)
	turn.emit(agent.Event{Kind: agent.EventKindMessageDelta, Message: notification.Delta, Origin: agent.EventOriginProvider})
}

func (appConnection *connection) validateItemNotification(ctx context.Context, params json.RawMessage) {
	var notification struct {
		Item struct {
			Type   string `json:"type"`
			Server string `json:"server"`
			Tool   string `json:"tool"`
		} `json:"item"`
		ThreadID string `json:"threadId"`
		TurnID   string `json:"turnId"`
	}
	if unmarshalErr := json.Unmarshal(params, &notification); unmarshalErr != nil {
		appConnection.failActiveUnsafe(ctx, fmt.Errorf("invalid item notification: %w", unmarshalErr))
		return
	}
	turn := appConnection.activeTurn(notification.ThreadID, notification.TurnID)
	if turn == nil {
		return
	}
	switch notification.Item.Type {
	case "userMessage", "agentMessage", "plan", "reasoning", "contextCompaction":
		return
	case "mcpToolCall":
		if notification.Item.Server == controlledMCPServerName && slices.Contains(controlledToolNames, notification.Item.Tool) {
			return
		}
		appConnection.failUnsafeTurn(ctx, turn, fmt.Errorf(
			"codex attempted non-allowlisted MCP tool %q from server %q",
			notification.Item.Tool,
			notification.Item.Server,
		))
	default:
		appConnection.failUnsafeTurn(ctx, turn, fmt.Errorf("codex attempted forbidden item type %q", notification.Item.Type))
	}
}

func (appConnection *connection) completeTurn(ctx context.Context, params json.RawMessage) {
	var notification struct {
		ThreadID string `json:"threadId"`
		Turn     struct {
			Error *struct {
				Message string `json:"message"`
			} `json:"error"`
			ID     string `json:"id"`
			Status string `json:"status"`
			Items  []struct {
				Text string `json:"text"`
				Type string `json:"type"`
			} `json:"items"`
		} `json:"turn"`
	}
	if unmarshalErr := json.Unmarshal(params, &notification); unmarshalErr != nil {
		appConnection.failActiveUnsafe(ctx, fmt.Errorf("invalid turn/completed notification: %w", unmarshalErr))
		return
	}
	turn := appConnection.activeTurn(notification.ThreadID, notification.Turn.ID)
	if turn == nil {
		return
	}
	switch notification.Turn.Status {
	case "completed":
		message := turn.messageText()
		if message == "" {
			for _, item := range notification.Turn.Items {
				if item.Type == "agentMessage" && strings.TrimSpace(item.Text) != "" {
					message = item.Text
				}
			}
		}
		turn.finish(agent.Event{Kind: agent.EventKindFinalResponse, Message: message, Origin: agent.EventOriginProvider})
	case "interrupted":
		turn.finish(agent.ErrorEvent(errors.New("codex turn interrupted")))
	case "failed":
		message := "codex turn failed"
		if notification.Turn.Error != nil && strings.TrimSpace(notification.Turn.Error.Message) != "" {
			message += ": " + notification.Turn.Error.Message
		}
		turn.finish(agent.ErrorEvent(errors.New(message)))
	default:
		appConnection.failUnsafeTurn(ctx, turn, fmt.Errorf("turn/completed returned invalid status %q", notification.Turn.Status))
		return
	}
	appConnection.clearTurn(turn)
}

func (appConnection *connection) recordErrorNotification(ctx context.Context, params json.RawMessage) {
	var notification struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
		Message string `json:"message"`
	}
	if unmarshalErr := json.Unmarshal(params, &notification); unmarshalErr != nil {
		appConnection.failActiveUnsafe(ctx, fmt.Errorf("invalid Codex error notification: %w", unmarshalErr))
		return
	}
	message := strings.TrimSpace(notification.Error.Message)
	if message == "" {
		message = strings.TrimSpace(notification.Message)
	}
	if message == "" {
		message = "codex reported an unknown error"
	}
	appConnection.failActive(errors.New(message))
}
