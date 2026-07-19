// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package codex

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/agent"
)

const (
	responseBufferSize     = 1
	eventBufferSize        = 64
	maxScannerBuffer       = 4 * 1024 * 1024
	maxStderrBytes         = 64 * 1024
	requestTimeout         = 30 * time.Second
	closeTimeout           = 2 * time.Second
	unsafeInterruptTimeout = 5 * time.Second
)

type connection struct {
	doneErr        error
	stdin          io.WriteCloser
	stdout         io.Reader
	stderr         io.Reader
	command        *exec.Cmd
	responses      map[string]chan rpcResponse
	done           chan struct{}
	turn           *turnState
	threadID       string
	localSessionID string
	nextRequestID  atomic.Uint64
	finishOnce     sync.Once
	responseMu     sync.Mutex
	stateMu        sync.Mutex
	turnMu         sync.Mutex
	writeMu        sync.Mutex
	closing        bool
}

type turnState struct {
	ctx        context.Context
	events     chan agent.Event
	idReady    chan struct{}
	done       chan struct{}
	threadID   string
	id         string
	message    strings.Builder
	idOnce     sync.Once
	finishOnce sync.Once
	unsafeOnce sync.Once
	messageMu  sync.Mutex
}

type rpcRequest struct {
	Params any    `json:"params,omitempty"`
	ID     string `json:"id"`
	Method string `json:"method"`
}

type rpcNotification struct {
	Params any    `json:"params,omitempty"`
	Method string `json:"method"`
}

type rpcError struct {
	Message string `json:"message"`
	Code    int    `json:"code"`
}

type incomingMessage struct {
	Error  *rpcError       `json:"error"`
	Method string          `json:"method"`
	ID     json.RawMessage `json:"id"`
	Params json.RawMessage `json:"params"`
	Result json.RawMessage `json:"result"`
}

type rpcResponse struct {
	err    error
	result json.RawMessage
}

type cappedWriter struct {
	buffer bytes.Buffer
	limit  int
}

func startConnection(lifecycleCtx context.Context, command string, args []string, workingDirectory string) (*connection, error) {
	appServerCmd := exec.Command(command, args...)
	appServerCmd.Dir = workingDirectory
	appServerCmd.Env = currentEnvironment()
	stdinPipe, stdinErr := appServerCmd.StdinPipe()
	if stdinErr != nil {
		return nil, fmt.Errorf("failed to open Codex app-server stdin: %w", stdinErr)
	}
	stdoutPipe, stdoutErr := appServerCmd.StdoutPipe()
	if stdoutErr != nil {
		return nil, fmt.Errorf("failed to open Codex app-server stdout: %w", stdoutErr)
	}
	stderrPipe, stderrErr := appServerCmd.StderrPipe()
	if stderrErr != nil {
		return nil, fmt.Errorf("failed to open Codex app-server stderr: %w", stderrErr)
	}
	if startErr := appServerCmd.Start(); startErr != nil {
		return nil, fmt.Errorf("failed to start Codex app-server: %w", startErr)
	}
	appConnection := &connection{
		command:   appServerCmd,
		stdin:     stdinPipe,
		stdout:    stdoutPipe,
		stderr:    stderrPipe,
		responses: make(map[string]chan rpcResponse),
		done:      make(chan struct{}),
	}
	go appConnection.readLoop(lifecycleCtx)
	return appConnection, nil
}

func (appConnection *connection) initialize(ctx context.Context) error {
	if _, requestErr := appConnection.request(ctx, "initialize", map[string]any{
		"clientInfo": map[string]string{
			"name":    "bydbctl",
			"title":   "Apache SkyWalking BanyanDB CLI",
			"version": "1",
		},
	}); requestErr != nil {
		return requestErr
	}
	return appConnection.notify("initialized", map[string]any{})
}

func (appConnection *connection) checkAccount(ctx context.Context) error {
	result, requestErr := appConnection.request(ctx, "account/read", map[string]any{})
	if requestErr != nil {
		return fmt.Errorf("failed to read Codex account: %w", requestErr)
	}
	var accountResult struct {
		Account            json.RawMessage `json:"account"`
		RequiresOpenAIAuth bool            `json:"requiresOpenaiAuth"`
	}
	if unmarshalErr := json.Unmarshal(result, &accountResult); unmarshalErr != nil {
		return fmt.Errorf("failed to parse Codex account: %w", unmarshalErr)
	}
	accountMissing := len(accountResult.Account) == 0 || string(accountResult.Account) == "null"
	if accountResult.RequiresOpenAIAuth && accountMissing {
		return errors.New("codex login is required; run `codex login` and try again")
	}
	return nil
}

func (appConnection *connection) startThread(ctx context.Context, workingDirectory, developerInstructions string) error {
	result, requestErr := appConnection.request(ctx, "thread/start", map[string]any{
		"cwd":                   workingDirectory,
		"approvalPolicy":        "never",
		"sandbox":               "read-only",
		"ephemeral":             true,
		"developerInstructions": developerInstructions,
	})
	if requestErr != nil {
		return requestErr
	}
	var threadResult struct {
		Thread struct {
			ID string `json:"id"`
		} `json:"thread"`
	}
	if unmarshalErr := json.Unmarshal(result, &threadResult); unmarshalErr != nil {
		return fmt.Errorf("failed to parse thread/start response: %w", unmarshalErr)
	}
	if strings.TrimSpace(threadResult.Thread.ID) == "" {
		return errors.New("thread/start returned no thread id")
	}
	appConnection.threadID = threadResult.Thread.ID
	return nil
}

func (appConnection *connection) validateMCPInventory(ctx context.Context) error {
	cursor := ""
	controlledFound := false
	seenServers := make(map[string]struct{})
	for {
		params := map[string]any{
			"threadId": appConnection.threadID,
			"detail":   "full",
			"limit":    100,
		}
		if cursor != "" {
			params["cursor"] = cursor
		}
		result, requestErr := appConnection.request(ctx, "mcpServerStatus/list", params)
		if requestErr != nil {
			return requestErr
		}
		var inventory struct {
			NextCursor string `json:"nextCursor"`
			Data       []struct {
				Tools             map[string]json.RawMessage `json:"tools"`
				Name              string                     `json:"name"`
				Resources         []json.RawMessage          `json:"resources"`
				ResourceTemplates []json.RawMessage          `json:"resourceTemplates"`
			} `json:"data"`
		}
		if unmarshalErr := json.Unmarshal(result, &inventory); unmarshalErr != nil {
			return fmt.Errorf("failed to parse MCP inventory: %w", unmarshalErr)
		}
		for _, server := range inventory.Data {
			if _, exists := seenServers[server.Name]; exists {
				return fmt.Errorf("duplicate MCP server %q in runtime inventory", server.Name)
			}
			seenServers[server.Name] = struct{}{}
			if server.Name == controlledMCPServerName {
				controlledFound = true
				if len(server.Resources) != 0 || len(server.ResourceTemplates) != 0 {
					return errors.New("controlled MCP server unexpectedly exposes resources")
				}
				toolNames := make([]string, 0, len(server.Tools))
				for toolName := range server.Tools {
					toolNames = append(toolNames, toolName)
				}
				if !equalStringSets(toolNames, controlledToolNames) {
					sort.Strings(toolNames)
					return fmt.Errorf("controlled MCP runtime tools do not match the allowlist: %s", strings.Join(toolNames, ", "))
				}
				continue
			}
			if len(server.Tools) != 0 || len(server.Resources) != 0 || len(server.ResourceTemplates) != 0 {
				return fmt.Errorf("uncontrolled MCP server %q exposes tools or resources", server.Name)
			}
		}
		cursor = strings.TrimSpace(inventory.NextCursor)
		if cursor == "" {
			break
		}
	}
	if !controlledFound {
		return fmt.Errorf("required MCP server %q is not ready", controlledMCPServerName)
	}
	return nil
}

func (appConnection *connection) send(ctx context.Context, req agent.TurnRequest) (<-chan agent.Event, error) {
	parts, promptErr := agent.BuildBydbqlPromptParts(req)
	if promptErr != nil {
		return nil, fmt.Errorf("failed to build Codex turn input: %w", promptErr)
	}
	turn := &turnState{
		events:   make(chan agent.Event, eventBufferSize),
		ctx:      ctx,
		threadID: appConnection.threadID,
		idReady:  make(chan struct{}),
		done:     make(chan struct{}),
	}
	if setErr := appConnection.setTurn(turn); setErr != nil {
		return nil, setErr
	}
	go func() {
		select {
		case <-ctx.Done():
			interruptCtx, cancelInterrupt := context.WithTimeout(context.WithoutCancel(ctx), unsafeInterruptTimeout)
			defer cancelInterrupt()
			_ = appConnection.interruptState(interruptCtx, turn)
		case <-turn.done:
		}
	}()
	go func(parentCtx context.Context) {
		startCtx, cancelStart := context.WithTimeout(context.WithoutCancel(parentCtx), requestTimeout)
		defer cancelStart()
		result, requestErr := appConnection.request(startCtx, "turn/start", map[string]any{
			"threadId": appConnection.threadID,
			"input": []map[string]string{{
				"type": "text",
				"text": parts.User,
			}},
		})
		if requestErr != nil {
			appConnection.failTurn(turn, fmt.Errorf("failed to start Codex turn: %w", requestErr))
			return
		}
		var turnResult struct {
			Turn struct {
				ID string `json:"id"`
			} `json:"turn"`
		}
		if unmarshalErr := json.Unmarshal(result, &turnResult); unmarshalErr != nil {
			appConnection.failTurn(turn, fmt.Errorf("failed to parse turn/start response: %w", unmarshalErr))
			return
		}
		if strings.TrimSpace(turnResult.Turn.ID) == "" {
			appConnection.failTurn(turn, errors.New("turn/start returned no turn id"))
			return
		}
		if setErr := turn.setID(turnResult.Turn.ID); setErr != nil {
			appConnection.failUnsafeTurn(startCtx, turn, setErr)
		}
	}(ctx)
	return turn.events, nil
}

func (appConnection *connection) interrupt(ctx context.Context) error {
	appConnection.turnMu.Lock()
	turn := appConnection.turn
	appConnection.turnMu.Unlock()
	if turn == nil {
		return nil
	}
	return appConnection.interruptState(ctx, turn)
}

func (appConnection *connection) interruptState(ctx context.Context, turn *turnState) error {
	turnID, idErr := turn.waitID(ctx)
	if idErr != nil {
		select {
		case <-turn.done:
			return nil
		default:
			return idErr
		}
	}
	_, requestErr := appConnection.request(ctx, "turn/interrupt", map[string]string{
		"threadId": turn.threadID,
		"turnId":   turnID,
	})
	if requestErr != nil {
		return fmt.Errorf("turn/interrupt failed: %w", requestErr)
	}
	return nil
}

func (appConnection *connection) request(ctx context.Context, method string, params any) (json.RawMessage, error) {
	requestID := strconv.FormatUint(appConnection.nextRequestID.Add(1), 10)
	responseCh := make(chan rpcResponse, responseBufferSize)
	appConnection.responseMu.Lock()
	appConnection.responses[requestID] = responseCh
	appConnection.responseMu.Unlock()
	requestBytes, marshalErr := json.Marshal(rpcRequest{ID: requestID, Method: method, Params: params})
	if marshalErr != nil {
		appConnection.deleteResponse(requestID)
		return nil, fmt.Errorf("failed to marshal Codex request %s: %w", method, marshalErr)
	}
	if writeErr := appConnection.writeLine(requestBytes); writeErr != nil {
		appConnection.deleteResponse(requestID)
		return nil, writeErr
	}
	select {
	case <-ctx.Done():
		appConnection.deleteResponse(requestID)
		return nil, ctx.Err()
	case <-appConnection.done:
		appConnection.deleteResponse(requestID)
		return nil, appConnection.processError()
	case response := <-responseCh:
		if response.err != nil {
			return nil, fmt.Errorf("codex request %s failed: %w", method, response.err)
		}
		return response.result, nil
	}
}

func (appConnection *connection) notify(method string, params any) error {
	notificationBytes, marshalErr := json.Marshal(rpcNotification{Method: method, Params: params})
	if marshalErr != nil {
		return fmt.Errorf("failed to marshal Codex notification %s: %w", method, marshalErr)
	}
	return appConnection.writeLine(notificationBytes)
}

func (appConnection *connection) writeLine(line []byte) error {
	appConnection.writeMu.Lock()
	defer appConnection.writeMu.Unlock()
	select {
	case <-appConnection.done:
		return appConnection.processError()
	default:
	}
	if _, writeErr := appConnection.stdin.Write(append(line, '\n')); writeErr != nil {
		return fmt.Errorf("failed to write Codex app-server message: %w", writeErr)
	}
	return nil
}

func (appConnection *connection) readLoop(ctx context.Context) {
	stderrOutput := &cappedWriter{limit: maxStderrBytes}
	stderrDone := make(chan error, 1)
	go func() {
		_, copyErr := io.Copy(stderrOutput, appConnection.stderr)
		stderrDone <- copyErr
	}()
	scanner := bufio.NewScanner(appConnection.stdout)
	scanner.Buffer(make([]byte, 0, 64*1024), maxScannerBuffer)
	var protocolErr error
	for scanner.Scan() {
		if handleErr := appConnection.handleLine(ctx, scanner.Bytes()); handleErr != nil {
			protocolErr = handleErr
			if appConnection.command.Process != nil {
				_ = appConnection.command.Process.Kill()
			}
			break
		}
	}
	if scanErr := scanner.Err(); scanErr != nil && protocolErr == nil {
		protocolErr = fmt.Errorf("failed to read Codex app-server output: %w", scanErr)
	}
	waitErr := appConnection.command.Wait()
	stderrErr := <-stderrDone
	appConnection.stateMu.Lock()
	closing := appConnection.closing
	appConnection.stateMu.Unlock()
	processErr := protocolErr
	if processErr == nil && stderrErr != nil {
		processErr = fmt.Errorf("failed to read Codex app-server stderr: %w", stderrErr)
	}
	if processErr == nil && !closing {
		message := strings.TrimSpace(stderrOutput.String())
		if message == "" && waitErr != nil {
			message = waitErr.Error()
		}
		if message == "" {
			message = "process exited"
		}
		processErr = fmt.Errorf("codex app-server exited unexpectedly: %s", message)
	}
	if closing && processErr == nil {
		processErr = errors.New("codex app-server closed")
	}
	appConnection.finish(processErr)
}

func (appConnection *connection) handleLine(ctx context.Context, line []byte) error {
	var message incomingMessage
	if unmarshalErr := json.Unmarshal(line, &message); unmarshalErr != nil {
		return fmt.Errorf("codex app-server emitted invalid JSON: %w", unmarshalErr)
	}
	hasID := len(message.ID) > 0 && string(message.ID) != "null"
	if hasID && strings.TrimSpace(message.Method) != "" {
		appConnection.rejectServerRequest(ctx, message.ID, message.Method)
		return nil
	}
	if hasID {
		requestID, idErr := responseID(message.ID)
		if idErr != nil {
			return idErr
		}
		if message.Error != nil {
			appConnection.completeResponse(requestID, rpcResponse{err: errors.New(message.Error.Message)})
			return nil
		}
		appConnection.completeResponse(requestID, rpcResponse{result: message.Result})
		return nil
	}
	if strings.TrimSpace(message.Method) == "" {
		return errors.New("codex app-server emitted a message without an id or method")
	}
	appConnection.handleNotification(ctx, message.Method, message.Params)
	return nil
}

func (appConnection *connection) rejectServerRequest(ctx context.Context, id json.RawMessage, method string) {
	responseBytes, marshalErr := json.Marshal(map[string]any{
		"id": id,
		"error": rpcError{
			Code:    -32601,
			Message: "server request denied by bydbctl",
		},
	})
	if marshalErr == nil {
		_ = appConnection.writeLine(responseBytes)
	}
	appConnection.turnMu.Lock()
	turn := appConnection.turn
	appConnection.turnMu.Unlock()
	if turn != nil {
		appConnection.failUnsafeTurn(ctx, turn, fmt.Errorf("codex attempted forbidden server request %q", method))
	}
}

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
		if notification.Item.Server == controlledMCPServerName && containsString(controlledToolNames, notification.Item.Tool) {
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
		turn.finish(errorEvent(errors.New("codex turn interrupted")))
	case "failed":
		message := "codex turn failed"
		if notification.Turn.Error != nil && strings.TrimSpace(notification.Turn.Error.Message) != "" {
			message += ": " + notification.Turn.Error.Message
		}
		turn.finish(errorEvent(errors.New(message)))
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

func (appConnection *connection) activeTurn(threadID, turnID string) *turnState {
	appConnection.turnMu.Lock()
	defer appConnection.turnMu.Unlock()
	turn := appConnection.turn
	if turn == nil || threadID != appConnection.threadID {
		return nil
	}
	currentTurnID := turn.currentID()
	if currentTurnID != "" && turnID != "" && currentTurnID != turnID {
		return nil
	}
	return turn
}

func (appConnection *connection) setTurn(turn *turnState) error {
	appConnection.turnMu.Lock()
	defer appConnection.turnMu.Unlock()
	if appConnection.turn != nil {
		return errors.New("codex thread already has an active turn")
	}
	appConnection.turn = turn
	return nil
}

func (appConnection *connection) clearTurn(turn *turnState) {
	appConnection.turnMu.Lock()
	defer appConnection.turnMu.Unlock()
	if appConnection.turn == turn {
		appConnection.turn = nil
	}
}

func (appConnection *connection) failTurn(turn *turnState, turnErr error) {
	turn.finish(errorEvent(turnErr))
	appConnection.clearTurn(turn)
}

func (appConnection *connection) failActive(turnErr error) {
	appConnection.turnMu.Lock()
	turn := appConnection.turn
	appConnection.turnMu.Unlock()
	if turn != nil {
		appConnection.failTurn(turn, turnErr)
	}
}

func (appConnection *connection) failActiveUnsafe(ctx context.Context, turnErr error) {
	appConnection.turnMu.Lock()
	turn := appConnection.turn
	appConnection.turnMu.Unlock()
	if turn != nil {
		appConnection.failUnsafeTurn(ctx, turn, turnErr)
	}
}

func (appConnection *connection) failUnsafeTurn(ctx context.Context, turn *turnState, turnErr error) {
	turn.unsafeOnce.Do(func() {
		turn.finish(errorEvent(turnErr))
		appConnection.clearTurn(turn)
		go func() {
			interruptCtx, cancelInterrupt := context.WithTimeout(context.WithoutCancel(ctx), unsafeInterruptTimeout)
			defer cancelInterrupt()
			_ = appConnection.interruptState(interruptCtx, turn)
		}()
	})
}

func (appConnection *connection) completeResponse(requestID string, response rpcResponse) {
	appConnection.responseMu.Lock()
	responseCh := appConnection.responses[requestID]
	delete(appConnection.responses, requestID)
	appConnection.responseMu.Unlock()
	if responseCh != nil {
		responseCh <- response
		close(responseCh)
	}
}

func (appConnection *connection) deleteResponse(requestID string) {
	appConnection.responseMu.Lock()
	delete(appConnection.responses, requestID)
	appConnection.responseMu.Unlock()
}

func (appConnection *connection) finish(processErr error) {
	appConnection.finishOnce.Do(func() {
		if processErr == nil {
			processErr = errors.New("codex app-server stopped")
		}
		appConnection.stateMu.Lock()
		appConnection.doneErr = processErr
		appConnection.stateMu.Unlock()
		appConnection.responseMu.Lock()
		for requestID, responseCh := range appConnection.responses {
			delete(appConnection.responses, requestID)
			responseCh <- rpcResponse{err: processErr}
			close(responseCh)
		}
		appConnection.responseMu.Unlock()
		appConnection.failActive(processErr)
		close(appConnection.done)
	})
}

func (appConnection *connection) processError() error {
	appConnection.stateMu.Lock()
	defer appConnection.stateMu.Unlock()
	if appConnection.doneErr == nil {
		return errors.New("codex app-server stopped")
	}
	return appConnection.doneErr
}

func (appConnection *connection) close() error {
	appConnection.stateMu.Lock()
	appConnection.closing = true
	appConnection.stateMu.Unlock()
	_ = appConnection.stdin.Close()
	timer := time.NewTimer(closeTimeout)
	defer timer.Stop()
	select {
	case <-appConnection.done:
		return nil
	case <-timer.C:
	}
	if appConnection.command.Process != nil {
		if killErr := appConnection.command.Process.Kill(); killErr != nil {
			return fmt.Errorf("failed to stop Codex app-server: %w", killErr)
		}
	}
	killTimer := time.NewTimer(closeTimeout)
	defer killTimer.Stop()
	select {
	case <-appConnection.done:
		return nil
	case <-killTimer.C:
		return errors.New("timed out waiting for Codex app-server to exit")
	}
}

func (turn *turnState) setID(turnID string) error {
	turnID = strings.TrimSpace(turnID)
	if turnID == "" {
		return errors.New("codex turn id is empty")
	}
	var setErr error
	turn.messageMu.Lock()
	if turn.id != "" && turn.id != turnID {
		setErr = fmt.Errorf("codex changed active turn id from %q to %q", turn.id, turnID)
	} else {
		turn.id = turnID
	}
	turn.messageMu.Unlock()
	if setErr == nil {
		turn.idOnce.Do(func() { close(turn.idReady) })
	}
	return setErr
}

func (turn *turnState) currentID() string {
	turn.messageMu.Lock()
	defer turn.messageMu.Unlock()
	return turn.id
}

func (turn *turnState) waitID(ctx context.Context) (string, error) {
	select {
	case <-turn.idReady:
		return turn.currentID(), nil
	case <-turn.done:
		if turnID := turn.currentID(); turnID != "" {
			return turnID, nil
		}
		return "", errors.New("codex turn ended before returning an id")
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func (turn *turnState) appendMessage(delta string) {
	turn.messageMu.Lock()
	turn.message.WriteString(delta)
	turn.messageMu.Unlock()
}

func (turn *turnState) messageText() string {
	turn.messageMu.Lock()
	defer turn.messageMu.Unlock()
	return strings.TrimSpace(turn.message.String())
}

func (turn *turnState) emit(event agent.Event) {
	select {
	case turn.events <- event:
	case <-turn.ctx.Done():
	case <-turn.done:
	}
}

func (turn *turnState) finish(event agent.Event) {
	turn.finishOnce.Do(func() {
		turn.emit(event)
		close(turn.done)
		close(turn.events)
	})
}

func (writer *cappedWriter) Write(content []byte) (int, error) {
	remaining := writer.limit - writer.buffer.Len()
	if remaining > 0 {
		writeLength := len(content)
		if writeLength > remaining {
			writeLength = remaining
		}
		_, _ = writer.buffer.Write(content[:writeLength])
	}
	return len(content), nil
}

func (writer *cappedWriter) String() string {
	return writer.buffer.String()
}

func responseID(rawID json.RawMessage) (string, error) {
	var stringID string
	if unmarshalErr := json.Unmarshal(rawID, &stringID); unmarshalErr == nil {
		return stringID, nil
	}
	return "", fmt.Errorf("codex app-server returned a non-string response id: %s", string(rawID))
}

func errorEvent(eventErr error) agent.Event {
	return agent.Event{
		Kind:    agent.EventKindError,
		Message: eventErr.Error(),
		Origin:  agent.EventOriginProvider,
		Err:     eventErr,
	}
}

func containsString(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}
