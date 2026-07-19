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

// Package acp provides a stdio JSON-RPC gateway for ACP-compatible agent processes.
package acp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/agent"
)

const (
	acpProtocolVersion = 1
	responseBufferSize = 1
	eventBufferSize    = 16
	maxScannerBuffer   = 4 * 1024 * 1024
)

// Gateway talks to an external ACP-compatible process over stdio JSON-RPC.
type Gateway struct {
	now              func() time.Time
	command          string
	workingDirectory string
	args             []string
	mcpServers       any
	mu               sync.Mutex
	sessions         map[string]*connection
}

// MaintainsConversationHistory reports that an ACP session retains prior turns.
func (gateway *Gateway) MaintainsConversationHistory() bool {
	return true
}

// NewGateway creates an ACP stdio gateway.
func NewGateway(command string, args ...string) *Gateway {
	return &Gateway{
		now:      time.Now,
		command:  command,
		args:     append([]string(nil), args...),
		sessions: make(map[string]*connection),
	}
}

// WithWorkingDirectory sets the process working directory.
func (gateway *Gateway) WithWorkingDirectory(workingDirectory string) *Gateway {
	gateway.workingDirectory = workingDirectory
	return gateway
}

// WithMCPServers sets MCP servers advertised to the ACP session.
func (gateway *Gateway) WithMCPServers(mcpServers any) *Gateway {
	gateway.mcpServers = mcpServers
	return gateway
}

// Start starts an ACP process, initializes it, and creates an ACP session.
func (gateway *Gateway) Start(ctx context.Context, req agent.StartRequest) (agent.Session, error) {
	if strings.TrimSpace(gateway.command) == "" {
		return agent.Session{}, fmt.Errorf("acp command is required")
	}
	acpConnection, connectionErr := gateway.startConnection(ctx)
	if connectionErr != nil {
		return agent.Session{}, connectionErr
	}
	if initializeErr := acpConnection.initialize(ctx); initializeErr != nil {
		acpConnection.close()
		return agent.Session{}, fmt.Errorf("failed to initialize acp process: %w", initializeErr)
	}
	acpSessionID, sessionErr := acpConnection.newSession(ctx, gateway.workingDirectory, gateway.mcpServers)
	if sessionErr != nil {
		acpConnection.close()
		return agent.Session{}, fmt.Errorf("failed to create acp session: %w", sessionErr)
	}
	localSessionID := "acp-" + uuid.NewString()
	acpConnection.sessionID = acpSessionID
	gateway.mu.Lock()
	gateway.sessions[localSessionID] = acpConnection
	gateway.mu.Unlock()
	return agent.Session{
		ID:        localSessionID,
		Provider:  req.Provider,
		StartedAt: gateway.now(),
	}, nil
}

// Send sends a prompt turn to an ACP session and streams normalized session updates.
func (gateway *Gateway) Send(ctx context.Context, sessionID string, req agent.TurnRequest) (<-chan agent.Event, error) {
	acpConnection, lookupErr := gateway.connection(sessionID)
	if lookupErr != nil {
		return nil, lookupErr
	}
	prompt, promptErr := buildPrompt(req)
	if promptErr != nil {
		return nil, fmt.Errorf("failed to build acp prompt: %w", promptErr)
	}
	events := make(chan agent.Event, eventBufferSize)
	turn := &turnState{events: events}
	if setErr := acpConnection.setTurn(turn); setErr != nil {
		return nil, setErr
	}
	go func() {
		defer close(events)
		defer acpConnection.clearTurn(turn)
		_, promptErr := acpConnection.request(ctx, "session/prompt", map[string]any{
			"sessionId": acpConnection.sessionID,
			"prompt": []map[string]string{
				{
					"type": "text",
					"text": prompt,
				},
			},
		})
		if promptErr != nil {
			_ = send(ctx, events, agent.Event{
				Kind:    agent.EventKindError,
				Message: promptErr.Error(),
				Err:     promptErr,
			})
			return
		}
		if message := turn.message(); strings.TrimSpace(message) != "" {
			_ = send(ctx, events, agent.Event{
				Kind:    agent.EventKindFinalResponse,
				Message: message,
			})
		}
	}()
	return events, nil
}

// Stop stops an ACP session.
func (gateway *Gateway) Stop(_ context.Context, sessionID string) error {
	gateway.mu.Lock()
	acpConnection := gateway.sessions[sessionID]
	delete(gateway.sessions, sessionID)
	gateway.mu.Unlock()
	if acpConnection == nil {
		return nil
	}
	acpConnection.close()
	return nil
}

func (gateway *Gateway) startConnection(ctx context.Context) (*connection, error) {
	command := exec.CommandContext(ctx, gateway.command, gateway.args...)
	if gateway.workingDirectory != "" {
		command.Dir = gateway.workingDirectory
	}
	stdinPipe, stdinErr := command.StdinPipe()
	if stdinErr != nil {
		return nil, fmt.Errorf("failed to open acp stdin: %w", stdinErr)
	}
	stdoutPipe, stdoutErr := command.StdoutPipe()
	if stdoutErr != nil {
		return nil, fmt.Errorf("failed to open acp stdout: %w", stdoutErr)
	}
	stderrPipe, stderrErr := command.StderrPipe()
	if stderrErr != nil {
		return nil, fmt.Errorf("failed to open acp stderr: %w", stderrErr)
	}
	if startErr := command.Start(); startErr != nil {
		return nil, fmt.Errorf("failed to start acp process: %w", startErr)
	}
	acpConnection := &connection{
		command:   command,
		stdin:     stdinPipe,
		stdout:    stdoutPipe,
		stderr:    stderrPipe,
		done:      make(chan error, 1),
		responses: make(map[string]chan rpcResponse),
	}
	go acpConnection.readLoop()
	return acpConnection, nil
}

func (gateway *Gateway) connection(sessionID string) (*connection, error) {
	gateway.mu.Lock()
	defer gateway.mu.Unlock()
	acpConnection := gateway.sessions[sessionID]
	if acpConnection == nil {
		return nil, fmt.Errorf("unknown acp session %q", sessionID)
	}
	return acpConnection, nil
}

type connection struct {
	command   *exec.Cmd
	stdin     io.WriteCloser
	stdout    io.Reader
	stderr    io.Reader
	sessionID string

	writeMu    sync.Mutex
	responseMu sync.Mutex
	responses  map[string]chan rpcResponse

	turnMu sync.Mutex
	turn   *turnState

	done      chan error
	closeOnce sync.Once
}

type turnState struct {
	events        chan<- agent.Event
	messageBuffer bytes.Buffer
	mu            sync.Mutex
}

func (turn *turnState) record(event agent.Event) {
	if event.Kind != agent.EventKindMessageDelta && event.Kind != agent.EventKindFinalResponse {
		return
	}
	message := strings.TrimSpace(event.Message)
	if message == "" {
		message = strings.TrimSpace(event.Candidate)
	}
	if message == "" {
		return
	}
	turn.mu.Lock()
	defer turn.mu.Unlock()
	if turn.messageBuffer.Len() > 0 {
		turn.messageBuffer.WriteByte('\n')
	}
	turn.messageBuffer.WriteString(message)
}

func (turn *turnState) message() string {
	turn.mu.Lock()
	defer turn.mu.Unlock()
	return strings.TrimSpace(turn.messageBuffer.String())
}

type rpcRequest struct {
	Params  any    `json:"params,omitempty"`
	JSONRPC string `json:"jsonrpc"`
	ID      string `json:"id"`
	Method  string `json:"method"`
}

type rpcResponse struct {
	raw map[string]any
	err error
}

type stderrResult struct {
	err  error
	text string
}

func (acpConnection *connection) initialize(ctx context.Context) error {
	_, requestErr := acpConnection.request(ctx, "initialize", map[string]any{
		"protocolVersion": acpProtocolVersion,
		"clientInfo": map[string]any{
			"name":    "bydbctl-agent",
			"version": "dev",
		},
		"clientCapabilities": map[string]any{},
	})
	return requestErr
}

func (acpConnection *connection) newSession(ctx context.Context, workingDirectory string, mcpServers any) (string, error) {
	cwd := workingDirectory
	if strings.TrimSpace(cwd) == "" {
		currentDirectory, wdErr := os.Getwd()
		if wdErr != nil {
			return "", fmt.Errorf("failed to get working directory: %w", wdErr)
		}
		cwd = currentDirectory
	}
	servers := mcpServers
	if servers == nil {
		servers = []any{}
	}
	params := map[string]any{
		"cwd":        cwd,
		"mcpServers": servers,
	}
	rawResponse, requestErr := acpConnection.request(ctx, "session/new", params)
	if requestErr != nil {
		return "", requestErr
	}
	result := mapValue(rawResponse, "result")
	sessionID := stringValue(result, "sessionId", "session_id", "id")
	if sessionID == "" {
		return "", fmt.Errorf("acp session/new returned no session id")
	}
	return sessionID, nil
}

func (acpConnection *connection) request(ctx context.Context, method string, params any) (map[string]any, error) {
	requestID := uuid.NewString()
	responseCh := make(chan rpcResponse, responseBufferSize)
	acpConnection.responseMu.Lock()
	acpConnection.responses[requestID] = responseCh
	acpConnection.responseMu.Unlock()
	requestBytes, marshalErr := json.Marshal(rpcRequest{
		JSONRPC: "2.0",
		ID:      requestID,
		Method:  method,
		Params:  params,
	})
	if marshalErr != nil {
		acpConnection.deleteResponse(requestID)
		return nil, fmt.Errorf("failed to marshal acp request: %w", marshalErr)
	}
	if writeErr := acpConnection.writeLine(requestBytes); writeErr != nil {
		acpConnection.deleteResponse(requestID)
		return nil, writeErr
	}
	select {
	case <-ctx.Done():
		acpConnection.deleteResponse(requestID)
		return nil, ctx.Err()
	case doneErr := <-acpConnection.done:
		acpConnection.deleteResponse(requestID)
		if doneErr == nil {
			doneErr = fmt.Errorf("acp process stopped")
		}
		return nil, doneErr
	case response := <-responseCh:
		if response.err != nil {
			return nil, response.err
		}
		if errValue, ok := response.raw["error"]; ok {
			message := errorMessage(errValue)
			return nil, fmt.Errorf("acp request %s failed: %s", method, message)
		}
		return response.raw, nil
	}
}

func (acpConnection *connection) writeLine(line []byte) error {
	acpConnection.writeMu.Lock()
	defer acpConnection.writeMu.Unlock()
	if _, writeErr := acpConnection.stdin.Write(append(line, '\n')); writeErr != nil {
		return fmt.Errorf("failed to write acp request: %w", writeErr)
	}
	return nil
}

func (acpConnection *connection) respond(id any, result any) {
	if id == nil {
		return
	}
	responseBytes, marshalErr := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"result":  result,
	})
	if marshalErr != nil {
		return
	}
	_ = acpConnection.writeLine(responseBytes)
}

func (acpConnection *connection) readLoop() {
	stderrCh := make(chan stderrResult, 1)
	go func() {
		stderrBytes, readErr := io.ReadAll(acpConnection.stderr)
		stderrCh <- stderrResult{
			err:  readErr,
			text: strings.TrimSpace(string(stderrBytes)),
		}
	}()
	scanner := bufio.NewScanner(acpConnection.stdout)
	scanner.Buffer(make([]byte, 0, 64*1024), maxScannerBuffer)
	for scanner.Scan() {
		acpConnection.handleLine(scanner.Bytes())
	}
	if scanErr := scanner.Err(); scanErr != nil {
		acpConnection.finish(fmt.Errorf("failed to read acp output: %w", scanErr))
		return
	}
	waitErr := acpConnection.command.Wait()
	stderrData := <-stderrCh
	if stderrData.err != nil {
		acpConnection.finish(fmt.Errorf("failed to read acp stderr: %w", stderrData.err))
		return
	}
	if waitErr != nil {
		message := stderrData.text
		if message == "" {
			message = waitErr.Error()
		}
		acpConnection.finish(fmt.Errorf("%s", message))
		return
	}
	acpConnection.finish(nil)
}

func (acpConnection *connection) handleLine(line []byte) {
	var raw map[string]any
	if unmarshalErr := json.Unmarshal(line, &raw); unmarshalErr != nil {
		acpConnection.emit(agent.Event{
			Kind:    agent.EventKindMessageDelta,
			Message: strings.TrimSpace(string(line)),
		})
		return
	}
	if idValue, ok := raw["id"]; ok {
		method := strings.ToLower(stringValue(raw, "method"))
		if method == "session/request_permission" {
			params := mapValue(raw, "params")
			acpConnection.emit(NormalizeEvent(line))
			acpConnection.respond(idValue, permissionDecision(params))
			return
		}
		if _, hasResult := raw["result"]; hasResult {
			acpConnection.completeResponse(idString(idValue), rpcResponse{raw: raw})
			return
		}
		if errValue, hasError := raw["error"]; hasError {
			acpConnection.completeResponse(idString(idValue), rpcResponse{
				err: fmt.Errorf("%s", errorMessage(errValue)),
				raw: raw,
			})
			return
		}
	}
	acpConnection.emit(NormalizeEvent(line))
}

func (acpConnection *connection) emit(event agent.Event) {
	if event.Kind == "" {
		return
	}
	acpConnection.turnMu.Lock()
	turn := acpConnection.turn
	acpConnection.turnMu.Unlock()
	if turn == nil {
		return
	}
	turn.record(event)
	_ = send(context.Background(), turn.events, event)
}

func (acpConnection *connection) completeResponse(id string, response rpcResponse) {
	if id == "" {
		return
	}
	acpConnection.responseMu.Lock()
	responseCh := acpConnection.responses[id]
	delete(acpConnection.responses, id)
	acpConnection.responseMu.Unlock()
	if responseCh != nil {
		responseCh <- response
		close(responseCh)
	}
}

func (acpConnection *connection) deleteResponse(id string) {
	acpConnection.responseMu.Lock()
	delete(acpConnection.responses, id)
	acpConnection.responseMu.Unlock()
}

func (acpConnection *connection) setTurn(turn *turnState) error {
	acpConnection.turnMu.Lock()
	defer acpConnection.turnMu.Unlock()
	if acpConnection.turn != nil {
		return fmt.Errorf("acp session already has an active turn")
	}
	acpConnection.turn = turn
	return nil
}

func (acpConnection *connection) clearTurn(turn *turnState) {
	acpConnection.turnMu.Lock()
	defer acpConnection.turnMu.Unlock()
	if acpConnection.turn == turn {
		acpConnection.turn = nil
	}
}

func (acpConnection *connection) finish(processErr error) {
	acpConnection.closeOnce.Do(func() {
		acpConnection.responseMu.Lock()
		for requestID, responseCh := range acpConnection.responses {
			delete(acpConnection.responses, requestID)
			responseCh <- rpcResponse{err: processErr}
			close(responseCh)
		}
		acpConnection.responseMu.Unlock()
		acpConnection.done <- processErr
		close(acpConnection.done)
	})
}

func (acpConnection *connection) close() {
	acpConnection.closeOnce.Do(func() {
		_ = acpConnection.stdin.Close()
		if acpConnection.command.Process != nil {
			_ = acpConnection.command.Process.Kill()
		}
		acpConnection.done <- fmt.Errorf("acp process stopped")
		close(acpConnection.done)
	})
}

// NormalizeEvent converts a JSON-RPC notification or response line into an AgentEvent.
func NormalizeEvent(line []byte) agent.Event {
	var raw map[string]any
	if unmarshalErr := json.Unmarshal(line, &raw); unmarshalErr != nil {
		message := strings.TrimSpace(string(line))
		if message == "" {
			return agent.Event{}
		}
		return agent.Event{
			Kind:    agent.EventKindMessageDelta,
			Message: message,
		}
	}
	if errValue, ok := raw["error"]; ok {
		message := errorMessage(errValue)
		return agent.Event{
			Kind:    agent.EventKindError,
			Message: message,
			Err:     fmt.Errorf("%s", message),
		}
	}
	method := strings.ToLower(stringValue(raw, "method", "type", "kind", "event"))
	params := mapValue(raw, "params", "result", "data")
	if method == "session/update" {
		return normalizeSessionUpdate(params)
	}
	message := stringValue(params, "message", "delta", "content", "text")
	candidate := stringValue(params, "candidate", "bydbql", "query", "final")
	switch {
	case strings.Contains(method, "permission"):
		permissionMessage := fallbackMessage(message, "ACP permission request")
		if isControlledToolPermission(params) {
			permissionMessage = "controlled bydbctl tool permission granted"
		} else {
			permissionMessage = "ACP permission request denied by bydbctl workflow"
		}
		return agent.Event{
			Kind:       agent.EventKindPermissionRequest,
			Message:    permissionMessage,
			Permission: message,
		}
	case strings.Contains(method, "plan"):
		return agent.Event{
			Kind:    agent.EventKindPlanUpdate,
			Message: message,
		}
	case strings.Contains(method, "final") || candidate != "":
		return agent.Event{
			Kind:      agent.EventKindFinalResponse,
			Message:   message,
			Candidate: candidate,
		}
	default:
		return agent.Event{
			Kind:    agent.EventKindMessageDelta,
			Message: message,
		}
	}
}

func normalizeSessionUpdate(params map[string]any) agent.Event {
	update := mapValue(params, "update")
	updateType := strings.ToLower(stringValue(update, "sessionUpdate", "type", "kind"))
	switch updateType {
	case "agent_message_chunk":
		return agent.Event{
			Kind:    agent.EventKindMessageDelta,
			Message: contentText(mapValue(update, "content")),
		}
	case "plan":
		return agent.Event{
			Kind:    agent.EventKindPlanUpdate,
			Message: planMessage(update),
		}
	case "clarification", "question":
		return agent.Event{
			Kind:    agent.EventKindClarification,
			Message: fallbackMessage(contentText(mapValue(update, "content")), stringValue(update, "message", "text")),
		}
	case "tool_call", "tool_call_update":
		// Tool lifecycle is reported by the bydbctl tool bridge; ACP progress updates are noisy.
		return agent.Event{}
	case "available_commands_update":
		return agent.Event{
			Kind:    agent.EventKindPlanUpdate,
			Message: "available commands updated",
		}
	default:
		message := stringValue(update, "message", "text")
		return agent.Event{
			Kind:    agent.EventKindMessageDelta,
			Message: message,
		}
	}
}

func buildPrompt(req agent.TurnRequest) (string, error) {
	return agent.BuildBydbqlPrompt(req)
}

func permissionDecision(params map[string]any) map[string]any {
	options, _ := params["options"].([]any)
	if isControlledToolPermission(params) {
		if optionID := firstAllowOption(options); optionID != "" {
			return selectedPermissionOutcome(optionID)
		}
	}
	for _, optionValue := range options {
		option, optionOK := optionValue.(map[string]any)
		if !optionOK {
			continue
		}
		optionID := stringValue(option, "optionId", "option_id", "id")
		optionKind := strings.ToLower(stringValue(option, "kind", "type", "name"))
		if optionID != "" && strings.Contains(optionKind, "reject") {
			return selectedPermissionOutcome(optionID)
		}
	}
	return cancelledPermissionOutcome()
}

func selectedPermissionOutcome(optionID string) map[string]any {
	return map[string]any{
		"outcome": map[string]any{
			"outcome":  "selected",
			"optionId": optionID,
		},
	}
}

func cancelledPermissionOutcome() map[string]any {
	return map[string]any{
		"outcome": map[string]any{
			"outcome": "cancelled",
		},
	}
}

func firstAllowOption(options []any) string {
	for _, optionValue := range options {
		option, optionOK := optionValue.(map[string]any)
		if !optionOK {
			continue
		}
		optionID := stringValue(option, "optionId", "option_id", "id")
		optionKind := strings.ToLower(stringValue(option, "kind", "type", "name"))
		if optionID == "" {
			continue
		}
		if strings.Contains(optionKind, "reject") || strings.Contains(optionKind, "deny") {
			continue
		}
		if strings.Contains(optionKind, "allow") || strings.Contains(optionKind, "approve") || strings.Contains(optionID, "allow") {
			return optionID
		}
	}
	for _, optionValue := range options {
		option, optionOK := optionValue.(map[string]any)
		if !optionOK {
			continue
		}
		optionID := stringValue(option, "optionId", "option_id", "id")
		optionKind := strings.ToLower(stringValue(option, "kind", "type", "name"))
		if optionID != "" && !strings.Contains(optionKind, "reject") && !strings.Contains(optionKind, "deny") {
			return optionID
		}
	}
	return ""
}

func isControlledToolPermission(params map[string]any) bool {
	toolCall := mapValue(params, "toolCall", "tool_call")
	candidates := []string{
		stringValue(toolCall, "name"),
		stringValue(toolCall, "title"),
	}
	for _, candidate := range candidates {
		if isControlledToolName(candidate) {
			return true
		}
	}
	return false
}

func isControlledToolName(raw string) bool {
	switch normalizeControlledToolName(raw) {
	case "list_groups_schemas", "describe_schema", "propose_query_plan", "validate_bydbql", "probe_bydbql", "execute_bydbql":
		return true
	default:
		return false
	}
}

func normalizeControlledToolName(raw string) string {
	trimmedValue := strings.TrimSpace(raw)
	if trimmedValue == "" {
		return ""
	}
	const mcpPrefix = "mcp__"
	if strings.HasPrefix(trimmedValue, mcpPrefix) {
		remainder := strings.TrimPrefix(trimmedValue, mcpPrefix)
		if separatorIdx := strings.LastIndex(remainder, "__"); separatorIdx >= 0 && separatorIdx < len(remainder)-2 {
			return strings.TrimSpace(remainder[separatorIdx+2:])
		}
	}
	return trimmedValue
}

func planMessage(update map[string]any) string {
	entries, _ := update["entries"].([]any)
	var lines []string
	for _, entryValue := range entries {
		entry, entryOK := entryValue.(map[string]any)
		if !entryOK {
			continue
		}
		content := contentText(mapValue(entry, "content"))
		status := stringValue(entry, "status")
		if content == "" {
			continue
		}
		if status != "" {
			lines = append(lines, fmt.Sprintf("%s: %s", status, content))
			continue
		}
		lines = append(lines, content)
	}
	return strings.Join(lines, "\n")
}

func toolMessage(update map[string]any) string {
	title := stringValue(update, "title", "kind")
	status := stringValue(update, "status")
	content := contentText(mapValue(update, "content"))
	parts := compactStrings([]string{title, status, content})
	return strings.Join(parts, ": ")
}

func contentText(content map[string]any) string {
	text := stringValue(content, "text", "value", "delta", "content")
	if text != "" {
		return text
	}
	return ""
}

func mapValue(raw map[string]any, keys ...string) map[string]any {
	for _, key := range keys {
		value, ok := raw[key]
		if !ok {
			continue
		}
		if typedValue, typedOK := value.(map[string]any); typedOK {
			return typedValue
		}
	}
	return raw
}

func stringValue(raw map[string]any, keys ...string) string {
	for _, key := range keys {
		value, ok := raw[key]
		if !ok {
			continue
		}
		if typedValue, typedOK := value.(string); typedOK {
			return strings.TrimSpace(typedValue)
		}
	}
	return ""
}

func idString(value any) string {
	switch typedValue := value.(type) {
	case string:
		return typedValue
	case float64:
		return fmt.Sprintf("%.0f", typedValue)
	default:
		return fmt.Sprintf("%v", typedValue)
	}
}

func errorMessage(value any) string {
	if typedValue, typedOK := value.(map[string]any); typedOK {
		if message := stringValue(typedValue, "message"); message != "" {
			return message
		}
	}
	return strings.TrimSpace(fmt.Sprintf("%v", value))
}

func fallbackMessage(message, fallback string) string {
	if strings.TrimSpace(message) == "" {
		return fallback
	}
	return message
}

func compactStrings(values []string) []string {
	var compactedValues []string
	for _, value := range values {
		trimmedValue := strings.TrimSpace(value)
		if trimmedValue != "" {
			compactedValues = append(compactedValues, trimmedValue)
		}
	}
	return compactedValues
}

func send(ctx context.Context, events chan<- agent.Event, event agent.Event) bool {
	select {
	case <-ctx.Done():
		return false
	case events <- event:
		return true
	}
}
