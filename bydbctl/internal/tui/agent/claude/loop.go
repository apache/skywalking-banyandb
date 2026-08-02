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
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package claude

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/agent"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/agent/prompt"
)

const (
	maxClaudeOutputLine = 4 * 1024 * 1024
	maxClaudeStderr     = 64 * 1024
	apiKeyEnv           = "ANTHROPIC_API_KEY" // #nosec G101 -- This is an environment variable name, not a credential.
	baseURLEnv          = "ANTHROPIC_BASE_URL"
)

type claudeMCPConfig struct {
	MCPServers map[string]claudeMCPServer `json:"mcpServers"`
}

type claudeMCPServer struct {
	Type    string   `json:"type"`
	Command string   `json:"command"`
	Args    []string `json:"args,omitempty"`
}

type claudeStreamMessage struct {
	Event      json.RawMessage         `json:"event"`
	Type       string                  `json:"type"`
	Subtype    string                  `json:"subtype"`
	SessionID  string                  `json:"session_id"`
	Result     string                  `json:"result"`
	Errors     []string                `json:"errors"`
	Tools      []string                `json:"tools"`
	MCPServers []claudeMCPServerStatus `json:"mcp_servers"`
	IsError    bool                    `json:"is_error"`
}

type claudeMCPServerStatus struct {
	Name   string `json:"name"`
	Status string `json:"status"`
}

type claudeStreamEvent struct {
	Type  string            `json:"type"`
	Delta claudeStreamDelta `json:"delta"`
}

type claudeStreamDelta struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type turnStreamState struct {
	expectedSessionID string
	providerSessionID string
	result            string
	initSeen          bool
	resultSeen        bool
}

func (gateway *Gateway) runTurn(
	ctx context.Context,
	handle *turnHandle,
	providerSessionID string,
	parts prompt.Parts,
	events chan agent.Event,
) {
	completedProviderSessionID := providerSessionID
	defer func() {
		gateway.finishTurn(handle, completedProviderSessionID)
		close(events)
		handle.cancel()
	}()
	args, argsErr := claudeArgs(gateway.config, providerSessionID, parts)
	if argsErr != nil {
		gateway.emitTerminal(events, errorEvent(fmt.Errorf("failed to configure Claude CLI: %w", argsErr)))
		return
	}
	// #nosec G204 -- The executable is the explicit --claude-command value and arguments are passed without a shell.
	command := exec.CommandContext(ctx, gateway.config.Command, args...)
	command.Dir = gateway.config.WorkingDirectory
	command.Env = claudeEnvironment(gateway.config)
	configureProcessTree(command)
	stdout, stdoutErr := command.StdoutPipe()
	if stdoutErr != nil {
		gateway.emitTerminal(events, errorEvent(fmt.Errorf("failed to open Claude CLI output: %w", stdoutErr)))
		return
	}
	var stderr cappedBuffer
	command.Stderr = &stderr
	if startErr := command.Start(); startErr != nil {
		gateway.emitTerminal(events, errorEvent(fmt.Errorf("failed to start Claude CLI: %w", startErr)))
		return
	}
	if !gateway.attachProcess(handle, command.Process) {
		cleanupErr := stopStartedCommand(command)
		interruptErr := errors.New("claude turn interrupted")
		if cleanupErr != nil {
			interruptErr = errors.Join(interruptErr, cleanupErr)
		}
		gateway.emitTerminal(events, errorEvent(interruptErr))
		return
	}
	state := turnStreamState{expectedSessionID: providerSessionID}
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 0, 64*1024), maxClaudeOutputLine)
	var turnErr error
	var terminateErr error
	for scanner.Scan() {
		event, consumeErr := consumeClaudeMessage(scanner.Bytes(), &state)
		if consumeErr != nil {
			turnErr = consumeErr
			terminateErr = killProcessTree(command.Process)
			break
		}
		if event != nil && !gateway.emit(ctx, events, *event) {
			break
		}
	}
	scanErr := scanner.Err()
	waitErr := command.Wait()
	if strings.TrimSpace(state.providerSessionID) != "" {
		completedProviderSessionID = state.providerSessionID
	}
	if ctx.Err() != nil || gateway.turnWasInterrupted(handle) {
		gateway.emitTerminal(events, errorEvent(fmt.Errorf("claude turn interrupted: %w", context.Canceled)))
		return
	}
	if turnErr != nil {
		if terminateErr != nil {
			turnErr = errors.Join(turnErr, fmt.Errorf("failed to stop invalid Claude CLI turn: %w", terminateErr))
		}
		var exitErr *exec.ExitError
		if waitErr != nil && !errors.As(waitErr, &exitErr) {
			turnErr = errors.Join(turnErr, fmt.Errorf("failed to reap invalid Claude CLI turn: %w", waitErr))
		}
		gateway.emitTerminal(events, errorEvent(fmt.Errorf("claude CLI turn failed: %w", turnErr)))
		return
	}
	if scanErr != nil {
		gateway.emitTerminal(events, errorEvent(fmt.Errorf("failed to read Claude CLI stream: %w", scanErr)))
		return
	}
	if waitErr != nil {
		message := strings.TrimSpace(stderr.String())
		if message == "" {
			message = waitErr.Error()
		}
		gateway.emitTerminal(events, errorEvent(fmt.Errorf("claude CLI failed: %s: %w", message, waitErr)))
		return
	}
	if !state.initSeen {
		gateway.emitTerminal(events, errorEvent(errors.New("claude CLI stream did not initialize")))
		return
	}
	if !state.resultSeen {
		gateway.emitTerminal(events, errorEvent(errors.New("claude CLI stream ended without a result")))
		return
	}
	gateway.emitTerminal(events, agent.Event{
		Kind:    agent.EventKindFinalResponse,
		Message: state.result,
		Origin:  agent.EventOriginProvider,
	})
}

func stopStartedCommand(command *exec.Cmd) error {
	killErr := killProcessTree(command.Process)
	waitErr := command.Wait()
	if killErr != nil {
		return fmt.Errorf("failed to stop Claude CLI process: %w", killErr)
	}
	var exitErr *exec.ExitError
	if waitErr != nil && !errors.As(waitErr, &exitErr) {
		return fmt.Errorf("failed to reap Claude CLI process: %w", waitErr)
	}
	return nil
}

func claudeArgs(config Config, providerSessionID string, parts prompt.Parts) ([]string, error) {
	mcpConfig := claudeMCPConfig{MCPServers: map[string]claudeMCPServer{
		controlledMCPServerName: {
			Type:    "stdio",
			Command: config.ControlledMCPServer.Command,
			Args:    append([]string(nil), config.ControlledMCPServer.Args...),
		},
	}}
	mcpJSON, marshalErr := json.Marshal(mcpConfig)
	if marshalErr != nil {
		return nil, fmt.Errorf("failed to encode controlled MCP config: %w", marshalErr)
	}
	// Do not restrict the tool set with --tools or --permission-mode. The controlled MCP
	// server connects asynchronously after the CLI's initial tool snapshot, so any --tools
	// allowlist (including an empty one) hides the controlled tools from the model, which
	// then writes tool calls as plain text instead of invoking them. Rely on --allowedTools
	// to grant only the controlled tools and let default permission mode deny the rest.
	args := []string{
		"--print",
		"--verbose",
		"--output-format", "stream-json",
		"--include-partial-messages",
		"--model", config.Model,
		"--max-turns", strconv.Itoa(config.MaxTurns),
		"--disable-slash-commands",
		"--no-chrome",
		"--prompt-suggestions", "false",
		"--strict-mcp-config",
		"--mcp-config", string(mcpJSON),
		"--allowedTools", strings.Join(expectedAllowedToolNames(), ","),
		"--system-prompt", parts.System,
	}
	if strings.TrimSpace(providerSessionID) != "" {
		args = append(args, "--resume", providerSessionID)
	}
	args = append(args, parts.User)
	return args, nil
}

func expectedAllowedToolNames() []string {
	toolNames := make([]string, 0, len(controlledToolNames))
	for _, toolName := range controlledToolNames {
		toolNames = append(toolNames, "mcp__"+controlledMCPServerName+"__"+toolName)
	}
	return toolNames
}

func claudeEnvironment(config Config) []string {
	environment := os.Environ()
	if strings.TrimSpace(config.APIKey) != "" {
		environment = replaceEnvironmentValue(environment, apiKeyEnv, config.APIKey)
	}
	if strings.TrimSpace(config.BaseURL) != "" {
		environment = replaceEnvironmentValue(environment, baseURLEnv, config.BaseURL)
	}
	return environment
}

func replaceEnvironmentValue(environment []string, name, value string) []string {
	prefix := name + "="
	replaced := make([]string, 0, len(environment)+1)
	for _, entry := range environment {
		if !strings.HasPrefix(entry, prefix) {
			replaced = append(replaced, entry)
		}
	}
	return append(replaced, prefix+value)
}

func consumeClaudeMessage(rawMessage []byte, state *turnStreamState) (*agent.Event, error) {
	var message claudeStreamMessage
	if unmarshalErr := json.Unmarshal(rawMessage, &message); unmarshalErr != nil {
		return nil, fmt.Errorf("failed to decode message: %w", unmarshalErr)
	}
	switch message.Type {
	case "system":
		if message.Subtype != "init" {
			return nil, nil
		}
		if state.initSeen {
			return nil, errors.New("received duplicate Claude initialization")
		}
		if inventoryErr := validateClaudeInventory(message); inventoryErr != nil {
			return nil, inventoryErr
		}
		if sessionErr := validateProviderSessionID(message.SessionID, state.expectedSessionID); sessionErr != nil {
			return nil, sessionErr
		}
		state.providerSessionID = message.SessionID
		state.initSeen = true
		return nil, nil
	case "stream_event":
		if !state.initSeen {
			return nil, errors.New("received a stream event before Claude initialization")
		}
		var streamEvent claudeStreamEvent
		if unmarshalErr := json.Unmarshal(message.Event, &streamEvent); unmarshalErr != nil {
			return nil, fmt.Errorf("failed to decode partial message: %w", unmarshalErr)
		}
		if streamEvent.Type != "content_block_delta" || streamEvent.Delta.Type != "text_delta" || streamEvent.Delta.Text == "" {
			return nil, nil
		}
		return &agent.Event{
			Kind:    agent.EventKindMessageDelta,
			Message: streamEvent.Delta.Text,
			Origin:  agent.EventOriginProvider,
		}, nil
	case "result":
		if !state.initSeen {
			return nil, errors.New("received a result before Claude initialization")
		}
		if state.resultSeen {
			return nil, errors.New("received duplicate Claude result")
		}
		if sessionErr := validateProviderSessionID(message.SessionID, state.providerSessionID); sessionErr != nil {
			return nil, sessionErr
		}
		if message.IsError || message.Subtype != "success" {
			failure := strings.TrimSpace(strings.Join(message.Errors, "; "))
			if failure == "" {
				failure = strings.TrimSpace(message.Result)
			}
			if failure == "" {
				failure = "unknown error"
			}
			return nil, fmt.Errorf("claude CLI returned %s: %s", message.Subtype, failure)
		}
		state.result = message.Result
		state.resultSeen = true
		return nil, nil
	default:
		return nil, nil
	}
}

func validateClaudeInventory(message claudeStreamMessage) error {
	if len(message.MCPServers) != 1 {
		return fmt.Errorf("unexpected Claude MCP inventory: got %d servers", len(message.MCPServers))
	}
	server := message.MCPServers[0]
	if server.Name != controlledMCPServerName {
		return fmt.Errorf("controlled Claude MCP server is unavailable: name=%q status=%q", server.Name, server.Status)
	}
	// The CLI exposes its built-in tools alongside the controlled MCP tools; --allowedTools
	// grants only the controlled ones and default permission mode denies the rest, so reject
	// only unexpected MCP-owned tools, never built-ins.
	expectedTools := expectedAllowedToolNames()
	if foreignTool := firstForeignMCPTool(message.Tools, expectedTools); foreignTool != "" {
		return fmt.Errorf("unexpected Claude MCP tool inventory: got %q", foreignTool)
	}
	switch server.Status {
	case "connected":
		if !containsAllTools(message.Tools, expectedTools) {
			return fmt.Errorf("connected Claude MCP server exposed %s, want %s", strings.Join(message.Tools, ", "), strings.Join(expectedTools, ", "))
		}
		return nil
	case "pending":
		if containsAnyMCPTool(message.Tools) {
			return fmt.Errorf("pending Claude MCP server exposed tools unexpectedly: %s", strings.Join(message.Tools, ", "))
		}
		return nil
	default:
		return fmt.Errorf("controlled Claude MCP server is unavailable: name=%q status=%q", server.Name, server.Status)
	}
}

// firstForeignMCPTool returns the first tool owned by an unexpected MCP server, if any.
func firstForeignMCPTool(tools, controlledTools []string) string {
	controlledSet := make(map[string]struct{}, len(controlledTools))
	for _, tool := range controlledTools {
		controlledSet[tool] = struct{}{}
	}
	for _, tool := range tools {
		if strings.HasPrefix(tool, "mcp__") {
			if _, owned := controlledSet[tool]; !owned {
				return tool
			}
		}
	}
	return ""
}

// containsAllTools reports whether every expected tool is present in the inventory.
func containsAllTools(tools, expected []string) bool {
	present := make(map[string]struct{}, len(tools))
	for _, tool := range tools {
		present[tool] = struct{}{}
	}
	for _, tool := range expected {
		if _, ok := present[tool]; !ok {
			return false
		}
	}
	return true
}

// containsAnyMCPTool reports whether any MCP-owned tool is present in the inventory.
func containsAnyMCPTool(tools []string) bool {
	for _, tool := range tools {
		if strings.HasPrefix(tool, "mcp__") {
			return true
		}
	}
	return false
}

func validateProviderSessionID(actual, expected string) error {
	if strings.TrimSpace(actual) == "" {
		return errors.New("claude CLI returned an empty provider session ID")
	}
	if strings.TrimSpace(expected) != "" && actual != expected {
		return fmt.Errorf("claude CLI changed provider session ID from %q to %q", expected, actual)
	}
	return nil
}

func (gateway *Gateway) emit(ctx context.Context, events chan agent.Event, event agent.Event) bool {
	if event.StartedAt.IsZero() {
		event.StartedAt = gateway.now()
	}
	select {
	case <-ctx.Done():
		return false
	case events <- event:
		return true
	}
}

func (gateway *Gateway) emitTerminal(events chan agent.Event, event agent.Event) {
	if event.StartedAt.IsZero() {
		event.StartedAt = gateway.now()
	}
	for {
		select {
		case events <- event:
			return
		default:
		}
		select {
		case <-events:
		default:
		}
	}
}

func errorEvent(turnErr error) agent.Event {
	return agent.Event{
		Kind:    agent.EventKindError,
		Message: turnErr.Error(),
		Origin:  agent.EventOriginProvider,
		Err:     turnErr,
	}
}

type cappedBuffer struct {
	content []byte
}

func (buffer *cappedBuffer) Write(content []byte) (int, error) {
	remaining := maxClaudeStderr - len(buffer.content)
	if remaining > 0 {
		writeContent := content
		if len(writeContent) > remaining {
			writeContent = writeContent[:remaining]
		}
		buffer.content = append(buffer.content, writeContent...)
	}
	return len(content), nil
}

func (buffer *cappedBuffer) String() string {
	return string(buffer.content)
}
