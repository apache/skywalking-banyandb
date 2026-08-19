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
	"context"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/agent"
)

const agentProviderClaude = "claude"

func TestGatewayMaintainsCLIConversationHistory(t *testing.T) {
	gateway := NewGateway(Config{})
	if !gateway.MaintainsConversationHistory() {
		t.Fatal("Claude CLI sessions should retain conversation history")
	}
}

func TestGatewayDrivesClaudeCLIAndResumesProviderSession(t *testing.T) {
	workingDirectory := t.TempDir()
	argumentLog := filepath.Join(t.TempDir(), "arguments.log")
	t.Setenv("CLAUDE_FAKE_ARGUMENT_LOG", argumentLog)
	t.Setenv("CLAUDE_FAKE_MODE", "success")
	gateway := NewGateway(Config{
		Command:             writeFakeClaudeCLI(t),
		Model:               "test-model",
		APIKey:              "test-api-key",
		BaseURL:             "https://claude.example.test",
		MaxTurns:            7,
		WorkingDirectory:    workingDirectory,
		ControlledMCPServer: testControlledMCPServer(),
	})
	session, startErr := gateway.Start(context.Background(), agent.StartRequest{Provider: agentProviderClaude})
	if startErr != nil {
		t.Fatalf("Start returned an error: %v", startErr)
	}
	firstRequest := agent.TurnRequest{Prompt: "first question"}
	firstEvents, firstSendErr := gateway.Send(context.Background(), session.ID, firstRequest)
	if firstSendErr != nil {
		t.Fatalf("first Send returned an error: %v", firstSendErr)
	}
	assertSuccessfulTurn(t, collectEvents(firstEvents), "hel", "hello")
	secondEvents, secondSendErr := gateway.Send(context.Background(), session.ID, agent.TurnRequest{Prompt: "second question"})
	if secondSendErr != nil {
		t.Fatalf("second Send returned an error: %v", secondSendErr)
	}
	assertSuccessfulTurn(t, collectEvents(secondEvents), "aga", "again")
	if closeErr := gateway.Close(); closeErr != nil {
		t.Fatalf("Close returned an error: %v", closeErr)
	}

	invocations := readArgumentLog(t, argumentLog)
	if len(invocations) != 2 {
		t.Fatalf("expected two Claude turn processes, got %d", len(invocations))
	}
	firstArgs := invocations[0]
	assertArgumentValue(t, firstArgs, "--model", "test-model")
	assertArgumentValue(t, firstArgs, "--max-turns", "7")
	assertArgumentValue(t, firstArgs, "--output-format", "stream-json")
	assertArgumentValue(t, firstArgs, "--allowedTools", strings.Join(expectedAllowedToolNames(), ","))
	assertContainsArgument(t, firstArgs, "--strict-mcp-config")
	assertContainsArgument(t, firstArgs, "--include-partial-messages")
	assertContainsArgument(t, firstArgs, "--disable-slash-commands")
	assertContainsArgument(t, firstArgs, "--no-chrome")
	assertNotContainsArgument(t, firstArgs, "--tools")
	assertNotContainsArgument(t, firstArgs, "--permission-mode")
	assertNotContainsArgument(t, firstArgs, "--setting-sources")
	assertNotContainsArgument(t, firstArgs, "--resume")
	parts, partsErr := agent.BuildBydbqlPromptParts(firstRequest)
	if partsErr != nil {
		t.Fatalf("failed to build expected prompt parts: %v", partsErr)
	}
	assertArgumentValue(t, firstArgs, "--system-prompt", parts.System)
	if firstArgs[len(firstArgs)-1] != parts.User {
		t.Fatalf("unexpected positional prompt: %q", firstArgs[len(firstArgs)-1])
	}
	assertMCPConfig(t, argumentValue(t, firstArgs, "--mcp-config"))
	assertArgumentValue(t, invocations[1], "--resume", "provider-session-1")
}

func TestGatewayRejectsUnexpectedClaudeToolInventory(t *testing.T) {
	t.Setenv("CLAUDE_FAKE_ARGUMENT_LOG", filepath.Join(t.TempDir(), "arguments.log"))
	t.Setenv("CLAUDE_FAKE_MODE", "invalid-inventory")
	gateway := newTestGateway(t)
	session, startErr := gateway.Start(context.Background(), agent.StartRequest{Provider: agentProviderClaude})
	if startErr != nil {
		t.Fatalf("Start returned an error: %v", startErr)
	}
	events, sendErr := gateway.Send(context.Background(), session.ID, agent.TurnRequest{Prompt: "question"})
	if sendErr != nil {
		t.Fatalf("Send returned an error: %v", sendErr)
	}
	collected := collectEvents(events)
	if len(collected) != 1 || collected[0].Kind != agent.EventKindError {
		t.Fatalf("expected one fail-closed error event, got %#v", collected)
	}
	if !strings.Contains(collected[0].Message, "unexpected Claude MCP tool inventory") {
		t.Fatalf("unexpected inventory error: %q", collected[0].Message)
	}
}

func TestGatewayAcceptsControlledMCPWhileClaudeHandshakeIsPending(t *testing.T) {
	t.Setenv("CLAUDE_FAKE_ARGUMENT_LOG", filepath.Join(t.TempDir(), "arguments.log"))
	t.Setenv("CLAUDE_FAKE_MODE", "pending-inventory")
	gateway := newTestGateway(t)
	session, startErr := gateway.Start(context.Background(), agent.StartRequest{Provider: agentProviderClaude})
	if startErr != nil {
		t.Fatalf("Start returned an error: %v", startErr)
	}
	events, sendErr := gateway.Send(context.Background(), session.ID, agent.TurnRequest{Prompt: "question"})
	if sendErr != nil {
		t.Fatalf("Send returned an error: %v", sendErr)
	}
	assertSuccessfulTurn(t, collectEvents(events), "hel", "hello")
}

func TestGatewayInterruptsActiveClaudeProcess(t *testing.T) {
	argumentLog := filepath.Join(t.TempDir(), "arguments.log")
	t.Setenv("CLAUDE_FAKE_ARGUMENT_LOG", argumentLog)
	t.Setenv("CLAUDE_FAKE_MODE", "block")
	gateway := newTestGateway(t)
	session, startErr := gateway.Start(context.Background(), agent.StartRequest{Provider: agentProviderClaude})
	if startErr != nil {
		t.Fatalf("Start returned an error: %v", startErr)
	}
	events, sendErr := gateway.Send(context.Background(), session.ID, agent.TurnRequest{Prompt: "wait"})
	if sendErr != nil {
		t.Fatalf("Send returned an error: %v", sendErr)
	}
	waitForFile(t, argumentLog)
	if interruptErr := gateway.Interrupt(context.Background(), session.ID); interruptErr != nil {
		t.Fatalf("Interrupt returned an error: %v", interruptErr)
	}
	select {
	case event, open := <-events:
		if !open || event.Kind != agent.EventKindError || !strings.Contains(event.Message, "interrupted") {
			t.Fatalf("unexpected interrupt terminal event: %#v (open=%t)", event, open)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for interrupted Claude process")
	}
}

func TestGatewayInterruptCompletesWhenEventBufferIsFull(t *testing.T) {
	argumentLog := filepath.Join(t.TempDir(), "arguments.log")
	t.Setenv("CLAUDE_FAKE_ARGUMENT_LOG", argumentLog)
	t.Setenv("CLAUDE_FAKE_MODE", "flood-block")
	gateway := newTestGateway(t)
	session, startErr := gateway.Start(context.Background(), agent.StartRequest{Provider: agentProviderClaude})
	if startErr != nil {
		t.Fatalf("Start returned an error: %v", startErr)
	}
	events, sendErr := gateway.Send(context.Background(), session.ID, agent.TurnRequest{Prompt: "wait"})
	if sendErr != nil {
		t.Fatalf("Send returned an error: %v", sendErr)
	}
	waitForFile(t, argumentLog)
	time.Sleep(50 * time.Millisecond)
	if interruptErr := gateway.Interrupt(context.Background(), session.ID); interruptErr != nil {
		t.Fatalf("Interrupt returned an error: %v", interruptErr)
	}
	collectedEvents := make(chan []agent.Event, 1)
	//panicdiag:allow-rawgo test-only event collector; a panic here must fail the test loudly rather than be recovered and hidden
	go func() {
		collectedEvents <- collectEvents(events)
	}()
	select {
	case collected := <-collectedEvents:
		if len(collected) == 0 || collected[len(collected)-1].Kind != agent.EventKindError {
			t.Fatalf("expected an interrupt terminal event after buffered deltas, got %#v", collected)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for a full event buffer to close")
	}
}

func TestGatewayCloseStopsActiveClaudeProcess(t *testing.T) {
	argumentLog := filepath.Join(t.TempDir(), "arguments.log")
	t.Setenv("CLAUDE_FAKE_ARGUMENT_LOG", argumentLog)
	t.Setenv("CLAUDE_FAKE_MODE", "block")
	gateway := newTestGateway(t)
	session, startErr := gateway.Start(context.Background(), agent.StartRequest{Provider: agentProviderClaude})
	if startErr != nil {
		t.Fatalf("Start returned an error: %v", startErr)
	}
	events, sendErr := gateway.Send(context.Background(), session.ID, agent.TurnRequest{Prompt: "wait"})
	if sendErr != nil {
		t.Fatalf("Send returned an error: %v", sendErr)
	}
	waitForFile(t, argumentLog)
	if closeErr := gateway.Close(); closeErr != nil {
		t.Fatalf("Close returned an error: %v", closeErr)
	}
	collected := collectEvents(events)
	if len(collected) != 1 || collected[0].Kind != agent.EventKindError || !strings.Contains(collected[0].Message, "interrupted") {
		t.Fatalf("unexpected close terminal events: %#v", collected)
	}
}

func TestGatewayStartRequiresControlledMCPServer(t *testing.T) {
	gateway := NewGateway(Config{Command: writeFakeClaudeCLI(t), WorkingDirectory: t.TempDir()})
	_, startErr := gateway.Start(context.Background(), agent.StartRequest{Provider: agentProviderClaude})
	if startErr == nil || !strings.Contains(startErr.Error(), "controlled MCP server") {
		t.Fatalf("expected controlled MCP validation error, got %v", startErr)
	}
}

func newTestGateway(t *testing.T) *Gateway {
	t.Helper()
	return NewGateway(Config{
		Command:             writeFakeClaudeCLI(t),
		WorkingDirectory:    t.TempDir(),
		ControlledMCPServer: testControlledMCPServer(),
	})
}

func testControlledMCPServer() agent.ControlledMCPServer {
	return agent.ControlledMCPServer{
		Name:         controlledMCPServerName,
		Command:      "/opt/bydbctl/bin/bydbctl",
		Args:         []string{"agent-tool-bridge", "--socket", "/tmp/bydbctl-test.sock"},
		EnabledTools: append([]string(nil), controlledToolNames...),
	}
}

func writeFakeClaudeCLI(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "claude")
	script := `#!/bin/sh
set -eu
if [ "${1:-}" = "--version" ]; then
  printf '%s\n' '2.1.202 (Claude Code)'
  exit 0
fi
{
  printf '%s\n' BEGIN
  for argument in "$@"; do
    printf '%s' "$argument" | od -An -v -tx1 | tr -d '[:space:]'
    printf '\n'
  done
  printf '%s\n' END
} >> "$CLAUDE_FAKE_ARGUMENT_LOG"
tools='["Task","Bash","Read","Edit","Write","WebFetch","WebSearch",'
tools="${tools}\"mcp__bydbctl-controlled-tools__list_groups_schemas\","
tools="${tools}\"mcp__bydbctl-controlled-tools__describe_schema\","
tools="${tools}\"mcp__bydbctl-controlled-tools__propose_query_plan\","
tools="${tools}\"mcp__bydbctl-controlled-tools__validate_bydbql\","
tools="${tools}\"mcp__bydbctl-controlled-tools__execute_bydbql\"]"
status=connected
if [ "$CLAUDE_FAKE_MODE" = invalid-inventory ]; then
  tools='["mcp__evil__foreign_tool"]'
fi
if [ "$CLAUDE_FAKE_MODE" = pending-inventory ]; then
  tools='[]'
  status=pending
fi
printf '%s%s%s%s%s\n' \
  '{"type":"system","subtype":"init","session_id":"provider-session-1","tools":' "$tools" \
  ',"mcp_servers":[{"name":"bydbctl-controlled-tools","status":"' "$status" '"}]}'
if [ "$CLAUDE_FAKE_MODE" = block ]; then
  trap 'exit 130' INT TERM
  while :; do sleep 1; done
fi
if [ "$CLAUDE_FAKE_MODE" = block-child ]; then
  sleep 30 &
  child_pid=$!
  printf '%s\n' "$child_pid" > "$CLAUDE_FAKE_CHILD_PID"
  wait "$child_pid"
fi
if [ "$CLAUDE_FAKE_MODE" = flood-block ]; then
  event_idx=0
  while [ "$event_idx" -lt 100 ]; do
    printf '%s\n' '{"type":"stream_event","session_id":"provider-session-1","event":{"type":"content_block_delta","delta":{"type":"text_delta","text":"x"}}}'
    event_idx=$((event_idx + 1))
  done
  trap 'exit 130' INT TERM
  while :; do sleep 1; done
fi
if [ "$CLAUDE_FAKE_MODE" = invalid-inventory ]; then
  printf '%s\n' '{"type":"result","subtype":"success","is_error":false,"result":"unsafe","session_id":"provider-session-1"}'
  exit 0
fi
case " $* " in
  *" --resume provider-session-1 "*)
    printf '%s\n' '{"type":"stream_event","session_id":"provider-session-1","event":{"type":"content_block_delta","delta":{"type":"text_delta","text":"aga"}}}'
    printf '%s\n' '{"type":"result","subtype":"success","is_error":false,"result":"again","session_id":"provider-session-1"}'
    ;;
  *)
    printf '%s\n' '{"type":"stream_event","session_id":"provider-session-1","event":{"type":"content_block_delta","delta":{"type":"text_delta","text":"hel"}}}'
    printf '%s\n' '{"type":"result","subtype":"success","is_error":false,"result":"hello","session_id":"provider-session-1"}'
    ;;
esac
`
	// #nosec G306 -- The owner-only fake CLI must be executable by the test process.
	if writeErr := os.WriteFile(path, []byte(script), 0o700); writeErr != nil {
		t.Fatalf("failed to write fake Claude CLI: %v", writeErr)
	}
	return path
}

func collectEvents(events <-chan agent.Event) []agent.Event {
	var collected []agent.Event
	for event := range events {
		collected = append(collected, event)
	}
	return collected
}

func assertSuccessfulTurn(t *testing.T, events []agent.Event, delta, final string) {
	t.Helper()
	if len(events) != 2 {
		t.Fatalf("expected a delta and final response, got %#v", events)
	}
	if events[0].Kind != agent.EventKindMessageDelta || events[0].Message != delta || events[0].Origin != agent.EventOriginProvider {
		t.Fatalf("unexpected delta event: %#v", events[0])
	}
	if events[1].Kind != agent.EventKindFinalResponse || events[1].Message != final || events[1].Origin != agent.EventOriginProvider {
		t.Fatalf("unexpected final event: %#v", events[1])
	}
}

func readArgumentLog(t *testing.T, path string) [][]string {
	t.Helper()
	content, readErr := os.ReadFile(path)
	if readErr != nil {
		t.Fatalf("failed to read fake Claude argument log: %v", readErr)
	}
	var invocations [][]string
	var current []string
	for _, line := range strings.Split(strings.TrimSpace(string(content)), "\n") {
		switch line {
		case "BEGIN":
			current = nil
		case "END":
			invocations = append(invocations, current)
		default:
			decoded, decodeErr := hex.DecodeString(line)
			if decodeErr != nil {
				t.Fatalf("failed to decode fake Claude argument: %v", decodeErr)
			}
			current = append(current, string(decoded))
		}
	}
	return invocations
}

func assertArgumentValue(t *testing.T, args []string, name, expected string) {
	t.Helper()
	if actual := argumentValue(t, args, name); actual != expected {
		t.Fatalf("argument %s=%q, want %q", name, actual, expected)
	}
}

func argumentValue(t *testing.T, args []string, name string) string {
	t.Helper()
	for argIdx, argument := range args {
		if argument == name {
			if argIdx+1 >= len(args) {
				t.Fatalf("argument %s has no value in %#v", name, args)
			}
			return args[argIdx+1]
		}
	}
	t.Fatalf("argument %s is missing from %#v", name, args)
	return ""
}

func assertContainsArgument(t *testing.T, args []string, expected string) {
	t.Helper()
	for _, argument := range args {
		if argument == expected {
			return
		}
	}
	t.Fatalf("argument %q is missing from %#v", expected, args)
}

func assertNotContainsArgument(t *testing.T, args []string, unexpected string) {
	t.Helper()
	for _, argument := range args {
		if argument == unexpected {
			t.Fatalf("unexpected argument %q in %#v", unexpected, args)
		}
	}
}

func assertMCPConfig(t *testing.T, rawConfig string) {
	t.Helper()
	var config struct {
		MCPServers map[string]struct {
			Type    string   `json:"type"`
			Command string   `json:"command"`
			Args    []string `json:"args"`
		} `json:"mcpServers"`
	}
	if unmarshalErr := json.Unmarshal([]byte(rawConfig), &config); unmarshalErr != nil {
		t.Fatalf("failed to parse MCP config: %v", unmarshalErr)
	}
	server, exists := config.MCPServers[controlledMCPServerName]
	if !exists || len(config.MCPServers) != 1 {
		t.Fatalf("unexpected MCP servers: %#v", config.MCPServers)
	}
	if server.Type != "stdio" || server.Command != "/opt/bydbctl/bin/bydbctl" || strings.Join(server.Args, " ") != "agent-tool-bridge --socket /tmp/bydbctl-test.sock" {
		t.Fatalf("unexpected MCP server config: %#v", server)
	}
}

func waitForFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if info, statErr := os.Stat(path); statErr == nil && info.Size() > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", path)
}
