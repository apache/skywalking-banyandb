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
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/agent"
)

const (
	helperEnabledEnv = "BYDBCTL_CODEX_HELPER"
	helperModeEnv    = "BYDBCTL_CODEX_HELPER_MODE"
	helperLogEnv     = "BYDBCTL_CODEX_HELPER_LOG"
)

type helperRecord struct {
	Params   map[string]any `json:"params,omitempty"`
	Kind     string         `json:"kind"`
	Method   string         `json:"method,omitempty"`
	ThreadID string         `json:"thread_id,omitempty"`
	Args     []string       `json:"args,omitempty"`
	PID      int            `json:"pid"`
}

func TestGatewayReusesProcessAndThreadAcrossTurns(t *testing.T) {
	gateway, logPath := newTestGateway(t, "normal")
	session := startTestGateway(t, gateway)
	firstEvents := sendAndCollect(t, gateway, session.ID, "first")
	secondEvents := sendAndCollect(t, gateway, session.ID, "second")
	if finalMessage(firstEvents) != "reply-1" || finalMessage(secondEvents) != "reply-2" {
		t.Fatalf("unexpected streamed replies: first=%+v second=%+v", firstEvents, secondEvents)
	}
	records := readHelperRecords(t, logPath)
	assertInitializationOrder(t, records)
	assertSingleProcessAndThread(t, records, 2)
	assertHardenedArgs(t, records)
	threadRecord := firstRecord(t, records, "thread/start")
	if threadRecord.Params["ephemeral"] != true || threadRecord.Params["sandbox"] != "read-only" || threadRecord.Params["approvalPolicy"] != "never" {
		t.Fatalf("thread was not created with enforced lifecycle settings: %+v", threadRecord.Params)
	}
	developerInstructions, _ := threadRecord.Params["developerInstructions"].(string)
	if !strings.Contains(developerInstructions, "Never publish a raw BYDBQL statement") {
		t.Fatalf("stable developer instructions are missing: %q", developerInstructions)
	}
	appConnection := gateway.conn
	if closeErr := gateway.Close(); closeErr != nil {
		t.Fatalf("Close returned error: %v", closeErr)
	}
	if gateway.conn != nil {
		t.Fatal("gateway retained a closed connection")
	}
	if appConnection == nil || appConnection.command.ProcessState == nil || !appConnection.command.ProcessState.Exited() {
		t.Fatal("Close did not reap the app-server process")
	}
}

func TestGatewayInterruptPreservesProcessAndThread(t *testing.T) {
	gateway, logPath := newTestGateway(t, "normal")
	session := startTestGateway(t, gateway)
	events, sendErr := gateway.Send(context.Background(), session.ID, testTurnRequest("block"))
	if sendErr != nil {
		t.Fatalf("Send returned error: %v", sendErr)
	}
	select {
	case event := <-events:
		if event.Kind != agent.EventKindMessageDelta || event.Message != "waiting" {
			t.Fatalf("unexpected pre-interrupt event: %+v", event)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for active turn")
	}
	if interruptErr := gateway.Interrupt(context.Background(), session.ID); interruptErr != nil {
		t.Fatalf("Interrupt returned error: %v", interruptErr)
	}
	for range events {
	}
	nextEvents := sendAndCollect(t, gateway, session.ID, "after interrupt")
	if finalMessage(nextEvents) != "reply-2" {
		t.Fatalf("next turn did not reuse the thread: %+v", nextEvents)
	}
	assertSingleProcessAndThread(t, readHelperRecords(t, logPath), 2)
	if closeErr := gateway.Close(); closeErr != nil {
		t.Fatalf("Close returned error: %v", closeErr)
	}
}

func TestGatewayMapsFailedTurn(t *testing.T) {
	gateway, _ := newTestGateway(t, "normal")
	session := startTestGateway(t, gateway)
	events := sendAndCollect(t, gateway, session.ID, "fail")
	if len(events) == 0 || events[len(events)-1].Kind != agent.EventKindError || !strings.Contains(events[len(events)-1].Message, "simulated failure") {
		t.Fatalf("failed turn was not mapped to an error: %+v", events)
	}
	if closeErr := gateway.Close(); closeErr != nil {
		t.Fatalf("Close returned error: %v", closeErr)
	}
}

func TestGatewayFailsClosedOnUnexpectedProcessExit(t *testing.T) {
	gateway, _ := newTestGateway(t, "normal")
	session := startTestGateway(t, gateway)
	events := sendAndCollect(t, gateway, session.ID, "exit")
	if len(events) == 0 || events[len(events)-1].Kind != agent.EventKindError || !strings.Contains(events[len(events)-1].Message, "exited unexpectedly") {
		t.Fatalf("unexpected exit was not surfaced: %+v", events)
	}
	if _, sendErr := gateway.Send(context.Background(), session.ID, testTurnRequest("retry")); sendErr == nil {
		t.Fatal("gateway silently accepted a turn after app-server exit")
	}
	if closeErr := gateway.Close(); closeErr != nil {
		t.Fatalf("Close returned error: %v", closeErr)
	}
}

func TestGatewayFailsClosedOnForbiddenActivity(t *testing.T) {
	testCases := []struct {
		name     string
		task     string
		expected string
	}{
		{name: "command approval", task: "unsafe-command", expected: "forbidden server request"},
		{name: "file change", task: "unsafe-file", expected: "forbidden item type"},
		{name: "fake MCP", task: "unsafe-mcp", expected: "non-allowlisted MCP tool"},
		{name: "elicitation", task: "unsafe-elicitation", expected: "forbidden server request"},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			gateway, _ := newTestGateway(t, "normal")
			session := startTestGateway(t, gateway)
			events := sendAndCollect(t, gateway, session.ID, testCase.task)
			if len(events) == 0 || events[len(events)-1].Kind != agent.EventKindError || !strings.Contains(events[len(events)-1].Message, testCase.expected) {
				t.Fatalf("forbidden activity was not rejected: %+v", events)
			}
			if closeErr := gateway.Close(); closeErr != nil {
				t.Fatalf("Close returned error: %v", closeErr)
			}
		})
	}
}

func TestGatewayRequiresCodexLogin(t *testing.T) {
	gateway, _ := newTestGateway(t, "logged-out")
	_, startErr := gateway.Start(context.Background(), agent.StartRequest{Provider: "codex"})
	if startErr == nil || !strings.Contains(startErr.Error(), "codex login") {
		t.Fatalf("missing login was not explained: %v", startErr)
	}
}

func TestGatewayRejectsUnsafeRuntimeInventory(t *testing.T) {
	gateway, _ := newTestGateway(t, "extra-inventory")
	_, startErr := gateway.Start(context.Background(), agent.StartRequest{Provider: "codex"})
	if startErr == nil || !strings.Contains(startErr.Error(), "uncontrolled MCP server") {
		t.Fatalf("unsafe MCP inventory was accepted: %v", startErr)
	}
}

func TestAppServerArgsDisableBuiltinsAndConfiguredMCP(t *testing.T) {
	server := testMCPServer()
	args, argsErr := appServerArgs(server, []string{"existing", "with-dash"})
	if argsErr != nil {
		t.Fatalf("appServerArgs returned error: %v", argsErr)
	}
	joinedArgs := strings.Join(args, " ")
	for _, expected := range []string{
		"--sandbox read-only",
		"--ask-for-approval never",
		`web_search="disabled"`,
		`history.persistence="none"`,
		`mcp_servers.existing={ command = "/path/to/bydbctl", args = [], enabled = false }`,
		`mcp_servers.with-dash={ command = "/path/to/bydbctl", args = [], enabled = false }`,
		`mcp_servers.bydbctl-controlled-tools={ command = "/path/to/bydbctl"`,
		`required = true, enabled_tools =`,
		"app-server --stdio",
	} {
		if !strings.Contains(joinedArgs, expected) {
			t.Fatalf("app-server args do not contain %q: %s", expected, joinedArgs)
		}
	}
	for _, featureName := range disabledFeatures {
		if !strings.Contains(joinedArgs, "--disable "+featureName) {
			t.Fatalf("feature %q was not disabled: %s", featureName, joinedArgs)
		}
	}
}

func TestRealCodexHandshakeAndInventory(t *testing.T) {
	if os.Getenv("BYDBCTL_REAL_CODEX_INTEGRATION") != "1" {
		t.Skip("set BYDBCTL_REAL_CODEX_INTEGRATION=1 to run the local Codex handshake test")
	}
	testDirectory := t.TempDir()
	wrapperPath := filepath.Join(testDirectory, "mcp-helper")
	wrapper := fmt.Sprintf("#!/bin/sh\n%s=1 exec %s -test.run=TestCodexAppServerHelper -- mcp-helper\n", helperEnabledEnv, shellQuote(os.Args[0]))
	if writeErr := os.WriteFile(wrapperPath, []byte(wrapper), 0o600); writeErr != nil {
		t.Fatalf("failed to write MCP helper wrapper: %v", writeErr)
	}
	if chmodErr := os.Chmod(wrapperPath, 0o700); chmodErr != nil {
		t.Fatalf("failed to make MCP helper executable: %v", chmodErr)
	}
	t.Setenv(helperEnabledEnv, "1")
	gateway := NewGateway(Config{
		Command:          "codex",
		WorkingDirectory: testDirectory,
		ControlledMCPServer: agent.ControlledMCPServer{
			Name:         controlledMCPServerName,
			Command:      wrapperPath,
			EnabledTools: append([]string(nil), controlledToolNames...),
		},
	})
	session, startErr := gateway.Start(context.Background(), agent.StartRequest{Provider: "codex"})
	if startErr != nil {
		t.Fatalf("real Codex handshake failed: %v", startErr)
	}
	if strings.TrimSpace(session.ID) == "" || gateway.conn == nil || strings.TrimSpace(gateway.conn.threadID) == "" {
		t.Fatalf("real Codex did not create an ephemeral thread: session=%+v", session)
	}
	if closeErr := gateway.Close(); closeErr != nil {
		t.Fatalf("Close returned error: %v", closeErr)
	}
}

func TestCodexAppServerHelper(t *testing.T) {
	if os.Getenv(helperEnabledEnv) != "1" {
		return
	}
	args := helperArgs(os.Args)
	if len(args) == 1 && args[0] == "--version" {
		fmt.Println("codex-cli 0.144.5")
		os.Exit(0)
	}
	if len(args) == 3 && args[0] == "mcp" && args[1] == "list" && args[2] == "--json" {
		fmt.Println(`[{"name":"existing","enabled":true}]`)
		os.Exit(0)
	}
	if containsString(args, "mcp-helper") {
		serveMCPHelper(t)
		os.Exit(0)
	}
	serveAppServerHelper(t, args)
	os.Exit(0)
}

func newTestGateway(t *testing.T, mode string) (*Gateway, string) {
	t.Helper()
	testDirectory := t.TempDir()
	wrapperPath := filepath.Join(testDirectory, "codex-helper")
	wrapper := fmt.Sprintf("#!/bin/sh\nexec %s -test.run=TestCodexAppServerHelper -- \"$@\"\n", shellQuote(os.Args[0]))
	if writeErr := os.WriteFile(wrapperPath, []byte(wrapper), 0o600); writeErr != nil {
		t.Fatalf("failed to write helper wrapper: %v", writeErr)
	}
	if chmodErr := os.Chmod(wrapperPath, 0o700); chmodErr != nil {
		t.Fatalf("failed to make helper executable: %v", chmodErr)
	}
	logPath := filepath.Join(testDirectory, "helper.jsonl")
	t.Setenv(helperEnabledEnv, "1")
	t.Setenv(helperModeEnv, mode)
	t.Setenv(helperLogEnv, logPath)
	return NewGateway(Config{
		Command:             wrapperPath,
		WorkingDirectory:    testDirectory,
		ControlledMCPServer: testMCPServer(),
	}), logPath
}

func testMCPServer() agent.ControlledMCPServer {
	return agent.ControlledMCPServer{
		Name:         controlledMCPServerName,
		Command:      "/path/to/bydbctl",
		Args:         []string{"agent-tool-bridge", "--socket", "/tmp/tools.sock"},
		EnabledTools: append([]string(nil), controlledToolNames...),
	}
}

func startTestGateway(t *testing.T, gateway *Gateway) agent.Session {
	t.Helper()
	session, startErr := gateway.Start(context.Background(), agent.StartRequest{Provider: "codex"})
	if startErr != nil {
		t.Fatalf("Start returned error: %v", startErr)
	}
	return session
}

func testTurnRequest(task string) agent.TurnRequest {
	return agent.TurnRequest{
		Prompt: task,
		Payload: agent.RequestPayload{
			Task: task,
			Goal: task,
		},
	}
}

func sendAndCollect(t *testing.T, gateway *Gateway, sessionID, task string) []agent.Event {
	t.Helper()
	events, sendErr := gateway.Send(context.Background(), sessionID, testTurnRequest(task))
	if sendErr != nil {
		t.Fatalf("Send returned error: %v", sendErr)
	}
	resultCh := make(chan []agent.Event, 1)
	go func() {
		var collected []agent.Event
		for event := range events {
			collected = append(collected, event)
		}
		resultCh <- collected
	}()
	select {
	case collected := <-resultCh:
		return collected
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out collecting turn %q", task)
		return nil
	}
}

func finalMessage(events []agent.Event) string {
	for eventIdx := len(events) - 1; eventIdx >= 0; eventIdx-- {
		if events[eventIdx].Kind == agent.EventKindFinalResponse {
			return events[eventIdx].Message
		}
	}
	return ""
}

func serveAppServerHelper(t *testing.T, args []string) {
	t.Helper()
	writeHelperRecord(t, helperRecord{Kind: "app_start", Args: args, PID: os.Getpid()})
	mode := os.Getenv(helperModeEnv)
	scanner := bufio.NewScanner(os.Stdin)
	encoder := json.NewEncoder(os.Stdout)
	turnCount := 0
	activeTurnID := ""
	for scanner.Scan() {
		var request map[string]any
		if unmarshalErr := json.Unmarshal(scanner.Bytes(), &request); unmarshalErr != nil {
			t.Fatalf("helper received invalid JSON: %v", unmarshalErr)
		}
		method, _ := request["method"].(string)
		params, _ := request["params"].(map[string]any)
		writeHelperRecord(t, helperRecord{Kind: "message", Method: method, Params: params, PID: os.Getpid()})
		id, hasID := request["id"]
		if !hasID {
			continue
		}
		switch method {
		case "initialize":
			encodeHelper(t, encoder, map[string]any{"id": id, "result": map[string]any{"userAgent": "test"}})
		case "account/read":
			account := any(map[string]any{"type": "chatgpt", "email": "test@example.com", "planType": "plus"})
			if mode == "logged-out" {
				account = nil
			}
			encodeHelper(t, encoder, map[string]any{"id": id, "result": map[string]any{"account": account, "requiresOpenaiAuth": true}})
		case "thread/start":
			encodeHelper(t, encoder, map[string]any{"id": id, "result": map[string]any{"thread": map[string]any{"id": "thread-1"}}})
		case "mcpServerStatus/list":
			inventory := helperInventory()
			if mode == "extra-inventory" {
				inventory = append(inventory, map[string]any{
					"name":      "evil",
					"tools":     map[string]any{"shell": map[string]any{}},
					"resources": []any{}, "resourceTemplates": []any{}, "authStatus": "unsupported",
				})
			}
			encodeHelper(t, encoder, map[string]any{"id": id, "result": map[string]any{"data": inventory, "nextCursor": nil}})
		case "turn/start":
			turnCount++
			activeTurnID = fmt.Sprintf("turn-%d", turnCount)
			encodeHelper(t, encoder, map[string]any{
				"id":     id,
				"result": map[string]any{"turn": map[string]any{"id": activeTurnID, "items": []any{}, "status": "inProgress"}},
			})
			encodeHelper(t, encoder, map[string]any{
				"method": "turn/started",
				"params": map[string]any{"threadId": "thread-1", "turn": map[string]any{"id": activeTurnID}},
			})
			handleHelperTurn(t, encoder, params, activeTurnID, turnCount)
			if turnTask(params) == "exit" {
				return
			}
		case "turn/interrupt":
			encodeHelper(t, encoder, map[string]any{"id": id, "result": map[string]any{}})
			encodeHelperCompleted(t, encoder, activeTurnID, "interrupted", "")
		default:
			encodeHelper(t, encoder, map[string]any{"id": id, "error": map[string]any{"code": -32601, "message": "unknown"}})
		}
	}
	if scanErr := scanner.Err(); scanErr != nil {
		t.Fatalf("helper scanner failed: %v", scanErr)
	}
}

func handleHelperTurn(t *testing.T, encoder *json.Encoder, params map[string]any, turnID string, turnCount int) {
	t.Helper()
	task := turnTask(params)
	encodeHelper(t, encoder, map[string]any{"method": "future/notification", "params": map[string]any{"ignored": true}})
	switch task {
	case "block":
		encodeHelper(t, encoder, agentDelta(turnID, "waiting"))
	case "fail":
		encodeHelperCompleted(t, encoder, turnID, "failed", "simulated failure")
	case "exit":
		return
	case "unsafe-command":
		encodeHelper(t, encoder, map[string]any{
			"id": "server-1", "method": "item/commandExecution/requestApproval", "params": map[string]any{"turnId": turnID},
		})
	case "unsafe-file":
		encodeHelper(t, encoder, itemNotification(turnID, map[string]any{"id": "file-1", "type": "fileChange"}))
	case "unsafe-mcp":
		encodeHelper(t, encoder, itemNotification(turnID, map[string]any{
			"id": "mcp-1", "type": "mcpToolCall", "server": "lookalike", "tool": "propose_query_plan",
		}))
	case "unsafe-elicitation":
		encodeHelper(t, encoder, map[string]any{
			"id": "server-2", "method": "mcpServer/elicitation/request", "params": map[string]any{"turnId": turnID},
		})
	default:
		message := fmt.Sprintf("reply-%d", turnCount)
		encodeHelper(t, encoder, agentDelta(turnID, message))
		encodeHelperCompleted(t, encoder, turnID, "completed", "")
	}
}

func helperInventory() []map[string]any {
	tools := make(map[string]any, len(controlledToolNames))
	for _, toolName := range controlledToolNames {
		tools[toolName] = map[string]any{"name": toolName, "inputSchema": map[string]any{"type": "object"}}
	}
	return []map[string]any{
		{
			"name": controlledMCPServerName, "tools": tools, "resources": []any{}, "resourceTemplates": []any{}, "authStatus": "unsupported",
		},
		{
			"name": "existing", "tools": map[string]any{}, "resources": []any{}, "resourceTemplates": []any{}, "authStatus": "unsupported",
		},
	}
}

func agentDelta(turnID, delta string) map[string]any {
	return map[string]any{
		"method": "item/agentMessage/delta",
		"params": map[string]any{"threadId": "thread-1", "turnId": turnID, "itemId": "message-1", "delta": delta},
	}
}

func itemNotification(turnID string, item map[string]any) map[string]any {
	return map[string]any{
		"method": "item/started",
		"params": map[string]any{"threadId": "thread-1", "turnId": turnID, "item": item},
	}
}

func encodeHelperCompleted(t *testing.T, encoder *json.Encoder, turnID, status, errorMessage string) {
	t.Helper()
	turn := map[string]any{"id": turnID, "status": status, "items": []any{}}
	if errorMessage != "" {
		turn["error"] = map[string]any{"message": errorMessage}
	}
	encodeHelper(t, encoder, map[string]any{
		"method": "turn/completed",
		"params": map[string]any{"threadId": "thread-1", "turn": turn},
	})
}

func encodeHelper(t *testing.T, encoder *json.Encoder, message any) {
	t.Helper()
	if encodeErr := encoder.Encode(message); encodeErr != nil {
		t.Fatalf("helper failed to encode response: %v", encodeErr)
	}
}

func turnTask(params map[string]any) string {
	inputs, _ := params["input"].([]any)
	if len(inputs) == 0 {
		return ""
	}
	input, _ := inputs[0].(map[string]any)
	text, _ := input["text"].(string)
	for _, task := range []string{
		"unsafe-command", "unsafe-file", "unsafe-mcp", "unsafe-elicitation", "after interrupt", "first", "second", "block", "fail", "exit",
	} {
		if strings.Contains(text, task) {
			return task
		}
	}
	return "normal"
}

func serveMCPHelper(t *testing.T) {
	t.Helper()
	scanner := bufio.NewScanner(os.Stdin)
	encoder := json.NewEncoder(os.Stdout)
	for scanner.Scan() {
		var request map[string]any
		if unmarshalErr := json.Unmarshal(scanner.Bytes(), &request); unmarshalErr != nil {
			continue
		}
		id, hasID := request["id"]
		if !hasID {
			continue
		}
		method, _ := request["method"].(string)
		result := map[string]any{}
		if method == "initialize" {
			result = map[string]any{
				"protocolVersion": "2024-11-05",
				"serverInfo":      map[string]string{"name": controlledMCPServerName, "version": "1"},
				"capabilities":    map[string]any{"tools": map[string]any{}},
			}
		} else if method == "tools/list" {
			var tools []map[string]any
			for _, toolName := range controlledToolNames {
				tools = append(tools, map[string]any{"name": toolName, "inputSchema": map[string]any{"type": "object"}})
			}
			result = map[string]any{"tools": tools}
		}
		encodeHelper(t, encoder, map[string]any{"id": id, "jsonrpc": "2.0", "result": result})
	}
}

func writeHelperRecord(t *testing.T, record helperRecord) {
	t.Helper()
	logFile, openErr := os.OpenFile(os.Getenv(helperLogEnv), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if openErr != nil {
		t.Fatalf("failed to open helper log: %v", openErr)
	}
	if encodeErr := json.NewEncoder(logFile).Encode(record); encodeErr != nil {
		_ = logFile.Close()
		t.Fatalf("failed to write helper log: %v", encodeErr)
	}
	if closeErr := logFile.Close(); closeErr != nil {
		t.Fatalf("failed to close helper log: %v", closeErr)
	}
}

func readHelperRecords(t *testing.T, logPath string) []helperRecord {
	t.Helper()
	content, readErr := os.ReadFile(logPath)
	if readErr != nil {
		t.Fatalf("failed to read helper log: %v", readErr)
	}
	var records []helperRecord
	scanner := bufio.NewScanner(strings.NewReader(string(content)))
	for scanner.Scan() {
		var record helperRecord
		if unmarshalErr := json.Unmarshal(scanner.Bytes(), &record); unmarshalErr != nil {
			t.Fatalf("failed to parse helper record: %v", unmarshalErr)
		}
		records = append(records, record)
	}
	return records
}

func assertInitializationOrder(t *testing.T, records []helperRecord) {
	t.Helper()
	var methods []string
	for _, record := range records {
		if record.Kind == "message" {
			methods = append(methods, record.Method)
		}
	}
	expected := []string{"initialize", "initialized", "account/read", "thread/start", "mcpServerStatus/list"}
	if len(methods) < len(expected) {
		t.Fatalf("missing initialization messages: %v", methods)
	}
	for methodIdx, expectedMethod := range expected {
		if methods[methodIdx] != expectedMethod {
			t.Fatalf("unexpected initialization order: %v", methods)
		}
	}
}

func assertSingleProcessAndThread(t *testing.T, records []helperRecord, expectedTurns int) {
	t.Helper()
	pids := make(map[int]struct{})
	threadIDs := make(map[string]struct{})
	turns := 0
	for _, record := range records {
		if record.Kind == "app_start" || record.Kind == "message" {
			pids[record.PID] = struct{}{}
		}
		if record.Method == "turn/start" {
			turns++
			threadID, _ := record.Params["threadId"].(string)
			threadIDs[threadID] = struct{}{}
		}
	}
	if len(pids) != 1 || len(threadIDs) != 1 || turns != expectedTurns {
		t.Fatalf("process/thread reuse failed: pids=%v threads=%v turns=%d records=%+v", pids, threadIDs, turns, records)
	}
}

func assertHardenedArgs(t *testing.T, records []helperRecord) {
	t.Helper()
	startRecord := firstRecord(t, records, "app_start")
	joinedArgs := strings.Join(startRecord.Args, " ")
	for _, expected := range []string{
		"--sandbox read-only",
		"--ask-for-approval never",
		`web_search="disabled"`,
		`mcp_servers.existing={ command = "/path/to/bydbctl", args = [], enabled = false }`,
		`mcp_servers.bydbctl-controlled-tools={ command = "/path/to/bydbctl"`,
		"app-server --stdio",
	} {
		if !strings.Contains(joinedArgs, expected) {
			t.Fatalf("missing hardened argument %q: %s", expected, joinedArgs)
		}
	}
}

func firstRecord(t *testing.T, records []helperRecord, methodOrKind string) helperRecord {
	t.Helper()
	for _, record := range records {
		if record.Method == methodOrKind || record.Kind == methodOrKind {
			return record
		}
	}
	t.Fatalf("helper record %q not found: %+v", methodOrKind, records)
	return helperRecord{}
}

func helperArgs(args []string) []string {
	for argIdx, arg := range args {
		if arg == "--" && argIdx+1 < len(args) {
			return args[argIdx+1:]
		}
	}
	return nil
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", `'\''`) + "'"
}
