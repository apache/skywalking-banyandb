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

package bridge

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const (
	mcpProtocolVersion = "2024-11-05"
	bridgeSocketName   = "tools.sock"
	maxMCPLineSize     = 4 * 1024 * 1024
)

// SocketServer serves private bridge calls over a per-session Unix socket.
type SocketServer struct {
	bridge     *ToolBridge
	listener   net.Listener
	socketPath string
	directory  string
	done       chan struct{}
	closeOnce  sync.Once
}

// StartSocketServer starts a private IPC listener for one TUI process.
func StartSocketServer(toolBridge *ToolBridge) (*SocketServer, error) {
	if toolBridge == nil {
		return nil, fmt.Errorf("tool bridge is required")
	}
	directory, mkdirErr := os.MkdirTemp("", "bydbctl-agent-")
	if mkdirErr != nil {
		return nil, fmt.Errorf("failed to create private bridge directory: %w", mkdirErr)
	}
	socketPath := filepath.Join(directory, bridgeSocketName)
	listener, listenErr := net.Listen("unix", socketPath)
	if listenErr != nil {
		_ = os.RemoveAll(directory)
		return nil, fmt.Errorf("failed to listen on private bridge socket: %w", listenErr)
	}
	if chmodErr := os.Chmod(socketPath, 0o600); chmodErr != nil {
		_ = listener.Close()
		_ = os.RemoveAll(directory)
		return nil, fmt.Errorf("failed to secure private bridge socket: %w", chmodErr)
	}
	socketServer := &SocketServer{
		bridge:     toolBridge,
		listener:   listener,
		socketPath: socketPath,
		directory:  directory,
		done:       make(chan struct{}),
	}
	go socketServer.accept()
	return socketServer, nil
}

// Path returns the private Unix-socket path.
func (socketServer *SocketServer) Path() string {
	if socketServer == nil {
		return ""
	}
	return socketServer.socketPath
}

// MCPServerConfig returns the only MCP server configuration given to an ACP session.
func (socketServer *SocketServer) MCPServerConfig(executable string) []any {
	if socketServer == nil || strings.TrimSpace(executable) == "" {
		return []any{}
	}
	return []any{map[string]any{
		"name":    "bydbctl-controlled-tools",
		"command": executable,
		"args":    []string{"agent-tool-bridge", "--socket", socketServer.Path()},
		"env":     []any{},
	}}
}

// Close stops the private socket server and removes its directory.
func (socketServer *SocketServer) Close() error {
	if socketServer == nil {
		return nil
	}
	var closeErr error
	socketServer.closeOnce.Do(func() {
		close(socketServer.done)
		closeErr = socketServer.listener.Close()
		removeErr := os.RemoveAll(socketServer.directory)
		if closeErr == nil && removeErr != nil {
			closeErr = fmt.Errorf("failed to remove private bridge directory: %w", removeErr)
		}
	})
	return closeErr
}

func (socketServer *SocketServer) accept() {
	for {
		connection, acceptErr := socketServer.listener.Accept()
		if acceptErr != nil {
			select {
			case <-socketServer.done:
				return
			default:
				continue
			}
		}
		go socketServer.handleConnection(connection)
	}
}

func (socketServer *SocketServer) handleConnection(connection net.Conn) {
	defer func() {
		_ = connection.Close()
	}()
	decoder := json.NewDecoder(io.LimitReader(connection, maxMCPLineSize))
	var request bridgeRequest
	if decodeErr := decoder.Decode(&request); decodeErr != nil {
		return
	}
	result := socketServer.bridge.Call(connectionContext(connection), request.Call)
	_ = json.NewEncoder(connection).Encode(bridgeResponse{Result: result.Content, Error: errorString(result.Err)})
}

// ServeMCP serves one standard MCP stdio process connected to the private socket.
func ServeMCP(socketPath string, input io.Reader, output io.Writer) error {
	if strings.TrimSpace(socketPath) == "" {
		return fmt.Errorf("private bridge socket is required")
	}
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 0, 64*1024), maxMCPLineSize)
	encoder := json.NewEncoder(output)
	for scanner.Scan() {
		var request mcpRequest
		if unmarshalErr := json.Unmarshal(scanner.Bytes(), &request); unmarshalErr != nil {
			continue
		}
		if request.ID == nil {
			continue
		}
		response := mcpResponse{JSONRPC: "2.0", ID: request.ID}
		switch request.Method {
		case "initialize":
			response.Result = map[string]any{
				"protocolVersion": mcpProtocolVersion,
				"serverInfo":      map[string]string{"name": "bydbctl-controlled-tools", "version": "1"},
				"capabilities":    map[string]any{"tools": map[string]any{}},
			}
		case "tools/list":
			response.Result = map[string]any{"tools": toolDefinitions()}
		case "tools/call":
			toolName, arguments := toolCallArguments(request.Params)
			result, callErr := callSocket(socketPath, Call{Name: toolName, Arguments: arguments})
			if callErr != nil {
				response.Result = toolErrorResult(callErr)
			} else {
				response.Result = toolTextResult(result)
			}
		default:
			response.Error = map[string]any{"code": -32601, "message": "method not found"}
		}
		if encodeErr := encoder.Encode(response); encodeErr != nil {
			return fmt.Errorf("failed to write MCP response: %w", encodeErr)
		}
	}
	if scanErr := scanner.Err(); scanErr != nil {
		return fmt.Errorf("failed to read MCP input: %w", scanErr)
	}
	return nil
}

func callSocket(socketPath string, call Call) (string, error) {
	connection, dialErr := net.Dial("unix", socketPath)
	if dialErr != nil {
		return "", fmt.Errorf("failed to connect to private tool bridge: %w", dialErr)
	}
	defer func() {
		_ = connection.Close()
	}()
	if encodeErr := json.NewEncoder(connection).Encode(bridgeRequest{Call: call}); encodeErr != nil {
		return "", fmt.Errorf("failed to send private tool request: %w", encodeErr)
	}
	var response bridgeResponse
	if decodeErr := json.NewDecoder(io.LimitReader(connection, maxMCPLineSize)).Decode(&response); decodeErr != nil {
		return "", fmt.Errorf("failed to read private tool response: %w", decodeErr)
	}
	if response.Error != "" {
		return "", fmt.Errorf("%s", response.Error)
	}
	return response.Result, nil
}

func toolDefinitions() []map[string]any {
	return []map[string]any{
		{
			"name":        ToolListGroupsSchemas,
			"description": "List BanyanDB groups and registered schemas available to this TUI session.",
			"inputSchema": map[string]any{"type": "object", "properties": map[string]any{}},
		},
		{
			"name":        ToolDescribeSchema,
			"description": "Describe a BanyanDB schema and typed columns for one ranked catalog resource. Call this before propose_query_plan for that resource.",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"type":   map[string]any{"type": "string"},
					"name":   map[string]any{"type": "string"},
					"groups": map[string]any{"type": "array", "items": map[string]string{"type": "string"}},
				},
			},
		},
		{
			"name":        ToolProposeQueryPlan,
			"description": "Submit a typed query plan after describe_schema returns columns for the same resource. This is the only path that registers a BYDBQL candidate in bydbctl.",
			"inputSchema": proposeQueryPlanInputSchema(),
		},
		{
			"name":        ToolValidateBydbQL,
			"description": "Parse/safety-check one read-only BYDBQL statement. This does not register a workspace candidate and does not replace propose_query_plan.",
			"inputSchema": map[string]any{
				"type":       "object",
				"required":   []string{"query"},
				"properties": map[string]any{"query": map[string]string{"type": "string"}},
			},
		},
		{
			"name":        ToolProbeBydbQL,
			"description": "Run a bounded read-only probe for the exact query returned by the latest successful propose_query_plan.",
			"inputSchema": map[string]any{
				"type":       "object",
				"required":   []string{"query"},
				"properties": map[string]any{"query": map[string]string{"type": "string"}},
			},
		},
		{
			"name":        ToolExecuteBydbQL,
			"description": "Request one-time user approval and execute exactly one validated BYDBQL statement.",
			"inputSchema": map[string]any{
				"type":       "object",
				"required":   []string{"query"},
				"properties": map[string]any{"query": map[string]string{"type": "string"}},
			},
		},
	}
}

func toolCallArguments(params map[string]any) (string, map[string]any) {
	toolName, _ := params["name"].(string)
	arguments, _ := params["arguments"].(map[string]any)
	return strings.TrimSpace(toolName), arguments
}

func toolTextResult(content string) map[string]any {
	return map[string]any{"content": []map[string]string{{"type": "text", "text": content}}}
}

func toolErrorResult(callErr error) map[string]any {
	return map[string]any{
		"content": []map[string]string{{"type": "text", "text": callErr.Error()}},
		"isError": true,
	}
}

func connectionContext(connection net.Conn) context.Context {
	return context.Background()
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

type bridgeRequest struct {
	Call Call `json:"call"`
}

type bridgeResponse struct {
	Result string `json:"result"`
	Error  string `json:"error,omitempty"`
}

type mcpRequest struct {
	Params  map[string]any `json:"params,omitempty"`
	JSONRPC string         `json:"jsonrpc"`
	ID      any            `json:"id"`
	Method  string         `json:"method"`
}

type mcpResponse struct {
	Result  any            `json:"result,omitempty"`
	Error   map[string]any `json:"error,omitempty"`
	JSONRPC string         `json:"jsonrpc"`
	ID      any            `json:"id"`
}
