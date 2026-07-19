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

// Package codex provides a fail-closed Codex app-server gateway for bydbctl.
package codex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/agent"
)

const (
	defaultCommand          = "codex"
	controlledMCPServerName = "bydbctl-controlled-tools"
	minimumCodexMajor       = 0
	minimumCodexMinor       = 144
	minimumCodexPatch       = 5
)

var (
	codexVersionPattern  = regexp.MustCompile(`codex-cli\s+v?(\d+)\.(\d+)\.(\d+)`)
	mcpServerNamePattern = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)
)

var controlledToolNames = []string{
	"list_groups_schemas",
	"describe_schema",
	"propose_query_plan",
	"validate_bydbql",
	"probe_bydbql",
	"execute_bydbql",
}

var disabledFeatures = []string{
	"apps",
	"auth_elicitation",
	"browser_use",
	"browser_use_external",
	"browser_use_full_cdp_access",
	"computer_use",
	"enable_mcp_apps",
	"goals",
	"hooks",
	"image_generation",
	"in_app_browser",
	"memories",
	"multi_agent",
	"plugins",
	"plugin_sharing",
	"remote_plugin",
	"shell_snapshot",
	"shell_tool",
	"skill_mcp_dependency_install",
	"tool_call_mcp_elicitation",
	"tool_suggest",
	"unified_exec",
	"workspace_dependencies",
}

// Config configures one isolated Codex app-server process.
type Config struct {
	Command             string
	WorkingDirectory    string
	ControlledMCPServer agent.ControlledMCPServer
}

// Gateway owns the single Codex process and ephemeral thread used by one TUI.
type Gateway struct {
	now     func() time.Time
	conn    *connection
	session agent.Session
	config  Config
	startMu sync.Mutex
	mu      sync.Mutex
	closed  bool
}

// NewGateway creates a Codex app-server gateway.
func NewGateway(config Config) *Gateway {
	if strings.TrimSpace(config.Command) == "" {
		config.Command = defaultCommand
	}
	return &Gateway{config: config, now: time.Now}
}

// MaintainsConversationHistory reports that the ephemeral Codex thread retains prior turns.
func (gateway *Gateway) MaintainsConversationHistory() bool {
	return true
}

// Start initializes Codex and creates the single ephemeral thread.
func (gateway *Gateway) Start(ctx context.Context, req agent.StartRequest) (agent.Session, error) {
	gateway.startMu.Lock()
	defer gateway.startMu.Unlock()
	gateway.mu.Lock()
	if gateway.closed {
		gateway.mu.Unlock()
		return agent.Session{}, errors.New("codex gateway is closed")
	}
	if gateway.conn != nil {
		existingSession := gateway.session
		gateway.mu.Unlock()
		return existingSession, nil
	}
	gateway.mu.Unlock()
	if validateErr := validateConfig(gateway.config); validateErr != nil {
		return agent.Session{}, validateErr
	}
	if versionErr := checkCodexVersion(ctx, gateway.config.Command, gateway.config.WorkingDirectory); versionErr != nil {
		return agent.Session{}, versionErr
	}
	mcpNames, listErr := configuredMCPNames(ctx, gateway.config.Command, gateway.config.WorkingDirectory)
	if listErr != nil {
		return agent.Session{}, listErr
	}
	for _, mcpName := range mcpNames {
		if mcpName == controlledMCPServerName {
			return agent.Session{}, fmt.Errorf("codex MCP server name %q is reserved by bydbctl", controlledMCPServerName)
		}
	}
	commandArgs, argsErr := appServerArgs(gateway.config.ControlledMCPServer, mcpNames)
	if argsErr != nil {
		return agent.Session{}, argsErr
	}
	appConnection, connectionErr := startConnection(
		context.WithoutCancel(ctx),
		gateway.config.Command,
		commandArgs,
		gateway.config.WorkingDirectory,
	)
	if connectionErr != nil {
		return agent.Session{}, connectionErr
	}
	if initializeErr := appConnection.initialize(ctx); initializeErr != nil {
		_ = appConnection.close()
		return agent.Session{}, fmt.Errorf("failed to initialize Codex app-server: %w", initializeErr)
	}
	if accountErr := appConnection.checkAccount(ctx); accountErr != nil {
		_ = appConnection.close()
		return agent.Session{}, accountErr
	}
	if threadErr := appConnection.startThread(ctx, gateway.config.WorkingDirectory, agent.DeveloperInstructions()); threadErr != nil {
		_ = appConnection.close()
		return agent.Session{}, fmt.Errorf("failed to start Codex thread: %w", threadErr)
	}
	if inventoryErr := appConnection.validateMCPInventory(ctx); inventoryErr != nil {
		_ = appConnection.close()
		return agent.Session{}, fmt.Errorf("failed to validate Codex MCP inventory: %w", inventoryErr)
	}
	startedSession := agent.Session{
		ID:        "codex-" + uuid.NewString(),
		Provider:  req.Provider,
		StartedAt: gateway.now(),
	}
	appConnection.localSessionID = startedSession.ID
	gateway.mu.Lock()
	if gateway.closed {
		gateway.mu.Unlock()
		_ = appConnection.close()
		return agent.Session{}, errors.New("codex gateway was closed during startup")
	}
	gateway.conn = appConnection
	gateway.session = startedSession
	gateway.mu.Unlock()
	return startedSession, nil
}

// Send starts one turn on the existing ephemeral Codex thread.
func (gateway *Gateway) Send(ctx context.Context, sessionID string, req agent.TurnRequest) (<-chan agent.Event, error) {
	appConnection, lookupErr := gateway.connection(sessionID)
	if lookupErr != nil {
		return nil, lookupErr
	}
	return appConnection.send(ctx, req)
}

// Interrupt interrupts only the active turn and preserves the process and thread.
func (gateway *Gateway) Interrupt(ctx context.Context, sessionID string) error {
	appConnection, lookupErr := gateway.connection(sessionID)
	if lookupErr != nil {
		return lookupErr
	}
	return appConnection.interrupt(ctx)
}

// Close stops the Codex process and releases the ephemeral thread.
func (gateway *Gateway) Close() error {
	gateway.startMu.Lock()
	defer gateway.startMu.Unlock()
	gateway.mu.Lock()
	if gateway.closed {
		gateway.mu.Unlock()
		return nil
	}
	gateway.closed = true
	appConnection := gateway.conn
	gateway.conn = nil
	gateway.mu.Unlock()
	if appConnection == nil {
		return nil
	}
	return appConnection.close()
}

func (gateway *Gateway) connection(sessionID string) (*connection, error) {
	gateway.mu.Lock()
	defer gateway.mu.Unlock()
	if gateway.closed {
		return nil, errors.New("codex gateway is closed")
	}
	if gateway.conn == nil || strings.TrimSpace(sessionID) == "" || sessionID != gateway.session.ID {
		return nil, fmt.Errorf("unknown Codex session %q", sessionID)
	}
	select {
	case <-gateway.conn.done:
		return nil, gateway.conn.processError()
	default:
	}
	return gateway.conn, nil
}

func validateConfig(config Config) error {
	if strings.TrimSpace(config.Command) == "" {
		return errors.New("codex command is required")
	}
	if !filepath.IsAbs(config.WorkingDirectory) {
		return errors.New("isolated Codex working directory must be absolute")
	}
	server := config.ControlledMCPServer
	if server.Name != controlledMCPServerName {
		return fmt.Errorf("controlled MCP server must be named %q", controlledMCPServerName)
	}
	if !filepath.IsAbs(server.Command) {
		return errors.New("controlled MCP server command must be absolute")
	}
	if !equalStringSets(server.EnabledTools, controlledToolNames) {
		return fmt.Errorf("controlled MCP tool allowlist must contain exactly %s", strings.Join(controlledToolNames, ", "))
	}
	return nil
}

func checkCodexVersion(ctx context.Context, command, workingDirectory string) error {
	versionCmd := exec.CommandContext(ctx, command, "--version")
	versionCmd.Dir = workingDirectory
	versionOutput, versionErr := versionCmd.CombinedOutput()
	if versionErr != nil {
		return fmt.Errorf("failed to read Codex CLI version: %w", versionErr)
	}
	matches := codexVersionPattern.FindStringSubmatch(string(versionOutput))
	if len(matches) != 4 {
		return fmt.Errorf("failed to parse Codex CLI version from %q", strings.TrimSpace(string(versionOutput)))
	}
	versionParts := make([]int, 3)
	for partIdx := range versionParts {
		parsedPart, parseErr := strconv.Atoi(matches[partIdx+1])
		if parseErr != nil {
			return fmt.Errorf("failed to parse Codex CLI version: %w", parseErr)
		}
		versionParts[partIdx] = parsedPart
	}
	minimum := []int{minimumCodexMajor, minimumCodexMinor, minimumCodexPatch}
	for partIdx, versionPart := range versionParts {
		if versionPart > minimum[partIdx] {
			return nil
		}
		if versionPart < minimum[partIdx] {
			return fmt.Errorf("codex CLI 0.144.5 or newer is required; found %d.%d.%d", versionParts[0], versionParts[1], versionParts[2])
		}
	}
	return nil
}

func configuredMCPNames(ctx context.Context, command, workingDirectory string) ([]string, error) {
	listCmd := exec.CommandContext(ctx, command, "mcp", "list", "--json")
	listCmd.Dir = workingDirectory
	listOutput, listErr := listCmd.Output()
	if listErr != nil {
		return nil, fmt.Errorf("failed to list configured Codex MCP servers: %w", listErr)
	}
	var servers []struct {
		Name string `json:"name"`
	}
	if unmarshalErr := json.Unmarshal(listOutput, &servers); unmarshalErr != nil {
		return nil, fmt.Errorf("failed to parse configured Codex MCP servers: %w", unmarshalErr)
	}
	names := make([]string, 0, len(servers))
	seen := make(map[string]struct{}, len(servers))
	for _, server := range servers {
		name := strings.TrimSpace(server.Name)
		if name == "" {
			return nil, errors.New("codex returned an MCP server with an empty name")
		}
		if _, exists := seen[name]; exists {
			return nil, fmt.Errorf("codex returned duplicate MCP server %q", name)
		}
		seen[name] = struct{}{}
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

func appServerArgs(server agent.ControlledMCPServer, configuredNames []string) ([]string, error) {
	if !equalStringSets(server.EnabledTools, controlledToolNames) {
		return nil, errors.New("invalid controlled MCP tool allowlist")
	}
	args := []string{
		"--sandbox", "read-only",
		"--ask-for-approval", "never",
		"--config", `web_search="disabled"`,
		"--config", `history.persistence="none"`,
		"--config", "project_doc_max_bytes=0",
		"--config", "project_root_markers=[]",
		"--config", `shell_environment_policy.inherit="none"`,
		"--config", "check_for_update_on_startup=false",
	}
	for _, featureName := range disabledFeatures {
		args = append(args, "--disable", featureName)
	}
	commandValue, commandErr := tomlValue(server.Command)
	if commandErr != nil {
		return nil, commandErr
	}
	for _, configuredName := range configuredNames {
		if !mcpServerNamePattern.MatchString(configuredName) {
			return nil, fmt.Errorf("codex MCP server name %q cannot be safely overridden", configuredName)
		}
		disabledServer := fmt.Sprintf("{ command = %s, args = [], enabled = false }", commandValue)
		args = append(args, "--config", mcpConfigPath(configuredName)+"="+disabledServer)
	}
	serverArgs := server.Args
	if serverArgs == nil {
		serverArgs = []string{}
	}
	argsValue, argsErr := tomlValue(serverArgs)
	if argsErr != nil {
		return nil, argsErr
	}
	toolsValue, toolsErr := tomlValue(controlledToolNames)
	if toolsErr != nil {
		return nil, toolsErr
	}
	controlledServer := fmt.Sprintf(
		"{ command = %s, args = %s, enabled = true, required = true, enabled_tools = %s }",
		commandValue,
		argsValue,
		toolsValue,
	)
	args = append(args, "--config", mcpConfigPath(controlledMCPServerName)+"="+controlledServer, "app-server", "--stdio")
	return args, nil
}

func mcpConfigPath(serverName string) string {
	return "mcp_servers." + serverName
}

func tomlValue(value any) (string, error) {
	encodedValue, marshalErr := json.Marshal(value)
	if marshalErr != nil {
		return "", fmt.Errorf("failed to encode Codex configuration value: %w", marshalErr)
	}
	return string(encodedValue), nil
}

func equalStringSets(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	leftCopy := append([]string(nil), left...)
	rightCopy := append([]string(nil), right...)
	sort.Strings(leftCopy)
	sort.Strings(rightCopy)
	for valueIdx := range leftCopy {
		if leftCopy[valueIdx] != rightCopy[valueIdx] {
			return false
		}
	}
	return true
}

func currentEnvironment() []string {
	return os.Environ()
}
