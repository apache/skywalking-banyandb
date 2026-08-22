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

// Package claude provides a fail-closed Claude CLI gateway for bydbctl.
package claude

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/agent"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/tuirun"
)

const (
	defaultCommand          = "claude"
	defaultModel            = "sonnet"
	defaultMaxTurns         = 12
	controlledMCPServerName = agent.ControlledMCPServerName
	agentEventBuffer        = 64
)

var (
	claudeVersionPattern = regexp.MustCompile(`^\d+\.\d+\.\d+ \(Claude Code\)$`)
	controlledToolNames  = agent.ControlledToolNames()
)

// Config configures one Claude CLI gateway.
type Config struct {
	Command             string
	Model               string
	APIKey              string
	BaseURL             string
	WorkingDirectory    string
	ControlledMCPServer agent.ControlledMCPServer
	MaxTurns            int
}

// turnHandle owns the cancellation and process for one Claude CLI turn.
type turnHandle struct {
	cancel      context.CancelFunc
	process     *os.Process
	interrupted bool
}

// Gateway owns one local session and starts one Claude CLI process per turn.
type Gateway struct {
	now               func() time.Time
	turn              *turnHandle
	session           agent.Session
	providerSessionID string
	config            Config
	startMu           sync.Mutex
	mu                sync.Mutex
	started           bool
	closed            bool
}

// NewGateway creates a Claude CLI gateway.
func NewGateway(config Config) *Gateway {
	if strings.TrimSpace(config.Command) == "" {
		config.Command = defaultCommand
	}
	if strings.TrimSpace(config.Model) == "" {
		config.Model = defaultModel
	}
	if config.MaxTurns <= 0 {
		config.MaxTurns = defaultMaxTurns
	}
	return &Gateway{config: config, now: time.Now}
}

// MaintainsConversationHistory reports that resumed Claude CLI sessions retain prior turns.
func (gateway *Gateway) MaintainsConversationHistory() bool {
	return true
}

// Start validates the isolated CLI configuration and checks that Claude Code is available.
func (gateway *Gateway) Start(ctx context.Context, req agent.StartRequest) (agent.Session, error) {
	gateway.startMu.Lock()
	defer gateway.startMu.Unlock()
	gateway.mu.Lock()
	if gateway.closed {
		gateway.mu.Unlock()
		return agent.Session{}, errors.New("claude gateway is closed")
	}
	if gateway.started {
		existingSession := gateway.session
		gateway.mu.Unlock()
		return existingSession, nil
	}
	gateway.mu.Unlock()
	if strings.TrimSpace(gateway.config.WorkingDirectory) == "" {
		gateway.config.WorkingDirectory = req.WorkingDirectory
	}
	if validateErr := validateConfig(gateway.config); validateErr != nil {
		return agent.Session{}, validateErr
	}
	if versionErr := checkClaudeVersion(ctx, gateway.config.Command, gateway.config.WorkingDirectory); versionErr != nil {
		return agent.Session{}, versionErr
	}
	startedSession := agent.Session{
		ID:        "claude-" + uuid.NewString(),
		Provider:  req.Provider,
		StartedAt: gateway.now(),
	}
	gateway.mu.Lock()
	defer gateway.mu.Unlock()
	if gateway.closed {
		return agent.Session{}, errors.New("claude gateway was closed during startup")
	}
	gateway.session = startedSession
	gateway.started = true
	return startedSession, nil
}

// Send starts one Claude CLI turn and streams provider-neutral events.
func (gateway *Gateway) Send(ctx context.Context, sessionID string, req agent.TurnRequest) (<-chan agent.Event, error) {
	parts, promptErr := agent.BuildBydbqlPromptParts(req)
	if promptErr != nil {
		return nil, fmt.Errorf("failed to build Claude turn input: %w", promptErr)
	}
	turnCtx, cancelTurn := context.WithCancel(ctx)
	handle := &turnHandle{cancel: cancelTurn}
	gateway.mu.Lock()
	if sessionErr := gateway.requireSessionLocked(sessionID); sessionErr != nil {
		gateway.mu.Unlock()
		cancelTurn()
		return nil, sessionErr
	}
	if gateway.turn != nil {
		gateway.mu.Unlock()
		cancelTurn()
		return nil, errors.New("a Claude turn is already active")
	}
	providerSessionID := gateway.providerSessionID
	gateway.turn = handle
	gateway.mu.Unlock()
	events := make(chan agent.Event, agentEventBuffer)
	tuirun.Go(turnCtx, "claude-turn", func(runCtx context.Context) {
		gateway.runTurn(runCtx, handle, providerSessionID, parts, events)
	})
	return events, nil
}

// Interrupt stops the active Claude CLI process while retaining its provider session ID.
func (gateway *Gateway) Interrupt(_ context.Context, sessionID string) error {
	gateway.mu.Lock()
	if sessionErr := gateway.requireSessionLocked(sessionID); sessionErr != nil {
		gateway.mu.Unlock()
		return sessionErr
	}
	handle := gateway.turn
	if handle == nil {
		gateway.mu.Unlock()
		return nil
	}
	process := cancelTurnLocked(handle)
	gateway.mu.Unlock()
	if process == nil {
		return nil
	}
	if killErr := killProcessTree(process); killErr != nil {
		return fmt.Errorf("failed to interrupt Claude CLI: %w", killErr)
	}
	return nil
}

// Close marks the gateway closed and stops any active Claude CLI process.
func (gateway *Gateway) Close() error {
	gateway.startMu.Lock()
	defer gateway.startMu.Unlock()
	gateway.mu.Lock()
	if gateway.closed {
		gateway.mu.Unlock()
		return nil
	}
	gateway.closed = true
	handle := gateway.turn
	var process *os.Process
	if handle != nil {
		process = cancelTurnLocked(handle)
	}
	gateway.mu.Unlock()
	if process == nil {
		return nil
	}
	if killErr := killProcessTree(process); killErr != nil {
		return fmt.Errorf("failed to stop Claude CLI: %w", killErr)
	}
	return nil
}

func (gateway *Gateway) requireSessionLocked(sessionID string) error {
	if gateway.closed {
		return errors.New("claude gateway is closed")
	}
	if !gateway.started || strings.TrimSpace(sessionID) == "" || sessionID != gateway.session.ID {
		return fmt.Errorf("unknown Claude session %q", sessionID)
	}
	return nil
}

func (gateway *Gateway) attachProcess(handle *turnHandle, process *os.Process) bool {
	gateway.mu.Lock()
	defer gateway.mu.Unlock()
	if gateway.turn != handle || handle.interrupted || gateway.closed {
		return false
	}
	handle.process = process
	return true
}

func (gateway *Gateway) finishTurn(handle *turnHandle, providerSessionID string) {
	gateway.mu.Lock()
	defer gateway.mu.Unlock()
	if gateway.turn != handle {
		return
	}
	if strings.TrimSpace(providerSessionID) != "" {
		gateway.providerSessionID = providerSessionID
	}
	gateway.turn = nil
}

func (gateway *Gateway) turnWasInterrupted(handle *turnHandle) bool {
	gateway.mu.Lock()
	defer gateway.mu.Unlock()
	return handle.interrupted
}

func cancelTurnLocked(handle *turnHandle) *os.Process {
	handle.interrupted = true
	process := handle.process
	handle.cancel()
	return process
}

func validateConfig(config Config) error {
	if strings.TrimSpace(config.Command) == "" {
		return errors.New("claude command is required")
	}
	if !filepath.IsAbs(config.WorkingDirectory) {
		return errors.New("isolated Claude working directory must be absolute")
	}
	serverErr := agent.ValidateControlledMCPServer(config.ControlledMCPServer)
	if serverErr != nil {
		return fmt.Errorf("invalid controlled MCP server: %w", serverErr)
	}
	return nil
}

func checkClaudeVersion(ctx context.Context, command, workingDirectory string) error {
	versionCmd := exec.CommandContext(ctx, command, "--version")
	versionCmd.Dir = workingDirectory
	versionOutput, versionErr := versionCmd.CombinedOutput()
	if versionErr != nil {
		return fmt.Errorf("failed to read Claude CLI version: %w", versionErr)
	}
	version := strings.TrimSpace(string(versionOutput))
	if !claudeVersionPattern.MatchString(version) {
		return fmt.Errorf("failed to parse Claude CLI version from %q", version)
	}
	return nil
}
