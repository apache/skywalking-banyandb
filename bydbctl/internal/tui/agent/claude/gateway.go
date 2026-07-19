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

// Package claude provides an in-process Anthropic Messages API gateway for bydbctl.
package claude

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"
	"github.com/anthropics/anthropic-sdk-go/packages/ssestream"
	"github.com/google/uuid"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/agent"
)

const (
	defaultModel     = "claude-sonnet-5"
	defaultMaxTokens = int64(4096)
	apiKeyEnv        = "ANTHROPIC_API_KEY"
	agentEventBuffer = 64
)

// Config configures one in-process Anthropic Messages API gateway.
type Config struct {
	Model            string
	APIKey           string
	BaseURL          string
	MaxTokens        int64
	WorkingDirectory string
	Tools            agent.ControlledTools
}

// turnHandle ties the active turn's cancel func to the gateway so Interrupt can reach it.
type turnHandle struct {
	cancel context.CancelFunc
}

// Gateway owns one Anthropic client and the single session used by one TUI.
type Gateway struct {
	config    Config
	now       func() time.Time
	client    *anthropic.Client
	session   agent.Session
	turn      *turnHandle
	newStream func(ctx context.Context, params anthropic.MessageNewParams) *ssestream.Stream[anthropic.MessageStreamEventUnion]
	startMu   sync.Mutex
	mu        sync.Mutex
	closed    bool
}

// NewGateway creates an in-process Anthropic gateway. The HTTP client is constructed lazily at Start.
func NewGateway(config Config) *Gateway {
	if strings.TrimSpace(config.Model) == "" {
		config.Model = defaultModel
	}
	if config.MaxTokens <= 0 {
		config.MaxTokens = defaultMaxTokens
	}
	return &Gateway{config: config, now: time.Now}
}

// MaintainsConversationHistory reports that each Send rebuilds messages from the payload.
func (gateway *Gateway) MaintainsConversationHistory() bool {
	return false
}

// Start validates configuration and constructs the Anthropic client.
func (gateway *Gateway) Start(_ context.Context, req agent.StartRequest) (agent.Session, error) {
	gateway.startMu.Lock()
	defer gateway.startMu.Unlock()
	gateway.mu.Lock()
	if gateway.closed {
		gateway.mu.Unlock()
		return agent.Session{}, errors.New("claude gateway is closed")
	}
	if gateway.client != nil {
		existingSession := gateway.session
		gateway.mu.Unlock()
		return existingSession, nil
	}
	gateway.mu.Unlock()
	if strings.TrimSpace(gateway.config.APIKey) == "" {
		gateway.config.APIKey = strings.TrimSpace(os.Getenv(apiKeyEnv))
	}
	if validateErr := validateConfig(gateway.config); validateErr != nil {
		return agent.Session{}, validateErr
	}
	clientOptions := []option.RequestOption{option.WithAPIKey(gateway.config.APIKey)}
	if strings.TrimSpace(gateway.config.BaseURL) != "" {
		clientOptions = append(clientOptions, option.WithBaseURL(gateway.config.BaseURL))
	}
	anthropicClient := anthropic.NewClient(clientOptions...)
	startedSession := agent.Session{
		ID:        "claude-" + uuid.NewString(),
		Provider:  req.Provider,
		StartedAt: gateway.now(),
	}
	gateway.mu.Lock()
	if gateway.closed {
		gateway.mu.Unlock()
		return agent.Session{}, errors.New("claude gateway was closed during startup")
	}
	gateway.client = &anthropicClient
	if gateway.newStream == nil {
		gateway.newStream = func(ctx context.Context, params anthropic.MessageNewParams) *ssestream.Stream[anthropic.MessageStreamEventUnion] {
			return anthropicClient.Messages.NewStreaming(ctx, params)
		}
	}
	gateway.session = startedSession
	gateway.mu.Unlock()
	return startedSession, nil
}

// Send starts one Anthropic turn and streams provider-neutral events.
func (gateway *Gateway) Send(ctx context.Context, sessionID string, req agent.TurnRequest) (<-chan agent.Event, error) {
	if lookupErr := gateway.requireSession(sessionID); lookupErr != nil {
		return nil, lookupErr
	}
	turnCtx, cancelTurn := context.WithCancel(ctx)
	handle := &turnHandle{cancel: cancelTurn}
	gateway.setTurn(handle)
	events := make(chan agent.Event, agentEventBuffer)
	go func() {
		defer gateway.clearTurn(handle)
		defer cancelTurn()
		gateway.runTurn(turnCtx, req, events)
	}()
	return events, nil
}

// Interrupt cancels the active turn's context.
func (gateway *Gateway) Interrupt(context.Context, string) error {
	gateway.mu.Lock()
	handle := gateway.turn
	gateway.mu.Unlock()
	if handle == nil {
		return nil
	}
	handle.cancel()
	return nil
}

// Close marks the gateway closed and cancels any active turn. There is no subprocess to terminate.
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
	gateway.mu.Unlock()
	if handle != nil {
		handle.cancel()
	}
	return nil
}

func (gateway *Gateway) requireSession(sessionID string) error {
	gateway.mu.Lock()
	defer gateway.mu.Unlock()
	if gateway.closed {
		return errors.New("claude gateway is closed")
	}
	if gateway.client == nil || gateway.newStream == nil || strings.TrimSpace(sessionID) == "" || sessionID != gateway.session.ID {
		return fmt.Errorf("unknown Claude session %q", sessionID)
	}
	return nil
}

func (gateway *Gateway) setTurn(handle *turnHandle) {
	gateway.mu.Lock()
	gateway.turn = handle
	gateway.mu.Unlock()
}

func (gateway *Gateway) clearTurn(handle *turnHandle) {
	gateway.mu.Lock()
	if gateway.turn == handle {
		gateway.turn = nil
	}
	gateway.mu.Unlock()
}

func validateConfig(config Config) error {
	if strings.TrimSpace(config.APIKey) == "" {
		return fmt.Errorf("claude API key is required (set --claude-api-key or %s)", apiKeyEnv)
	}
	if config.Tools == nil {
		return errors.New("claude controlled tools bridge is required")
	}
	return nil
}
