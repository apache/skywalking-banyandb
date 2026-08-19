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

//go:build !windows

package claude

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/agent"
)

func TestGatewayInterruptKillsClaudeDescendants(t *testing.T) {
	argumentLog := filepath.Join(t.TempDir(), "arguments.log")
	childPIDPath := filepath.Join(t.TempDir(), "child.pid")
	t.Setenv("CLAUDE_FAKE_ARGUMENT_LOG", argumentLog)
	t.Setenv("CLAUDE_FAKE_CHILD_PID", childPIDPath)
	t.Setenv("CLAUDE_FAKE_MODE", "block-child")
	gateway := newTestGateway(t)
	session, startErr := gateway.Start(context.Background(), agent.StartRequest{Provider: agentProviderClaude})
	if startErr != nil {
		t.Fatalf("Start returned an error: %v", startErr)
	}
	events, sendErr := gateway.Send(context.Background(), session.ID, agent.TurnRequest{Prompt: "wait"})
	if sendErr != nil {
		t.Fatalf("Send returned an error: %v", sendErr)
	}
	waitForFile(t, childPIDPath)
	childPIDContent, readErr := os.ReadFile(childPIDPath)
	if readErr != nil {
		t.Fatalf("failed to read child PID: %v", readErr)
	}
	childPID, parseErr := strconv.Atoi(strings.TrimSpace(string(childPIDContent)))
	if parseErr != nil {
		t.Fatalf("failed to parse child PID: %v", parseErr)
	}
	if interruptErr := gateway.Interrupt(context.Background(), session.ID); interruptErr != nil {
		t.Fatalf("Interrupt returned an error: %v", interruptErr)
	}
	for event := range events {
		_ = event
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		signalErr := syscall.Kill(childPID, 0)
		if errors.Is(signalErr, syscall.ESRCH) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("Claude descendant process %d survived interruption", childPID)
}
