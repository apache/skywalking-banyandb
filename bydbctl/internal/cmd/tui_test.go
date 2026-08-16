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

package cmd

import (
	"strings"
	"testing"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/agent"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/agent/claude"
)

func TestAgentCommandRegistersProviderFlags(t *testing.T) {
	agentCmd := newAgentCmd()
	if agentCmd.Flags().Lookup("agent") != nil {
		t.Fatal("agent provider selector should not be registered")
	}
	providerFlag := agentCmd.Flags().Lookup("provider")
	if providerFlag == nil {
		t.Fatal("provider flag was not registered")
	}
	if providerFlag.DefValue != agentProviderCodex {
		t.Fatalf("expected default provider %q, got %q", agentProviderCodex, providerFlag.DefValue)
	}
	codexCommandFlag := agentCmd.Flags().Lookup("codex-command")
	if codexCommandFlag == nil {
		t.Fatal("codex-command flag was not registered")
	}
	if codexCommandFlag.DefValue != "codex" {
		t.Fatalf("expected default Codex command, got %q", codexCommandFlag.DefValue)
	}
	for _, claudeFlag := range []string{"claude-command", "claude-model", "claude-api-key", "claude-base-url", "claude-max-turns"} {
		if agentCmd.Flags().Lookup(claudeFlag) == nil {
			t.Fatalf("Claude flag %q was not registered", claudeFlag)
		}
	}
	if agentCmd.Flags().Lookup("claude-max-tokens") != nil {
		t.Fatal("obsolete claude-max-tokens flag is still registered")
	}
	for _, removedFlag := range []string{"agent-model", "agent-base-url", "acp-command", "acp-arg", "mcp-config"} {
		if agentCmd.Flags().Lookup(removedFlag) != nil {
			t.Fatalf("API-backed agent flag %q is still registered", removedFlag)
		}
	}
}

func TestNewAgentGatewayUsesProvidedCodexCommand(t *testing.T) {
	mcpServer := testControlledMCPServer(t)
	agentGateway, gatewayErr := newAgentGateway(agentProviderCodex, "/custom/codex", t.TempDir(), mcpServer, claude.Config{})
	if gatewayErr != nil {
		t.Fatalf("newAgentGateway returned error: %v", gatewayErr)
	}
	if agentGateway == nil {
		t.Fatal("newAgentGateway returned a nil gateway")
	}
}

func TestNewAgentGatewayRequiresCodexCommand(t *testing.T) {
	agentGateway, gatewayErr := newAgentGateway(agentProviderCodex, " ", t.TempDir(), testControlledMCPServer(t), claude.Config{})
	if gatewayErr == nil {
		t.Fatalf("expected an error, got gateway %#v", agentGateway)
	}
	if !strings.Contains(gatewayErr.Error(), "--codex-command is required") {
		t.Fatalf("unexpected error: %v", gatewayErr)
	}
}

func TestNewAgentGatewayClaudeBranch(t *testing.T) {
	agentGateway, gatewayErr := newAgentGateway(agentProviderClaude, "", t.TempDir(), testControlledMCPServer(t), claude.Config{Command: "/custom/claude"})
	if gatewayErr != nil {
		t.Fatalf("newAgentGateway returned error: %v", gatewayErr)
	}
	if agentGateway == nil {
		t.Fatal("newAgentGateway returned a nil gateway")
	}
	if _, ok := agentGateway.(*claude.Gateway); !ok {
		t.Fatalf("expected *claude.Gateway, got %T", agentGateway)
	}
}

func TestNewAgentGatewayRequiresClaudeCommand(t *testing.T) {
	agentGateway, gatewayErr := newAgentGateway(agentProviderClaude, "", t.TempDir(), testControlledMCPServer(t), claude.Config{Command: " "})
	if gatewayErr == nil {
		t.Fatalf("expected an error, got gateway %#v", agentGateway)
	}
	if !strings.Contains(gatewayErr.Error(), "--claude-command is required") {
		t.Fatalf("unexpected error: %v", gatewayErr)
	}
}

func TestNewAgentGatewayRejectsUnknownProvider(t *testing.T) {
	agentGateway, gatewayErr := newAgentGateway("bogus", "", t.TempDir(), agent.ControlledMCPServer{}, claude.Config{})
	if gatewayErr == nil {
		t.Fatalf("expected an error, got gateway %#v", agentGateway)
	}
	if !strings.Contains(gatewayErr.Error(), "unknown agent provider") {
		t.Fatalf("unexpected error: %v", gatewayErr)
	}
}

func testControlledMCPServer(t *testing.T) agent.ControlledMCPServer {
	t.Helper()
	return agent.ControlledMCPServer{
		Name:         "bydbctl-controlled-tools",
		Command:      "/path/to/bydbctl",
		Args:         []string{"agent-tool-bridge", "--socket", "/tmp/tools.sock"},
		EnabledTools: []string{"list_groups_schemas", "describe_schema", "propose_query_plan", "validate_bydbql", "execute_bydbql"},
	}
}
