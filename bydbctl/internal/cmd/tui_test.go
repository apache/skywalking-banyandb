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
)

func TestAgentCommandUsesACPOnly(t *testing.T) {
	agentCmd := newAgentCmd()
	agentFlag := agentCmd.Flags().Lookup("agent")
	if agentFlag == nil {
		t.Fatal("agent flag was not registered")
	}
	if agentFlag.DefValue != agentProviderCodexACP {
		t.Fatalf("expected default agent %q, got %q", agentProviderCodexACP, agentFlag.DefValue)
	}
	for _, removedFlag := range []string{"agent-model", "agent-base-url"} {
		if agentCmd.Flags().Lookup(removedFlag) != nil {
			t.Fatalf("API-backed agent flag %q is still registered", removedFlag)
		}
	}
}

func TestNewAgentGatewayCreatesDefaultACPProvider(t *testing.T) {
	agentGateway, gatewayErr := newAgentGateway(agentProviderCodexACP, "", nil, t.TempDir(), nil)
	if gatewayErr != nil {
		t.Fatalf("newAgentGateway returned error: %v", gatewayErr)
	}
	if agentGateway == nil {
		t.Fatal("newAgentGateway returned a nil gateway")
	}
}

func TestNewAgentGatewayRequiresCustomACPCommand(t *testing.T) {
	agentGateway, gatewayErr := newAgentGateway(agentProviderACP, " ", nil, t.TempDir(), nil)
	if gatewayErr == nil {
		t.Fatalf("expected an error, got gateway %#v", agentGateway)
	}
	if !strings.Contains(gatewayErr.Error(), "--acp-command is required") {
		t.Fatalf("unexpected error: %v", gatewayErr)
	}
}

func TestNewAgentGatewayRejectsNonACPProvider(t *testing.T) {
	agentGateway, gatewayErr := newAgentGateway("builtin", "", nil, t.TempDir(), nil)
	if gatewayErr == nil {
		t.Fatalf("expected an error, got gateway %#v", agentGateway)
	}
	if !strings.Contains(gatewayErr.Error(), "use codex-acp or acp") {
		t.Fatalf("unexpected error: %v", gatewayErr)
	}
}
