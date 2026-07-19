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
	if agentCmd.Flags().Lookup("agent") != nil {
		t.Fatal("agent provider selector should not be registered")
	}
	acpCommandFlag := agentCmd.Flags().Lookup("acp-command")
	if acpCommandFlag == nil {
		t.Fatal("acp-command flag was not registered")
	}
	if acpCommandFlag.DefValue != "" {
		t.Fatalf("expected no default ACP command, got %q", acpCommandFlag.DefValue)
	}
	for _, removedFlag := range []string{"agent-model", "agent-base-url"} {
		if agentCmd.Flags().Lookup(removedFlag) != nil {
			t.Fatalf("API-backed agent flag %q is still registered", removedFlag)
		}
	}
}

func TestAgentCommandRequiresACPCommand(t *testing.T) {
	agentCmd := newAgentCmd()
	agentCmd.SetArgs(nil)
	executeErr := agentCmd.Execute()
	if executeErr == nil {
		t.Fatal("expected agent command to require an ACP command")
	}
	if !strings.Contains(executeErr.Error(), "--acp-command is required") {
		t.Fatalf("unexpected error: %v", executeErr)
	}
}

func TestNewAgentGatewayUsesProvidedACPCommand(t *testing.T) {
	agentGateway, gatewayErr := newAgentGateway("npx", []string{"-y", "custom-acp-provider"}, t.TempDir(), nil)
	if gatewayErr != nil {
		t.Fatalf("newAgentGateway returned error: %v", gatewayErr)
	}
	if agentGateway == nil {
		t.Fatal("newAgentGateway returned a nil gateway")
	}
}

func TestNewAgentGatewayRequiresCustomACPCommand(t *testing.T) {
	agentGateway, gatewayErr := newAgentGateway(" ", nil, t.TempDir(), nil)
	if gatewayErr == nil {
		t.Fatalf("expected an error, got gateway %#v", agentGateway)
	}
	if !strings.Contains(gatewayErr.Error(), "--acp-command is required") {
		t.Fatalf("unexpected error: %v", gatewayErr)
	}
}
