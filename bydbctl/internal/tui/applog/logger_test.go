// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package applog

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/agent"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
)

func TestNewWritesSessionLog(t *testing.T) {
	tempDir := t.TempDir()
	sessionLog, createErr := New(tempDir)
	if createErr != nil {
		t.Fatalf("failed to create session log: %v", createErr)
	}
	defer func() {
		_ = sessionLog.Close()
	}()
	sessionLog.WriteAgentTurn([]agent.Event{
		{Kind: agent.EventKindMessageDelta, Message: "agent raw output"},
		{Kind: agent.EventKindFinalResponse, Message: "agent raw output"},
	})
	sessionLog.WriteError("workflow", os.ErrInvalid)
	logBytes, readErr := os.ReadFile(sessionLog.Path())
	if readErr != nil {
		t.Fatalf("failed to read log file: %v", readErr)
	}
	logContent := string(logBytes)
	for _, expected := range []string{"agent_turn", "non_empty_deltas=1", "workflow", os.ErrInvalid.Error()} {
		if !strings.Contains(logContent, expected) {
			t.Fatalf("expected log to contain %q:\n%s", expected, logContent)
		}
	}
	if strings.Contains(logContent, "agent raw output") {
		t.Fatalf("provider output must not be persisted by default:\n%s", logContent)
	}
	if !strings.HasPrefix(sessionLog.Path(), filepath.Join(tempDir, "agent-")) {
		t.Fatalf("unexpected log path: %s", sessionLog.Path())
	}
	fileInfo, statErr := os.Stat(sessionLog.Path())
	if statErr != nil {
		t.Fatalf("failed to stat session log: %v", statErr)
	}
	if fileInfo.Mode().Perm() != 0o600 {
		t.Fatalf("unexpected session log permissions: %o", fileInfo.Mode().Perm())
	}
}

// A schema that appeared and vanished has to be diagnosable from the log, so the snapshot, the
// conversation entries, and the panel state that decides what renders are all recorded.
func TestWritesSchemaAndViewDiagnostics(t *testing.T) {
	tempDir := t.TempDir()
	sessionLog, createErr := New(tempDir)
	if createErr != nil {
		t.Fatalf("failed to create session log: %v", createErr)
	}
	defer func() {
		_ = sessionLog.Close()
	}()
	sessionLog.WriteSchemaSnapshot("schema_answer", session.SchemaSnapshot{
		Loaded: true, Type: session.ResourceTypeTrace, Name: "segment", Groups: []string{"sw_trace"},
		Columns: []session.SchemaColumn{
			{Name: "trace_id", Kind: session.SchemaColumnTag, Type: session.SchemaValueTypeString, Indexed: true},
		},
		EntityTags: []string{"trace_id"},
	})
	sessionLog.WriteChatMessages([]session.ChatMessage{
		{Role: session.ChatRoleAssistant, Kind: session.ChatMessageKindSchema, Content: "schema TRACE segment in sw_trace", Detail: "## TRACE segment"},
	})
	sessionLog.WriteViewState("schema answer applied · evidence=schema-pinned")

	logBytes, readErr := os.ReadFile(sessionLog.Path())
	if readErr != nil {
		t.Fatalf("failed to read log file: %v", readErr)
	}
	logContent := string(logBytes)
	for _, expected := range []string{
		"schema_answer", "name=segment", "loaded=true", "trace_id:tag/string",
		"chat", "kind=schema", "detail_bytes=16",
		"view", "evidence=schema-pinned",
	} {
		if !strings.Contains(logContent, expected) {
			t.Fatalf("expected log to contain %q:\n%s", expected, logContent)
		}
	}
}
