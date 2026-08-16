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

// Package applog writes detailed bydbctl agent TUI diagnostics to a session log file.
package applog

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/agent"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
)

const defaultLogDirName = ".bydbctl/logs"

// Logger appends structured session diagnostics to a log file.
type Logger struct {
	mu   sync.Mutex
	file *os.File
	path string
}

// New creates a timestamped agent session log under dir or $HOME/.bydbctl/logs.
func New(dir string) (*Logger, error) {
	logDir := strings.TrimSpace(dir)
	if logDir == "" {
		homeDir, homeErr := os.UserHomeDir()
		if homeErr != nil {
			logDir = filepath.Join(os.TempDir(), "bydbctl", "logs")
		} else {
			logDir = filepath.Join(homeDir, defaultLogDirName)
		}
	}
	if mkdirErr := os.MkdirAll(logDir, 0o700); mkdirErr != nil {
		return nil, fmt.Errorf("failed to create log directory: %w", mkdirErr)
	}
	logPath := filepath.Join(logDir, fmt.Sprintf("agent-%s.log", time.Now().Format("20060102-150405")))
	logFile, openErr := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if openErr != nil {
		return nil, fmt.Errorf("failed to open log file: %w", openErr)
	}
	sessionLogger := &Logger{
		file: logFile,
		path: logPath,
	}
	sessionLogger.Write("session", "bydbctl agent TUI session log created")
	return sessionLogger, nil
}

// Path returns the absolute log file path.
func (sessionLogger *Logger) Path() string {
	if sessionLogger == nil {
		return ""
	}
	return sessionLogger.path
}

// Close closes the underlying log file.
func (sessionLogger *Logger) Close() error {
	if sessionLogger == nil || sessionLogger.file == nil {
		return nil
	}
	sessionLogger.mu.Lock()
	defer sessionLogger.mu.Unlock()
	closeErr := sessionLogger.file.Close()
	sessionLogger.file = nil
	return closeErr
}

// Write appends one categorized log line.
func (sessionLogger *Logger) Write(category, message string) {
	if sessionLogger == nil || sessionLogger.file == nil {
		return
	}
	trimmedMessage := strings.TrimSpace(message)
	if trimmedMessage == "" {
		return
	}
	sessionLogger.mu.Lock()
	defer sessionLogger.mu.Unlock()
	_, _ = fmt.Fprintf(sessionLogger.file, "[%s] %s: %s\n", time.Now().Format(time.RFC3339), category, trimmedMessage)
}

// WriteError appends an error with optional wrapped context.
func (sessionLogger *Logger) WriteError(category string, err error) {
	if err == nil {
		return
	}
	sessionLogger.Write(category, err.Error())
}

// WriteAgentTurn appends one summary line per agent turn instead of per delta event.
func (sessionLogger *Logger) WriteAgentTurn(events []agent.Event) {
	if sessionLogger == nil || len(events) == 0 {
		return
	}
	deltaCount := 0
	kindCounts := make(map[string]int)
	var candidate string
	var agentErr error
	var toolFailures []string
	for _, event := range events {
		kindCounts[string(event.Kind)]++
		switch event.Kind {
		case agent.EventKindMessageDelta:
			if strings.TrimSpace(event.Message) != "" {
				deltaCount++
			}
		case agent.EventKindFinalResponse:
			if strings.TrimSpace(event.Candidate) != "" {
				candidate = event.Candidate
			}
		case agent.EventKindError:
			if event.Err != nil {
				agentErr = event.Err
			}
		case agent.EventKindToolResult:
			if event.Status == agent.EventStatusFailed && strings.TrimSpace(event.Message) != "" {
				toolFailures = append(toolFailures, event.ToolName+": "+event.Message)
			}
		}
	}
	parts := []string{fmt.Sprintf("events=%d", len(events))}
	if deltaCount > 0 {
		parts = append(parts, fmt.Sprintf("non_empty_deltas=%d", deltaCount))
	}
	if len(kindCounts) > 0 {
		parts = append(parts, "kinds="+summarizeKindCounts(kindCounts))
	}
	if len(toolFailures) > 0 {
		parts = append(parts, "tool_failures="+strings.Join(toolFailures, "; "))
	}
	if candidate != "" {
		parts = append(parts, "candidate="+truncateLogField(candidate))
	}
	if agentErr != nil {
		parts = append(parts, "error="+agentErr.Error())
	}
	sessionLogger.Write("agent_turn", strings.Join(parts, " | "))
}

func summarizeKindCounts(kindCounts map[string]int) string {
	if len(kindCounts) == 0 {
		return ""
	}
	kinds := make([]string, 0, len(kindCounts))
	for kind := range kindCounts {
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	parts := make([]string, 0, len(kinds))
	for _, kind := range kinds {
		parts = append(parts, fmt.Sprintf("%s=%d", kind, kindCounts[kind]))
	}
	return strings.Join(parts, ",")
}

func truncateLogField(value string) string {
	trimmedValue := strings.Join(strings.Fields(value), " ")
	const maxLogFieldLength = 500
	if len(trimmedValue) <= maxLogFieldLength {
		return trimmedValue
	}
	return trimmedValue[:maxLogFieldLength] + "..."
}

// WriteQuerySession appends the current workflow snapshot.
func (sessionLogger *Logger) WriteQuerySession(querySession *session.QuerySession) {
	if sessionLogger == nil || querySession == nil {
		return
	}
	sessionLogger.Write("session", fmt.Sprintf(
		"phase=%s goal=%q resource=%s/%s groups=%v validation=%s",
		querySession.Phase,
		querySession.UserGoal,
		querySession.ResourceType,
		querySession.ResourceName,
		querySession.Groups,
		querySession.Validation.Message,
	))
	if currentCandidate := querySession.CurrentCandidate(); currentCandidate != nil {
		sessionLogger.Write("candidate", currentCandidate.Query)
	}
	if querySession.ExecutionResult.Summary != "" || querySession.ExecutionResult.Error != "" || querySession.ExecutionResult.Hint != "" {
		executionResult := querySession.ExecutionResult
		sessionLogger.Write("execution", fmt.Sprintf(
			"resource_type=%s duration=%s rows=%d preview_rows=%d truncated=%t summary=%q error=%q hint=%q",
			executionResult.ResourceType,
			executionResult.Duration,
			executionResult.Rows,
			len(executionResult.Preview),
			executionResult.Truncated,
			executionResult.Summary,
			executionResult.Error,
			executionResult.Hint,
		))
	}
}

// WriteSchemaSnapshot appends the resource description a schema lookup or discovery produced.
//
// The columns are named rather than counted, so a lookup that returned the wrong resource, or an
// empty one, is identifiable from the log without reproducing the run.
func (sessionLogger *Logger) WriteSchemaSnapshot(category string, snapshot session.SchemaSnapshot) {
	if sessionLogger == nil {
		return
	}
	columnNames := make([]string, 0, len(snapshot.Columns))
	for _, column := range snapshot.Columns {
		columnNames = append(columnNames, fmt.Sprintf("%s:%s/%s", column.Name, column.Kind, column.Type))
	}
	sessionLogger.Write(category, fmt.Sprintf(
		"type=%s name=%s groups=%v loaded=%t columns=%d tags=%d fields=%d entity=%v indexed=%v sortable=%d trace_id=%s timestamp=%s source_measure=%s column_list=%s",
		snapshot.Type,
		snapshot.Name,
		snapshot.Groups,
		snapshot.Loaded,
		len(snapshot.Columns),
		len(snapshot.Tags),
		len(snapshot.Fields),
		snapshot.EntityTags,
		snapshot.IndexedFields,
		len(snapshot.SortableIndexes),
		snapshot.TraceIDTag,
		snapshot.TimestampTag,
		snapshot.SourceMeasure,
		truncateLogField(strings.Join(columnNames, ",")),
	))
}

// WriteChatMessages appends one line per conversation entry, so a message that never rendered is
// still distinguishable from one that was never recorded.
func (sessionLogger *Logger) WriteChatMessages(messages []session.ChatMessage) {
	if sessionLogger == nil || len(messages) == 0 {
		return
	}
	for messageIndex, message := range messages {
		sessionLogger.Write("chat", fmt.Sprintf("#%d role=%s kind=%s tool=%s content=%q detail_bytes=%d",
			messageIndex,
			message.Role,
			message.Kind,
			message.ToolName,
			truncateLogField(message.Content),
			len(message.Detail),
		))
	}
}

// WriteViewState appends the panel state a frame would render from.
//
// Rendering is derived state, so a panel that appears and then vanishes is diagnosed by comparing
// these lines across the frames around it rather than by reading the view itself.
func (sessionLogger *Logger) WriteViewState(state string) {
	sessionLogger.Write("view", state)
}

// DisplayPath shortens the log path for the TUI, preferring ~/ when possible.
func DisplayPath(path string) string {
	trimmedPath := strings.TrimSpace(path)
	if trimmedPath == "" {
		return ""
	}
	homeDir, homeErr := os.UserHomeDir()
	if homeErr != nil {
		return trimmedPath
	}
	if strings.HasPrefix(trimmedPath, homeDir) {
		return "~" + strings.TrimPrefix(trimmedPath, homeDir)
	}
	return trimmedPath
}
