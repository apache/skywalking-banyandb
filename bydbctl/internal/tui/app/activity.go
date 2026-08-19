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

package app

import (
	"fmt"
	"strings"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/agent"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/tuitext"
)

const maxActivityEntries = 200

type activityEntry struct {
	category string
	title    string
	detail   string
}

func (m *Model) recordActivity(category, title, detail string) {
	title = strings.TrimSpace(title)
	if title == "" {
		return
	}
	m.activityLog = append(m.activityLog, activityEntry{
		category: category,
		title:    title,
		detail:   strings.TrimSpace(detail),
	})
	if len(m.activityLog) > maxActivityEntries {
		m.activityLog = m.activityLog[len(m.activityLog)-maxActivityEntries:]
	}
}

func (m *Model) recordAgentActivities(events []agent.Event) {
	for _, event := range events {
		if !m.shouldRecordAgentActivity(event) {
			continue
		}
		m.recordActivity(activityCategory(event), activityTitle(event), activityDetail(event))
		if m.querySession != nil && event.Kind == agent.EventKindToolCall && strings.TrimSpace(event.ToolName) != "" {
			toolDetail := strings.TrimSpace(event.InputDetail)
			if toolDetail == "" {
				toolDetail = strings.TrimSpace(event.InputSummary)
			}
			m.querySession.AddChatMessage(session.ChatMessage{
				Role:      session.ChatRoleTool,
				ToolName:  event.ToolName,
				Content:   fallback(tuitext.SingleLine(event.InputSummary), event.ToolName),
				Detail:    toolDetail,
				CreatedAt: event.StartedAt,
			})
		}
	}
}

func (m *Model) shouldRecordAgentActivity(event agent.Event) bool {
	if event.Kind == agent.EventKindMessageDelta {
		return false
	}
	return shouldShowAgentEvent(event)
}

func activityCategory(event agent.Event) string {
	switch event.Kind {
	case agent.EventKindToolCall, agent.EventKindToolResult:
		return "tool"
	case agent.EventKindCandidate:
		return "candidate"
	case agent.EventKindClarification:
		return "clarification"
	case agent.EventKindCancelled:
		return "canceled"
	case agent.EventKindPlanUpdate:
		return "plan"
	case agent.EventKindMessageDelta:
		return "output"
	case agent.EventKindError:
		return "error"
	case agent.EventKindPermissionRequest:
		return "policy"
	default:
		return "agent"
	}
}

func activityTitle(event agent.Event) string {
	switch event.Kind {
	case agent.EventKindToolCall, agent.EventKindToolResult:
		toolName := fallback(event.ToolName, "tool")
		return fmt.Sprintf("tool %s: %s", toolName, fallback(string(event.Status), "updated"))
	case agent.EventKindCandidate:
		if event.Status == agent.EventStatusFailed {
			return "candidate: draft"
		}
		return "candidate: validated"
	case agent.EventKindClarification:
		return "agent question: " + fallback(tuitext.SingleLine(event.Message), "clarification needed")
	case agent.EventKindCancelled:
		return "canceled: " + fallback(tuitext.SingleLine(event.Message), "agent action")
	case agent.EventKindPlanUpdate:
		if strings.TrimSpace(event.Message) != "" {
			return "plan: " + tuitext.SingleLine(event.Message)
		}
		return "plan update"
	case agent.EventKindMessageDelta:
		return "output: " + truncateRunes(tuitext.SingleLine(event.Message), 96)
	case agent.EventKindFinalResponse:
		if strings.TrimSpace(event.Candidate) != "" {
			return "agent: BYDBQL candidate"
		}
		return "agent: response"
	case agent.EventKindError:
		if event.Err != nil {
			return "error: " + event.Err.Error()
		}
		return "error"
	case agent.EventKindPermissionRequest:
		return "permission: " + fallback(tuitext.SingleLine(event.Message), "denied by workflow")
	default:
		if strings.TrimSpace(event.Message) != "" {
			return string(event.Kind) + ": " + tuitext.SingleLine(event.Message)
		}
		return string(event.Kind)
	}
}

func activityDetail(event agent.Event) string {
	var parts []string
	if strings.TrimSpace(event.Candidate) != "" {
		parts = append(parts, "candidate="+event.Candidate)
	}
	if strings.TrimSpace(event.InputSummary) != "" {
		parts = append(parts, "input="+event.InputSummary)
	}
	if strings.TrimSpace(event.OutputSummary) != "" {
		parts = append(parts, "output="+event.OutputSummary)
	}
	if strings.TrimSpace(event.Message) != "" {
		parts = append(parts, event.Message)
	}
	if strings.TrimSpace(event.Explanation) != "" {
		parts = append(parts, event.Explanation)
	}
	if strings.TrimSpace(event.Permission) != "" {
		parts = append(parts, event.Permission)
	}
	return strings.Join(parts, "\n")
}

func (m *Model) recordExecutionActivity(querySession *session.QuerySession) {
	if querySession == nil {
		return
	}
	executionResult := querySession.ExecutionResult
	if executionResult.Summary == "" && executionResult.Error == "" && executionResult.Response == "" {
		return
	}
	title := fmt.Sprintf("execution: %s", executionResult.Summary)
	if executionResult.Error != "" {
		title = "execution failed: " + executionResult.Error
	}
	detailParts := []string{
		fmt.Sprintf("command=%s", executionResult.Command),
		fmt.Sprintf("path=%s", executionResult.Path),
		fmt.Sprintf("rows=%d", executionResult.Rows),
	}
	if executionResult.Hint != "" {
		detailParts = append(detailParts, "hint="+executionResult.Hint)
	}
	m.recordActivity("execution", title, strings.Join(detailParts, "\n"))
}
