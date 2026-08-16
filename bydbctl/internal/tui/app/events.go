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
	"strings"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/agent"
)

const (
	maxUIEventRunes = 72
	maxUIErrorRunes = 88
)

func summarizeAgentEvent(event agent.Event) string {
	switch event.Kind {
	case agent.EventKindPlanUpdate:
		return "agent: planning"
	case agent.EventKindToolCall:
		return "tool: " + fallback(event.ToolName, singleLine(event.Message))
	case agent.EventKindToolResult:
		return "tool complete: " + fallback(event.ToolName, singleLine(event.Message))
	case agent.EventKindCandidate:
		return "agent: validated BYDBQL candidate ready"
	case agent.EventKindClarification:
		return "agent question: " + singleLine(event.Message)
	case agent.EventKindApproval:
		return "execution approval required"
	case agent.EventKindCancelled:
		return "agent action canceled"
	case agent.EventKindMessageDelta:
		return "agent: drafting"
	case agent.EventKindFinalResponse:
		if strings.TrimSpace(event.Candidate) != "" {
			return "agent: BYDBQL candidate ready"
		}
		return "agent: response ready"
	case agent.EventKindPermissionRequest:
		return "agent: permission denied"
	case agent.EventKindError:
		if event.Err != nil {
			return summarizeError("agent", event.Err.Error())
		}
		return "agent: failed"
	default:
		return "agent: " + string(event.Kind)
	}
}

func summarizeError(prefix, message string) string {
	trimmedMessage := strings.TrimSpace(message)
	if trimmedMessage == "" {
		return prefix + ": failed"
	}
	oneLine := singleLine(trimmedMessage)
	if idx := strings.Index(oneLine, ";"); idx > 0 {
		oneLine = strings.TrimSpace(oneLine[:idx])
	}
	return truncateRunes(prefix+": "+oneLine, maxUIErrorRunes)
}

func summarizeStatusEvent(message string) string {
	return truncateRunes(singleLine(message), maxUIEventRunes)
}

func truncateRunes(value string, maxRunes int) string {
	if maxRunes <= 3 {
		return value
	}
	runes := []rune(strings.TrimSpace(value))
	if len(runes) <= maxRunes {
		return string(runes)
	}
	return string(runes[:maxRunes-3]) + "..."
}

func formatValidationHint(message string) string {
	if strings.TrimSpace(message) == "" {
		return ""
	}
	return truncateRunes("validation: "+singleLine(message), maxUIEventRunes)
}

func formatInvalidCandidateHint(query string) string {
	if strings.TrimSpace(query) == "" {
		return ""
	}
	return truncateRunes("invalid candidate (see log)", maxUIEventRunes)
}
