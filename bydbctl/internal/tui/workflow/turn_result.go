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

package workflow

import (
	"strings"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/agent"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/bridge"
)

// A turn's outcome is read back out of its event stream rather than tracked as it runs, so the same
// helpers serve both the streaming and the blocking entry points.

func finalCandidate(events []agent.Event) string {
	if candidateEvent := finalProposeCandidateEvent(events); candidateEvent != nil {
		if candidateEvent.Status == agent.EventStatusFailed || candidateEvent.Status == agent.EventStatusCancelled {
			return ""
		}
		return cleanBydbqlCandidate(candidateEvent.Candidate)
	}
	return ""
}

func finalProposeCandidateEvent(events []agent.Event) *agent.Event {
	for eventIdx := len(events) - 1; eventIdx >= 0; eventIdx-- {
		event := events[eventIdx]
		if event.Kind != agent.EventKindCandidate || event.Origin != agent.EventOriginToolBridge || event.ToolName != bridge.ToolProposeQueryPlan {
			continue
		}
		if candidate := cleanBydbqlCandidate(event.Candidate); candidate != "" {
			copiedEvent := event
			copiedEvent.Candidate = candidate
			return &copiedEvent
		}
	}
	return nil
}

// agentOutputText collects the readable output of a turn that produced no candidate.
//
// The text keeps its line breaks: the conversation panel formats headings and lists from them, and
// the caller normalizes a single-line copy for the message headline.
func agentOutputText(events []agent.Event) string {
	for eventIdx := len(events) - 1; eventIdx >= 0; eventIdx-- {
		event := events[eventIdx]
		if event.Origin != agent.EventOriginToolBridge && event.Kind == agent.EventKindFinalResponse && strings.TrimSpace(event.Message) != "" {
			return strings.TrimSpace(event.Message)
		}
	}
	var messages []string
	for _, event := range events {
		if event.Origin == agent.EventOriginToolBridge || event.Kind == agent.EventKindPlanUpdate {
			continue
		}
		if strings.TrimSpace(event.Message) != "" {
			messages = append(messages, strings.TrimSpace(event.Message))
		}
	}
	return strings.Join(messages, "\n")
}

func finalExplanation(events []agent.Event) string {
	for eventIdx := len(events) - 1; eventIdx >= 0; eventIdx-- {
		event := events[eventIdx]
		if event.Origin == agent.EventOriginToolBridge {
			continue
		}
		if strings.TrimSpace(event.Explanation) != "" {
			return strings.TrimSpace(event.Explanation)
		}
		if strings.TrimSpace(event.Message) != "" {
			return strings.TrimSpace(event.Message)
		}
	}
	return "agent returned a BYDBQL candidate"
}

func finalClarification(events []agent.Event) string {
	for eventIdx := len(events) - 1; eventIdx >= 0; eventIdx-- {
		event := events[eventIdx]
		if event.Kind == agent.EventKindClarification && strings.TrimSpace(event.Message) != "" {
			return strings.TrimSpace(event.Message)
		}
	}
	return ""
}

func containsUncontrolledBydbql(events []agent.Event) bool {
	var outputParts []string
	for _, event := range events {
		if event.Origin == agent.EventOriginToolBridge {
			continue
		}
		outputParts = append(outputParts, event.Candidate, event.Message, event.Explanation)
	}
	normalizedText := strings.ToUpper(RepairFragmentedQuery(strings.Join(outputParts, " ")))
	if strings.Contains(normalizedText, "SHOW TOP ") && strings.Contains(normalizedText, " FROM MEASURE ") {
		return true
	}
	if !strings.Contains(normalizedText, "SELECT ") {
		return false
	}
	for _, resourceType := range []string{"MEASURE", "STREAM", "TRACE", "PROPERTY"} {
		if strings.Contains(normalizedText, " FROM "+resourceType+" ") {
			return true
		}
	}
	return false
}
