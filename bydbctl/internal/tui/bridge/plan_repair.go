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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bridge

import (
	"fmt"
	"strings"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/agent"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
)

// MaxPlanRepairAttempts is the number of compile/validation repairs allowed per describe_schema cycle.
const MaxPlanRepairAttempts = 8

func planFailurePayload(querySession *session.QuerySession, message string, step int, attempt int, draftQuery string) map[string]any {
	payload := map[string]any{
		"valid":              false,
		"message":            message,
		"schema_hint":        queryPlanSchemaHint(),
		"attempt":            attempt,
		"attempts_remaining": attemptsRemaining(attempt),
		"repair_hint":        repairHintForMessage(message),
	}
	if strings.TrimSpace(draftQuery) != "" {
		payload["draft_query"] = draftQuery
	}
	if step > 0 {
		payload["step"] = step
	}
	if querySession == nil {
		return payload
	}
	snapshot := querySession.SchemaSnapshot
	if snapshot.Loaded && len(snapshot.Columns) > 0 {
		payload["columns"] = columnsForProvider(snapshot.Columns)
		payload["indexed_fields"] = append([]string(nil), snapshot.IndexedFields...)
	}
	if planExample := buildDescribePlanExample(snapshot); planExample != nil {
		payload["plan_example"] = planExample
	}
	return payload
}

func attemptsRemaining(attempt int) int {
	remaining := MaxPlanRepairAttempts - attempt
	if remaining < 0 {
		return 0
	}
	return remaining
}

func repairHintForMessage(message string) string {
	lowerMessage := strings.ToLower(message)
	switch {
	case strings.Contains(lowerMessage, "describe_schema"):
		return "Call describe_schema for the target resource, then resubmit propose_query_plan using only returned columns."
	case strings.Contains(lowerMessage, "typed schema metadata is required"):
		return "Replace unknown columns with names from columns or indexed_fields in the latest describe_schema result."
	case strings.Contains(lowerMessage, "order by column") && strings.Contains(lowerMessage, "not indexed"):
		return "Use TIME or a column from indexed_fields for order_by.column, or omit order_by."
	case strings.Contains(lowerMessage, "unsupported filter operator"):
		return "Use =, !=, >, >=, <, <=, IN, or NOT IN for filter.operator."
	case strings.Contains(lowerMessage, "requires exactly one of plan or workflow"):
		return "Submit {\"plan\":{...}} with resource nested inside plan, not at the root."
	case strings.Contains(lowerMessage, "invalid query plan") || strings.Contains(lowerMessage, "failed to decode"):
		return "Copy plan_example from describe_schema or the tool error payload and fill only known columns."
	case strings.Contains(lowerMessage, "repair limit"):
		return "Call describe_schema again to reset the repair budget, then submit one corrected plan."
	default:
		return "Fix the reported issue, keep the correct resource and time_range, and call propose_query_plan again."
	}
}

func buildDescribePlanExample(snapshot session.SchemaSnapshot) map[string]any {
	if !snapshot.Loaded || strings.TrimSpace(snapshot.Name) == "" || len(snapshot.Groups) == 0 {
		return nil
	}
	groups := make([]string, 0, len(snapshot.Groups))
	for _, group := range snapshot.Groups {
		trimmedGroup := strings.TrimSpace(group)
		if trimmedGroup != "" {
			groups = append(groups, trimmedGroup)
		}
	}
	if len(groups) == 0 {
		return nil
	}
	resource := map[string]any{
		"type":   snapshot.Type.String(),
		"name":   snapshot.Name,
		"groups": groups,
	}
	planExample := map[string]any{
		"resource":   resource,
		"time_range": map[string]any{"start": "-30m"},
		"limit":      10,
	}
	if len(snapshot.Columns) > 0 {
		projection := make([]map[string]any, 0, minInt(3, len(snapshot.Columns)))
		for columnIdx, column := range snapshot.Columns {
			if columnIdx >= 3 {
				break
			}
			if column.Type == session.SchemaValueTypeUnknown {
				continue
			}
			projection = append(projection, map[string]any{"column": column.Name})
		}
		if len(projection) > 0 {
			planExample["projection"] = projection
		}
	}
	return map[string]any{"plan": planExample}
}

func minInt(left, right int) int {
	if left < right {
		return left
	}
	return right
}

func planRepairLimitMessage() string {
	return fmt.Sprintf(
		"automatic query plan repair limit reached after %d attempts; call describe_schema to reset the repair budget, submit one corrected plan, and do not call validate_bydbql, probe_bydbql, or execute_bydbql until propose_query_plan returns valid=true",
		MaxPlanRepairAttempts,
	)
}

func (toolBridge *ToolBridge) emitProposeCandidate(callID, query string, draft bool, message string) {
	trimmedQuery := strings.TrimSpace(query)
	if trimmedQuery == "" {
		return
	}
	status := agent.EventStatusSucceeded
	eventMessage := strings.TrimSpace(message)
	if draft {
		status = agent.EventStatusFailed
		if eventMessage == "" {
			eventMessage = "query plan draft (propose_query_plan has not validated yet)"
		}
	} else if eventMessage == "" {
		eventMessage = "query plan compiled through controlled tool"
	}
	toolBridge.emit(agent.Event{
		ID:          callID,
		Kind:        agent.EventKindCandidate,
		ToolName:    ToolProposeQueryPlan,
		Candidate:   trimmedQuery,
		Message:     eventMessage,
		Status:      status,
		CompletedAt: toolBridge.now(),
	})
}
