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

package bridge

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/planner"
)

// MCP tool arguments arrive as untyped JSON, so they are read defensively here and summarized into
// the short labels and detail blocks the TUI activity log shows.

func jsonResult(value any) Result {
	encodedValue, marshalErr := json.Marshal(value)
	if marshalErr != nil {
		return Result{Err: fmt.Errorf("failed to encode tool result: %w", marshalErr)}
	}
	return Result{Content: string(encodedValue)}
}

func stringArgument(arguments map[string]any, name string) string {
	if arguments == nil {
		return ""
	}
	value, ok := arguments[name].(string)
	if !ok {
		return ""
	}
	return strings.TrimSpace(value)
}

func stringSliceArgument(arguments map[string]any, name string) []string {
	if arguments == nil {
		return nil
	}
	switch value := arguments[name].(type) {
	case string:
		return trimmedNonEmpty(strings.Split(value, ","))
	case []string:
		return trimmedNonEmpty(value)
	case []any:
		groups := make([]string, 0, len(value))
		for _, item := range value {
			if group, ok := item.(string); ok {
				groups = append(groups, group)
			}
		}
		return trimmedNonEmpty(groups)
	default:
		return nil
	}
}

// trimmedNonEmpty drops blank entries but keeps duplicates, since a repeated group in a tool
// argument is the caller's error to report rather than something to silently collapse.
func trimmedNonEmpty(values []string) []string {
	compactedValues := make([]string, 0, len(values))
	for _, value := range values {
		if trimmedValue := strings.TrimSpace(value); trimmedValue != "" {
			compactedValues = append(compactedValues, trimmedValue)
		}
	}
	return compactedValues
}

func summarizeArguments(arguments map[string]any) string {
	if len(arguments) == 0 {
		return "no parameters"
	}
	if query := stringArgument(arguments, "query"); query != "" {
		trimmedQuery := strings.Join(strings.Fields(query), " ")
		if len(trimmedQuery) > 120 {
			return "query=" + trimmedQuery[:120] + "..."
		}
		return "query=" + trimmedQuery
	}
	if planValue, hasPlan := arguments["plan"]; hasPlan {
		return "plan=" + summarizePlanArgument(planValue)
	}
	if workflowValue, hasWorkflow := arguments["workflow"]; hasWorkflow {
		return "workflow=" + summarizePlanArgument(workflowValue)
	}
	keys := make([]string, 0, len(arguments))
	for key := range arguments {
		keys = append(keys, key)
	}
	return "parameters=" + strings.Join(keys, ",")
}

func formatArgumentsDetail(arguments map[string]any) string {
	if len(arguments) == 0 {
		return ""
	}
	if query := stringArgument(arguments, "query"); query != "" {
		return "query:\n" + strings.TrimSpace(query)
	}
	if planValue, hasPlan := arguments["plan"]; hasPlan {
		return formatJSONDetailSection("plan", planValue)
	}
	if workflowValue, hasWorkflow := arguments["workflow"]; hasWorkflow {
		return formatJSONDetailSection("workflow", workflowValue)
	}
	return formatJSONDetailSection("parameters", arguments)
}

func formatJSONDetailSection(label string, value any) string {
	encodedValue, marshalErr := json.MarshalIndent(value, "", "  ")
	if marshalErr != nil {
		return label + ":\n" + fmt.Sprint(value)
	}
	return label + ":\n" + string(encodedValue)
}

func summarizePlanArgument(value any) string {
	encodedValue, marshalErr := json.Marshal(value)
	if marshalErr != nil {
		return "structured plan"
	}
	trimmedValue := strings.TrimSpace(string(encodedValue))
	if len(trimmedValue) > 120 {
		return trimmedValue[:120] + "..."
	}
	return trimmedValue
}

func summarizeResult(result Result) string {
	if result.Err != nil {
		return result.Err.Error()
	}
	if result.Content == "" {
		return "completed"
	}
	return fmt.Sprintf("result=%d characters", len([]rune(result.Content)))
}

func schemaNotReadyMessage(step int, resource planner.Resource) string {
	groupLabel := strings.Join(resource.Groups, ", ")
	if groupLabel == "" {
		groupLabel = "<group>"
	}
	return fmt.Sprintf(
		"query plan step %d: call describe_schema for %s %s in %s before propose_query_plan; use only typed columns from describe_schema",
		step,
		resource.Type,
		resource.Name,
		groupLabel,
	)
}
