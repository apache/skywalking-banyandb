// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package claude

import (
	"github.com/anthropics/anthropic-sdk-go"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/agent"
)

// convertTools maps the closed bydbctl tool schemas onto Anthropic tool definitions.
// The bridge owns the schemas; this only reshapes them so the model sees the same
// contract codex sees, including the propose_query_plan oneOf/$defs schema.
func convertTools(definitions []map[string]any) []anthropic.ToolUnionParam {
	tools := make([]anthropic.ToolUnionParam, 0, len(definitions))
	for _, definition := range definitions {
		toolParam := anthropic.ToolParam{
			Name:        stringValue(definition, "name"),
			Description: anthropic.String(stringValue(definition, "description")),
			InputSchema: inputSchemaParam(mapValue(definition, "inputSchema")),
		}
		tools = append(tools, anthropic.ToolUnionParam{OfTool: &toolParam})
	}
	return tools
}

// inputSchemaParam builds an Anthropic input schema from a bydbctl tool schema map.
// Anthropic requires type:"object" (the SDK default), so named fields carry the
// properties and required list while everything else (oneOf, $defs, additionalProperties)
// passes through ExtraFields verbatim.
func inputSchemaParam(schema map[string]any) anthropic.ToolInputSchemaParam {
	schemaParam := anthropic.ToolInputSchemaParam{}
	if properties, hasProperties := schema["properties"]; hasProperties {
		schemaParam.Properties = properties
	}
	schemaParam.Required = stringSliceValue(schema["required"])
	extraFields := make(map[string]any, len(schema))
	for key, value := range schema {
		switch key {
		case "type", "properties", "required":
			continue
		default:
			extraFields[key] = value
		}
	}
	if len(extraFields) > 0 {
		schemaParam.ExtraFields = extraFields
	}
	return schemaParam
}

func stringValue(definition map[string]any, key string) string {
	if definition == nil {
		return ""
	}
	value, ok := definition[key].(string)
	if !ok {
		return ""
	}
	return value
}

func mapValue(definition map[string]any, key string) map[string]any {
	if definition == nil {
		return nil
	}
	value, ok := definition[key].(map[string]any)
	if !ok {
		return nil
	}
	return value
}

func stringSliceValue(value any) []string {
	switch items := value.(type) {
	case []string:
		return append([]string(nil), items...)
	case []any:
		result := make([]string, 0, len(items))
		for _, item := range items {
			if text, ok := item.(string); ok {
				result = append(result, text)
			}
		}
		return result
	default:
		return nil
	}
}

// errorEvent reports a terminal provider error to the workflow runner.
func errorEvent(turnErr error) agent.Event {
	return agent.Event{
		Kind:    agent.EventKindError,
		Message: turnErr.Error(),
		Origin:  agent.EventOriginProvider,
		Err:     turnErr,
	}
}
