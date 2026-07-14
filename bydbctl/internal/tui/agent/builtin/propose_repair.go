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

package builtin

import (
	"encoding/json"
	"strings"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/bridge"
)

const proposeRepairNudge = "propose_query_plan returned valid=false. Read message, repair_hint, columns, and plan_example from the tool result, then call propose_query_plan again with a corrected typed plan. Do not end the turn until valid=true or the repair limit is reached."

func proposeToolFailed(toolName, toolResult string) bool {
	if toolName != bridge.ToolProposeQueryPlan {
		return false
	}
	payload := map[string]any{}
	if decodeErr := json.Unmarshal([]byte(strings.TrimSpace(toolResult)), &payload); decodeErr != nil {
		return strings.Contains(strings.ToLower(toolResult), `"valid":false`) ||
			strings.Contains(strings.ToLower(toolResult), "repair limit")
	}
	validValue, hasValid := payload["valid"].(bool)
	return hasValid && !validValue
}

func proposeToolSucceeded(toolName, toolResult string) bool {
	if toolName != bridge.ToolProposeQueryPlan {
		return false
	}
	payload := map[string]any{}
	if decodeErr := json.Unmarshal([]byte(strings.TrimSpace(toolResult)), &payload); decodeErr != nil {
		return strings.Contains(strings.ToLower(toolResult), `"valid":true`)
	}
	validValue, hasValid := payload["valid"].(bool)
	return hasValid && validValue
}
