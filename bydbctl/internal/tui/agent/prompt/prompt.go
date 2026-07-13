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

// Package prompt builds BYDBQL generation prompts for agent adapters.
// Workflow guidance is aligned with skills/bydbql/SKILL.md and references/.
package prompt

import (
	"bytes"
	_ "embed"
	"strings"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/approval"
)

//go:embed references/syntax.md
var syntaxReference string

// Input carries rendered prompt inputs for BYDBQL generation.
type Input struct {
	TaskPrompt      string
	PayloadJSON     string
	Candidate       string
	ExecutionPolicy string
}

// Build renders the provider prompt for BYDBQL generation or revision.
func Build(input Input) string {
	if strings.TrimSpace(input.Candidate) == "" {
		return buildInitial(input)
	}
	return buildRevise(input)
}

func buildInitial(input Input) string {
	taskPrompt := strings.TrimSpace(input.TaskPrompt)
	if taskPrompt == "" {
		taskPrompt = "Continue the BanyanDB conversation. Discover schemas when useful and submit a structured query plan only when the request is ready."
	}
	var promptBuffer bytes.Buffer
	writeRole(&promptBuffer)
	promptBuffer.WriteString("Task:\n")
	promptBuffer.WriteString(taskPrompt)
	promptBuffer.WriteString("\n\n")
	writeWorkflow(&promptBuffer, true)
	writeHardRules(&promptBuffer, true, input.ExecutionPolicy)
	writeNLRules(&promptBuffer)
	writeReferences(&promptBuffer)
	writeOutputContract(&promptBuffer)
	promptBuffer.WriteString("Context JSON:\n")
	promptBuffer.WriteString(input.PayloadJSON)
	return promptBuffer.String()
}

func buildRevise(input Input) string {
	taskPrompt := strings.TrimSpace(input.TaskPrompt)
	if taskPrompt == "" {
		taskPrompt = "Revise the structured query plan using validation or execution feedback in the context JSON."
	}
	var promptBuffer bytes.Buffer
	writeRole(&promptBuffer)
	promptBuffer.WriteString("Task:\n")
	promptBuffer.WriteString(taskPrompt)
	promptBuffer.WriteString("\n\n")
	writeWorkflow(&promptBuffer, false)
	writeHardRules(&promptBuffer, false, input.ExecutionPolicy)
	writeNLRules(&promptBuffer)
	writeReferences(&promptBuffer)
	writeOutputContract(&promptBuffer)
	promptBuffer.WriteString("Context JSON:\n")
	promptBuffer.WriteString(input.PayloadJSON)
	return promptBuffer.String()
}

func writeRole(prompt *bytes.Buffer) {
	prompt.WriteString("You are a BanyanDB query workspace assistant.\n")
	prompt.WriteString("Have a useful multi-turn conversation, discover schemas, and produce typed query plans when a query is ready.\n\n")
}

func writeWorkflow(prompt *bytes.Buffer, initial bool) {
	prompt.WriteString("Controlled workflow (follow in order when the user asks for a query):\n")
	prompt.WriteString("1. If the group, resource name, or type is missing or ambiguous, call list_groups_schemas.\n")
	prompt.WriteString("2. Pick a resource from schema.ranked_candidates, then call describe_schema for that exact resource before any propose_query_plan call.\n")
	prompt.WriteString("3. Build a typed plan using only columns and indexed_fields returned by describe_schema. Never invent tags, fields, groups, or indexes.\n")
	prompt.WriteString("4. Submit the plan with propose_query_plan and do not end the turn until it returns valid=true.\n")
	prompt.WriteString("5. After valid=true, bydbctl may auto-probe the compiled statement; use probe findings to refine the plan when needed.\n")
	prompt.WriteString("6. Keep every statement read-only: only SELECT and SHOW TOP, one statement, no semicolons, and no SQL comments.\n")
	prompt.WriteString("7. validate_bydbql is parse/safety-only. It does not register a candidate and does not prove schema or index-rule correctness.\n")
	prompt.WriteString("8. probe_bydbql and execute_bydbql accept only the exact query from the latest successful propose_query_plan.\n")
	prompt.WriteString("9. When the plan includes ORDER BY or the user asks for latest/top/ranking, sort only by TIME or schema.indexed_fields; omit ORDER BY when no indexed field fits.\n")
	if initial {
		prompt.WriteString("10. Omitted time and row-count constraints render as last 30 minutes and LIMIT 10.\n")
	} else {
		prompt.WriteString("10. Fix validation_error, probe_summary.error, or execution_summary.error while preserving correct parts of the prior plan.\n")
	}
	prompt.WriteString("\n")
}

func writeHardRules(prompt *bytes.Buffer, initial bool, executionPolicy string) {
	policy := approval.NormalizeExecutionPolicy(executionPolicy)
	prompt.WriteString("Hard rules:\n")
	prompt.WriteString("- Never publish a raw BYDBQL statement in free text. Publish candidates only through propose_query_plan.\n")
	prompt.WriteString("- The session working directory is not a codebase. Do not read files, search source, or run shell commands.\n")
	prompt.WriteString("- For a query-planning request, never call propose_query_plan before describe_schema returns typed columns for the target resource.\n")
	prompt.WriteString("- A normal conversational response is valid when no query is ready.\n")
	prompt.WriteString("- Submit a typed query plan only when the user asks for a query and the request is specific enough.\n")
	prompt.WriteString("- When you submit a query plan, do not end the turn until propose_query_plan returns valid=true. Free-text BYDBQL is ignored by bydbctl.\n")
	prompt.WriteString("- Do not call probe_bydbql or execute_bydbql with hand-written BYDBQL, even when validate_bydbql returned valid=true.\n")
	prompt.WriteString("- For a new query plan, rank at most five catalog candidates, and call describe_schema for at most three.\n")
	prompt.WriteString("- Use only the typed columns returned by describe_schema. Do not invent a resource, group, field, tag, type, or index.\n")
	prompt.WriteString("- Use only the provided bydbctl tools. Do not use shell commands, external MCP servers, downloads, or runtime tool registration.\n")
	prompt.WriteString("- propose_query_plan accepts a plan or workflow. Its result is the only structured candidate that bydbctl will show.\n")
	prompt.WriteString("- validate_bydbql may help debug syntax, but only a successful propose_query_plan registers the workspace candidate.\n")
	switch policy {
	case approval.PolicyTrustSession:
		prompt.WriteString("- Execution policy is trust_session: mutating statements are also auto-approved when supported.\n")
	case approval.PolicyAutoProbe:
		prompt.WriteString("- Execution policy is auto_probe: read-only statements are auto-approved; mutating statements require explicit user approval unless probed first.\n")
	default:
		prompt.WriteString("- Execution policy is auto read: SELECT and SHOW TOP are auto-approved for probe_bydbql and execute_bydbql.\n")
		prompt.WriteString("- Mutating statements still require explicit user approval.\n")
	}
	prompt.WriteString("- Read-only BYDBQL may be probed or executed without waiting for user approval.\n")
	prompt.WriteString("- Keep every plan read-only. Never generate create, update, delete, drop, or apply operations.\n")
	prompt.WriteString("- The deterministic planner supports projections, typed comparison/IN filters, AND/OR trees, indexed ordering, ")
	prompt.WriteString("measure aggregation/grouping, and SHOW TOP.\n")
	prompt.WriteString("- Supported aggregation functions are MEAN, COUNT, MAX, MIN, and SUM. ORDER BY may use TIME or a typed indexed column.\n")
	prompt.WriteString("- Do not request MATCH, HAVING, OFFSET, STAGES, WITH QUERY_TRACE, joins, or unsupported expressions. Ask one clarification instead.\n")
	prompt.WriteString("- turn_hint is the user's instruction for the current round; apply it on top of goal and prior conversation.\n")
	if initial {
		prompt.WriteString("- Omitted time and row-count constraints are rendered by bydbctl as the safe defaults: last 30 minutes and LIMIT 10.\n")
	} else {
		prompt.WriteString("- Fix validation_error, probe_summary.error, or execution_summary.error when present; preserve correct parts of the prior plan.\n")
		prompt.WriteString("- Execution previews contain at most 50 rows and may be used to plan the next independently approved step.\n")
	}
	prompt.WriteString("- When a catalog choice remains ambiguous after inspection, ask exactly one concise clarification question instead of guessing.\n")
	prompt.WriteString("- When query_hints.prefer_show_top is true and describe_schema already returned typed columns for a matched resource, submit propose_query_plan as a best-effort draft even if optional filters (for example a service category) are unclear; state assumptions briefly.\n")
	prompt.WriteString("- When turn_hint names a catalog resource directly, prefer that resource and submit propose_query_plan with the closest available catalog entry when the exact name is missing.\n\n")
}

func writeNLRules(prompt *bytes.Buffer) {
	prompt.WriteString("Natural language rules:\n")
	prompt.WriteString("- schema.ranked_candidates and schema.catalog are discovery hints, not user selections. Select a resource only after inspecting its actual typed schema.\n")
	prompt.WriteString("- query_hints.prefer_show_top means use a SHOW TOP plan, not SELECT with LIMIT.\n")
	prompt.WriteString("- Distinguish time ranges from data-point limits; use the user wording when it is explicit.\n")
	prompt.WriteString("- A goal spanning multiple resources requires a workflow with one independently approved plan step per resource.\n")
	prompt.WriteString("- conversation lists prior user hints and compiled candidates; continue from the latest state.\n")
	prompt.WriteString("- When the user asks for a query that should return data, inspect schema with describe_schema, compile a plan, and rely on propose_query_plan plus the auto-probe preview to refine before finishing the turn.\n\n")
}

func writeReferences(prompt *bytes.Buffer) {
	prompt.WriteString("Compiler vocabulary reference (understand it, but submit a plan rather than BYDBQL text):\n")
	prompt.WriteString(strings.TrimSpace(syntaxReference))
	prompt.WriteString("\n\n")
}

func writeOutputContract(prompt *bytes.Buffer) {
	prompt.WriteString("Output contract:\n")
	prompt.WriteString("- Explain your reasoning, selected schema, and probe findings briefly in user-facing language.\n")
	prompt.WriteString("- When a query is ready, submit propose_query_plan with {\"plan\":{...}} or {\"workflow\":{\"steps\":[...]}}.\n")
	prompt.WriteString("- Nest resource fields under resource: {\"type\",\"name\",\"groups\"}. Never put name/type/groups at the plan root.\n")
	prompt.WriteString("- SHOW TOP goals: resource.type TOPN, top_n as integer, aggregate.function, order_by.direction, time_range.start. No column in aggregate or order_by.\n")
	prompt.WriteString("- SELECT goals: resource.type MEASURE|STREAM|TRACE|PROPERTY with typed projection/filter/order_by/limit.\n")
	prompt.WriteString("- When plan_example is present in context JSON, copy its shape and fill columns from describe_schema.\n")
	prompt.WriteString("- Do not embed BYDBQL in free text; propose_query_plan is the only accepted candidate path.\n\n")
}
