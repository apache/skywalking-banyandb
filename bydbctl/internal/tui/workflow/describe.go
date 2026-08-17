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
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/agent"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/tools"
)

// DescribeRequest names the resource a direct schema lookup should describe.
type DescribeRequest struct {
	ResourceType session.ResourceType
	Name         string
	Group        string
}

// ResolveDescribeTarget reports the resource a turn asks to have described, when there is exactly one.
//
// A schema question that names one catalog resource is answerable from the BanyanDB schema API, so
// bydbctl serves it directly. Anything else — an unnamed, ambiguous, or data-seeking turn — is left
// to the agent, which can rank candidates and ask a clarifying question.
func ResolveDescribeTarget(turnHint string, reference *session.CatalogEntry, entries []session.CatalogEntry) (DescribeRequest, bool) {
	trimmedHint := strings.TrimSpace(turnHint)
	if trimmedHint == "" || !agent.IsSchemaDescriptionRequest(trimmedHint) {
		return DescribeRequest{}, false
	}
	if reference != nil && strings.TrimSpace(reference.Name) != "" {
		return DescribeRequest{ResourceType: reference.Type, Name: reference.Name, Group: reference.Group}, true
	}
	if explicitEntry := FindExplicitResourceMention(trimmedHint, entries); explicitEntry != nil {
		return DescribeRequest{ResourceType: explicitEntry.Type, Name: explicitEntry.Name, Group: explicitEntry.Group}, true
	}
	namedEntry := findNamedResource(trimmedHint, entries)
	if namedEntry == nil {
		return DescribeRequest{}, false
	}
	return DescribeRequest{ResourceType: namedEntry.Type, Name: namedEntry.Name, Group: namedEntry.Group}, true
}

// resourceWordPattern matches the identifier-shaped words of a turn.
//
// Resource names are ASCII, so this also picks a name out of a CJK question that embeds one.
var resourceWordPattern = regexp.MustCompile(`[a-z][a-z0-9_]*`)

// minNamedResourceLength is the shortest word treated as naming a resource.
//
// The shared explicit matcher requires eight characters to keep generic English words out of an
// agent prompt. A describe target is resolved against exact catalog names instead, so a short name
// such as "segment" or "task" is safe here, but a one- or two-letter word never is.
const minNamedResourceLength = 3

// findNamedResource resolves the one catalog resource a turn names by its exact name.
//
// Two resources sharing a name across groups stay unresolved, so the agent can ask which group is
// meant rather than bydbctl silently describing the wrong one.
func findNamedResource(turnHint string, entries []session.CatalogEntry) *session.CatalogEntry {
	words := resourceWordPattern.FindAllString(strings.ToLower(turnHint), -1)
	var matchedEntry *session.CatalogEntry
	for _, word := range words {
		if len(word) < minNamedResourceLength {
			continue
		}
		for entryIndex := range entries {
			entry := &entries[entryIndex]
			if !strings.EqualFold(strings.TrimSpace(entry.Name), word) {
				continue
			}
			if matchedEntry != nil && (matchedEntry.Name != entry.Name ||
				matchedEntry.Group != entry.Group || matchedEntry.Type != entry.Type) {
				return nil
			}
			matchedEntry = entry
		}
	}
	return matchedEntry
}

// DescribeResource loads one resource schema and records it as a direct catalog answer.
//
// The turn produces no BYDBQL candidate: it reports schema, so the workspace must show it as a
// schema lookup rather than as a query that happened to return columns.
func (runner *Runner) DescribeResource(ctx context.Context, querySession *session.QuerySession, request DescribeRequest, turnHint string) error {
	if querySession == nil {
		return errors.New("query session is required")
	}
	if runner.executor == nil {
		return errors.New("schema executor is not configured")
	}
	trimmedHint := strings.TrimSpace(turnHint)
	if trimmedHint != "" {
		querySession.AddTranscript("user", trimmedHint, runner.now())
		querySession.AddChatMessage(session.ChatMessage{
			Role:      session.ChatRoleUser,
			Content:   trimmedHint,
			CreatedAt: runner.now(),
		})
		if strings.TrimSpace(querySession.UserGoal) == "" {
			querySession.UserGoal = trimmedHint
		}
	}
	groups := []string(nil)
	if trimmedGroup := strings.TrimSpace(request.Group); trimmedGroup != "" {
		groups = []string{trimmedGroup}
	}
	snapshot, schemaErr := runner.executor.DiscoverSchema(ctx, tools.SchemaRequest{
		Type:   request.ResourceType,
		Name:   request.Name,
		Groups: groups,
	})
	if schemaErr != nil {
		querySession.Phase = session.PhaseError
		return fmt.Errorf("failed to describe %s %s: %w", request.ResourceType, request.Name, schemaErr)
	}
	querySession.ActivateSchema(snapshot)
	querySession.CandidateSuperseded = true
	querySession.Validation = session.ValidationReport{}
	querySession.SetPlannedQueries(nil)
	querySession.Phase = session.PhaseSchema
	querySession.AddChatMessage(session.ChatMessage{
		Role:      session.ChatRoleAssistant,
		Kind:      session.ChatMessageKindSchema,
		Content:   DescribeHeadline(snapshot),
		Detail:    FormatSchemaMarkdown(snapshot),
		CreatedAt: runner.now(),
	})
	querySession.AddTranscript("workflow", "described "+describeLabel(snapshot)+" from the schema catalog", runner.now())
	return nil
}

// DescribeHeadline summarizes a described schema in one conversation line.
func DescribeHeadline(snapshot session.SchemaSnapshot) string {
	if !snapshot.Loaded {
		return "schema " + describeLabel(snapshot) + " · typed columns unavailable"
	}
	tagCount, fieldCount := countSchemaColumns(snapshot)
	return fmt.Sprintf("schema %s · %d tags, %d fields", describeLabel(snapshot), tagCount, fieldCount)
}

// describeLabel names a described resource as type, name, and group.
func describeLabel(snapshot session.SchemaSnapshot) string {
	label := strings.TrimSpace(snapshot.Type.String() + " " + snapshot.Name)
	if groups := normalizeGroupsIfProvided(snapshot.Groups); len(groups) > 0 {
		return label + " in " + strings.Join(groups, ", ")
	}
	return label
}

// countSchemaColumns splits typed columns into the tag and field totals shown in the headline.
func countSchemaColumns(snapshot session.SchemaSnapshot) (int, int) {
	tagCount := 0
	fieldCount := 0
	for _, column := range snapshot.Columns {
		if column.Kind == session.SchemaColumnField {
			fieldCount++
			continue
		}
		tagCount++
	}
	if len(snapshot.Columns) == 0 {
		return len(snapshot.Tags) + len(snapshot.EntityTags), len(snapshot.Fields)
	}
	return tagCount, fieldCount
}

// FormatSchemaMarkdown renders a resource schema as the markdown body of a conversation message.
//
// The conversation renders markdown, so a described schema is written as one: a table of typed
// columns reads as a resource description rather than as a query result.
func FormatSchemaMarkdown(snapshot session.SchemaSnapshot) string {
	var builder strings.Builder
	builder.WriteString("## " + describeLabel(snapshot) + "\n")
	if !snapshot.Loaded {
		builder.WriteString("\nTyped columns are not available for this resource. " +
			"Check the BanyanDB address and reload the catalog with Ctrl+L.\n")
		return builder.String()
	}
	writeSchemaColumnTable(&builder, snapshot)
	writeSchemaNameList(&builder, "Entity (series key)", snapshot.EntityTags)
	writeSchemaNameList(&builder, "Indexed tags (ORDER BY)", snapshot.IndexedFields)
	writeSchemaIndexRules(&builder, snapshot.SortableIndexes)
	writeSchemaTraceTags(&builder, snapshot)
	if sourceMeasure := strings.TrimSpace(snapshot.SourceMeasure); sourceMeasure != "" {
		builder.WriteString("\n### Source measure\n\n")
		builder.WriteString("`" + sourceMeasure + "`")
		if sourceGroup := strings.TrimSpace(snapshot.SourceMeasureGroup); sourceGroup != "" {
			builder.WriteString(" in `" + sourceGroup + "`")
		}
		builder.WriteString("\n")
	}
	return builder.String()
}

// writeSchemaColumnTable writes the typed columns, falling back to the untyped name lists.
func writeSchemaColumnTable(builder *strings.Builder, snapshot session.SchemaSnapshot) {
	if len(snapshot.Columns) == 0 {
		writeSchemaNameList(builder, "Tags", snapshot.Tags)
		writeSchemaNameList(builder, "Fields", snapshot.Fields)
		if len(snapshot.Tags) == 0 && len(snapshot.Fields) == 0 && len(snapshot.EntityTags) == 0 {
			builder.WriteString("\nNo tags or fields are declared on this resource.\n")
		}
		return
	}
	builder.WriteString("\n### Columns\n\n")
	builder.WriteString("| Column | Kind | Type | Indexed |\n")
	builder.WriteString("| --- | --- | --- | --- |\n")
	for _, column := range snapshot.Columns {
		indexed := ""
		if column.Indexed {
			indexed = "yes"
		}
		builder.WriteString(fmt.Sprintf("| %s | %s | %s | %s |\n",
			column.Name, column.Kind, column.Type, indexed))
	}
}

// writeSchemaNameList writes one titled bullet list, skipping the section when it holds nothing.
func writeSchemaNameList(builder *strings.Builder, title string, names []string) {
	if len(names) == 0 {
		return
	}
	builder.WriteString("\n### " + title + "\n\n")
	for _, name := range names {
		builder.WriteString("- `" + name + "`\n")
	}
}

// writeSchemaIndexRules writes the index rules an ORDER BY clause may name.
func writeSchemaIndexRules(builder *strings.Builder, indexes []session.SortableIndex) {
	if len(indexes) == 0 {
		return
	}
	sortedIndexes := append([]session.SortableIndex(nil), indexes...)
	sort.Slice(sortedIndexes, func(leftIndex, rightIndex int) bool {
		return sortedIndexes[leftIndex].RuleName < sortedIndexes[rightIndex].RuleName
	})
	builder.WriteString("\n### Sortable index rules\n\n")
	for _, index := range sortedIndexes {
		builder.WriteString("- `" + index.RuleName + "`")
		if len(index.Tags) > 0 {
			builder.WriteString(" → " + strings.Join(index.Tags, ", "))
		}
		builder.WriteString("\n")
	}
}

// writeSchemaTraceTags names the two TRACE tags that decide whether a trace scan is bounded.
func writeSchemaTraceTags(builder *strings.Builder, snapshot session.SchemaSnapshot) {
	if snapshot.Type != session.ResourceTypeTrace {
		return
	}
	traceIDTag := strings.TrimSpace(snapshot.TraceIDTag)
	timestampTag := strings.TrimSpace(snapshot.TimestampTag)
	if traceIDTag == "" && timestampTag == "" {
		return
	}
	builder.WriteString("\n### Trace scan\n\n")
	if traceIDTag != "" {
		builder.WriteString("- trace ID tag: `" + traceIDTag + "`\n")
	}
	if timestampTag != "" {
		builder.WriteString("- timestamp tag: `" + timestampTag + "`\n")
	}
	builder.WriteString("- A trace query needs an `ORDER BY` on a sortable index rule, " +
		"or an equality filter on the trace ID tag.\n")
}
