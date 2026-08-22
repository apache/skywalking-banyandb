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
	"strings"
	"testing"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/tools"
)

// errDescribeUnavailable stands in for a BanyanDB schema endpoint that cannot be reached.
var errDescribeUnavailable = errors.New("schema endpoint unavailable")

// describeExecutor serves one schema snapshot, or one failure, to the direct describe path.
type describeExecutor struct {
	schemaErr error
	snapshot  session.SchemaSnapshot
}

func (executor *describeExecutor) DiscoverCatalog(_ context.Context) (session.SchemaCatalog, error) {
	return session.SchemaCatalog{Groups: []string{"sw_trace", "sw_metrics"}, Entries: describeCatalog}, nil
}

func (executor *describeExecutor) DiscoverSchema(_ context.Context, _ tools.SchemaRequest) (session.SchemaSnapshot, error) {
	if executor.schemaErr != nil {
		return session.SchemaSnapshot{}, executor.schemaErr
	}
	return executor.snapshot, nil
}

func (executor *describeExecutor) Execute(_ context.Context, _ *session.QuerySession, _ string) (session.ExecutionResult, error) {
	return session.ExecutionResult{}, errors.New("a schema lookup must never execute a query")
}

// describeCatalog is the catalog the direct schema lookup resolves resource names against.
var describeCatalog = []session.CatalogEntry{
	{Group: "sw_trace", Type: session.ResourceTypeTrace, Name: "segment"},
	{Group: "sw_metrics", Type: session.ResourceTypeMeasure, Name: "service_cpm"},
}

func TestResolveDescribeTargetUsesTheComposerReference(t *testing.T) {
	reference := &session.CatalogEntry{Group: "sw_trace", Type: session.ResourceTypeTrace, Name: "segment"}
	request, ok := ResolveDescribeTarget("describe @sw_trace/segment", reference, describeCatalog)
	if !ok {
		t.Fatal("an explicit @ reference plus a schema question must resolve a describe target")
	}
	if request.Name != "segment" || request.Group != "sw_trace" || request.ResourceType != session.ResourceTypeTrace {
		t.Fatalf("unexpected describe target: %+v", request)
	}
}

func TestResolveDescribeTargetMatchesAResourceNamedInTheText(t *testing.T) {
	request, ok := ResolveDescribeTarget("sw_trace 下面的 segment 有哪些字段", nil, describeCatalog)
	if !ok {
		t.Fatal("a schema question naming one catalog resource must resolve without an @ reference")
	}
	if request.Name != "segment" || request.Group != "sw_trace" {
		t.Fatalf("unexpected describe target: %+v", request)
	}
}

func TestResolveDescribeTargetLeavesOtherTurnsToTheAgent(t *testing.T) {
	for _, turnHint := range []string{
		// Asks for stored rows, not for the shape of a resource.
		"show the latest 10 rows from segment",
		// Names no resource, so the agent must rank candidates or ask which one.
		"what fields are available",
		// A catalog question rather than a resource description.
		"which resources can I use to inspect errors?",
		// Reads the schema but also wants data, so the full query workflow has to run.
		"describe segment and show me the data",
		"",
	} {
		if _, ok := ResolveDescribeTarget(turnHint, nil, describeCatalog); ok {
			t.Fatalf("expected %q to stay an agent turn", turnHint)
		}
	}
}

func TestResolveDescribeTargetLeavesAnAmbiguousNameToTheAgent(t *testing.T) {
	ambiguousCatalog := []session.CatalogEntry{
		{Group: "sw_trace", Type: session.ResourceTypeTrace, Name: "segment"},
		{Group: "sw_trace_archive", Type: session.ResourceTypeTrace, Name: "segment"},
	}
	if _, ok := ResolveDescribeTarget("describe segment", nil, ambiguousCatalog); ok {
		t.Fatal("a name shared across groups must reach the agent so it can ask which group is meant")
	}
	// The same turn resolves once the composer pins one of the two.
	reference := &session.CatalogEntry{Group: "sw_trace", Type: session.ResourceTypeTrace, Name: "segment"}
	if _, ok := ResolveDescribeTarget("describe segment", reference, ambiguousCatalog); !ok {
		t.Fatal("an @ reference must resolve a name that is otherwise ambiguous")
	}
}

func TestDescribeResourceRecordsASchemaAnswerWithoutACandidate(t *testing.T) {
	snapshot := session.SchemaSnapshot{
		Loaded: true,
		Type:   session.ResourceTypeTrace,
		Name:   "segment",
		Groups: []string{"sw_trace"},
		Columns: []session.SchemaColumn{
			{Name: "trace_id", Kind: session.SchemaColumnTag, Type: session.SchemaValueTypeString, Indexed: true},
			{Name: "duration", Kind: session.SchemaColumnField, Type: session.SchemaValueTypeInt},
		},
		EntityTags:      []string{"trace_id"},
		SortableIndexes: []session.SortableIndex{{RuleName: "start_time", Tags: []string{"start_time"}}},
		TraceIDTag:      "trace_id",
		TimestampTag:    "start_time",
	}
	runner := NewRunner(Config{Executor: &describeExecutor{snapshot: snapshot}})
	querySession := &session.QuerySession{}

	describeErr := runner.DescribeResource(context.Background(), querySession,
		DescribeRequest{ResourceType: session.ResourceTypeTrace, Name: "segment", Group: "sw_trace"},
		"segment 有哪些字段")
	if describeErr != nil {
		t.Fatalf("unexpected describe error: %v", describeErr)
	}
	if querySession.Phase != session.PhaseSchema {
		t.Fatalf("a schema lookup must record the schema phase, got %q", querySession.Phase)
	}
	if len(querySession.Candidates) != 0 {
		t.Fatalf("a schema lookup must not publish a BYDBQL candidate: %+v", querySession.Candidates)
	}
	if querySession.ExecutionResult.Summary != "" || querySession.ExecutionResult.Rows != 0 {
		t.Fatalf("a schema lookup must not produce an execution result: %+v", querySession.ExecutionResult)
	}
	if querySession.SchemaSnapshot.Name != "segment" {
		t.Fatalf("the described schema must become the active snapshot, got %q", querySession.SchemaSnapshot.Name)
	}
	if len(querySession.ChatMessages) != 2 {
		t.Fatalf("expected the question and its answer in the conversation, got %d", len(querySession.ChatMessages))
	}
	answer := querySession.ChatMessages[1]
	if answer.Kind != session.ChatMessageKindSchema {
		t.Fatalf("the answer must be marked as a schema lookup, got kind %q", answer.Kind)
	}
	if !strings.Contains(answer.Content, "TRACE segment in sw_trace") {
		t.Fatalf("the headline must name the described resource: %q", answer.Content)
	}
	for _, expected := range []string{"| trace_id | tag | string | yes |", "### Sortable index rules", "`start_time`", "### Trace scan"} {
		if !strings.Contains(answer.Detail, expected) {
			t.Fatalf("expected %q in the schema markdown:\n%s", expected, answer.Detail)
		}
	}
}

func TestDescribeResourceReportsAnUnloadedSchema(t *testing.T) {
	runner := NewRunner(Config{Executor: &describeExecutor{snapshot: session.SchemaSnapshot{
		Type:   session.ResourceTypeMeasure,
		Name:   "service_cpm",
		Groups: []string{"sw_metrics"},
	}}})
	querySession := &session.QuerySession{}

	if describeErr := runner.DescribeResource(context.Background(), querySession,
		DescribeRequest{ResourceType: session.ResourceTypeMeasure, Name: "service_cpm", Group: "sw_metrics"},
		"describe service_cpm"); describeErr != nil {
		t.Fatalf("an unloaded schema must be reported, not fail the turn: %v", describeErr)
	}
	answer := querySession.ChatMessages[len(querySession.ChatMessages)-1]
	if !strings.Contains(answer.Content, "typed columns unavailable") {
		t.Fatalf("expected the headline to report the missing columns: %q", answer.Content)
	}
	if !strings.Contains(answer.Detail, "Ctrl+L") {
		t.Fatalf("expected the body to name the recovery action:\n%s", answer.Detail)
	}
}

func TestDescribeResourceFailsWhenTheSchemaCannotBeRead(t *testing.T) {
	runner := NewRunner(Config{Executor: &describeExecutor{schemaErr: errDescribeUnavailable}})
	querySession := &session.QuerySession{}

	describeErr := runner.DescribeResource(context.Background(), querySession,
		DescribeRequest{ResourceType: session.ResourceTypeMeasure, Name: "service_cpm", Group: "sw_metrics"}, "describe service_cpm")
	if describeErr == nil {
		t.Fatal("expected a schema read failure to surface")
	}
	if querySession.Phase != session.PhaseError {
		t.Fatalf("a failed schema read must record the error phase, got %q", querySession.Phase)
	}
}

func TestFormatSchemaMarkdownFallsBackToUntypedNames(t *testing.T) {
	markdown := FormatSchemaMarkdown(session.SchemaSnapshot{
		Loaded: true,
		Type:   session.ResourceTypeMeasure,
		Name:   "service_cpm",
		Groups: []string{"sw_metrics"},
		Tags:   []string{"service_id"},
		Fields: []string{"value"},
	})
	for _, expected := range []string{"### Tags", "`service_id`", "### Fields", "`value`"} {
		if !strings.Contains(markdown, expected) {
			t.Fatalf("expected %q in the untyped schema markdown:\n%s", expected, markdown)
		}
	}
}
