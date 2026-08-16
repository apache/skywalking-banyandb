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

package bydbql

import (
	"context"
	"strings"
	"testing"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
)

// traceSchema builds the TRACE schema used by the scan-bound tests.
func traceSchema() *session.SchemaSnapshot {
	return &session.SchemaSnapshot{
		Type:         session.ResourceTypeTrace,
		Name:         "sw_trace",
		Groups:       []string{"sw_trace"},
		Tags:         []string{"trace_id", "span_id", "start_time", "service_id", "status"},
		TraceIDTag:   "trace_id",
		TimestampTag: "start_time",
		Columns: []session.SchemaColumn{
			{Name: "trace_id", Kind: session.SchemaColumnTag, Type: session.SchemaValueTypeString},
			{Name: "span_id", Kind: session.SchemaColumnTag, Type: session.SchemaValueTypeString},
			{Name: "start_time", Kind: session.SchemaColumnTag, Type: session.SchemaValueTypeInt, Indexed: true},
			{Name: "service_id", Kind: session.SchemaColumnTag, Type: session.SchemaValueTypeString},
			{Name: "status", Kind: session.SchemaColumnTag, Type: session.SchemaValueTypeString},
		},
		SortableIndexes: []session.SortableIndex{
			{RuleName: "start_time", Tags: []string{"start_time"}},
		},
		Loaded: true,
	}
}

func TestUnboundedTraceScanIsRejectedBeforeExecution(t *testing.T) {
	validator := NewSemanticValidator()
	query := "SELECT * FROM TRACE sw_trace IN sw_trace TIME > '-30m' LIMIT 10"

	report, validateErr := validator.Validate(context.Background(), query, traceSchema())
	if validateErr != nil {
		t.Fatalf("unexpected validation error: %v", validateErr)
	}
	if report.Valid {
		t.Fatal("a TRACE query without ORDER BY or a trace_id filter must not be reported as valid")
	}
	if !strings.Contains(report.Message, "ORDER BY start_time DESC") {
		t.Fatalf("the diagnostic must name a sortable index rule: %q", report.Message)
	}
	if !strings.Contains(report.Message, "trace_id = '<id>'") {
		t.Fatalf("the diagnostic must offer the trace_id filter: %q", report.Message)
	}
}

func TestBoundedTraceScansStayValid(t *testing.T) {
	validator := NewSemanticValidator()
	boundedQueries := []string{
		"SELECT * FROM TRACE sw_trace IN sw_trace TIME > '-30m' ORDER BY start_time DESC LIMIT 10",
		"SELECT * FROM TRACE sw_trace IN sw_trace TIME > '-30m' WHERE trace_id = 'abc123' LIMIT 10",
		"SELECT * FROM TRACE sw_trace IN sw_trace TIME > '-30m' WHERE trace_id IN ('abc', 'def') LIMIT 10",
		"SELECT * FROM TRACE sw_trace IN sw_trace TIME > '-30m' WHERE status = 'error' AND trace_id = 'abc' LIMIT 10",
		"SELECT * FROM TRACE sw_trace IN sw_trace TIME > '-30m' WHERE (trace_id = 'abc' OR trace_id = 'def') LIMIT 10",
	}
	for _, query := range boundedQueries {
		report, validateErr := validator.Validate(context.Background(), query, traceSchema())
		if validateErr != nil {
			t.Fatalf("unexpected validation error for %q: %v", query, validateErr)
		}
		if !report.Valid {
			t.Fatalf("a bounded TRACE scan must stay valid: %q rejected with %q", query, report.Message)
		}
	}
}

func TestTraceScanBoundsIgnoreOtherResourceTypes(t *testing.T) {
	validator := NewSemanticValidator()
	measureSchema := &session.SchemaSnapshot{
		Type:   session.ResourceTypeMeasure,
		Name:   "service_cpm",
		Groups: []string{"sw_metrics"},
		Tags:   []string{"service_id"},
		Fields: []string{"value"},
		Loaded: true,
	}
	query := "SELECT * FROM MEASURE service_cpm IN sw_metrics TIME > '-30m' LIMIT 10"

	report, validateErr := validator.Validate(context.Background(), query, measureSchema)
	if validateErr != nil {
		t.Fatalf("unexpected validation error: %v", validateErr)
	}
	if !report.Valid {
		t.Fatalf("a MEASURE query must not inherit the TRACE scan rule: %q", report.Message)
	}
}

func TestTraceWithoutADiscoveredTraceIDTagAsksForSchemaDiscovery(t *testing.T) {
	schema := &session.SchemaSnapshot{
		Type:   session.ResourceTypeTrace,
		Name:   "sw_trace",
		Groups: []string{"sw_trace"},
		Tags:   []string{"span_id", "service_id"},
		Loaded: true,
	}
	message := validateTraceScanBounds("SELECT * FROM TRACE sw_trace IN sw_trace TIME > '-30m' LIMIT 10", schema)
	if !strings.Contains(message, "describe_schema") {
		t.Fatalf("an unknown trace ID tag must point at schema discovery: %q", message)
	}
}

func TestTraceIDTagFallsBackToTheConventionalName(t *testing.T) {
	schema := &session.SchemaSnapshot{
		Type:   session.ResourceTypeTrace,
		Name:   "sw_trace",
		Groups: []string{"sw_trace"},
		Tags:   []string{"trace_id", "span_id"},
		Loaded: true,
	}
	if traceIDTag := traceIDTagName(schema); traceIDTag != "trace_id" {
		t.Fatalf("a schema without an explicit trace ID tag must fall back to trace_id, got %q", traceIDTag)
	}
	message := validateTraceScanBounds("SELECT * FROM TRACE sw_trace IN sw_trace TIME > '-30m' WHERE trace_id = 'abc' LIMIT 10", schema)
	if message != "" {
		t.Fatalf("the fallback tag must satisfy the scan bound: %q", message)
	}
}
