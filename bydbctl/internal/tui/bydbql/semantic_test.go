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

func TestSemanticValidatorRequiresTimeClause(t *testing.T) {
	validator := NewSemanticValidator()
	schema := &session.SchemaSnapshot{Type: session.ResourceTypeMeasure}
	report, validateErr := validator.Validate(context.Background(), "SELECT * FROM MEASURE service_latency IN production LIMIT 10", schema)
	if validateErr != nil {
		t.Fatalf("Validate returned error: %v", validateErr)
	}
	if report.Valid {
		t.Fatal("expected missing TIME clause to be invalid")
	}
	if !strings.Contains(report.Message, "TIME clause") {
		t.Fatalf("unexpected message: %s", report.Message)
	}
}

func TestSemanticValidatorRequiresLimitClause(t *testing.T) {
	validator := NewSemanticValidator()
	schema := &session.SchemaSnapshot{Type: session.ResourceTypeMeasure}
	report, validateErr := validator.Validate(context.Background(), "SELECT * FROM MEASURE service_latency IN production TIME > '-30m'", schema)
	if validateErr != nil {
		t.Fatalf("Validate returned error: %v", validateErr)
	}
	if report.Valid {
		t.Fatal("expected missing LIMIT clause to be invalid")
	}
	if !strings.Contains(report.Message, "LIMIT clause") {
		t.Fatalf("unexpected message: %s", report.Message)
	}
}

func TestSemanticValidatorRejectsNonIndexedOrderBy(t *testing.T) {
	validator := NewSemanticValidator()
	schema := &session.SchemaSnapshot{
		Type:            session.ResourceTypeTrace,
		SortableIndexes: []session.SortableIndex{{RuleName: "timestamp_rule", Tags: []string{"timestamp_millis"}}},
	}
	report, validateErr := validator.Validate(
		context.Background(),
		"SELECT * FROM TRACE zipkin_span IN default TIME > '-30m' ORDER BY start_time DESC LIMIT 10",
		schema,
	)
	if validateErr != nil {
		t.Fatalf("Validate returned error: %v", validateErr)
	}
	if report.Valid {
		t.Fatal("expected non-indexed ORDER BY to be invalid")
	}
	if !strings.Contains(report.Message, "not sortable") {
		t.Fatalf("unexpected message: %s", report.Message)
	}
}

func TestSemanticValidatorAcceptsIndexedOrderBy(t *testing.T) {
	validator := NewSemanticValidator()
	schema := &session.SchemaSnapshot{
		Type:            session.ResourceTypeTrace,
		SortableIndexes: []session.SortableIndex{{RuleName: "timestamp_rule", Tags: []string{"timestamp_millis"}}},
	}
	report, validateErr := validator.Validate(
		context.Background(),
		"SELECT * FROM TRACE zipkin_span IN default TIME > '-30m' ORDER BY timestamp_rule DESC LIMIT 10",
		schema,
	)
	if validateErr != nil {
		t.Fatalf("Validate returned error: %v", validateErr)
	}
	if !report.Valid {
		t.Fatalf("expected valid report, got %q", report.Message)
	}
}
