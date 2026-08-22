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

func TestSemanticValidatorRejectsUnknownProjection(t *testing.T) {
	validator := NewSemanticValidator()
	schema := &session.SchemaSnapshot{
		Type:   session.ResourceTypeMeasure,
		Tags:   []string{"endpoint"},
		Fields: []string{"latency"},
	}
	report, validateErr := validator.Validate(
		context.Background(),
		"SELECT unknown_field FROM MEASURE service_latency IN production TIME > '-30m' LIMIT 10",
		schema,
	)
	if validateErr != nil {
		t.Fatalf("Validate returned error: %v", validateErr)
	}
	if report.Valid {
		t.Fatal("expected unknown projection to be invalid")
	}
	if !strings.Contains(report.Message, "unknown_field") {
		t.Fatalf("unexpected message: %s", report.Message)
	}
}

func TestSemanticValidatorAcceptsKnownProjection(t *testing.T) {
	validator := NewSemanticValidator()
	schema := &session.SchemaSnapshot{
		Type:   session.ResourceTypeMeasure,
		Tags:   []string{"endpoint"},
		Fields: []string{"latency"},
	}
	report, validateErr := validator.Validate(
		context.Background(),
		"SELECT endpoint, AVG(latency) FROM MEASURE service_latency IN production TIME > '-30m' GROUP BY endpoint LIMIT 10",
		schema,
	)
	if validateErr != nil {
		t.Fatalf("Validate returned error: %v", validateErr)
	}
	if !report.Valid {
		t.Fatalf("expected valid report, got %q", report.Message)
	}
}

func TestSemanticValidatorAcceptsUnambiguousTagFamilySuffix(t *testing.T) {
	validator := NewSemanticValidator()
	report, validateErr := validator.Validate(context.Background(),
		"SELECT endpoint FROM MEASURE service_latency IN production TIME > '-30m' LIMIT 10",
		&session.SchemaSnapshot{
			Type: session.ResourceTypeMeasure,
			Tags: []string{"default.endpoint"},
			Columns: []session.SchemaColumn{{
				Name: "default.endpoint",
				Kind: session.SchemaColumnTag,
				Type: session.SchemaValueTypeString,
			}},
		},
	)
	if validateErr != nil {
		t.Fatalf("Validate returned error: %v", validateErr)
	}
	if !report.Valid {
		t.Fatalf("expected tag family suffix to be accepted: %+v", report)
	}
}
