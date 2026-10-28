// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain a
// copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package execution

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/tools"
)

const (
	readQuery = "SELECT * FROM MEASURE service_cpm IN default TIME > '-30m' LIMIT 10"
	// planFingerprint is the schema fingerprint the workflow was compiled against.
	planFingerprint = "v1"
)

type fakeExecutor struct {
	schemaErr  error
	executeErr error
	executed   []string
	schema     session.SchemaSnapshot
	result     session.ExecutionResult
}

func (executor *fakeExecutor) DiscoverCatalog(context.Context) (session.SchemaCatalog, error) {
	return session.SchemaCatalog{}, nil
}

func (executor *fakeExecutor) DiscoverSchema(context.Context, tools.SchemaRequest) (session.SchemaSnapshot, error) {
	return executor.schema, executor.schemaErr
}

func (executor *fakeExecutor) Execute(_ context.Context, _ *session.QuerySession, query string) (session.ExecutionResult, error) {
	executor.executed = append(executor.executed, query)
	return executor.result, executor.executeErr
}

type fakeValidator struct {
	schema *session.SchemaSnapshot
	report session.ValidationReport
}

func (validator *fakeValidator) Validate(_ context.Context, _ string, schema *session.SchemaSnapshot) (session.ValidationReport, error) {
	validator.schema = schema
	return validator.report, nil
}

func validReport() session.ValidationReport {
	return session.ValidationReport{Valid: true, QueryType: "MEASURE"}
}

func liveSchema(fingerprint string) session.SchemaSnapshot {
	return session.SchemaSnapshot{Type: session.ResourceType("measure"), Name: "service_cpm", Groups: []string{"default"}, Fingerprint: fingerprint}
}

// plannedSession is a session whose compiled workflow holds query, planned
// against the schema with planFingerprint.
func plannedSession(query string) *session.QuerySession {
	querySession := &session.QuerySession{}
	querySession.SetPlannedQueries([]session.PlannedQuery{
		{Query: query, Name: "service_cpm", Groups: []string{"default"}, SchemaFingerprint: planFingerprint},
		{Query: "SHOW TOP 5 FROM MEASURE service_cpm IN default TIME > '-30m' ORDER BY DESC"},
	})
	return querySession
}

func TestExecutePlannedRunsAgainstTheRefreshedSchemaAndAdvances(t *testing.T) {
	executor := &fakeExecutor{schema: liveSchema(planFingerprint), result: session.ExecutionResult{Rows: 3}}
	validator := &fakeValidator{report: validReport()}
	querySession := plannedSession(readQuery)

	outcome, err := New(executor, validator).ExecutePlanned(context.Background(), querySession, readQuery)
	if err != nil {
		t.Fatalf("ExecutePlanned: %v", err)
	}
	if !outcome.Executed || outcome.Phase != session.PhaseExecuted || outcome.Result.Rows != 3 {
		t.Fatalf("outcome = %+v, want an executed run with 3 rows", outcome)
	}
	if validator.schema == nil || validator.schema.Fingerprint != "v1" {
		t.Fatalf("revalidated against %+v, want the refreshed schema", validator.schema)
	}
	if outcome.Next == nil || !strings.HasPrefix(outcome.Next.Query, "SHOW TOP") {
		t.Fatalf("next step = %+v, want the second planned statement", outcome.Next)
	}
}

func TestExecutePlannedRejectsAWriteEvenWhenPlanned(t *testing.T) {
	const writeQuery = "CREATE MEASURE foo IN production"
	executor := &fakeExecutor{schema: liveSchema(planFingerprint)}
	querySession := plannedSession(writeQuery)

	if _, err := New(executor, &fakeValidator{report: validReport()}).ExecutePlanned(context.Background(), querySession, writeQuery); err == nil {
		t.Fatal("expected a non read-only statement to be rejected")
	}
	if len(executor.executed) != 0 {
		t.Fatalf("executed %v, want nothing", executor.executed)
	}
}

func TestExecutePlannedRequiresTheCurrentPlannedStatement(t *testing.T) {
	executor := &fakeExecutor{schema: liveSchema(planFingerprint)}
	querySession := plannedSession(readQuery)
	other := "SELECT * FROM MEASURE service_cpm IN default TIME > '-1h' LIMIT 5"

	if _, err := New(executor, &fakeValidator{report: validReport()}).ExecutePlanned(context.Background(), querySession, other); err == nil {
		t.Fatal("expected a statement other than the planned one to be rejected")
	}
	if len(executor.executed) != 0 {
		t.Fatalf("executed %v, want nothing", executor.executed)
	}
}

func TestExecutePlannedStopsWhenTheSchemaChangedAfterPlanning(t *testing.T) {
	executor := &fakeExecutor{schema: liveSchema("v2")}
	querySession := plannedSession(readQuery)

	outcome, err := New(executor, &fakeValidator{report: validReport()}).ExecutePlanned(context.Background(), querySession, readQuery)
	if err == nil || !strings.Contains(err.Error(), "regenerate the query plan") {
		t.Fatalf("error = %v, want a schema change error", err)
	}
	if outcome.Phase != session.PhaseValidate || outcome.Executed || len(executor.executed) != 0 {
		t.Fatalf("outcome = %+v, executed %v; want back to validate without running", outcome, executor.executed)
	}
	if querySession.CurrentPlannedQuery().Query != readQuery {
		t.Fatal("the plan should not advance")
	}
}

func TestExecutePlannedStopsWhenTheSchemaRefreshFails(t *testing.T) {
	executor := &fakeExecutor{schemaErr: errors.New("connection refused")}
	querySession := plannedSession(readQuery)

	outcome, err := New(executor, &fakeValidator{report: validReport()}).ExecutePlanned(context.Background(), querySession, readQuery)
	if err == nil || outcome.Phase != session.PhaseError || len(executor.executed) != 0 {
		t.Fatalf("outcome = %+v, err = %v, executed %v; want an error without running", outcome, err, executor.executed)
	}
}

func TestExecutePlannedRecordsAnExecutionFailure(t *testing.T) {
	executor := &fakeExecutor{schema: liveSchema(planFingerprint), executeErr: errors.New("deadline exceeded")}
	querySession := plannedSession(readQuery)

	outcome, err := New(executor, &fakeValidator{report: validReport()}).ExecutePlanned(context.Background(), querySession, readQuery)
	if err == nil || outcome.Phase != session.PhaseError || !outcome.Executed {
		t.Fatalf("outcome = %+v, err = %v; want an executed run that failed", outcome, err)
	}
	if querySession.ExecutionResult.Error != "deadline exceeded" || querySession.ExecutionResult.Summary != "deadline exceeded" {
		t.Fatalf("result = %+v, want the error recorded and used as the summary", querySession.ExecutionResult)
	}
	if outcome.Next != nil || querySession.CurrentPlannedQuery().Query != readQuery {
		t.Fatal("a failed run should not advance the plan")
	}
}

func TestExecuteCurrentRejectsACandidateThatDiffersFromThePlan(t *testing.T) {
	executor := &fakeExecutor{schema: liveSchema(planFingerprint)}
	querySession := plannedSession(readQuery)
	querySession.AddCandidate(session.BydbqlCandidate{
		Query:      "SELECT * FROM MEASURE service_cpm IN default TIME > '-1h' LIMIT 5",
		Validation: validReport(),
	})

	outcome, err := New(executor, &fakeValidator{report: validReport()}).ExecuteCurrent(context.Background(), querySession)
	if err == nil || outcome.Phase != session.PhaseValidate || len(executor.executed) != 0 {
		t.Fatalf("outcome = %+v, err = %v, executed %v; want a rejection without running", outcome, err, executor.executed)
	}
}

func TestExecuteNeedsAConfiguredEngine(t *testing.T) {
	querySession := &session.QuerySession{}
	querySession.AddCandidate(session.BydbqlCandidate{Query: readQuery, Validation: validReport()})

	outcome, err := New(nil, nil).ExecuteCurrent(context.Background(), querySession)
	if err == nil || outcome.Phase != session.PhaseError {
		t.Fatalf("outcome = %+v, err = %v; want a configuration error", outcome, err)
	}
}
