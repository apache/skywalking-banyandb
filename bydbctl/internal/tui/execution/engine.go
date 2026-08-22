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

// Package execution owns the shared validation and execution lifecycle for BYDBQL candidates.
package execution

import (
	"context"
	"errors"
	"fmt"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/bydbql"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/tools"
)

// Validator validates a BYDBQL query against the active schema.
type Validator interface {
	Validate(ctx context.Context, query string, schema *session.SchemaSnapshot) (session.ValidationReport, error)
}

// Outcome contains the state produced by one successful validation attempt and optional execution.
type Outcome struct {
	Next       *session.PlannedQuery
	Phase      session.Phase
	Validation session.ValidationReport
	Result     session.ExecutionResult
	Executed   bool
}

// Engine refreshes planned schemas, revalidates exact statements, executes them, and advances workflows.
type Engine struct {
	executor  tools.Executor
	validator Validator
}

// New creates an execution engine from the shared BanyanDB dependencies.
func New(executor tools.Executor, validator Validator) *Engine {
	return &Engine{executor: executor, validator: validator}
}

// ExecuteCurrent executes the selected candidate, allowing a manual candidate without a compiled workflow.
func (engine *Engine) ExecuteCurrent(ctx context.Context, querySession *session.QuerySession) (Outcome, error) {
	if querySession == nil {
		return Outcome{}, errors.New("query session is required")
	}
	currentCandidate := querySession.CurrentCandidate()
	if currentCandidate == nil {
		return Outcome{}, errors.New("query candidate is required")
	}
	if !currentCandidate.Validation.Valid {
		return Outcome{Phase: session.PhaseValidate}, errors.New("only a valid BYDBQL candidate can be executed")
	}
	plannedQuery := querySession.CurrentPlannedQuery()
	if plannedQuery != nil && plannedQuery.Query != currentCandidate.Query {
		return Outcome{Phase: session.PhaseValidate}, errors.New("only the current compiled workflow statement can be executed")
	}
	return engine.execute(ctx, querySession, currentCandidate.Query, plannedQuery)
}

// ExecutePlanned executes only the exact current statement published by the controlled plan compiler.
func (engine *Engine) ExecutePlanned(ctx context.Context, querySession *session.QuerySession, query string) (Outcome, error) {
	if querySession == nil {
		return Outcome{}, errors.New("query session is required")
	}
	if !bydbql.IsReadOnly(query) {
		return Outcome{}, errors.New("only one read-only SELECT or SHOW TOP statement can be executed")
	}
	plannedQuery := querySession.CurrentPlannedQuery()
	if plannedQuery == nil || plannedQuery.Query != query {
		return Outcome{}, errors.New("execution requires propose_query_plan to return valid=true and the query to match the current compiled workflow statement")
	}
	return engine.execute(ctx, querySession, query, plannedQuery)
}

func (engine *Engine) execute(
	ctx context.Context,
	querySession *session.QuerySession,
	query string,
	plannedQuery *session.PlannedQuery,
) (Outcome, error) {
	if engine == nil || engine.validator == nil || engine.executor == nil {
		return Outcome{Phase: session.PhaseError}, errors.New("BYDBQL execution is not configured")
	}
	if plannedQuery != nil {
		refreshPhase, refreshErr := engine.refreshPlannedSchema(ctx, querySession, *plannedQuery)
		if refreshErr != nil {
			return Outcome{Phase: refreshPhase}, refreshErr
		}
	}
	validation, validationErr := engine.validator.Validate(ctx, query, &querySession.SchemaSnapshot)
	if validationErr != nil {
		return Outcome{Phase: session.PhaseError}, fmt.Errorf("failed to revalidate query before execution: %w", validationErr)
	}
	querySession.Validation = validation
	if currentCandidate := querySession.CurrentCandidate(); currentCandidate != nil && currentCandidate.Query == query {
		currentCandidate.Validation = validation
	}
	outcome := Outcome{Validation: validation, Phase: session.PhaseExecuted}
	if !validation.Valid {
		outcome.Phase = session.PhaseValidate
		return outcome, nil
	}
	executionResult, executeErr := engine.executor.Execute(ctx, querySession, query)
	outcome.Executed = true
	if executeErr != nil {
		executionResult.Error = executeErr.Error()
		if executionResult.Summary == "" {
			executionResult.Summary = executeErr.Error()
		}
	}
	querySession.ExecutionResult = executionResult
	outcome.Result = executionResult
	if executeErr != nil {
		outcome.Phase = session.PhaseError
		return outcome, fmt.Errorf("failed to execute query: %w", executeErr)
	}
	if plannedQuery != nil {
		outcome.Next = querySession.CompletePlannedQuery(query)
	}
	return outcome, nil
}

func (engine *Engine) refreshPlannedSchema(
	ctx context.Context,
	querySession *session.QuerySession,
	plannedQuery session.PlannedQuery,
) (session.Phase, error) {
	schemaSnapshot, schemaErr := engine.executor.DiscoverSchema(ctx, tools.SchemaRequest{
		Type:   plannedQuery.ResourceType,
		Name:   plannedQuery.Name,
		Groups: plannedQuery.Groups,
	})
	if schemaErr != nil {
		return session.PhaseError, fmt.Errorf("failed to refresh schema before execution: %w", schemaErr)
	}
	schemaSnapshot = querySession.CacheSchema(schemaSnapshot)
	if plannedQuery.SchemaFingerprint != "" && plannedQuery.SchemaFingerprint != schemaSnapshot.Fingerprint {
		return session.PhaseValidate, errors.New("resource schema changed after plan compilation; regenerate the query plan")
	}
	querySession.ActivateSchema(schemaSnapshot)
	return "", nil
}
