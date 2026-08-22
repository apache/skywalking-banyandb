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

// Package tools defines the bydbctl-owned tool boundary used by the TUI workflow.
package tools

import (
	"context"
	"time"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
)

// SchemaRequest describes a read-only schema discovery request.
type SchemaRequest struct {
	Type   session.ResourceType
	Name   string
	Groups []string
}

// Executor is the read-only tool boundary owned by WorkflowRunner.
type Executor interface {
	DiscoverCatalog(ctx context.Context) (session.SchemaCatalog, error)
	DiscoverSchema(ctx context.Context, req SchemaRequest) (session.SchemaSnapshot, error)
	Execute(ctx context.Context, querySession *session.QuerySession, query string) (session.ExecutionResult, error)
}

// ExecutionLimits are the bounded execution settings shown before query approval.
type ExecutionLimits struct {
	Timeout     time.Duration
	PreviewRows int
}

// ReadOnlyExecutor is the default local executor used before MCP tool exposure exists.
type ReadOnlyExecutor struct {
	now func() time.Time
}

// NewReadOnlyExecutor creates a read-only executor.
func NewReadOnlyExecutor() *ReadOnlyExecutor {
	return &ReadOnlyExecutor{
		now: time.Now,
	}
}

// DiscoverCatalog returns an empty catalog for the local placeholder executor.
func (executor *ReadOnlyExecutor) DiscoverCatalog(_ context.Context) (session.SchemaCatalog, error) {
	return session.SchemaCatalog{UpdatedAt: executor.now()}, nil
}

// DiscoverSchema returns the schema request as a snapshot placeholder.
func (executor *ReadOnlyExecutor) DiscoverSchema(_ context.Context, req SchemaRequest) (session.SchemaSnapshot, error) {
	return session.SchemaSnapshot{
		UpdatedAt: executor.now(),
		Type:      req.Type,
		Name:      req.Name,
		Groups:    append([]string(nil), req.Groups...),
	}, nil
}

// Execute reports that real BYDBQL execution requires a connected BanyanDB tool executor.
func (executor *ReadOnlyExecutor) Execute(_ context.Context, _ *session.QuerySession, query string) (session.ExecutionResult, error) {
	return session.ExecutionResult{
		CheckedAt: executor.now(),
		Rows:      0,
		Query:     query,
		Command:   "POST /api/v1/bydbql/query",
		Path:      "/api/v1/bydbql/query",
		Summary:   "BYDBQL execution is not configured; connect BanyanDB HTTP tools before executing",
	}, nil
}
