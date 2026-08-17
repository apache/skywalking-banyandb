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

package app

import (
	"context"
	"fmt"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/agent"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/tools"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/workflow"
)

// The messages below are the only way background work re-enters Update, so every asynchronous
// result carries the session it produced rather than mutating shared state.

type catalogMsg struct {
	loadErr error
	catalog session.SchemaCatalog
}

type schemaDetailMsg struct {
	loadErr  error
	entry    session.CatalogEntry
	snapshot session.SchemaSnapshot
}

type workflowMsg struct {
	err           error
	querySession  *session.QuerySession
	status        string
	events        []agent.Event
	clearTurnHint bool
	// schemaAnswer marks a turn answered from the schema catalog rather than by running a query.
	schemaAnswer bool
}

type agentStartedMsg struct {
	querySession *session.QuerySession
	updates      <-chan workflow.TurnUpdate
	startErr     error
}

type agentTurnUpdateMsg struct {
	updates <-chan workflow.TurnUpdate
	update  workflow.TurnUpdate
}

type queryDebounceMsg struct {
	revision int
}

type turnTimeoutMsg struct {
	startedAt time.Time
}

func (m Model) nextAgentUpdateCmd(updates <-chan workflow.TurnUpdate) tea.Cmd {
	return func() tea.Msg {
		update, open := <-updates
		if !open {
			return agentTurnUpdateMsg{update: workflow.TurnUpdate{Done: true, Err: fmt.Errorf("agent stream closed unexpectedly")}, updates: updates}
		}
		return agentTurnUpdateMsg{update: update, updates: updates}
	}
}

func (m Model) queryDebounceCmd(revision int) tea.Cmd {
	return tea.Tick(queryValidationDebounce, func(time.Time) tea.Msg {
		return queryDebounceMsg{revision: revision}
	})
}

func (m Model) turnTimeoutCmd(startedAt time.Time) tea.Cmd {
	return tea.Tick(20*time.Second, func(time.Time) tea.Msg {
		return turnTimeoutMsg{startedAt: startedAt}
	})
}

func (m Model) agentCmd(ctx context.Context, messageValue string) tea.Cmd {
	runner := m.runner
	options := m.startOptions()
	query := m.query.Value()
	querySession := m.querySession
	return func() tea.Msg {
		updatedSession, ensureErr := ensureSession(ctx, runner, querySession, options, query)
		if ensureErr != nil {
			return agentStartedMsg{startErr: ensureErr}
		}
		updates, startErr := runner.StartAgentTurn(ctx, updatedSession, messageValue)
		return agentStartedMsg{querySession: updatedSession, updates: updates, startErr: startErr}
	}
}

// describeCmd reads one resource schema and records it as a direct catalog answer.
func (m Model) describeCmd(ctx context.Context, request workflow.DescribeRequest, messageValue string) tea.Cmd {
	runner := m.runner
	options := m.startOptions()
	query := m.query.Value()
	querySession := m.querySession
	return func() tea.Msg {
		updatedSession, ensureErr := ensureSession(ctx, runner, querySession, options, query)
		if ensureErr != nil {
			return workflowMsg{err: ensureErr}
		}
		if describeErr := runner.DescribeResource(ctx, updatedSession, request, messageValue); describeErr != nil {
			return workflowMsg{querySession: updatedSession, err: describeErr}
		}
		return workflowMsg{
			querySession:  updatedSession,
			status:        statusSchemaComplete,
			clearTurnHint: true,
			schemaAnswer:  true,
		}
	}
}

func (m Model) validateCmd() tea.Cmd {
	runner := m.runner
	options := m.startOptions()
	query := m.query.Value()
	querySession := m.querySession
	return func() tea.Msg {
		updatedSession, ensureErr := ensureSession(context.Background(), runner, querySession, options, query)
		if ensureErr != nil {
			return workflowMsg{err: ensureErr}
		}
		if strings.TrimSpace(query) == "" {
			if currentCandidate := updatedSession.CurrentCandidate(); currentCandidate != nil {
				query = currentCandidate.Query
			}
		}
		if validateErr := runner.ValidateManualQuery(context.Background(), updatedSession, query); validateErr != nil {
			return workflowMsg{
				querySession: updatedSession,
				err:          validateErr,
			}
		}
		return workflowMsg{
			querySession: updatedSession,
			status:       "validation complete",
		}
	}
}

func (m Model) executeCmd(ctx context.Context) tea.Cmd {
	runner := m.runner
	options := m.startOptions()
	query := m.query.Value()
	querySession := m.querySession
	return func() tea.Msg {
		updatedSession, ensureErr := ensureSession(ctx, runner, querySession, options, query)
		if ensureErr != nil {
			return workflowMsg{err: ensureErr}
		}
		if executeErr := runner.ExecuteCurrent(ctx, updatedSession); executeErr != nil {
			return workflowMsg{
				querySession: updatedSession,
				err:          executeErr,
			}
		}
		return workflowMsg{
			querySession: updatedSession,
			status:       "execution complete",
		}
	}
}

func (m Model) loadCatalogCmd() tea.Cmd {
	executor := m.executor
	return func() tea.Msg {
		if executor == nil {
			return catalogMsg{loadErr: fmt.Errorf("schema executor is not configured")}
		}
		catalog, catalogErr := executor.DiscoverCatalog(context.Background())
		if catalogErr != nil {
			return catalogMsg{loadErr: catalogErr}
		}
		return catalogMsg{catalog: catalog}
	}
}

func (m Model) loadSchemaDetailCmd(entry session.CatalogEntry) tea.Cmd {
	executor := m.executor
	return func() tea.Msg {
		if executor == nil {
			return schemaDetailMsg{entry: entry, loadErr: fmt.Errorf("schema executor is not configured")}
		}
		snapshot, schemaErr := executor.DiscoverSchema(context.Background(), tools.SchemaRequest{
			Type:   entry.Type,
			Name:   entry.Name,
			Groups: []string{entry.Group},
		})
		if schemaErr != nil {
			return schemaDetailMsg{entry: entry, loadErr: schemaErr}
		}
		return schemaDetailMsg{entry: entry, snapshot: snapshot}
	}
}

// startOptions describes the resource and time slots a turn should run against.
func (m *Model) startOptions() workflow.StartOptions {
	options := workflow.StartOptions{
		TimeRange: session.TimeRange{
			Start: m.start.Value(),
			End:   m.end.Value(),
		},
		Goal: m.currentGoal(),
	}
	if m.composerReference != nil {
		options.ResourceType = m.composerReference.Type
		options.ResourceName = m.composerReference.Name
		options.Groups = []string{m.composerReference.Group}
		options.NameProvided = true
		options.GroupsProvided = true
		options.TypeProvided = true
	}
	return options
}

func (m Model) currentGoal() string {
	if m.querySession != nil && strings.TrimSpace(m.querySession.UserGoal) != "" {
		return m.querySession.UserGoal
	}
	if queuedMessage := strings.TrimSpace(m.queuedMessage); queuedMessage != "" {
		return queuedMessage
	}
	return strings.TrimSpace(m.message.Value())
}

// ensureSession starts or resyncs the workflow session, then registers a manual edit as a candidate.
//
// Every command runs this first so a turn always sees the query the user can currently see.
func ensureSession(
	ctx context.Context,
	runner *workflow.Runner,
	querySession *session.QuerySession,
	options workflow.StartOptions,
	query string,
) (*session.QuerySession, error) {
	updatedSession := querySession
	if updatedSession == nil {
		var startErr error
		updatedSession, startErr = runner.StartSession(ctx, options)
		if startErr != nil {
			return nil, startErr
		}
	} else {
		var syncErr error
		updatedSession, syncErr = runner.SyncSession(ctx, updatedSession, options)
		if syncErr != nil {
			return nil, syncErr
		}
	}
	currentCandidate := updatedSession.CurrentCandidate()
	if strings.TrimSpace(query) != "" && (currentCandidate == nil || strings.TrimSpace(currentCandidate.Query) != strings.TrimSpace(query)) {
		if validateErr := runner.ValidateManualQuery(ctx, updatedSession, query); validateErr != nil {
			return nil, validateErr
		}
	}
	return updatedSession, nil
}
