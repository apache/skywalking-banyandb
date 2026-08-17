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
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/tools"
)

// StartSession creates a session and discovers its initial schema context.
func (runner *Runner) StartSession(ctx context.Context, options StartOptions) (*session.QuerySession, error) {
	catalog, catalogErr := runner.executor.DiscoverCatalog(ctx)
	if catalogErr != nil {
		return nil, fmt.Errorf("failed to discover schema catalog: %w", catalogErr)
	}
	if usesAutonomousDiscovery(options) {
		return newAutonomousSession(options, catalog, runner.now()), nil
	}
	resolved, schemaSnapshot, schemaErr := runner.discoverSessionSchema(ctx, options, catalog)
	if schemaErr != nil {
		return nil, fmt.Errorf("failed to discover schema: %w", schemaErr)
	}
	querySession := &session.QuerySession{
		ID:             uuid.NewString(),
		Phase:          session.PhaseIntent,
		UserGoal:       resolved.Goal,
		ResourceType:   resolved.ResourceType,
		ResourceName:   resolved.ResourceName,
		Groups:         append([]string(nil), resolved.Groups...),
		TimeRange:      resolved.TimeRange,
		SchemaSnapshot: schemaSnapshot,
		SlotsPinned:    resolved.SlotsPinned,
		AutoMatched:    resolved.AutoMatched,
	}
	querySession.ActivateSchema(schemaSnapshot)
	querySession.AddTranscript("workflow", "created BYDBQL agent session", runner.now())
	if resolved.AutoMatched {
		querySession.AddTranscript(
			"workflow",
			fmt.Sprintf("auto-matched resource %s %s in %s from goal", resolved.ResourceType, resolved.ResourceName, strings.Join(resolved.Groups, ",")),
			runner.now(),
		)
	}
	return querySession, nil
}

// SyncSession updates session slots and refreshes schema when the TUI inputs change.
func (runner *Runner) SyncSession(ctx context.Context, querySession *session.QuerySession, options StartOptions) (*session.QuerySession, error) {
	if querySession == nil {
		return runner.StartSession(ctx, options)
	}
	if !slotsChanged(querySession, options) {
		return querySession, nil
	}
	catalog, catalogErr := runner.executor.DiscoverCatalog(ctx)
	if catalogErr != nil {
		return nil, fmt.Errorf("failed to discover schema catalog: %w", catalogErr)
	}
	if usesAutonomousDiscovery(options) {
		querySession.UserGoal = strings.TrimSpace(options.Goal)
		querySession.TimeRange = applyTimeDefaults(options.TimeRange)
		querySession.SchemaSnapshot.SetCatalog(catalog)
		querySession.SlotsPinned = false
		querySession.AutoMatched = false
		querySession.AddTranscript("workflow", "refreshed catalog for autonomous schema discovery", runner.now())
		return querySession, nil
	}
	resolved, schemaSnapshot, schemaErr := runner.discoverSessionSchema(ctx, options, catalog)
	if schemaErr != nil {
		return nil, fmt.Errorf("failed to refresh schema: %w", schemaErr)
	}
	querySession.UserGoal = resolved.Goal
	querySession.ActivateSchema(schemaSnapshot)
	querySession.TimeRange = resolved.TimeRange
	querySession.SlotsPinned = resolved.SlotsPinned
	querySession.AutoMatched = resolved.AutoMatched
	querySession.AddTranscript("workflow", "refreshed schema after slot change", runner.now())
	if resolved.AutoMatched {
		querySession.AddTranscript(
			"workflow",
			fmt.Sprintf("auto-matched resource %s %s in %s from goal", resolved.ResourceType, resolved.ResourceName, strings.Join(resolved.Groups, ",")),
			runner.now(),
		)
	}
	return querySession, nil
}

func newAutonomousSession(options StartOptions, catalog session.SchemaCatalog, now time.Time) *session.QuerySession {
	schemaSnapshot := session.SchemaSnapshot{UpdatedAt: catalog.UpdatedAt}
	schemaSnapshot.SetCatalog(catalog)
	querySession := &session.QuerySession{
		ID:             uuid.NewString(),
		Phase:          session.PhaseIntent,
		UserGoal:       strings.TrimSpace(options.Goal),
		TimeRange:      applyTimeDefaults(options.TimeRange),
		AutoMatched:    false,
		SchemaSnapshot: schemaSnapshot,
	}
	querySession.AddTranscript("workflow", "created autonomous BYDBQL agent session", now)
	return querySession
}

func (runner *Runner) discoverSessionSchema(
	ctx context.Context,
	options StartOptions,
	catalog session.SchemaCatalog,
) (ResolvedSlots, session.SchemaSnapshot, error) {
	resolved := ResolveSessionSlots(options, catalog)
	schemaSnapshot, schemaErr := runner.executor.DiscoverSchema(ctx, tools.SchemaRequest{
		Type:   resolved.ResourceType,
		Name:   resolved.ResourceName,
		Groups: resolved.Groups,
	})
	if schemaErr != nil {
		return ResolvedSlots{}, session.SchemaSnapshot{}, schemaErr
	}
	schemaSnapshot.SetCatalog(catalog)
	return resolved, schemaSnapshot, nil
}

func usesAutonomousDiscovery(options StartOptions) bool {
	if options.NameProvided || options.GroupsProvided || options.TypeProvided {
		return false
	}
	if strings.TrimSpace(options.ResourceName) != "" {
		return false
	}
	return len(normalizeGroupsIfProvided(options.Groups)) == 0
}

func slotsChanged(querySession *session.QuerySession, options StartOptions) bool {
	if querySession.UserGoal != strings.TrimSpace(options.Goal) {
		return true
	}
	if options.TypeProvided && querySession.ResourceType != options.ResourceType {
		return true
	}
	if options.NameProvided && querySession.ResourceName != strings.TrimSpace(options.ResourceName) {
		return true
	}
	if options.GroupsProvided && !slices.Equal(querySession.Groups, normalizeGroupsIfProvided(options.Groups)) {
		return true
	}
	if querySession.TimeRange.Start != strings.TrimSpace(options.TimeRange.Start) || querySession.TimeRange.End != strings.TrimSpace(options.TimeRange.End) {
		return true
	}
	return false
}
