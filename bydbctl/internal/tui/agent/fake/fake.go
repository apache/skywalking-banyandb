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

// Package fake provides a deterministic agent implementation for TUI tests and demos.
package fake

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/agent"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/tuirun"
)

var limitPattern = regexp.MustCompile(`(?i)limit\s*(\d+)`)

// placeholderResourceName names the resource in a fixture candidate when the payload sets none.
const placeholderResourceName = "service_endpoint_latency"

// Gateway is a deterministic agent implementation.
type Gateway struct {
	now func() time.Time
}

// NewGateway creates a fake gateway.
func NewGateway() *Gateway {
	return &Gateway{
		now: time.Now,
	}
}

// Start creates a local fake session.
func (gateway *Gateway) Start(_ context.Context, req agent.StartRequest) (agent.Session, error) {
	return agent.Session{
		ID:        "fake-" + uuid.NewString(),
		Provider:  req.Provider,
		StartedAt: gateway.now(),
	}, nil
}

// Send emits a deterministic review and final BYDBQL candidate.
func (gateway *Gateway) Send(ctx context.Context, _ string, req agent.TurnRequest) (<-chan agent.Event, error) {
	events := make(chan agent.Event, 4)
	tuirun.Go(ctx, "fake-turn", func(turnCtx context.Context) {
		defer close(events)
		if !send(turnCtx, events, agent.Event{
			Kind:    agent.EventKindPlanUpdate,
			Message: "generate read-only BYDBQL candidate from goal and schema",
		}) {
			return
		}
		candidate := strings.TrimSpace(req.Payload.Candidate)
		if candidate == "" {
			candidate = buildCandidate(req.Payload)
		}
		explanation := "fake agent generated a BYDBQL candidate"
		if req.Payload.ValidationError != nil {
			explanation = fmt.Sprintf("fake agent kept the candidate after validation feedback: %s", *req.Payload.ValidationError)
		}
		if !send(turnCtx, events, agent.Event{
			Kind:      agent.EventKindCandidate,
			Candidate: candidate,
			Origin:    agent.EventOriginToolBridge,
			ToolName:  "propose_query_plan",
		}) {
			return
		}
		_ = send(turnCtx, events, agent.Event{
			Kind:        agent.EventKindFinalResponse,
			Message:     explanation,
			Explanation: explanation,
		})
	})
	return events, nil
}

// Interrupt interrupts a fake turn.
func (gateway *Gateway) Interrupt(_ context.Context, _ string) error {
	return nil
}

// Close closes the fake gateway.
func (gateway *Gateway) Close() error {
	return nil
}

// buildCandidate renders a deterministic candidate from whatever the payload supplies.
//
// The placeholders below stand in only for an unset slot, so a test that cares about the rendered
// statement supplies the schema it expects to see rather than relying on them.
func buildCandidate(payload agent.RequestPayload) string {
	resourceType := strings.ToUpper(strings.TrimSpace(payload.Schema.Type))
	if resourceType == "" {
		resourceType = "MEASURE"
	}
	resourceName := strings.TrimSpace(payload.Schema.Name)
	if resourceName == "" {
		resourceName = placeholderResourceName
	}
	group := "default"
	if len(payload.Schema.Groups) > 0 && strings.TrimSpace(payload.Schema.Groups[0]) != "" {
		group = strings.TrimSpace(payload.Schema.Groups[0])
	}
	limit := "10"
	if payload.QueryHints.LimitHint > 0 {
		limit = fmt.Sprintf("%d", payload.QueryHints.LimitHint)
	} else if matches := limitPattern.FindStringSubmatch(payload.Goal); len(matches) == 2 {
		limit = matches[1]
	}
	timeClause := buildTimeClause(payload.TimeRange, payload.QueryHints.TimeRangeHint)
	if strings.TrimSpace(payload.TemplateHint) != "" && payload.QueryHints.PreferShowTop {
		return strings.TrimSpace(payload.TemplateHint)
	}
	if payload.QueryHints.PreferShowTop || resourceType == "TOPN" {
		return fmt.Sprintf("SHOW TOP %s FROM MEASURE %s IN %s %s AGGREGATE BY SUM ORDER BY DESC", limit, resourceName, group, timeClause)
	}
	if resourceType == "PROPERTY" {
		return fmt.Sprintf("SELECT * FROM PROPERTY %s IN %s LIMIT %s", resourceName, group, limit)
	}
	return fmt.Sprintf("SELECT * FROM %s %s IN %s %s LIMIT %s", resourceType, resourceName, group, timeClause, limit)
}

func buildTimeClause(timeRange agent.TimeRangePayload, hint string) string {
	start := strings.TrimSpace(timeRange.Start)
	end := strings.TrimSpace(timeRange.End)
	if start == "" {
		start = strings.TrimSpace(hint)
	}
	if start == "" {
		start = "-30m"
	}
	if end != "" {
		return fmt.Sprintf("TIME BETWEEN '%s' AND '%s'", start, end)
	}
	return fmt.Sprintf("TIME > '%s'", start)
}

func send(ctx context.Context, events chan<- agent.Event, event agent.Event) bool {
	select {
	case <-ctx.Done():
		return false
	case events <- event:
		return true
	}
}
