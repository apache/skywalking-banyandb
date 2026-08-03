// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package approval coordinates one-time user decisions for BYDBQL execution.
package approval

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

const requestBufferSize = 16

var (
	timeRangePattern = regexp.MustCompile(`(?i)\bTIME\s+(?:BETWEEN\s+'[^']+'\s+AND\s+'[^']+'|[><]=?\s+'[^']+')`)
	limitPattern     = regexp.MustCompile(`(?i)\bLIMIT\s+(\d+)`)
	topNPattern      = regexp.MustCompile(`(?i)\bSHOW\s+TOP\s+(\d+)`)
)

// Source identifies the caller that requested access to execute BYDBQL.
type Source string

// Approval request sources.
const (
	SourceAgentTool   Source = "agent_tool"
	SourceAgentProbe  Source = "agent_probe"
	SourceManualProbe Source = "manual_probe"
	SourceManual      Source = "manual"
)

// IsProbe reports whether the source requests a bounded preview rather than a full execution.
func (source Source) IsProbe() bool {
	return source == SourceAgentProbe || source == SourceManualProbe
}

// Request describes the exact statement that needs a user decision.
type Request struct {
	CreatedAt   time.Time
	ID          string
	Query       string
	Resource    string
	Groups      []string
	TimeRange   string
	Limit       string
	Timeout     time.Duration
	PreviewRows int
	Source      Source
}

// Decision records whether the user approved the pending statement.
type Decision struct {
	Approved bool
}

// NewRequest describes one exact BYDBQL statement for approval.
func NewRequest(query, resource string, groups []string, source Source) Request {
	trimmedQuery := strings.TrimSpace(query)
	return Request{
		Query:     trimmedQuery,
		Resource:  strings.TrimSpace(resource),
		Groups:    append([]string(nil), groups...),
		TimeRange: strings.TrimSpace(timeRangePattern.FindString(trimmedQuery)),
		Limit:     queryLimit(trimmedQuery),
		Source:    source,
	}
}

// WithLimits adds the executor's effective bounds to an approval request.
func WithLimits(request Request, timeout time.Duration, previewRows int) Request {
	request.Timeout = timeout
	request.PreviewRows = previewRows
	return request
}

// Controller owns the lifecycle of pending one-time execution approvals.
type Controller struct {
	now      func() time.Time
	requests chan Request

	mu      sync.Mutex
	pending map[string]chan Decision
	policy  ExecutionPolicy
}

// NewController creates an approval controller.
func NewController() *Controller {
	return &Controller{
		now:      time.Now,
		requests: make(chan Request, requestBufferSize),
		pending:  make(map[string]chan Decision),
		policy:   PolicyAskEveryTime,
	}
}

// SetPolicy stores the active execution policy for this TUI session.
func (controller *Controller) SetPolicy(policy ExecutionPolicy) {
	if controller == nil {
		return
	}
	controller.mu.Lock()
	controller.policy = NormalizeExecutionPolicy(string(policy))
	controller.mu.Unlock()
}

// Policy returns the active execution policy.
func (controller *Controller) Policy() ExecutionPolicy {
	if controller == nil {
		return PolicyAskEveryTime
	}
	controller.mu.Lock()
	defer controller.mu.Unlock()
	return controller.policy
}

// Requests returns newly pending execution requests.
func (controller *Controller) Requests() <-chan Request {
	return controller.requests
}

// Request waits until the exact statement is approved, rejected, or cancelled.
func (controller *Controller) Request(ctx context.Context, request Request) (Decision, error) {
	if controller == nil {
		return Decision{}, fmt.Errorf("approval controller is required")
	}
	request.Query = strings.TrimSpace(request.Query)
	if request.Query == "" {
		return Decision{}, fmt.Errorf("BYDBQL query is required for approval")
	}
	request.ID = uuid.NewString()
	request.CreatedAt = controller.now()
	request.Groups = append([]string(nil), request.Groups...)
	if controller.Policy().AutoApprove(request.Source, request.Source.IsProbe(), request.Query) {
		return Decision{Approved: true}, nil
	}
	decisionCh := make(chan Decision, 1)
	controller.mu.Lock()
	controller.pending[request.ID] = decisionCh
	controller.mu.Unlock()
	defer controller.remove(request.ID)
	select {
	case <-ctx.Done():
		return Decision{}, ctx.Err()
	case controller.requests <- request:
	}
	select {
	case <-ctx.Done():
		return Decision{}, ctx.Err()
	case decision := <-decisionCh:
		return decision, nil
	}
}

// Resolve completes an outstanding approval request exactly once.
func (controller *Controller) Resolve(requestID string, decision Decision) error {
	if controller == nil {
		return fmt.Errorf("approval controller is required")
	}
	controller.mu.Lock()
	decisionCh := controller.pending[requestID]
	if decisionCh != nil {
		delete(controller.pending, requestID)
	}
	controller.mu.Unlock()
	if decisionCh == nil {
		return fmt.Errorf("approval request %q is no longer pending", requestID)
	}
	decisionCh <- decision
	return nil
}

// Cancel rejects every outstanding approval request.
func (controller *Controller) Cancel() {
	if controller == nil {
		return
	}
	controller.mu.Lock()
	pending := controller.pending
	controller.pending = make(map[string]chan Decision)
	controller.mu.Unlock()
	for _, decisionCh := range pending {
		decisionCh <- Decision{}
	}
}

func (controller *Controller) remove(requestID string) {
	controller.mu.Lock()
	delete(controller.pending, requestID)
	controller.mu.Unlock()
}

func queryLimit(query string) string {
	if matches := limitPattern.FindStringSubmatch(query); len(matches) == 2 {
		return matches[1]
	}
	if matches := topNPattern.FindStringSubmatch(query); len(matches) == 2 {
		return matches[1]
	}
	return "-"
}
