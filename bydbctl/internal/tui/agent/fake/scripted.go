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

package fake

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/agent"
)

// Script describes normalized lifecycle events emitted by a test-only agent.
type Script struct {
	Events  []agent.Event
	Delay   time.Duration
	SendErr error
}

// ScriptedGateway is a test-only, scriptable agent event source.
type ScriptedGateway struct {
	script Script
	now    func() time.Time
}

// NewScriptedGateway creates a deterministic scripted event source for tests.
func NewScriptedGateway(script Script) *ScriptedGateway {
	return &ScriptedGateway{script: script, now: time.Now}
}

// Start creates a test agent session.
func (gateway *ScriptedGateway) Start(_ context.Context, req agent.StartRequest) (agent.Session, error) {
	return agent.Session{ID: "fake-agent-" + uuid.NewString(), Provider: req.Provider, StartedAt: gateway.now()}, nil
}

// Send streams the configured normalized events.
func (gateway *ScriptedGateway) Send(ctx context.Context, _ string, _ agent.TurnRequest) (<-chan agent.Event, error) {
	if gateway.script.SendErr != nil {
		return nil, gateway.script.SendErr
	}
	events := make(chan agent.Event, len(gateway.script.Events))
	go func() {
		defer close(events)
		for _, event := range gateway.script.Events {
			if gateway.script.Delay > 0 {
				timer := time.NewTimer(gateway.script.Delay)
				select {
				case <-ctx.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
			}
			if !send(ctx, events, event) {
				return
			}
		}
	}()
	return events, nil
}

// Interrupt interrupts a scripted turn.
func (gateway *ScriptedGateway) Interrupt(_ context.Context, _ string) error {
	return nil
}

// Close closes a scripted gateway.
func (gateway *ScriptedGateway) Close() error {
	return nil
}
