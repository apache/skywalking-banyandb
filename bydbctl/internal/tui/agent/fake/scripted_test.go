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
	"testing"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/agent"
)

func TestScriptedGatewayStreamsLifecycleEvents(t *testing.T) {
	gateway := NewScriptedGateway(Script{Events: []agent.Event{
		{Kind: agent.EventKindMessageDelta, Message: "drafting"},
		{Kind: agent.EventKindPlanUpdate, Message: "inspect schema"},
		{Kind: agent.EventKindToolCall, ToolName: "describe_schema", Status: agent.EventStatusRunning},
		{Kind: agent.EventKindClarification, Message: "Which group?"},
		{Kind: agent.EventKindApproval, ToolName: "execute_bydbql", Status: agent.EventStatusWaiting},
		{Kind: agent.EventKindCancelled, Message: "rejected", Status: agent.EventStatusCancelled},
		{Kind: agent.EventKindError, Message: "temporary failure", Status: agent.EventStatusFailed},
	}})
	agentSession, startErr := gateway.Start(context.Background(), agent.StartRequest{Provider: "fake-agent"})
	if startErr != nil {
		t.Fatalf("Start returned error: %v", startErr)
	}
	events, sendErr := gateway.Send(context.Background(), agentSession.ID, agent.TurnRequest{})
	if sendErr != nil {
		t.Fatalf("Send returned error: %v", sendErr)
	}
	var received []agent.EventKind
	for event := range events {
		received = append(received, event.Kind)
	}
	if len(received) != 7 || received[3] != agent.EventKindClarification || received[5] != agent.EventKindCancelled {
		t.Fatalf("unexpected lifecycle stream: %v", received)
	}
}
