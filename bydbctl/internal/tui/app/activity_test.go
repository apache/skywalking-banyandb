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
	"testing"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/agent"
)

func TestRecordAgentActivitiesSkipsMessageDelta(t *testing.T) {
	model := NewModel(Config{})
	initialCount := len(model.activityLog)
	model.recordAgentActivities([]agent.Event{
		{Kind: agent.EventKindMessageDelta, Message: "SELECT"},
		{Kind: agent.EventKindMessageDelta, Message: " *"},
		{Kind: agent.EventKindPlanUpdate, Message: "draft BYDBQL"},
	})

	if len(model.activityLog) != initialCount+1 {
		t.Fatalf("expected %d activity entries, got %d", initialCount+1, len(model.activityLog))
	}
	entry := model.activityLog[len(model.activityLog)-1]
	if entry.title != "plan: draft BYDBQL" {
		t.Fatalf("unexpected activity title: %q", entry.title)
	}
}
