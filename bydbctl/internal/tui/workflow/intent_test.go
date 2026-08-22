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
	"testing"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
)

func TestClassifyIntentPreferShowTop(t *testing.T) {
	hints := ClassifyIntent(&session.QuerySession{
		UserGoal:     "top 10 slow endpoints in the last 30 minutes",
		ResourceName: "service_latency",
		Groups:       []string{"production"},
		TimeRange:    session.TimeRange{Start: "-30m"},
		SlotsPinned:  true,
	})
	if !hints.PreferShowTop {
		t.Fatal("expected prefer_show_top")
	}
	if hints.LimitHint != 10 {
		t.Fatalf("unexpected limit hint: %d", hints.LimitHint)
	}
	if hints.TimeRangeHint != "-30m" {
		t.Fatalf("unexpected time range hint: %s", hints.TimeRangeHint)
	}
	if !hints.UseSlots {
		t.Fatal("expected use_slots")
	}
}

func TestClassifyIntentLimitHint(t *testing.T) {
	hints := ClassifyIntent(&session.QuerySession{
		UserGoal: "show the last 30 zipkin spans",
	})
	if hints.LimitHint != 30 {
		t.Fatalf("unexpected limit hint: %d", hints.LimitHint)
	}
}
