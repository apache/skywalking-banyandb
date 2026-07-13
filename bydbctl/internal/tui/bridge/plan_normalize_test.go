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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bridge

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/planner"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
)

func TestNormalizePlanArgumentRepairsCommonAgentMistakes(t *testing.T) {
	normalized := normalizePlanArgument(map[string]any{
		"type":     "MEASURE",
		"name":     "service_endpoint_latency",
		"groups":   []any{"sw_metrics"},
		"top_n":    map[string]any{"value": 10},
		"aggregate": "MEAN",
		"order_by":  "DESC",
		"time_range": map[string]any{"start": "-30m"},
	}).(map[string]any)

	resource, _ := normalized["resource"].(map[string]any)
	if resource["type"] != "TOPN" || resource["name"] != "service_endpoint_latency" {
		t.Fatalf("unexpected normalized resource: %+v", resource)
	}
	if normalized["top_n"] != 10 {
		t.Fatalf("expected top_n integer, got %#v", normalized["top_n"])
	}
	aggregate, _ := normalized["aggregate"].(map[string]any)
	if aggregate["function"] != "MEAN" {
		t.Fatalf("unexpected aggregate: %+v", aggregate)
	}
	orderBy, _ := normalized["order_by"].(map[string]any)
	if orderBy["direction"] != "DESC" {
		t.Fatalf("unexpected order_by: %+v", orderBy)
	}

	var plan planner.QueryPlan
	if decodeErr := decodePlanArgument(normalized, &plan); decodeErr != nil {
		t.Fatalf("failed to decode normalized plan: %v", decodeErr)
	}
	if plan.Resource.Type != session.ResourceTypeTopN || plan.TopN != 10 {
		t.Fatalf("unexpected decoded plan: %+v", plan)
	}
}

func TestPlannedQueriesDecodesNormalizedWorkflow(t *testing.T) {
	plans, planErr := plannedQueries(map[string]any{
		"workflow": map[string]any{"steps": []any{
			map[string]any{
				"id":       "failed-requests",
				"resource": map[string]any{"type": "STREAM", "name": "logs", "groups": []any{"production"}},
				"filter":   map[string]any{"column": "status", "operator": "=", "value": 500},
				"limit":    10,
			},
		}},
	})
	if planErr != nil {
		t.Fatalf("plannedQueries failed: %v", planErr)
	}
	if len(plans) != 1 || plans[0].ID != "failed-requests" {
		t.Fatalf("unexpected plans: %+v", plans)
	}
}

func TestProposeQueryPlanReturnsSchemaHintForMalformedInput(t *testing.T) {
	toolBridge := New(Config{
		Executor:  &stubExecutor{},
		Validator: &stubValidator{report: session.ValidationReport{Valid: true}},
	})
	toolBridge.SetSession(&session.QuerySession{})
	result := toolBridge.Call(context.Background(), Call{
		Name: ToolProposeQueryPlan,
		Arguments: map[string]any{
			"plan":     map[string]any{"limit": 10},
			"workflow": map[string]any{"steps": []any{}},
		},
	})
	if result.Err != nil || !strings.Contains(result.Content, `"valid":false`) || !strings.Contains(result.Content, "schema_hint") {
		t.Fatalf("expected structured malformed-plan feedback, got %+v", result)
	}
}

func TestProposeQueryPlanCompilesNormalizedShowTopPlanForMeasureCatalogEntry(t *testing.T) {
	schema := session.SchemaSnapshot{
		Type:   session.ResourceTypeMeasure,
		Name:   "service_endpoint_latency",
		Groups: []string{"sw_metrics"},
		Loaded: true,
		Columns: []session.SchemaColumn{
			{Name: "endpoint", Kind: session.SchemaColumnTag, Type: session.SchemaValueTypeString, Indexed: true},
			{Name: "latency", Kind: session.SchemaColumnField, Type: session.SchemaValueTypeFloat},
		},
	}
	toolBridge := New(Config{
		Executor:  &stubExecutor{schema: schema},
		Validator: &stubValidator{report: session.ValidationReport{Valid: true, Message: "valid", QueryType: "TOPN"}},
	})
	toolBridge.SetSession(&session.QuerySession{SchemaSnapshot: session.SchemaSnapshot{
		Loaded: true,
		Type:   session.ResourceTypeMeasure,
		Name:   "service_endpoint_latency",
		Groups: []string{"sw_metrics"},
		Columns: []session.SchemaColumn{
			{Name: "endpoint", Kind: session.SchemaColumnTag, Type: session.SchemaValueTypeString, Indexed: true},
			{Name: "latency", Kind: session.SchemaColumnField, Type: session.SchemaValueTypeFloat},
		},
		Catalog: []session.CatalogEntry{
			{Group: "sw_metrics", Type: session.ResourceTypeMeasure, Name: "service_endpoint_latency"},
		},
	}})
	toolBridge.SetRankedCandidates([]session.CatalogEntry{
		{Group: "sw_metrics", Type: session.ResourceTypeMeasure, Name: "service_endpoint_latency"},
	})
	result := toolBridge.Call(context.Background(), Call{
		Name: ToolProposeQueryPlan,
		Arguments: map[string]any{
			"plan": map[string]any{
				"type":      "MEASURE",
				"name":      "service_endpoint_latency",
				"groups":    []any{"sw_metrics"},
				"top_n":     map[string]any{"limit": 10},
				"aggregate": map[string]any{"function": "MEAN", "column": ""},
				"order_by":  map[string]any{"direction": "DESC", "column": ""},
				"time_range": map[string]any{
					"start": "-30m",
				},
			},
		},
	})
	if result.Err != nil {
		t.Fatalf("propose plan failed: %v", result.Err)
	}
	var response map[string]any
	if unmarshalErr := json.Unmarshal([]byte(result.Content), &response); unmarshalErr != nil {
		t.Fatalf("failed to parse response: %v", unmarshalErr)
	}
	if response["valid"] != true {
		t.Fatalf("expected valid plan, got %s", result.Content)
	}
	if query, _ := response["query"].(string); !strings.Contains(query, "SHOW TOP 10 FROM MEASURE service_endpoint_latency") {
		t.Fatalf("unexpected compiled query: %s", query)
	}
}
