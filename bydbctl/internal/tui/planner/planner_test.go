// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package planner

import (
	"strings"
	"testing"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
)

func TestCompileBuildsTypedMeasureQuery(t *testing.T) {
	schema := session.SchemaSnapshot{
		Type:   session.ResourceTypeMeasure,
		Name:   "service_latency",
		Groups: []string{"production"},
		Loaded: true,
		Columns: []session.SchemaColumn{
			{Name: "endpoint", Kind: session.SchemaColumnTag, Type: session.SchemaValueTypeString, Indexed: true},
			{Name: "status", Kind: session.SchemaColumnTag, Type: session.SchemaValueTypeInt},
			{Name: "latency", Kind: session.SchemaColumnField, Type: session.SchemaValueTypeFloat},
		},
	}
	plan := QueryPlan{
		Resource: Resource{Type: session.ResourceTypeMeasure, Name: "service_latency", Groups: []string{"production"}},
		Projection: []Projection{
			{Column: "endpoint"},
			{Aggregate: &Aggregate{Function: AggregateMean, Column: "latency"}},
		},
		Filter:    &Predicate{Column: "status", Operator: OperatorEqual, Value: 500},
		GroupBy:   []string{"endpoint"},
		OrderBy:   &Order{Column: "endpoint", Direction: OrderAscending},
		TimeRange: TimeRange{Start: "-30m"},
		Limit:     10,
	}

	compiled, compileErr := Compile(plan, schema)
	if compileErr != nil {
		t.Fatalf("Compile returned error: %v", compileErr)
	}
	expected := "SELECT endpoint, MEAN(latency) FROM MEASURE service_latency IN production TIME > '-30m' " +
		"WHERE status = 500 GROUP BY endpoint ORDER BY endpoint ASC LIMIT 10"
	if compiled.Query != expected {
		t.Fatalf("unexpected query:\nwant: %s\n got: %s", expected, compiled.Query)
	}
}

func TestCompileRejectsTypedFilterWithoutSchemaMetadata(t *testing.T) {
	plan := QueryPlan{
		Resource:  Resource{Type: session.ResourceTypeMeasure, Name: "service_latency", Groups: []string{"production"}},
		Filter:    &Predicate{Column: "status", Operator: OperatorEqual, Value: 500},
		TimeRange: TimeRange{Start: "-30m"},
		Limit:     10,
	}

	_, compileErr := Compile(plan, session.SchemaSnapshot{Type: session.ResourceTypeMeasure, Name: "service_latency"})
	if compileErr == nil {
		t.Fatal("expected typed filter to require schema metadata")
	}
	if !strings.Contains(compileErr.Error(), "typed schema metadata") {
		t.Fatalf("unexpected error: %v", compileErr)
	}
}

func TestCompileRejectsUnsupportedPlanShapes(t *testing.T) {
	measure := session.SchemaSnapshot{
		Type:   session.ResourceTypeMeasure,
		Name:   "service_latency",
		Groups: []string{"production"},
		Columns: []session.SchemaColumn{
			{Name: "endpoint", Kind: session.SchemaColumnTag, Type: session.SchemaValueTypeString},
			{Name: "latency", Kind: session.SchemaColumnField, Type: session.SchemaValueTypeFloat},
			{Name: "cpm", Kind: session.SchemaColumnField, Type: session.SchemaValueTypeInt},
		},
	}
	stream := session.SchemaSnapshot{
		Type:    session.ResourceTypeStream,
		Name:    "logs",
		Groups:  []string{"production"},
		Columns: []session.SchemaColumn{{Name: "service", Kind: session.SchemaColumnTag, Type: session.SchemaValueTypeString}},
	}
	topN := session.SchemaSnapshot{
		Type:   session.ResourceTypeTopN,
		Name:   "service_latency_topn",
		Groups: []string{"production"},
	}
	cases := []struct {
		name   string
		plan   QueryPlan
		schema session.SchemaSnapshot
	}{
		{
			name:   "stream group by",
			plan:   QueryPlan{Resource: Resource{Type: session.ResourceTypeStream, Name: "logs", Groups: []string{"production"}}, GroupBy: []string{"service"}},
			schema: stream,
		},
		{
			name: "two aggregates",
			plan: QueryPlan{
				Resource: Resource{Type: session.ResourceTypeMeasure, Name: "service_latency", Groups: []string{"production"}},
				Projection: []Projection{
					{Aggregate: &Aggregate{Function: AggregateMean, Column: "latency"}},
					{Aggregate: &Aggregate{Function: AggregateSum, Column: "cpm"}},
				},
			},
			schema: measure,
		},
		{
			name: "topn ignored projection",
			plan: QueryPlan{
				Resource:   Resource{Type: session.ResourceTypeTopN, Name: "service_latency_topn", Groups: []string{"production"}},
				Projection: []Projection{{Column: "endpoint"}},
			},
			schema: topN,
		},
		{
			name: "topn ignored aggregate column",
			plan: QueryPlan{
				Resource:  Resource{Type: session.ResourceTypeTopN, Name: "service_latency_topn", Groups: []string{"production"}},
				Aggregate: &Aggregate{Function: AggregateSum, Column: "latency"},
			},
			schema: topN,
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if _, compileErr := Compile(testCase.plan, testCase.schema); compileErr == nil {
				t.Fatal("expected unsupported plan shape to be rejected")
			}
		})
	}
}

func TestCompileDisplayDraftRendersResourceSkeleton(t *testing.T) {
	draft := CompileDisplayDraft(QueryPlan{
		Resource: Resource{Type: session.ResourceTypeMeasure, Name: "instance_jvm_cpu_hour", Groups: []string{"sw_metricsHour"}},
		Limit:    5,
	})
	if !strings.Contains(draft, "SELECT * FROM MEASURE instance_jvm_cpu_hour IN sw_metricsHour") {
		t.Fatalf("unexpected display draft: %s", draft)
	}
	if !strings.Contains(draft, "LIMIT 5") {
		t.Fatalf("expected limit in display draft: %s", draft)
	}
}
