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
		OrderBy:   &Order{IndexRule: "endpoint", Direction: OrderAscending},
		TimeRange: TimeRange{Start: "-30m"},
		Limit:     10,
	}

	compiled, compileErr := Compile(plan, schema)
	if compileErr != nil {
		t.Fatalf("Compile returned error: %v", compileErr)
	}
	expected := "SELECT endpoint, MEAN(latency) FROM MEASURE service_latency IN production TIME > '-30m' " +
		"WHERE status = 500 GROUP BY endpoint::TAG ORDER BY endpoint ASC LIMIT 10"
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

func TestCompileRejectsFieldFiltersAndTagAggregations(t *testing.T) {
	schema := session.SchemaSnapshot{
		Type:   session.ResourceTypeMeasure,
		Name:   "service_latency",
		Groups: []string{"production"},
		Columns: []session.SchemaColumn{
			{Name: "service", Kind: session.SchemaColumnTag, Type: session.SchemaValueTypeString},
			{Name: "latency", Kind: session.SchemaColumnField, Type: session.SchemaValueTypeFloat},
		},
	}
	resource := Resource{Type: session.ResourceTypeMeasure, Name: "service_latency", Groups: []string{"production"}}
	_, filterErr := Compile(QueryPlan{
		Resource:  resource,
		Filter:    &Predicate{Column: "latency", Operator: OperatorGreaterThan, Value: 100},
		TimeRange: TimeRange{Start: "-30m"},
	}, schema)
	if filterErr == nil || DescribeError(filterErr).Code != "FILTER_COLUMN_NOT_TAG" {
		t.Fatalf("expected field-filter diagnostic, got %v", filterErr)
	}
	_, aggregateErr := Compile(QueryPlan{
		Resource:   resource,
		Projection: []Projection{{Aggregate: &Aggregate{Function: AggregateCount, Column: "service"}}},
		TimeRange:  TimeRange{Start: "-30m"},
	}, schema)
	if aggregateErr == nil || DescribeError(aggregateErr).Code != "AGGREGATE_COLUMN_NOT_FIELD" {
		t.Fatalf("expected tag-aggregation diagnostic, got %v", aggregateErr)
	}
}

func TestCompileUsesIndexRuleNameForOrderBy(t *testing.T) {
	schema := session.SchemaSnapshot{
		Type:            session.ResourceTypeStream,
		Name:            "logs",
		Groups:          []string{"production"},
		Columns:         []session.SchemaColumn{{Name: "service", Kind: session.SchemaColumnTag, Type: session.SchemaValueTypeString, Indexed: true}},
		SortableIndexes: []session.SortableIndex{{RuleName: "service_sort", Tags: []string{"service"}}},
	}
	resource := Resource{Type: session.ResourceTypeStream, Name: "logs", Groups: []string{"production"}}
	compiled, compileErr := Compile(QueryPlan{
		Resource:  resource,
		OrderBy:   &Order{IndexRule: "service_sort", Direction: OrderDescending},
		TimeRange: TimeRange{Start: "-30m"},
	}, schema)
	if compileErr != nil {
		t.Fatalf("Compile returned error: %v", compileErr)
	}
	if !strings.Contains(compiled.Query, "ORDER BY service_sort DESC") {
		t.Fatalf("expected index rule in query, got %s", compiled.Query)
	}
	_, tagOrderErr := Compile(QueryPlan{
		Resource:  resource,
		OrderBy:   &Order{IndexRule: "service", Direction: OrderDescending},
		TimeRange: TimeRange{Start: "-30m"},
	}, schema)
	if tagOrderErr == nil || DescribeError(tagOrderErr).Code != "ORDER_INDEX_NOT_SORTABLE" {
		t.Fatalf("expected tag name to be rejected as an index rule, got %v", tagOrderErr)
	}
}

func TestCompileRejectsIdentifierCaseChanges(t *testing.T) {
	schema := session.SchemaSnapshot{
		Type:            session.ResourceTypeStream,
		Name:            "logs",
		Groups:          []string{"production"},
		Columns:         []session.SchemaColumn{{Name: "service", Kind: session.SchemaColumnTag, Type: session.SchemaValueTypeString}},
		SortableIndexes: []session.SortableIndex{{RuleName: "service_sort", Tags: []string{"service"}}},
	}
	resource := Resource{Type: session.ResourceTypeStream, Name: "logs", Groups: []string{"production"}}
	_, projectionErr := Compile(QueryPlan{
		Resource:   resource,
		Projection: []Projection{{Column: "Service"}},
	}, schema)
	if projectionErr == nil || !strings.Contains(projectionErr.Error(), `column "Service"`) {
		t.Fatalf("expected case-sensitive column rejection, got %v", projectionErr)
	}
	_, orderErr := Compile(QueryPlan{
		Resource: resource,
		OrderBy:  &Order{IndexRule: "Service_sort", Direction: OrderDescending},
	}, schema)
	if orderErr == nil || DescribeError(orderErr).Code != "ORDER_INDEX_NOT_SORTABLE" {
		t.Fatalf("expected case-sensitive index rule rejection, got %v", orderErr)
	}
}

func TestCompileSupportsPropertyIDAndEmptyTraceProjection(t *testing.T) {
	propertySchema := session.SchemaSnapshot{
		Type:    session.ResourceTypeProperty,
		Name:    "metadata",
		Groups:  []string{"production"},
		Columns: []session.SchemaColumn{{Name: "region", Kind: session.SchemaColumnTag, Type: session.SchemaValueTypeString}},
	}
	propertyQuery, propertyErr := Compile(QueryPlan{
		Resource: Resource{Type: session.ResourceTypeProperty, Name: "metadata", Groups: []string{"production"}},
		Filter:   &Predicate{Column: "ID", Operator: OperatorEqual, Value: "server-1"},
	}, propertySchema)
	if propertyErr != nil || !strings.Contains(propertyQuery.Query, "WHERE ID = 'server-1'") {
		t.Fatalf("unexpected property ID query: query=%q err=%v", propertyQuery.Query, propertyErr)
	}
	traceSchema := session.SchemaSnapshot{
		Type:    session.ResourceTypeTrace,
		Name:    "traces",
		Groups:  []string{"production"},
		Columns: []session.SchemaColumn{{Name: "trace_id", Kind: session.SchemaColumnTag, Type: session.SchemaValueTypeString}},
	}
	traceQuery, traceErr := Compile(QueryPlan{
		Resource:       Resource{Type: session.ResourceTypeTrace, Name: "traces", Groups: []string{"production"}},
		ProjectionMode: ProjectionModeNone,
		TimeRange:      TimeRange{Start: "-30m"},
	}, traceSchema)
	if traceErr != nil || !strings.HasPrefix(traceQuery.Query, "SELECT () FROM TRACE") {
		t.Fatalf("unexpected empty trace projection: query=%q err=%v", traceQuery.Query, traceErr)
	}
}

func TestCompileValidatesTimeAndResultBounds(t *testing.T) {
	schema := session.SchemaSnapshot{
		Type:    session.ResourceTypeStream,
		Name:    "logs",
		Groups:  []string{"production"},
		Columns: []session.SchemaColumn{{Name: "service", Kind: session.SchemaColumnTag, Type: session.SchemaValueTypeString}},
	}
	resource := Resource{Type: session.ResourceTypeStream, Name: "logs", Groups: []string{"production"}}
	for testName, plan := range map[string]QueryPlan{
		"invalid time": {Resource: resource, TimeRange: TimeRange{Start: "yesterday"}},
		"large limit":  {Resource: resource, TimeRange: TimeRange{Start: "-30m"}, Limit: maximumLimit + 1},
	} {
		t.Run(testName, func(t *testing.T) {
			if _, compileErr := Compile(plan, schema); compileErr == nil {
				t.Fatal("expected invalid bounded plan to be rejected")
			}
		})
	}
}

func TestCompileTopNUsesRealAggregationCapabilities(t *testing.T) {
	schema := session.SchemaSnapshot{
		Type:           session.ResourceTypeTopN,
		Name:           "service_latency_topn",
		Groups:         []string{"production"},
		Loaded:         true,
		EntityTags:     []string{"service_id"},
		SourceMeasure:  "service_latency",
		FieldValueSort: "SORT_DESC",
		Columns:        []session.SchemaColumn{{Name: "service_id", Kind: session.SchemaColumnTag, Type: session.SchemaValueTypeString}},
	}
	resource := Resource{Type: session.ResourceTypeTopN, Name: "service_latency_topn", Groups: []string{"production"}}
	compiled, compileErr := Compile(QueryPlan{
		Resource:  resource,
		Filter:    &Predicate{Column: "service_id", Operator: OperatorEqual, Value: "checkout"},
		Aggregate: &Aggregate{Function: AggregateMean},
		OrderBy:   &Order{Direction: OrderDescending},
		TimeRange: TimeRange{Start: "-30m"},
		TopN:      10,
	}, schema)
	if compileErr != nil {
		t.Fatalf("Compile returned error: %v", compileErr)
	}
	wantFragment := "FROM MEASURE service_latency_topn IN production TIME > '-30m' WHERE service_id = 'checkout' AGGREGATE BY MEAN ORDER BY DESC"
	if !strings.Contains(compiled.Query, wantFragment) {
		t.Fatalf("unexpected TopN query: %s", compiled.Query)
	}
	_, directionErr := Compile(QueryPlan{
		Resource:  resource,
		OrderBy:   &Order{Direction: OrderAscending},
		TimeRange: TimeRange{Start: "-30m"},
		TopN:      10,
	}, schema)
	if directionErr == nil {
		t.Fatal("expected schema-restricted TopN direction to be rejected")
	}
	_, orErr := Compile(QueryPlan{
		Resource: resource,
		Filter: &Predicate{Operator: OperatorOr, Children: []Predicate{
			{Column: "service_id", Operator: OperatorEqual, Value: "checkout"},
			{Column: "service_id", Operator: OperatorEqual, Value: "payment"},
		}},
		TimeRange: TimeRange{Start: "-30m"},
		TopN:      10,
	}, schema)
	if orErr == nil {
		t.Fatal("expected OR in TopN filter to be rejected")
	}
	_, operatorErr := Compile(QueryPlan{
		Resource:  resource,
		Filter:    &Predicate{Column: "service_id", Operator: OperatorIn, Value: []any{"checkout", "payment"}},
		TimeRange: TimeRange{Start: "-30m"},
		TopN:      10,
	}, schema)
	if operatorErr == nil || DescribeError(operatorErr).Code != "TOPN_FILTER_OPERATOR_UNSUPPORTED" {
		t.Fatalf("expected non-equality TopN filter rejection, got %v", operatorErr)
	}
}
