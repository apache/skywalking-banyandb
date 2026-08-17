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

package planner

import (
	"testing"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
	corebydbql "github.com/apache/skywalking-banyandb/pkg/bydbql"
)

func TestE2EDerivedGoldenPlans(t *testing.T) {
	measure := goldenMeasureSchema()
	stream := goldenStreamSchema()
	trace := goldenTraceSchema()
	property := goldenPropertySchema()
	topN := session.SchemaSnapshot{Type: session.ResourceTypeTopN, Name: "service_latency_topn", Groups: []string{"production"}, Loaded: true}
	measureResource := Resource{Type: session.ResourceTypeMeasure, Name: "service_latency", Groups: []string{"production"}}
	streamResource := Resource{Type: session.ResourceTypeStream, Name: "logs", Groups: []string{"production"}}
	traceResource := Resource{Type: session.ResourceTypeTrace, Name: "traces", Groups: []string{"production"}}
	propertyResource := Resource{Type: session.ResourceTypeProperty, Name: "service_properties", Groups: []string{"production"}}
	topNResource := Resource{Type: session.ResourceTypeTopN, Name: "service_latency_topn", Groups: []string{"production"}}
	defaultRange := TimeRange{Start: "-30m"}

	goldens := []struct {
		name   string
		want   string
		schema session.SchemaSnapshot
		plan   QueryPlan
	}{
		{
			name:   "measure/all",
			plan:   QueryPlan{Resource: measureResource, TimeRange: defaultRange, Limit: 10},
			schema: measure,
			want:   "SELECT * FROM MEASURE service_latency IN production TIME > '-30m' LIMIT 10",
		},
		{
			name:   "measure/all_latency",
			plan:   QueryPlan{Resource: measureResource, Projection: []Projection{{Column: "latency"}}, TimeRange: defaultRange, Limit: 10},
			schema: measure,
			want:   "SELECT latency FROM MEASURE service_latency IN production TIME > '-30m' LIMIT 10",
		},
		{
			name:   "measure/tag_filter_int",
			plan:   QueryPlan{Resource: measureResource, Filter: equal("status", 500), TimeRange: defaultRange, Limit: 10},
			schema: measure,
			want:   "SELECT * FROM MEASURE service_latency IN production TIME > '-30m' WHERE status = 500 LIMIT 10",
		},
		{
			name:   "measure/tag_filter_ne",
			plan:   QueryPlan{Resource: measureResource, Filter: notEqual("status", 500), TimeRange: defaultRange, Limit: 10},
			schema: measure,
			want:   "SELECT * FROM MEASURE service_latency IN production TIME > '-30m' WHERE status != 500 LIMIT 10",
		},
		{
			name:   "measure/gen_leaf_gt_int",
			plan:   QueryPlan{Resource: measureResource, Filter: comparison("status", OperatorGreaterThan, 400), TimeRange: defaultRange, Limit: 10},
			schema: measure,
			want:   "SELECT * FROM MEASURE service_latency IN production TIME > '-30m' WHERE status > 400 LIMIT 10",
		},
		{
			name:   "measure/gen_leaf_ge_int",
			plan:   QueryPlan{Resource: measureResource, Filter: comparison("status", OperatorGreaterEqual, 400), TimeRange: defaultRange, Limit: 10},
			schema: measure,
			want:   "SELECT * FROM MEASURE service_latency IN production TIME > '-30m' WHERE status >= 400 LIMIT 10",
		},
		{
			name:   "measure/gen_leaf_lt_int",
			plan:   QueryPlan{Resource: measureResource, Filter: comparison("status", OperatorLessThan, 600), TimeRange: defaultRange, Limit: 10},
			schema: measure,
			want:   "SELECT * FROM MEASURE service_latency IN production TIME > '-30m' WHERE status < 600 LIMIT 10",
		},
		{
			name:   "measure/gen_leaf_le_int",
			plan:   QueryPlan{Resource: measureResource, Filter: comparison("status", OperatorLessEqual, 500), TimeRange: defaultRange, Limit: 10},
			schema: measure,
			want:   "SELECT * FROM MEASURE service_latency IN production TIME > '-30m' WHERE status <= 500 LIMIT 10",
		},
		{
			name:   "measure/tag_filter_not_in",
			plan:   QueryPlan{Resource: measureResource, Filter: in("status", OperatorNotIn, 404, 503), TimeRange: defaultRange, Limit: 10},
			schema: measure,
			want:   "SELECT * FROM MEASURE service_latency IN production TIME > '-30m' WHERE status NOT IN (404, 503) LIMIT 10",
		},
		{
			name:   "measure/entity_service",
			plan:   QueryPlan{Resource: measureResource, Filter: equal("service", "payment"), TimeRange: defaultRange, Limit: 10},
			schema: measure,
			want:   "SELECT * FROM MEASURE service_latency IN production TIME > '-30m' WHERE service = 'payment' LIMIT 10",
		},
		{
			name: "measure/complex_and_or",
			plan: QueryPlan{
				Resource: measureResource, Filter: and(equal("service", "payment"), equal("status", 500)), TimeRange: defaultRange, Limit: 10,
			},
			schema: measure,
			want:   "SELECT * FROM MEASURE service_latency IN production TIME > '-30m' WHERE (service = 'payment') AND (status = 500) LIMIT 10",
		},
		{
			name: "measure/linked_or",
			plan: QueryPlan{
				Resource: measureResource, Filter: or(equal("service", "payment"), equal("service", "checkout")), TimeRange: defaultRange, Limit: 10,
			},
			schema: measure,
			want:   "SELECT * FROM MEASURE service_latency IN production TIME > '-30m' WHERE (service = 'payment') OR (service = 'checkout') LIMIT 10",
		},
		{
			name: "measure/group_mean",
			plan: QueryPlan{
				Resource: measureResource, Projection: aggregateProjection(AggregateMean, "latency"), GroupBy: []string{"endpoint"}, TimeRange: defaultRange, Limit: 10,
			},
			schema: measure,
			want:   "SELECT endpoint, MEAN(latency) FROM MEASURE service_latency IN production TIME > '-30m' GROUP BY endpoint::TAG LIMIT 10",
		},
		{
			name: "measure/group_sum",
			plan: QueryPlan{
				Resource: measureResource, Projection: aggregateProjection(AggregateSum, "cpm"), GroupBy: []string{"endpoint"}, TimeRange: defaultRange, Limit: 10,
			},
			schema: measure,
			want:   "SELECT endpoint, SUM(cpm) FROM MEASURE service_latency IN production TIME > '-30m' GROUP BY endpoint::TAG LIMIT 10",
		},
		{
			name: "measure/group_count",
			plan: QueryPlan{
				Resource: measureResource, Projection: aggregateProjection(AggregateCount, "latency"), GroupBy: []string{"endpoint"}, TimeRange: defaultRange, Limit: 10,
			},
			schema: measure,
			want:   "SELECT endpoint, COUNT(latency) FROM MEASURE service_latency IN production TIME > '-30m' GROUP BY endpoint::TAG LIMIT 10",
		},
		{
			name: "measure/group_min",
			plan: QueryPlan{
				Resource: measureResource, Projection: aggregateProjection(AggregateMin, "latency"), GroupBy: []string{"endpoint"}, TimeRange: defaultRange, Limit: 10,
			},
			schema: measure,
			want:   "SELECT endpoint, MIN(latency) FROM MEASURE service_latency IN production TIME > '-30m' GROUP BY endpoint::TAG LIMIT 10",
		},
		{
			name: "measure/group_max",
			plan: QueryPlan{
				Resource: measureResource, Projection: aggregateProjection(AggregateMax, "latency"), GroupBy: []string{"endpoint"}, TimeRange: defaultRange, Limit: 10,
			},
			schema: measure,
			want:   "SELECT endpoint, MAX(latency) FROM MEASURE service_latency IN production TIME > '-30m' GROUP BY endpoint::TAG LIMIT 10",
		},
		{
			name: "measure/order_tag_asc",
			plan: QueryPlan{
				Resource: measureResource, OrderBy: &Order{IndexRule: "endpoint", Direction: OrderAscending}, TimeRange: defaultRange, Limit: 10,
			},
			schema: measure,
			want:   "SELECT * FROM MEASURE service_latency IN production TIME > '-30m' ORDER BY endpoint ASC LIMIT 10",
		},
		{
			name: "measure/order_tag_desc",
			plan: QueryPlan{
				Resource: measureResource, OrderBy: &Order{IndexRule: "endpoint", Direction: OrderDescending}, TimeRange: defaultRange, Limit: 10,
			},
			schema: measure,
			want:   "SELECT * FROM MEASURE service_latency IN production TIME > '-30m' ORDER BY endpoint DESC LIMIT 10",
		},
		{
			name:   "measure/all_max_limit",
			plan:   QueryPlan{Resource: measureResource, TimeRange: defaultRange, Limit: 100},
			schema: measure,
			want:   "SELECT * FROM MEASURE service_latency IN production TIME > '-30m' LIMIT 100",
		},
		{
			name: "measure/multi_group",
			plan: QueryPlan{
				Resource:  Resource{Type: session.ResourceTypeMeasure, Name: "service_latency", Groups: []string{"production", "staging"}},
				TimeRange: defaultRange, Limit: 10,
			},
			schema: goldenMultiGroupMeasureSchema(),
			want:   "SELECT * FROM MEASURE service_latency IN (production, staging) TIME > '-30m' LIMIT 10",
		},
		{
			name:   "measure/time_between",
			plan:   QueryPlan{Resource: measureResource, TimeRange: TimeRange{Start: "-2h", End: "-1h"}, Limit: 10},
			schema: measure,
			want:   "SELECT * FROM MEASURE service_latency IN production TIME BETWEEN '-2h' AND '-1h' LIMIT 10",
		},
		{
			name:   "stream/all",
			plan:   QueryPlan{Resource: streamResource, TimeRange: defaultRange, Limit: 10},
			schema: stream,
			want:   "SELECT * FROM STREAM logs IN production TIME > '-30m' LIMIT 10",
		},
		{
			name:   "stream/filter_tag",
			plan:   QueryPlan{Resource: streamResource, Filter: equal("service", "payment"), TimeRange: defaultRange, Limit: 10},
			schema: stream,
			want:   "SELECT * FROM STREAM logs IN production TIME > '-30m' WHERE service = 'payment' LIMIT 10",
		},
		{
			name:   "stream/less_eq",
			plan:   QueryPlan{Resource: streamResource, Filter: comparison("status", OperatorLessEqual, 500), TimeRange: defaultRange, Limit: 10},
			schema: stream,
			want:   "SELECT * FROM STREAM logs IN production TIME > '-30m' WHERE status <= 500 LIMIT 10",
		},
		{
			name: "stream/logical",
			plan: QueryPlan{
				Resource: streamResource, Filter: and(equal("service", "payment"), equal("status", 500)), TimeRange: defaultRange, Limit: 10,
			},
			schema: stream,
			want:   "SELECT * FROM STREAM logs IN production TIME > '-30m' WHERE (service = 'payment') AND (status = 500) LIMIT 10",
		},
		{
			name: "stream/order_desc",
			plan: QueryPlan{
				Resource:  streamResource,
				OrderBy:   &Order{IndexRule: "service", Direction: OrderDescending},
				TimeRange: defaultRange,
				Limit:     10,
			},
			schema: stream,
			want:   "SELECT * FROM STREAM logs IN production TIME > '-30m' ORDER BY service DESC LIMIT 10",
		},
		{
			// A TRACE scan needs an entry point, so the plainest trace plan still orders by TIME.
			name: "trace/all",
			plan: QueryPlan{
				Resource: traceResource, OrderBy: &Order{IndexRule: "TIME", Direction: OrderDescending},
				TimeRange: defaultRange, Limit: 10,
			},
			schema: trace,
			want:   "SELECT * FROM TRACE traces IN production TIME > '-30m' ORDER BY TIME DESC LIMIT 10",
		},
		{
			name:   "trace/eq_trace_id_without_order",
			plan:   QueryPlan{Resource: traceResource, Filter: equal("trace_id", "abc123"), TimeRange: defaultRange, Limit: 10},
			schema: trace,
			want:   "SELECT * FROM TRACE traces IN production TIME > '-30m' WHERE trace_id = 'abc123' LIMIT 10",
		},
		{
			name: "trace/eq_service_order_timestamp_desc",
			plan: QueryPlan{
				Resource: traceResource, Filter: equal("service_id", "payment"), OrderBy: &Order{IndexRule: "TIME", Direction: OrderDescending},
				TimeRange: defaultRange, Limit: 10,
			},
			schema: trace,
			want:   "SELECT * FROM TRACE traces IN production TIME > '-30m' WHERE service_id = 'payment' ORDER BY TIME DESC LIMIT 10",
		},
		{
			name: "trace/duration_range_order_timestamp",
			plan: QueryPlan{
				Resource: traceResource, Filter: and(comparison("duration", OperatorGreaterEqual, 100), comparison("duration", OperatorLessEqual, 500)),
				OrderBy: &Order{IndexRule: "TIME", Direction: OrderAscending}, TimeRange: defaultRange, Limit: 10,
			},
			schema: trace,
			want:   "SELECT * FROM TRACE traces IN production TIME > '-30m' WHERE (duration >= 100) AND (duration <= 500) ORDER BY TIME ASC LIMIT 10",
		},
		{
			name: "trace/gen_leaf_in_service_id",
			plan: QueryPlan{
				Resource: traceResource, Filter: in("service_id", OperatorIn, "payment", "checkout"),
				OrderBy: &Order{IndexRule: "TIME", Direction: OrderDescending}, TimeRange: defaultRange, Limit: 10,
			},
			schema: trace,
			want:   "SELECT * FROM TRACE traces IN production TIME > '-30m' WHERE service_id IN ('payment', 'checkout') ORDER BY TIME DESC LIMIT 10",
		},
		{
			name:   "property/all",
			plan:   QueryPlan{Resource: propertyResource, Limit: 10},
			schema: property,
			want:   "SELECT * FROM PROPERTY service_properties IN production LIMIT 10",
		},
		{
			name:   "property/query_by_criteria",
			plan:   QueryPlan{Resource: propertyResource, Filter: equal("in_service", "true"), Limit: 10},
			schema: property,
			want:   "SELECT * FROM PROPERTY service_properties IN production WHERE in_service = 'true' LIMIT 10",
		},
		{
			name:   "property/order_by_asc",
			plan:   QueryPlan{Resource: propertyResource, OrderBy: &Order{IndexRule: "priority", Direction: OrderAscending}, Limit: 10},
			schema: property,
			want:   "SELECT * FROM PROPERTY service_properties IN production ORDER BY priority ASC LIMIT 10",
		},
		{
			name:   "property/order_by_desc",
			plan:   QueryPlan{Resource: propertyResource, OrderBy: &Order{IndexRule: "priority", Direction: OrderDescending}, Limit: 10},
			schema: property,
			want:   "SELECT * FROM PROPERTY service_properties IN production ORDER BY priority DESC LIMIT 10",
		},
		{
			name:   "topn/topn_sum",
			plan:   QueryPlan{Resource: topNResource, Aggregate: &Aggregate{Function: AggregateSum}, TimeRange: defaultRange, TopN: 10},
			schema: topN,
			want:   "SHOW TOP 10 FROM MEASURE service_latency_topn IN production TIME > '-30m' AGGREGATE BY SUM ORDER BY DESC",
		},
		{
			name:   "topn/topn_mean",
			plan:   QueryPlan{Resource: topNResource, Aggregate: &Aggregate{Function: AggregateMean}, TimeRange: defaultRange, TopN: 10},
			schema: topN,
			want:   "SHOW TOP 10 FROM MEASURE service_latency_topn IN production TIME > '-30m' AGGREGATE BY MEAN ORDER BY DESC",
		},
		{
			name: "topn/topn_min",
			plan: QueryPlan{
				Resource: topNResource, Aggregate: &Aggregate{Function: AggregateMin}, OrderBy: &Order{Direction: OrderAscending},
				TimeRange: defaultRange, TopN: 5,
			},
			schema: topN,
			want:   "SHOW TOP 5 FROM MEASURE service_latency_topn IN production TIME > '-30m' AGGREGATE BY MIN ORDER BY ASC",
		},
		{
			name:   "topn/topn_max",
			plan:   QueryPlan{Resource: topNResource, Aggregate: &Aggregate{Function: AggregateMax}, TimeRange: defaultRange, TopN: 5},
			schema: topN,
			want:   "SHOW TOP 5 FROM MEASURE service_latency_topn IN production TIME > '-30m' AGGREGATE BY MAX ORDER BY DESC",
		},
		{
			name:   "topn/topn_count",
			plan:   QueryPlan{Resource: topNResource, Aggregate: &Aggregate{Function: AggregateCount}, TimeRange: defaultRange, TopN: 3},
			schema: topN,
			want:   "SHOW TOP 3 FROM MEASURE service_latency_topn IN production TIME > '-30m' AGGREGATE BY COUNT ORDER BY DESC",
		},
	}
	if len(goldens) != 41 {
		t.Fatalf("expected 41 e2e-derived golden cases, got %d", len(goldens))
	}
	for _, golden := range goldens {
		t.Run(golden.name, func(t *testing.T) {
			compiled, compileErr := Compile(golden.plan, golden.schema)
			if compileErr != nil {
				t.Fatalf("Compile returned error: %v", compileErr)
			}
			if compiled.Query != golden.want {
				t.Fatalf("unexpected query:\nwant: %s\n got: %s", golden.want, compiled.Query)
			}
			if _, parseErr := corebydbql.ParseQuery(compiled.Query); parseErr != nil {
				t.Fatalf("compiled query must parse: %v", parseErr)
			}
		})
	}
}

func goldenMeasureSchema() session.SchemaSnapshot {
	return session.SchemaSnapshot{
		Type:   session.ResourceTypeMeasure,
		Name:   "service_latency",
		Groups: []string{"production"},
		Loaded: true,
		Columns: []session.SchemaColumn{
			{Name: "endpoint", Kind: session.SchemaColumnTag, Type: session.SchemaValueTypeString, Indexed: true},
			{Name: "service", Kind: session.SchemaColumnTag, Type: session.SchemaValueTypeString},
			{Name: "status", Kind: session.SchemaColumnTag, Type: session.SchemaValueTypeInt},
			{Name: "latency", Kind: session.SchemaColumnField, Type: session.SchemaValueTypeFloat},
			{Name: "cpm", Kind: session.SchemaColumnField, Type: session.SchemaValueTypeInt},
		},
	}
}

func goldenStreamSchema() session.SchemaSnapshot {
	return session.SchemaSnapshot{
		Type:   session.ResourceTypeStream,
		Name:   "logs",
		Groups: []string{"production"},
		Loaded: true,
		Columns: []session.SchemaColumn{
			{Name: "service", Kind: session.SchemaColumnTag, Type: session.SchemaValueTypeString, Indexed: true},
			{Name: "status", Kind: session.SchemaColumnTag, Type: session.SchemaValueTypeInt},
		},
	}
}

func goldenMultiGroupMeasureSchema() session.SchemaSnapshot {
	schema := goldenMeasureSchema()
	schema.Groups = []string{"production", "staging"}
	return schema
}

func goldenTraceSchema() session.SchemaSnapshot {
	return session.SchemaSnapshot{
		Type:       session.ResourceTypeTrace,
		Name:       "traces",
		Groups:     []string{"production"},
		Loaded:     true,
		TraceIDTag: "trace_id",
		Columns: []session.SchemaColumn{
			{Name: "trace_id", Kind: session.SchemaColumnTag, Type: session.SchemaValueTypeString},
			{Name: "service_id", Kind: session.SchemaColumnTag, Type: session.SchemaValueTypeString},
			{Name: "duration", Kind: session.SchemaColumnTag, Type: session.SchemaValueTypeInt},
		},
	}
}

func goldenPropertySchema() session.SchemaSnapshot {
	return session.SchemaSnapshot{
		Type:   session.ResourceTypeProperty,
		Name:   "service_properties",
		Groups: []string{"production"},
		Loaded: true,
		Columns: []session.SchemaColumn{
			{Name: "in_service", Kind: session.SchemaColumnTag, Type: session.SchemaValueTypeString},
			{Name: "priority", Kind: session.SchemaColumnTag, Type: session.SchemaValueTypeInt, Indexed: true},
		},
	}
}

func aggregateProjection(function AggregateFunction, column string) []Projection {
	return []Projection{{Column: "endpoint"}, {Aggregate: &Aggregate{Function: function, Column: column}}}
}

func equal(column string, value any) *Predicate {
	return comparison(column, OperatorEqual, value)
}

func notEqual(column string, value any) *Predicate {
	return comparison(column, OperatorNotEqual, value)
}

func comparison(column string, operator Operator, value any) *Predicate {
	return &Predicate{Column: column, Operator: operator, Value: value}
}

func in(column string, operator Operator, values ...any) *Predicate {
	return &Predicate{Column: column, Operator: operator, Value: values}
}

func and(children ...*Predicate) *Predicate {
	return predicateSet(OperatorAnd, children...)
}

func or(children ...*Predicate) *Predicate {
	return predicateSet(OperatorOr, children...)
}

func predicateSet(operator Operator, children ...*Predicate) *Predicate {
	predicates := make([]Predicate, 0, len(children))
	for _, child := range children {
		predicates = append(predicates, *child)
	}
	return &Predicate{Operator: operator, Children: predicates}
}
