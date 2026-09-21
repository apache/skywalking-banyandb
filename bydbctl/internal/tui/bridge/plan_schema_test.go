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
	"testing"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
)

func selectPlanProperties(t *testing.T) map[string]any {
	t.Helper()
	properties, ok := selectPlanSchema()["properties"].(map[string]any)
	if !ok {
		t.Fatal("selectPlanSchema has no properties map")
	}
	return properties
}

func aggregateFunctionEnum(t *testing.T, schema map[string]any) []string {
	t.Helper()
	properties, ok := schema["properties"].(map[string]any)
	if !ok {
		t.Fatal("aggregate schema has no properties map")
	}
	function, ok := properties["function"].(map[string]any)
	if !ok {
		t.Fatal("aggregate schema has no function property")
	}
	values, ok := function["enum"].([]string)
	if !ok {
		t.Fatalf("aggregate function enum is %T, want []string", function["enum"])
	}
	return values
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

// TestSelectPlanSchemaExposesTimeBucket pins that time_bucket is advertised on
// the SELECT plan. selectPlanSchema sets additionalProperties:false, so an
// omitted property is not merely undiscoverable -- a plan carrying it is
// invalid against the tool contract, which would leave QueryPlan.TimeBucket
// unreachable from the structured path no matter what Compile supports.
func TestSelectPlanSchemaExposesTimeBucket(t *testing.T) {
	properties := selectPlanProperties(t)
	timeBucket, ok := properties["time_bucket"].(map[string]any)
	if !ok {
		t.Fatal("selectPlanSchema does not advertise time_bucket")
	}
	bucketProperties, ok := timeBucket["properties"].(map[string]any)
	if !ok {
		t.Fatal("time_bucket schema has no properties map")
	}
	if _, hasWidth := bucketProperties["width"]; !hasWidth {
		t.Fatal("time_bucket schema does not advertise width")
	}
}

// TestAggregateSchemaOffersCountDistinctOnlyWithAColumn pins both halves of the
// rule: COUNT_DISTINCT always needs an explicit target, and compileTopN rejects
// it outright because the TOPN grammar has no DISTINCT production.
func TestAggregateSchemaOffersCountDistinctOnlyWithAColumn(t *testing.T) {
	withColumn := aggregateFunctionEnum(t, aggregateSchema(true))
	if !containsString(withColumn, "COUNT_DISTINCT") {
		t.Fatalf("column-bearing aggregate enum omits COUNT_DISTINCT: %v", withColumn)
	}
	withoutColumn := aggregateFunctionEnum(t, aggregateSchema(false))
	if containsString(withoutColumn, "COUNT_DISTINCT") {
		t.Fatalf("TOPN aggregate enum must not offer COUNT_DISTINCT: %v", withoutColumn)
	}
}

// TestPlanConstraintsListCountDistinctColumns pins that the advertised target
// list follows compileAggregate's own gate: numeric fields plus every tag that
// is not an array or a timestamp. numeric_fields alone would understate it.
func TestPlanConstraintsListCountDistinctColumns(t *testing.T) {
	snapshot := session.SchemaSnapshot{
		Type:   session.ResourceTypeMeasure,
		Name:   "service_latency",
		Groups: []string{"production"},
		Columns: []session.SchemaColumn{
			{Name: "service", Kind: session.SchemaColumnTag, Type: session.SchemaValueTypeString},
			{Name: "tags", Kind: session.SchemaColumnTag, Type: session.SchemaValueTypeStringArray},
			{Name: "seen_at", Kind: session.SchemaColumnTag, Type: session.SchemaValueTypeTimestamp},
			{Name: "latency", Kind: session.SchemaColumnField, Type: session.SchemaValueTypeFloat},
			{Name: "payload", Kind: session.SchemaColumnField, Type: session.SchemaValueTypeBinary},
		},
	}
	columns, ok := planConstraintsForSnapshot(snapshot)["count_distinct_columns"].([]string)
	if !ok {
		t.Fatal("constraints omit count_distinct_columns")
	}
	for _, allowed := range []string{"service", "latency"} {
		if !containsString(columns, allowed) {
			t.Fatalf("count_distinct_columns omits %q: %v", allowed, columns)
		}
	}
	for _, rejected := range []string{"tags", "seen_at", "payload"} {
		if containsString(columns, rejected) {
			t.Fatalf("count_distinct_columns must not offer %q: %v", rejected, columns)
		}
	}
}
