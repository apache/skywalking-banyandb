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

package qlgen

import (
	"strings"
	"testing"
	"time"
)

func TestBuildCaseIndependentOracle(t *testing.T) {
	query := Query{
		Columns: []string{"rid", "v::field"}, BeginMillis: -1, EndMillis: 8000,
		Where: &Predicate{Op: "=", Column: "host", Values: []any{"a"}}, Bind: true, OrderBy: "TIME", Direction: "DESC",
	}
	generated, err := BuildCase("bound-filter", query, "predicate:eq")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(generated.Query, "host = ?") || !strings.Contains(generated.Query, "#!params:") {
		t.Fatal(generated.Query)
	}
	text, err := Render(string(generated.WantJSON), time.Unix(1700000000, 0))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(text, `"value":"2"`) || strings.Contains(text, `"value":"3"`) {
		t.Fatal(text)
	}
	if !generated.Ordered {
		t.Fatal("TIME order must be checked")
	}
}

func TestBuildCaseRejectsUnknownModel(t *testing.T) {
	_, err := BuildCase("unsupported", Query{Columns: []string{"missing"}})
	if err == nil {
		t.Fatal("oracle must reject unknown columns rather than guess")
	}
	_, err = BuildCase("unsupported-op", Query{Columns: []string{"rid"}, Where: &Predicate{Column: "rid", Op: "LIKE", Values: []any{1}}})
	if err == nil {
		t.Fatal("oracle must reject unknown predicates")
	}
	_, err = BuildCase("null-filter", Query{Columns: []string{"rid"}, Where: &Predicate{Column: "zone", Op: "=", Values: []any{nil}}})
	if err == nil {
		t.Fatal("NULL predicates require an explicit contract")
	}
}

func TestBuildCaseRejectsOverflowingPagination(t *testing.T) {
	limit := ^uint32(0)
	_, err := BuildCase("overflow-page", Query{Columns: []string{"rid"}, OrderBy: "TIME", Limit: &limit, Offset: 1})
	if err == nil {
		t.Fatal("limit plus offset cannot overflow the uint32 execution bound")
	}
}

func TestRenderValidatesEveryPlaceholder(t *testing.T) {
	text, err := Render(`'{{time:-1}}' {{time:1000}}`, time.Unix(0, 0))
	if err != nil || text != "'1969-12-31T23:59:59.999Z' 1970-01-01T00:00:01Z" {
		t.Fatalf("%s %v", text, err)
	}
	if _, err = Render("{{time:no}}", time.Unix(0, 0)); err == nil {
		t.Fatal("invalid offset accepted")
	}
	if _, err = Render("{{unknown}}", time.Unix(0, 0)); err == nil {
		t.Fatal("unknown placeholder accepted")
	}
}

func TestBuildCaseTracePrecedesPagination(t *testing.T) {
	limit := uint32(2)
	generated, err := BuildCase("trace-page", Query{
		Columns: []string{"rid"}, BeginMillis: -1, EndMillis: 8000,
		OrderBy: "TIME", Direction: "ASC", Trace: true, Limit: &limit, Offset: 1, Bind: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(generated.Query, "ORDER BY TIME ASC WITH QUERY_TRACE LIMIT ? OFFSET ?") {
		t.Fatal(generated.Query)
	}
}

func TestBuildCaseTimeBucketUsesAlignedBase(t *testing.T) {
	generated, err := BuildCase("bucket", Query{
		Columns: []string{"host", "v::FIELD"}, Aggregate: "SUM", Target: "v", GroupBy: []string{"host"},
		BucketMillis: 2000, BeginMillis: -1, EndMillis: 8000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(generated.Query, "GROUP BY TIME_BUCKET('2000ms'), host") {
		t.Fatal(generated.Query)
	}
	if _, err = Render(generated.Query, time.Unix(1700000000, 0)); err != nil {
		t.Fatal(err)
	}
	if _, err = Render(generated.Query, time.Unix(1700000001, 0)); err == nil {
		t.Fatal("unaligned bucket base accepted")
	}
	if !strings.Contains(string(generated.WantJSON), `"timestamp":"{{time:6000}}"`) {
		t.Fatal(string(generated.WantJSON))
	}
}
