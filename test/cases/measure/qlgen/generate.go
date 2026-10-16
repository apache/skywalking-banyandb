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
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package qlgen

import (
	"fmt"
	"maps"
	"slices"
	"strings"
)

type suiteBuilder struct {
	err      error
	features map[string]bool
	cases    []Case
}

// Generate expands the finite measure-QL model into executable QL-only cases.
// Expected results come from fixture rows, never from native requests or captures.
func Generate() (Suite, error) {
	b := &suiteBuilder{features: make(map[string]bool)}
	b.add("generated_scan_all", baseQuery(), "source.explicit", "projection.mixed", "values.nulls")
	indexed := baseQuery()
	indexed.Where = &Predicate{Op: "IN", Column: "host", Values: []any{"a", "b", "c", "d"}}
	b.add("generated_scan_indexed", indexed, "filter.in", "values.string_list")
	generateProjections(b)
	generatePredicates(b)
	generateEmptyEntityPredicates(b)
	generateTimeAndPagination(b)
	generateAggregates(b)
	generateLegacyScalarCounts(b)
	pairs := generateInteractions(b)
	generateRejections(b)
	generateT03(b)
	aggregateReadiness, aggregateScenarios, aggregateErr := generateT07(b)
	if aggregateErr != nil {
		return Suite{}, aggregateErr
	}
	aggregateRejections := generateT07Rejections(b)
	if b.err != nil {
		return Suite{}, b.err
	}
	features := make([]Feature, 0, len(b.features))
	for _, id := range slices.Sorted(maps.Keys(b.features)) {
		status := "supported"
		if !b.features[id] {
			status = "rejection"
		}
		features = append(features, Feature{ID: id, Status: status})
	}
	features = append(features, pendingFeatures()...)
	reuseScenarios, reuseErr := generateT05()
	if reuseErr != nil {
		return Suite{}, reuseErr
	}
	reuseReadiness, reuseReadinessErr := t05ReadinessCase()
	if reuseReadinessErr != nil {
		return Suite{}, reuseReadinessErr
	}
	suite := Suite{
		AggregateReadiness: aggregateReadiness, AggregateScenarios: aggregateScenarios,
		AggregateRejections: aggregateRejections,
		ReuseScenarios:      reuseScenarios, ReuseReadiness: reuseReadiness, Version: 1, Seed: 1412,
		FixtureDigest: FixtureDigest(), Features: features, Cases: b.cases, Pairs: pairs,
	}
	return suite, suite.Validate()
}

func baseQuery() Query {
	return Query{
		Columns: []string{"rid", "host", "zone", "v::FIELD", "f::FIELD"},
		TimeOp:  "BETWEEN", BeginMillis: -1, EndMillis: 8000,
	}
}

func (b *suiteBuilder) add(name string, query Query, features ...string) {
	if b.err != nil {
		return
	}
	c, err := BuildCase(name, query, features...)
	if err != nil {
		b.err = fmt.Errorf("generate QL case %s: %w", name, err)
		return
	}
	for _, id := range features {
		b.features[id] = true
	}
	b.cases = append(b.cases, c)
}

func generateProjections(b *suiteBuilder) {
	for index, columns := range [][]string{{"*"}, {"rid", "host"}, {"v::FIELD", "f::FIELD"}, {"rid::TAG", "host::TAG", "v::FIELD"}} {
		query := baseQuery()
		query.Columns = columns
		feature := []string{"projection.all", "projection.tags", "projection.fields", "projection.typed"}[index]
		features := []string{feature}
		if index == 0 {
			features = append(features, "values.arrays")
		}
		b.add(fmt.Sprintf("generated_projection_%d", index), query, features...)
	}
	query := baseQuery()
	query.ParenthesizedGroups = true
	b.add("generated_parenthesized_source", query, "source.parenthesized")
}

func generatePredicates(b *suiteBuilder) {
	operators := []string{"=", "!=", "<", "<=", ">", ">="}
	names := []string{"eq", "ne", "lt", "le", "gt", "ge"}
	for index, op := range operators {
		for _, column := range []string{"host", "rid"} {
			var value any = "b"
			kind := "string"
			if column == "rid" {
				value, kind = int64(3), "int"
			}
			for _, bound := range []bool{false, true} {
				query := baseQuery()
				query.Bind = bound
				query.Where = &Predicate{Op: op, Column: column, Values: []any{value}}
				b.add(fmt.Sprintf("generated_%s_%s_bound_%t", names[index], kind, bound), query, "filter."+names[index], "values."+kind)
			}
		}
	}
	for _, op := range []string{"IN", "NOT IN"} {
		for _, column := range []string{"host", "rid"} {
			for size := 0; size <= 3; size++ {
				values := []any{"a", "b", "absent"}[:size]
				kind := "string_list"
				if column == "rid" {
					values, kind = []any{int64(1), int64(3), int64(99)}[:size], "int_list"
				}
				query := baseQuery()
				query.Where = &Predicate{Op: op, Column: column, Values: values}
				b.add(fmt.Sprintf("generated_%s_%s_%d", featureName(op), kind, size), query, "filter."+featureName(op), "values."+kind)
			}
		}
	}
	for _, op := range []string{"HAVING", "NOT HAVING"} {
		for _, column := range []string{"labels", "nums"} {
			values := []any{"red"}
			if column == "nums" {
				values = []any{int64(1)}
			}
			query := baseQuery()
			query.Where = &Predicate{Op: op, Column: column, Values: values}
			b.add("generated_"+featureName(op)+"_"+column, query, "filter."+featureName(op))
		}
	}
	query := baseQuery()
	query.Where = &Predicate{Op: "MATCH", Column: "host", Values: []any{"a"}}
	b.add("generated_match", query, "filter.match")
	for _, op := range []string{"AND", "OR"} {
		for depth := 1; depth <= 3; depth++ {
			query = baseQuery()
			query.Where = predicateTree(op, depth)
			b.add(fmt.Sprintf("generated_%s_depth_%d", strings.ToLower(op), depth), query, "filter."+strings.ToLower(op), "filter.nested")
		}
	}
}

func predicateTree(op string, depth int) *Predicate {
	left := &Predicate{Op: "=", Column: "host", Values: []any{"a"}}
	if depth == 0 {
		return left
	}
	left = &Predicate{Op: "<=", Column: "rid", Values: []any{int64(depth * 2)}}
	other := "AND"
	if op == other {
		other = "OR"
	}
	return &Predicate{Op: op, Children: []*Predicate{left, predicateTree(other, depth-1)}}
}

func featureName(value string) string { return strings.ToLower(strings.ReplaceAll(value, " ", "_")) }
