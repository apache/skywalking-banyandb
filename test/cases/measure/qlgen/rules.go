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
	"strings"

	"google.golang.org/grpc/codes"
)

func generateTimeAndPagination(b *suiteBuilder) {
	for index, op := range []string{"BETWEEN", "=", ">", ">=", "<", "<="} {
		for _, bound := range []bool{false, true} {
			query := baseQuery()
			query.TimeOp, query.Bind = op, bound
			if op != "BETWEEN" {
				query.BeginMillis = []int64{-1, 2000, 1500, 1500, 4500, 4500}[index]
			}
			b.add(fmt.Sprintf("generated_time_%d_bound_%t", index, bound), query, "time."+op)
		}
	}
	for _, direction := range []string{"ASC", "DESC"} {
		for _, limit := range []uint32{0, 1, 3, 100, 4294967295} {
			for _, offset := range []uint32{0, 1, 9} {
				if uint64(limit)+uint64(offset) > 4294967295 {
					continue
				}
				query := baseQuery()
				query.OrderBy, query.Direction, query.Limit, query.Offset = "TIME", direction, &limit, offset
				b.add(fmt.Sprintf("generated_page_%s_%d_%d", direction, limit, offset), query,
					"order."+direction, "pagination.limit", "pagination.offset")
			}
		}
	}
	query := baseQuery()
	query.Bind, query.Trace, query.OrderBy = true, true, "TIME"
	limit := uint32(3)
	query.Limit, query.Offset = &limit, 1
	b.add("generated_trace_bound_pagination", query, "trace.enabled", "binding.params")
	for _, value := range []string{"a", "b", "absent"} {
		query = baseQuery()
		query.Bind = true
		query.Where = &Predicate{Op: "=", Column: "host", Values: []any{value}}
		b.add("generated_rebind_"+value, query, "binding.rebind")
	}
	for _, direction := range []string{"ASC", "DESC"} {
		query = baseQuery()
		query.Target, query.Direction = "v", direction
		top := uint32(2)
		query.Top = &top
		query.Where = &Predicate{Op: "IN", Column: "rid", Values: []any{int64(1), int64(2), int64(3), int64(5)}}
		b.add("generated_top_"+direction, query, "projection.top")
	}
}

func generateAggregates(b *suiteBuilder) {
	for _, function := range []string{"SUM", "MIN", "MAX", "COUNT", "AVG", "MEAN"} {
		for _, target := range []string{"v", "f"} {
			query := baseQuery()
			query.Columns = []string{"host", target + "::FIELD"}
			query.Aggregate, query.Target, query.GroupBy = function, target, []string{"host"}
			if target == "f" && (function == "AVG" || function == "MEAN") {
				// The legacy native mean clamps values below one. That separate
				// semantic gap is reported, never copied into the scalar oracle.
				query.Where = &Predicate{Op: ">=", Column: "rid", Values: []any{int64(3)}}
			}
			b.add("generated_aggregate_"+function+"_"+target, query, "aggregate."+function, "group.tags")
		}
	}
	query := baseQuery()
	query.Columns, query.GroupBy = []string{"host", "rid"}, []string{"host", "rid"}
	query.Aggregate, query.Target = "COUNT_DISTINCT", "host"
	b.add("generated_count_distinct", query, "aggregate.COUNT_DISTINCT")
	query = baseQuery()
	query.Columns = []string{"host", "v::FIELD"}
	query.Aggregate, query.Target, query.GroupBy = "SUM", "v", []string{"host"}
	query.BucketMillis = 2000
	b.add("generated_time_bucket", query, "group.time_bucket")
}

func generateInteractions(b *suiteBuilder) PairCoverage {
	dimensions := interactionDimensions()
	vectors, coverage, err := Cover(dimensions, nil)
	if err != nil {
		b.err = err
		return coverage
	}
	for index, vector := range vectors {
		query := baseQuery()
		query.OrderBy, query.Direction, query.Bind = "TIME", vector["direction"], vector["binding"] == "bound"
		switch vector["projection"] {
		case "tags":
			query.Columns = []string{"rid", "host"}
		case "fields":
			query.Columns = []string{"v::FIELD", "f::FIELD"}
		default:
			query.Columns = []string{"*"}
		}
		switch vector["filter"] {
		case "eq":
			query.Where = &Predicate{Op: "=", Column: "host", Values: []any{"a"}}
		case "in":
			query.Where = &Predicate{Op: "IN", Column: "rid", Values: []any{int64(1), int64(3), int64(5)}}
		case "and", "or":
			query.Where = predicateTree(strings.ToUpper(vector["filter"]), 2)
		}
		switch vector["window"] {
		case "narrow":
			query.BeginMillis, query.EndMillis = 1500, 4500
		case "empty":
			query.BeginMillis, query.EndMillis = 9000, 10000
		}
		if vector["pagination"] != "all" {
			limit := uint32(3)
			query.Limit = &limit
			if vector["pagination"] == "offset" {
				query.Offset = 1
			}
		}
		b.add(fmt.Sprintf("generated_pair_%03d", index), query, "combinations.pairwise")
		if b.err == nil {
			b.cases[len(b.cases)-1].Dimensions = vector
		}
	}
	return coverage
}

func interactionDimensions() map[string][]string {
	return map[string][]string{
		"projection": {"tags", "fields", "all"},
		"filter":     {"none", "eq", "in", "and", "or"},
		"window":     {"all", "narrow", "empty"},
		"direction":  {"ASC", "DESC"},
		"binding":    {"literal", "bound"},
		"pagination": {"all", "limit", "offset"},
	}
}

func generateRejections(b *suiteBuilder) {
	if b.err != nil {
		return
	}
	base := b.cases[0].Query
	for index, mutation := range []struct{ old, replacement string }{
		{"SELECT", "SELEKT"}, {"FROM", "FORM"}, {"BETWEEN", "BETWEEEN"}, {"SELECT", "SELECT ,"},
	} {
		b.reject(fmt.Sprintf("generated_invalid_syntax_%d", index), strings.Replace(base, mutation.old, mutation.replacement, 1), "invalid.syntax", codes.InvalidArgument)
	}
	for _, column := range []string{"absent::FIELD", "absent::TAG"} {
		query := strings.Replace(base, "rid, host, zone, v::FIELD, f::FIELD", column, 1)
		b.reject("generated_invalid_projection_"+strings.Split(column, "::")[1], query, "invalid.projection", codes.Internal)
	}
	query := baseQuery()
	query.Bind = true
	bound, err := BuildCase("bound", query)
	if err != nil {
		b.err = err
		return
	}
	text := strings.Split(bound.Query, "\n#!params:")[0]
	b.reject("generated_missing_parameters", text, "invalid.parameters", codes.InvalidArgument)
	b.reject("generated_wrong_time_parameters", text+"\n#!params: [{\"int\":{\"value\":\"1\"}},{\"int\":{\"value\":\"2\"}}]", "invalid.parameters", codes.InvalidArgument)
	for _, clause := range []string{" LIMIT -1", " LIMIT 4294967296", " OFFSET -1", " OFFSET 4294967296"} {
		// The existing RPC maps transformer validation errors to Internal.
		b.reject(fmt.Sprintf("generated_invalid_range_%d", len(b.cases)), base+clause, "invalid.range", codes.Internal)
	}
	for _, function := range []string{"SUM", "MIN", "MAX", "AVG", "MEAN"} {
		queryText := strings.Replace(base, "SELECT ", "SELECT "+function+"(DISTINCT v), ", 1)
		b.reject("generated_invalid_distinct_"+function, queryText, "invalid.aggregate", codes.InvalidArgument)
	}
}

func (b *suiteBuilder) reject(name, query, feature string, code codes.Code) {
	b.features[feature] = false
	b.cases = append(b.cases, Case{Name: name, Query: query, WantCode: code, Features: []string{feature}})
}

func pendingFeatures() []Feature {
	return PendingCoverage()
}
