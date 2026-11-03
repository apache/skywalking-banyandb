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

func generateT17(b *suiteBuilder) ([]AggregateFixture, []SortingScenario, []Case, []SortingResource, error) {
	readiness, err := t17Readiness()
	if err != nil {
		return nil, nil, nil, nil, err
	}
	b.features["t17.fixture"] = true
	var scenarios []SortingScenario
	add := func(name string, q t17Query, bindings [][]any, windows [][2]int64, features ...string) {
		if b.err != nil {
			return
		}
		if q.Fixture == "" {
			q.Fixture = T16FixtureName
		}
		if windows == nil {
			windows = [][2]int64{{0, 6000}, {1000, 5000}, {200000, 201000}}
		}
		features = append(features, "catalog.B5", "t17.rebind")
		if q.Limit == nil {
			features = append(features, "t17.full_sort")
		} else {
			features = append(features, "t17.topk")
		}
		scenario := SortingScenario{Name: "t17_" + name}
		for binding, window := range windows {
			q.Begin, q.End = window[0], window[1]
			if bindings != nil {
				q.Params = bindings[binding]
			}
			c, buildErr := buildT17Case(fmt.Sprintf("%s_binding_%d", scenario.Name, binding), q, features...)
			if buildErr != nil {
				b.err = buildErr
				return
			}
			scenario.Cases = append(scenario.Cases, c)
		}
		scenarios = append(scenarios, scenario)
		for _, feature := range features {
			b.features[feature] = true
		}
	}
	plain := func(name string, q t17Query, features ...string) { add(name, q, nil, nil, features...) }
	f := t17Query{t16Query: t16Query{Columns: []t10Column{
		{Expr: t10Source("rid")}, {Expr: t10Source("host")}, {Expr: t10Source("zone")}, {Expr: t10Source("v")}, {Expr: t10Source("f")},
	}}, Keys: []t17Key{{Expr: t10Source("v"), Direction: "DESC", Nulls: "LAST"}, {Expr: t10Source("rid"), Direction: "ASC"}}}
	plain("sort_F", f, "t17.multikey", "t17.nulls_explicit")
	top := f
	top.Limit, top.Offset = t17Limit(2), 1
	plain("topk_F", top, "t17.multikey", "t17.pagination")
	asc := f
	asc.Keys = []t17Key{{Expr: t10Source("v")}, {Expr: t10Source("rid")}}
	plain("ascending_default", asc, "t17.nulls_default", "t17.directions")
	desc := f
	desc.Keys = []t17Key{{Expr: t10Source("v"), Direction: "DESC"}, {Expr: t10Source("rid")}}
	plain("descending_default", desc, "t17.nulls_default", "t17.directions")
	asc.Keys = []t17Key{{Expr: t10Source("v"), Direction: "ASC", Nulls: "FIRST"}, {Expr: t10Source("rid"), Direction: "DESC"}}
	plain("ascending_nulls_first", asc, "t17.nulls_explicit", "t17.directions")
	single := f
	single.Keys = []t17Key{{Expr: t10Source("rid"), Direction: "DESC"}}
	plain("single_key", single, "t17.single_key")
	ties := f
	ties.Keys = []t17Key{{Expr: t10Source("host")}, {Expr: t10Source("rid"), Direction: "DESC"}}
	plain("ties", ties, "t17.ties", "t17.multikey")
	aliases := f
	aliases.Columns = []t10Column{{Expr: t10Source("rid"), Alias: "v"}, {Expr: t10Source("v"), Alias: "source_value"}}
	aliases.Keys = []t17Key{{Expr: t17Spelling(t10Source("rid"), "v"), Direction: "DESC"}}
	plain("alias_precedence", aliases, "t17.alias_precedence")
	hidden := f
	hidden.Columns = []t10Column{{Expr: t10Source("rid")}}
	plain("hidden_sort", hidden, "t17.hidden_sort")
	expression := hidden
	expression.Keys = []t17Key{
		{Expr: t10Binary("*", t10Source("v"), t10Cast(t10Parameter(0), "INT")), Direction: "ASC", Nulls: "LAST"},
		{Expr: t10Source("rid")},
	}
	sameWindows := [][2]int64{{0, 6000}, {0, 6000}, {0, 6000}}
	add("expression_params", expression, [][]any{{int64(-1)}, {int64(1)}, {int64(2)}}, sameWindows, "t17.expressions", "t17.parameters")
	qualified := hidden
	qualified.Keys = []t17Key{
		{Expr: t17Spelling(t10Source("zone"), "default.zone::TAG"), Nulls: "FIRST"},
		{Expr: t17Spelling(t10Source("v"), "v::FIELD"), Direction: "DESC", Nulls: "LAST"},
		{Expr: t10Source("rid")},
	}
	plain("qualified_refs", qualified, "t17.qualified_refs", "t17.multikey")
	bare := hidden
	bare.Keys = []t17Key{{Expr: t17Spelling(t10Source("v"), "v"), Direction: "DESC", Nulls: "LAST"}, {Expr: t10Source("rid")}}
	plain("bare_refs", bare, "t17.bare_refs")
	total := t14Aggregate("SUM", "v")
	grouped := t17Query{t16Query: t16Query{
		Columns: []t10Column{{Expr: t10Source("host"), Alias: "node"}, t10Field("total", total)}, GroupBy: []*t10Expr{t10Source("host")},
	}, Keys: []t17Key{{Expr: t17Spelling(total, "total"), Direction: "DESC"}, {Expr: t17Spelling(t10Source("host"), "node")}}}
	plain("group_sort", grouped, "t17.grouped", "t17.aggregate_alias")
	hiddenGroup := grouped
	hiddenGroup.Columns = []t10Column{{Expr: t10Source("host"), Alias: "node"}}
	hiddenGroup.Having = t10Binary(">", t14Aggregate("COUNT", "v"), t10Literal(int64(0)))
	hiddenGroup.Keys = []t17Key{{Expr: total, Direction: "DESC"}, {Expr: t17Spelling(t10Source("host"), "node")}}
	plain("hidden_group_sort", hiddenGroup, "t17.grouped", "t17.hidden_aggregate", "t17.having")
	composed := grouped
	composed.Limit, composed.Offset = t17Limit(1), 1
	composed.Where = t10Binary(">", t10Source("v"), t10Cast(t10Parameter(0), "DOUBLE"))
	composed.Having = t10Binary(">", total, t10Literal(int64(3)))
	composed.Keys[1].Direction = "ASC"
	add("parent_filter_having_page", composed, [][]any{{int64(0)}, {int64(4)}, {int64(1)}}, sameWindows,
		"t17.parent_composition", "t17.grouped", "t17.having", "t17.pagination")
	band := t10Call("CASE", t10Binary(">=", t10Call("COALESCE", t10Source("v"), t10Literal(int64(0))), t10Cast(t10Parameter(0), "INT")),
		t10Literal("high"), t10Literal("low"))
	average := t14Aggregate("AVG", "f")
	caseAvg := t17Query{t16Query: t16Query{
		Columns: []t10Column{t10Field("band", band), t10Field("average", average)}, GroupBy: []*t10Expr{t16Ordinal(1)},
		Having: t10Binary(">", average, t10Literal(float64(0))),
	}, Keys: []t17Key{{Expr: t17Spelling(average, "average"), Direction: "DESC"}, {Expr: t17Spelling(band, "band")}}, Limit: t17Limit(2)}
	add("case_avg_ordinal", caseAvg, [][]any{{int64(4)}, {int64(2)}, {int64(8)}}, sameWindows,
		"t17.case_avg_composition", "t17.ordinal", "t17.grouped", "t17.having")
	value := t10Call("COALESCE", t10Source("v"), t10Literal(int64(0)))
	distinct := t17Query{
		t16Query: t16Query{Columns: []t10Column{{Expr: t10Source("zone"), Alias: "z"}, t10Field("value", value)}, Distinct: true},
		Keys:     []t17Key{{Expr: t17Spelling(value, "value"), Direction: "DESC"}, {Expr: t17Spelling(t10Source("zone"), "z"), Nulls: "LAST"}},
		Limit:    t17Limit(2), Offset: 1,
	}
	plain("distinct_multikey_topk", distinct, "t17.distinct", "t17.distinct_visible", "t17.multikey", "t17.pagination")
	bucket := t16Bucket(2000, "'2s'")
	bucketQuery := t17Query{t16Query: t16Query{
		Columns: []t10Column{t10Field("bucket", bucket), t10Field("total", total)},
		GroupBy: []*t10Expr{t16Ordinal(1)},
	}, Keys: []t17Key{{Expr: t17Spelling(bucket, "bucket"), Direction: "DESC"}}}
	plain("bucket_sort", bucketQuery, "t17.bucket", "t17.grouped")
	hiddenBucket := bucketQuery
	hiddenBucket.Columns = []t10Column{t10Field("total", total)}
	hiddenBucket.GroupBy = []*t10Expr{bucket}
	hiddenBucket.Keys = []t17Key{{Expr: bucket, Direction: "DESC"}}
	plain("hidden_bucket_sort", hiddenBucket, "t17.bucket", "t17.hidden_sort", "t17.grouped")
	bucketQuery.Distinct = true
	bucketQuery.Limit = t17Limit(2)
	plain("distinct_bucket_sort", bucketQuery, "t17.bucket", "t17.distinct", "t17.distinct_visible", "t17.grouped")
	add("empty", hidden, nil, [][2]int64{{6000, 0}, {7000, 1000}, {8000, 2000}}, "t17.no_scan")
	zero := hidden
	zero.Limit = t17Limit(0)
	plain("limit_zero", zero, "t17.no_scan")
	wide := f
	wide.Fixture = T17WideFixtureName
	wide.Columns = []t10Column{
		{Expr: t10Source("rid")},
		{Expr: t10Source("v")},
		{Expr: t10Source("f")},
		{Expr: t10Source("zone")},
	}
	wideWindows := [][2]int64{{0, 8000}, {1000, 7000}, {200000, 201000}}
	wide.Keys = []t17Key{{Expr: t10Source("v")}, {Expr: t10Source("rid")}}
	add("wide_exact_integer", wide, nil, wideWindows, "t17.exact_int64")
	wide.Keys = []t17Key{{Expr: t10Source("zone")}, {Expr: t10Source("rid")}}
	add("wide_string", wide, nil, wideWindows, "t17.byte_strings")
	wide.Keys = []t17Key{{Expr: t10Source("f")}, {Expr: t10Source("rid")}}
	add("wide_float", wide, nil, wideWindows, "t17.nan", "t17.nulls_default")
	wide.Columns = []t10Column{{Expr: t10Source("rid")}, {Expr: t10Source("host")}}
	wide.Keys = []t17Key{{Expr: t10Source("v"), Direction: "DESC", Nulls: "LAST"}, {Expr: t10Source("rid")}}
	wide.Limit = t17Limit(2)
	add("wide_payload_topk", wide, nil, wideWindows, "t17.wide_candidates")
	large := hidden
	large.Limit, large.Offset = t17Limit(^uint32(0)), ^uint32(0)
	plain("large_offset", large, "t17.checked_k", "t17.pagination")
	bulk := hidden
	bulk.Fixture = T16BulkFixtureName
	bulk.Limit, bulk.Offset = t17Limit(3), 2
	bulk.Keys = []t17Key{{Expr: t10Binary("*", t10Source("v"), t10Cast(t10Parameter(0), "INT")), Direction: "DESC"}, {Expr: t10Source("rid")}}
	bulk.Columns = []t10Column{{Expr: t10Source("rid")}, t10Field("payload", t10Cast(t10Parameter(1), "STRING"))}
	payload := strings.Repeat("owned-candidate/", 1024)
	add("bulk_topk_complete", bulk, [][]any{{int64(1), payload}, {int64(-1), payload + "changed"}, {int64(2), payload}},
		[][2]int64{{0, 200000}, {0, 200000}, {200000, 201000}},
		"t17.complete_input", "t17.expressions", "t17.parameters")

	hiddenFilter := t10Binary(">", t10Source("f"), t10Cast(t10Parameter(0), "DOUBLE"))
	thresholds := [][]any{{int64(1)}, {int64(3)}, {int64(9)}}
	hiddenWhere := t17Query{t16Query: t16Query{
		Columns: []t10Column{{Expr: t10Source("host"), Alias: "node"}, {Expr: t10Source("rid")}}, Where: hiddenFilter,
	}, Keys: []t17Key{{Expr: t17Spelling(t10Source("host"), "node"), Direction: "ASC"}, {Expr: t10Source("rid"), Direction: "ASC"}}}
	add("hidden_where_raw", hiddenWhere, thresholds, sameWindows, "t17.hidden_where.raw")
	computedNode := t10Cast(t10Source("time"), "TIMESTAMP")
	computedWhere := hiddenWhere
	computedWhere.Columns = []t10Column{t10Field("observed", computedNode), {Expr: t10Source("rid")}}
	computedWhere.Keys = []t17Key{
		{Expr: t17Spelling(computedNode, "observed::TAG"), Direction: "DESC", Nulls: "LAST"},
		{Expr: t10Source("rid"), Direction: "ASC"},
	}
	add("hidden_where_computed", computedWhere, thresholds, sameWindows, "t17.hidden_where.computed")
	groupedWhere := grouped
	groupedWhere.Where = hiddenFilter
	groupedWhere.Keys = []t17Key{
		{Expr: t17Spelling(total, "total"), Direction: "DESC", Nulls: "LAST"},
		{Expr: t17Spelling(t10Source("host"), "node"), Direction: "ASC"},
	}
	groupedWhere.Limit = t17Limit(1)
	add("hidden_where_grouped", groupedWhere, thresholds, sameWindows, "t17.hidden_where.grouped", "t17.grouped")
	distinctWhere := hiddenWhere
	distinctWhere.Columns = []t10Column{{Expr: t10Source("host"), Alias: "node"}}
	distinctWhere.Keys = hiddenWhere.Keys[:1]
	distinctWhere.Distinct, distinctWhere.Limit = true, t17Limit(1)
	add("hidden_where_distinct", distinctWhere, thresholds, sameWindows, "t17.hidden_where.distinct", "t17.distinct")
	distinctCount := countDistinctExpression("zone")
	countQuery := t17Query{t16Query: t16Query{
		Columns: []t10Column{{Expr: t10Source("host"), Alias: "node"}, t10Field("unique_zones", distinctCount), t10Field("total", t14Aggregate("SUM", "v"))},
		GroupBy: []*t10Expr{t10Source("host")},
	}, Keys: []t17Key{
		{Expr: t17Spelling(distinctCount, "unique_zones"), Direction: "DESC"},
		{Expr: t17Spelling(t10Source("host"), "node"), Direction: "ASC"},
	}, Limit: t17Limit(2)}
	plain("count_distinct_ordered", countQuery, "count_distinct.order", "count_distinct.multiple")
	countQuery.Having = t10Binary(">=", distinctCount, t10Parameter(0))
	add("count_distinct_having", countQuery, [][]any{{int64(1)}, {int64(2)}, {int64(3)}}, sameWindows, "count_distinct.having")
	bulkDistinct := countDistinctExpression("rid")
	page := t17Query{t16Query: t16Query{
		Fixture: T16BulkFixtureName,
		Columns: []t10Column{
			{Expr: t10Source("rid")},
			t10Field("unique_ids", bulkDistinct), t10Field("total", t14Aggregate("SUM", "v")),
		},
		GroupBy: []*t10Expr{t10Source("rid")},
	}, Keys: []t17Key{
		{Expr: t17Spelling(bulkDistinct, "unique_ids"), Direction: "DESC"}, {Expr: t10Source("rid"), Direction: "ASC"},
	}, Limit: t17Limit(7), Offset: 1100}
	add("count_distinct_complete_page", page, nil, [][2]int64{{0, 200000}, {1000, 200000}, {200000, 201000}}, "count_distinct.complete_page")
	rejections := generateT17Rejections(b)
	resources, resourceErr := generateT17Resources(b, bulk, payload)
	if resourceErr != nil {
		return nil, nil, nil, nil, resourceErr
	}
	return readiness, scenarios, rejections, resources, b.err
}

func t17Limit(value uint32) *uint32 { return &value }
func t17Spelling(expression *t10Expr, text string) *t10Expr {
	clone := *expression
	clone.Text = text
	return &clone
}

func generateT17Rejections(b *suiteBuilder) []Case {
	var cases []Case
	window := " WHERE time >= '" + timePlaceholder(0) + "' AND time < '" + timePlaceholder(6000) + "'"
	from := " FROM " + FixtureGroup + "." + T16FixtureName + window
	arrays := " FROM " + FixtureGroup + "." + FixtureName + window
	for _, rejection := range []struct{ name, query, feature string }{
		{"qualified_source", "SELECT rid" + from + " ORDER BY absent_sort_key::FIELD", "invalid_source"},
		{"array_labels", "SELECT rid" + arrays + " ORDER BY labels, rid", "invalid_type"},
		{"array_nums", "SELECT rid" + arrays + " ORDER BY nums, rid", "invalid_type"},
		{"ambiguous_alias", "SELECT host AS same, v::FIELD AS same" + from + " ORDER BY same", "ambiguous_alias"},
		{"group_source", "SELECT host, SUM(v::FIELD) AS total" + from + " GROUP BY host ORDER BY v::FIELD", "invalid_group_scope"},
		{"distinct_hidden", "SELECT DISTINCT host" + from + " ORDER BY v::FIELD", "distinct_hidden"},
		{"distinct_hidden_group", "SELECT DISTINCT SUM(v::FIELD) AS total" + from + " GROUP BY host ORDER BY host", "distinct_hidden"},
		{"distinct_hidden_aggregate", "SELECT DISTINCT host" + from + " GROUP BY host ORDER BY SUM(v::FIELD)", "distinct_hidden"},
		{"limit_negative", "SELECT rid" + from + " ORDER BY rid LIMIT -1", "invalid_pagination"},
		{"limit_range", "SELECT rid" + from + " ORDER BY rid LIMIT 4294967296", "invalid_pagination"},
		{"offset_range", "SELECT rid" + from + " ORDER BY rid LIMIT 1 OFFSET 4294967296", "invalid_pagination"},
		{"uint64_range", "SELECT rid" + from + " ORDER BY rid LIMIT 18446744073709551616", "invalid_pagination"},
	} {
		feature := "t17." + rejection.feature
		cases = append(cases, Case{
			Name: "t17_reject_" + rejection.name, Ticket: "T17", Query: rejection.query,
			WantCode: codes.InvalidArgument, NoScan: true, Features: []string{feature},
		})
		b.features[feature] = false
	}
	return cases
}

func requiredT17Scenarios() []string {
	return strings.Fields(`sort_F topk_F ascending_default descending_default ascending_nulls_first single_key ties
        alias_precedence hidden_sort expression_params qualified_refs bare_refs group_sort hidden_group_sort
        parent_filter_having_page case_avg_ordinal distinct_multikey_topk bucket_sort hidden_bucket_sort distinct_bucket_sort
        empty limit_zero wide_exact_integer wide_string wide_float wide_payload_topk large_offset bulk_topk_complete
        hidden_where_raw hidden_where_computed hidden_where_grouped hidden_where_distinct
        count_distinct_ordered count_distinct_having count_distinct_complete_page`)
}

func generateT17Resources(b *suiteBuilder, q t17Query, payload string) ([]SortingResource, error) {
	q.Begin, q.End, q.Params = 0, 200000, []any{int64(1), payload}
	var resources []SortingResource
	for _, fullSort := range []bool{false, true} {
		for _, cancel := range []bool{false, true} {
			mode, feature := "budget", "t17.resource_budget"
			code, budget := codes.ResourceExhausted, int64(16<<20)
			q.Limit, q.Offset = t17Limit(600), 0
			if fullSort {
				q.Limit, q.Offset = t17Limit(3), 2
			}
			if cancel {
				mode, feature, code, budget = "cancel", "t17.resource_cancel", codes.Canceled, 0
				q.Limit, q.Offset = t17Limit(3), 2
			}
			name := fmt.Sprintf("t17_%s_full_%t", mode, fullSort)
			c, err := buildT17Case(name, q, feature)
			if err != nil {
				return nil, err
			}
			c.WantCode, c.WantJSON = code, nil
			resource := SortingResource{Case: c, FullSort: fullSort, Cancel: cancel, BudgetBytes: budget}
			if !cancel {
				compact := q
				compact.Limit, compact.Offset = t17Limit(3), 2
				control, controlErr := buildT17Case(name+"_compact_control", compact, "t17.budget_success")
				if controlErr != nil {
					return nil, controlErr
				}
				resource.Control = &control
				b.features["t17.budget_success"] = true
			}
			resources = append(resources, resource)
			b.features[feature] = false
		}
	}
	return resources, nil
}
