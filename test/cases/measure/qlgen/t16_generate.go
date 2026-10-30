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
	"encoding/json"
	"fmt"
	"strings"

	"google.golang.org/grpc/codes"
)

func generateT16(b *suiteBuilder) ([]AggregateFixture, []GroupingScenario, []Case, error) {
	readiness, err := t16Readiness()
	if err != nil {
		return nil, nil, nil, err
	}
	b.features["t16.fixture"] = true
	band := t10Call("CASE", t10Binary(">=", t10Source("v"), t10Literal(int64(4))), t10Literal("high"), t10Literal("low"))
	var scenarios []GroupingScenario
	add := func(name string, q t16Query, bindings [][]any, windows [][2]int64, branches uint64, features ...string) error {
		if q.Fixture == "" {
			q.Fixture = T16FixtureName
		}
		if windows == nil {
			windows = [][2]int64{{0, 6000}, {1000, 5000}, {200000, 201000}}
		}
		features = append(features, "t16.rebind", "catalog.B9")
		scenario := GroupingScenario{Name: "t16_" + name}
		scenario.Branches = branches
		for binding, window := range windows {
			q.Begin, q.End = window[0], window[1]
			if bindings != nil {
				q.Params = bindings[binding]
			}
			c, buildErr := buildT16Case(fmt.Sprintf("%s_binding_%d", scenario.Name, binding), q, features...)
			if buildErr != nil {
				return buildErr
			}
			scenario.Cases = append(scenario.Cases, c)
		}
		scenarios = append(scenarios, scenario)
		for _, feature := range features {
			b.features[feature] = true
		}
		return nil
	}
	plain := func(name string, q t16Query, features ...string) {
		if b.err == nil {
			b.err = add(name, q, nil, nil, 0, features...)
		}
	}
	grouped := func(key *t10Expr) t16Query {
		return t16Query{Columns: []t10Column{t10Field("key", key), t10Field("total", t14Aggregate("SUM", "v"))}, GroupBy: []*t10Expr{key}}
	}
	f := grouped(band)
	f.Columns[0].Alias = "band"
	plain("computed_group_F", f, "t16.computed")
	f.GroupBy = []*t10Expr{t16Ordinal(1)}
	plain("ordinal_equivalence", f, "t16.computed", "t16.ordinal")
	for _, kind := range []string{"arithmetic", "cast"} {
		key := t10Binary("%", t10Source("v"), t10Literal(int64(4)))
		if kind == "cast" {
			key = t10Cast(t10Source("zone"), "STRING")
		}
		q := grouped(key)
		plain(kind+"_explicit", q, "t16.computed", "t16."+kind)
		q.GroupBy = []*t10Expr{t16Ordinal(1)}
		plain(kind+"_ordinal", q, "t16.computed", "t16."+kind, "t16.ordinal")
	}
	residue := t10Binary("%", t10Source("v"), t10Literal(int64(4)))
	zone := t10Cast(t10Source("zone"), "STRING")
	multiple := t16Query{
		Columns: []t10Column{t10Field("band", band), t10Field("residue", residue), t10Field("zone", zone), t10Field("total", t14Aggregate("SUM", "v"))},
		GroupBy: []*t10Expr{band, residue, zone},
	}
	plain("multiple_computed", multiple, "t16.computed", "t16.multiple")
	multiple.GroupBy = []*t10Expr{t16Ordinal(1), t16Ordinal(2), t16Ordinal(3)}
	plain("multiple_ordinals", multiple, "t16.computed", "t16.multiple", "t16.ordinal")
	repeated := grouped(band)
	repeated.Columns = append([]t10Column{t10Field("band", band), t10Field("same", band)}, repeated.Columns[1])
	repeated.GroupBy = []*t10Expr{t16Ordinal(1), t16Ordinal(2), band, t16Ordinal(1)}
	plain("repeated_keys", repeated, "t16.computed", "t16.repeated")
	parenthesized := *band
	parenthesized.Text = "(" + band.sql() + ")"
	structural := grouped(band)
	structural.GroupBy = []*t10Expr{&parenthesized}
	plain("structural_parentheses", structural, "t16.computed", "t16.structural")
	plain("group_only_case", t16Query{Columns: []t10Column{t10Field("band", band)}, GroupBy: []*t10Expr{band}}, "t16.computed", "t16.group_only")
	plain("null_key_cast", grouped(zone), "t16.computed", "t16.null_keys")
	plain("constant_null", grouped(t10Cast(t10Literal(nil), "STRING")), "t16.computed", "t16.null_keys")
	parameterCase := grouped(t10Call("CASE", t10Binary(">=", t10Source("v"), t10Parameter(0)), t10Literal("high"), t10Literal("low")))
	parameterCase.GroupBy = []*t10Expr{t16Ordinal(1)}
	parameterWindows := [][2]int64{{0, 6000}, {0, 6000}, {0, 6000}}
	if err = add("param_case", parameterCase, [][]any{{int64(4)}, {int64(2)}, {int64(8)}}, parameterWindows, 0,
		"t16.computed", "t16.ordinal"); err != nil {
		return nil, nil, nil, err
	}
	parameterMath := grouped(t10Binary("%", t10Source("v"), t10Parameter(0)))
	parameterMath.GroupBy = []*t10Expr{t16Ordinal(1)}
	if err = add("param_arithmetic", parameterMath, [][]any{{int64(4)}, {int64(3)}, {int64(2)}}, parameterWindows, 0,
		"t16.computed", "t16.ordinal"); err != nil {
		return nil, nil, nil, err
	}
	independentParams := t16Query{
		Columns: []t10Column{
			t10Field("a", t10Binary("%", t10Source("v"), t10Parameter(0))),
			t10Field("b", t10Binary("%", t10Source("v"), t10Parameter(1))), t10Field("total", t14Aggregate("SUM", "v")),
		},
		GroupBy: []*t10Expr{t16Ordinal(1), t16Ordinal(2)},
	}
	if err = add("independent_parameter_keys", independentParams, [][]any{{int64(2), int64(3)}, {int64(3), int64(2)}, {int64(2), int64(2)}},
		parameterWindows, 0, "t16.parameter_identity", "t16.multiple"); err != nil {
		return nil, nil, nil, err
	}
	parameterMath.GroupBy = []*t10Expr{t16Ordinal(1), t16Ordinal(1)}
	if err = add("repeated_parameter_key", parameterMath, [][]any{{int64(2)}, {int64(3)}, {int64(4)}}, parameterWindows, 0,
		"t16.parameter_identity", "t16.repeated"); err != nil {
		return nil, nil, nil, err
	}
	bareSource := t10Source("v")
	bareSource.Text = "v"
	equivalent := t10Binary("%", bareSource, t10Literal(int64(2)))
	equivalent.Text = "(" + equivalent.sql() + ")"
	spellings := grouped(t10Binary("%", t10Source("v"), t10Literal(int64(2))))
	spellings.GroupBy = []*t10Expr{spellings.Columns[0].Expr, equivalent}
	if err = add("equivalent_key_spellings", spellings, nil, parameterWindows, 0, "t16.structural", "t16.repeated"); err != nil {
		return nil, nil, nil, err
	}
	composition := t16Query{
		Columns: []t10Column{t10Field("n", t14Aggregate("COUNT", "v"))}, GroupBy: []*t10Expr{band}, Distinct: true,
		Where: t10Binary(">=", t10Source("v"), t10Parameter(0)), Having: t10Binary(">", t14Aggregate("SUM", "v"), t10Literal(int64(0))),
	}
	if err = add("where_having_distinct", composition, [][]any{{int64(0)}, {int64(2)}, {int64(4)}}, nil, 0,
		"t16.computed", "t16.where", "t16.having", "t16.distinct"); err != nil {
		return nil, nil, nil, err
	}
	plain("hidden_keys", t16Query{Columns: []t10Column{t10Field("total", t14Aggregate("SUM", "v"))}, GroupBy: []*t10Expr{band}},
		"t16.computed", "t16.hidden_keys")
	star := t16Query{
		Fixture: T16StarFixtureName, Columns: []t10Column{{Expr: &t10Expr{Op: "star", Text: "*"}}, t10Field("n", t14Aggregate("COUNT", "v"))},
		GroupBy: []*t10Expr{t16Ordinal(1), t16Ordinal(2)},
	}
	plain("star_ordinals", star, "t16.star_expansion", "t16.ordinal")
	star.Columns = []t10Column{{Expr: &t10Expr{Op: "star", Text: "*"}}, t10Field("label", t10Cast(t10Source("v"), "STRING"))}
	star.GroupBy = []*t10Expr{t16Ordinal(1), t16Ordinal(2), t16Ordinal(3)}
	plain("star_following_expression", star, "t16.star_expansion", "t16.ordinal", "t16.computed")
	bulk := t16Query{
		Fixture: T16BulkFixtureName,
		Columns: []t10Column{
			t10Field("mod", t10Cast(t10Binary("%", t10Source("v"), t10Literal(int64(7))), "STRING")),
			t10Field("band", band), t10Field("total", t14Aggregate("SUM", "v")),
		},
		GroupBy: []*t10Expr{t16Ordinal(1), t16Ordinal(2), t16Ordinal(1)},
		Where:   t10Binary(">", t10Cast(t10Source("rid"), "INT"), t10Parameter(0)),
	}
	if err = add("bulk_selected_multi_key", bulk, [][]any{{int64(2)}, {int64(4)}, {int64(1)}},
		[][2]int64{{0, 200000}, {0, 1000}, {200000, 201000}}, 0, "t16.computed", "t16.multiple", "t16.repeated", "t16.complete_input", "t16.where"); err != nil {
		return nil, nil, nil, err
	}
	bucket := t16Bucket(2000, "'2s'")
	bucketF := grouped(bucket)
	bucketF.Columns[0].Alias = "bucket"
	plain("bucket_sum_F", bucketF, "t16.bucket_projection")
	bucketF.GroupBy = []*t10Expr{t16Ordinal(1)}
	plain("bucket_ordinal", bucketF, "t16.bucket_projection", "t16.ordinal")
	defaultBucket := grouped(t16Bucket(1000, ""))
	defaultBucket.Columns[0].Alias = "bucket"
	plain("bucket_default", defaultBucket, "t16.bucket_projection", "t16.bucket_default")
	if err = add("bucket_unaligned_range", bucketF, nil, [][2]int64{{1000, 5000}, {3000, 6000}, {200000, 201000}}, 0,
		"t16.bucket_projection", "t16.bucket_alignment"); err != nil {
		return nil, nil, nil, err
	}
	plain("bucket_group_only", t16Query{Columns: []t10Column{t10Field("bucket", bucket)}, GroupBy: []*t10Expr{t16Ordinal(1)}},
		"t16.bucket_projection", "t16.group_only")
	plain("bucket_computed", t16Query{
		Columns: []t10Column{t10Field("bucket", bucket), t10Field("band", band), t10Field("total", t14Aggregate("SUM", "v"))},
		GroupBy: []*t10Expr{t16Ordinal(1), t16Ordinal(2)},
	}, "t16.bucket_projection", "t16.computed", "t16.multiple")
	plain("bucket_raw_time", t16Query{
		Columns: []t10Column{t10Field("original", t10Source("time")), t10Field("bucket", bucket), t10Field("total", t14Aggregate("SUM", "v"))},
		GroupBy: []*t10Expr{t10Source("time"), bucket},
	}, "t16.bucket_projection", "t16.bucket_raw_time")
	if reviewErr := generateT16BucketReview(add); reviewErr != nil {
		return nil, nil, nil, reviewErr
	}
	native := t16Query{
		Fixture: T16BulkFixtureName, Hosts: []string{"bulk"}, GroupBy: []*t10Expr{t10Source("host"), t16Ordinal(1)},
		Columns: []t10Column{
			t10Field("bucket", bucket),
			{Expr: t10Source("host")},
			t10Field("low", t14Aggregate("MIN", "v")),
			t10Field("high", t14Aggregate("MAX", "v")), t10Field("n", t14Aggregate("COUNT", "v")), t10Field("avg", t14Aggregate("AVG", "f")),
		},
	}
	if err = add("bucket_native_merge", native, nil, nil, 5, "t16.bucket_projection", "t16.bucket_native", "t16.bucket_dedup"); err != nil {
		return nil, nil, nil, err
	}
	rejections, rejectErr := generateT16Rejections(b)
	if rejectErr != nil {
		return nil, nil, nil, rejectErr
	}
	return readiness, scenarios, rejections, b.err
}

func generateT16Rejections(b *suiteBuilder) ([]Case, error) {
	var rejections []Case
	for _, rejection := range []struct{ name, selectSQL, groupSQL, fixture, feature string }{
		{"ordinal_zero", "host, SUM(v::FIELD) AS total", "0", T16FixtureName, "t16.invalid_ordinal"},
		{"ordinal_negative", "host, SUM(v::FIELD) AS total", "-1", T16FixtureName, "t16.invalid_ordinal"},
		{"ordinal_range", "host, SUM(v::FIELD) AS total", "99", T16FixtureName, "t16.invalid_ordinal"},
		{"ordinal_aggregate", "host, SUM(v::FIELD) AS total", "2", T16FixtureName, "t16.invalid_ordinal"},
		{"ordinal_aggregate_expression", "host, (SUM(v::FIELD) + 1) AS total", "2", T16FixtureName, "t16.invalid_ordinal"},
		{"ordinal_star_aggregate", "*, COUNT(v::FIELD) AS n", "1, 2, 3", T16StarFixtureName, "t16.invalid_ordinal"},
		{"structural_mismatch", "(v::FIELD + 1) AS key, SUM(v::FIELD) AS total", "(v::FIELD + 2)", T16FixtureName, "t16.invalid_group"},
		{"group_aggregate", "host, SUM(v::FIELD) AS total", "SUM(v::FIELD)", T16FixtureName, "t16.invalid_group"},
		{"bucket_zero", "TIME_BUCKET('0s') AS bucket, SUM(v::FIELD) AS total", "1", T16FixtureName, "t16.invalid_bucket"},
		{"bucket_negative", "TIME_BUCKET('-2s') AS bucket, SUM(v::FIELD) AS total", "1", T16FixtureName, "t16.invalid_bucket"},
		{"bucket_invalid", "TIME_BUCKET('calendar') AS bucket, SUM(v::FIELD) AS total", "1", T16FixtureName, "t16.invalid_bucket"},
		{"bucket_ungrouped", "TIME_BUCKET('2s') AS bucket, SUM(v::FIELD) AS total", "host", T16FixtureName, "t16.invalid_bucket"},
		{"bucket_mismatch", "TIME_BUCKET('2s') AS bucket, SUM(v::FIELD) AS total", "TIME_BUCKET('1s')", T16FixtureName, "t16.invalid_bucket"},
	} {
		query := "SELECT " + rejection.selectSQL + " FROM " + FixtureGroup + "." + rejection.fixture +
			" WHERE time >= ? AND time < ? GROUP BY " + rejection.groupSQL
		params := []any{jsonValue(timePlaceholder(0)), jsonValue(timePlaceholder(6000))}
		encoded, err := json.Marshal(params)
		if err != nil {
			return nil, err
		}
		rejections = append(rejections, Case{
			Name: "t16_reject_" + rejection.name, Ticket: "T16", Query: query + "\n#!params: " + string(encoded),
			WantCode: codes.InvalidArgument, NoScan: true, Features: []string{rejection.feature},
		})
		b.features[rejection.feature] = false
	}
	query, err := renderT16(t16Query{
		Fixture: T16FixtureName, Begin: 0, End: 6000, Params: []any{"selected", "grouped"},
		Columns: []t10Column{t10Field("key", t10Cast(t10Parameter(0), "STRING")), t10Field("total", t14Aggregate("SUM", "v"))},
		GroupBy: []*t10Expr{t10Cast(t10Parameter(1), "STRING")},
	})
	if err != nil {
		return nil, err
	}
	rejections = append(rejections, Case{
		Name: "t16_reject_parameter_identity", Ticket: "T16", Query: query,
		WantCode: codes.InvalidArgument, NoScan: true, Features: []string{"t16.invalid_group"},
	})
	more, moreErr := generateT16BucketRejections(b)
	return append(rejections, more...), moreErr
}

func validateGroupingScenarios(s Suite) error {
	if len(s.GroupingReadiness) == 0 || len(s.GroupingScenarios) == 0 {
		return fmt.Errorf("T16 grouping coverage missing")
	}
	required := map[string]bool{}
	for _, name := range []string{
		"computed_group_F", "ordinal_equivalence", "arithmetic_explicit", "arithmetic_ordinal", "cast_explicit", "cast_ordinal",
		"multiple_computed", "multiple_ordinals", "repeated_keys", "structural_parentheses", "group_only_case", "null_key_cast", "constant_null",
		"param_case", "param_arithmetic", "where_having_distinct", "hidden_keys", "star_ordinals", "star_following_expression", "bulk_selected_multi_key",
		"independent_parameter_keys", "repeated_parameter_key", "equivalent_key_spellings",
		"bucket_sum_F", "bucket_ordinal", "bucket_default", "bucket_unaligned_range", "bucket_group_only", "bucket_computed", "bucket_native_merge",
		"bucket_raw_time",
		"bucket_distinct_visible", "bucket_distinct_having", "bucket_hidden_distinct", "bucket_hidden_distinct_having",
		"bucket_default_match", "bucket_duration_match", "bucket_equivalent_keys", "bucket_alias_containers",
		"raw_time_bucket_having", "mixed_time_bucket_having", "hidden_raw_time_bucket_having",
	} {
		required["t16_"+name] = true
	}
	for _, scenario := range s.GroupingScenarios {
		if !required[scenario.Name] {
			return fmt.Errorf("unexpected T16 grouping scenario %s", scenario.Name)
		}
		delete(required, scenario.Name)
		branches := uint64(0)
		if scenario.Name == "t16_bucket_native_merge" {
			branches = 5
		}
		if scenario.Branches != branches {
			return fmt.Errorf("T16 native bucket branch evidence changed")
		}
		if len(scenario.Cases) != 3 {
			return fmt.Errorf("T16 grouping bindings missing")
		}
		query := strings.Split(scenario.Cases[0].Query, "\n#!params:")[0]
		for _, c := range scenario.Cases {
			if strings.Split(c.Query, "\n#!params:")[0] != query || c.Grouping == nil ||
				c.Grouping.KeyEvaluations != c.Grouping.KeyExpressions*c.Grouping.InputRows || c.Grouping.InputRows != c.AggregateInputs {
				return fmt.Errorf("T16 grouping statement or key evidence changed")
			}
		}
	}
	if len(required) > 0 {
		return fmt.Errorf("T16 bounded grouping witnesses missing: %v", required)
	}
	if len(s.GroupingReadiness) != 3 || len(s.GroupingRejections) != 23 {
		return fmt.Errorf("T16 fixture or rejection witnesses missing")
	}
	for ordinal, name := range []string{T16FixtureName, T16StarFixtureName, T16BulkFixtureName} {
		fixture := s.GroupingReadiness[ordinal]
		if fixture.Name != name || len(fixture.Cases) != (len(t16Rows(name))+127)/128 {
			return fmt.Errorf("T16 fixture %s lacks complete QL visibility witnesses", name)
		}
	}
	for _, c := range s.GroupingRejections {
		if !c.NoScan || c.WantCode != codes.InvalidArgument {
			return fmt.Errorf("T16 invalid grouping must reject before scanning")
		}
	}
	return nil
}
