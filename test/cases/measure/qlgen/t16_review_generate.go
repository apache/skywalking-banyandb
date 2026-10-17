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

import "google.golang.org/grpc/codes"

func generateT16BucketReview(add t16ScenarioAdder) error {
	bucket := t16Bucket(2000, "'2s'")
	columns := []t10Column{t10Field("bucket", bucket), t10Field("total", t14Aggregate("SUM", "v"))}
	matchedHaving := t10Binary(">=", t10Cast(t16Bucket(2000, "'2000ms'"), "TIMESTAMP"), t16Timestamp(2000))
	countHost := &t10Expr{Op: "aggregate", Value: t07Column{Function: "COUNT", Target: "host"}, Text: "COUNT(host::TAG)"}
	for _, scenario := range []struct {
		name     string
		query    *t16Query
		features []string
	}{
		{"bucket_distinct_visible", &t16Query{
			Columns: columns, GroupBy: []*t10Expr{t16Ordinal(1)}, Distinct: true,
		}, []string{"t16.bucket_projection", "t16.bucket_distinct_visible"}},
		{"bucket_distinct_having", &t16Query{
			Columns: columns, GroupBy: []*t10Expr{t16Ordinal(1)}, Distinct: true, Having: matchedHaving,
		}, []string{"t16.bucket_projection", "t16.bucket_distinct_visible", "t16.bucket_having"}},
		{"bucket_hidden_distinct", &t16Query{
			Columns: []t10Column{t10Field("n", countHost)}, GroupBy: []*t10Expr{bucket}, Distinct: true,
		}, []string{"t16.bucket_distinct_hidden"}},
		{"bucket_hidden_distinct_having", &t16Query{
			Columns: []t10Column{t10Field("n", countHost)}, GroupBy: []*t10Expr{bucket}, Distinct: true,
			Having: t10Binary(">", countHost, t10Literal(int64(0))),
		}, []string{"t16.bucket_distinct_hidden", "t16.bucket_having"}},
		{"bucket_default_match", &t16Query{
			Fixture: T16StarFixtureName,
			Columns: []t10Column{t10Field("bucket", t16Bucket(0, "")), t10Field("total", t14Aggregate("SUM", "v"))},
			GroupBy: []*t10Expr{bucket}, Having: matchedHaving,
		}, []string{"t16.bucket_projection", "t16.bucket_width_equivalence", "t16.bucket_having"}},
		{"bucket_duration_match", &t16Query{
			Fixture: T16StarFixtureName,
			Columns: []t10Column{t10Field("bucket", t16Bucket(2000, "'2000ms'")), t10Field("total", t14Aggregate("SUM", "v"))},
			GroupBy: []*t10Expr{t16Bucket(0, "")}, Having: t10Binary(">=", t10Cast(bucket, "TIMESTAMP"), t16Timestamp(2000)),
		}, []string{"t16.bucket_projection", "t16.bucket_width_equivalence", "t16.bucket_having"}},
		{"bucket_equivalent_keys", &t16Query{
			Fixture: T16StarFixtureName,
			Columns: []t10Column{
				t10Field("a", t16Bucket(0, "")), t10Field("b", bucket),
				t10Field("c", t16Bucket(2000, "'2000ms'")), t10Field("total", t14Aggregate("SUM", "v")),
			},
			GroupBy: []*t10Expr{t16Ordinal(1), t16Ordinal(2), t16Ordinal(3)},
		}, []string{"t16.bucket_projection", "t16.bucket_width_equivalence", "t16.repeated"}},
		{"bucket_alias_containers", &t16Query{
			Columns: []t10Column{t10Field("total", bucket), t10Field("total", t14Aggregate("SUM", "v"))},
			GroupBy: []*t10Expr{t16Ordinal(1)}, Having: t10Binary(">", t14Aggregate("SUM", "v"), t10Literal(int64(0))),
		}, []string{"t16.bucket_projection", "t16.output_containers", "t16.bucket_having"}},
		{"raw_time_bucket_having", &t16Query{
			Columns: []t10Column{t10Field("original", t10Source("time")), t10Field("total", t14Aggregate("SUM", "v"))},
			GroupBy: []*t10Expr{t10Source("time")}, Having: t10Binary(">=", bucket, t16Timestamp(2000)),
		}, []string{"t16.bucket_having"}},
		{"mixed_time_bucket_having", &t16Query{
			Columns: []t10Column{t10Field("original", t10Source("time")), t10Field("bucket", bucket), t10Field("total", t14Aggregate("SUM", "v"))},
			GroupBy: []*t10Expr{t10Source("time"), bucket}, Having: t10Binary(">=", t16Bucket(3000, "'3s'"), t16Timestamp(3000)),
		}, []string{"t16.bucket_projection", "t16.bucket_having"}},
		{"hidden_raw_time_bucket_having", &t16Query{
			Columns: []t10Column{t10Field("total", t14Aggregate("SUM", "v"))},
			GroupBy: []*t10Expr{t10Source("time")}, Having: t10Binary(">=", t16Bucket(3000, "'3s'"), t16Timestamp(3000)),
		}, []string{"t16.hidden_keys", "t16.bucket_having"}},
	} {
		if err := add(scenario.name, *scenario.query, nil, nil, 0, scenario.features...); err != nil {
			return err
		}
	}
	return nil
}

func generateT16BucketRejections(b *suiteBuilder) ([]Case, error) {
	bucket := t16Bucket(2000, "'2s'")
	columns := []t10Column{t10Field("bucket", bucket), t10Field("total", t14Aggregate("SUM", "v"))}
	wrongBucket := t10Binary("=", t16Bucket(3000, "'3s'"), t16Timestamp(3000))
	var rejections []Case
	for _, rejection := range []struct {
		name, feature string
		query         t16Query
	}{
		{"having_bucket_mismatch", "t16.invalid_bucket_scope", t16Query{
			Columns: columns, GroupBy: []*t10Expr{t16Ordinal(1)}, Having: wrongBucket,
		}},
		{"having_bucket_mismatch_nested", "t16.invalid_bucket_scope", t16Query{
			Columns: columns, GroupBy: []*t10Expr{t16Ordinal(1)},
			Having: t10Binary("=", t10Cast(t16Bucket(3000, "'3s'"), "TIMESTAMP"), t16Timestamp(3000)),
		}},
		{"having_bucket_absent", "t16.invalid_bucket_scope", t16Query{
			Columns: []t10Column{{Expr: t10Source("host")}, t10Field("total", t14Aggregate("SUM", "v"))},
			GroupBy: []*t10Expr{t10Source("host")}, Having: t10Binary(">=", bucket, t16Timestamp(0)),
		}},
		{"having_bucket_ungrouped", "t16.invalid_bucket_scope", t16Query{
			Columns: []t10Column{t10Field("total", t14Aggregate("SUM", "v"))}, Having: t10Binary(">=", bucket, t16Timestamp(0)),
		}},
		{"where_bucket_grouped", "t16.invalid_bucket_scope", t16Query{
			Columns: columns, GroupBy: []*t10Expr{t16Ordinal(1)}, Where: t10Binary(">=", bucket, t16Timestamp(0)),
		}},
		{"where_bucket_raw", "t16.invalid_bucket_scope", t16Query{
			Columns: []t10Column{t10Field("value", t10Source("v"))}, Where: t10Binary(">=", bucket, t16Timestamp(0)),
		}},
		{"raw_bucket_projection", "t16.invalid_bucket_scope", t16Query{
			Columns: []t10Column{t10Field("bucket", bucket), t10Field("value", t10Source("v"))},
		}},
		{"bucket_duplicate_tag_alias", "t16.invalid_output_container", t16Query{
			Columns: []t10Column{t10Field("same", bucket), t10Field("same", bucket), t10Field("total", t14Aggregate("SUM", "v"))},
			GroupBy: []*t10Expr{t16Ordinal(1)},
		}},
		{"bucket_duplicate_field_alias", "t16.invalid_output_container", t16Query{
			Columns: []t10Column{t10Field("bucket", bucket), t10Field("same", t14Aggregate("SUM", "v")), t10Field("same", t14Aggregate("COUNT", "v"))},
			GroupBy: []*t10Expr{t16Ordinal(1)},
		}},
	} {
		q := rejection.query
		q.Fixture, q.Begin, q.End = T16FixtureName, 0, 6000
		query, err := renderT16(q)
		if err != nil {
			return nil, err
		}
		rejections = append(rejections, Case{
			Name: "t16_reject_" + rejection.name, Ticket: "T16", Query: query,
			WantCode: codes.InvalidArgument, NoScan: true, Features: []string{rejection.feature},
		})
		b.features[rejection.feature] = false
	}
	return rejections, nil
}
