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
	"fmt"
	"strings"

	"google.golang.org/grpc/codes"
)

func (b *suiteBuilder) addT03(name string, q T03Query, features ...string) {
	if b.err != nil {
		return
	}
	c, err := BuildT03Case("t03_"+name, q, features...)
	if err != nil {
		b.err = fmt.Errorf("generate T03 %s: %w", name, err)
		return
	}
	for _, feature := range features {
		b.features[feature] = true
	}
	b.cases = append(b.cases, c)
}

func generateT03(b *suiteBuilder) {
	b.addT03("scan_all", T03Query{Columns: []T03Column{t03RID()}, Shorthand: true}, "catalog.B2", "t03.limit.complete_input")
	generateT03Aliases(b)
	generateT03Times(b)
	generateT03Limits(b)
	generateT03Errors(b)
	trace := T03Query{
		Columns:   []T03Column{{Source: "host", Alias: "node"}, {Source: "v::FIELD", Alias: "value"}},
		Shorthand: true, Where: t03Window(), Trace: true,
	}
	b.addT03("trace", trace, "catalog.B2", "trace.enabled")
}

func generateT03Aliases(b *suiteBuilder) {
	projections := [][]T03Column{
		{{Source: "rid"}, {Source: "host", Alias: "node"}, {Source: "v::FIELD", Alias: "value"}, {Source: "f::FIELD", Alias: "floating"}},
		{{Source: "host", Alias: "node"}, {Source: "host", Alias: "other_node"}, {Source: "v::FIELD", Alias: "value"}, {Source: "v::FIELD", Alias: "copy"}},
		{{Source: "default.host::TAG", Alias: "node"}, {Source: "extra.location::TAG", Alias: "site"}, {Source: "zone", Alias: "nullable_zone"}},
		{
			{Source: "time", Alias: "observed"},
			{Source: "time", Alias: "again"},
			{Source: "__ql.clock::TAG", Alias: "stored"},
			{Source: "clash::TAG", Alias: "tagged"},
			{Source: "clash::FIELD", Alias: "numeric"},
		},
	}
	for _, short := range []bool{false, true} {
		for _, descending := range []bool{false, true} {
			q := T03Query{
				Columns: []T03Column{{Source: "time", Alias: "observed"}, t03RID()},
				Where:   t03Window(), OrderBy: "observed", Shorthand: short, Descending: descending,
			}
			b.addT03(fmt.Sprintf("timestamp_alias_order_short_%t_desc_%t", short, descending), q, "catalog.B2", "t03.order.timestamp_alias")
		}
	}
	b.addT03("same_alias_separate_containers", T03Query{
		Columns:   []T03Column{{Source: "host", Alias: "same"}, {Source: "extra.location::TAG", Alias: "same"}, {Source: "v::FIELD", Alias: "same"}},
		Shorthand: true, Where: t03Window(),
	}, "catalog.B2", "t03.projection.families")
	for _, alias := range []string{"", "observed"} {
		for _, short := range []bool{false, true} {
			q := T03Query{Columns: []T03Column{{Source: "time", Alias: alias}}, Shorthand: short, Where: t03Window()}
			name := alias
			if name == "" {
				name = "native"
			}
			b.addT03(fmt.Sprintf("timestamp_only_%s_short_%t", name, short), q, "catalog.B2", "t03.projection.timestamp")
		}
	}
	for index, columns := range projections {
		for _, short := range []bool{false, true} {
			for _, bind := range []bool{false, true} {
				for _, descending := range []bool{false, true} {
					q := T03Query{Columns: columns, Where: t03Window(), Shorthand: short, Bind: bind, Descending: descending}
					features := []string{"catalog.B2", "t03.projection.typed"}
					if index == 1 || index == 3 {
						features = append(features, "t03.projection.repeated")
					}
					if index >= 2 {
						features = append(features, "t03.projection.families")
					}
					if index == 3 {
						features = append(features, "t03.projection.timestamp")
					}
					b.addT03(fmt.Sprintf("projection_%d_short_%t_bind_%t_desc_%t", index, short, bind, descending), q, features...)
				}
			}
		}
	}
}

func generateT03Times(b *suiteBuilder) {
	minimum := "1677-09-21T00:12:43.146Z"
	maximum := "2262-04-11T23:47:16.854Z"
	for _, bound := range []bool{false, true} {
		q := T03Query{
			Columns: []T03Column{t03RID()}, Shorthand: true, Bind: bound,
			Where: t03And(&T03Time{Op: ">=", Absolute: minimum}, &T03Time{Op: "<=", Absolute: maximum}),
		}
		b.addT03(fmt.Sprintf("storage_domain_full_bind_%t", bound), q, "catalog.B1", "t03.time.domain")
		for index, extreme := range []string{minimum, maximum} {
			q.Where = &T03Time{Op: "=", Absolute: extreme}
			b.addT03(fmt.Sprintf("storage_domain_equal_%d_bind_%t", index, bound), q, "catalog.B1", "t03.time.domain")
		}
	}
	for _, op := range []string{"=", "!=", ">", ">=", "<", "<="} {
		for _, delta := range []int64{-1, 0, 1} {
			for _, bound := range []bool{false, true} {
				q := T03Query{Columns: []T03Column{t03RID()}, Shorthand: true, Bind: bound, Where: t03And(t03Window(), &T03Time{Op: op, Nanos: 2_000_000_000 + delta})}
				b.addT03(fmt.Sprintf("time_%s_delta_%d_bind_%t", timeOperatorName(op), delta, bound), q, "catalog.B1", "t03.time.fractional")
			}
		}
	}
	for _, bound := range []bool{false, true} {
		for _, timestampParam := range []bool{false, true} {
			q := T03Query{
				Columns: []T03Column{t03RID()}, Bind: bound, TimestampParams: timestampParam, Shorthand: true,
				Where: t03And(t03Compare(">", 1000), t03Compare("<=", 3000)),
			}
			b.addT03(fmt.Sprintf("boundary_bind_%t_timestamp_%t", bound, timestampParam), q, "catalog.B1", "t03.time.boundaries")
		}
	}
	or := &T03Time{Op: "OR", Children: []*T03Time{t03Compare("=", 0), t03Compare("=", 5000)}}
	for _, bound := range []bool{false, true} {
		q := T03Query{Columns: []T03Column{t03RID()}, Shorthand: true, Bind: bound, Where: or}
		b.addT03(fmt.Sprintf("time_or_bind_%t", bound), q, "catalog.B1", "t03.time.or")
		begin := int64(1000)
		q.LegacyBegin = &begin
		q.LegacyEnd = 4000
		q.NoScan = true
		b.addT03(fmt.Sprintf("time_intersection_empty_bind_%t", bound), q, "catalog.B1", "t03.time.intersection", "t03.empty")
		q.NoScan = false
		q.Where = t03And(t03Compare(">", 1000), t03Compare("<=", 3000))
		b.addT03(fmt.Sprintf("time_intersection_bind_%t", bound), q, "catalog.B1", "t03.time.intersection")
	}
	for _, millis := range []int64{3000, 6000} {
		for _, bind := range []bool{false, true} {
			clock := millis
			q := T03Query{
				Columns: []T03Column{t03RID()}, Shorthand: true, Bind: bind, ClockMillis: &clock,
				Where: t03And(&T03Time{Op: ">=", Now: true, Nanos: -1_000_000_000}, &T03Time{Op: "<=", Now: true}),
			}
			b.addT03(fmt.Sprintf("fresh_now_%d_bind_%t", millis, bind), q, "catalog.B1", "t03.time.fresh_now", "t03.time.interval")
		}
	}
	for _, window := range [][2]int64{{0, 2000}, {4000, 6000}, {9000, 11000}} {
		q := T03Query{Columns: []T03Column{t03RID()}, Shorthand: true, Bind: true, Where: t03And(t03Compare(">=", window[0]), t03Compare("<", window[1]))}
		b.addT03(fmt.Sprintf("range_rebind_%d", window[0]), q, "catalog.B1", "t03.time.rebind")
	}
	q := T03Query{Columns: []T03Column{t03RID()}, Shorthand: true, Where: t03And(t03Compare(">", 3000), t03Compare("<", 3000))}
	q.NoScan = true
	b.addT03("contradictory_time", q, "catalog.B1", "t03.empty")
}

func generateT03Limits(b *suiteBuilder) {
	for _, limit := range []*uint32{nil, uint32Pointer(0), uint32Pointer(3), uint32Pointer(125)} {
		for _, offset := range []uint32{0, 3, 150} {
			q := T03Query{Columns: []T03Column{t03RID(), {Source: "v::FIELD", Alias: "value"}}, Shorthand: true, Limit: limit, Offset: offset}
			name := "absent"
			if limit != nil {
				name = fmt.Sprintf("%d", *limit)
			}
			b.addT03(fmt.Sprintf("limit_%s_offset_%d", name, offset), q, "catalog.B2", "t03.limit.extended", "t03.limit.complete_input")
		}
	}
	for _, limit := range []*uint32{nil, uint32Pointer(0)} {
		begin := int64(-1)
		q := T03Query{Columns: []T03Column{t03RID()}, LegacyBegin: &begin, LegacyEnd: 130000, Limit: limit, Legacy: true}
		name := "absent"
		if limit != nil {
			name = "zero"
		}
		b.addT03("legacy_limit_"+name, q, "legacy.default_limit")
	}
}

func generateT03Errors(b *suiteBuilder) {
	projections := [][]T03Column{
		{{Source: "host", Alias: "same"}, {Source: "zone", Alias: "same"}},
		{{Source: "v::FIELD", Alias: "same"}, {Source: "f::FIELD", Alias: "same"}},
		{{Source: "host"}, {Source: "host"}},
		{{Source: "clash"}},
		{{Source: "time", Alias: "clock"}, {Source: "__ql.clock::TAG"}},
		{{Source: "missing::TAG"}},
	}
	for index, columns := range projections {
		for _, short := range []bool{false, true} {
			text, err := renderT03(T03Query{Columns: columns, Shorthand: short, Where: t03Window()})
			if err != nil {
				b.err = err
				return
			}
			feature := "t03.invalid.names"
			b.features[feature] = false
			b.cases = append(b.cases, Case{
				Name: fmt.Sprintf("t03_invalid_projection_%d_short_%t", index, short), Ticket: "T03", NoScan: true, Query: text,
				WantCode: codes.InvalidArgument, Features: []string{feature},
			})
		}
	}
	for index, query := range []T03Query{
		{Columns: []T03Column{{Source: "v::FIELD", Alias: "value"}}, OrderBy: "absent_sort_key::FIELD"},
		{Columns: []T03Column{t03RID()}, OrderBy: "absent_sort_key::TAG"},
		{Columns: []T03Column{{Source: "host", Alias: "same"}, {Source: "v::FIELD", Alias: "same"}}, OrderBy: "same"},
	} {
		query.Shorthand = true
		query.Where = t03Window()
		text, err := renderT03(query)
		if err != nil {
			b.err = err
			return
		}
		feature := "t03.invalid.order"
		b.features[feature] = false
		b.cases = append(b.cases, Case{
			Name: fmt.Sprintf("t03_invalid_order_%d", index), Ticket: "T03", NoScan: true, Query: text,
			WantCode: codes.InvalidArgument, Features: []string{feature},
		})
	}
	for index, literal := range []string{
		"0001-01-01T00:00:00Z", "9999-12-31T23:59:59.999Z",
		"2262-04-11T23:47:16.855Z", "1677-09-21T00:12:43.145Z",
		"2262-04-11T23:47:16.854775808Z", "1677-09-21T00:12:43.145224191Z",
	} {
		text, err := renderT03(T03Query{Columns: []T03Column{t03RID()}, Shorthand: true, Where: t03Compare("=", 2000)})
		if err != nil {
			b.err = err
			return
		}
		text = strings.Replace(text, "{{nanos:2000000000}}", literal, 1)
		feature := "t03.invalid.time"
		b.features[feature] = false
		b.cases = append(b.cases, Case{
			Name: fmt.Sprintf("t03_invalid_storage_domain_%d", index), Ticket: "T03", NoScan: true, Query: text,
			WantCode: codes.InvalidArgument, Features: []string{feature},
		})
	}
	for _, interval := range []string{"1month", "1year", "-1s", "9223372036854775808h"} {
		text, err := renderT03(T03Query{Columns: []T03Column{t03RID()}, Shorthand: true, Where: &T03Time{Op: ">=", Now: true, Nanos: -1_000_000_000}})
		if err != nil {
			b.err = err
			return
		}
		text = strings.Replace(text, "INTERVAL '1s'", "INTERVAL '"+interval+"'", 1)
		feature := "t03.invalid.time"
		b.features[feature] = false
		b.cases = append(b.cases, Case{
			Name: "t03_invalid_interval_" + strings.ReplaceAll(interval, "-", "negative_"), Ticket: "T03", NoScan: true, Query: text,
			WantCode: codes.InvalidArgument, Features: []string{feature},
		})
	}
}

func timeOperatorName(op string) string {
	return map[string]string{"=": "eq", "!=": "ne", ">": "gt", ">=": "ge", "<": "lt", "<=": "le"}[op]
}
func uint32Pointer(value uint32) *uint32 { return &value }
