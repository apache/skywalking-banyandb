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

// Package measure_test contains integration test cases of the measure.
package measure_test

import (
	"time"

	g "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"

	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	measureTestData "github.com/apache/skywalking-banyandb/test/cases/measure/data"
)

var (
	// SharedContext is the parallel execution context.
	SharedContext helpers.SharedContext
	verify        = func(args helpers.Args) {
		gm.Eventually(func(innerGm gm.Gomega) {
			measureTestData.VerifyFn(innerGm, SharedContext, args)
		}, flags.EventuallyTimeout).Should(gm.Succeed())
	}
)

var _ = g.DescribeTable("Scanning Measures", verify,
	g.Entry("filter hidden tag projection", helpers.Args{Input: "filter_hidden_tag", Duration: 25 * time.Minute, Offset: -20 * time.Minute, DisOrder: true}),
	g.Entry("index mode filter hidden tag projection", helpers.Args{Input: "index_mode_filter_hidden_tag", Duration: 25 * time.Minute, Offset: -20 * time.Minute}),
	g.Entry("all", helpers.Args{Input: "all", Duration: 25 * time.Minute, Offset: -20 * time.Minute}),
	g.Entry("all only fields", helpers.Args{Input: "all_only_fields", Duration: 25 * time.Minute, Offset: -20 * time.Minute}),
	g.Entry("the max limit", helpers.Args{Input: "all_max_limit", Want: "all", Duration: 25 * time.Minute, Offset: -20 * time.Minute}),
	g.Entry("filter by tag", helpers.Args{Input: "tag_filter", Duration: 25 * time.Minute, Offset: -20 * time.Minute}),
	g.Entry("filter by a integer tag", helpers.Args{Input: "tag_filter_int", Duration: 25 * time.Minute, Offset: -20 * time.Minute, DisOrder: true}),
	g.Entry("filter by an unknown tag", helpers.Args{Input: "tag_filter_unknown", Duration: 25 * time.Minute, Offset: -20 * time.Minute, WantEmpty: true}),
	g.Entry("group and max", helpers.Args{Input: "group_max", Duration: 25 * time.Minute, Offset: -20 * time.Minute}),
	g.Entry("group and min", helpers.Args{Input: "group_min", Duration: 25 * time.Minute, Offset: -20 * time.Minute}),
	g.Entry("group and sum", helpers.Args{Input: "group_sum", Duration: 25 * time.Minute, Offset: -20 * time.Minute}),
	g.Entry("group without field", helpers.Args{Input: "group_no_field", Duration: 25 * time.Minute, Offset: -20 * time.Minute}),
	g.Entry("top 2 by id", helpers.Args{Input: "top", Duration: 25 * time.Minute, Offset: -20 * time.Minute}),
	g.Entry("bottom 2 by id", helpers.Args{Input: "bottom", Duration: 25 * time.Minute, Offset: -20 * time.Minute}),
	g.Entry("top 5 by entity id", helpers.Args{Input: "top_entity", Duration: 30 * time.Minute, Offset: -20 * time.Minute}),
	g.Entry("bottom 5 by entity id", helpers.Args{Input: "bottom_entity", Duration: 30 * time.Minute, Offset: -20 * time.Minute}),
	g.Entry("top 2 by entity id in svc_1", helpers.Args{Input: "top_entity_svc", Duration: 30 * time.Minute, Offset: -20 * time.Minute}),
	g.Entry("bottom 2 by entity id in svc_1", helpers.Args{Input: "bottom_entity_svc", Duration: 30 * time.Minute, Offset: -20 * time.Minute}),
	g.Entry("order by time asc", helpers.Args{Input: "order_asc", Duration: 25 * time.Minute, Offset: -20 * time.Minute}),
	g.Entry("order by time desc", helpers.Args{Input: "order_desc", Duration: 25 * time.Minute, Offset: -20 * time.Minute}),
	g.Entry("order by tag asc", helpers.Args{Input: "order_tag_asc", Duration: 25 * time.Minute, Offset: -20 * time.Minute}),
	g.Entry("order by tag desc", helpers.Args{Input: "order_tag_desc", Duration: 25 * time.Minute, Offset: -20 * time.Minute}),
	g.Entry("limit 3,2", helpers.Args{Input: "limit", Duration: 25 * time.Minute, Offset: -20 * time.Minute}),
	g.Entry("match a node", helpers.Args{Input: "match_node", Duration: 25 * time.Minute, Offset: -20 * time.Minute}),
	g.Entry("match nodes", helpers.Args{Input: "match_nodes", Duration: 25 * time.Minute, Offset: -20 * time.Minute}),
	g.Entry("filter by entity id", helpers.Args{Input: "entity", Duration: 25 * time.Minute, Offset: -20 * time.Minute, DisOrder: true}),
	g.Entry("filter by several entity ids", helpers.Args{Input: "entity_in", Duration: 25 * time.Minute, Offset: -20 * time.Minute, DisOrder: true}),
	g.Entry("filter by entity id and service id", helpers.Args{Input: "entity_service", Duration: 25 * time.Minute, Offset: -20 * time.Minute, DisOrder: true}),
	g.Entry("without field", helpers.Args{Input: "no_field", Duration: 25 * time.Minute, Offset: -20 * time.Minute, DisOrder: true}),
	g.Entry("invalid logical expression", helpers.Args{Input: "err_invalid_le", Duration: 25 * time.Minute, Offset: -20 * time.Minute, WantErr: true}),
	g.Entry("linked or expressions", helpers.Args{Input: "linked_or", Duration: 25 * time.Minute, Offset: -20 * time.Minute}),
	g.Entry("In and not In expressions", helpers.Args{Input: "in", Duration: 25 * time.Minute, Offset: -20 * time.Minute}),
	g.Entry("float64 value", helpers.Args{Input: "float", Duration: 25 * time.Minute, Offset: -20 * time.Minute}),
	g.Entry("float64 aggregation:min", helpers.Args{Input: "float_agg_min", Duration: 25 * time.Minute, Offset: -20 * time.Minute}),
	g.Entry("all_latency", helpers.Args{Input: "all_latency", Duration: 25 * time.Minute, Offset: -20 * time.Minute}),
	g.Entry("duplicated in a part", helpers.Args{Input: "duplicated_part", Duration: 25 * time.Minute, Offset: -20 * time.Minute}),
	g.Entry("match a tag belongs to the entity", helpers.Args{Input: "entity_match", Duration: 25 * time.Minute, Offset: -20 * time.Minute}),
	g.Entry("all of index mode", helpers.Args{Input: "index_mode_all", Duration: 25 * time.Minute, Offset: -20 * time.Minute, DisOrder: true}),
	g.Entry("all of index mode in a larger time range",
		helpers.Args{Input: "index_mode_all", Want: "index_mode_all_xl", Duration: 96 * time.Hour, Offset: -72 * time.Hour, DisOrder: true}),
	g.Entry("all in all segments of index mode",
		helpers.Args{Input: "index_mode_all", Want: "index_mode_all_segs", Duration: 96 * time.Hour, Offset: -72 * time.Hour, DisOrder: true}),
	g.Entry("order by desc of index mode", helpers.Args{Input: "index_mode_order_desc", Duration: 25 * time.Minute, Offset: -20 * time.Minute}),
	g.Entry("range of index mode", helpers.Args{Input: "index_mode_range", Duration: 25 * time.Minute, Offset: -20 * time.Minute, DisOrder: true}),
	g.Entry("none of index mode", helpers.Args{Input: "index_mode_none", WantEmpty: true, Duration: 25 * time.Minute, Offset: -20 * time.Minute}),
	g.Entry("query by id in index mode", helpers.Args{Input: "index_mode_by_id", Duration: 25 * time.Minute, Offset: -20 * time.Minute}),
	g.Entry("multi groups: unchanged", helpers.Args{Input: "multi_group_unchanged", Duration: 35 * time.Minute, Offset: -20 * time.Minute}),
	g.Entry("multi groups: new tag and fields", helpers.Args{Input: "multi_group_new_tag_field", Duration: 35 * time.Minute, Offset: -20 * time.Minute}),
	g.Entry("filter by non-existent tag", helpers.Args{Input: "filter_non_existent_tag", Duration: 25 * time.Minute, Offset: -20 * time.Minute, WantErr: true}),
	g.Entry("project non-existent tag", helpers.Args{Input: "project_non_existent_tag", Duration: 25 * time.Minute, Offset: -20 * time.Minute, WantErr: true}),
	g.Entry("write mixed", helpers.Args{Input: "write_mixed", Duration: 15 * time.Minute, Offset: 25 * time.Minute, DisOrder: true}),
)
