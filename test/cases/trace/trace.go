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

// Package trace_test contains integration test cases of the trace.
package trace_test

import (
	"time"

	g "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"

	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	trace_test_data "github.com/apache/skywalking-banyandb/test/cases/trace/data"
)

var (
	// SharedContext is the parallel execution context.
	SharedContext helpers.SharedContext
	verify        = func(innerGm gm.Gomega, args helpers.Args) {
		trace_test_data.VerifyFn(innerGm, SharedContext, args)
	}
)

var traceEntries = []any{
	g.Entry("query by trace id", helpers.Args{Input: "eq_trace_id", Duration: 1 * time.Hour}),
	g.Entry("query by trace ids", helpers.Args{Input: "in_trace_ids", Duration: 1 * time.Hour}),
	g.Entry("query by empty span ids", helpers.Args{Input: "in_empty_span_ids", Duration: 1 * time.Hour, WantEmpty: true}),
	g.Entry("order by timestamp", helpers.Args{Input: "order_timestamp_desc", Duration: 1 * time.Hour}),
	g.Entry("order by duration", helpers.Args{Input: "order_duration_desc", Duration: 1 * time.Hour}),
	g.Entry("duration range 10-1000 order by timestamp",
		helpers.Args{Input: "duration_range_order_timestamp", Duration: 1 * time.Hour}),
	g.Entry("duration range and ipv4 filter order by timestamp",
		helpers.Args{Input: "duration_range_and_ipv4_order_timestamp", Duration: 1 * time.Hour}),
	g.Entry("filter by service id", helpers.Args{Input: "eq_service_order_timestamp_desc", Duration: 1 * time.Hour}),
	g.Entry("filter by service instance id", helpers.Args{Input: "eq_service_instance_order_time_asc", Duration: 1 * time.Hour}),
	g.Entry("filter by service instance id and endpoint id", helpers.Args{Input: "eq_service_instance_and_endpoint_order_timestamp_asc", Duration: 1 * time.Hour}),
	g.Entry("filter by state and duration range and ids order by timestamp desc",
		helpers.Args{Input: "state_duration_range_and_ids_order_timestamp_desc", Duration: 1 * time.Hour}),
	g.Entry("filter by endpoint", helpers.Args{Input: "eq_endpoint_order_duration_asc", Duration: 1 * time.Hour}),
	g.Entry("order by timestamp limit 2", helpers.Args{Input: "order_timestamp_desc_limit", Duration: 1 * time.Hour}),
	g.Entry("filter by trace id and service unknown", helpers.Args{Input: "eq_trace_id_and_service_unknown", Duration: 1 * time.Hour, WantEmpty: true}),
	g.Entry("filter by query", helpers.Args{Input: "having_query_tag", Duration: 1 * time.Hour}),
	g.Entry("err in arr", helpers.Args{Input: "err_in_arr", Duration: 1 * time.Hour, WantErr: true}),
	g.Entry("filter by query with having condition", helpers.Args{Input: "having_query_tag_cond", Want: "having_query_tag", Duration: 1 * time.Hour}),
	g.Entry("multi-groups: unchanged tags", helpers.Args{Input: "multi_group_unchanged", Duration: 1 * time.Hour}),
	g.Entry("multi-groups: new tag", helpers.Args{Input: "multi_group_new_tag", Duration: 1 * time.Hour}),
	g.Entry("multi-groups: tag type change", helpers.Args{Input: "multi_group_tag_type", Duration: 1 * time.Hour}),
	g.Entry("multi-groups: sort by duration", helpers.Args{Input: "multi_group_sort_duration", Duration: 1 * time.Hour}),
	g.Entry("filter by non-existent tag", helpers.Args{Input: "filter_non_existent_tag", Duration: 1 * time.Hour, WantErr: true}),
	g.Entry("project non-existent tag", helpers.Args{Input: "project_non_existent_tag", Duration: 1 * time.Hour, WantErr: true}),
	g.Entry("write mixed", helpers.Args{Input: "write_mixed", Duration: 1 * time.Hour, DisOrder: true}),
	g.Entry("gen_leaf_eq_trace_id", helpers.Args{Input: "gen_leaf_eq_trace_id", DisOrder: true, Duration: 1 * time.Hour}),
	g.Entry("gen_leaf_ne_trace_id", helpers.Args{Input: "gen_leaf_ne_trace_id", DisOrder: true, Duration: 1 * time.Hour}),
	g.Entry("gen_leaf_in_trace_id", helpers.Args{Input: "gen_leaf_in_trace_id", DisOrder: true, Duration: 1 * time.Hour}),
	g.Entry("gen_leaf_not_in_trace_id", helpers.Args{Input: "gen_leaf_not_in_trace_id", DisOrder: true, Duration: 1 * time.Hour}),
	g.Entry("gen_leaf_eq_service_id", helpers.Args{Input: "gen_leaf_eq_service_id", DisOrder: true, Duration: 1 * time.Hour}),
	g.Entry("gen_leaf_ne_service_id", helpers.Args{Input: "gen_leaf_ne_service_id", DisOrder: true, Duration: 1 * time.Hour}),
	g.Entry("gen_leaf_in_service_id", helpers.Args{Input: "gen_leaf_in_service_id", DisOrder: true, Duration: 1 * time.Hour}),
	g.Entry("gen_leaf_not_in_service_id", helpers.Args{Input: "gen_leaf_not_in_service_id", DisOrder: true, Duration: 1 * time.Hour}),
	g.Entry("gen_leaf_eq_state", helpers.Args{Input: "gen_leaf_eq_state", DisOrder: true, Duration: 1 * time.Hour}),
	g.Entry("gen_leaf_ne_state", helpers.Args{Input: "gen_leaf_ne_state", DisOrder: true, Duration: 1 * time.Hour}),
	g.Entry("gen_leaf_lt_state", helpers.Args{Input: "gen_leaf_lt_state", DisOrder: true, Duration: 1 * time.Hour}),
	g.Entry("gen_leaf_gt_state", helpers.Args{Input: "gen_leaf_gt_state", DisOrder: true, Duration: 1 * time.Hour}),
	g.Entry("gen_leaf_le_state", helpers.Args{Input: "gen_leaf_le_state", DisOrder: true, Duration: 1 * time.Hour}),
	g.Entry("gen_leaf_ge_state", helpers.Args{Input: "gen_leaf_ge_state", DisOrder: true, Duration: 1 * time.Hour}),
	g.Entry("gen_leaf_lt_duration", helpers.Args{Input: "gen_leaf_lt_duration", DisOrder: true, Duration: 1 * time.Hour}),
	g.Entry("gen_leaf_gt_duration", helpers.Args{Input: "gen_leaf_gt_duration", DisOrder: true, Duration: 1 * time.Hour}),
	g.Entry("gen_leaf_le_duration", helpers.Args{Input: "gen_leaf_le_duration", DisOrder: true, Duration: 1 * time.Hour}),
	g.Entry("gen_leaf_ge_duration", helpers.Args{Input: "gen_leaf_ge_duration", DisOrder: true, Duration: 1 * time.Hour}),
	g.Entry("gen_leaf_eq_duration", helpers.Args{Input: "gen_leaf_eq_duration", WantEmpty: true, DisOrder: true, Duration: 1 * time.Hour}),
	g.Entry("gen_leaf_eq_service_id_null", helpers.Args{Input: "gen_leaf_eq_service_id_null", WantEmpty: true, DisOrder: true, Duration: 1 * time.Hour}),
	g.Entry("gen_err_match_sw", helpers.Args{Input: "gen_err_match_sw", WantErr: true, Duration: 1 * time.Hour}),
	g.Entry("gen_tree_depth1_leaf", helpers.Args{Input: "gen_tree_depth1_leaf", Duration: 1 * time.Hour}),
	g.Entry("gen_tree_depth2_and", helpers.Args{Input: "gen_tree_depth2_and", DisOrder: true, Duration: 1 * time.Hour}),
	g.Entry("gen_tree_depth2_or", helpers.Args{Input: "gen_tree_depth2_or", DisOrder: true, Duration: 1 * time.Hour}),
	g.Entry("gen_tree_depth3_and_or", helpers.Args{Input: "gen_tree_depth3_and_or", WantEmpty: true, DisOrder: true, Duration: 1 * time.Hour}),
	g.Entry("gen_tree_depth3_or_and", helpers.Args{Input: "gen_tree_depth3_or_and", DisOrder: true, Duration: 1 * time.Hour}),
	g.Entry("gen_tree_depth5_deep_and", helpers.Args{Input: "gen_tree_depth5_deep_and", DisOrder: true, Duration: 1 * time.Hour}),
	g.Entry("gen_tree_depth5_deep_or", helpers.Args{Input: "gen_tree_depth5_deep_or", WantEmpty: true, DisOrder: true, Duration: 1 * time.Hour}),
	g.Entry("gen_feat_traceid_0", helpers.Args{Input: "gen_feat_traceid_0", DisOrder: true, Duration: 1 * time.Hour}),
	g.Entry("gen_feat_order_duration_asc_limit2_offset1_proj_explicit_1",
		helpers.Args{Input: "gen_feat_order_duration_asc_limit2_offset1_proj_explicit_1", Duration: 1 * time.Hour}),
	g.Entry("gen_feat_traceid_limit5_offset3_proj_explicit_2",
		helpers.Args{Input: "gen_feat_traceid_limit5_offset3_proj_explicit_2", WantEmpty: true, DisOrder: true, Duration: 1 * time.Hour}),
	g.Entry("gen_feat_order_duration_desc_proj_explicit_3", helpers.Args{Input: "gen_feat_order_duration_desc_proj_explicit_3", Duration: 1 * time.Hour}),
	g.Entry("gen_feat_traceid_proj_explicit_4", helpers.Args{Input: "gen_feat_traceid_proj_explicit_4", DisOrder: true, Duration: 1 * time.Hour}),
	g.Entry("gen_feat_traceid_limit2_5", helpers.Args{Input: "gen_feat_traceid_limit2_5", DisOrder: true, Duration: 1 * time.Hour}),
	g.Entry("gen_feat_order_duration_asc_limit5_proj_explicit_6", helpers.Args{Input: "gen_feat_order_duration_asc_limit5_proj_explicit_6", Duration: 1 * time.Hour}),
	g.Entry("gen_feat_order_duration_desc_filter_service_eq_limit2_7",
		helpers.Args{Input: "gen_feat_order_duration_desc_filter_service_eq_limit2_7", Duration: 1 * time.Hour}),
	g.Entry("gen_feat_order_timestamp_asc_filter_state_eq_limit5_offset1_proj_explicit_8",
		helpers.Args{Input: "gen_feat_order_timestamp_asc_filter_state_eq_limit5_offset1_proj_explicit_8", Duration: 1 * time.Hour}),
	g.Entry("gen_feat_order_timestamp_desc_filter_duration_range_limit2_9",
		helpers.Args{Input: "gen_feat_order_timestamp_desc_filter_duration_range_limit2_9", Duration: 1 * time.Hour}),
}

// RegisterTable registers the trace test table with the given description.
func RegisterTable(description string) bool {
	return g.DescribeTable(description, append([]any{func(args helpers.Args) {
		gm.Eventually(func(innerGm gm.Gomega) {
			verify(innerGm, args)
		}, flags.EventuallyTimeout).Should(gm.Succeed())
	}}, traceEntries...)...)
}

var _ = RegisterTable("Scanning Traces")
