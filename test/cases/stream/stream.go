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

// Package stream_test contains integration test cases of the stream.
package stream_test

import (
	"math"
	"time"

	g "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	stream_test_data "github.com/apache/skywalking-banyandb/test/cases/stream/data"
)

var (
	// SharedContext is the parallel execution context.
	SharedContext helpers.SharedContext
	verify        = func(innerGm gm.Gomega, args helpers.Args) {
		stream_test_data.VerifyFn(innerGm, SharedContext, args)
	}
)

var streamEntries = []any{
	g.Entry("all elements", helpers.Args{Input: "all", Duration: 1 * time.Hour}),
	// Window sits ~6 days back, well past the "default" group's 3-day TTL. The
	// data seeded there (see test/cases/init.go) is in a fully expired segment
	// and must be dropped by the retention filter, yielding an empty result.
	// Without the filter the expired elements leak in and this case fails.
	g.Entry("excludes data expired beyond TTL", helpers.Args{Input: "all", Offset: -156 * time.Hour, Duration: 24 * time.Hour, WantEmpty: true}),
	g.Entry("projection with http.method", helpers.Args{Input: "all_with_http_method", Duration: 1 * time.Hour}),
	g.Entry("limit", helpers.Args{Input: "limit", Duration: 1 * time.Hour}),
	g.Entry("max limit", helpers.Args{Input: "all_max_limit", Want: "all", Duration: 1 * time.Hour}),
	g.Entry("offset", helpers.Args{Input: "offset", Duration: 1 * time.Hour}),
	g.Entry("order asc", helpers.Args{Input: "order_asc", Duration: 1 * time.Hour}),
	g.Entry("order desc", helpers.Args{Input: "order_desc", Duration: 1 * time.Hour}),
	g.Entry("nothing", helpers.Args{
		Input:     "all",
		Begin:     timestamppb.New(time.Unix(0, 0).Truncate(time.Millisecond)),
		End:       timestamppb.New(time.Unix(0, 1).Truncate(time.Millisecond)),
		WantEmpty: true,
	}),
	g.Entry("invalid time range", helpers.Args{
		Input: "all",
		Begin: timestamppb.New(time.Unix(0, int64(math.MinInt64+time.Millisecond)).Truncate(time.Millisecond)),
		End:   timestamppb.New(time.Unix(0, math.MaxInt64).Truncate(time.Millisecond)),
	}),
	g.Entry("sort desc", helpers.Args{Input: "sort_desc", Duration: 1 * time.Hour}),
	g.Entry("sort with filter", helpers.Args{Input: "sort_filter", Duration: 1 * time.Hour}),
	g.Entry("sort with empty result", helpers.Args{Input: "sort_empty", Duration: 1 * time.Hour, WantEmpty: true}),
	g.Entry("global index", helpers.Args{Input: "global_index", Duration: 1 * time.Hour}),
	g.Entry("multi-global index", helpers.Args{Input: "global_indices", Duration: 1 * time.Hour}),
	g.Entry("filter by non-indexed tag", helpers.Args{Input: "filter_tag", Duration: 1 * time.Hour}),
	g.Entry("filter with bound parameters", helpers.Args{Input: "params_bind", Want: "filter_tag", Duration: 1 * time.Hour}),
	g.Entry("filter hidden tag projection", helpers.Args{Input: "filter_hidden_tag", Duration: 1 * time.Hour}),
	g.Entry("filter by non-indexed tag order by duration desc with limit 3", helpers.Args{Input: "sort_duration_no_index_limit", Duration: 1 * time.Hour}),
	g.Entry("get empty result by non-indexed tag", helpers.Args{Input: "filter_tag_empty", Duration: 1 * time.Hour, WantEmpty: true}),
	g.Entry("get results by no non-index tag", helpers.Args{Input: "filter_no_indexed", Duration: 1 * time.Hour}),
	g.Entry("numeric local index: less", helpers.Args{Input: "less", Duration: 1 * time.Hour}),
	g.Entry("numeric local index: less and eq", helpers.Args{Input: "less_eq", Duration: 1 * time.Hour}),
	g.Entry("logical expression", helpers.Args{Input: "logical", Duration: 1 * time.Hour}),
	g.Entry("having", helpers.Args{Input: "having", Duration: 1 * time.Hour}),
	g.Entry("having non indexed", helpers.Args{Input: "having_non_indexed", Duration: 1 * time.Hour}),
	g.Entry("having non indexed array", helpers.Args{Input: "having_non_indexed_arr", Duration: 1 * time.Hour}),
	g.Entry("full text searching", helpers.Args{Input: "search", Duration: 1 * time.Hour}),
	g.Entry("filter by non-indexed tag with or", helpers.Args{Input: "filter_no_indexed_or", Duration: 1 * time.Hour}),
	g.Entry("filter with desc order", helpers.Args{Input: "filter_order_desc", Duration: 1 * time.Hour}),
	g.Entry("duplicated all elements", helpers.Args{Input: "duplicated_all", Duration: 1 * time.Hour, DisOrder: true}),
	g.Entry("duplicated entity filter", helpers.Args{Input: "duplicated_entity_filter", Duration: 1 * time.Hour, DisOrder: true}),
	g.Entry("duplicated index filter", helpers.Args{Input: "duplicated_index_filter", Duration: 1 * time.Hour, DisOrder: true}),
	g.Entry("duplicated order by index", helpers.Args{Input: "duplicated_order_by_index", Duration: 1 * time.Hour}),
	g.Entry("duplicated order by index with the index filter", helpers.Args{Input: "duplicated_order_by_filter", Duration: 1 * time.Hour}),
	g.Entry("deduplication test limit 10", helpers.Args{Input: "deduplication_test_limit_10", Duration: 1 * time.Hour}),
	g.Entry("deduplication test limit 25", helpers.Args{Input: "deduplication_test_limit_25", Duration: 1 * time.Hour}),
	g.Entry("deduplication test limit 40", helpers.Args{Input: "deduplication_test_limit_40", Duration: 1 * time.Hour}),
	g.Entry("deduplication test limit 100", helpers.Args{Input: "deduplication_test_limit_100", Duration: 1 * time.Hour}),
	g.Entry("multi-groups: unchanged tags", helpers.Args{Input: "multi_group_unchanged", Duration: 1 * time.Hour, IgnoreElementID: true}),
	g.Entry("multi-groups: new tags", helpers.Args{Input: "multi_group_new_tag", Duration: 1 * time.Hour, IgnoreElementID: true}),
	g.Entry("multi-groups: update tag type", helpers.Args{Input: "multi_group_tag_type", Duration: 1 * time.Hour, IgnoreElementID: true}),
	g.Entry("multi-groups: sort duration", helpers.Args{Input: "multi_group_sort_duration", Duration: 1 * time.Hour, IgnoreElementID: true}),
	g.Entry("hybrid index", helpers.Args{Input: "hybrid_index", Duration: 1 * time.Hour, IgnoreElementID: true}),
	g.Entry("err in arr", helpers.Args{Input: "err_in_arr", Duration: 1 * time.Hour, WantErr: true}),
	g.Entry("filter by non-existent tag", helpers.Args{Input: "filter_non_existent_tag", Duration: 1 * time.Hour, WantErr: true}),
	g.Entry("project non-existent tag", helpers.Args{Input: "project_non_existent_tag", Duration: 1 * time.Hour, WantErr: true}),
	g.Entry("write mixed", helpers.Args{Input: "write_mixed", Duration: 1 * time.Hour, IgnoreElementID: true}),
	// --- generated entries (do not edit by hand; produced by cmd/generate) ---
	g.Entry("gen_leaf_eq_duration", helpers.Args{Input: "gen_leaf_eq_duration", Duration: 1 * time.Hour}),
	g.Entry("gen_leaf_ne_duration", helpers.Args{Input: "gen_leaf_ne_duration", Duration: 1 * time.Hour}),
	g.Entry("gen_leaf_lt_duration", helpers.Args{Input: "gen_leaf_lt_duration", Duration: 1 * time.Hour}),
	g.Entry("gen_leaf_gt_duration", helpers.Args{Input: "gen_leaf_gt_duration", Duration: 1 * time.Hour}),
	g.Entry("gen_leaf_le_duration", helpers.Args{Input: "gen_leaf_le_duration", Duration: 1 * time.Hour}),
	g.Entry("gen_leaf_ge_duration", helpers.Args{Input: "gen_leaf_ge_duration", Duration: 1 * time.Hour}),
	g.Entry("gen_leaf_eq_state", helpers.Args{Input: "gen_leaf_eq_state", Duration: 1 * time.Hour}),
	g.Entry("gen_err_ne_state", helpers.Args{Input: "gen_err_ne_state", WantErr: true, Duration: 1 * time.Hour}),
	g.Entry("gen_err_lt_state", helpers.Args{Input: "gen_err_lt_state", WantErr: true, Duration: 1 * time.Hour}),
	g.Entry("gen_err_gt_state", helpers.Args{Input: "gen_err_gt_state", WantErr: true, Duration: 1 * time.Hour}),
	g.Entry("gen_err_le_state", helpers.Args{Input: "gen_err_le_state", WantErr: true, Duration: 1 * time.Hour}),
	g.Entry("gen_err_ge_state", helpers.Args{Input: "gen_err_ge_state", WantErr: true, Duration: 1 * time.Hour}),
	g.Entry("gen_leaf_eq_service_id", helpers.Args{Input: "gen_leaf_eq_service_id", Duration: 1 * time.Hour}),
	g.Entry("gen_err_ne_service_id", helpers.Args{Input: "gen_err_ne_service_id", WantErr: true, Duration: 1 * time.Hour}),
	g.Entry("gen_leaf_in_service_id", helpers.Args{Input: "gen_leaf_in_service_id", Duration: 1 * time.Hour}),
	g.Entry("gen_err_not_in_service_id", helpers.Args{Input: "gen_err_not_in_service_id", WantErr: true, Duration: 1 * time.Hour}),
	g.Entry("gen_leaf_in_state", helpers.Args{Input: "gen_leaf_in_state", Duration: 1 * time.Hour}),
	g.Entry("gen_err_not_in_state", helpers.Args{Input: "gen_err_not_in_state", WantErr: true, Duration: 1 * time.Hour}),
	g.Entry("gen_leaf_having_extended_tags", helpers.Args{Input: "gen_leaf_having_extended_tags", Duration: 1 * time.Hour}),
	g.Entry("gen_leaf_not_having_extended_tags", helpers.Args{Input: "gen_leaf_not_having_extended_tags", Duration: 1 * time.Hour}),
	g.Entry("gen_leaf_match_db.instance", helpers.Args{Input: "gen_leaf_match_db.instance", Duration: 1 * time.Hour}),
	g.Entry("gen_err_match_trace_id", helpers.Args{Input: "gen_err_match_trace_id", WantErr: true, Duration: 1 * time.Hour}),
	g.Entry("gen_tree_depth1_leaf", helpers.Args{Input: "gen_tree_depth1_leaf", Duration: 1 * time.Hour}),
	g.Entry("gen_tree_depth2_and", helpers.Args{Input: "gen_tree_depth2_and", Duration: 1 * time.Hour}),
	g.Entry("gen_tree_depth2_or", helpers.Args{Input: "gen_tree_depth2_or", DisOrder: true, Duration: 1 * time.Hour}),
	g.Entry("gen_tree_depth3_and_or", helpers.Args{Input: "gen_tree_depth3_and_or", DisOrder: true, Duration: 1 * time.Hour}),
	g.Entry("gen_tree_depth3_or_and", helpers.Args{Input: "gen_tree_depth3_or_and", DisOrder: true, Duration: 1 * time.Hour}),
	g.Entry("gen_tree_depth5_deep_and", helpers.Args{Input: "gen_tree_depth5_deep_and", DisOrder: true, Duration: 1 * time.Hour}),
	g.Entry("gen_tree_depth5_deep_or", helpers.Args{Input: "gen_tree_depth5_deep_or", DisOrder: true, Duration: 1 * time.Hour}),
	g.Entry("gen_tree_depth2_contradict_and", helpers.Args{Input: "gen_tree_depth2_contradict_and", WantEmpty: true, Duration: 1 * time.Hour}),
	g.Entry("gen_feat_none_filter_none_limit2_proj_explicit_group_single_0", helpers.Args{
		Input:    "gen_feat_none_filter_none_limit2_proj_explicit_group_single_0",
		Duration: 1 * time.Hour,
	}),
	g.Entry("gen_feat_ts_asc_filter_none_limit5_offset1_proj_all_group_single_1", helpers.Args{
		Input:           "gen_feat_ts_asc_filter_none_limit5_offset1_proj_all_group_single_1",
		DisOrder:        true,
		IgnoreElementID: true,
		Duration:        1 * time.Hour,
	}),
	g.Entry("gen_feat_ts_desc_filter_none_limit2_offset3_proj_all_group_single_2", helpers.Args{
		Input:           "gen_feat_ts_desc_filter_none_limit2_offset3_proj_all_group_single_2",
		DisOrder:        true,
		IgnoreElementID: true,
		Duration:        1 * time.Hour,
	}),
	g.Entry("gen_feat_duration_asc_filter_none_limit2_offset1_proj_explicit_group_single_3", helpers.Args{
		Input:    "gen_feat_duration_asc_filter_none_limit2_offset1_proj_explicit_group_single_3",
		Duration: 1 * time.Hour,
	}),
	g.Entry("gen_feat_duration_desc_filter_none_limit2_proj_all_group_single_4", helpers.Args{
		Input:    "gen_feat_duration_desc_filter_none_limit2_proj_all_group_single_4",
		Duration: 1 * time.Hour,
	}),
	g.Entry("gen_feat_ts_asc_filter_none_limit2_proj_explicit_group_single_5", helpers.Args{
		Input:           "gen_feat_ts_asc_filter_none_limit2_proj_explicit_group_single_5",
		DisOrder:        true,
		IgnoreElementID: true,
		Duration:        1 * time.Hour,
	}),
	g.Entry("gen_feat_ts_desc_filter_none_limit2_proj_explicit_group_single_6", helpers.Args{
		Input:           "gen_feat_ts_desc_filter_none_limit2_proj_explicit_group_single_6",
		DisOrder:        true,
		IgnoreElementID: true,
		Duration:        1 * time.Hour,
	}),
	g.Entry("gen_feat_duration_asc_filter_none_limit2_proj_all_group_single_7", helpers.Args{
		Input:    "gen_feat_duration_asc_filter_none_limit2_proj_all_group_single_7",
		Duration: 1 * time.Hour,
	}),
	g.Entry("gen_feat_none_filter_none_limit2_proj_all_group_single_8", helpers.Args{
		Input:    "gen_feat_none_filter_none_limit2_proj_all_group_single_8",
		Duration: 1 * time.Hour,
	}),
	g.Entry("gen_feat_none_filter_none_proj_explicit_group_single_9", helpers.Args{
		Input:    "gen_feat_none_filter_none_proj_explicit_group_single_9",
		Duration: 1 * time.Hour,
	}),
	g.Entry("gen_feat_ts_asc_filter_service_eq_limit2_proj_all_group_single_10", helpers.Args{
		Input:           "gen_feat_ts_asc_filter_service_eq_limit2_proj_all_group_single_10",
		DisOrder:        true,
		IgnoreElementID: true,
		Duration:        1 * time.Hour,
	}),
	g.Entry("gen_feat_ts_desc_filter_state_eq_limit5_offset1_proj_explicit_group_single_11", helpers.Args{
		Input:           "gen_feat_ts_desc_filter_state_eq_limit5_offset1_proj_explicit_group_single_11",
		DisOrder:        true,
		IgnoreElementID: true,
		Duration:        1 * time.Hour,
	}),
	g.Entry("gen_feat_duration_asc_filter_duration_range_limit5_offset3_proj_all_group_single_12", helpers.Args{
		Input:     "gen_feat_duration_asc_filter_duration_range_limit5_offset3_proj_all_group_single_12",
		WantEmpty: true,
		Duration:  1 * time.Hour,
	}),
	g.Entry("gen_feat_duration_desc_filter_none_limit2_proj_explicit_group_single_13", helpers.Args{
		Input:    "gen_feat_duration_desc_filter_none_limit2_proj_explicit_group_single_13",
		Duration: 1 * time.Hour,
	}),
	g.Entry("gen_feat_duration_asc_filter_service_eq_proj_explicit_group_single_14", helpers.Args{
		Input:    "gen_feat_duration_asc_filter_service_eq_proj_explicit_group_single_14",
		Duration: 1 * time.Hour,
	}),
	g.Entry("gen_feat_duration_desc_filter_duration_range_proj_all_group_multi_15", helpers.Args{
		Input:           "gen_feat_duration_desc_filter_duration_range_proj_all_group_multi_15",
		DisOrder:        true,
		IgnoreElementID: true,
		Duration:        1 * time.Hour,
	}),
	g.Entry("gen_feat_ts_asc_filter_state_eq_proj_all_group_single_16", helpers.Args{
		Input:           "gen_feat_ts_asc_filter_state_eq_proj_all_group_single_16",
		DisOrder:        true,
		IgnoreElementID: true,
		Duration:        1 * time.Hour,
	}),
	// --- end generated entries ---
}

// RegisterTable registers the stream test table with the given description.
func RegisterTable(description string) bool {
	return g.DescribeTable(description, append([]any{func(args helpers.Args) {
		test.EventuallyConsistently(func(innerGm gm.Gomega) {
			verify(innerGm, args)
		}, flags.EventuallyTimeout).Should(gm.Succeed())
	}}, streamEntries...)...)
}

var _ = RegisterTable("Scanning Streams")
