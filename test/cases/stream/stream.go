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

var _ = g.DescribeTable("Scanning Streams", func(args helpers.Args) {
	gm.Eventually(func(innerGm gm.Gomega) {
		verify(innerGm, args)
	}, flags.EventuallyTimeout).Should(gm.Succeed())
},
	g.Entry("all elements", helpers.Args{Input: "all", Duration: 1 * time.Hour}),
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
	g.Entry("multi-groups: unchanged tags", helpers.Args{Input: "multi_group_unchanged", Duration: 1 * time.Hour, IgnoreElementID: true}),
	g.Entry("multi-groups: new tags", helpers.Args{Input: "multi_group_new_tag", Duration: 1 * time.Hour, IgnoreElementID: true}),
	g.Entry("multi-groups: update tag type", helpers.Args{Input: "multi_group_tag_type", Duration: 1 * time.Hour, IgnoreElementID: true}),
	g.Entry("multi-groups: sort duration", helpers.Args{Input: "multi_group_sort_duration", Duration: 1 * time.Hour, IgnoreElementID: true}),
	g.Entry("hybrid index", helpers.Args{Input: "hybrid_index", Duration: 1 * time.Hour, IgnoreElementID: true}),
	g.Entry("err in arr", helpers.Args{Input: "err_in_arr", Duration: 1 * time.Hour, WantErr: true}),
)
