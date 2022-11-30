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

// Package measure_test contains integration test cases of the measure
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
	// SharedContext is the parallel execution context
	SharedContext helpers.SharedContext
	verify        = func(args helpers.Args) {
		gm.Eventually(func(innerGm gm.Gomega) {
			measureTestData.VerifyFn(innerGm, SharedContext, args)
		}, flags.EventuallyTimeout).Should(gm.Succeed())
	}
)

var _ = g.DescribeTable("Scanning Measures", verify,
	g.Entry("all", helpers.Args{Input: "all", Duration: 25 * time.Minute, Offset: -20 * time.Minute}),
	g.Entry("filter by tag", helpers.Args{Input: "tag_filter", Duration: 25 * time.Minute, Offset: -20 * time.Minute}),
	g.Entry("filter by a integer tag", helpers.Args{Input: "tag_filter_int", Duration: 25 * time.Minute, Offset: -20 * time.Minute}),
	g.Entry("filter by an unknown tag", helpers.Args{Input: "tag_filter_unknown", Duration: 25 * time.Minute, Offset: -20 * time.Minute, WantEmpty: true}),
	g.Entry("group and max", helpers.Args{Input: "group_max", Duration: 25 * time.Minute, Offset: -20 * time.Minute}),
	g.Entry("group without field", helpers.Args{Input: "group_no_field", Duration: 25 * time.Minute, Offset: -20 * time.Minute}),
	g.Entry("top 2", helpers.Args{Input: "top", Duration: 25 * time.Minute, Offset: -20 * time.Minute}),
	g.Entry("bottom 2", helpers.Args{Input: "bottom", Duration: 25 * time.Minute, Offset: -20 * time.Minute}),
	g.Entry("order by time asc", helpers.Args{Input: "order_asc", Duration: 25 * time.Minute, Offset: -20 * time.Minute}),
	g.Entry("order by time desc", helpers.Args{Input: "order_desc", Duration: 25 * time.Minute, Offset: -20 * time.Minute}),
	g.Entry("limit 3,2", helpers.Args{Input: "limit", Duration: 25 * time.Minute, Offset: -20 * time.Minute}),
	g.Entry("match a node", helpers.Args{Input: "match_node", Duration: 25 * time.Minute, Offset: -20 * time.Minute}),
	g.Entry("match nodes", helpers.Args{Input: "match_nodes", Duration: 25 * time.Minute, Offset: -20 * time.Minute}),
	g.Entry("filter by entity id", helpers.Args{Input: "entity", Duration: 25 * time.Minute, Offset: -20 * time.Minute}),
	g.Entry("filter by entity id and service id", helpers.Args{Input: "entity_service", Want: "entity", Duration: 25 * time.Minute, Offset: -20 * time.Minute}),
	g.Entry("without field", helpers.Args{Input: "no_field", Duration: 25 * time.Minute, Offset: -20 * time.Minute}),
	g.Entry("invalid logical expression", helpers.Args{Input: "err_invalid_le", Duration: 25 * time.Minute, Offset: -20 * time.Minute, WantErr: true}),
	g.Entry("linked or expressions", helpers.Args{Input: "linked_or", Duration: 25 * time.Minute, Offset: -20 * time.Minute}),
)
