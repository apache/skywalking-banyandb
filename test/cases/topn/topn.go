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

// Package topn_test contains integration test cases of the TopN
package topn_test

import (
	"time"

	g "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"

	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	topNTestData "github.com/apache/skywalking-banyandb/test/cases/topn/data"
)

var (
	// SharedContext is the parallel execution context
	SharedContext helpers.SharedContext
	verify        = func(args helpers.Args) {
		gm.Eventually(func(innerGm gm.Gomega) {
			topNTestData.VerifyFn(innerGm, SharedContext, args)
		}, flags.EventuallyTimeout).WithTimeout(10 * time.Second).WithPolling(2 * time.Second).Should(gm.Succeed())
	}
)

var _ = g.DescribeTable("TopN Tests", verify,
	g.Entry("desc", helpers.Args{Input: "desc", Duration: 25 * time.Minute, Offset: -20 * time.Minute}),
	g.Entry("asc", helpers.Args{Input: "asc", Duration: 25 * time.Minute, Offset: -20 * time.Minute}),
	g.Entry("max top3 order by desc", helpers.Args{Input: "aggr_desc", Duration: 25 * time.Minute, Offset: -20 * time.Minute}),
	g.Entry("max top3 with condition order by desc", helpers.Args{Input: "condition_aggr_desc", Duration: 25 * time.Minute, Offset: -20 * time.Minute}),
)
