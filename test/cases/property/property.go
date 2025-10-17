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

package property

import (
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	propertyTestData "github.com/apache/skywalking-banyandb/test/cases/property/data"
	g "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

var (
	// SharedContext is the parallel execution context.
	SharedContext helpers.SharedContext
	verify        = func(args helpers.Args) {
		gm.Eventually(func(innerGm gm.Gomega) {
			propertyTestData.VerifyFn(innerGm, SharedContext, args)
		}, flags.EventuallyTimeout).Should(gm.Succeed())
	}
)

var _ = g.DescribeTable("Scanning Properties", verify,
	g.Entry("all", helpers.Args{Input: "all"}),
	g.Entry("limit", helpers.Args{Input: "limit"}),
	g.Entry("query by criteria", helpers.Args{Input: "query_by_criteria"}),
	g.Entry("query by ids", helpers.Args{Input: "query_by_ids"}),
)
