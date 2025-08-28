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

var _ = g.FDescribeTable("Scanning Traces", func(args helpers.Args) {
	gm.Eventually(func(innerGm gm.Gomega) {
		verify(innerGm, args)
	}, flags.EventuallyTimeout).Should(gm.Succeed())
},
	g.Entry("all elements", helpers.Args{Input: "all", Duration: 1 * time.Hour}),
)
