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

// Package property_test provides the property test cases.
package property_test

import (
	"time"

	g "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"

	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	propertyTestData "github.com/apache/skywalking-banyandb/test/cases/property/data"
)

var (
	// SharedContext is the parallel execution context.
	SharedContext helpers.SharedContext
	verify        = func(args helpers.Args) {
		gm.Eventually(func(innerGm gm.Gomega) {
			propertyTestData.VerifyFn(innerGm, SharedContext, args)
		}, flags.EventuallyTimeout).WithTimeout(10 * time.Second).WithPolling(2 * time.Second).Should(gm.Succeed())
	}
)

var _ = g.Describe("Property Tests", func() {
	g.It("property lifecycle", func() {
		for _, entry := range []Entry{
			{
				Desc: "create new property",
				Args: helpers.Args{Input: "create", Mode: helpers.TestModeCreate},
			},
			{
				Desc: "update first property",
				Args: helpers.Args{Input: "create", Mode: helpers.TestModeUpdate},
			},
			{
				Desc: "update first property with different value",
				Args: helpers.Args{Input: "update_value", Mode: helpers.TestModeUpdate},
			},
			{
				Desc: "create second property",
				Args: helpers.Args{Input: "create_other", Mode: helpers.TestModeCreate},
			},
			{
				Desc: "delete first property",
				Args: helpers.Args{Input: "delete_first", Mode: helpers.TestModeDelete},
			},
			{
				Desc: "select all after delete first property",
				Args: helpers.Args{Input: "after_delete_first", Mode: helpers.TestModeQuery},
			},
		} {
			g.By(entry.Desc)
			verify(entry.Args)
		}
	})
})

// Entry represents a test case entry with description and arguments.
type Entry struct {
	Desc string
	Args helpers.Args
}
