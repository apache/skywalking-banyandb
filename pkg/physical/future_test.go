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

package physical_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/apache/skywalking-banyandb/pkg/physical"
)

var _ = Describe("Future", func() {
	It("should run single future", func() {
		f := physical.NewFuture(func() physical.Result {
			return physical.Success(physical.NewChunkIDs(1, 2, 3))
		})

		Eventually(func() bool {
			return f.IsComplete()
		}).Should(BeTrue())
		Eventually(func() physical.Data {
			return f.Value().Success()
		}).Should(BeEquivalentTo(physical.NewChunkIDs(1, 2, 3)))
	})

	It("should return error if panic", func() {
		f := physical.NewFuture(func() physical.Result {
			panic("panic in future")
		})

		Eventually(func() bool {
			return f.IsComplete()
		}).Should(BeTrue())
		Eventually(func() error {
			return f.Value().Error()
		}).Should(HaveOccurred())
	})
})
