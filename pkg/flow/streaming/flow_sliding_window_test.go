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

package streaming

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/apache/skywalking-banyandb/pkg/flow"
	"github.com/apache/skywalking-banyandb/pkg/flow/streaming/sink"
)

var _ = Describe("Sliding Window", func() {
	var (
		num            int
		baseTs         time.Time
		snk            *sink.Slice
		input          []flow.StreamRecord
		slidingWindows *SlidingTimeWindows

		aggrFunc flow.AggregateFunction = func(i []interface{}) interface{} {
			num++
			return nil
		}
	)

	JustBeforeEach(func() {
		num = 0
		snk = sink.NewSlice()
		baseTs = time.Now()
		slidingWindows = NewSlidingTimeWindows(time.Minute*1, time.Second*15)
		slidingWindows.Aggregate(aggrFunc)
		Expect(slidingWindows.Setup(context.TODO())).Should(Succeed())
		Expect(snk.Setup(context.TODO())).Should(Succeed())
		slidingWindows.Exec(snk)
		for _, r := range input {
			slidingWindows.In() <- r
		}
	})

	AfterEach(func() {
		close(slidingWindows.in)
		Expect(slidingWindows.Teardown(context.TODO())).Should(Succeed())
	})

	When("input a single element", func() {
		BeforeEach(func() {
			input = []flow.StreamRecord{
				flow.NewStreamRecord(1, baseTs.UnixMilli()),
			}
		})

		It("Should receive nothing", func() {
			Consistently(func(g Gomega) {
				g.Expect(num).Should(Equal(0))
			})
		})
	})

	When("input two elements", func() {
		BeforeEach(func() {
			input = []flow.StreamRecord{
				flow.NewStreamRecord(1, baseTs.UnixMilli()),
				flow.NewStreamRecord(2, baseTs.Add(time.Minute*1).UnixMilli()),
			}
		})

		It("Should trigger once", func() {
			Consistently(func(g Gomega) {
				g.Expect(snk.Value()).Should(HaveLen(1))
			}).Should(Succeed())
		})
	})
})
