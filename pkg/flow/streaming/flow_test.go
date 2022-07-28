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
	flowTest "github.com/apache/skywalking-banyandb/pkg/test/flow"
)

// numberRange generates a slice with `count` number of integers starting from `begin`,
// i.e. [begin, begin + count)
func numberRange(begin, count int) []int {
	result := make([]int, 0)
	for i := 0; i < count; i++ {
		result = append(result, begin+i)
	}
	return result
}

var _ = Describe("Streaming", func() {
	var (
		f     flow.Flow
		snk   *sink.Slice
		errCh <-chan error
	)

	AfterEach(func() {
		Expect(f.Close()).Should(Succeed())
		Consistently(errCh).ShouldNot(Receive())
	})

	Context("With Filter operator", func() {
		var (
			filter flow.UnaryFunc[bool]

			input = flowTest.NewSlice(numberRange(0, 10))
		)

		JustBeforeEach(func() {
			snk = sink.NewSlice()
			f = New(input).
				Filter(filter).
				To(snk)
			errCh = f.Open()
			Expect(errCh).ShouldNot(BeNil())
		})

		When("Given a odd filter", func() {
			BeforeEach(func() {
				filter = func(ctx context.Context, i interface{}) bool {
					return i.(int)%2 == 0
				}
			})

			It("Should filter odd number", func() {
				Eventually(func(g Gomega) {
					g.Expect(snk.Value()).Should(Equal([]interface{}{
						flow.NewStreamRecordWithoutTS(0),
						flow.NewStreamRecordWithoutTS(2),
						flow.NewStreamRecordWithoutTS(4),
						flow.NewStreamRecordWithoutTS(6),
						flow.NewStreamRecordWithoutTS(8),
					}))
				}).Should(Succeed())
			})
		})
	})

	Context("With Mapper operator", func() {
		var (
			mapper flow.UnaryFunc[any]

			input = flowTest.NewSlice(numberRange(0, 10))
		)

		JustBeforeEach(func() {
			snk = sink.NewSlice()
			f = New(input).
				Map(mapper).
				To(snk)
			errCh = f.Open()
			Expect(errCh).ShouldNot(BeNil())
		})

		When("given a multiplier", func() {
			BeforeEach(func() {
				mapper = func(ctx context.Context, i interface{}) interface{} {
					return i.(int) * 2
				}
			})

			It("Should multiply by 2", func() {
				Eventually(func(g Gomega) {
					g.Expect(snk.Value()).Should(Equal([]interface{}{
						flow.NewStreamRecordWithoutTS(0),
						flow.NewStreamRecordWithoutTS(2),
						flow.NewStreamRecordWithoutTS(4),
						flow.NewStreamRecordWithoutTS(6),
						flow.NewStreamRecordWithoutTS(8),
						flow.NewStreamRecordWithoutTS(10),
						flow.NewStreamRecordWithoutTS(12),
						flow.NewStreamRecordWithoutTS(14),
						flow.NewStreamRecordWithoutTS(16),
						flow.NewStreamRecordWithoutTS(18),
					}))
				}).Should(Succeed())
			})
		})
	})

	Context("With TopN operator", func() {
		type record struct {
			service  string
			instance string
			value    int
		}

		var (
			input []flow.StreamRecord
		)

		JustBeforeEach(func() {
			snk = sink.NewSlice()

			f = New(flowTest.NewSlice(input)).
				Map(flow.UnaryFunc[any](func(ctx context.Context, item interface{}) interface{} {
					// groupBy
					return flow.Data{item.(*record).service, int64(item.(*record).value)}
				})).
				Window(NewSlidingTimeWindows(60*time.Second, 15*time.Second)).
				TopN(3, WithSortKeyExtractor(func(elem interface{}) int64 {
					return elem.(flow.Data)[1].(int64)
				})).
				To(snk)

			errCh = f.Open()
			Expect(errCh).ShouldNot(BeNil())
		})

		When("Top3", func() {
			BeforeEach(func() {
				input = []flow.StreamRecord{
					flow.NewStreamRecord(&record{"e2e-service-provider", "instance-001", 10000}, 1000),
					flow.NewStreamRecord(&record{"e2e-service-consumer", "instance-001", 9900}, 2000),
					flow.NewStreamRecord(&record{"e2e-service-provider", "instance-002", 9800}, 3000),
					flow.NewStreamRecord(&record{"e2e-service-consumer", "instance-002", 9700}, 4000),
					flow.NewStreamRecord(&record{"e2e-service-provider", "instance-003", 9700}, 5000),
					flow.NewStreamRecord(&record{"e2e-service-consumer", "instance-004", 9600}, 6000),
					flow.NewStreamRecord(&record{"e2e-service-consumer", "instance-001", 9500}, 7000),
					flow.NewStreamRecord(&record{"e2e-service-provider", "instance-002", 9800}, 61000),
				}
			})

			It("Should take top 3 elements", func() {
				Eventually(func(g Gomega) {
					g.Expect(snk.Value()).Should(Equal([]*Tuple2{
						{int64(9500), flow.Data{"e2e-service-consumer", int64(9500)}},
						{int64(9600), flow.Data{"e2e-service-consumer", int64(9600)}},
						{int64(9700), flow.Data{"e2e-service-consumer", int64(9700)}},
					}))
				}).Should(Succeed())
			})
		})
	})
})
