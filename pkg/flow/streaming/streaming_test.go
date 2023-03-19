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
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/apache/skywalking-banyandb/pkg/flow"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	flowTest "github.com/apache/skywalking-banyandb/pkg/test/flow"
)

// numberRange generates a slice with `count` number of integers starting from `begin`,
// i.e. [begin, begin + count).
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
		snk   *slice
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
			snk = newSlice()
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
				}, flags.EventuallyTimeout).Should(Succeed())
			})
		})
	})

	Context("With Mapper operator", func() {
		var (
			mapper flow.UnaryFunc[any]

			input = flowTest.NewSlice(numberRange(0, 10))
		)

		JustBeforeEach(func() {
			snk = newSlice()
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
				}, flags.EventuallyTimeout).Should(Succeed())
			})
		})
	})

	Context("With TopN operator order by ASC", func() {
		type record struct {
			service  string
			instance string
			value    int
		}

		var input []flow.StreamRecord

		JustBeforeEach(func() {
			snk = newSlice()

			f = New(flowTest.NewSlice(input)).
				Map(flow.UnaryFunc[any](func(ctx context.Context, item interface{}) interface{} {
					// groupBy
					return flow.Data{item.(*record).service, int64(item.(*record).value)}
				})).
				Window(NewTumblingTimeWindows(15*time.Second)).
				TopN(3, WithSortKeyExtractor(func(record flow.StreamRecord) int64 {
					return record.Data().(flow.Data)[1].(int64)
				}), OrderBy(ASC), WithGroupKeyExtractor(func(record flow.StreamRecord) string {
					return record.Data().(flow.Data)[0].(string)
				})).
				To(snk)

			errCh = f.Open()
			Expect(errCh).ShouldNot(BeNil())
		})

		When("Bottom3", func() {
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

			It("Should take bottom 3 elements", func() {
				Eventually(func(g Gomega) {
					g.Expect(len(snk.Value())).Should(BeNumerically(">=", 1))
					// e2e-service-consumer Group
					g.Expect(snk.Value()[0].(flow.StreamRecord).Data().(map[string][]*Tuple2)["e2e-service-consumer"]).Should(BeEquivalentTo([]*Tuple2{
						{int64(9500), flow.NewStreamRecord(flow.Data{"e2e-service-consumer", int64(9500)}, 7000)},
						{int64(9600), flow.NewStreamRecord(flow.Data{"e2e-service-consumer", int64(9600)}, 6000)},
						{int64(9700), flow.NewStreamRecord(flow.Data{"e2e-service-consumer", int64(9700)}, 4000)},
					}))
					// e2e-service-provider Group
					g.Expect(snk.Value()[0].(flow.StreamRecord).Data().(map[string][]*Tuple2)["e2e-service-provider"]).Should(BeEquivalentTo([]*Tuple2{
						{int64(9700), flow.NewStreamRecord(flow.Data{"e2e-service-provider", int64(9700)}, 5000)},
						{int64(9800), flow.NewStreamRecord(flow.Data{"e2e-service-provider", int64(9800)}, 3000)},
						{int64(10000), flow.NewStreamRecord(flow.Data{"e2e-service-provider", int64(10000)}, 1000)},
					}))
				}).WithTimeout(flags.EventuallyTimeout).Should(Succeed())
			})
		})
	})

	Context("With TopN operator order by DESC", func() {
		type record struct {
			service  string
			instance string
			value    int
		}

		var input []flow.StreamRecord

		JustBeforeEach(func() {
			snk = newSlice()

			f = New(flowTest.NewSlice(input)).
				Map(flow.UnaryFunc[any](func(ctx context.Context, item interface{}) interface{} {
					// groupBy
					return flow.Data{item.(*record).service, int64(item.(*record).value)}
				})).
				Window(NewTumblingTimeWindows(15*time.Second)).
				TopN(3, WithSortKeyExtractor(func(record flow.StreamRecord) int64 {
					return record.Data().(flow.Data)[1].(int64)
				}), WithGroupKeyExtractor(func(record flow.StreamRecord) string {
					return record.Data().(flow.Data)[0].(string)
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
					g.Expect(len(snk.Value())).Should(BeNumerically(">=", 1))
					// e2e-service-consumer Group
					g.Expect(snk.Value()[0].(flow.StreamRecord).Data().(map[string][]*Tuple2)["e2e-service-consumer"]).Should(BeEquivalentTo([]*Tuple2{
						{int64(9900), flow.NewStreamRecord(flow.Data{"e2e-service-consumer", int64(9900)}, 2000)},
						{int64(9700), flow.NewStreamRecord(flow.Data{"e2e-service-consumer", int64(9700)}, 4000)},
						{int64(9600), flow.NewStreamRecord(flow.Data{"e2e-service-consumer", int64(9600)}, 6000)},
					}))
					// e2e-service-provider Group
					g.Expect(snk.Value()[0].(flow.StreamRecord).Data().(map[string][]*Tuple2)["e2e-service-provider"]).Should(BeEquivalentTo([]*Tuple2{
						{int64(10000), flow.NewStreamRecord(flow.Data{"e2e-service-provider", int64(10000)}, 1000)},
						{int64(9800), flow.NewStreamRecord(flow.Data{"e2e-service-provider", int64(9800)}, 3000)},
						{int64(9700), flow.NewStreamRecord(flow.Data{"e2e-service-provider", int64(9700)}, 5000)},
					}))
				}).WithTimeout(flags.EventuallyTimeout).Should(Succeed())
			})
		})
	})
})

var _ flow.Sink = (*slice)(nil)

type slice struct {
	in    chan flow.StreamRecord
	slice []interface{}
	flow.ComponentState
	sync.RWMutex
}

func newSlice() *slice {
	return &slice{
		slice: make([]interface{}, 0),
		in:    make(chan flow.StreamRecord),
	}
}

func (s *slice) Value() []interface{} {
	s.RLock()
	defer s.RUnlock()
	return s.slice
}

func (s *slice) In() chan<- flow.StreamRecord {
	return s.in
}

func (s *slice) Setup(ctx context.Context) error {
	go s.run(ctx)

	return nil
}

func (s *slice) run(ctx context.Context) {
	s.Add(1)
	defer func() {
		s.Done()
	}()
	for {
		select {
		case item, ok := <-s.in:
			if !ok {
				return
			}
			s.Lock()
			s.slice = append(s.slice, item)
			s.Unlock()
		case <-ctx.Done():
			return
		}
	}
}

func (s *slice) Teardown(_ context.Context) error {
	s.Wait()
	return nil
}
