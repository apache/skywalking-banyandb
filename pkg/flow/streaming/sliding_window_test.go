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
	"container/heap"
	"context"
	"time"

	g "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	"github.com/apache/skywalking-banyandb/pkg/flow"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
)

var _ flow.AggregationOp = (*intSumAggregator)(nil)

type intSumAggregator struct {
	sum   int
	dirty bool
}

func (i *intSumAggregator) Add(input []flow.StreamRecord) {
	for _, item := range input {
		i.sum += item.Data().(int)
	}
	if len(input) > 0 {
		i.dirty = true
	}
}

func (i *intSumAggregator) Snapshot() interface{} {
	i.dirty = false
	return i.sum
}

func (i *intSumAggregator) Dirty() bool {
	return i.dirty
}

var _ = g.Describe("Sliding Window", func() {
	var (
		baseTS         time.Time
		snk            *slice
		input          []flow.StreamRecord
		slidingWindows *tumblingTimeWindows

		aggrFactory = func() flow.AggregationOp {
			return &intSumAggregator{}
		}
	)

	g.BeforeEach(func() {
		baseTS = time.Now()
	})

	g.JustBeforeEach(func() {
		snk = newSlice()

		slidingWindows = NewTumblingTimeWindows(time.Second*15, time.Second*15).(*tumblingTimeWindows)
		slidingWindows.aggregationFactory = aggrFactory
		slidingWindows.windowCount = 2
		slidingWindows.l = logger.GetLogger("tumblingTimeWindows")

		gomega.Expect(slidingWindows.Setup(context.TODO())).Should(gomega.Succeed())
		gomega.Expect(snk.Setup(context.TODO())).Should(gomega.Succeed())
		slidingWindows.Exec(snk)
		for _, r := range input {
			slidingWindows.In() <- r
		}
	})

	g.AfterEach(func() {
		close(slidingWindows.in)
		gomega.Expect(slidingWindows.Teardown(context.TODO())).Should(gomega.Succeed())
	})

	g.When("input a single element", func() {
		g.BeforeEach(func() {
			input = []flow.StreamRecord{
				flow.NewStreamRecord(1, baseTS.UnixMilli()),
			}
		})

		g.It("Should not trigger", func() {
			gomega.Eventually(func(g gomega.Gomega) {
				g.Expect(snk.Value()).Should(gomega.BeEmpty())
			}).WithTimeout(flags.EventuallyTimeout).Should(gomega.Succeed())
		})
	})

	g.When("input two elements within the same bucket", func() {
		g.BeforeEach(func() {
			baseTS = time.Unix(baseTS.Unix()-baseTS.Unix()%15, 0)
			input = []flow.StreamRecord{
				flow.NewStreamRecord(1, baseTS.UnixMilli()),
				flow.NewStreamRecord(2, baseTS.Add(time.Second*5).UnixMilli()),
			}
		})

		g.It("Should not trigger", func() {
			gomega.Eventually(func(g gomega.Gomega) {
				g.Expect(snk.Value()).Should(gomega.BeEmpty())
			}).WithTimeout(flags.EventuallyTimeout).Should(gomega.Succeed())
		})
	})

	g.When("input two elements within adjacent buckets", func() {
		g.BeforeEach(func() {
			baseTS = time.Unix(baseTS.Unix()-baseTS.Unix()%15+14, 0)
			input = []flow.StreamRecord{
				flow.NewStreamRecord(1, baseTS.UnixMilli()),
				flow.NewStreamRecord(2, baseTS.Add(time.Second*5).UnixMilli()),
			}
		})

		g.It("Should trigger once due to the expiry", func() {
			gomega.Eventually(func(g gomega.Gomega) {
				g.Expect(snk.Value()).Should(gomega.HaveLen(1))
			}).WithTimeout(flags.EventuallyTimeout).Should(gomega.Succeed())
		})
	})

	g.Describe("Timer Heap Deduplication", func() {
		var timerHeap *flow.DedupPriorityQueue

		g.BeforeEach(func() {
			timerHeap = flow.NewPriorityQueue(func(a, b interface{}) int {
				return int(a.(*internalTimer).w.MaxTimestamp() - b.(*internalTimer).w.MaxTimestamp())
			}, false)
		})

		g.It("Should deduplicate same reference internalTimer objects", func() {
			timer1 := &internalTimer{
				w: timeWindow{start: 1000, end: 2000},
			}

			// Push the same reference twice
			heap.Push(timerHeap, timer1)
			heap.Push(timerHeap, timer1)

			// Should only have one item due to reference-based deduplication
			gomega.Expect(timerHeap.Len()).Should(gomega.Equal(1))
		})

		g.It("Should deduplicate different internalTimer objects with same window content", func() {
			timer1 := &internalTimer{
				w: timeWindow{start: 1000, end: 2000},
			}
			timer2 := &internalTimer{
				w: timeWindow{start: 1000, end: 2000}, // Same window content
			}

			// Push different objects with same content
			heap.Push(timerHeap, timer1)
			heap.Push(timerHeap, timer2)

			// Should only have one item due to content-based deduplication
			gomega.Expect(timerHeap.Len()).Should(gomega.Equal(1))
		})

		g.It("Should not deduplicate internalTimer objects with different windows", func() {
			timer1 := &internalTimer{
				w: timeWindow{start: 1000, end: 2000},
			}
			timer2 := &internalTimer{
				w: timeWindow{start: 2000, end: 3000}, // Different window
			}

			// Push different objects with different content
			heap.Push(timerHeap, timer1)
			heap.Push(timerHeap, timer2)

			// Should have two items as they have different content
			gomega.Expect(timerHeap.Len()).Should(gomega.Equal(2))
		})

		g.It("Should maintain proper ordering after deduplication", func() {
			timer1 := &internalTimer{
				w: timeWindow{start: 3000, end: 4000}, // Later timestamp
			}
			timer2 := &internalTimer{
				w: timeWindow{start: 1000, end: 2000}, // Earlier timestamp
			}
			timer3 := &internalTimer{
				w: timeWindow{start: 1000, end: 2000}, // Duplicate of timer2
			}

			// Push in order: later, earlier, duplicate
			heap.Push(timerHeap, timer1)
			heap.Push(timerHeap, timer2)
			heap.Push(timerHeap, timer3) // Should be deduplicated

			// Should only have 2 items
			gomega.Expect(timerHeap.Len()).Should(gomega.Equal(2))

			// Peek should return the earliest timer (timer2)
			earliest := timerHeap.Peek().(*internalTimer)
			gomega.Expect(earliest.w.start).Should(gomega.Equal(int64(1000)))
			gomega.Expect(earliest.w.end).Should(gomega.Equal(int64(2000)))
		})

		g.It("Should verify Hash and Equal methods work correctly", func() {
			timer1 := &internalTimer{
				w: timeWindow{start: 1000, end: 2000},
			}
			timer2 := &internalTimer{
				w: timeWindow{start: 1000, end: 2000}, // Same content
			}
			timer3 := &internalTimer{
				w: timeWindow{start: 1000, end: 3000}, // Different end
			}

			// Test Hash method
			gomega.Expect(timer1.Hash()).Should(gomega.Equal(timer2.Hash()))
			gomega.Expect(timer1.Hash()).ShouldNot(gomega.Equal(timer3.Hash()))

			// Test Equal method
			gomega.Expect(timer1.Equal(timer2)).Should(gomega.BeTrue())
			gomega.Expect(timer1.Equal(timer3)).Should(gomega.BeFalse())
		})

		g.It("Should work with heap operations", func() {
			timer1 := &internalTimer{
				w: timeWindow{start: 3000, end: 4000}, // Later
			}
			timer2 := &internalTimer{
				w: timeWindow{start: 1000, end: 2000}, // Earlier
			}
			timer3 := &internalTimer{
				w: timeWindow{start: 1000, end: 2000}, // Duplicate of timer2
			}

			// Initialize heap
			heap.Init(timerHeap)

			// Push timers
			heap.Push(timerHeap, timer1)
			heap.Push(timerHeap, timer2)
			heap.Push(timerHeap, timer3) // Should be deduplicated

			// Should only have 2 items
			gomega.Expect(timerHeap.Len()).Should(gomega.Equal(2))

			// Pop should return earliest first
			earliest := heap.Pop(timerHeap).(*internalTimer)
			gomega.Expect(earliest.w.start).Should(gomega.Equal(int64(1000)))
			gomega.Expect(earliest.w.end).Should(gomega.Equal(int64(2000)))

			// Next should be the later timer
			later := heap.Pop(timerHeap).(*internalTimer)
			gomega.Expect(later.w.start).Should(gomega.Equal(int64(3000)))
			gomega.Expect(later.w.end).Should(gomega.Equal(int64(4000)))

			// Heap should be empty now
			gomega.Expect(timerHeap.Len()).Should(gomega.Equal(0))
		})
	})
})
