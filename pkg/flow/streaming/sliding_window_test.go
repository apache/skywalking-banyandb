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

	g "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	"github.com/apache/skywalking-banyandb/pkg/flow"
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

		slidingWindows = NewTumblingTimeWindows(time.Second * 15).(*tumblingTimeWindows)
		slidingWindows.aggregationFactory = aggrFactory
		slidingWindows.windowCount = 2

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
})
