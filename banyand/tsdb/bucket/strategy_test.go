// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
package bucket_test

import (
	"sync"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gleak"

	"github.com/apache/skywalking-banyandb/banyand/tsdb/bucket"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
)

var _ = Describe("Strategy", func() {
	BeforeEach(func() {
		goods := gleak.Goroutines()
		DeferCleanup(func() {
			Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
		})
	})
	Context("Applying the strategy", func() {
		var strategy *bucket.Strategy
		It("uses the golden settings", func() {
			ctrl := newController(2, 1)
			var err error
			strategy, err = bucket.NewStrategy(ctrl)
			Expect(err).NotTo(HaveOccurred())
			strategy.Run()
			Eventually(ctrl.isFull, flags.EventuallyTimeout).Should(BeTrue())
		})
		It("never reaches the limit", func() {
			ctrl := newController(1, 0)
			var err error
			strategy, err = bucket.NewStrategy(ctrl)
			Expect(err).NotTo(HaveOccurred())
			strategy.Run()
			Consistently(ctrl.isFull).ShouldNot(BeTrue())
		})
		It("exceeds the limit", func() {
			ctrl := newController(2, 3)
			var err error
			strategy, err = bucket.NewStrategy(ctrl)
			Expect(err).NotTo(HaveOccurred())
			strategy.Run()
			Eventually(ctrl.isFull, flags.EventuallyTimeout).Should(BeTrue())
		})
		It("'s first step exceeds the limit", func() {
			ctrl := newController(2, 15)
			var err error
			strategy, err = bucket.NewStrategy(ctrl)
			Expect(err).NotTo(HaveOccurred())
			strategy.Run()
			Eventually(ctrl.isFull, flags.EventuallyTimeout).Should(BeTrue())
		})
		AfterEach(func() {
			if strategy != nil {
				strategy.Close()
			}
		})
	})
	Context("Invalid parameter", func() {
		It("passes a ratio > 1.0", func() {
			ctrl := newController(2, 3)
			_, err := bucket.NewStrategy(ctrl, bucket.WithNextThreshold(1.1))
			Expect(err).To(MatchError(bucket.ErrInvalidParameter))
		})
	})
})

type controller struct {
	reporter    *reporter
	maxBuckets  int
	usedBuckets int
	capacity    int
	step        int
	mux         sync.RWMutex
}

func newController(maxBuckets, step int) *controller {
	ctrl := &controller{step: step, maxBuckets: maxBuckets, capacity: 10}
	ctrl.newReporter()
	return ctrl
}

func (c *controller) Next() (bucket.Reporter, error) {
	c.mux.Lock()
	defer c.mux.Unlock()
	if c.usedBuckets >= c.maxBuckets {
		return nil, bucket.ErrNoMoreBucket
	}
	c.usedBuckets++
	c.newReporter()
	return c.reporter, nil
}

func (c *controller) Current() (bucket.Reporter, error) {
	c.mux.RLock()
	defer c.mux.RUnlock()
	return c.reporter, nil
}

func (c *controller) OnMove(prev bucket.Reporter, next bucket.Reporter) {
}

func (c *controller) newReporter() {
	c.reporter = &reporter{step: c.step, capacity: c.capacity}
}

func (c *controller) isFull() bool {
	c.mux.RLock()
	defer c.mux.RUnlock()
	return c.usedBuckets >= c.maxBuckets
}

type reporter struct {
	capacity int
	step     int
}

func (r *reporter) Report() (bucket.Channel, error) {
	ch := make(bucket.Channel, r.capacity)
	go func() {
		var volume int
		for i := 0; i < r.capacity; i++ {
			volume += r.step
			ch <- bucket.Status{
				Capacity: r.capacity,
				Volume:   volume,
			}
		}
		close(ch)
	}()
	return ch, nil
}

func (r *reporter) String() string {
	return "default"
}
