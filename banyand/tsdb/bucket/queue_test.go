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
	"context"
	"strconv"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gleak"

	"github.com/apache/skywalking-banyandb/banyand/tsdb/bucket"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

type queueEntryID struct {
	first  uint16
	second uint16
}

func (q queueEntryID) String() string {
	return strconv.Itoa(int(q.first))
}

func entryID(id uint16) queueEntryID {
	return queueEntryID{
		first:  id,
		second: id + 1,
	}
}

var _ = Describe("Queue", func() {
	var lock sync.Mutex
	var evictLst []queueEntryID
	var l bucket.Queue
	var clock timestamp.MockClock
	var scheduler *timestamp.Scheduler
	BeforeEach(func() {
		goods := gleak.Goroutines()
		DeferCleanup(func() {
			Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
		})
		evictLst = make([]queueEntryID, 0)
		clock = timestamp.NewMockClock()
		clock.Set(time.Date(1970, 0o1, 0o1, 0, 0, 0, 0, time.Local))
		scheduler = timestamp.NewScheduler(logger.GetLogger("queue-test"), clock)
		var err error
		l, err = bucket.NewQueue(logger.GetLogger("test"), 128, 192, scheduler, func(_ context.Context, id interface{}) error {
			lock.Lock()
			defer lock.Unlock()
			evictLst = append(evictLst, id.(queueEntryID))
			return nil
		})
		Expect(err).ShouldNot(HaveOccurred())
		DeferCleanup(func() {
			scheduler.Close()
			evictLst = evictLst[:0]
		})
	})
	It("pushes to recent", func() {
		enRecentSize := 0
		for i := 0; i < 256; i++ {
			Expect(l.Push(context.Background(), entryID(uint16(i)), func() error {
				enRecentSize++
				return nil
			})).To(Succeed())
		}
		Expect(enRecentSize).To(Equal(256))
		Expect(l.Len()).To(Equal(128))
		Expect(len(evictLst)).To(Equal(64))
		for i := 0; i < 64; i++ {
			Expect(evictLst[i]).To(Equal(entryID(uint16(i))))
		}
	})

	It("promotes to frequent", func() {
		enRecentSize := 0
		for i := 0; i < 128; i++ {
			Expect(l.Push(context.Background(), entryID(uint16(i)), func() error {
				enRecentSize++
				return nil
			})).To(Succeed())
		}
		Expect(enRecentSize).To(Equal(128))
		Expect(l.Len()).To(Equal(128))
		Expect(len(evictLst)).To(Equal(0))
		for i := 0; i < 64; i++ {
			Expect(l.Touch(entryID(uint16(i)))).To(BeTrue())
		}
		enRecentSize = 0
		for i := 128; i < 256; i++ {
			Expect(l.Push(context.Background(), entryID(uint16(i)), func() error {
				enRecentSize++
				return nil
			})).To(Succeed())
		}
		Expect(enRecentSize).To(Equal(128))
		Expect(l.Len()).To(Equal(128))
		Expect(len(evictLst)).To(Equal(64))
		for i := 0; i < 64; i++ {
			Expect(evictLst[i]).To(Equal(entryID(uint16(i + 64))))
		}
	})

	It("cleans up evict queue", func() {
		enRecentSize := 0
		for i := 0; i < 192; i++ {
			Expect(l.Push(context.Background(), entryID(uint16(i)), func() error {
				enRecentSize++
				return nil
			})).To(Succeed())
		}
		Expect(enRecentSize).To(Equal(192))
		Expect(l.Len()).To(Equal(128))
		Expect(len(evictLst)).To(Equal(0))
		clock.Add(6 * time.Minute)
		if !scheduler.Trigger(bucket.QueueName) {
			Fail("trigger fails")
		}
		Eventually(func() int {
			lock.Lock()
			defer lock.Unlock()
			return len(evictLst)
		}).WithTimeout(flags.EventuallyTimeout).Should(BeNumerically(">", 1))
	})
})
