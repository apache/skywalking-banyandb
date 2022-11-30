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
package tsdb_test

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gleak"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

var _ = Describe("Shard", func() {
	Describe("Generate segments and blocks", func() {
		var tmp string
		var deferFn func()
		var shard tsdb.Shard
		var clock timestamp.MockClock
		var goods []gleak.Goroutine

		BeforeEach(func() {
			goods = gleak.Goroutines()
			var err error
			tmp, deferFn, err = test.NewSpace()
			Expect(err).NotTo(HaveOccurred())
			clock = timestamp.NewMockClock()
			clock.Set(time.Date(1970, 0o1, 0o1, 0, 0, 0, 0, time.Local))
			Expect(err).NotTo(HaveOccurred())
		})
		AfterEach(func() {
			GinkgoWriter.Printf("shard state:%+v \n", shard.State())
			shard.Close()
			deferFn()
			Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
		})
		started := func(tasks ...string) {
			for _, task := range tasks {
				Eventually(func() bool {
					return shard.TriggerSchedule(task)
				}, flags.EventuallyTimeout).Should(BeTrue())
			}
		}
		forward := func(hours int, tasks ...string) {
			for i := 0; i < hours; i++ {
				clock.Add(1 * time.Hour)
				for _, task := range tasks {
					shard.TriggerSchedule(task)
				}
			}
		}
		It("generates several segments and blocks", func() {
			By("open 4 blocks")
			var err error
			shard, err = tsdb.OpenShard(timestamp.SetClock(context.Background(), clock), common.ShardID(0), tmp,
				tsdb.IntervalRule{
					Unit: tsdb.DAY,
					Num:  1,
				},
				tsdb.IntervalRule{
					Unit: tsdb.HOUR,
					Num:  12,
				},
				tsdb.IntervalRule{
					Unit: tsdb.DAY,
					Num:  7,
				},
				2,
				3,
			)
			Expect(err).NotTo(HaveOccurred())
			started("BlockID-19700101-1970010100-1", "SegID-19700101-1")
			By("01/01 00:00 1st block is opened")
			t1 := clock.Now()
			Eventually(func() []tsdb.BlockState {
				return shard.State().Blocks
			}, flags.EventuallyTimeout).Should(Equal([]tsdb.BlockState{
				{
					ID: tsdb.BlockID{
						SegID:   tsdb.GenerateInternalID(tsdb.DAY, 19700101),
						BlockID: tsdb.GenerateInternalID(tsdb.HOUR, 1970010100),
					},
					TimeRange: timestamp.NewTimeRangeDuration(t1, 12*time.Hour, true, false),
				},
			}))
			By("01/01 11:00 2nd block is opened")
			forward(11, "BlockID-19700101-1970010100-1", "SegID-19700101-1")
			t2 := clock.Now().Add(1 * time.Hour)
			Eventually(func() []tsdb.BlockState {
				return shard.State().Blocks
			}, flags.EventuallyTimeout).Should(Equal([]tsdb.BlockState{
				{
					ID: tsdb.BlockID{
						SegID:   tsdb.GenerateInternalID(tsdb.DAY, 19700101),
						BlockID: tsdb.GenerateInternalID(tsdb.HOUR, 1970010100),
					},
					TimeRange: timestamp.NewTimeRangeDuration(t1, 12*time.Hour, true, false),
				},
				{
					ID: tsdb.BlockID{
						SegID:   tsdb.GenerateInternalID(tsdb.DAY, 19700101),
						BlockID: tsdb.GenerateInternalID(tsdb.HOUR, 1970010112),
					},
					TimeRange: timestamp.NewTimeRangeDuration(t2, 12*time.Hour, true, false),
				},
			}))
			Eventually(func() []tsdb.BlockID {
				return shard.State().OpenBlocks
			}, flags.EventuallyTimeout).Should(Equal([]tsdb.BlockID{}))
			By("01/01 13:00 moves to the 2nd block")
			forward(2, "BlockID-19700101-1970010100-1", "SegID-19700101-1")
			started("BlockID-19700101-1970010112-1")
			Eventually(func() []tsdb.BlockID {
				return shard.State().OpenBlocks
			}, flags.EventuallyTimeout).Should(Equal([]tsdb.BlockID{
				{
					SegID:   tsdb.GenerateInternalID(tsdb.DAY, 19700101),
					BlockID: tsdb.GenerateInternalID(tsdb.HOUR, 1970010100),
				},
			}))
			By("01/01 23:00 3rd block is opened")
			forward(10, "BlockID-19700101-1970010112-1", "SegID-19700101-1")
			t3 := clock.Now().Add(1 * time.Hour)
			Eventually(func() []tsdb.BlockState {
				return shard.State().Blocks
			}, flags.EventuallyTimeout).Should(Equal([]tsdb.BlockState{
				{
					ID: tsdb.BlockID{
						SegID:   tsdb.GenerateInternalID(tsdb.DAY, 19700101),
						BlockID: tsdb.GenerateInternalID(tsdb.HOUR, 1970010100),
					},
					TimeRange: timestamp.NewTimeRangeDuration(t1, 12*time.Hour, true, false),
				},
				{
					ID: tsdb.BlockID{
						SegID:   tsdb.GenerateInternalID(tsdb.DAY, 19700101),
						BlockID: tsdb.GenerateInternalID(tsdb.HOUR, 1970010112),
					},
					TimeRange: timestamp.NewTimeRangeDuration(t2, 12*time.Hour, true, false),
				},
				{
					ID: tsdb.BlockID{
						SegID:   tsdb.GenerateInternalID(tsdb.DAY, 19700102),
						BlockID: tsdb.GenerateInternalID(tsdb.HOUR, 1970010200),
					},
					TimeRange: timestamp.NewTimeRangeDuration(t3, 12*time.Hour, true, false),
				},
			}))
			By("01/02 01:00 moves to 3rd block")
			forward(2, "BlockID-19700101-1970010112-1", "SegID-19700101-1")
			started("BlockID-19700102-1970010200-1", "SegID-19700102-1")
			Eventually(func() []tsdb.BlockID {
				if clock.TriggerTimer() {
					GinkgoWriter.Println("01/02 01:00 has been triggered")
				}
				return shard.State().OpenBlocks
			}, flags.EventuallyTimeout).Should(Equal([]tsdb.BlockID{
				{
					SegID:   tsdb.GenerateInternalID(tsdb.DAY, 19700101),
					BlockID: tsdb.GenerateInternalID(tsdb.HOUR, 1970010100),
				},
				{
					SegID:   tsdb.GenerateInternalID(tsdb.DAY, 19700101),
					BlockID: tsdb.GenerateInternalID(tsdb.HOUR, 1970010112),
				},
			}))
			By("01/02 11:00 4th block is opened")
			forward(10, "BlockID-19700102-1970010200-1", "SegID-19700102-1")
			t4 := clock.Now().Add(1 * time.Hour)
			Eventually(func() []tsdb.BlockState {
				if clock.TriggerTimer() {
					GinkgoWriter.Println("01/02 10:00 has been triggered")
				}
				return shard.State().Blocks
			}, flags.EventuallyTimeout).Should(Equal([]tsdb.BlockState{
				{
					ID: tsdb.BlockID{
						SegID:   tsdb.GenerateInternalID(tsdb.DAY, 19700101),
						BlockID: tsdb.GenerateInternalID(tsdb.HOUR, 1970010100),
					},
					TimeRange: timestamp.NewTimeRangeDuration(t1, 12*time.Hour, true, false),
				},
				{
					ID: tsdb.BlockID{
						SegID:   tsdb.GenerateInternalID(tsdb.DAY, 19700101),
						BlockID: tsdb.GenerateInternalID(tsdb.HOUR, 1970010112),
					},
					TimeRange: timestamp.NewTimeRangeDuration(t2, 12*time.Hour, true, false),
				},
				{
					ID: tsdb.BlockID{
						SegID:   tsdb.GenerateInternalID(tsdb.DAY, 19700102),
						BlockID: tsdb.GenerateInternalID(tsdb.HOUR, 1970010200),
					},
					TimeRange: timestamp.NewTimeRangeDuration(t3, 12*time.Hour, true, false),
				},
				{
					ID: tsdb.BlockID{
						SegID:   tsdb.GenerateInternalID(tsdb.DAY, 19700102),
						BlockID: tsdb.GenerateInternalID(tsdb.HOUR, 1970010212),
					},
					TimeRange: timestamp.NewTimeRangeDuration(t4, 12*time.Hour, true, false),
				},
			}))
			By("01/02 13:00 moves to 4th block")
			forward(2, "BlockID-19700102-1970010200-1", "SegID-19700102-1")
			started("BlockID-19700102-1970010212-1")
			Eventually(func() []tsdb.BlockID {
				return shard.State().OpenBlocks
			}, flags.EventuallyTimeout).Should(Equal([]tsdb.BlockID{
				{
					SegID:   tsdb.GenerateInternalID(tsdb.DAY, 19700101),
					BlockID: tsdb.GenerateInternalID(tsdb.HOUR, 1970010100),
				},
				{
					SegID:   tsdb.GenerateInternalID(tsdb.DAY, 19700101),
					BlockID: tsdb.GenerateInternalID(tsdb.HOUR, 1970010112),
				},
				{
					SegID:   tsdb.GenerateInternalID(tsdb.DAY, 19700102),
					BlockID: tsdb.GenerateInternalID(tsdb.HOUR, 1970010200),
				},
			}))
			By("01/02 23:00 5th block is opened")
			forward(10, "BlockID-19700102-1970010212-1", "SegID-19700102-1")
			t5 := clock.Now().Add(1 * time.Hour)
			Eventually(func() []tsdb.BlockState {
				return shard.State().Blocks
			}, flags.EventuallyTimeout).Should(Equal([]tsdb.BlockState{
				{
					ID: tsdb.BlockID{
						SegID:   tsdb.GenerateInternalID(tsdb.DAY, 19700101),
						BlockID: tsdb.GenerateInternalID(tsdb.HOUR, 1970010100),
					},
					TimeRange: timestamp.NewTimeRangeDuration(t1, 12*time.Hour, true, false),
				},
				{
					ID: tsdb.BlockID{
						SegID:   tsdb.GenerateInternalID(tsdb.DAY, 19700101),
						BlockID: tsdb.GenerateInternalID(tsdb.HOUR, 1970010112),
					},
					TimeRange: timestamp.NewTimeRangeDuration(t2, 12*time.Hour, true, false),
				},
				{
					ID: tsdb.BlockID{
						SegID:   tsdb.GenerateInternalID(tsdb.DAY, 19700102),
						BlockID: tsdb.GenerateInternalID(tsdb.HOUR, 1970010200),
					},
					TimeRange: timestamp.NewTimeRangeDuration(t3, 12*time.Hour, true, false),
				},
				{
					ID: tsdb.BlockID{
						SegID:   tsdb.GenerateInternalID(tsdb.DAY, 19700102),
						BlockID: tsdb.GenerateInternalID(tsdb.HOUR, 1970010212),
					},
					TimeRange: timestamp.NewTimeRangeDuration(t4, 12*time.Hour, true, false),
				},
				{
					ID: tsdb.BlockID{
						SegID:   tsdb.GenerateInternalID(tsdb.DAY, 19700103),
						BlockID: tsdb.GenerateInternalID(tsdb.HOUR, 1970010300),
					},
					TimeRange: timestamp.NewTimeRangeDuration(t5, 12*time.Hour, true, false),
				},
			}))
			By("01/03 01:00 close 1st block by adding 5th block")
			forward(2, "BlockID-19700102-1970010212-1", "SegID-19700102-1")
			started("BlockID-19700103-1970010300-1", "SegID-19700103-1")
			Eventually(func() []tsdb.BlockID {
				return shard.State().OpenBlocks
			}, flags.EventuallyTimeout).Should(Equal([]tsdb.BlockID{
				{
					SegID:   tsdb.GenerateInternalID(tsdb.DAY, 19700101),
					BlockID: tsdb.GenerateInternalID(tsdb.HOUR, 1970010112),
				},
				{
					SegID:   tsdb.GenerateInternalID(tsdb.DAY, 19700102),
					BlockID: tsdb.GenerateInternalID(tsdb.HOUR, 1970010200),
				},
				{
					SegID:   tsdb.GenerateInternalID(tsdb.DAY, 19700102),
					BlockID: tsdb.GenerateInternalID(tsdb.HOUR, 1970010212),
				},
			}))
			Eventually(func() []tsdb.BlockState {
				return shard.State().Blocks
			}, flags.EventuallyTimeout).Should(Equal([]tsdb.BlockState{
				{
					ID: tsdb.BlockID{
						SegID:   tsdb.GenerateInternalID(tsdb.DAY, 19700101),
						BlockID: tsdb.GenerateInternalID(tsdb.HOUR, 1970010100),
					},
					TimeRange: timestamp.NewTimeRangeDuration(t1, 12*time.Hour, true, false),
					Closed:    true,
				},
				{
					ID: tsdb.BlockID{
						SegID:   tsdb.GenerateInternalID(tsdb.DAY, 19700101),
						BlockID: tsdb.GenerateInternalID(tsdb.HOUR, 1970010112),
					},
					TimeRange: timestamp.NewTimeRangeDuration(t2, 12*time.Hour, true, false),
				},
				{
					ID: tsdb.BlockID{
						SegID:   tsdb.GenerateInternalID(tsdb.DAY, 19700102),
						BlockID: tsdb.GenerateInternalID(tsdb.HOUR, 1970010200),
					},
					TimeRange: timestamp.NewTimeRangeDuration(t3, 12*time.Hour, true, false),
				},
				{
					ID: tsdb.BlockID{
						SegID:   tsdb.GenerateInternalID(tsdb.DAY, 19700102),
						BlockID: tsdb.GenerateInternalID(tsdb.HOUR, 1970010212),
					},
					TimeRange: timestamp.NewTimeRangeDuration(t4, 12*time.Hour, true, false),
				},
				{
					ID: tsdb.BlockID{
						SegID:   tsdb.GenerateInternalID(tsdb.DAY, 19700103),
						BlockID: tsdb.GenerateInternalID(tsdb.HOUR, 1970010300),
					},
					TimeRange: timestamp.NewTimeRangeDuration(t5, 12*time.Hour, true, false),
				},
			}))
			By("reopen 1st block")
			series, err := shard.Series().GetByID(common.SeriesID(11))
			Expect(err).NotTo(HaveOccurred())
			t1Range := timestamp.NewInclusiveTimeRangeDuration(t1, 1*time.Hour)
			span, err := series.Span(context.Background(), t1Range)
			Expect(err).NotTo(HaveOccurred())
			defer span.Close()
			writer, err := span.WriterBuilder().Family([]byte("test"), []byte("test")).Time(t1Range.End).Build()
			Expect(err).NotTo(HaveOccurred())
			_, err = writer.Write()
			Expect(err).NotTo(HaveOccurred())
			Eventually(func() []tsdb.BlockID {
				return shard.State().OpenBlocks
			}, flags.EventuallyTimeout).Should(Equal([]tsdb.BlockID{
				{
					SegID:   tsdb.GenerateInternalID(tsdb.DAY, 19700101),
					BlockID: tsdb.GenerateInternalID(tsdb.HOUR, 1970010100),
				},
				{
					SegID:   tsdb.GenerateInternalID(tsdb.DAY, 19700102),
					BlockID: tsdb.GenerateInternalID(tsdb.HOUR, 1970010200),
				},
				{
					SegID:   tsdb.GenerateInternalID(tsdb.DAY, 19700102),
					BlockID: tsdb.GenerateInternalID(tsdb.HOUR, 1970010212),
				},
			}))
			Eventually(func() []tsdb.BlockState {
				return shard.State().Blocks
			}, flags.EventuallyTimeout).Should(Equal([]tsdb.BlockState{
				{
					ID: tsdb.BlockID{
						SegID:   tsdb.GenerateInternalID(tsdb.DAY, 19700101),
						BlockID: tsdb.GenerateInternalID(tsdb.HOUR, 1970010100),
					},
					TimeRange: timestamp.NewTimeRangeDuration(t1, 12*time.Hour, true, false),
				},
				{
					ID: tsdb.BlockID{
						SegID:   tsdb.GenerateInternalID(tsdb.DAY, 19700101),
						BlockID: tsdb.GenerateInternalID(tsdb.HOUR, 1970010112),
					},
					TimeRange: timestamp.NewTimeRangeDuration(t2, 12*time.Hour, true, false),
					Closed:    true,
				},
				{
					ID: tsdb.BlockID{
						SegID:   tsdb.GenerateInternalID(tsdb.DAY, 19700102),
						BlockID: tsdb.GenerateInternalID(tsdb.HOUR, 1970010200),
					},
					TimeRange: timestamp.NewTimeRangeDuration(t3, 12*time.Hour, true, false),
				},
				{
					ID: tsdb.BlockID{
						SegID:   tsdb.GenerateInternalID(tsdb.DAY, 19700102),
						BlockID: tsdb.GenerateInternalID(tsdb.HOUR, 1970010212),
					},
					TimeRange: timestamp.NewTimeRangeDuration(t4, 12*time.Hour, true, false),
				},
				{
					ID: tsdb.BlockID{
						SegID:   tsdb.GenerateInternalID(tsdb.DAY, 19700103),
						BlockID: tsdb.GenerateInternalID(tsdb.HOUR, 1970010300),
					},
					TimeRange: timestamp.NewTimeRangeDuration(t5, 12*time.Hour, true, false),
				},
			}))
		})
		It("retention", func() {
			var err error
			shard, err = tsdb.OpenShard(timestamp.SetClock(context.Background(), clock), common.ShardID(0), tmp,
				tsdb.IntervalRule{
					Unit: tsdb.DAY,
					Num:  1,
				},
				tsdb.IntervalRule{
					Unit: tsdb.HOUR,
					Num:  12,
				},
				tsdb.IntervalRule{
					Unit: tsdb.DAY,
					Num:  1,
				},
				10,
				15,
			)
			Expect(err).NotTo(HaveOccurred())
			started("BlockID-19700101-1970010100-1", "SegID-19700101-1", "retention")
			By("01/01 00:00 1st block is opened")
			t1 := clock.Now()
			By("01/01 11:00 2nd block is opened")
			forward(11, "BlockID-19700101-1970010100-1", "SegID-19700101-1", "retention")
			t2 := clock.Now().Add(1 * time.Hour)
			By("01/01 13:00 moves to the 2nd block")
			forward(2, "BlockID-19700101-1970010100-1", "SegID-19700101-1", "retention")
			started("BlockID-19700101-1970010112-1", "retention")
			By("01/01 23:00 3rd block is opened")
			forward(10, "BlockID-19700101-1970010112-1", "SegID-19700101-1", "retention")
			t3 := clock.Now().Add(1 * time.Hour)
			By("01/02 01:00 moves to 3rd block")
			forward(2, "BlockID-19700101-1970010112-1", "SegID-19700101-1", "retention")
			started("BlockID-19700102-1970010200-1", "SegID-19700102-1", "retention")
			By("01/02 11:00 4th block is opened")
			forward(10, "BlockID-19700102-1970010200-1", "SegID-19700102-1", "retention")
			t4 := clock.Now().Add(1 * time.Hour)

			Eventually(func() []tsdb.BlockState {
				return shard.State().Blocks
			}, flags.EventuallyTimeout).Should(Equal([]tsdb.BlockState{
				{
					ID: tsdb.BlockID{
						SegID:   tsdb.GenerateInternalID(tsdb.DAY, 19700101),
						BlockID: tsdb.GenerateInternalID(tsdb.HOUR, 1970010100),
					},
					TimeRange: timestamp.NewTimeRangeDuration(t1, 12*time.Hour, true, false),
				},
				{
					ID: tsdb.BlockID{
						SegID:   tsdb.GenerateInternalID(tsdb.DAY, 19700101),
						BlockID: tsdb.GenerateInternalID(tsdb.HOUR, 1970010112),
					},
					TimeRange: timestamp.NewTimeRangeDuration(t2, 12*time.Hour, true, false),
				},
				{
					ID: tsdb.BlockID{
						SegID:   tsdb.GenerateInternalID(tsdb.DAY, 19700102),
						BlockID: tsdb.GenerateInternalID(tsdb.HOUR, 1970010200),
					},
					TimeRange: timestamp.NewTimeRangeDuration(t3, 12*time.Hour, true, false),
				},
				{
					ID: tsdb.BlockID{
						SegID:   tsdb.GenerateInternalID(tsdb.DAY, 19700102),
						BlockID: tsdb.GenerateInternalID(tsdb.HOUR, 1970010212),
					},
					TimeRange: timestamp.NewTimeRangeDuration(t4, 12*time.Hour, true, false),
				},
			}))
			By("01/02 13:00 moves to 4th block")
			forward(2, "BlockID-19700102-1970010200-1", "SegID-19700102-1", "retention")
			started("BlockID-19700102-1970010212-1", "retention")
			By("01/02 23:00 5th block is opened")
			forward(10, "BlockID-19700102-1970010212-1", "SegID-19700102-1", "retention")
			t5 := clock.Now().Add(1 * time.Hour)
			By("01/03 01:00 close 1st block by adding 5th block")
			forward(2, "BlockID-19700102-1970010212-1", "SegID-19700102-1", "retention")
			started("BlockID-19700103-1970010300-1", "SegID-19700103-1", "retention")
			Eventually(func() []tsdb.BlockState {
				started("retention")
				return shard.State().Blocks
			}, flags.EventuallyTimeout).Should(Equal([]tsdb.BlockState{
				{
					ID: tsdb.BlockID{
						SegID:   tsdb.GenerateInternalID(tsdb.DAY, 19700102),
						BlockID: tsdb.GenerateInternalID(tsdb.HOUR, 1970010200),
					},
					TimeRange: timestamp.NewTimeRangeDuration(t3, 12*time.Hour, true, false),
				},
				{
					ID: tsdb.BlockID{
						SegID:   tsdb.GenerateInternalID(tsdb.DAY, 19700102),
						BlockID: tsdb.GenerateInternalID(tsdb.HOUR, 1970010212),
					},
					TimeRange: timestamp.NewTimeRangeDuration(t4, 12*time.Hour, true, false),
				},
				{
					ID: tsdb.BlockID{
						SegID:   tsdb.GenerateInternalID(tsdb.DAY, 19700103),
						BlockID: tsdb.GenerateInternalID(tsdb.HOUR, 1970010300),
					},
					TimeRange: timestamp.NewTimeRangeDuration(t5, 12*time.Hour, true, false),
				},
			}))
			Eventually(func() []tsdb.BlockID {
				return shard.State().OpenBlocks
			}, flags.EventuallyTimeout).Should(Equal([]tsdb.BlockID{
				{
					SegID:   tsdb.GenerateInternalID(tsdb.DAY, 19700102),
					BlockID: tsdb.GenerateInternalID(tsdb.HOUR, 1970010200),
				},
				{
					SegID:   tsdb.GenerateInternalID(tsdb.DAY, 19700102),
					BlockID: tsdb.GenerateInternalID(tsdb.HOUR, 1970010212),
				},
			}))
		})
		It("creates arbitrary blocks", func() {
			clock.Set(time.Date(1970, 0o1, 0o1, 1, 0, 0, 0, time.Local))
			By("open 1 block")
			var err error
			shard, err = tsdb.OpenShard(timestamp.SetClock(context.Background(), clock), common.ShardID(0), tmp,
				tsdb.IntervalRule{
					Unit: tsdb.DAY,
					Num:  1,
				},
				tsdb.IntervalRule{
					Unit: tsdb.HOUR,
					Num:  12,
				},
				tsdb.IntervalRule{
					Unit: tsdb.DAY,
					Num:  7,
				},
				2,
				3,
			)
			Expect(err).NotTo(HaveOccurred())
			started("BlockID-19700101-1970010101-1", "SegID-19700101-1", "retention")
			By("01/01 00:01 1st block is opened")
			t1 := clock.Now()
			Eventually(func() []tsdb.BlockState {
				return shard.State().Blocks
			}, flags.EventuallyTimeout).Should(Equal([]tsdb.BlockState{
				{
					ID: tsdb.BlockID{
						SegID:   tsdb.GenerateInternalID(tsdb.DAY, 19700101),
						BlockID: tsdb.GenerateInternalID(tsdb.HOUR, 1970010101),
					},
					TimeRange: timestamp.NewTimeRangeDuration(t1, 12*time.Hour, true, false),
				},
			}))
			By("01/01 12:00 2nd block is opened")
			forward(11, "BlockID-19700101-1970010101-1", "SegID-19700101-1")
			t2 := clock.Now().Add(1 * time.Hour)
			By("01/01 14:00 moves to the 2nd block")
			forward(2, "BlockID-19700101-1970010101-1", "SegID-19700101-1")
			started("BlockID-19700101-1970010113-1")
			Eventually(func() []tsdb.BlockState {
				return shard.State().Blocks
			}, flags.EventuallyTimeout).Should(Equal([]tsdb.BlockState{
				{
					ID: tsdb.BlockID{
						SegID:   tsdb.GenerateInternalID(tsdb.DAY, 19700101),
						BlockID: tsdb.GenerateInternalID(tsdb.HOUR, 1970010101),
					},
					TimeRange: timestamp.NewTimeRangeDuration(t1, 12*time.Hour, true, false),
				},
				{
					ID: tsdb.BlockID{
						SegID:   tsdb.GenerateInternalID(tsdb.DAY, 19700101),
						BlockID: tsdb.GenerateInternalID(tsdb.HOUR, 1970010113),
					},
					// The last block only takes 11 hours to align the segment's size
					TimeRange: timestamp.NewTimeRangeDuration(t2, 11*time.Hour, true, false),
				},
			}))
		})
	})
})
