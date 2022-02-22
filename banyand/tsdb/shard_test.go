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
//
package tsdb_test

import (
	"context"
	"errors"
	"os"
	"path"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

var _ = Describe("Shard", func() {
	Describe("Generate segments and blocks", func() {
		var tmp string
		var deferFn func()
		var shard tsdb.Shard

		BeforeEach(func() {
			var err error
			tmp, deferFn, err = test.NewSpace()
			Expect(err).NotTo(HaveOccurred())
		})
		AfterEach(func() {
			shard.Close()
			deferFn()
		})
		It("generates several segments and blocks", func() {
			var err error
			shard, err = tsdb.OpenShard(context.TODO(), common.ShardID(0), tmp,
				tsdb.IntervalRule{
					Unit: tsdb.MILLISECOND,
					Num:  3000,
				},
				tsdb.IntervalRule{
					Unit: tsdb.MILLISECOND,
					Num:  1000,
				},
				1<<4,
			)
			Expect(err).NotTo(HaveOccurred())
			segDirectories := make([]string, 3)
			Eventually(func() int {
				num := 0
				err := tsdb.WalkDir(tmp+"/shard-0", "seg-", func(suffix, absolutePath string) error {
					if num < 3 {
						segDirectories[num] = absolutePath
					}
					num++
					return nil
				})
				Expect(err).NotTo(HaveOccurred())
				return num
			}).WithTimeout(30 * time.Second).Should(BeNumerically(">=", 3))
			for _, d := range segDirectories {
				Eventually(func() int {
					num := 0
					err := tsdb.WalkDir(d, "block-", func(suffix, absolutePath string) error {
						num++
						return nil
					})
					Expect(err).NotTo(HaveOccurred())
					return num
				}).WithTimeout(30 * time.Second).Should(BeNumerically(">=", 3))
			}
		})
		It("closes blocks", func() {
			var err error
			shard, err = tsdb.OpenShard(context.TODO(), common.ShardID(0), tmp,
				tsdb.IntervalRule{
					Unit: tsdb.DAY,
					Num:  1,
				},
				tsdb.IntervalRule{
					Unit: tsdb.MILLISECOND,
					Num:  1000,
				},
				2,
			)
			Expect(err).NotTo(HaveOccurred())
			var segDirectory string
			Eventually(func() int {
				num := 0
				errInternal := tsdb.WalkDir(tmp+"/shard-0", "seg-", func(suffix, absolutePath string) error {
					if num < 1 {
						segDirectory = absolutePath
					}
					num++
					return nil
				})
				Expect(errInternal).NotTo(HaveOccurred())
				return num
			}).WithTimeout(30 * time.Second).Should(BeNumerically(">=", 1))
			Eventually(func() int {
				num := 0
				errInternal := tsdb.WalkDir(segDirectory, "block-", func(suffix, absolutePath string) error {
					if _, err := os.Stat(path.Join(absolutePath, "store", "LOCK")); errors.Is(err, os.ErrNotExist) {
						num++
					}
					return nil
				})
				Expect(errInternal).NotTo(HaveOccurred())
				return num
			}).WithTimeout(30 * time.Second).Should(BeNumerically(">=", 1))
		})
		It("reopens closed blocks", func() {
			var err error
			shard, err = tsdb.OpenShard(context.TODO(), common.ShardID(0), tmp,
				tsdb.IntervalRule{
					Unit: tsdb.MILLISECOND,
					Num:  3000,
				},
				tsdb.IntervalRule{
					Unit: tsdb.MILLISECOND,
					Num:  1000,
				},
				2,
			)
			Expect(err).NotTo(HaveOccurred())
			Eventually(func() int {
				num := 0
				for _, bs := range shard.State().OpenedBlocks {
					if !bs.Closed {
						num++
					}
				}
				return num
			}).WithTimeout(30 * time.Second).Should(BeNumerically(">=", 2))
			var closedBlocks []tsdb.BlockState
			Eventually(func() int {
				closedBlocks = nil
				for _, ob := range shard.State().OpenedBlocks {
					if ob.Closed {
						closedBlocks = append(closedBlocks, ob)
					}
				}
				return len(closedBlocks)
			}).WithTimeout(30 * time.Second).Should(BeNumerically(">=", 1))
			series, err := shard.Series().GetByID(common.SeriesID(11))
			Expect(err).NotTo(HaveOccurred())
			writeFn := func(bs tsdb.BlockState) {
				span, err := series.Span(bs.TimeRange)
				Expect(err).NotTo(HaveOccurred())
				defer span.Close()
				writer, err := span.WriterBuilder().Family([]byte("test"), []byte("test")).Time(bs.TimeRange.Start).Build()
				Expect(err).NotTo(HaveOccurred())
				_, err = writer.Write()
				Expect(err).NotTo(HaveOccurred())
			}
			for _, bs := range closedBlocks {
				writeFn(bs)
			}
		})
	})
})
