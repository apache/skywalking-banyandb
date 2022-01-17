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
	"io/ioutil"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

var _ = Describe("Shard", func() {
	Describe("Generate segments", func() {
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
		It("generates several segments", func() {
			var err error
			shard, err = tsdb.OpenShard(context.TODO(), common.ShardID(0), tmp, tsdb.IntervalRule{
				Unit: tsdb.MILLISECOND,
				Num:  1000,
			})
			Expect(err).NotTo(HaveOccurred())
			Eventually(func() int {
				files, err := ioutil.ReadDir(tmp + "/shard-0")
				Expect(err).NotTo(HaveOccurred())
				num := 0
				for _, fi := range files {
					if fi.IsDir() && strings.HasPrefix(fi.Name(), "seg-") {
						num++
					}
				}
				return num
			}).WithTimeout(10 * time.Second).Should(BeNumerically(">=", 3))
		})

	})
})
