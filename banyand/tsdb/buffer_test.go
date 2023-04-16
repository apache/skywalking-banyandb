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
	"bytes"
	"crypto/rand"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v3/skl"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gleak"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
)

var _ = Describe("Buffer", func() {
	var (
		buffer *tsdb.Buffer
		log    = logger.GetLogger("buffer-test")
		goods  []gleak.Goroutine
	)

	BeforeEach(func() {
		goods = gleak.Goroutines()
	})
	AfterEach(func() {
		Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
	})
	Context("Write and Read", func() {
		BeforeEach(func() {
			var err error
			buffer, err = tsdb.NewBuffer(log, common.Position{}, 1024*1024, 16, 4, func(shardIndex int, skl *skl.Skiplist) error {
				return nil
			})
			Expect(err).ToNot(HaveOccurred())
		})

		AfterEach(func() {
			buffer.Close()
		})
		It("should write and read data correctly", func() {
			var wg sync.WaitGroup
			wg.Add(100)

			for i := 0; i < 100; i++ {
				go func(idx int) {
					defer GinkgoRecover()
					defer wg.Done()

					key := []byte(fmt.Sprintf("key-%d", idx))
					value := []byte(fmt.Sprintf("value-%d", idx))
					ts := time.Now()

					buffer.Write(key, value, ts)
					Eventually(func(g Gomega) {
						readValue, ok := buffer.Read(key, ts)
						g.Expect(ok).To(BeTrue())
						g.Expect(bytes.Equal(value, readValue)).To(BeTrue())
					}, flags.EventuallyTimeout).Should(Succeed())
				}(i)
			}

			wg.Wait()
		})
	})

	Context("Flush", func() {
		It("should trigger flush when buffer size exceeds the limit", func() {
			numShards := 4
			doneChs := make([]chan struct{}, numShards)
			for i := 0; i < numShards; i++ {
				doneChs[i] = make(chan struct{})
			}

			onFlushFn := func(shardIndex int, skl *skl.Skiplist) error {
				if doneChs[shardIndex] == nil {
					return nil
				}
				close(doneChs[shardIndex])
				doneChs[shardIndex] = nil
				return nil
			}

			var wg sync.WaitGroup
			wg.Add(numShards)

			for _, ch := range doneChs {
				go func(c <-chan struct{}) {
					select {
					case res := <-c:
						GinkgoWriter.Printf("Received value: %d\n", res)
					case <-time.After(10 * time.Second):
						GinkgoWriter.Printf("Timeout")
					}
					wg.Done()
				}(ch)
			}

			buffer, err := tsdb.NewBuffer(log, common.Position{}, 1024, 16, numShards, onFlushFn)
			defer func() {
				_ = buffer.Close()
			}()
			Expect(err).ToNot(HaveOccurred())

			randInt := func() int {
				n, err := rand.Int(rand.Reader, big.NewInt(1000))
				if err != nil {
					return 0
				}
				return int(n.Int64())
			}
			for i := 0; i < 1000; i++ {
				key := fmt.Sprintf("key-%d", randInt())
				value := fmt.Sprintf("value-%d", randInt())
				ts := time.Now()

				buffer.Write([]byte(key), []byte(value), ts)
			}

			wg.Wait()
			for i, elem := range doneChs {
				if elem != nil {
					Fail(fmt.Sprintf("%d in doneChs is not nil", i))
				}
			}
		})
	})
})
