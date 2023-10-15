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
	"os"
	"path/filepath"
	"strconv"
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

var emptyFn = func(shardIndex int, skl *skl.Skiplist) error {
	return nil
}

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
			buffer, err = tsdb.NewBuffer(log, common.Position{}, 1024*1024, 16, 4)
			Expect(err).ToNot(HaveOccurred())
			buffer.Register("test", emptyFn)
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

			var err error
			buffer, err = tsdb.NewBuffer(log, common.Position{}, 1024, 16, numShards)
			defer func() {
				_ = buffer.Close()
			}()
			Expect(err).ToNot(HaveOccurred())
			buffer.Register("test", onFlushFn)

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

	Context("Write and Recover of wal correctly", func() {
		writeConcurrency := 2
		numShards := 2
		flushSize := 1024
		baseTime := time.Now()
		var path string

		BeforeEach(func() {
			var err error
			path, err = os.MkdirTemp("", "banyandb-test-buffer-wal-*")
			Expect(err).ToNot(HaveOccurred())
		})

		AfterEach(func() {
			err := os.RemoveAll(path)
			Expect(err).ToNot(HaveOccurred())
		})

		It("should write and rotate wal file correctly", func() {
			var err error
			var flushMutex sync.Mutex

			shardWalFileHistory := make(map[int][]string)
			buffer, err = tsdb.NewBufferWithWal(
				log,
				common.Position{},
				flushSize,
				writeConcurrency,
				numShards,
				true,
				path)
			Expect(err).ToNot(HaveOccurred())
			buffer.Register("test", func(shardIndex int, skl *skl.Skiplist) error {
				flushMutex.Lock()
				defer flushMutex.Unlock()

				shardWalDir := filepath.Join(path, "buffer-"+strconv.Itoa(shardIndex))
				var shardWalList []os.DirEntry
				shardWalList, err = os.ReadDir(shardWalDir)
				Expect(err).ToNot(HaveOccurred())
				for _, shardWalFile := range shardWalList {
					Expect(shardWalFile.IsDir()).To(BeFalse())
					Expect(shardWalFile.Name()).To(HaveSuffix(".wal"))
					shardWalFileHistory[shardIndex] = append(shardWalFileHistory[shardIndex], shardWalFile.Name())
				}
				return nil
			})
			// Write buffer & wal
			var wg sync.WaitGroup
			wg.Add(writeConcurrency)
			for i := 0; i < writeConcurrency; i++ {
				go func(writerIndex int) {
					for j := 0; j < numShards; j++ {
						for k := 0; k < flushSize; k++ {
							buffer.Write(
								[]byte(fmt.Sprintf("writer-%d-shard-%d-key-%d", writerIndex, j, k)),
								[]byte(fmt.Sprintf("writer-%d-shard-%d-value-%d", writerIndex, j, k)),
								time.UnixMilli(baseTime.UnixMilli()+int64(writerIndex+j+k)))
						}
					}
					wg.Done()
				}(i)
			}
			wg.Wait()
			buffer.Close()

			// Check wal
			Expect(len(shardWalFileHistory) == numShards).To(BeTrue())
			for shardIndex := 0; shardIndex < numShards; shardIndex++ {
				// Check wal rotate
				Expect(len(shardWalFileHistory[shardIndex]) > 1).To(BeTrue())

				shardWalDir := filepath.Join(path, "buffer-"+strconv.Itoa(shardIndex))
				currentShardWalFiles, err := os.ReadDir(shardWalDir)
				Expect(err).ToNot(HaveOccurred())
				Expect(len(currentShardWalFiles) <= 2).To(BeTrue())
				// Check wal delete
				Expect(len(shardWalFileHistory[shardIndex]) > len(currentShardWalFiles)).To(BeTrue())
			}
		})

		It("should recover buffer from wal file correctly", func() {
			var err error
			var flushMutex sync.Mutex
			var bufferFlushed bool

			buffer, err = tsdb.NewBufferWithWal(
				log,
				common.Position{},
				flushSize,
				writeConcurrency,
				numShards,
				true,
				path)
			Expect(err).ToNot(HaveOccurred())
			buffer.Register("test", func(shardIndex int, skl *skl.Skiplist) error {
				flushMutex.Lock()
				defer flushMutex.Unlock()

				if !bufferFlushed {
					bufferFlushed = true
				}
				return nil
			})

			// Write buffer & wal
			for i := 0; i < numShards; i++ {
				buffer.Write(
					[]byte(fmt.Sprintf("shard-%d-key-1", i)),
					[]byte(fmt.Sprintf("shard-%d-value-1", i)),
					time.UnixMilli(baseTime.UnixMilli()+int64(i)))
			}

			flushMutex.Lock()
			Expect(bufferFlushed).To(BeFalse())
			flushMutex.Unlock()

			// Restart buffer
			buffer.Close()
			buffer, err = tsdb.NewBufferWithWal(
				log,
				common.Position{},
				flushSize,
				writeConcurrency,
				numShards,
				true,
				path)
			Expect(err).ToNot(HaveOccurred())
			defer buffer.Close()
			buffer.Register("test", emptyFn)

			// Check buffer was recovered from wal
			for i := 0; i < numShards; i++ {
				expectValue := []byte(fmt.Sprintf("shard-%d-value-1", i))
				value, exist := buffer.Read(
					[]byte(fmt.Sprintf("shard-%d-key-1", i)),
					time.UnixMilli(baseTime.UnixMilli()+int64(i)))
				Expect(exist).To(BeTrue())
				Expect(bytes.Equal(expectValue, value)).To(BeTrue())
			}
		})
	})
})

var _ = Describe("bufferSupplier", func() {
	var (
		b     *tsdb.BufferSupplier
		goods []gleak.Goroutine
	)

	BeforeEach(func() {
		goods = gleak.Goroutines()
		b = tsdb.NewBufferSupplier(logger.GetLogger("buffer-supplier-test"), common.Position{}, 16, 4, false, "")
	})
	AfterEach(func() {
		b.Close()
		Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
	})

	Describe("Borrow", func() {
		Context("when borrowing a buffer with a new name", func() {
			It("should return a new buffer instance", func() {
				buf, err := b.Borrow("buffer", "test", 1024*1024, emptyFn)
				Expect(err).ToNot(HaveOccurred())
				Expect(buf).ToNot(BeNil())
				Expect(b.Volume()).To(Equal(1))
				b.Return("buffer", "test")
				Expect(b.Volume()).To(Equal(0))
			})
		})

		Context("when borrowing a buffer with an existing name", func() {
			It("should return the same buffer instance", func() {
				buf1, err := b.Borrow("buffer", "test", 1024*1024, emptyFn)
				Expect(err).ToNot(HaveOccurred())
				Expect(buf1).ToNot(BeNil())
				Expect(b.Volume()).To(Equal(1))

				buf2, err := b.Borrow("buffer", "test", 1024*1024, emptyFn)
				Expect(err).ToNot(HaveOccurred())
				Expect(buf2).ToNot(BeNil())
				Expect(b.Volume()).To(Equal(1))

				Expect(buf2).To(Equal(buf1))
				b.Return("buffer", "test")
				Expect(b.Volume()).To(Equal(0))
			})
		})

		Context("when borrowing a buffer from different buffer pools", func() {
			It("should return different buffer instances", func() {
				buf1, err := b.Borrow("buffer1", "test", 1024*1024, emptyFn)
				Expect(err).ToNot(HaveOccurred())
				Expect(buf1).ToNot(BeNil())
				Expect(b.Volume()).To(Equal(1))

				buf2, err := b.Borrow("buffer2", "test", 1024*1024, emptyFn)
				Expect(err).ToNot(HaveOccurred())
				Expect(buf2).ToNot(BeNil())
				Expect(b.Volume()).To(Equal(2))

				Expect(buf2).ToNot(Equal(buf1))
				b.Return("buffer1", "test")
				b.Return("buffer2", "test")
				Expect(b.Volume()).To(Equal(0))
			})
		})
	})
})
