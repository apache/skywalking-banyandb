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

package pub

import (
	"context"
	"io"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/onsi/gomega/gleak"
	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/apache/skywalking-banyandb/api/data"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
)

var _ = ginkgo.Describe("Publish and Broadcast", func() {
	var goods []gleak.Goroutine
	ginkgo.BeforeEach(func() {
		goods = gleak.Goroutines()
	})
	ginkgo.AfterEach(func() {
		gomega.Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
	})

	ginkgo.Context("Publisher and batch publisher", func() {
		ginkgo.It("should publish messages", func() {
			addr1 := getAddress()
			addr2 := getAddress()
			closeFn1 := setup(addr1, codes.OK, 200*time.Millisecond)
			closeFn2 := setup(addr2, codes.OK, 10*time.Millisecond)
			p := newPub()
			defer func() {
				p.GracefulStop()
				closeFn1()
				closeFn2()
			}()
			node1 := getDataNode("node1", addr1)
			p.OnAddOrUpdate(node1)
			node2 := getDataNode("node2", addr2)
			p.OnAddOrUpdate(node2)

			bp := p.NewBatchPublisher(3 * time.Second)
			defer bp.Close()
			ctx := context.TODO()
			for i := 0; i < 10; i++ {
				_, err := bp.Publish(ctx, data.TopicStreamWrite,
					bus.NewBatchMessageWithNode(bus.MessageID(i), "node1", &streamv1.InternalWriteRequest{}),
					bus.NewBatchMessageWithNode(bus.MessageID(i), "node2", &streamv1.InternalWriteRequest{}),
				)
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
			}
		})

		ginkgo.It("should go to evict queue when node is unavailable", func() {
			addr1 := getAddress()
			addr2 := getAddress()
			closeFn1 := setup(addr1, codes.OK, 200*time.Millisecond)
			closeFn2 := setup(addr2, codes.Unavailable, 0)
			p := newPub()
			defer func() {
				p.GracefulStop()
				closeFn1()
				closeFn2()
			}()
			node1 := getDataNode("node1", addr1)
			p.OnAddOrUpdate(node1)
			node2 := getDataNode("node2", addr2)
			p.OnAddOrUpdate(node2)

			bp := p.NewBatchPublisher(3 * time.Second)
			ctx := context.TODO()
			for i := 0; i < 10; i++ {
				_, err := bp.Publish(ctx, data.TopicStreamWrite,
					bus.NewBatchMessageWithNode(bus.MessageID(i), "node1", &streamv1.InternalWriteRequest{}),
					bus.NewBatchMessageWithNode(bus.MessageID(i), "node2", &streamv1.InternalWriteRequest{}),
				)
				if err != nil {
					// The mock server will return io.EOF when the node is unavailable
					// It will close the stream and return io.EOF to the send
					gomega.Expect(errors.Is(err, io.EOF))
				}
			}
			gomega.Expect(bp.Close()).ShouldNot(gomega.HaveOccurred())
			gomega.Eventually(func() int {
				p.mu.RLock()
				defer p.mu.RUnlock()
				return len(p.active)
			}, flags.EventuallyTimeout).Should(gomega.Equal(1))
			func() {
				p.mu.RLock()
				defer p.mu.RUnlock()
				gomega.Expect(p.evictable).Should(gomega.HaveLen(1))
				gomega.Expect(p.evictable).Should(gomega.HaveKey("node2"))
				gomega.Expect(p.active).Should(gomega.HaveKey("node1"))
			}()
		})

		ginkgo.It("should stay in active queue when operation takes a long time", func() {
			addr1 := getAddress()
			addr2 := getAddress()
			closeFn1 := setup(addr1, codes.OK, 0)
			closeFn2 := setup(addr2, codes.OK, 5*time.Second)
			p := newPub()
			defer func() {
				p.GracefulStop()
				closeFn1()
				closeFn2()
			}()
			node1 := getDataNode("node1", addr1)
			p.OnAddOrUpdate(node1)
			node2 := getDataNode("node2", addr2)
			p.OnAddOrUpdate(node2)

			bp := p.NewBatchPublisher(3 * time.Second)
			ctx := context.TODO()
			for i := 0; i < 10; i++ {
				_, err := bp.Publish(ctx, data.TopicStreamWrite,
					bus.NewBatchMessageWithNode(bus.MessageID(i), "node1", &streamv1.InternalWriteRequest{}),
					bus.NewBatchMessageWithNode(bus.MessageID(i), "node2", &streamv1.InternalWriteRequest{}),
				)
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
			}
			gomega.Expect(bp.Close()).ShouldNot(gomega.HaveOccurred())
			gomega.Consistently(func() int {
				p.mu.RLock()
				defer p.mu.RUnlock()
				return len(p.active)
			}, "1s").Should(gomega.Equal(2))
		})
	})

	ginkgo.Context("Broadcast", func() {
		ginkgo.It("should broadcast messages", func() {
			addr1 := getAddress()
			addr2 := getAddress()
			closeFn1 := setup(addr1, codes.OK, 200*time.Millisecond)
			closeFn2 := setup(addr2, codes.OK, 10*time.Millisecond)
			p := newPub()
			defer func() {
				p.GracefulStop()
				closeFn1()
				closeFn2()
			}()
			node1 := getDataNode("node1", addr1)
			p.OnAddOrUpdate(node1)
			node2 := getDataNode("node2", addr2)
			p.OnAddOrUpdate(node2)

			ff, err := p.Broadcast(3*time.Second, data.TopicStreamQuery, bus.NewMessage(bus.MessageID(1), &streamv1.QueryRequest{}))
			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(ff).Should(gomega.HaveLen(2))
			messages, err := ff[0].GetAll()
			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(messages).Should(gomega.HaveLen(1))
		})

		ginkgo.It("should broadcast messages to failed nodes", func() {
			addr1 := getAddress()
			addr2 := getAddress()
			closeFn1 := setup(addr1, codes.OK, 200*time.Millisecond)
			closeFn2 := setup(addr2, codes.Unavailable, 0)
			p := newPub()
			defer func() {
				p.GracefulStop()
				closeFn1()
				closeFn2()
			}()
			node1 := getDataNode("node1", addr1)
			p.OnAddOrUpdate(node1)
			node2 := getDataNode("node2", addr2)
			p.OnAddOrUpdate(node2)

			ff, err := p.Broadcast(3*time.Second, data.TopicStreamQuery, bus.NewMessage(bus.MessageID(1), &streamv1.QueryRequest{}))
			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(ff).Should(gomega.HaveLen(2))
			for i := range ff {
				_, err := ff[i].GetAll()
				ginkgo.GinkgoWriter.Printf("error: %v \n", err)
				if err != nil {
					s, ok := status.FromError(err)
					gomega.Expect(ok).Should(gomega.BeTrue())
					gomega.Expect(s.Code()).Should(gomega.Equal(codes.Unavailable))
					return
				}
			}
			ginkgo.Fail("should not reach here")
		})

		ginkgo.It("should broadcast messages to slow nodes", func() {
			addr1 := getAddress()
			addr2 := getAddress()
			closeFn1 := setup(addr1, codes.OK, 200*time.Millisecond)
			closeFn2 := setup(addr2, codes.OK, 5*time.Second)
			p := newPub()
			defer func() {
				p.GracefulStop()
				closeFn1()
				closeFn2()
			}()
			node1 := getDataNode("node1", addr1)
			p.OnAddOrUpdate(node1)
			node2 := getDataNode("node2", addr2)
			p.OnAddOrUpdate(node2)

			ff, err := p.Broadcast(3*time.Second, data.TopicStreamQuery, bus.NewMessage(bus.MessageID(1), &streamv1.QueryRequest{}))
			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(ff).Should(gomega.HaveLen(2))
			for i := range ff {
				_, err := ff[i].GetAll()
				ginkgo.GinkgoWriter.Printf("error: %v \n", err)
				if err != nil {
					s, ok := status.FromError(err)
					gomega.Expect(ok).Should(gomega.BeTrue())
					gomega.Expect(s.Code()).Should(gomega.Equal(codes.DeadlineExceeded))
					return
				}
			}
			ginkgo.Fail("should not reach here")
		})
	})
})
