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
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/onsi/gomega/gleak"
	"google.golang.org/grpc/codes"

	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
)

var _ = ginkgo.Describe("publish clients register/unregister", func() {
	var goods []gleak.Goroutine
	ginkgo.BeforeEach(func() {
		goods = gleak.Goroutines()
	})
	ginkgo.AfterEach(func() {
		gomega.Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
	})

	ginkgo.Context("publisher and batch publisher", func() {
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

			bp := p.NewBatchPublisher()
			defer bp.Close()
			t := bus.UniTopic("test")
			for i := 0; i < 10; i++ {
				_, err := bp.Publish(t,
					bus.NewBatchMessageWithNode(bus.MessageID(i), "node1", &streamv1.InternalWriteRequest{}),
					bus.NewBatchMessageWithNode(bus.MessageID(i), "node2", &streamv1.InternalWriteRequest{}),
				)
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
			}
		})

		ginkgo.FIt("should go to evict queue when node is unavailable", func() {
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

			bp := p.NewBatchPublisher()
			t := bus.UniTopic("test")
			for i := 0; i < 10; i++ {
				_, err := bp.Publish(t,
					bus.NewBatchMessageWithNode(bus.MessageID(i), "node1", &streamv1.InternalWriteRequest{}),
					bus.NewBatchMessageWithNode(bus.MessageID(i), "node2", &streamv1.InternalWriteRequest{}),
				)
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
			}
			gomega.Expect(bp.Close()).ShouldNot(gomega.HaveOccurred())
			gomega.Eventually(func() int {
				p.mu.RLock()
				defer p.mu.RUnlock()
				return len(p.clients)
			}, flags.EventuallyTimeout).Should(gomega.Equal(1))
			func() {
				p.mu.RLock()
				defer p.mu.RUnlock()
				gomega.Expect(p.evictClients).Should(gomega.HaveLen(1))
				gomega.Expect(p.evictClients).Should(gomega.HaveKey("node2"))
				gomega.Expect(p.clients).Should(gomega.HaveKey("node1"))
			}()
		})
	})
})
