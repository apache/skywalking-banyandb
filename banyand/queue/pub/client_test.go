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
	ginkgo.It("should register and unregister clients", func() {
		addr1 := getAddress()
		closeFn := setup(addr1, codes.OK, 200*time.Millisecond)
		p := newPub()
		defer func() {
			p.GracefulStop()
			closeFn()
		}()
		node1 := getDataNode("node1", addr1)
		p.OnAddOrUpdate(node1)
		verifyClients(p, 1, 0, 1, 0)
		addr2 := getAddress()
		node2 := getDataNode("node2", addr2)
		p.OnAddOrUpdate(node2)
		verifyClients(p, 1, 1, 1, 1)

		p.OnDelete(node1)
		verifyClients(p, 1, 1, 1, 1)
		p.OnDelete(node2)
		verifyClients(p, 1, 0, 1, 1)
		closeFn()
		p.OnDelete(node1)
		verifyClients(p, 0, 0, 1, 2)
	})

	ginkgo.It("should move back to active queue", func() {
		addr1 := getAddress()
		node1 := getDataNode("node1", addr1)
		p := newPub()
		defer p.GracefulStop()
		p.OnAddOrUpdate(node1)
		verifyClients(p, 0, 1, 0, 1)
		closeFn := setup(addr1, codes.OK, 200*time.Millisecond)
		defer closeFn()
		gomega.Eventually(func() int {
			p.mu.RLock()
			defer p.mu.RUnlock()
			return len(p.clients)
		}, flags.EventuallyTimeout).Should(gomega.Equal(1))
		verifyClients(p, 1, 0, 1, 1)
	})
})

func verifyClients(p *pub, active, evict, onAdd, onDelete int) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	gomega.Expect(p.clients).Should(gomega.HaveLen(active))
	gomega.Expect(p.evictClients).Should(gomega.HaveLen(evict))
	h := p.handler.(*mockHandler)
	gomega.Expect(h.addOrUpdateCount).Should(gomega.Equal(onAdd))
	gomega.Expect(h.deleteCount).Should(gomega.Equal(onDelete))
}
