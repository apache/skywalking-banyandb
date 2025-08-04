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

package gossip

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"google.golang.org/grpc"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
)

const mockGroup = "mock-group"

func TestGossip(t *testing.T) {
	gomega.RegisterFailHandler(ginkgo.Fail)
	ginkgo.RunSpecs(t, "Gossip Suite")
}

var _ = ginkgo.Describe("Propagation Messenger", func() {
	gomega.Expect(logger.Init(logger.Logging{
		Env:   "dev",
		Level: flags.LogLevel,
	})).To(gomega.Succeed())

	var nodes []*nodeContext
	ginkgo.AfterEach(func() {
		for _, node := range nodes {
			if node != nil {
				node.stop()
			}
		}
		nodes = nil
	})

	ginkgo.It("single node", func() {
		nodes = startNodes(1)
		node := nodes[0]

		err := node.messenger.Propagation([]string{node.nodeID}, "test", 0)
		gomega.Expect(err).To(gomega.HaveOccurred())
	})

	ginkgo.It("two nodes and send from self", func() {
		nodes = startNodes(2)
		node1, node2 := nodes[0], nodes[1]

		err := node1.messenger.Propagation([]string{node1.nodeID, node2.nodeID}, mockGroup, 0)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		nodeVerify(node1, []string{node2.nodeID}, 1)
	})

	ginkgo.It("two nodes and send from other node", func() {
		nodes = startNodes(2)
		node1, node2 := nodes[0], nodes[1]

		err := node2.messenger.Propagation([]string{node1.nodeID, node2.nodeID}, mockGroup, 0)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		nodeVerify(node1, []string{node2.nodeID}, 1)
		nodeVerify(node2, []string{}, 0)
	})

	ginkgo.It("two nodes with not existing node", func() {
		nodes = startNodes(2)
		node1, node2 := nodes[0], nodes[1]

		err := node2.messenger.Propagation([]string{node1.nodeID, node2.nodeID, "no-existing"}, mockGroup, 0)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		nodeVerify(node1, []string{node2.nodeID, node2.nodeID}, 2)
		nodeVerify(node2, []string{node1.nodeID}, 1)
	})

	ginkgo.It("multiple nodes with propagation", func() {
		nodes = startNodes(3)
		node1, node2, node3 := nodes[0], nodes[1], nodes[2]

		err := node1.messenger.Propagation([]string{node1.nodeID, node2.nodeID, node3.nodeID}, mockGroup, 0)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		nodeVerify(node1, []string{node2.nodeID}, 1)
		nodeVerify(node2, []string{node3.nodeID}, 1)
		nodeVerify(node3, []string{node1.nodeID}, 1)
	})

	ginkgo.It("multiple nodes with propagation with error", func() {
		nodes = startNodes(3)
		node1, node2, node3 := nodes[0], nodes[1], nodes[2]
		node3.listener.mockErr = fmt.Errorf("mock error")

		err := node1.messenger.Propagation([]string{node1.nodeID, node2.nodeID, node3.nodeID}, mockGroup, 0)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		nodeVerify(node1, []string{node2.nodeID}, 1)
		nodeVerify(node2, []string{node3.nodeID}, 1)
		nodeVerify(node3, []string{}, 0)
	})

	ginkgo.It("multiple propagation with same group", func() {
		nodes = startNodes(3)
		node1, node2, node3 := nodes[0], nodes[1], nodes[2]
		node1.listener.delay = time.Second

		err := node1.messenger.Propagation([]string{node1.nodeID, node2.nodeID, node3.nodeID}, mockGroup, 0)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = node2.messenger.Propagation([]string{node1.nodeID, node2.nodeID, node3.nodeID}, mockGroup, 0)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// should only the first propagation execute, the second one is ignored
		nodeVerify(node1, []string{node2.nodeID}, 1)
		nodeVerify(node2, []string{node3.nodeID}, 1)
		nodeVerify(node3, []string{node1.nodeID}, 1)
		nodeListenFromVerify(node1, []string{node1.nodeID})
		nodeListenFromVerify(node2, []string{node1.nodeID})
		nodeListenFromVerify(node3, []string{node1.nodeID})
	})
})

func nodeVerify(n *nodeContext, targets []string, messagesCount int) {
	gomega.Eventually(func() []string {
		n.listener.mu.RLock()
		defer n.listener.mu.RUnlock()
		return n.listener.targets
	}, flags.EventuallyTimeout).Should(gomega.Equal(targets))
	gomega.Eventually(func() int {
		n.listener.mu.RLock()
		defer n.listener.mu.RUnlock()
		return len(n.listener.messages)
	}, flags.EventuallyTimeout).Should(gomega.Equal(messagesCount))
}

func nodeListenFromVerify(n *nodeContext, fromNode []string) {
	gomega.Eventually(func() []string {
		n.listener.mu.RLock()
		defer n.listener.mu.RUnlock()
		return n.listener.fromNodes
	}, flags.EventuallyTimeout).Should(gomega.Equal(fromNode))
}

type nodeContext struct {
	messenger Messenger
	listener  *mockListener
	stop      func()
	nodeID    string
}

func startNodes(count int) []*nodeContext {
	result := make([]*nodeContext, count)
	messengers := make([]Messenger, count)
	for i := 0; i < count; i++ {
		listener := newMockListener()

		// starting grpc server node
		ports, err := test.AllocateFreePorts(1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// starting gossip messenger
		messenger := NewMessengerWithoutMetadata(observability.NewBypassRegistry(), ports[0])
		gomega.Expect(messenger).NotTo(gomega.BeNil())
		addr := fmt.Sprintf("127.0.0.1:%d", ports[0])
		messenger.(run.PreRunner).PreRun(context.WithValue(context.Background(), common.ContextNodeKey, common.Node{
			NodeID: addr,
		}))
		messenger.Subscribe(listener)
		err = messenger.(*service).Validate()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		stopper := make(chan struct{}, 1)
		messenger.Serve(stopper)
		messengers[i] = messenger

		gomega.Eventually(func() error {
			conn, err := net.DialTimeout("tcp", addr, time.Second*2)
			if err == nil {
				_ = conn.Close()
			}
			return err
		}, flags.EventuallyTimeout).Should(gomega.Succeed())

		// adding node context
		result[i] = &nodeContext{
			nodeID:    addr,
			listener:  listener,
			messenger: messenger,
			stop: func() {
				messenger.GracefulStop()
				close(stopper)
			},
		}
	}

	// registering the node in the gossip system
	for _, m := range messengers {
		for _, n := range result {
			m.(*service).OnAddOrUpdate(schema.Metadata{
				TypeMeta: schema.TypeMeta{
					Name: n.nodeID,
					Kind: schema.KindNode,
				},
				Spec: &databasev1.Node{
					Metadata: &commonv1.Metadata{
						Name: n.nodeID,
					},
					Roles:       []databasev1.Role{databasev1.Role_ROLE_DATA},
					GrpcAddress: n.nodeID,

					PropertyRepairGossipGrpcAddress: n.nodeID,
				},
			})
		}
	}
	return result
}

type mockListener struct {
	mockErr   error
	targets   []string
	messages  []*propertyv1.PropagationRequest
	fromNodes []string
	delay     time.Duration
	mu        sync.RWMutex
}

func newMockListener() *mockListener {
	return &mockListener{
		targets:   make([]string, 0),
		messages:  make([]*propertyv1.PropagationRequest, 0),
		fromNodes: make([]string, 0),
	}
}

func (m *mockListener) Rev(_ context.Context, nextNode *grpc.ClientConn, req *propertyv1.PropagationRequest) error {
	if m.mockErr != nil {
		return m.mockErr
	}
	if m.delay > 0 {
		time.Sleep(m.delay)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.targets = append(m.targets, nextNode.Target())
	m.messages = append(m.messages, req)
	m.fromNodes = append(m.fromNodes, req.Context.OriginNode)
	return nil
}
