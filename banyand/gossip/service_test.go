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
	"strconv"
	"testing"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	gossipv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/gossip/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/banyand/queue/pub"
	"github.com/apache/skywalking-banyandb/banyand/queue/sub"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
)

const mockTopic = "mock-topic"

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
	ginkgo.BeforeEach(func() {
		TopicMessageRequestMap[mockTopic] = func() proto.Message {
			return &commonv1.Group{}
		}
	})
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

		_, err := node.messenger.Propagation([]string{node.nodeID}, mockTopic, bus.NewMessage(1, &commonv1.Group{}))
		gomega.Expect(err).To(gomega.HaveOccurred())
	})

	ginkgo.It("two nodes and send from self", func() {
		nodes = startNodes(2)
		node1, node2 := nodes[0], nodes[1]

		f, err := node1.messenger.Propagation([]string{node1.nodeID, node2.nodeID}, mockTopic, bus.NewMessage(1, &commonv1.Group{}))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		verifyFuture(f, true)
		gomega.Expect(node1.listener.targets).To(gomega.Equal([]string{node2.nodeID}))
		gomega.Expect(node1.listener.messages).To(gomega.HaveLen(1))
	})

	ginkgo.It("two nodes and send from other node", func() {
		nodes = startNodes(2)
		node1, node2 := nodes[0], nodes[1]

		f, err := node2.messenger.Propagation([]string{node1.nodeID, node2.nodeID}, mockTopic, bus.NewMessage(1, &commonv1.Group{}))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		verifyFuture(f, true)
		gomega.Expect(node1.listener.targets).To(gomega.Equal([]string{node2.nodeID}))
		gomega.Expect(node1.listener.messages).To(gomega.HaveLen(1))

		gomega.Expect(node2.listener.targets).To(gomega.HaveLen(0))
	})

	ginkgo.It("multiple nodes with propagation", func() {
		nodes = startNodes(3)
		node1, node2, node3 := nodes[0], nodes[1], nodes[2]

		f, err := node1.messenger.Propagation([]string{node1.nodeID, node2.nodeID, node3.nodeID}, mockTopic, bus.NewMessage(1, &commonv1.Group{}))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		verifyFuture(f, true)
		gomega.Expect(node1.listener.targets).To(gomega.Equal([]string{node2.nodeID}))
		gomega.Expect(node1.listener.messages).To(gomega.HaveLen(1))

		gomega.Expect(node2.listener.targets).To(gomega.Equal([]string{node3.nodeID}))
		gomega.Expect(node2.listener.messages).To(gomega.HaveLen(1))

		gomega.Expect(node3.listener.targets).To(gomega.Equal([]string{node1.nodeID}))
		gomega.Expect(node3.listener.messages).To(gomega.HaveLen(1))
	})

	ginkgo.It("multiple nodes with propagation with error", func() {
		nodes = startNodes(3)
		node1, node2, node3 := nodes[0], nodes[1], nodes[2]
		node3.listener.mockErr = fmt.Errorf("mock error")

		f, err := node1.messenger.Propagation([]string{node1.nodeID, node2.nodeID, node3.nodeID}, mockTopic, bus.NewMessage(1, &commonv1.Group{}))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		verifyFuture(f, false)
		gomega.Expect(node1.listener.targets).To(gomega.Equal([]string{node2.nodeID}))
		gomega.Expect(node1.listener.messages).To(gomega.HaveLen(1))

		gomega.Expect(node2.listener.targets).To(gomega.Equal([]string{node3.nodeID}))
		gomega.Expect(node2.listener.messages).To(gomega.HaveLen(1))

		gomega.Expect(node3.listener.targets).To(gomega.HaveLen(0))
		gomega.Expect(node3.listener.messages).To(gomega.HaveLen(0))
	})
})

type nodeContext struct {
	messenger Messenger
	listener  *mockListener
	stop      func()
	nodeID    string
}

func startNodes(count int) []*nodeContext {
	p := pub.NewWithoutMetadata()
	result := make([]*nodeContext, count)
	for i := 0; i < count; i++ {
		listener := &mockListener{}

		// starting grpc server node
		addr, server := startNode()
		gomega.Expect(server).NotTo(gomega.BeNil())

		// starting gossip messenger
		messenger := NewMessenger(observability.NewBypassRegistry(), p, server)
		gomega.Expect(messenger).NotTo(gomega.BeNil())
		messenger.(run.PreRunner).PreRun(context.WithValue(context.Background(), common.ContextNodeKey, common.Node{
			NodeID: addr,
		}))
		messenger.Subscribe(mockTopic, listener)
		_ = messenger.Serve()

		// adding node context
		result[i] = &nodeContext{
			nodeID:    addr,
			listener:  listener,
			messenger: messenger,
			stop: func() {
				messenger.GracefulStop()
				server.(run.Service).GracefulStop()
			},
		}

		// registering the node in the gossip system
		p.OnAddOrUpdate(schema.Metadata{
			TypeMeta: schema.TypeMeta{
				Name: addr,
				Kind: schema.KindNode,
			},
			Spec: &databasev1.Node{
				Metadata: &commonv1.Metadata{
					Name: addr,
				},
				Roles:       []databasev1.Role{databasev1.Role_ROLE_DATA},
				GrpcAddress: addr,
			},
		})
	}

	// make sure all nodes are activated
	gomega.Eventually(func() error {
		for _, node := range result {
			_, exist := p.ClusterNode(node.nodeID)
			if !exist {
				return fmt.Errorf("node %s is not registered in gossip", node.nodeID)
			}
		}
		return nil
	}, flags.EventuallyTimeout).Should(gomega.Succeed())
	return result
}

func startNode() (string, queue.Server) {
	ports, err := test.AllocateFreePorts(2)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	server := sub.NewServerWithPorts(observability.NewBypassRegistry(), "",
		uint32(ports[0]), uint32(ports[1]))
	server.(run.PreRunner).PreRun(context.Background())
	err = server.(run.Config).Validate()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	_ = server.(run.Service).Serve()
	addr := net.JoinHostPort("localhost", strconv.Itoa(ports[0]))
	gomega.Eventually(func() error {
		conn, err := net.DialTimeout("tcp", addr, time.Second*2)
		if err == nil {
			_ = conn.Close()
		}
		return err
	}, flags.EventuallyTimeout).Should(gomega.Succeed())
	return addr, server
}

func verifyFuture(f Future, success bool) {
	gomega.Expect(f).NotTo(gomega.BeNil())
	resp, err := f.Get()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(resp).NotTo(gomega.BeNil())
	gomega.Expect(resp.Data()).NotTo(gomega.BeNil())
	data, ok := resp.Data().(*gossipv1.PropagationMessageResponse)
	gomega.Expect(ok).To(gomega.BeTrue())
	if success {
		gomega.Expect(data.Success).To(gomega.BeTrue(), "Propagation should be successful")
	} else {
		gomega.Expect(data.Success).To(gomega.BeFalse(), "Propagation should not be successful")
	}
}

type mockListener struct {
	mockErr  error
	targets  []string
	messages []bus.Message
}

func (m *mockListener) Rev(_ context.Context, nextNode *grpc.ClientConn, message bus.Message) error {
	if m.mockErr != nil {
		return m.mockErr
	}
	m.targets = append(m.targets, nextNode.Target())
	m.messages = append(m.messages, message)
	return nil
}
