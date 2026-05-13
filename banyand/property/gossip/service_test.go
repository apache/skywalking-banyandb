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
	"strings"
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

	// Latest scheduled round must reach Rev when the worker is busy.
	ginkgo.It("does not silently drop scheduled rounds when handler is slow", func() {
		nodes = startNodes(3)
		svc := nodes[0].messenger.(*service)

		coalesced := newRecordingCounter()
		svc.serverMetrics.totalCoalesced = coalesced

		blocking := newBlockingListener()
		svc.listenersLock.Lock()
		svc.listeners = []MessageListener{blocking}
		svc.listenersLock.Unlock()

		nodeList := []string{nodes[0].nodeID, nodes[1].nodeID, nodes[2].nodeID}
		const totalRounds = 5
		buildRequest := func(roundIndex int32) *propertyv1.PropagationRequest {
			return &propertyv1.PropagationRequest{
				Context: &propertyv1.PropagationContext{
					Nodes:                   nodeList,
					OriginNode:              nodes[0].nodeID,
					MaxPropagationCount:     int32(len(nodeList)*2 - 3),
					CurrentPropagationCount: roundIndex,
				},
				Group:   mockGroup,
				ShardId: 0,
			}
		}

		_, sendErr := svc.protocolHandler.Propagation(context.Background(), buildRequest(0))
		gomega.Expect(sendErr).NotTo(gomega.HaveOccurred())
		gomega.Eventually(func() bool {
			select {
			case <-blocking.entered:
				return true
			default:
				return false
			}
		}, flags.EventuallyTimeout, "10ms").Should(gomega.BeTrue(),
			"worker should have drained the first request and entered listener.Rev")

		_, sendErr = svc.protocolHandler.Propagation(context.Background(), buildRequest(1))
		gomega.Expect(sendErr).NotTo(gomega.HaveOccurred())
		for i := int32(2); i < int32(totalRounds); i++ {
			_, sendErr = svc.protocolHandler.Propagation(context.Background(), buildRequest(i))
			gomega.Expect(sendErr).NotTo(gomega.HaveOccurred())
		}
		gomega.Expect(coalesced.get(mockGroup)).To(gomega.Equal(3.0),
			"3 same-round overwrites of a non-nil pending must each increment totalCoalesced")

		close(blocking.release)
		gomega.Eventually(func() int32 {
			return blocking.maxObservedCount()
		}, flags.EventuallyTimeout, "100ms").Should(gomega.Equal(int32(4)),
			"latest scheduled round must reach Rev under latest-wins semantics")
		gomega.Expect(blocking.count()).To(gomega.BeNumerically(">=", 2),
			"at least the in-flight round and the coalesced latest round should reach Rev")
	})

	// TTL takeover wires the new originator into pending when it is empty.
	ginkgo.It("TTL-expired branch takes over to new originator with empty pending", func() {
		nodes = startNodes(2)
		svc := nodes[0].messenger.(*service)
		svc.scheduleInterval = 50 * time.Millisecond

		blocking := newBlockingListener()
		svc.listenersLock.Lock()
		svc.listeners = []MessageListener{blocking}
		svc.listenersLock.Unlock()

		nodeList := []string{nodes[0].nodeID, nodes[1].nodeID}
		buildReq := func(origin string, idx int32) *propertyv1.PropagationRequest {
			return &propertyv1.PropagationRequest{
				Context: &propertyv1.PropagationContext{
					Nodes:                   nodeList,
					OriginNode:              origin,
					MaxPropagationCount:     1,
					CurrentPropagationCount: idx,
				},
				Group:   mockGroup,
				ShardId: 0,
			}
		}

		_, err := svc.protocolHandler.Propagation(context.Background(), buildReq(nodes[0].nodeID, 0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Eventually(func() bool {
			select {
			case <-blocking.entered:
				return true
			default:
				return false
			}
		}, flags.EventuallyTimeout, "10ms").Should(gomega.BeTrue())

		time.Sleep(250 * time.Millisecond)

		_, err = svc.protocolHandler.Propagation(context.Background(), buildReq(nodes[1].nodeID, 99))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		close(blocking.release)
		gomega.Eventually(func() int32 {
			return blocking.maxObservedCount()
		}, flags.EventuallyTimeout, "100ms").Should(gomega.Equal(int32(99)),
			"round-B must reach Rev — confirms the TTL-expired branch ran and wired pending correctly")
	})

	// TTL takeover overwrites a non-nil pending and bumps totalCoalesced.
	ginkgo.It("TTL-expired branch coalesces when pending is non-nil", func() {
		nodes = startNodes(2)
		svc := nodes[0].messenger.(*service)
		svc.scheduleInterval = 50 * time.Millisecond
		coalesced := newRecordingCounter()
		svc.serverMetrics.totalCoalesced = coalesced

		blocking := newBlockingListener()
		svc.listenersLock.Lock()
		svc.listeners = []MessageListener{blocking}
		svc.listenersLock.Unlock()

		nodeList := []string{nodes[0].nodeID, nodes[1].nodeID}
		buildReq := func(origin string, idx int32) *propertyv1.PropagationRequest {
			return &propertyv1.PropagationRequest{
				Context: &propertyv1.PropagationContext{
					Nodes:                   nodeList,
					OriginNode:              origin,
					MaxPropagationCount:     1,
					CurrentPropagationCount: idx,
				},
				Group:   mockGroup,
				ShardId: 0,
			}
		}

		_, err := svc.protocolHandler.Propagation(context.Background(), buildReq(nodes[0].nodeID, 0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Eventually(func() bool {
			select {
			case <-blocking.entered:
				return true
			default:
				return false
			}
		}, flags.EventuallyTimeout, "10ms").Should(gomega.BeTrue())

		_, err = svc.protocolHandler.Propagation(context.Background(), buildReq(nodes[0].nodeID, 1))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(coalesced.get(mockGroup)).To(gomega.Equal(0.0),
			"first same-origin round writes pending without coalescing")

		time.Sleep(250 * time.Millisecond)

		_, err = svc.protocolHandler.Propagation(context.Background(), buildReq(nodes[1].nodeID, 99))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(coalesced.get(mockGroup)).To(gomega.Equal(1.0),
			"TTL-expired branch must increment totalCoalesced when overwriting a non-nil pending")

		close(blocking.release)
		gomega.Eventually(func() int32 {
			return blocking.maxObservedCount()
		}, flags.EventuallyTimeout, "100ms").Should(gomega.Equal(int32(99)),
			"round-B (the latest take-over) must reach Rev")
	})

	// drain-all flushes every queued pending even when groupNotify drops signals.
	ginkgo.It("does not strand pending entries when groupNotify is saturated", func() {
		nodes = startNodes(2)
		svc := nodes[0].messenger.(*service)

		blocking := newBlockingListener()
		svc.listenersLock.Lock()
		svc.listeners = []MessageListener{blocking}
		svc.listenersLock.Unlock()

		nodeList := []string{nodes[0].nodeID, nodes[1].nodeID}
		buildReq := func(shardID uint32, idx int32) *propertyv1.PropagationRequest {
			return &propertyv1.PropagationRequest{
				Context: &propertyv1.PropagationContext{
					Nodes:                   nodeList,
					OriginNode:              nodes[0].nodeID,
					MaxPropagationCount:     1,
					CurrentPropagationCount: idx,
				},
				Group:   mockGroup,
				ShardId: shardID,
			}
		}

		_, err := svc.protocolHandler.Propagation(context.Background(), buildReq(0, 0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Eventually(func() bool {
			select {
			case <-blocking.entered:
				return true
			default:
				return false
			}
		}, flags.EventuallyTimeout, "10ms").Should(gomega.BeTrue(),
			"worker should drain the initial request and enter listener.Rev")

		const extraShards uint32 = 12
		for i := uint32(1); i <= extraShards; i++ {
			_, err = svc.protocolHandler.Propagation(context.Background(), buildReq(i, int32(i)))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		close(blocking.release)
		gomega.Eventually(func() int {
			return blocking.count()
		}, flags.EventuallyTimeout, "100ms").Should(gomega.Equal(int(extraShards)+1),
			"drain-all on each wake-up must flush every pending shard even when notifyNewRequest dropped some signals")
	})

	// CloseNotify must abort the drain loop between handles.
	ginkgo.It("worker honors CloseNotify in drain loop even with backlog", func() {
		nodes = startNodes(2)
		// Slow listener so each handle takes a measurable amount of time.
		nodes[0].listener.delay = 200 * time.Millisecond

		svc := nodes[0].messenger.(*service)
		listener := nodes[0].listener
		nodeList := []string{nodes[0].nodeID, nodes[1].nodeID}
		const backlogShards uint32 = 10

		for i := uint32(0); i < backlogShards; i++ {
			req := &propertyv1.PropagationRequest{
				Context: &propertyv1.PropagationContext{
					Nodes:               nodeList,
					OriginNode:          nodes[0].nodeID,
					MaxPropagationCount: 1,
				},
				Group:   mockGroup,
				ShardId: i,
			}
			_, err := svc.protocolHandler.Propagation(context.Background(), req)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		// Give the worker a moment to start the first handle so the listener
		// delay is in flight when we trigger close.
		time.Sleep(250 * time.Millisecond)

		listener.mu.RLock()
		countAtClose := len(listener.messages)
		listener.mu.RUnlock()

		// Trigger close. Without the close check in the drain loop, the worker
		// would keep processing every queued shard before exiting.
		svc.GracefulStop()
		nodes[0] = nil // suite-level AfterEach must not double-stop

		// Wait long enough that an unfixed worker would have drained the full
		// backlog: backlogShards * 200ms = 2s. Use 2.5s safety margin.
		time.Sleep(2500 * time.Millisecond)

		listener.mu.RLock()
		extraHandles := len(listener.messages) - countAtClose
		listener.mu.RUnlock()

		gomega.Expect(extraHandles).To(gomega.BeNumerically("<=", 2),
			"worker should exit within ~one in-flight handle after CloseNotify; saw %d extra handles after close (full backlog would be ~%d)",
			extraHandles, int(backlogShards))
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
		messenger := NewMessengerWithoutMetadata("property-repair",
			func(n *databasev1.Node) string { return n.PropertyRepairGossipGrpcAddress },
			observability.NewBypassRegistry(), ports[0])
		gomega.Expect(messenger).NotTo(gomega.BeNil())
		addr := fmt.Sprintf("127.0.0.1:%d", ports[0])
		messenger.(run.PreRunner).PreRun(context.WithValue(context.Background(), common.ContextNodeKey, common.Node{
			NodeID: addr,
		}))
		messenger.Subscribe(listener)
		err = messenger.(*service).Validate()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		stopper := run.NewCloser(0)
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
				stopper.CloseThenWait()
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

func (m *mockListener) Rev(_ context.Context, _ Trace, nextNode *grpc.ClientConn, req *propertyv1.PropagationRequest) error {
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

// blockingListener.Rev blocks on release; propCounts records which rounds reached Rev.
type blockingListener struct {
	release     chan struct{}
	entered     chan struct{}
	propCounts  []int32
	enteredOnce sync.Once
	mu          sync.RWMutex
}

func newBlockingListener() *blockingListener {
	return &blockingListener{
		release: make(chan struct{}),
		entered: make(chan struct{}),
	}
}

func (b *blockingListener) Rev(_ context.Context, _ Trace, _ *grpc.ClientConn, req *propertyv1.PropagationRequest) error {
	b.enteredOnce.Do(func() { close(b.entered) })
	<-b.release
	b.mu.Lock()
	b.propCounts = append(b.propCounts, req.Context.CurrentPropagationCount)
	b.mu.Unlock()
	return nil
}

func (b *blockingListener) count() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.propCounts)
}

func (b *blockingListener) maxObservedCount() int32 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if len(b.propCounts) == 0 {
		return 0
	}
	maxCount := b.propCounts[0]
	for _, c := range b.propCounts[1:] {
		if c > maxCount {
			maxCount = c
		}
	}
	return maxCount
}

// recordingCounter exposes Inc deltas — the bypass registry cannot be introspected.
type recordingCounter struct {
	totals map[string]float64
	mu     sync.RWMutex
}

func newRecordingCounter() *recordingCounter {
	return &recordingCounter{totals: make(map[string]float64)}
}

func (c *recordingCounter) Inc(delta float64, labelValues ...string) {
	key := strings.Join(labelValues, "\x00")
	c.mu.Lock()
	c.totals[key] += delta
	c.mu.Unlock()
}

func (c *recordingCounter) Delete(labelValues ...string) bool {
	key := strings.Join(labelValues, "\x00")
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.totals[key]; !ok {
		return false
	}
	delete(c.totals, key)
	return true
}

func (c *recordingCounter) get(labelValues ...string) float64 {
	key := strings.Join(labelValues, "\x00")
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.totals[key]
}
