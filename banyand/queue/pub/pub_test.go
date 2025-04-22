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
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
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
			ctx := context.TODO()
			for i := 0; i < 10; i++ {
				_, err := bp.Publish(ctx, data.TopicStreamWrite,
					bus.NewBatchMessageWithNode(bus.MessageID(i), "node1", &streamv1.InternalWriteRequest{}),
					bus.NewBatchMessageWithNode(bus.MessageID(i), "node2", &streamv1.InternalWriteRequest{}),
				)
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
			}
			cee, err := bp.Close()
			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(cee).Should(gomega.HaveLen(0))
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
			cee, err := bp.Close()
			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(cee).Should(gomega.HaveLen(1))
			gomega.Expect(cee).Should(gomega.HaveKey("node2"))
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

		ginkgo.It("should go to evict queue when node's disk is full", func() {
			addr1 := getAddress()
			addr2 := getAddress()
			_, closeFn1 := setupWithStatus(addr1, modelv1.Status_STATUS_SUCCEED)
			_, closeFn2 := setupWithStatus(addr2, modelv1.Status_STATUS_DISK_FULL)
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
				gomega.Expect(err).Should(gomega.MatchError(common.NewErrorWithStatus(
					modelv1.Status_STATUS_DISK_FULL, modelv1.Status_name[int32(modelv1.Status_STATUS_DISK_FULL)]).Error()))
			}
			cee, err := bp.Close()
			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(cee).Should(gomega.BeNil())
			gomega.Consistently(func(g gomega.Gomega) {
				p.mu.RLock()
				defer p.mu.RUnlock()
				g.Expect(p.active).Should(gomega.HaveLen(2))
				g.Expect(p.evictable).Should(gomega.HaveLen(0))
			}).Should(gomega.Succeed())
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
			cee, err := bp.Close()
			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(cee).Should(gomega.HaveLen(1))
			gomega.Expect(cee).Should(gomega.HaveKey("node2"))
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

			req := &streamv1.QueryRequest{}
			ff, err := p.Broadcast(3*time.Second, data.TopicStreamQuery, bus.NewMessage(bus.MessageID(1), req))
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

		ginkgo.It("should broadcast messages to nodes with empty selector", func() {
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

			group1 := svc
			timeRange := &modelv1.TimeRange{
				Begin: timestamppb.New(time.Now().Add(-time.Hour)),
				End:   timestamppb.New(time.Now()),
			}

			node1Labels := map[string]string{
				"role": "ingest",
				"zone": "east",
			}
			node1Boundaries := map[string]*modelv1.TimeRange{
				group1: timeRange,
			}

			node2Labels := map[string]string{
				"role": "query",
				"zone": "west",
			}
			node2Boundaries := map[string]*modelv1.TimeRange{
				group1: timeRange,
			}

			node1 := getDataNodeWithLabels("node1", addr1, node1Labels, node1Boundaries)
			p.OnAddOrUpdate(node1)
			node2 := getDataNodeWithLabels("node2", addr2, node2Labels, node2Boundaries)
			p.OnAddOrUpdate(node2)

			nodeSelectors := map[string][]string{
				group1: {""},
			}

			ff, err := p.Broadcast(3*time.Second, data.TopicStreamQuery,
				bus.NewMessageWithNodeSelectors(bus.MessageID(1), nodeSelectors, timeRange, &streamv1.QueryRequest{}))

			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
			// Both nodes should be selected
			gomega.Expect(ff).Should(gomega.HaveLen(2))

			for _, f := range ff {
				messages, err := f.GetAll()
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
				gomega.Expect(messages).Should(gomega.HaveLen(1))
			}
		})

		ginkgo.It("should broadcast messages to nodes with specific label selector", func() {
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

			group1 := svc
			timeRange := &modelv1.TimeRange{
				Begin: timestamppb.New(time.Now().Add(-time.Hour)),
				End:   timestamppb.New(time.Now()),
			}

			node1Labels := map[string]string{
				"role": "ingest",
				"zone": "east",
			}
			node1Boundaries := map[string]*modelv1.TimeRange{
				group1: timeRange,
			}

			node2Labels := map[string]string{
				"role": "query",
				"zone": "west",
			}
			node2Boundaries := map[string]*modelv1.TimeRange{
				group1: timeRange,
			}

			node1 := getDataNodeWithLabels("node1", addr1, node1Labels, node1Boundaries)
			p.OnAddOrUpdate(node1)
			node2 := getDataNodeWithLabels("node2", addr2, node2Labels, node2Boundaries)
			p.OnAddOrUpdate(node2)

			nodeSelectors := map[string][]string{
				group1: {"role=ingest"},
			}

			ff, err := p.Broadcast(3*time.Second, data.TopicStreamQuery,
				bus.NewMessageWithNodeSelectors(bus.MessageID(1), nodeSelectors, timeRange, &streamv1.QueryRequest{}))

			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(ff).Should(gomega.HaveLen(1))

			messages, err := ff[0].GetAll()
			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(messages).Should(gomega.HaveLen(1))
		})

		ginkgo.It("should broadcast messages based on time range overlap", func() {
			addr1 := getAddress()
			addr2 := getAddress()
			addr3 := getAddress()
			closeFn1 := setup(addr1, codes.OK, 200*time.Millisecond)
			closeFn2 := setup(addr2, codes.OK, 10*time.Millisecond)
			closeFn3 := setup(addr3, codes.OK, 10*time.Millisecond)
			p := newPub()
			defer func() {
				p.GracefulStop()
				closeFn1()
				closeFn2()
				closeFn3()
			}()

			group1 := svc

			now := time.Now()
			timeRange1 := &modelv1.TimeRange{
				Begin: timestamppb.New(now.Add(-3 * time.Hour)),
				End:   timestamppb.New(now.Add(-2 * time.Hour)),
			}

			timeRange2 := &modelv1.TimeRange{
				Begin: timestamppb.New(now.Add(-2 * time.Hour)),
				End:   timestamppb.New(now.Add(-1 * time.Hour)),
			}

			timeRange3 := &modelv1.TimeRange{
				Begin: timestamppb.New(now.Add(-1 * time.Hour)),
				End:   timestamppb.New(now),
			}

			node1Labels := map[string]string{"zone": "east"}
			node1Boundaries := map[string]*modelv1.TimeRange{
				group1: timeRange1,
			}

			node2Labels := map[string]string{"zone": "east"}
			node2Boundaries := map[string]*modelv1.TimeRange{
				group1: timeRange2,
			}

			node3Labels := map[string]string{"zone": "east"}
			node3Boundaries := map[string]*modelv1.TimeRange{
				group1: timeRange3,
			}

			node1 := getDataNodeWithLabels("node1", addr1, node1Labels, node1Boundaries)
			p.OnAddOrUpdate(node1)
			node2 := getDataNodeWithLabels("node2", addr2, node2Labels, node2Boundaries)
			p.OnAddOrUpdate(node2)
			node3 := getDataNodeWithLabels("node3", addr3, node3Labels, node3Boundaries)
			p.OnAddOrUpdate(node3)

			queryTimeRange := &modelv1.TimeRange{
				Begin: timestamppb.New(now.Add(-2 * time.Hour).Add(-30 * time.Minute)),
				End:   timestamppb.New(now.Add(-1 * time.Hour).Add(-30 * time.Minute)),
			}

			nodeSelectors := map[string][]string{
				group1: {""},
			}

			ff, err := p.Broadcast(3*time.Second, data.TopicStreamQuery,
				bus.NewMessageWithNodeSelectors(bus.MessageID(1), nodeSelectors, queryTimeRange, &streamv1.QueryRequest{}))

			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(ff).Should(gomega.HaveLen(2))

			for _, f := range ff {
				messages, err := f.GetAll()
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
				gomega.Expect(messages).Should(gomega.HaveLen(1))
			}
		})

		ginkgo.It("should broadcast messages with both label selector and time range filter", func() {
			addr1 := getAddress()
			addr2 := getAddress()
			addr3 := getAddress()
			addr4 := getAddress()
			closeFn1 := setup(addr1, codes.OK, 200*time.Millisecond)
			closeFn2 := setup(addr2, codes.OK, 10*time.Millisecond)
			closeFn3 := setup(addr3, codes.OK, 10*time.Millisecond)
			closeFn4 := setup(addr4, codes.OK, 10*time.Millisecond)
			p := newPub()
			defer func() {
				p.GracefulStop()
				closeFn1()
				closeFn2()
				closeFn3()
				closeFn4()
			}()

			group1 := svc

			now := time.Now()
			timeRange1 := &modelv1.TimeRange{
				Begin: timestamppb.New(now.Add(-2 * time.Hour)),
				End:   timestamppb.New(now.Add(-1 * time.Hour)),
			}

			timeRange2 := &modelv1.TimeRange{
				Begin: timestamppb.New(now.Add(-1 * time.Hour)),
				End:   timestamppb.New(now),
			}

			node1Labels := map[string]string{
				"env":  "prod",
				"zone": "east",
			}
			node1Boundaries := map[string]*modelv1.TimeRange{
				group1: timeRange1,
			}

			node2Labels := map[string]string{
				"env":  "prod",
				"zone": "west",
			}
			node2Boundaries := map[string]*modelv1.TimeRange{
				group1: timeRange1,
			}

			node3Labels := map[string]string{
				"env":  "dev",
				"zone": "east",
			}
			node3Boundaries := map[string]*modelv1.TimeRange{
				group1: timeRange2,
			}

			node4Labels := map[string]string{
				"env":  "prod",
				"zone": "east",
			}
			node4Boundaries := map[string]*modelv1.TimeRange{
				group1: timeRange2,
			}

			node1 := getDataNodeWithLabels("node1", addr1, node1Labels, node1Boundaries)
			p.OnAddOrUpdate(node1)
			node2 := getDataNodeWithLabels("node2", addr2, node2Labels, node2Boundaries)
			p.OnAddOrUpdate(node2)
			node3 := getDataNodeWithLabels("node3", addr3, node3Labels, node3Boundaries)
			p.OnAddOrUpdate(node3)
			node4 := getDataNodeWithLabels("node4", addr4, node4Labels, node4Boundaries)
			p.OnAddOrUpdate(node4)

			queryTimeRange := &modelv1.TimeRange{
				Begin: timestamppb.New(now.Add(-30 * time.Minute)),
				End:   timestamppb.New(now.Add(30 * time.Minute)),
			}

			nodeSelectors := map[string][]string{
				group1: {"env=prod"},
			}

			ff, err := p.Broadcast(3*time.Second, data.TopicStreamQuery,
				bus.NewMessageWithNodeSelectors(bus.MessageID(1), nodeSelectors, queryTimeRange, &streamv1.QueryRequest{}))

			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(ff).Should(gomega.HaveLen(1))

			messages, err := ff[0].GetAll()
			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(messages).Should(gomega.HaveLen(1))
		})
	})
})
