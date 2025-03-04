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
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/onsi/gomega/gleak"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
)

const svc = "service"

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
			return len(p.active)
		}, flags.EventuallyTimeout).Should(gomega.Equal(1))
		verifyClients(p, 1, 0, 1, 1)
	})

	ginkgo.It("should be removed", func() {
		addr1 := getAddress()
		node1 := getDataNode("node1", addr1)
		p := newPub()
		defer p.GracefulStop()
		closeFn := setup(addr1, codes.OK, 200*time.Millisecond)
		p.OnAddOrUpdate(node1)
		verifyClients(p, 1, 0, 1, 0)
		closeFn()
		p.failover("node1", common.NewError("test"), data.TopicCommon)
		verifyClients(p, 0, 1, 1, 2)
		p.OnDelete(node1)
		verifyClients(p, 0, 0, 1, 2)
	})

	ginkgo.It("should be removed eventually", func() {
		addr1 := getAddress()
		node1 := getDataNode("node1", addr1)
		p := newPub()
		defer p.GracefulStop()
		closeFn := setup(addr1, codes.OK, 200*time.Millisecond)
		p.OnAddOrUpdate(node1)
		verifyClients(p, 1, 0, 1, 0)
		p.OnDelete(node1)
		verifyClients(p, 1, 0, 1, 0)
		closeFn()
		gomega.Eventually(func(g gomega.Gomega) {
			verifyClientsWithGomega(g, p, data.TopicCommon, 0, 0, 1, 1)
		}, flags.EventuallyTimeout).Should(gomega.Succeed())
	})

	ginkgo.It("should remove handler", func() {
		addr1 := getAddress()
		node1 := getDataNode("node1", addr1)
		hs, closeFn := setupWithStatus(addr1, modelv1.Status_STATUS_DISK_FULL)
		defer closeFn()
		p := newPub()
		defer p.GracefulStop()
		p.OnAddOrUpdate(node1)
		verifyClients(p, 1, 0, 1, 0)
		bp := p.NewBatchPublisher(3 * time.Second)
		ctx := context.TODO()
		for i := 0; i < 10; i++ {
			_, err := bp.Publish(ctx, data.TopicStreamWrite,
				bus.NewBatchMessageWithNode(bus.MessageID(i), "node1", &streamv1.InternalWriteRequest{}),
			)
			gomega.Expect(err).Should(gomega.MatchError(common.NewErrorWithStatus(
				modelv1.Status_STATUS_DISK_FULL, modelv1.Status_name[int32(modelv1.Status_STATUS_DISK_FULL)]).Error()))
		}
		cee, err := bp.Close()
		gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
		gomega.Expect(cee).Should(gomega.BeNil())
		verifyClientsWithGomega(gomega.Default, p, data.TopicStreamWrite, 1, 0, 1, 1)
		verifyClientsWithGomega(gomega.Default, p, data.TopicMeasureWrite, 1, 0, 1, 0)
		hs.SetServingStatus(data.TopicStreamWrite.String(), grpc_health_v1.HealthCheckResponse_SERVING)
		gomega.Eventually(func(g gomega.Gomega) {
			verifyClientsWithGomega(g, p, data.TopicStreamWrite, 1, 0, 2, 1)
		}, flags.EventuallyTimeout).Should(gomega.Succeed())
	})

	ginkgo.It("should update node when labels or data boundaries change", func() {
		addr1 := getAddress()
		closeFn := setup(addr1, codes.OK, 200*time.Millisecond)
		defer closeFn()
		p := newPub()
		defer p.GracefulStop()

		// Replace hard-coded "service" with the Service constant
		group1 := svc
		now := time.Now()
		timeRange1 := &modelv1.TimeRange{
			Begin: timestamppb.New(now.Add(-3 * time.Hour)),
			End:   timestamppb.New(now.Add(-2 * time.Hour)),
		}

		initialLabels := map[string]string{
			"role": "ingest",
			"zone": "east",
		}
		initialBoundaries := map[string]*modelv1.TimeRange{
			group1: timeRange1,
		}

		node1 := getDataNodeWithLabels("node1", addr1, initialLabels, initialBoundaries)
		p.OnAddOrUpdate(node1)
		verifyClients(p, 1, 0, 1, 0)

		p.mu.RLock()
		registeredNode := p.registered["node1"]
		gomega.Expect(registeredNode.Labels).Should(gomega.Equal(initialLabels))
		gomega.Expect(registeredNode.DataSegmentsBoundary).Should(gomega.Equal(initialBoundaries))
		p.mu.RUnlock()

		updatedLabels := map[string]string{
			"role": "query",
			"zone": "east",
			"env":  "prod",
		}
		updatedNode1 := getDataNodeWithLabels("node1", addr1, updatedLabels, initialBoundaries)
		p.OnAddOrUpdate(updatedNode1)

		p.mu.RLock()
		registeredNode = p.registered["node1"]
		gomega.Expect(registeredNode.Labels).Should(gomega.Equal(updatedLabels))
		gomega.Expect(len(p.active)).Should(gomega.Equal(1))
		p.mu.RUnlock()

		timeRange2 := &modelv1.TimeRange{
			Begin: timestamppb.New(now.Add(-1 * time.Hour)),
			End:   timestamppb.New(now),
		}
		updatedBoundaries := map[string]*modelv1.TimeRange{
			group1:      timeRange1,
			"inventory": timeRange2,
		}
		updatedNode2 := getDataNodeWithLabels("node1", addr1, updatedLabels, updatedBoundaries)
		p.OnAddOrUpdate(updatedNode2)

		p.mu.RLock()
		registeredNode = p.registered["node1"]
		gomega.Expect(registeredNode.DataSegmentsBoundary).Should(gomega.Equal(updatedBoundaries))
		gomega.Expect(len(p.active)).Should(gomega.Equal(1))
		p.mu.RUnlock()
	})
})

func verifyClients(p *pub, active, evict, onAdd, onDelete int) {
	verifyClientsWithGomega(gomega.Default, p, data.TopicCommon, active, evict, onAdd, onDelete)
}

func verifyClientsWithGomega(g gomega.Gomega, p *pub, topic bus.Topic, active, evict, onAdd, onDelete int) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	g.Expect(len(p.active)).Should(gomega.Equal(active))
	g.Expect(len(p.evictable)).Should(gomega.Equal(evict))
	for t, eh := range p.handlers {
		if topic != data.TopicCommon && t != topic {
			continue
		}
		h := eh.(*mockHandler)
		g.Expect(h.addOrUpdateCount).Should(gomega.Equal(onAdd), "topic: %s", t)
		g.Expect(h.deleteCount).Should(gomega.Equal(onDelete), "topic: %s", t)
	}
}
