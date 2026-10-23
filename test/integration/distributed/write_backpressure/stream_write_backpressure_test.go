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

package write_backpressure_test

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	g "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
)

const (
	streamGroup = "default"
	streamName  = "sw"
)

// elementRow builds one element of the "sw" stream. Only the first row of a stream carries the metadata.
func elementRow(traceID string, messageID uint64, first bool) *streamv1.WriteRequest {
	req := &streamv1.WriteRequest{
		MessageId: messageID,
		Element: &streamv1.ElementValue{
			ElementId: fmt.Sprintf("%s-%d", traceID, messageID),
			Timestamp: timestamppb.New(time.Now().Add(-time.Minute).Truncate(time.Millisecond)),
			TagFamilies: []*modelv1.TagFamilyForWrite{
				{Tags: []*modelv1.TagValue{{Value: &modelv1.TagValue_BinaryData{BinaryData: []byte(traceID)}}}},
				{Tags: []*modelv1.TagValue{str(traceID), integer(0), str("svc"), str("svc-instance"), str("/endpoint"), integer(10), integer(1)}},
			},
		},
	}
	if first {
		req.Metadata = &commonv1.Metadata{Group: streamGroup, Name: streamName}
	}
	return req
}

// unknownElementRow is rejected by the liaison on its own, so its reply never waits for a data node.
func unknownElementRow(messageID uint64) *streamv1.WriteRequest {
	req := elementRow("unknown", messageID, false)
	req.Metadata = &commonv1.Metadata{Group: streamGroup, Name: "missing-stream", ModRevision: 1}
	return req
}

// streamAckCollector reads a Write stream in the background so a spec can observe acknowledgements while it is open.
type streamAckCollector struct {
	done chan struct{}
	err  error
	acks []*streamv1.WriteResponse
	mu   sync.Mutex
}

func collectStreamAcks(stream streamv1.StreamService_WriteClient) *streamAckCollector {
	c := &streamAckCollector{done: make(chan struct{})}
	go func() {
		defer close(c.done)
		for {
			resp, err := stream.Recv()
			c.mu.Lock()
			if err != nil {
				c.err = err
				c.mu.Unlock()
				return
			}
			c.acks = append(c.acks, resp)
			c.mu.Unlock()
		}
	}()
	return c
}

func (c *streamAckCollector) snapshot() []*streamv1.WriteResponse {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*streamv1.WriteResponse(nil), c.acks...)
}

func (c *streamAckCollector) streamErr() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

// ackOf returns the acknowledgement of one message, if it has arrived.
func (c *streamAckCollector) ackOf(messageID uint64) *streamv1.WriteResponse {
	for _, ack := range c.snapshot() {
		if ack.GetMessageId() == messageID {
			return ack
		}
	}
	return nil
}

// busyHint reports whether any reply other than the acknowledgements of messageIDs asks the client to pause.
func (c *streamAckCollector) busyHint(messageIDs ...uint64) bool {
	skip := make(map[uint64]struct{}, len(messageIDs))
	for _, id := range messageIDs {
		skip[id] = struct{}{}
	}
	for _, ack := range c.snapshot() {
		if _, ok := skip[ack.GetMessageId()]; !ok && ack.GetRetryAfterMs() > 0 {
			return true
		}
	}
	return false
}

func countElements(client streamv1.StreamServiceClient, traceID string) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := client.Query(ctx, &streamv1.QueryRequest{
		Groups: []string{streamGroup},
		Name:   streamName,
		TimeRange: &modelv1.TimeRange{
			Begin: timestamppb.New(time.Now().Add(-time.Hour).Truncate(time.Millisecond)),
			End:   timestamppb.New(time.Now().Add(time.Hour).Truncate(time.Millisecond)),
		},
		Projection: &modelv1.TagProjection{TagFamilies: []*modelv1.TagProjection_TagFamily{{Name: "searchable", Tags: []string{"trace_id"}}}},
		Criteria: &modelv1.Criteria{Exp: &modelv1.Criteria_Condition{Condition: &modelv1.Condition{
			Name: "trace_id", Op: modelv1.Condition_BINARY_OP_EQ, Value: str(traceID),
		}}},
		Limit: 10,
	})
	if err != nil {
		return 0, err
	}
	return len(resp.GetElements()), nil
}

// openAdmittedElementStream opens a Write stream and waits for the liaison to answer a row on it. A liaison under
// memory pressure sheds new streams, so a spec must have its stream admitted before it raises the pressure.
func openAdmittedElementStream(ctx context.Context, client streamv1.StreamServiceClient) (streamv1.StreamService_WriteClient, *streamAckCollector) {
	stream, err := client.Write(ctx)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	acks := collectStreamAcks(stream)
	gomega.Expect(stream.Send(unknownElementRow(99))).To(gomega.Succeed())
	gomega.Eventually(func() *streamv1.WriteResponse { return acks.ackOf(99) }, flags.EventuallyTimeout).ShouldNot(gomega.BeNil())
	return stream, acks
}

var _ = g.Describe("Stream Write backpressure from the liaison write listener under memory pressure", func() {
	var conn *grpclib.ClientConn
	var client streamv1.StreamServiceClient

	g.BeforeEach(func() {
		var err error
		conn, err = grpclib.NewClient(liaisonAddr, grpclib.WithTransportCredentials(insecure.NewCredentials()))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		client = streamv1.NewStreamServiceClient(conn)
		g.DeferCleanup(func() {
			gomega.Expect(setup.SetLiaisonMemoryHigh(liaisonAddr, false)).To(gomega.Succeed())
			gomega.Expect(conn.Close()).To(gomega.Succeed())
		})
		// Wait until the liaison can route and acknowledge a write while the data node is healthy.
		gomega.Eventually(func(innerGm gomega.Gomega) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			stream, streamErr := client.Write(ctx)
			innerGm.Expect(streamErr).NotTo(gomega.HaveOccurred())
			innerGm.Expect(stream.Send(elementRow("bp-warmup", 1, true))).To(gomega.Succeed())
			innerGm.Expect(stream.CloseSend()).To(gomega.Succeed())
			ack, recvErr := stream.Recv()
			innerGm.Expect(recvErr).NotTo(gomega.HaveOccurred())
			innerGm.Expect(ack.GetStatusCode()).To(gomega.Equal(modelv1.Status_STATUS_SUCCEED))
		}, flags.EventuallyTimeout).Should(gomega.Succeed())
	})

	g.It("asks the client to pause while the write listener is busy, then acknowledges and stores the rows", func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		stream, acks := openAdmittedElementStream(ctx, client)
		gomega.Expect(setup.SetLiaisonMemoryHigh(liaisonAddr, true)).To(gomega.Succeed())
		traceIDs := []string{"bp-ok-1", "bp-ok-2", "bp-ok-3"}
		for i, traceID := range traceIDs {
			gomega.Expect(stream.Send(elementRow(traceID, uint64(i+1), i == 0))).To(gomega.Succeed())
		}

		g.By("observing retry_after_ms on replies while the batch is rejected as busy")
		messageID := uint64(100)
		gomega.Eventually(func() bool {
			messageID++
			gomega.Expect(stream.Send(unknownElementRow(messageID))).To(gomega.Succeed())
			return acks.busyHint(1, 2, 3)
		}, flags.EventuallyTimeout, 100*time.Millisecond).Should(gomega.BeTrue())
		// The seal interval is 200ms, so a batch that was not rejected would be acknowledged well within this window.
		gomega.Consistently(func() bool {
			return acks.ackOf(1) == nil && acks.ackOf(2) == nil && acks.ackOf(3) == nil
		}, 1500*time.Millisecond, 100*time.Millisecond).Should(gomega.BeTrue(), "rows must not be acknowledged while the listener is busy")
		for _, ack := range acks.snapshot() {
			gomega.Expect(ack.GetStatusCode()).To(gomega.Equal(modelv1.Status_STATUS_NOT_FOUND))
		}
		gomega.Expect(acks.streamErr()).NotTo(gomega.HaveOccurred(), "the stream stays open while pressure is shorter than the timeout")

		g.By("releasing the pressure")
		gomega.Expect(setup.SetLiaisonMemoryHigh(liaisonAddr, false)).To(gomega.Succeed())
		gomega.Eventually(func(innerGm gomega.Gomega) {
			for id := uint64(1); id <= 3; id++ {
				ack := acks.ackOf(id)
				innerGm.Expect(ack).NotTo(gomega.BeNil(), "row %d", id)
				innerGm.Expect(ack.GetStatusCode()).To(gomega.Equal(modelv1.Status_STATUS_SUCCEED), "row %d", id)
			}
		}, flags.EventuallyTimeout, 50*time.Millisecond).Should(gomega.Succeed())

		gomega.Expect(stream.CloseSend()).To(gomega.Succeed())
		select {
		case <-acks.done:
		case <-time.After(20 * time.Second):
			g.Fail("the stream did not end after half-close")
		}
		gomega.Expect(acks.streamErr()).To(gomega.MatchError(io.EOF))

		g.By("querying the rows that were held back")
		for _, traceID := range traceIDs {
			gomega.Eventually(func() (int, error) { return countElements(client, traceID) }, flags.EventuallyTimeout, 200*time.Millisecond).
				Should(gomega.Equal(1), traceID)
		}
	})

	g.It("ends the stream with RESOURCE_EXHAUSTED when the pressure outlasts --write-pressure-timeout", func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		stream, acks := openAdmittedElementStream(ctx, client)
		gomega.Expect(setup.SetLiaisonMemoryHigh(liaisonAddr, true)).To(gomega.Succeed())
		start := time.Now()
		for i := 0; i < 2; i++ {
			gomega.Expect(stream.Send(elementRow(fmt.Sprintf("bp-timeout-%d", i), uint64(i+1), i == 0))).To(gomega.Succeed())
		}
		select {
		case <-acks.done:
		case <-time.After(30 * time.Second):
			g.Fail("the liaison did not end the stream after the pressure timeout")
		}
		gomega.Expect(status.Code(acks.streamErr())).To(gomega.Equal(codes.ResourceExhausted), "%v", acks.streamErr())
		gomega.Expect(time.Since(start)).To(gomega.BeNumerically(">=", 3*time.Second), "the batch is retried before the stream is failed")
		for id := uint64(1); id <= 2; id++ {
			ack := acks.ackOf(id)
			gomega.Expect(ack).NotTo(gomega.BeNil(), "row %d", id)
			gomega.Expect(ack.GetStatusCode()).To(gomega.Equal(modelv1.Status_STATUS_SERVER_BUSY), "row %d", id)
		}
	})
})
