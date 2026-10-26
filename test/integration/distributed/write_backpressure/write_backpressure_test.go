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
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
)

const (
	traceGroup = "test-trace-group"
	traceName  = "sw"
)

func str(v string) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: v}}}
}

func integer(v int64) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: v}}}
}

// spanRow builds one row of the "sw" trace in schema tag order. Only the first row of a stream carries the metadata.
func spanRow(traceID string, version uint64, first bool) *tracev1.WriteRequest {
	req := &tracev1.WriteRequest{
		Version: version,
		Tags: []*modelv1.TagValue{
			str(traceID), integer(0), str("svc"), str("svc-instance"), str("/endpoint"), integer(10), str(fmt.Sprintf("%s-span", traceID)),
			{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(time.Now().Add(-time.Minute).Truncate(time.Millisecond))}},
		},
		Span: []byte(traceID),
	}
	if first {
		req.Metadata = &commonv1.Metadata{Group: traceGroup, Name: traceName}
	}
	return req
}

// unknownTraceRow is rejected by the liaison on its own, so its reply never waits for a data node.
func unknownTraceRow(version uint64) *tracev1.WriteRequest {
	req := spanRow("unknown", version, false)
	req.Metadata = &commonv1.Metadata{Group: traceGroup, Name: "missing-trace"}
	return req
}

// ackCollector reads a Write stream in the background so a spec can observe acknowledgements while the stream is open.
type ackCollector struct {
	done chan struct{}
	err  error
	acks []*tracev1.WriteResponse
	mu   sync.Mutex
}

func collectAcks(stream tracev1.TraceService_WriteClient) *ackCollector {
	c := &ackCollector{done: make(chan struct{})}
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

func (c *ackCollector) snapshot() []*tracev1.WriteResponse {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*tracev1.WriteResponse(nil), c.acks...)
}

func (c *ackCollector) streamErr() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

// ackOf returns the acknowledgement of one version, if it has arrived.
func (c *ackCollector) ackOf(version uint64) *tracev1.WriteResponse {
	for _, ack := range c.snapshot() {
		if ack.GetVersion() == version {
			return ack
		}
	}
	return nil
}

// busyHint reports whether any reply other than the acknowledgements of versions asks the client to pause.
func (c *ackCollector) busyHint(versions ...uint64) bool {
	skip := make(map[uint64]struct{}, len(versions))
	for _, v := range versions {
		skip[v] = struct{}{}
	}
	for _, ack := range c.snapshot() {
		if _, ok := skip[ack.GetVersion()]; !ok && ack.GetRetryAfterMs() > 0 {
			return true
		}
	}
	return false
}

func countTraces(client tracev1.TraceServiceClient, traceID string) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := client.Query(ctx, &tracev1.QueryRequest{
		Groups: []string{traceGroup},
		Name:   traceName,
		TimeRange: &modelv1.TimeRange{
			Begin: timestamppb.New(time.Now().Add(-time.Hour).Truncate(time.Millisecond)),
			End:   timestamppb.New(time.Now().Add(time.Hour).Truncate(time.Millisecond)),
		},
		TagProjection: []string{"trace_id"},
		Criteria: &modelv1.Criteria{Exp: &modelv1.Criteria_Condition{Condition: &modelv1.Condition{
			Name: "trace_id", Op: modelv1.Condition_BINARY_OP_EQ, Value: str(traceID),
		}}},
	})
	if err != nil {
		return 0, err
	}
	return len(resp.GetTraces()), nil
}

// openAdmittedStream opens a Write stream and waits for the liaison to answer a row on it. A liaison under memory
// pressure sheds new streams, so a spec must have its stream admitted before it raises the pressure.
func openAdmittedStream(ctx context.Context, client tracev1.TraceServiceClient) (tracev1.TraceService_WriteClient, *ackCollector) {
	stream, err := client.Write(ctx)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	acks := collectAcks(stream)
	gomega.Expect(stream.Send(unknownTraceRow(99))).To(gomega.Succeed())
	gomega.Eventually(func() *tracev1.WriteResponse { return acks.ackOf(99) }, flags.EventuallyTimeout).ShouldNot(gomega.BeNil())
	return stream, acks
}

var _ = g.Describe("Trace Write backpressure from the liaison write listener under memory pressure", func() {
	var conn *grpclib.ClientConn
	var client tracev1.TraceServiceClient

	g.BeforeEach(func() {
		var err error
		conn, err = grpclib.NewClient(liaisonAddr, grpclib.WithTransportCredentials(insecure.NewCredentials()))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		client = tracev1.NewTraceServiceClient(conn)
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
			innerGm.Expect(stream.Send(spanRow("bp-warmup", 1, true))).To(gomega.Succeed())
			innerGm.Expect(stream.CloseSend()).To(gomega.Succeed())
			ack, recvErr := stream.Recv()
			innerGm.Expect(recvErr).NotTo(gomega.HaveOccurred())
			innerGm.Expect(ack.GetStatusCode()).To(gomega.Equal(modelv1.Status_STATUS_SUCCEED))
		}, flags.EventuallyTimeout).Should(gomega.Succeed())
	})

	g.It("asks the client to pause while the write listener is busy, then acknowledges and stores the rows", func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		stream, acks := openAdmittedStream(ctx, client)
		gomega.Expect(setup.SetLiaisonMemoryHigh(liaisonAddr, true)).To(gomega.Succeed())
		traceIDs := []string{"bp-ok-1", "bp-ok-2", "bp-ok-3"}
		for i, traceID := range traceIDs {
			gomega.Expect(stream.Send(spanRow(traceID, uint64(i+1), i == 0))).To(gomega.Succeed())
		}

		g.By("observing retry_after_ms on replies while the batch is rejected as busy")
		version := uint64(100)
		gomega.Eventually(func() bool {
			version++
			gomega.Expect(stream.Send(unknownTraceRow(version))).To(gomega.Succeed())
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
			for v := uint64(1); v <= 3; v++ {
				ack := acks.ackOf(v)
				innerGm.Expect(ack).NotTo(gomega.BeNil(), "row %d", v)
				innerGm.Expect(ack.GetStatusCode()).To(gomega.Equal(modelv1.Status_STATUS_SUCCEED), "row %d", v)
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
			gomega.Eventually(func() (int, error) { return countTraces(client, traceID) }, flags.EventuallyTimeout, 200*time.Millisecond).
				Should(gomega.Equal(1), traceID)
		}
	})

	g.It("ends the stream with RESOURCE_EXHAUSTED when the pressure outlasts --write-pressure-timeout", func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		stream, acks := openAdmittedStream(ctx, client)
		gomega.Expect(setup.SetLiaisonMemoryHigh(liaisonAddr, true)).To(gomega.Succeed())
		start := time.Now()
		for i := 0; i < 2; i++ {
			gomega.Expect(stream.Send(spanRow(fmt.Sprintf("bp-timeout-%d", i), uint64(i+1), i == 0))).To(gomega.Succeed())
		}
		select {
		case <-acks.done:
		case <-time.After(30 * time.Second):
			g.Fail("the liaison did not end the stream after the pressure timeout")
		}
		gomega.Expect(status.Code(acks.streamErr())).To(gomega.Equal(codes.ResourceExhausted), "%v", acks.streamErr())
		gomega.Expect(time.Since(start)).To(gomega.BeNumerically(">=", 3*time.Second), "the batch is retried before the stream is failed")
		for v := uint64(1); v <= 2; v++ {
			ack := acks.ackOf(v)
			gomega.Expect(ack).NotTo(gomega.BeNil(), "row %d", v)
			gomega.Expect(ack.GetStatusCode()).To(gomega.Equal(modelv1.Status_STATUS_SERVER_BUSY), "row %d", v)
		}
	})
})
