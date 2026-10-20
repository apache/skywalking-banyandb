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

package grpc

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"golang.org/x/time/rate"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
)

// gatedChunkNode answers init with ready and, once gate is closed, one chunk per size whose
// byte i is byte(i).
type gatedChunkNode struct {
	transferv1.UnimplementedExportServiceServer
	gate   <-chan struct{}
	exited chan struct{}
	sizes  []int
	rows   uint64
}

func (g *gatedChunkNode) Export(stream transferv1.ExportService_ExportServer) error {
	defer close(g.exited)
	if _, err := stream.Recv(); err != nil {
		return err
	}
	if err := stream.Send(readyFrame()); err != nil {
		return err
	}
	select {
	case <-g.gate:
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
	for _, size := range g.sizes {
		content := make([]byte, size)
		for i := range content {
			content[i] = byte(i)
		}
		if err := stream.Send(&transferv1.ExportResponse{Frame: &transferv1.ExportResponse_Chunk{Chunk: &transferv1.DataChunk{Content: content, Rows: g.rows}}}); err != nil {
			return err
		}
	}
	return nil
}

// relayedChunks is what one client received: every chunk frame and how the stream ended.
type relayedChunks struct {
	err    error
	chunks []*transferv1.DataChunk
}

// openRelays starts one relay per node at bytesPerSecond and returns their streams once each
// has relayed ready, so every relay is open on the liaison.
func openRelays(t *testing.T, client transferv1.ExportServiceClient, bytesPerSecond uint64,
	nodes ...string,
) []transferv1.ExportService_ExportClient {
	t.Helper()
	streams := make([]transferv1.ExportService_ExportClient, 0, len(nodes))
	for _, node := range nodes {
		stream, err := client.Export(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if err = stream.Send(rateInit(node, bytesPerSecond)); err != nil {
			t.Fatal(err)
		}
		if _, err = stream.Recv(); err != nil {
			t.Fatal(err)
		}
		_ = stream.CloseSend()
		streams = append(streams, stream)
	}
	return streams
}

// receiveAll drains every stream concurrently until it ends.
func receiveAll(streams []transferv1.ExportService_ExportClient) []*relayedChunks {
	out := make([]*relayedChunks, len(streams))
	done := make(chan struct{}, len(streams))
	for i, stream := range streams {
		got := &relayedChunks{}
		out[i] = got
		//panicdiag:allow-rawgo the test reads every relay at once so that they share the rate
		go func() {
			defer func() { done <- struct{}{} }()
			for {
				frame, err := stream.Recv()
				if errors.Is(err, io.EOF) {
					return
				}
				if err != nil {
					got.err = err
					return
				}
				if chunk := frame.GetChunk(); chunk != nil {
					got.chunks = append(got.chunks, chunk)
				}
			}
		}()
	}
	for range streams {
		<-done
	}
	return out
}

// assertChunkBytes checks that the chunks rebuild size bytes where byte i is byte(i), each
// chunk carrying rows.
func assertChunkBytes(t *testing.T, got *relayedChunks, size int, rows uint64) {
	t.Helper()
	if got.err != nil {
		t.Fatal(got.err)
	}
	var content []byte
	for _, c := range got.chunks {
		if c.GetRows() != rows {
			t.Fatalf("a piece carries %d rows, want the chunk's %d", c.GetRows(), rows)
		}
		content = append(content, c.GetContent()...)
	}
	if len(content) != size {
		t.Fatalf("got %d bytes, want %d", len(content), size)
	}
	for i, b := range content {
		if b != byte(i) {
			t.Fatalf("byte %d is %d: the pieces must keep the chunk's bytes in order", i, b)
		}
	}
}

// Two relays at one rate share it: each relays pieces of half the burst, together they take
// the time the shared rate needs, both streams arrive whole and in order, and once both end
// the rate is unlimited again with the full burst.
func TestExportRelay_TwoRelaysShareOneRate(t *testing.T) {
	const bytesPerSecond, size = 128 << 10, 160 << 10
	gate := make(chan struct{})
	nodeA := &gatedChunkNode{gate: gate, exited: make(chan struct{}), sizes: []int{size}, rows: 3}
	nodeB := &gatedChunkNode{gate: gate, exited: make(chan struct{}), sizes: []int{size}, rows: 5}
	svc, client := newExportRelayService(t, map[string]transferv1.ExportServiceServer{testNodeA: nodeA, testNodeB: nodeB}, fanOutExtras{})
	streams := openRelays(t, client, bytesPerSecond, testNodeA, testNodeB)
	waitRelayRate(t, svc.relayRate, bytesPerSecond)
	if piece := svc.relayRate.pieceSize(); piece != bytesPerSecond/2 {
		t.Fatalf("two relays get pieces of %d bytes, want half the burst %d", piece, bytesPerSecond/2)
	}
	start := time.Now()
	close(gate)
	got := receiveAll(streams)
	elapsed := time.Since(start)

	assertChunkBytes(t, got[0], size, 3)
	assertChunkBytes(t, got[1], size, 5)
	for i, g := range got {
		for _, c := range g.chunks {
			if len(c.GetContent()) > bytesPerSecond/2 {
				t.Fatalf("relay %d sent a piece of %d bytes, want at most half the burst %d", i, len(c.GetContent()), bytesPerSecond/2)
			}
		}
		if len(g.chunks) != 3 {
			t.Fatalf("relay %d sent %d pieces, want 3 (64 KiB, 64 KiB, 32 KiB)", i, len(g.chunks))
		}
	}
	// 320 KiB at a shared 128 KiB/s from a full 128 KiB bucket: about 1.5s, not 0.25s per relay.
	if want := 1500 * time.Millisecond; elapsed < want*85/100 || elapsed > want+5*time.Second {
		t.Fatalf("both relays finished in %v, want about %v", elapsed, want)
	}

	waitClosed(t, nodeA.exited, "node a's handler must end")
	waitClosed(t, nodeB.exited, "node b's handler must end")
	waitRelayRate(t, svc.relayRate, rate.Inf)
	if piece := svc.relayRate.pieceSize(); piece != relayBurst {
		t.Fatalf("an idle relay goes back to whole chunks, got pieces of %d", piece)
	}
	if burst := svc.relayRate.limiter.Burst(); burst != relayBurst {
		t.Fatalf("an idle relay goes back to a burst of %d, got %d", relayBurst, burst)
	}
}

// Unlimited relays forward every chunk as it is, a full 1 MiB chunk included, whether one
// relay is active or two.
func TestExportRelay_UnlimitedRelaysNeverSplitChunks(t *testing.T) {
	sizes := []int{1 << 20, 16}
	for name, nodes := range map[string][]string{"one relay": {testNodeA}, "two relays": {testNodeA, testNodeB}} {
		t.Run(name, func(t *testing.T) {
			gate := make(chan struct{})
			servers := map[string]transferv1.ExportServiceServer{}
			var exited []chan struct{}
			for _, node := range nodes {
				n := &gatedChunkNode{gate: gate, exited: make(chan struct{}), sizes: sizes, rows: 1}
				servers[node] = n
				exited = append(exited, n.exited)
			}
			svc, client := newExportRelayService(t, servers, fanOutExtras{})
			streams := openRelays(t, client, 0, nodes...)
			close(gate)
			for i, g := range receiveAll(streams) {
				if g.err != nil {
					t.Fatal(g.err)
				}
				var got []int
				for _, c := range g.chunks {
					got = append(got, len(c.GetContent()))
				}
				if len(got) != len(sizes) || got[0] != sizes[0] || got[1] != sizes[1] {
					t.Fatalf("relay %d sent chunks of %v bytes, want them unsplit: %v", i, got, sizes)
				}
			}
			for _, ch := range exited {
				waitClosed(t, ch, "the node's handler must end")
			}
			waitRelayRate(t, svc.relayRate, rate.Inf)
		})
	}
}

// A wait canceled while it waits returns Canceled promptly and gives its reservation back,
// so the waits canceled before it do not delay the next one.
func TestRelayLimiter_CanceledWaitGivesItsReservationBack(t *testing.T) {
	const bytesPerSecond = 1000
	r := newRelayLimiter()
	defer r.open(bytesPerSecond)()
	// Empty the bucket: every following step of the burst waits about a second.
	if err := r.wait(context.Background(), bytesPerSecond); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		time.AfterFunc(50*time.Millisecond, cancel)
		start := time.Now()
		err := r.wait(ctx, bytesPerSecond)
		cancel()
		if status.Code(err) != codes.Canceled {
			t.Fatalf("wait %d: got %v, want Canceled", i, err)
		}
		if elapsed := time.Since(start); elapsed > 750*time.Millisecond {
			t.Fatalf("wait %d returned %v after it started, want right after the cancel", i, elapsed)
		}
	}
	// Without the three reservations given back the next second's worth would wait about 4s.
	start := time.Now()
	if err := r.wait(context.Background(), bytesPerSecond); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > 2500*time.Millisecond {
		t.Fatalf("the wait after canceled ones took %v, want about 1s", elapsed)
	}
}
