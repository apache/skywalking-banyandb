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
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/apache/skywalking-banyandb/api/data"
	clusterv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/cluster/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bus"
)

type scanHandler func(req *clusterv1.ScanRequest, stream clusterv1.Service_ScanServer) error

type scanServer struct {
	clusterv1.UnimplementedServiceServer
	handler scanHandler
}

func (s *scanServer) Scan(req *clusterv1.ScanRequest, stream clusterv1.Service_ScanServer) error {
	return s.handler(req, stream)
}

func (s *scanServer) HealthCheck(context.Context, *clusterv1.HealthCheckRequest) (*clusterv1.HealthCheckResponse, error) {
	return &clusterv1.HealthCheckResponse{}, nil
}

// startScanServer serves handler and returns its address and a hard stop.
func startScanServer(t *testing.T, handler scanHandler) (string, func()) {
	t.Helper()
	srv := grpc.NewServer()
	clusterv1.RegisterServiceServer(srv, &scanServer{handler: handler})
	grpc_health_v1.RegisterHealthServer(srv, health.NewServer())
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = srv.Serve(lis) }()
	var once sync.Once
	stop := func() { once.Do(srv.Stop) }
	t.Cleanup(stop)
	return lis.Addr().String(), stop
}

func chunk(t *testing.T, seq uint32, traceID string, last bool) *clusterv1.ScanResponse {
	t.Helper()
	body, err := proto.Marshal(&tracev1.InternalQueryResponse{InternalTraces: []*tracev1.InternalTrace{{TraceId: traceID}}})
	require.NoError(t, err)
	return &clusterv1.ScanResponse{Seq: seq, Body: body, Last: last}
}

func traceIDOf(t *testing.T, m bus.Message) string {
	t.Helper()
	resp, ok := m.Data().(*tracev1.InternalQueryResponse)
	require.True(t, ok, "unexpected chunk type %T", m.Data())
	require.Len(t, resp.InternalTraces, 1)
	return resp.InternalTraces[0].TraceId
}

func newScatterPub(t *testing.T, nodes map[string]string) *pub {
	t.Helper()
	p := newPub()
	t.Cleanup(p.GracefulStop)
	for name, addr := range nodes {
		p.OnAddOrUpdate(getDataNode(name, addr))
	}
	return p
}

func scatterOne(ctx context.Context, t *testing.T, p *pub, opts queue.ScatterOptions) queue.NodeStream {
	t.Helper()
	streams, err := p.Scatter(ctx, data.TopicTraceQuery, bus.NewMessage(7, &tracev1.QueryRequest{}), opts)
	require.NoError(t, err)
	require.Len(t, streams, 1)
	t.Cleanup(streams[0].Close)
	return streams[0]
}

func TestScatterDeliversChunksInOrderThenEOF(t *testing.T) {
	var gotReq *clusterv1.ScanRequest
	addr, _ := startScanServer(t, func(req *clusterv1.ScanRequest, stream clusterv1.Service_ScanServer) error {
		gotReq = req
		for i, id := range []string{"t1", "t2", "t3"} {
			if err := stream.Send(chunk(t, uint32(i+1), id, false)); err != nil {
				return err
			}
		}
		// The last chunk may carry an empty body.
		return stream.Send(&clusterv1.ScanResponse{Seq: 4, Last: true})
	})
	p := newScatterPub(t, map[string]string{"n1": addr})
	ns := scatterOne(context.Background(), t, p, queue.ScatterOptions{MaxChunkBytes: 4096, MaxChunkRows: 16})
	assert.Equal(t, "n1", ns.Node())
	for _, want := range []string{"t1", "t2", "t3"} {
		m, err := ns.Next(context.Background())
		require.NoError(t, err)
		assert.Equal(t, want, traceIDOf(t, m))
		assert.Equal(t, "n1", m.Node())
	}
	_, err := ns.Next(context.Background())
	assert.ErrorIs(t, err, io.EOF)
	_, err = ns.Next(context.Background())
	assert.ErrorIs(t, err, io.EOF)
	require.NotNil(t, gotReq)
	assert.Equal(t, data.TopicTraceQuery.String(), gotReq.Topic)
	assert.EqualValues(t, 7, gotReq.MessageId)
	assert.EqualValues(t, 4096, gotReq.MaxChunkBytes)
	assert.EqualValues(t, 16, gotReq.MaxChunkRows)
	assert.NotNil(t, gotReq.VersionInfo)
}

func TestScatterLastChunkWithBodyIsDeliveredBeforeEOF(t *testing.T) {
	addr, _ := startScanServer(t, func(_ *clusterv1.ScanRequest, stream clusterv1.Service_ScanServer) error {
		return stream.Send(chunk(t, 1, "only", true))
	})
	p := newScatterPub(t, map[string]string{"n1": addr})
	ns := scatterOne(context.Background(), t, p, queue.ScatterOptions{})
	m, err := ns.Next(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "only", traceIDOf(t, m))
	_, err = ns.Next(context.Background())
	assert.ErrorIs(t, err, io.EOF)
}

func TestScatterHeartbeatsKeepASlowStreamAlive(t *testing.T) {
	const idle = 300 * time.Millisecond
	release := make(chan struct{})
	addr, _ := startScanServer(t, func(_ *clusterv1.ScanRequest, stream clusterv1.Service_ScanServer) error {
		// Heartbeat well within the idle timeout until released, so that the silence
		// before the first chunk outlives the idle timeout several times over.
		ticker := time.NewTicker(idle / 6)
		defer ticker.Stop()
		beats := 0
		for beats < 20 {
			select {
			case <-ticker.C:
				if err := stream.Send(&clusterv1.ScanResponse{}); err != nil {
					return err
				}
				beats++
			case <-stream.Context().Done():
				return stream.Context().Err()
			}
		}
		close(release)
		return stream.Send(chunk(t, 1, "late", true))
	})
	p := newScatterPub(t, map[string]string{"n1": addr})
	ns := scatterOne(context.Background(), t, p, queue.ScatterOptions{IdleTimeout: idle})
	m, err := ns.Next(context.Background())
	require.NoError(t, err, "heartbeats must keep the stream alive")
	assert.Equal(t, "late", traceIDOf(t, m), "heartbeats are not delivered to Next")
	select {
	case <-release:
	default:
		t.Fatal("the chunk arrived before the heartbeats finished")
	}
	_, err = ns.Next(context.Background())
	assert.ErrorIs(t, err, io.EOF)
}

func TestScatterIdleTimeoutFiresOnASilentNode(t *testing.T) {
	serverCtxDone := make(chan struct{})
	addr, _ := startScanServer(t, func(_ *clusterv1.ScanRequest, stream clusterv1.Service_ScanServer) error {
		<-stream.Context().Done()
		close(serverCtxDone)
		return stream.Context().Err()
	})
	p := newScatterPub(t, map[string]string{"n1": addr})
	ns := scatterOne(context.Background(), t, p, queue.ScatterOptions{IdleTimeout: 150 * time.Millisecond})
	_, err := ns.Next(context.Background())
	require.Error(t, err)
	assert.ErrorIs(t, err, queue.ErrScanIdleTimeout)
	assert.NotErrorIs(t, err, io.EOF)
	<-serverCtxDone // the failed stream is canceled on the node too
}

func TestScatterTransportErrorIsNotEOF(t *testing.T) {
	addr, _ := startScanServer(t, func(_ *clusterv1.ScanRequest, stream clusterv1.Service_ScanServer) error {
		if err := stream.Send(chunk(t, 1, "t1", false)); err != nil {
			return err
		}
		return status.Error(codes.Internal, "disk on fire")
	})
	p := newScatterPub(t, map[string]string{"n1": addr})
	ns := scatterOne(context.Background(), t, p, queue.ScatterOptions{})
	m, err := ns.Next(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "t1", traceIDOf(t, m))
	_, err = ns.Next(context.Background())
	require.Error(t, err)
	assert.NotErrorIs(t, err, io.EOF)
	assert.Contains(t, err.Error(), "disk on fire")
	_, err2 := ns.Next(context.Background())
	assert.Equal(t, err.Error(), err2.Error(), "the terminal error is sticky")
}

func TestScatterServerEndingWithoutLastChunkIsNotEOF(t *testing.T) {
	addr, _ := startScanServer(t, func(_ *clusterv1.ScanRequest, stream clusterv1.Service_ScanServer) error {
		return stream.Send(chunk(t, 1, "t1", false))
	})
	p := newScatterPub(t, map[string]string{"n1": addr})
	ns := scatterOne(context.Background(), t, p, queue.ScatterOptions{})
	_, err := ns.Next(context.Background())
	require.NoError(t, err)
	_, err = ns.Next(context.Background())
	require.Error(t, err)
	assert.NotErrorIs(t, err, io.EOF)
	assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
}

func TestScatterErrorChunkIsTerminal(t *testing.T) {
	addr, _ := startScanServer(t, func(_ *clusterv1.ScanRequest, stream clusterv1.Service_ScanServer) error {
		return stream.Send(&clusterv1.ScanResponse{Error: "scan failed"})
	})
	p := newScatterPub(t, map[string]string{"n1": addr})
	ns := scatterOne(context.Background(), t, p, queue.ScatterOptions{})
	_, err := ns.Next(context.Background())
	require.Error(t, err)
	assert.NotErrorIs(t, err, io.EOF)
	assert.Contains(t, err.Error(), "scan failed")
}

func TestScatterCallerCancellationReachesTheNode(t *testing.T) {
	serverCtxDone := make(chan struct{})
	started := make(chan struct{})
	addr, _ := startScanServer(t, func(_ *clusterv1.ScanRequest, stream clusterv1.Service_ScanServer) error {
		close(started)
		<-stream.Context().Done()
		close(serverCtxDone)
		return stream.Context().Err()
	})
	p := newScatterPub(t, map[string]string{"n1": addr})
	ctx, cancel := context.WithCancel(context.Background())
	ns := scatterOne(ctx, t, p, queue.ScatterOptions{})
	<-started
	cancel()
	<-serverCtxDone
	_, err := ns.Next(context.Background())
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
	assert.NotErrorIs(t, err, io.EOF)
}

func TestScatterCloseCancelsTheNodeScan(t *testing.T) {
	serverCtxDone := make(chan struct{})
	started := make(chan struct{})
	addr, _ := startScanServer(t, func(_ *clusterv1.ScanRequest, stream clusterv1.Service_ScanServer) error {
		close(started)
		<-stream.Context().Done()
		close(serverCtxDone)
		return stream.Context().Err()
	})
	p := newScatterPub(t, map[string]string{"n1": addr})
	ns := scatterOne(context.Background(), t, p, queue.ScatterOptions{})
	<-started
	ns.Close()
	<-serverCtxDone
	_, err := ns.Next(context.Background())
	assert.ErrorIs(t, err, queue.ErrStreamClosed)
	ns.Close() // idempotent
}

func TestScatterPrefetchBoundsBufferedChunks(t *testing.T) {
	const prefetch = 2
	const total = 12
	addr, _ := startScanServer(t, func(_ *clusterv1.ScanRequest, stream clusterv1.Service_ScanServer) error {
		for i := 0; i < total; i++ {
			if err := stream.Send(chunk(t, uint32(i+1), string(rune('a'+i)), i == total-1)); err != nil {
				return err
			}
		}
		return nil
	})
	p := newScatterPub(t, map[string]string{"n1": addr})
	ns := scatterOne(context.Background(), t, p, queue.ScatterOptions{Prefetch: prefetch}).(*nodeStream)
	// The receive goroutine fills the buffer, then holds one more chunk in hand and blocks.
	require.Eventually(t, func() bool { return len(ns.ch) == prefetch }, 5*time.Second, 5*time.Millisecond)
	require.Never(t, func() bool { return len(ns.ch) > prefetch }, 100*time.Millisecond, 10*time.Millisecond)
	for i := 0; i < total; i++ {
		m, err := ns.Next(context.Background())
		require.NoError(t, err)
		assert.Equal(t, string(rune('a'+i)), traceIDOf(t, m))
	}
	_, err := ns.Next(context.Background())
	assert.ErrorIs(t, err, io.EOF)
}

func TestScatterIdleClockPausesWhileTheConsumerLags(t *testing.T) {
	const idle = 200 * time.Millisecond
	addr, _ := startScanServer(t, func(_ *clusterv1.ScanRequest, stream clusterv1.Service_ScanServer) error {
		for i := 0; i < 5; i++ {
			if err := stream.Send(chunk(t, uint32(i+1), string(rune('a'+i)), i == 4)); err != nil {
				return err
			}
		}
		return nil
	})
	p := newScatterPub(t, map[string]string{"n1": addr})
	ns := scatterOne(context.Background(), t, p, queue.ScatterOptions{Prefetch: 1, IdleTimeout: idle}).(*nodeStream)
	require.Eventually(t, func() bool { return len(ns.ch) == 1 }, 5*time.Second, 5*time.Millisecond)
	// Stay away for longer than the idle timeout: the node is not silent, we are slow.
	time.Sleep(3 * idle)
	for i := 0; i < 5; i++ {
		m, err := ns.Next(context.Background())
		require.NoError(t, err)
		assert.Equal(t, string(rune('a'+i)), traceIDOf(t, m))
	}
	_, err := ns.Next(context.Background())
	assert.ErrorIs(t, err, io.EOF)
}

func TestScatterPartialOpenFailureClosesOpenedStreams(t *testing.T) {
	firstCtxDone := make(chan struct{})
	started := make(chan struct{})
	addrA, _ := startScanServer(t, func(_ *clusterv1.ScanRequest, stream clusterv1.Service_ScanServer) error {
		close(started)
		<-stream.Context().Done()
		close(firstCtxDone)
		return stream.Context().Err()
	})
	addrB, stopB := startScanServer(t, func(*clusterv1.ScanRequest, clusterv1.Service_ScanServer) error { return nil })
	// Nodes open in name order: "a" opens before "b".
	p := newScatterPub(t, map[string]string{"a": addrA, "b": addrB})
	stopB()
	_, err := p.Scatter(context.Background(), data.TopicTraceQuery, bus.NewMessage(1, &tracev1.QueryRequest{}),
		queue.ScatterOptions{IdleTimeout: 300 * time.Millisecond})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "b")
	<-started
	<-firstCtxDone // the stream opened to "a" was closed
}

func TestScatterHonoursNodeSelectors(t *testing.T) {
	var mu sync.Mutex
	hits := map[string]int{}
	handler := func(name string) scanHandler {
		return func(_ *clusterv1.ScanRequest, stream clusterv1.Service_ScanServer) error {
			mu.Lock()
			hits[name]++
			mu.Unlock()
			return stream.Send(&clusterv1.ScanResponse{Last: true})
		}
	}
	addrHot, _ := startScanServer(t, handler("hot"))
	addrCold, _ := startScanServer(t, handler("cold"))
	p := newPub()
	t.Cleanup(p.GracefulStop)
	p.OnAddOrUpdate(getDataNodeWithLabels("hot", addrHot, map[string]string{"type": "hot"}))
	p.OnAddOrUpdate(getDataNodeWithLabels("cold", addrCold, map[string]string{"type": "cold"}))

	msg := bus.NewMessageWithNodeSelectors(1, map[string][]string{"g": {"type=cold"}}, nil, &tracev1.QueryRequest{})
	streams, err := p.Scatter(context.Background(), data.TopicTraceQuery, msg, queue.ScatterOptions{})
	require.NoError(t, err)
	require.Len(t, streams, 1)
	assert.Equal(t, "cold", streams[0].Node())
	_, err = streams[0].Next(context.Background())
	assert.ErrorIs(t, err, io.EOF)
	streams[0].Close()

	all, err := p.Scatter(context.Background(), data.TopicTraceQuery, bus.NewMessage(2, &tracev1.QueryRequest{}), queue.ScatterOptions{})
	require.NoError(t, err)
	require.Len(t, all, 2)
	for _, s := range all {
		_, err = s.Next(context.Background())
		assert.ErrorIs(t, err, io.EOF)
		s.Close()
	}

	none := bus.NewMessageWithNodeSelectors(3, map[string][]string{"g": {"type=warm"}}, nil, &tracev1.QueryRequest{})
	_, err = p.Scatter(context.Background(), data.TopicTraceQuery, none, queue.ScatterOptions{})
	require.Error(t, err)
	assert.False(t, errors.Is(err, io.EOF))

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, map[string]int{"hot": 1, "cold": 2}, hits)
}
