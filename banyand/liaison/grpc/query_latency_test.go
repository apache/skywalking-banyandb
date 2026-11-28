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
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"

	bydbqlv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/bydbql/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/banyand/observability/services"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/meter/prom"
)

// queryLatencyCounts returns the number of observations of query_latency_seconds, keyed by "service/status".
func queryLatencyCounts(t *testing.T, reg *prometheus.Registry) map[string]uint64 {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	counts := make(map[string]uint64)
	for _, family := range families {
		if family.GetName() != "banyandb_liaison_grpc_query_latency_seconds" {
			continue
		}
		for _, metric := range family.GetMetric() {
			labels := make(map[string]string)
			for _, label := range metric.GetLabel() {
				labels[label.GetName()] = label.GetValue()
			}
			require.Len(t, labels, 2, "only service and status are labels")
			counts[labels["service"]+"/"+labels["status"]] += metric.GetHistogram().GetSampleCount()
		}
	}
	return counts
}

func newQueryLatencyMeasureService() (*measureService, *metrics, *prometheus.Registry) {
	reg := prometheus.NewRegistry()
	m := newMetrics(services.NewFactory(prom.NewProvider(liaisonGrpcScope, reg), nil, nil))
	svc := &measureService{
		discoveryService: &discoveryService{groupRepo: &groupRepo{inflight: make(map[string]*groupInflight)}},
		broadcaster:      topNBroadcaster{resp: bus.NewMessage(1, &measurev1.TopNResponse{})},
		l:                logger.GetLogger("test"),
		metrics:          m,
	}
	return svc, m, reg
}

// TestQueryLatencyInterceptor drives the real measure handlers through the interceptor: a request is observed once
// however many groups it spans, with the status of its outcome.
func TestQueryLatencyInterceptor(t *testing.T) {
	now := timestamppb.New(time.Now().Truncate(time.Millisecond))
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	expiredCtx, expire := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer expire()
	tests := []struct {
		ctx    context.Context
		call   func(ctx context.Context, svc *measureService) error
		name   string
		method string
		want   string
	}{
		{
			name: "multi-group topn", method: measurev1.MeasureService_TopN_FullMethodName, want: "topn/ok", ctx: context.Background(),
			call: func(ctx context.Context, svc *measureService) error {
				_, err := svc.TopN(ctx, &measurev1.TopNRequest{Groups: []string{"g1", "g2"}, TimeRange: &modelv1.TimeRange{Begin: now, End: now}})
				return err
			},
		},
		{
			name: "multi-group measure query that fails", method: measurev1.MeasureService_Query_FullMethodName, want: "measure/err", ctx: context.Background(),
			call: func(ctx context.Context, svc *measureService) error {
				_, err := svc.Query(ctx, &measurev1.QueryRequest{Groups: []string{"g1", "g2"}})
				return err
			},
		},
		{
			name: "multi-group measure query canceled by the client", method: measurev1.MeasureService_Query_FullMethodName,
			want: "measure/canceled", ctx: canceledCtx,
			call: func(ctx context.Context, svc *measureService) error {
				_, err := svc.Query(ctx, &measurev1.QueryRequest{Groups: []string{"g1", "g2"}, TimeRange: &modelv1.TimeRange{Begin: now, End: now}})
				return err
			},
		},
		{
			name: "multi-group measure query whose deadline expired", method: measurev1.MeasureService_Query_FullMethodName,
			want: "measure/deadline_exceeded", ctx: expiredCtx,
			call: func(ctx context.Context, svc *measureService) error {
				_, err := svc.Query(ctx, &measurev1.QueryRequest{Groups: []string{"g1", "g2"}, TimeRange: &modelv1.TimeRange{Begin: now, End: now}})
				return err
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc, m, reg := newQueryLatencyMeasureService()
			_, err := newQueryLatencyInterceptor(m)(tt.ctx, nil, &grpc.UnaryServerInfo{FullMethod: tt.method},
				func(ctx context.Context, _ any) (any, error) { return nil, tt.call(ctx, svc) })
			require.Equal(t, tt.want != "topn/ok", err != nil)
			require.Equal(t, map[string]uint64{tt.want: 1}, queryLatencyCounts(t, reg))
		})
	}
}

// TestQueryLatencyBydbQLTopN checks that a BydbQL TopN query, which the handler dispatches to the TopN method as a
// Go call, is observed under bydbql only.
func TestQueryLatencyBydbQLTopN(t *testing.T) {
	svc, m, reg := newQueryLatencyMeasureService()
	bydbql := &bydbQLService{metrics: m, l: logger.GetLogger("test"), cache: newPreparedCache(16, 1<<20, m), measureSvc: svc}
	_, err := newQueryLatencyInterceptor(m)(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: bydbQLQueryFullMethod},
		func(ctx context.Context, _ any) (any, error) {
			return bydbql.Query(ctx, &bydbqlv1.QueryRequest{Query: "SHOW TOP 10 FROM MEASURE service_latency IN g1, g2 TIME > '-30m' ORDER BY DESC"})
		})
	require.NoError(t, err)
	require.Equal(t, float64(2), counterSeries(t, reg, "total_started")["topn/query"], "the TopN handler ran for both groups")
	require.Equal(t, map[string]uint64{"bydbql/ok": 1}, queryLatencyCounts(t, reg))
}

// TestQueryLatencyStatusFollowsRequestContext checks that the status depends on the request context, not on the type
// of the error: the distributed stream, trace and TopN paths return wrapped errors that carry no gRPC code and do not
// unwrap to a context error, so only the context tells that the client went away.
func TestQueryLatencyStatusFollowsRequestContext(t *testing.T) {
	wrapped := errors.New("execute the query plan for stream log: stream query on node data-0:17912: context canceled")
	tests := []struct {
		err       error
		name      string
		want      string
		deadline  time.Duration
		ctxEnds   bool
		ctxExpire bool
	}{
		{name: "success", want: "stream/ok"},
		{name: "plain error", err: wrapped, want: "stream/err"},
		{name: "wrapped plain error after the client canceled", err: wrapped, ctxEnds: true, want: "stream/canceled"},
		{name: "wrapped plain error after the client deadline", err: wrapped, ctxEnds: true, ctxExpire: true, want: "stream/deadline_exceeded"},
		{
			name: "wrapped plain error after the client canceled within the tolerance of its deadline", err: wrapped, ctxEnds: true,
			deadline: 50 * time.Millisecond, want: "stream/deadline_exceeded",
		},
		// A context error while the request context is alive comes from a failed node connection, not from the client.
		{name: "context.Canceled while the request context is alive", err: context.Canceled, want: "stream/err"},
		{name: "gRPC Canceled while the request context is alive", err: status.Error(codes.Canceled, "client connection is closing"), want: "stream/err"},
		{name: "success although the context ended", ctxEnds: true, want: "stream/ok"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			if tt.ctxExpire {
				cancel()
				ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
			}
			if tt.deadline > 0 {
				cancel()
				ctx, cancel = context.WithTimeout(context.Background(), tt.deadline)
			}
			defer cancel()
			_, m, reg := newQueryLatencyMeasureService()
			_, err := newQueryLatencyInterceptor(m)(ctx, nil, &grpc.UnaryServerInfo{FullMethod: streamv1.StreamService_Query_FullMethodName},
				func(context.Context, any) (any, error) {
					if tt.ctxEnds {
						cancel()
					}
					return nil, tt.err
				})
			require.Equal(t, tt.err, err)
			require.Equal(t, map[string]uint64{tt.want: 1}, queryLatencyCounts(t, reg))
		})
	}
}

// TestQueryLatencyCancelBeforeToleranceReturnedInside checks that the status follows the moment the request context
// ended: a cancellation more than the tolerance before the deadline stays canceled although the handler returns
// within the tolerance of the deadline.
func TestQueryLatencyCancelBeforeToleranceReturnedInside(t *testing.T) {
	_, m, reg := newQueryLatencyMeasureService()
	deadline := time.Now().Add(time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	_, err := newQueryLatencyInterceptor(m)(ctx, nil, &grpc.UnaryServerInfo{FullMethod: streamv1.StreamService_Query_FullMethodName},
		func(context.Context, any) (any, error) {
			cancel()
			time.Sleep(time.Until(deadline.Add(-queryDeadlineTolerance / 2)))
			return nil, errors.New("handler returned after cleanup")
		})
	require.Error(t, err)
	require.Equal(t, map[string]uint64{"stream/canceled": 1}, queryLatencyCounts(t, reg))
}

// TestQueryLatencyCallbackScheduledAfterHandlerReturned checks that a context callback that runs late does not move
// the end of the request later. With one P, the handler cancels the context and then queues busy goroutines ahead of
// the callback, so the callback runs after the handler returned. The request was canceled more than the tolerance
// before its deadline and stays canceled.
func TestQueryLatencyCallbackScheduledAfterHandlerReturned(t *testing.T) {
	prev := runtime.GOMAXPROCS(1)
	t.Cleanup(func() { runtime.GOMAXPROCS(prev) })
	const rounds = 5
	got := make(map[string]int)
	valid := 0
	for i := 0; i < rounds; i++ {
		_, m, reg := newQueryLatencyMeasureService()
		deadline := time.Now().Add(400 * time.Millisecond)
		ctx, cancel := context.WithDeadline(context.Background(), deadline)
		var stop atomic.Bool
		var wg sync.WaitGroup
		var left time.Duration
		_, _ = newQueryLatencyInterceptor(m)(ctx, nil, &grpc.UnaryServerInfo{FullMethod: streamv1.StreamService_Query_FullMethodName},
			func(context.Context, any) (any, error) {
				time.Sleep(time.Until(deadline.Add(-queryDeadlineTolerance - 20*time.Millisecond)))
				cancel()
				for j := 0; j < 4; j++ {
					wg.Add(1)
					//panicdiag:allow-rawgo test helper goroutine
					go func() {
						defer wg.Done()
						// revive:disable-next-line:empty-block
						for !stop.Load() {
						}
					}()
				}
				left = time.Until(deadline)
				return nil, errors.New("canceled by the client")
			})
		stop.Store(true)
		wg.Wait()
		cancel()
		counts := queryLatencyCounts(t, reg)
		// A round counts only when the handler returned more than the tolerance before the deadline.
		if left <= queryDeadlineTolerance {
			continue
		}
		valid++
		for k, v := range counts {
			got[k] += int(v)
		}
	}
	if valid == 0 {
		t.Skip("the scheduler delayed every handler past the tolerance")
	}
	assert.Equal(t, map[string]int{"stream/canceled": valid}, got)
}

// TestDeadlineReached checks the boundary of the tolerance with fixed times: only a context that ended within
// queryDeadlineTolerance of its deadline, or after it, has reached the deadline.
func TestDeadlineReached(t *testing.T) {
	base := time.Now()
	tests := []struct {
		deadline time.Time
		endedAt  time.Time
		name     string
		want     bool
	}{
		{name: "no deadline", endedAt: base},
		{name: "deadline far away", deadline: base.Add(time.Hour), endedAt: base},
		{name: "ended before the tolerance", deadline: base.Add(queryDeadlineTolerance + time.Millisecond), endedAt: base},
		{name: "ended exactly at the tolerance", deadline: base.Add(queryDeadlineTolerance), endedAt: base, want: true},
		{name: "ended within the tolerance", deadline: base.Add(50 * time.Millisecond), endedAt: base, want: true},
		{name: "ended at the deadline", deadline: base, endedAt: base, want: true},
		{name: "ended after the deadline", deadline: base.Add(-time.Second), endedAt: base, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			if !tt.deadline.IsZero() {
				var cancel context.CancelFunc
				ctx, cancel = context.WithDeadline(ctx, tt.deadline)
				defer cancel()
			}
			require.Equal(t, tt.want, deadlineReached(ctx, tt.endedAt))
		})
	}
}

// blockingStreamServer blocks every stream query until the request context ends, then fails it with that context's
// error, the way a handler waiting on a data node does.
type blockingStreamServer struct {
	streamv1.UnimplementedStreamServiceServer
	started chan struct{}
}

func (b *blockingStreamServer) Query(ctx context.Context, _ *streamv1.QueryRequest) (*streamv1.QueryResponse, error) {
	close(b.started)
	<-ctx.Done()
	return nil, ctx.Err()
}

// TestQueryLatencyStatusOverGRPCTransport sends the request through a real gRPC server: the transport derives the
// request context from the grpc-timeout header and also runs its own deadline timer that cancels the stream, so the
// handler may observe context.Canceled although the client's deadline is what ended the request.
func TestQueryLatencyStatusOverGRPCTransport(t *testing.T) {
	tests := []struct {
		call func(client streamv1.StreamServiceClient, started <-chan struct{}) error
		name string
		want string
	}{
		{
			name: "client deadline expires", want: "stream/deadline_exceeded",
			call: func(client streamv1.StreamServiceClient, _ <-chan struct{}) error {
				ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
				defer cancel()
				_, err := client.Query(ctx, &streamv1.QueryRequest{})
				return err
			},
		},
		{
			name: "client cancels", want: "stream/canceled",
			call: func(client streamv1.StreamServiceClient, started <-chan struct{}) error {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				//panicdiag:allow-rawgo test helper goroutine
				go func() {
					<-started
					cancel()
				}()
				_, err := client.Query(ctx, &streamv1.QueryRequest{})
				return err
			},
		},
		{
			name: "client cancels with a deadline far away", want: "stream/canceled",
			call: func(client streamv1.StreamServiceClient, started <-chan struct{}) error {
				ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
				defer cancel()
				//panicdiag:allow-rawgo test helper goroutine
				go func() {
					<-started
					cancel()
				}()
				_, err := client.Query(ctx, &streamv1.QueryRequest{})
				return err
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, m, reg := newQueryLatencyMeasureService()
			lis := bufconn.Listen(1 << 20)
			server := grpc.NewServer(grpc.ChainUnaryInterceptor(newQueryLatencyInterceptor(m)))
			handler := &blockingStreamServer{started: make(chan struct{})}
			streamv1.RegisterStreamServiceServer(server, handler)
			//panicdiag:allow-rawgo test server
			go func() { _ = server.Serve(lis) }()
			defer server.Stop()
			conn, err := grpc.NewClient("passthrough:///bufnet",
				grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
				grpc.WithTransportCredentials(insecure.NewCredentials()))
			require.NoError(t, err)
			defer conn.Close()
			// Connect before the call, so that the call's deadline does not include the connection setup.
			conn.Connect()
			connectCtx, connectCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer connectCancel()
			for state := conn.GetState(); state != connectivity.Ready; state = conn.GetState() {
				require.True(t, conn.WaitForStateChange(connectCtx, state), "the connection must become ready")
			}

			require.Error(t, tt.call(streamv1.NewStreamServiceClient(conn), handler.started))
			require.Eventually(t, func() bool {
				return len(queryLatencyCounts(t, reg)) > 0
			}, 5*time.Second, 10*time.Millisecond, "the interceptor must observe the finished request")
			require.Equal(t, map[string]uint64{tt.want: 1}, queryLatencyCounts(t, reg))
		})
	}
}
