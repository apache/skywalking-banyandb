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
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
		ctxEnds   bool
		ctxExpire bool
	}{
		{name: "success", want: "stream/ok"},
		{name: "plain error", err: wrapped, want: "stream/err"},
		{name: "wrapped plain error after the client canceled", err: wrapped, ctxEnds: true, want: "stream/canceled"},
		{name: "wrapped plain error after the client deadline", err: wrapped, ctxEnds: true, ctxExpire: true, want: "stream/deadline_exceeded"},
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
