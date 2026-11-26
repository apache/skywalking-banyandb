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
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/observability/services"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/meter/prom"
)

type topNBroadcaster struct {
	queue.Client
	err  error
	resp bus.Message
}

func (b topNBroadcaster) Publish(context.Context, bus.Topic, ...bus.Message) (bus.Future, error) {
	if b.err != nil {
		return nil, b.err
	}
	return topNFuture{msg: b.resp}, nil
}

type topNFuture struct {
	msg bus.Message
}

func (f topNFuture) Get() (bus.Message, error)      { return f.msg, nil }
func (f topNFuture) GetAll() ([]bus.Message, error) { return []bus.Message{f.msg}, nil }

// TestTopNRecordsQueryMetrics checks that a TopN RPC is counted under service="topn", once per group, and that a
// failed one also bumps total_err.
func TestTopNRecordsQueryMetrics(t *testing.T) {
	now := timestamppb.New(time.Now().Truncate(time.Millisecond))
	validRange := &modelv1.TimeRange{Begin: now, End: now}
	tests := []struct {
		req         *measurev1.TopNRequest
		name        string
		broadcaster topNBroadcaster
		wantErr     float64
	}{
		{
			name:        "success",
			broadcaster: topNBroadcaster{resp: bus.NewMessage(1, &measurev1.TopNResponse{})},
			req:         &measurev1.TopNRequest{Groups: []string{"g1", "g2"}, TimeRange: validRange},
		},
		{
			name:        "data node error",
			broadcaster: topNBroadcaster{err: status.Error(codes.Unavailable, "no node")},
			req:         &measurev1.TopNRequest{Groups: []string{"g1", "g2"}, TimeRange: validRange},
			wantErr:     2,
		},
		{
			name:    "invalid time range",
			req:     &measurev1.TopNRequest{Groups: []string{"g1", "g2"}},
			wantErr: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			svc := &measureService{
				discoveryService: &discoveryService{groupRepo: &groupRepo{inflight: make(map[string]*groupInflight)}},
				broadcaster:      tt.broadcaster,
				l:                logger.GetLogger("test"),
				metrics:          newMetrics(services.NewFactory(prom.NewProvider(liaisonGrpcScope, reg), nil, nil)),
			}
			_, err := svc.TopN(context.Background(), tt.req)
			require.Equal(t, tt.wantErr > 0, err != nil)
			require.Equal(t, float64(2), counterSeries(t, reg, "total_started")["topn/query"])
			require.Equal(t, float64(2), counterSeries(t, reg, "total_finished")["topn/query"])
			require.Equal(t, tt.wantErr, counterSeries(t, reg, "total_err")["topn/query"])
			require.Contains(t, counterSeries(t, reg, "total_latency"), "topn/query")
		})
	}
}
