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
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
)

func TestBatchQueryModularPipeline(t *testing.T) {
	tests := []struct {
		name             string
		queryCount       int
		concurrencyLimit int
		maxAllowed       int
		injectErrorAt    int
		expectedErrCode  codes.Code
	}{
		{
			name:             "normal_concurrency_and_ordering",
			queryCount:       6,
			concurrencyLimit: 2,
			maxAllowed:       10,
			injectErrorAt:    -1,
			expectedErrCode:  codes.OK,
		},
		{
			name:             "partial_failure_tolerance",
			queryCount:       4,
			concurrencyLimit: 2,
			maxAllowed:       10,
			injectErrorAt:    2,
			expectedErrCode:  codes.OK,
		},
		{
			name:             "admission_limit_exceeded",
			queryCount:       5,
			concurrencyLimit: 2,
			maxAllowed:       3,
			injectErrorAt:    -1,
			expectedErrCode:  codes.InvalidArgument,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := &measureService{
				maxBatchQueries:       tt.maxAllowed,
				batchQueryConcurrency: tt.concurrencyLimit,
			}

			queries := make([]*measurev1.QueryRequest, tt.queryCount)
			for i := 0; i < tt.queryCount; i++ {
				queries[i] = &measurev1.QueryRequest{Name: fmt.Sprintf("m-%d", i)}
			}

			req := &measurev1.BatchQueryRequest{Queries: queries}

			if tt.expectedErrCode != codes.OK {
				_, err := svc.BatchQuery(context.Background(), req)
				require.Error(t, err)
				assert.Equal(t, tt.expectedErrCode, status.Code(err))
				return
			}

			mockExecutor := func(_ context.Context, q *measurev1.QueryRequest) (*measurev1.QueryResponse, error) {
				var idx int
				_, _ = fmt.Sscanf(q.GetName(), "m-%d", &idx)

				// Invert sleep duration so later queries finish faster
				time.Sleep(time.Duration((tt.queryCount-idx)*2) * time.Millisecond)

				if idx == tt.injectErrorAt {
					return nil, errors.New("simulated shard read failure")
				}

				return &measurev1.QueryResponse{
					DataPoints: []*measurev1.DataPoint{{Version: int64(idx)}},
				}, nil
			}

			batchResultsCh := make(chan indexedMeasureResponse, tt.queryCount)
			chunkSize := svc.batchQueryConcurrency
			for start := 0; start < tt.queryCount; start += chunkSize {
				end := min(start+chunkSize, tt.queryCount)
				subBatch := queries[start:end]
				svc.dispatchMeasureSubBatch(context.Background(), subBatch, start, batchResultsCh, mockExecutor)
			}

			responses := svc.collectBatchResponses(batchResultsCh, tt.queryCount)

			// Assert deterministic 1-to-1 index matching
			for i := 0; i < tt.queryCount; i++ {
				require.NotNil(t, responses[i])
				if i == tt.injectErrorAt {
					assert.Empty(t, responses[i].GetDataPoints())
				} else {
					require.NotEmpty(t, responses[i].GetDataPoints())
					assert.Equal(t, int64(i), responses[i].GetDataPoints()[0].Version)
				}
			}
		})
	}
}

func TestDispatchMeasureSubBatchIsolated(t *testing.T) {
	svc := &measureService{}
	subBatch := []*measurev1.QueryRequest{
		{Name: "c0"},
		{Name: "c1"},
		{Name: "c2"},
	}

	batchResultsCh := make(chan indexedMeasureResponse, len(subBatch))
	mockExecutor := func(_ context.Context, q *measurev1.QueryRequest) (*measurev1.QueryResponse, error) {
		return &measurev1.QueryResponse{
			DataPoints: []*measurev1.DataPoint{{Version: 100}},
		}, nil
	}

	// Dispatch sub-batch starting at global offset 10
	offset := 10
	svc.dispatchMeasureSubBatch(context.Background(), subBatch, offset, batchResultsCh, mockExecutor)

	// Collect indexed results
	seenIndices := make(map[int]bool)
	for i := 0; i < len(subBatch); i++ {
		res := <-batchResultsCh
		require.NoError(t, res.err)
		seenIndices[res.index] = true
	}

	// Verify all items are properly tagged with global offset
	assert.True(t, seenIndices[10])
	assert.True(t, seenIndices[11])
	assert.True(t, seenIndices[12])
}

// mockWireMeasureServer implements MeasureServiceServer for the wire round-trip test.
type mockWireMeasureServer struct {
	measurev1.UnimplementedMeasureServiceServer
	svc *measureService
}

func (m *mockWireMeasureServer) BatchQuery(ctx context.Context, req *measurev1.BatchQueryRequest) (*measurev1.BatchQueryResponse, error) {
	queries := req.GetQueries()
	queryAmount := len(queries)
	batchResultsCh := make(chan indexedMeasureResponse, queryAmount)

	mockExecutor := func(_ context.Context, q *measurev1.QueryRequest) (*measurev1.QueryResponse, error) {
		return &measurev1.QueryResponse{
			DataPoints: []*measurev1.DataPoint{{Sid: 123}},
		}, nil
	}

	m.svc.dispatchMeasureSubBatch(ctx, queries, 0, batchResultsCh, mockExecutor)
	responses := m.svc.collectBatchResponses(batchResultsCh, queryAmount)
	return &measurev1.BatchQueryResponse{Responses: responses}, nil
}

func TestBatchQueryWireRoundTrip(t *testing.T) {
	lis := bufconn.Listen(1024 * 1024)
	t.Cleanup(func() { _ = lis.Close() })

	s := grpclib.NewServer()
	mockServer := &mockWireMeasureServer{svc: &measureService{}}
	measurev1.RegisterMeasureServiceServer(s, mockServer)

	go func() { _ = s.Serve(lis) }()
	t.Cleanup(s.Stop)

	conn, err := grpclib.NewClient("passthrough://bufnet",
		grpclib.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpclib.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	client := measurev1.NewMeasureServiceClient(conn)

	req := &measurev1.BatchQueryRequest{
		Queries: []*measurev1.QueryRequest{
			{Name: "service_relation_client_side"},
			{Name: "service_relation_server_side"},
		},
	}

	resp, err := client.BatchQuery(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, 2, len(resp.GetResponses()))
	assert.Equal(t, uint64(123), resp.GetResponses()[0].GetDataPoints()[0].Sid)
	assert.Equal(t, uint64(123), resp.GetResponses()[1].GetDataPoints()[0].Sid)
}
