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

package tracestreaming

import (
	"context"
	"errors"
	"fmt"
	"io"
	"runtime"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
)

// QueryRunner executes trace queries with various configurations.
type QueryRunner struct {
	client  tracev1.TraceServiceClient
	conn    *grpc.ClientConn
	metrics *PerformanceMetrics
}

// NewQueryRunner creates a new query runner.
func NewQueryRunner(addr string) (*QueryRunner, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", addr, err)
	}

	return &QueryRunner{
		client:  tracev1.NewTraceServiceClient(conn),
		conn:    conn,
		metrics: NewPerformanceMetrics(),
	}, nil
}

// Close closes the gRPC connection.
func (r *QueryRunner) Close() error {
	if r.conn != nil {
		return r.conn.Close()
	}
	return nil
}

// QueryConfig defines parameters for a query.
type QueryConfig struct {
	StartTime     time.Time
	EndTime       time.Time
	IndexRuleName string
	MaxTraceSize  uint32
	OrderByAsc    bool
}

// ExecuteQuery executes a trace query with the given configuration.
func (r *QueryRunner) ExecuteQuery(ctx context.Context, config QueryConfig) (int, error) {
	start := time.Now()

	// Truncate to millisecond precision as required by trace query API
	startTime := config.StartTime.Truncate(time.Millisecond)
	endTime := config.EndTime.Truncate(time.Millisecond)

	// Default to time_based_index if no index rule is specified
	indexRuleName := config.IndexRuleName
	if indexRuleName == "" {
		indexRuleName = "time_based_index"
	}

	req := &tracev1.QueryRequest{
		Groups: []string{"trace_streaming_test"},
		Name:   "segment_trace",
		TimeRange: &modelv1.TimeRange{
			Begin: timestamppb.New(startTime),
			End:   timestamppb.New(endTime),
		},
		Limit: config.MaxTraceSize,
		OrderBy: &modelv1.QueryOrder{
			IndexRuleName: indexRuleName,
			Sort:          modelv1.Sort_SORT_DESC, // Default to descending (newest first)
		},
	}

	// Override sort order if specified
	if config.OrderByAsc {
		req.OrderBy.Sort = modelv1.Sort_SORT_ASC
	}

	resp, err := r.client.Query(ctx, req)
	if err != nil {
		r.metrics.RecordError()
		return 0, fmt.Errorf("failed to query: %w", err)
	}

	traceCount := len(resp.Traces)

	// Track trace sizes
	totalSpans := 0
	for _, trace := range resp.Traces {
		spanCount := len(trace.Spans)
		totalSpans += spanCount
		r.metrics.RecordTraceSize(spanCount)
	}

	duration := time.Since(start)
	r.metrics.RecordQuery(duration, int64(traceCount))

	return traceCount, nil
}

// ExecuteQueryWithMemoryTracking executes a query and returns memory metrics.
func (r *QueryRunner) ExecuteQueryWithMemoryTracking(ctx context.Context, config QueryConfig) (traceCount int, heapBefore, heapPeak, heapAfter uint64, err error) {
	// Measure before
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)
	heapBefore = memBefore.HeapAlloc

	// Execute query
	traceCount, err = r.ExecuteQuery(ctx, config)

	// Measure peak (approximate - right after query)
	var memPeak runtime.MemStats
	runtime.ReadMemStats(&memPeak)
	heapPeak = memPeak.HeapAlloc

	// Force GC and measure after
	runtime.GC()
	time.Sleep(50 * time.Millisecond)
	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)
	heapAfter = memAfter.HeapAlloc

	return traceCount, heapBefore, heapPeak, heapAfter, err
}

// RunContinuousQueries executes queries continuously for a specified duration.
func (r *QueryRunner) RunContinuousQueries(ctx context.Context, config QueryConfig, interval time.Duration) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			_, err := r.ExecuteQuery(ctx, config)
			if err != nil {
				return err
			}
		}
	}
}

// GetMetrics returns the performance metrics.
func (r *QueryRunner) GetMetrics() *PerformanceMetrics {
	return r.metrics
}

// TraceWriteClient handles writing trace data.
type TraceWriteClient struct {
	client tracev1.TraceServiceClient
	conn   *grpc.ClientConn
}

// NewTraceWriteClient creates a new trace write client.
func NewTraceWriteClient(addr string) (*TraceWriteClient, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", addr, err)
	}

	return &TraceWriteClient{
		client: tracev1.NewTraceServiceClient(conn),
		conn:   conn,
	}, nil
}

// Close closes the gRPC connection.
func (c *TraceWriteClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// WriteTraces writes multiple traces.
func (c *TraceWriteClient) WriteTraces(ctx context.Context, traces []*TraceData) error {
	stream, err := c.client.Write(ctx)
	if err != nil {
		return fmt.Errorf("failed to create write stream: %w", err)
	}

	version := uint64(1)
	for _, trace := range traces {
		for _, span := range trace.Spans {
			req := span.ToTraceWriteRequest()
			req.Version = version
			version++

			if err := stream.Send(req); err != nil {
				return fmt.Errorf("failed to send write request: %w", err)
			}
		}
	}

	// Close send and wait for final response
	if err := stream.CloseSend(); err != nil {
		return fmt.Errorf("failed to close send: %w", err)
	}

	// Drain responses
	for {
		_, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to receive write response: %w", err)
		}
	}

	return nil
}

// WriteBatch writes a batch of spans.
func (c *TraceWriteClient) WriteBatch(ctx context.Context, spans []*SpanData) error {
	stream, err := c.client.Write(ctx)
	if err != nil {
		return fmt.Errorf("failed to create write stream: %w", err)
	}

	for i, span := range spans {
		req := span.ToTraceWriteRequest()
		req.Version = uint64(i + 1)

		if err := stream.Send(req); err != nil {
			return fmt.Errorf("failed to send write request: %w", err)
		}
	}

	// Close send and wait for final response
	if err := stream.CloseSend(); err != nil {
		return fmt.Errorf("failed to close send: %w", err)
	}

	// Drain responses
	for {
		_, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to receive write response: %w", err)
		}
	}

	return nil
}
