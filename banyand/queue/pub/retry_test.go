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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	clusterv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/cluster/v1"
)

// MockSendClient implements the clusterv1.Service_SendClient interface for testing.
type MockSendClient struct {
	ctx           context.Context
	sendFunc      func(*clusterv1.SendRequest) error
	recvFunc      func() (*clusterv1.SendResponse, error)
	closeSendFunc func() error
}

func NewMockSendClient(ctx context.Context) *MockSendClient {
	return &MockSendClient{ctx: ctx}
}

func (m *MockSendClient) Send(req *clusterv1.SendRequest) error {
	if m.sendFunc != nil {
		return m.sendFunc(req)
	}
	return nil
}

func (m *MockSendClient) Recv() (*clusterv1.SendResponse, error) {
	if m.recvFunc != nil {
		return m.recvFunc()
	}
	return nil, nil
}

func (m *MockSendClient) CloseSend() error {
	if m.closeSendFunc != nil {
		return m.closeSendFunc()
	}
	return nil
}

func (m *MockSendClient) Context() context.Context {
	return m.ctx
}

func (m *MockSendClient) Header() (metadata.MD, error) {
	return metadata.MD{}, nil
}

func (m *MockSendClient) Trailer() metadata.MD {
	return metadata.MD{}
}

func (m *MockSendClient) SendMsg(_ interface{}) error {
	return nil
}

func (m *MockSendClient) RecvMsg(_ interface{}) error {
	return nil
}

func (m *MockSendClient) SetSendFunc(f func(*clusterv1.SendRequest) error) {
	m.sendFunc = f
}

func (m *MockSendClient) SetRecvFunc(f func() (*clusterv1.SendResponse, error)) {
	m.recvFunc = f
}

func (m *MockSendClient) SetCloseSendFunc(f func() error) {
	m.closeSendFunc = f
}

func TestRetrySendSuccess(t *testing.T) {
	ctx := context.Background()
	mockStream := NewMockSendClient(ctx)

	// Mock successful send on first attempt
	mockStream.SetSendFunc(func(*clusterv1.SendRequest) error {
		return nil
	})

	bp := &batchPublisher{}
	req := &clusterv1.SendRequest{}

	err := bp.retrySend(ctx, mockStream, req, "test-node")

	assert.NoError(t, err, "successful send should not return error")
}

func TestRetrySendTransientErrorWithRecovery(t *testing.T) {
	ctx := context.Background()
	mockStream := NewMockSendClient(ctx)

	// Mock transient error on first attempt, success on second
	callCount := 0
	mockStream.SetSendFunc(func(*clusterv1.SendRequest) error {
		callCount++
		if callCount == 1 {
			return status.Error(codes.Unavailable, "unavailable")
		}
		return nil
	})

	bp := &batchPublisher{}
	req := &clusterv1.SendRequest{}

	err := bp.retrySend(ctx, mockStream, req, "test-node")

	assert.NoError(t, err, "should succeed after retry")
}

func TestRetrySendNonTransientError(t *testing.T) {
	ctx := context.Background()
	mockStream := NewMockSendClient(ctx)

	// Mock non-transient error
	nonTransientErr := status.Error(codes.InvalidArgument, "invalid argument")
	mockStream.SetSendFunc(func(*clusterv1.SendRequest) error {
		return nonTransientErr
	})

	bp := &batchPublisher{}
	req := &clusterv1.SendRequest{}

	err := bp.retrySend(ctx, mockStream, req, "test-node")

	assert.Error(t, err, "non-transient error should be returned immediately")
	assert.Equal(t, nonTransientErr, err, "should return the original error")
}

func TestRetrySendExhaustedRetries(t *testing.T) {
	ctx := context.Background()
	mockStream := NewMockSendClient(ctx)

	// Mock transient error on all attempts
	transientErr := status.Error(codes.Unavailable, "unavailable")
	mockStream.SetSendFunc(func(*clusterv1.SendRequest) error {
		return transientErr
	})

	bp := &batchPublisher{}
	req := &clusterv1.SendRequest{}

	err := bp.retrySend(ctx, mockStream, req, "test-node")

	assert.Error(t, err, "should return error after exhausting retries")
	assert.Contains(t, err.Error(), "retry exhausted", "error should indicate retry exhaustion")
	assert.Contains(t, err.Error(), "test-node", "error should contain node name")
}

func TestRetrySendContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	mockStream := NewMockSendClient(context.Background())

	// Cancel context before retry
	cancel()

	// Mock transient error (may or may not be called)
	mockStream.SetSendFunc(func(*clusterv1.SendRequest) error {
		return status.Error(codes.Unavailable, "unavailable")
	})

	bp := &batchPublisher{}
	req := &clusterv1.SendRequest{}

	err := bp.retrySend(ctx, mockStream, req, "test-node")

	assert.Error(t, err, "should return error when context is canceled")
	assert.Equal(t, context.Canceled, err, "should return context cancellation error")
}

func TestRetrySendStreamContextDone(t *testing.T) {
	ctx := context.Background()
	streamCtx, cancel := context.WithCancel(context.Background())

	// Cancel stream context
	cancel()

	mockStream := NewMockSendClient(streamCtx)

	bp := &batchPublisher{}
	req := &clusterv1.SendRequest{}

	err := bp.retrySend(ctx, mockStream, req, "test-node")

	assert.Error(t, err, "should return error when stream context is done")
	assert.Equal(t, context.Canceled, err, "should return stream context cancellation error")
}

func TestRetrySendPerAttemptTimeout(t *testing.T) {
	ctx := context.Background()
	mockStream := NewMockSendClient(ctx)

	// Mock slow send that would trigger per-attempt timeout
	mockStream.SetSendFunc(func(*clusterv1.SendRequest) error {
		time.Sleep(defaultPerRequestTimeout + 100*time.Millisecond)
		return nil
	})

	bp := &batchPublisher{}
	req := &clusterv1.SendRequest{}

	start := time.Now()
	_ = bp.retrySend(ctx, mockStream, req, "test-node")
	duration := time.Since(start)

	// Should timeout quickly due to per-attempt timeout, not wait for the full operation
	assert.Less(t, duration, defaultPerRequestTimeout*time.Duration(defaultMaxRetries+1)*2,
		"should timeout relatively quickly due to per-attempt timeouts")
}

func TestRetrySendBackoffTiming(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping timing test in short mode")
	}

	ctx := context.Background()
	mockStream := NewMockSendClient(ctx)

	// Mock transient errors for first few attempts, then success
	transientErr := status.Error(codes.Unavailable, "unavailable")
	callCount := 0
	mockStream.SetSendFunc(func(*clusterv1.SendRequest) error {
		callCount++
		if callCount <= 2 {
			return transientErr
		}
		return nil
	})

	bp := &batchPublisher{}
	req := &clusterv1.SendRequest{}

	start := time.Now()
	err := bp.retrySend(ctx, mockStream, req, "test-node")
	duration := time.Since(start)

	assert.NoError(t, err, "should eventually succeed")

	// Should have some backoff delay but not too much (considering jitter)
	expectedMinDelay := defaultBackoffBase                              // First backoff
	expectedMaxDelay := (defaultBackoffBase + defaultBackoffBase*2) * 2 // With max jitter

	assert.GreaterOrEqual(t, duration, expectedMinDelay/2,
		"should have some backoff delay")
	assert.LessOrEqual(t, duration, expectedMaxDelay,
		"should not take too long with reasonable backoff")
}

func TestRetrySendConcurrency(t *testing.T) {
	const numGoroutines = 50

	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			ctx := context.Background()
			mockStream := NewMockSendClient(ctx)

			// Mock some transient errors followed by success
			transientErr := status.Error(codes.Unavailable, "unavailable")
			callCount := 0 // Each goroutine has its own callCount
			mockStream.SetSendFunc(func(*clusterv1.SendRequest) error {
				callCount++
				if callCount <= 2 { // Fail for first 2 attempts, succeed on 3rd (within defaultMaxRetries=3)
					return transientErr
				}
				return nil // Succeed on final attempt
			})

			bp := &batchPublisher{}
			req := &clusterv1.SendRequest{}

			err := bp.retrySend(ctx, mockStream, req, fmt.Sprintf("test-node-%d", id))
			errors <- err
		}(i)
	}

	wg.Wait()
	close(errors)

	successCount := 0
	failureCount := 0
	for err := range errors {
		if err == nil {
			successCount++
		} else {
			failureCount++
		}
	}

	assert.Equal(t, numGoroutines, successCount, "all goroutines should succeed eventually (got %d successes, %d failures)", successCount, failureCount)
}
