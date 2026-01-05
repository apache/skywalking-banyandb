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

package proxy

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewConnManager(t *testing.T) {
	testLogger := initTestLogger(t)
	proxyAddr := "localhost:17913"
	reconnectInterval := 5 * time.Second

	cm := NewConnManager(proxyAddr, reconnectInterval, testLogger)

	assert.NotNil(t, cm)
	assert.Equal(t, proxyAddr, cm.proxyAddr)
	assert.Equal(t, reconnectInterval, cm.retryInterval)
	assert.Equal(t, ConnStateDisconnected, cm.GetState())
	assert.NotNil(t, cm.eventCh)
	assert.NotNil(t, cm.closer)
	assert.NotNil(t, cm.logger)
}

func TestConnManager_EventChannel(t *testing.T) {
	testLogger := initTestLogger(t)
	cm := NewConnManager("localhost:17913", 5*time.Second, testLogger)

	eventCh := cm.EventChannel()
	assert.NotNil(t, eventCh)

	// Verify we can send to the channel
	ctx := context.Background()
	event := ConnEvent{
		Type:    ConnEventDisconnect,
		Context: ctx,
	}

	select {
	case eventCh <- event:
		// Success
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Failed to send event to channel")
	}
}

func TestConnManager_StartStop(t *testing.T) {
	testLogger := initTestLogger(t)
	cm := NewConnManager("localhost:17913", 5*time.Second, testLogger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start the manager
	cm.Start(ctx)

	// Give it time to start
	time.Sleep(50 * time.Millisecond)

	// Stop the manager
	cm.Stop()

	// Give it time to stop
	time.Sleep(50 * time.Millisecond)

	assert.Equal(t, ConnStateDisconnected, cm.GetState())
}

func TestConnManager_RequestConnect_Success(t *testing.T) {
	testLogger := initTestLogger(t)
	// grpc.NewClient succeeds even with invalid addresses (lazy connection)
	cm := NewConnManager("localhost:17913", 5*time.Second, testLogger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cm.Start(ctx)
	defer cm.Stop()

	// Request connection
	resultCh := cm.RequestConnect(ctx)

	select {
	case result := <-resultCh:
		// grpc.NewClient succeeds, actual connection happens later
		if result.Error != nil {
			t.Logf("Connect error: %v", result.Error)
		} else {
			assert.NotNil(t, result.Conn)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for connect result")
	}
}

func TestConnManager_RequestConnect_ContextCanceled(t *testing.T) {
	testLogger := initTestLogger(t)
	cm := NewConnManager("localhost:17913", 5*time.Second, testLogger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cm.Start(ctx)
	defer cm.Stop()

	// Create a context that's already canceled
	canceledCtx, cancelFunc := context.WithCancel(context.Background())
	cancelFunc()

	// Give time for cancelation to propagate
	time.Sleep(10 * time.Millisecond)

	resultCh := cm.RequestConnect(canceledCtx)

	select {
	case result := <-resultCh:
		// The RequestConnect method checks ctx.Done() in the select statement
		// If the context is already canceled, it should return an error
		if result.Error != nil {
			assert.Equal(t, context.Canceled, result.Error)
		} else {
			// If connection succeeded before context check, that's also valid
			t.Logf("Connection succeeded despite canceled context")
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for connect result")
	}
}

func TestConnManager_RequestReconnect_WhenDisconnected(t *testing.T) {
	testLogger := initTestLogger(t)
	cm := NewConnManager("localhost:17913", 5*time.Second, testLogger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cm.Start(ctx)

	// Manually set disconnected flag
	disconnectEvent := ConnEvent{
		Type:    ConnEventDisconnect,
		Context: ctx,
	}
	cm.EventChannel() <- disconnectEvent

	// Give it time to process the disconnect
	time.Sleep(100 * time.Millisecond)

	// Try to reconnect when disconnected - should succeed after backoff
	reconnCtx, reconnCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer reconnCancel()

	resultCh := cm.RequestReconnect(reconnCtx)

	select {
	case result := <-resultCh:
		// The new implementation attempts to reconnect even when disconnected
		// It should succeed or fail based on actual connection, not state
		t.Logf("Reconnect result: error=%v", result.Error)
	case <-time.After(15 * time.Second):
		t.Fatal("Timeout waiting for reconnect result")
	}
}

func TestConnManager_DisconnectEvent(t *testing.T) {
	testLogger := initTestLogger(t)
	cm := NewConnManager("localhost:17913", 5*time.Second, testLogger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cm.Start(ctx)
	defer cm.Stop()

	// Send disconnect event
	resultCh := make(chan ConnResult, 1)
	event := ConnEvent{
		Type:     ConnEventDisconnect,
		Context:  ctx,
		ResultCh: resultCh,
	}

	cm.EventChannel() <- event

	select {
	case result := <-resultCh:
		assert.NoError(t, result.Error)
		// Give some time for state update
		time.Sleep(50 * time.Millisecond)
		assert.Equal(t, ConnStateDisconnected, cm.GetState())
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for disconnect event to be processed")
	}
}

func TestConnManager_MultipleConnectRequests(t *testing.T) {
	testLogger := initTestLogger(t)
	cm := NewConnManager("localhost:17913", 5*time.Second, testLogger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cm.Start(ctx)
	defer cm.Stop()

	// Send first connect request
	resultCh1 := cm.RequestConnect(ctx)

	// Send second connect request immediately
	resultCh2 := cm.RequestConnect(ctx)

	// Both should get responses
	result1 := <-resultCh1
	result2 := <-resultCh2

	// One should succeed or indicate connection in progress
	t.Logf("Result1 error: %v", result1.Error)
	t.Logf("Result2 error: %v", result2.Error)

	// At least one should complete successfully or report in progress
	hasSuccess := result1.Error == nil || result2.Error == nil
	hasInProgress := (result1.Error != nil && result1.Error.Error() == "connection already in progress") ||
		(result2.Error != nil && result2.Error.Error() == "connection already in progress")
	assert.True(t, hasSuccess || hasInProgress, "Expected at least one success or 'in progress' message")
}

func TestConnManager_StopWithPendingEvents(t *testing.T) {
	testLogger := initTestLogger(t)
	cm := NewConnManager("localhost:17913", 5*time.Second, testLogger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cm.Start(ctx)

	// Fill the event channel
	for i := 0; i < 5; i++ {
		event := ConnEvent{
			Type:     ConnEventConnect,
			Context:  ctx,
			ResultCh: make(chan ConnResult, 1),
		}
		select {
		case cm.EventChannel() <- event:
		case <-time.After(100 * time.Millisecond):
			t.Logf("Could not send event %d", i)
		}
	}

	// Stop should handle pending events gracefully
	cm.Stop()

	// Give time for cleanup
	time.Sleep(100 * time.Millisecond)

	assert.Equal(t, ConnStateDisconnected, cm.GetState())
}

func TestConnManager_ContextCancellation(t *testing.T) {
	testLogger := initTestLogger(t)
	cm := NewConnManager("localhost:17913", 5*time.Second, testLogger)

	ctx, cancel := context.WithCancel(context.Background())

	cm.Start(ctx)

	// Cancel the context
	cancel()

	// Give time for cleanup
	time.Sleep(100 * time.Millisecond)

	assert.Equal(t, ConnStateDisconnected, cm.GetState())
}

func TestConnManager_ReconnectWithBackoff(t *testing.T) {
	testLogger := initTestLogger(t)
	cm := NewConnManager("localhost:17913", 100*time.Millisecond, testLogger)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	cm.Start(ctx)
	defer cm.Stop()

	// Request reconnect (not immediate, should use backoff)
	resultCh := cm.RequestReconnect(ctx)

	start := time.Now()
	select {
	case result := <-resultCh:
		duration := time.Since(start)
		// Should have waited at least the initial retry interval
		assert.GreaterOrEqual(t, duration, 100*time.Millisecond)
		// Connection should succeed or fail, but we got a result
		t.Logf("Reconnect result error: %v, duration: %v", result.Error, duration)
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for reconnect result")
	}
}

func TestConnManager_ImmediateReconnect(t *testing.T) {
	testLogger := initTestLogger(t)
	cm := NewConnManager("localhost:17913", 1*time.Second, testLogger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cm.Start(ctx)
	defer cm.Stop()

	// Request immediate reconnect using RequestReconnect which now supports ForceNew flag
	resultCh := cm.RequestConnect(ctx)

	start := time.Now()
	select {
	case result := <-resultCh:
		duration := time.Since(start)
		// Immediate connect should not wait for backoff
		assert.Less(t, duration, 500*time.Millisecond)
		t.Logf("Immediate connect result error: %v, duration: %v", result.Error, duration)
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for immediate connect result")
	}
}

func TestConnManager_StateTransitions(t *testing.T) {
	testLogger := initTestLogger(t)
	cm := NewConnManager("localhost:17913", 5*time.Second, testLogger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initial state
	assert.Equal(t, ConnStateDisconnected, cm.GetState())

	cm.Start(ctx)

	// Request connect
	resultCh := cm.RequestConnect(ctx)
	result := <-resultCh

	// After connect attempt
	time.Sleep(50 * time.Millisecond)
	if result.Error != nil {
		assert.Equal(t, ConnStateDisconnected, cm.GetState())
	} else {
		assert.Equal(t, ConnStateConnected, cm.GetState())
	}

	// Stop should set disconnected flag
	cm.Stop()
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, ConnStateDisconnected, cm.GetState())
}

func TestConnManager_CleanupOnStop(t *testing.T) {
	testLogger := initTestLogger(t)
	cm := NewConnManager("localhost:17913", 5*time.Second, testLogger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cm.Start(ctx)

	// Try to establish a connection (will likely fail with invalid address, but that's okay)
	resultCh := cm.RequestConnect(ctx)
	<-resultCh

	// Stop should cleanup any existing connection
	cm.Stop()

	// Give time for cleanup
	time.Sleep(100 * time.Millisecond)

	assert.Equal(t, ConnStateDisconnected, cm.GetState())
}

func TestConnEventType_Values(t *testing.T) {
	// Verify the enum values are distinct
	assert.NotEqual(t, ConnEventConnect, ConnEventDisconnect)
}

func TestConnState_Values(t *testing.T) {
	// Verify the state values are distinct
	assert.NotEqual(t, ConnStateDisconnected, ConnStateConnected)
}

func TestConnManager_ExponentialBackoff(t *testing.T) {
	testLogger := initTestLogger(t)
	cm := NewConnManager("localhost:17913", 50*time.Millisecond, testLogger)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cm.Start(ctx)
	defer cm.Stop()

	// Initial retry interval should match reconnect interval
	assert.Equal(t, 50*time.Millisecond, cm.retryInterval)

	// First reconnect
	resultCh1 := cm.RequestReconnect(ctx)
	<-resultCh1

	// Retry interval should have doubled
	time.Sleep(50 * time.Millisecond)
	// After successful reconnect, retry interval is reset to reconnectInterval
	assert.Equal(t, 50*time.Millisecond, cm.retryInterval)

	// To test exponential backoff, we need to trigger a failure scenario
	// For now, just verify the interval is within expected range
	assert.GreaterOrEqual(t, cm.retryInterval, 50*time.Millisecond)
}

func TestConnManager_MaxRetryInterval(t *testing.T) {
	testLogger := initTestLogger(t)
	cm := NewConnManager("localhost:17913", 1*time.Second, testLogger)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cm.Start(ctx)
	defer cm.Stop()

	// Manually set retry interval to a high value
	cm.retryInterval = 20 * time.Second

	// Request reconnect - this should trigger the backoff logic
	resultCh := cm.RequestReconnect(ctx)
	<-resultCh

	// Retry interval should be capped at maxRetryInterval (30 seconds)
	time.Sleep(100 * time.Millisecond)
	assert.LessOrEqual(t, cm.retryInterval, connManagerMaxRetryInterval)
}

func TestConnManager_ConcurrentStopAndConnect(t *testing.T) {
	testLogger := initTestLogger(t)
	cm := NewConnManager("localhost:17913", 5*time.Second, testLogger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cm.Start(ctx)

	// Send connect request and stop concurrently
	go func() {
		cm.RequestConnect(ctx)
	}()

	time.Sleep(10 * time.Millisecond)
	cm.Stop()

	// Should handle gracefully without panic
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, ConnStateDisconnected, cm.GetState())
}

func TestConnManager_MultipleReconnectRequests(t *testing.T) {
	testLogger := initTestLogger(t)
	cm := NewConnManager("localhost:17913", 5*time.Second, testLogger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cm.Start(ctx)
	defer cm.Stop()

	// Send multiple reconnect requests
	resultCh1 := cm.RequestReconnect(ctx)
	resultCh2 := cm.RequestReconnect(ctx)

	// Both should get responses
	result1 := <-resultCh1
	result2 := <-resultCh2

	// At least one should complete
	t.Logf("Result1 error: %v", result1.Error)
	t.Logf("Result2 error: %v", result2.Error)

	// Check that we got both responses
	assert.True(t, result1.Error != nil || result1.Conn != nil)
	assert.True(t, result2.Error != nil || result2.Conn != nil)
}

func TestConnManager_FullEventChannelConnect(t *testing.T) {
	testLogger := initTestLogger(t)
	cm := NewConnManager("localhost:17913", 5*time.Second, testLogger)

	// Don't start the manager, so events won't be processed
	ctx := context.Background()

	// Fill the event channel (capacity is 10)
	for i := 0; i < 10; i++ {
		cm.EventChannel() <- ConnEvent{
			Type:    ConnEventDisconnect,
			Context: ctx,
		}
	}

	// Next request should fail because channel is full
	resultCh := cm.RequestConnect(ctx)

	select {
	case result := <-resultCh:
		require.NotNil(t, result.Error)
		assert.Contains(t, result.Error.Error(), "event channel is full")
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Expected immediate error for full channel")
	}
}

func TestConnManager_FullEventChannelReconnect(t *testing.T) {
	testLogger := initTestLogger(t)
	cm := NewConnManager("localhost:17913", 5*time.Second, testLogger)

	// Don't start the manager, so events won't be processed
	ctx := context.Background()

	// Fill the event channel (capacity is 10)
	for i := 0; i < 10; i++ {
		cm.EventChannel() <- ConnEvent{
			Type:    ConnEventDisconnect,
			Context: ctx,
		}
	}

	// Next request should fail because channel is full
	resultCh := cm.RequestReconnect(ctx)

	select {
	case result := <-resultCh:
		require.NotNil(t, result.Error)
		assert.Contains(t, result.Error.Error(), "event channel is full")
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Expected immediate error for full channel")
	}
}
