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

package grpchelper

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

const testNodeName = "test-node"

var (
	// Test errors for circuit breaker testing.
	errTransient = status.Error(codes.Unavailable, "service unavailable")
	errInternal  = status.Error(codes.Internal, "internal server error")
	errNonRetry  = status.Error(codes.InvalidArgument, "invalid argument")
)

func init() {
	_ = logger.Init(logger.Logging{
		Env:   "dev",
		Level: "warn",
	})
}

func newTestConnManager() *ConnManager[*mockClient] {
	return NewConnManager(ConnManagerConfig[*mockClient]{
		Handler: &mockHandler{},
		Logger:  logger.GetLogger("test-cb"),
	})
}

func TestCircuitBreakerStateTransitions(t *testing.T) {
	tests := []struct {
		name           string
		setup          func(*ConnManager[*mockClient])
		actions        []func(*ConnManager[*mockClient], string)
		expectedState  CircuitState
		allowsRequests bool
	}{
		{
			name: "closed_to_open_after_failures",
			setup: func(m *ConnManager[*mockClient]) {
				m.RecordSuccess(testNodeName)
			},
			actions: []func(*ConnManager[*mockClient], string){
				func(m *ConnManager[*mockClient], node string) { m.RecordFailure(node, errTransient) },
				func(m *ConnManager[*mockClient], node string) { m.RecordFailure(node, errTransient) },
				func(m *ConnManager[*mockClient], node string) { m.RecordFailure(node, errTransient) },
				func(m *ConnManager[*mockClient], node string) { m.RecordFailure(node, errTransient) },
				func(m *ConnManager[*mockClient], node string) { m.RecordFailure(node, errTransient) },
			},
			expectedState:  StateOpen,
			allowsRequests: false,
		},
		{
			name: "closed_remains_closed_below_threshold",
			setup: func(m *ConnManager[*mockClient]) {
				m.RecordSuccess(testNodeName)
			},
			actions: []func(*ConnManager[*mockClient], string){
				func(m *ConnManager[*mockClient], node string) { m.RecordFailure(node, errTransient) },
				func(m *ConnManager[*mockClient], node string) { m.RecordFailure(node, errTransient) },
				func(m *ConnManager[*mockClient], node string) { m.RecordFailure(node, errTransient) },
			},
			expectedState:  StateClosed,
			allowsRequests: true,
		},
		{
			name: "open_to_half_open_after_cooldown",
			setup: func(m *ConnManager[*mockClient]) {
				m.RecordSuccess(testNodeName)
				// Trip the circuit breaker
				for i := 0; i < defaultCBThreshold; i++ {
					m.RecordFailure(testNodeName, errTransient)
				}
				// Simulate cooldown period has passed
				m.cbMu.Lock()
				cb := m.cbStates[testNodeName]
				cb.openTime = time.Now().Add(-defaultCBResetTimeout - time.Second)
				m.cbMu.Unlock()
			},
			actions:        []func(*ConnManager[*mockClient], string){},
			expectedState:  StateOpen, // Will transition to half-open in IsRequestAllowed
			allowsRequests: true,      // Should allow requests after cooldown
		},
		{
			name: "half_open_to_closed_on_success",
			setup: func(m *ConnManager[*mockClient]) {
				m.RecordSuccess(testNodeName)
				// Trip the circuit breaker
				for i := 0; i < defaultCBThreshold; i++ {
					m.RecordFailure(testNodeName, errTransient)
				}
				// Set to half-open state
				m.cbMu.Lock()
				cb := m.cbStates[testNodeName]
				cb.state = StateHalfOpen
				m.cbMu.Unlock()
			},
			actions: []func(*ConnManager[*mockClient], string){
				func(m *ConnManager[*mockClient], node string) { m.RecordSuccess(node) },
			},
			expectedState:  StateClosed,
			allowsRequests: true,
		},
		{
			name: "half_open_to_open_on_failure",
			setup: func(m *ConnManager[*mockClient]) {
				m.RecordSuccess(testNodeName)
				// Trip the circuit breaker
				for i := 0; i < defaultCBThreshold; i++ {
					m.RecordFailure(testNodeName, errTransient)
				}
				// Set to half-open state
				m.cbMu.Lock()
				cb := m.cbStates[testNodeName]
				cb.state = StateHalfOpen
				m.cbMu.Unlock()
			},
			actions: []func(*ConnManager[*mockClient], string){
				func(m *ConnManager[*mockClient], node string) { m.RecordFailure(node, errTransient) },
			},
			expectedState:  StateOpen,
			allowsRequests: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := newTestConnManager()
			defer m.GracefulStop()

			// Setup
			if tt.setup != nil {
				tt.setup(m)
			}

			// Execute actions
			for _, action := range tt.actions {
				action(m, testNodeName)
			}

			// Check final state
			m.cbMu.RLock()
			cb, exists := m.cbStates[testNodeName]
			m.cbMu.RUnlock()

			require.True(t, exists, "circuit breaker state should exist")
			assert.Equal(t, tt.expectedState, cb.state, "circuit breaker state mismatch")

			// Check if requests are allowed
			allowed := m.IsRequestAllowed(testNodeName)
			assert.Equal(t, tt.allowsRequests, allowed, "request allowance mismatch")
		})
	}
}

func TestCircuitBreakerConcurrency(t *testing.T) {
	m := newTestConnManager()
	defer m.GracefulStop()

	const numGoroutines = 100
	const numOperations = 50
	node := testNodeName

	var wg sync.WaitGroup
	wg.Add(numGoroutines * 2) // Half for success, half for failure

	// Simulate concurrent successes
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				m.RecordSuccess(node)
				m.IsRequestAllowed(node)
			}
		}()
	}

	// Simulate concurrent failures
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				m.RecordFailure(node, errTransient)
				m.IsRequestAllowed(node)
			}
		}()
	}

	wg.Wait()

	// Verify circuit breaker state exists and is in a valid state
	m.cbMu.RLock()
	cb, exists := m.cbStates[node]
	m.cbMu.RUnlock()

	require.True(t, exists, "circuit breaker state should exist")
	assert.Contains(t, []CircuitState{StateClosed, StateOpen, StateHalfOpen}, cb.state, "circuit breaker should be in a valid state")
}

func TestCircuitBreakerMultipleNodes(t *testing.T) {
	m := newTestConnManager()
	defer m.GracefulStop()

	nodes := []string{"node1", "node2", "node3"}

	// Initialize all nodes
	for _, node := range nodes {
		m.RecordSuccess(node)
	}

	// Trip circuit breaker for node1 only
	for i := 0; i < defaultCBThreshold; i++ {
		m.RecordFailure("node1", errTransient)
	}

	// Add some failures to node2 but below threshold
	for i := 0; i < defaultCBThreshold-1; i++ {
		m.RecordFailure("node2", errTransient)
	}

	// Keep node3 healthy
	m.RecordSuccess("node3")

	// Verify states
	assert.False(t, m.IsRequestAllowed("node1"), "node1 should have circuit breaker open")
	assert.True(t, m.IsRequestAllowed("node2"), "node2 should still allow requests")
	assert.True(t, m.IsRequestAllowed("node3"), "node3 should allow requests")

	// Check circuit breaker states
	m.cbMu.RLock()
	defer m.cbMu.RUnlock()

	cb1, exists1 := m.cbStates["node1"]
	require.True(t, exists1)
	assert.Equal(t, StateOpen, cb1.state)

	cb2, exists2 := m.cbStates["node2"]
	require.True(t, exists2)
	assert.Equal(t, StateClosed, cb2.state)

	cb3, exists3 := m.cbStates["node3"]
	require.True(t, exists3)
	assert.Equal(t, StateClosed, cb3.state)
}

func TestCircuitBreakerRecoveryAfterCooldown(t *testing.T) {
	m := newTestConnManager()
	defer m.GracefulStop()
	node := testNodeName

	// Initialize node
	m.RecordSuccess(node)

	// Trip circuit breaker
	for i := 0; i < defaultCBThreshold; i++ {
		m.RecordFailure(node, errTransient)
	}

	// Verify circuit is open
	assert.False(t, m.IsRequestAllowed(node), "circuit should be open")

	// Simulate cooldown period passage
	m.cbMu.Lock()
	cb := m.cbStates[node]
	cb.openTime = time.Now().Add(-defaultCBResetTimeout - time.Second)
	m.cbMu.Unlock()

	// Check that circuit allows requests (transitions to half-open)
	allowed := m.IsRequestAllowed(node)
	assert.True(t, allowed, "circuit should allow requests after cooldown")

	// Verify state transitioned to half-open
	m.cbMu.RLock()
	assert.Equal(t, StateHalfOpen, cb.state, "circuit should be in half-open state")
	m.cbMu.RUnlock()

	// Successful request should close the circuit
	m.RecordSuccess(node)

	m.cbMu.RLock()
	assert.Equal(t, StateClosed, cb.state, "circuit should be closed after success")
	assert.Equal(t, 0, cb.consecutiveFailures, "failure count should be reset")
	m.cbMu.RUnlock()
}

func TestCircuitBreakerInitialization(t *testing.T) {
	m := newTestConnManager()
	defer m.GracefulStop()
	node := "new-node"

	// First request to non-existent circuit breaker should be allowed
	allowed := m.IsRequestAllowed(node)
	assert.True(t, allowed, "requests should be allowed for non-existent circuit breaker")

	// Record success should initialize the circuit breaker
	m.RecordSuccess(node)

	m.cbMu.RLock()
	cb, exists := m.cbStates[node]
	m.cbMu.RUnlock()

	require.True(t, exists, "circuit breaker should be initialized")
	assert.Equal(t, StateClosed, cb.state, "new circuit breaker should be closed")
	assert.Equal(t, 0, cb.consecutiveFailures, "new circuit breaker should have zero failures")
}

func TestCircuitBreakerFailureThresholdEdgeCase(t *testing.T) {
	m := newTestConnManager()
	defer m.GracefulStop()
	node := testNodeName

	// Initialize node
	m.RecordSuccess(node)

	// Add failures just below threshold
	for i := 0; i < defaultCBThreshold-1; i++ {
		m.RecordFailure(node, errTransient)
	}

	// Circuit should still be closed
	m.cbMu.RLock()
	cb := m.cbStates[node]
	assert.Equal(t, StateClosed, cb.state, "circuit should still be closed")
	assert.Equal(t, defaultCBThreshold-1, cb.consecutiveFailures, "failure count should be at threshold-1")
	m.cbMu.RUnlock()

	// One more failure should open the circuit
	m.RecordFailure(node, errTransient)

	m.cbMu.RLock()
	assert.Equal(t, StateOpen, cb.state, "circuit should be open after reaching threshold")
	assert.Equal(t, defaultCBThreshold, cb.consecutiveFailures, "failure count should be at threshold")
	m.cbMu.RUnlock()
}

func TestCircuitBreakerSingleProbeEnforcement(t *testing.T) {
	m := newTestConnManager()
	defer m.GracefulStop()
	node := testNodeName

	// Initialize and trip circuit breaker
	m.RecordSuccess(node)
	for i := 0; i < defaultCBThreshold; i++ {
		m.RecordFailure(node, errTransient)
	}

	// Verify circuit is open
	assert.False(t, m.IsRequestAllowed(node), "circuit should be open")

	// Simulate cooldown period passage
	m.cbMu.Lock()
	cb := m.cbStates[node]
	cb.openTime = time.Now().Add(-defaultCBResetTimeout - time.Second)
	m.cbMu.Unlock()

	// First request should transition to half-open and be allowed
	allowed1 := m.IsRequestAllowed(node)
	assert.True(t, allowed1, "first request after cooldown should be allowed and transition to half-open")

	// Verify state is half-open with probe in flight
	m.cbMu.RLock()
	assert.Equal(t, StateHalfOpen, cb.state, "circuit should be in half-open state")
	assert.True(t, cb.halfOpenProbeInFlight, "probe should be marked as in flight")
	m.cbMu.RUnlock()

	// Second request should be denied while probe is in flight
	allowed2 := m.IsRequestAllowed(node)
	assert.False(t, allowed2, "second request should be denied while probe is in flight")

	// Third request should also be denied
	allowed3 := m.IsRequestAllowed(node)
	assert.False(t, allowed3, "third request should also be denied while probe is in flight")
}

func TestCircuitBreakerSingleProbeSuccess(t *testing.T) {
	m := newTestConnManager()
	defer m.GracefulStop()
	node := testNodeName

	// Setup half-open state with probe in flight
	m.RecordSuccess(node)
	for i := 0; i < defaultCBThreshold; i++ {
		m.RecordFailure(node, errTransient)
	}

	m.cbMu.Lock()
	cb := m.cbStates[node]
	cb.openTime = time.Now().Add(-defaultCBResetTimeout - time.Second)
	m.cbMu.Unlock()

	// Transition to half-open
	allowed := m.IsRequestAllowed(node)
	assert.True(t, allowed, "first request should be allowed")

	// Verify probe is in flight
	m.cbMu.RLock()
	assert.True(t, cb.halfOpenProbeInFlight, "probe should be in flight")
	m.cbMu.RUnlock()

	// Record success (probe succeeds)
	m.RecordSuccess(node)

	// Verify circuit is closed and probe token is cleared
	m.cbMu.RLock()
	assert.Equal(t, StateClosed, cb.state, "circuit should be closed after successful probe")
	assert.False(t, cb.halfOpenProbeInFlight, "probe token should be cleared")
	m.cbMu.RUnlock()

	// Subsequent requests should be allowed in closed state
	assert.True(t, m.IsRequestAllowed(node), "requests should be allowed in closed state")
}

func TestCircuitBreakerSingleProbeFailure(t *testing.T) {
	m := newTestConnManager()
	defer m.GracefulStop()
	node := testNodeName

	// Setup half-open state with probe in flight
	m.RecordSuccess(node)
	for i := 0; i < defaultCBThreshold; i++ {
		m.RecordFailure(node, errTransient)
	}

	m.cbMu.Lock()
	cb := m.cbStates[node]
	cb.openTime = time.Now().Add(-defaultCBResetTimeout - time.Second)
	m.cbMu.Unlock()

	// Transition to half-open
	allowed := m.IsRequestAllowed(node)
	assert.True(t, allowed, "first request should be allowed")

	// Verify probe is in flight
	m.cbMu.RLock()
	assert.True(t, cb.halfOpenProbeInFlight, "probe should be in flight")
	m.cbMu.RUnlock()

	// Record failure (probe fails)
	m.RecordFailure(node, errTransient)

	// Verify circuit is back to open and probe token is cleared
	m.cbMu.RLock()
	assert.Equal(t, StateOpen, cb.state, "circuit should be back to open after failed probe")
	assert.False(t, cb.halfOpenProbeInFlight, "probe token should be cleared")
	m.cbMu.RUnlock()

	// Subsequent requests should be denied in open state
	assert.False(t, m.IsRequestAllowed(node), "requests should be denied in open state")
}

func TestCircuitBreakerConcurrentProbeAttempts(t *testing.T) {
	m := newTestConnManager()
	defer m.GracefulStop()
	node := testNodeName

	// Setup open state ready for half-open transition
	m.RecordSuccess(node)
	for i := 0; i < defaultCBThreshold; i++ {
		m.RecordFailure(node, errTransient)
	}

	m.cbMu.Lock()
	cb := m.cbStates[node]
	cb.openTime = time.Now().Add(-defaultCBResetTimeout - time.Second)
	m.cbMu.Unlock()

	const numGoroutines = 10
	var wg sync.WaitGroup
	results := make(chan bool, numGoroutines)

	// Simulate concurrent requests attempting to probe
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			allowed := m.IsRequestAllowed(node)
			results <- allowed
		}()
	}

	wg.Wait()
	close(results)

	// Count allowed requests
	allowedCount := 0
	for result := range results {
		if result {
			allowedCount++
		}
	}

	// Only one request should be allowed (the one that set the probe token)
	assert.Equal(t, 1, allowedCount, "exactly one request should be allowed in half-open state")

	// Verify circuit is in half-open with probe in flight
	m.cbMu.RLock()
	assert.Equal(t, StateHalfOpen, cb.state, "circuit should be in half-open state")
	assert.True(t, cb.halfOpenProbeInFlight, "probe should be marked as in flight")
	m.cbMu.RUnlock()
}

func TestCircuitBreakerProbeTokenInitialization(t *testing.T) {
	m := newTestConnManager()
	defer m.GracefulStop()
	node := testNodeName

	// Test that new circuit breaker states have probe token cleared
	m.RecordSuccess(node)

	m.cbMu.RLock()
	cb := m.cbStates[node]
	assert.False(t, cb.halfOpenProbeInFlight, "new circuit breaker should have probe token cleared")
	m.cbMu.RUnlock()
}

func TestCircuitBreakerErrorFiltering(t *testing.T) {
	m := newTestConnManager()
	defer m.GracefulStop()
	node := testNodeName

	// Verify initial state is closed
	assert.True(t, m.IsRequestAllowed(node))
	m.cbMu.Lock()
	_, exists := m.cbStates[node]
	m.cbMu.Unlock()
	assert.False(t, exists)

	// Try to trip circuit with non-retryable error
	for i := 0; i < defaultCBThreshold*2; i++ {
		m.RecordFailure(node, errNonRetry)
	}

	// Circuit should still be closed (non-retryable errors don't count)
	assert.True(t, m.IsRequestAllowed(node))
	m.cbMu.Lock()
	cb, exists := m.cbStates[node]
	m.cbMu.Unlock()
	// State should not exist or be closed with 0 failures
	if exists {
		assert.Equal(t, StateClosed, cb.state)
		assert.Equal(t, 0, cb.consecutiveFailures)
	}

	// Now try with transient errors - these should count
	for i := 0; i < defaultCBThreshold; i++ {
		m.RecordFailure(node, errTransient)
	}

	// Circuit should now be open
	assert.False(t, m.IsRequestAllowed(node))
	m.cbMu.Lock()
	cb, exists = m.cbStates[node]
	m.cbMu.Unlock()
	assert.True(t, exists)
	assert.Equal(t, StateOpen, cb.state)
	assert.Equal(t, defaultCBThreshold, cb.consecutiveFailures)

	// Reset for internal error test
	m.RecordSuccess(node)
	assert.True(t, m.IsRequestAllowed(node))

	// Try with internal errors - these should also count
	for i := 0; i < defaultCBThreshold; i++ {
		m.RecordFailure(node, errInternal)
	}

	// Circuit should be open again
	assert.False(t, m.IsRequestAllowed(node))
	m.cbMu.Lock()
	cb, exists = m.cbStates[node]
	m.cbMu.Unlock()
	assert.True(t, exists)
	assert.Equal(t, StateOpen, cb.state)
	assert.Equal(t, defaultCBThreshold, cb.consecutiveFailures)
}

func TestCircuitBreakerFailoverErrors(t *testing.T) {
	m := newTestConnManager()
	defer m.GracefulStop()
	node := testNodeName

	// Test failover errors from Recv() - using codes.Unavailable which is a failover error
	failoverErr := status.Error(codes.Unavailable, "service unavailable")

	// Verify initial state is closed
	assert.True(t, m.IsRequestAllowed(node))

	// Simulate failover errors that should increment circuit breaker
	for i := 0; i < defaultCBThreshold; i++ {
		m.RecordFailure(node, failoverErr)
	}

	// Circuit should be open after failover errors
	assert.False(t, m.IsRequestAllowed(node))
	m.cbMu.Lock()
	cb, exists := m.cbStates[node]
	m.cbMu.Unlock()
	assert.True(t, exists)
	assert.Equal(t, StateOpen, cb.state)
	assert.Equal(t, defaultCBThreshold, cb.consecutiveFailures)

	// Reset and test with common.Error (simulating Close() method errors)
	m.RecordSuccess(node)
	assert.True(t, m.IsRequestAllowed(node))

	// Test with common.Error types that would come from Close() method
	internalErr := common.NewErrorWithStatus(modelv1.Status_STATUS_INTERNAL_ERROR, "internal error")
	for i := 0; i < defaultCBThreshold; i++ {
		m.RecordFailure(node, internalErr)
	}

	// Circuit should be open again
	assert.False(t, m.IsRequestAllowed(node))
	m.cbMu.Lock()
	cb, exists = m.cbStates[node]
	m.cbMu.Unlock()
	assert.True(t, exists)
	assert.Equal(t, StateOpen, cb.state)
	assert.Equal(t, defaultCBThreshold, cb.consecutiveFailures)
}
