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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/logger"
)

func TestCircuitBreakerStateTransitions(t *testing.T) {
	tests := []struct {
		name           string
		setup          func(*pub)
		actions        []func(*pub, string)
		expectedState  CircuitState
		allowsRequests bool
	}{
		{
			name: "closed_to_open_after_failures",
			setup: func(p *pub) {
				p.recordSuccess("test-node")
			},
			actions: []func(*pub, string){
				func(p *pub, node string) { p.recordFailure(node) },
				func(p *pub, node string) { p.recordFailure(node) },
				func(p *pub, node string) { p.recordFailure(node) },
				func(p *pub, node string) { p.recordFailure(node) },
				func(p *pub, node string) { p.recordFailure(node) },
			},
			expectedState:  StateOpen,
			allowsRequests: false,
		},
		{
			name: "closed_remains_closed_below_threshold",
			setup: func(p *pub) {
				p.recordSuccess("test-node")
			},
			actions: []func(*pub, string){
				func(p *pub, node string) { p.recordFailure(node) },
				func(p *pub, node string) { p.recordFailure(node) },
				func(p *pub, node string) { p.recordFailure(node) },
			},
			expectedState:  StateClosed,
			allowsRequests: true,
		},
		{
			name: "open_to_half_open_after_cooldown",
			setup: func(p *pub) {
				p.recordSuccess("test-node")
				// Trip the circuit breaker
				for i := 0; i < defaultCBThreshold; i++ {
					p.recordFailure("test-node")
				}
				// Simulate cooldown period has passed
				p.cbMu.Lock()
				cb := p.cbStates["test-node"]
				cb.openTime = time.Now().Add(-defaultCBResetTimeout - time.Second)
				p.cbMu.Unlock()
			},
			actions:        []func(*pub, string){},
			expectedState:  StateOpen, // Will transition to half-open in isRequestAllowed
			allowsRequests: true,      // Should allow requests after cooldown
		},
		{
			name: "half_open_to_closed_on_success",
			setup: func(p *pub) {
				p.recordSuccess("test-node")
				// Trip the circuit breaker
				for i := 0; i < defaultCBThreshold; i++ {
					p.recordFailure("test-node")
				}
				// Set to half-open state
				p.cbMu.Lock()
				cb := p.cbStates["test-node"]
				cb.state = StateHalfOpen
				p.cbMu.Unlock()
			},
			actions: []func(*pub, string){
				func(p *pub, node string) { p.recordSuccess(node) },
			},
			expectedState:  StateClosed,
			allowsRequests: true,
		},
		{
			name: "half_open_to_open_on_failure",
			setup: func(p *pub) {
				p.recordSuccess("test-node")
				// Trip the circuit breaker
				for i := 0; i < defaultCBThreshold; i++ {
					p.recordFailure("test-node")
				}
				// Set to half-open state
				p.cbMu.Lock()
				cb := p.cbStates["test-node"]
				cb.state = StateHalfOpen
				p.cbMu.Unlock()
			},
			actions: []func(*pub, string){
				func(p *pub, node string) { p.recordFailure(node) },
			},
			expectedState:  StateOpen,
			allowsRequests: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &pub{
				cbStates: make(map[string]*circuitState),
				cbMu:     sync.RWMutex{},
				log:      logger.GetLogger("test"),
			}

			// Setup
			if tt.setup != nil {
				tt.setup(p)
			}

			// Execute actions
			for _, action := range tt.actions {
				action(p, "test-node")
			}

			// Check final state
			p.cbMu.RLock()
			cb, exists := p.cbStates["test-node"]
			p.cbMu.RUnlock()

			require.True(t, exists, "circuit breaker state should exist")
			assert.Equal(t, tt.expectedState, cb.state, "circuit breaker state mismatch")

			// Check if requests are allowed
			allowed := p.isRequestAllowed("test-node")
			assert.Equal(t, tt.allowsRequests, allowed, "request allowance mismatch")
		})
	}
}

func TestCircuitBreakerConcurrency(t *testing.T) {
	p := &pub{
		cbStates: make(map[string]*circuitState),
		cbMu:     sync.RWMutex{},
		log:      logger.GetLogger("test"),
	}

	const numGoroutines = 100
	const numOperations = 50
	node := "test-node"

	var wg sync.WaitGroup
	wg.Add(numGoroutines * 2) // Half for success, half for failure

	// Simulate concurrent successes
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				p.recordSuccess(node)
				p.isRequestAllowed(node)
			}
		}()
	}

	// Simulate concurrent failures
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				p.recordFailure(node)
				p.isRequestAllowed(node)
			}
		}()
	}

	wg.Wait()

	// Verify circuit breaker state exists and is in a valid state
	p.cbMu.RLock()
	cb, exists := p.cbStates[node]
	p.cbMu.RUnlock()

	require.True(t, exists, "circuit breaker state should exist")
	assert.Contains(t, []CircuitState{StateClosed, StateOpen, StateHalfOpen}, cb.state, "circuit breaker should be in a valid state")
}

func TestCircuitBreakerMultipleNodes(t *testing.T) {
	p := &pub{
		cbStates: make(map[string]*circuitState),
		cbMu:     sync.RWMutex{},
		log:      logger.GetLogger("test"),
	}

	nodes := []string{"node1", "node2", "node3"}

	// Initialize all nodes
	for _, node := range nodes {
		p.recordSuccess(node)
	}

	// Trip circuit breaker for node1 only
	for i := 0; i < defaultCBThreshold; i++ {
		p.recordFailure("node1")
	}

	// Add some failures to node2 but below threshold
	for i := 0; i < defaultCBThreshold-1; i++ {
		p.recordFailure("node2")
	}

	// Keep node3 healthy
	p.recordSuccess("node3")

	// Verify states
	assert.False(t, p.isRequestAllowed("node1"), "node1 should have circuit breaker open")
	assert.True(t, p.isRequestAllowed("node2"), "node2 should still allow requests")
	assert.True(t, p.isRequestAllowed("node3"), "node3 should allow requests")

	// Check circuit breaker states
	p.cbMu.RLock()
	defer p.cbMu.RUnlock()

	cb1, exists1 := p.cbStates["node1"]
	require.True(t, exists1)
	assert.Equal(t, StateOpen, cb1.state)

	cb2, exists2 := p.cbStates["node2"]
	require.True(t, exists2)
	assert.Equal(t, StateClosed, cb2.state)

	cb3, exists3 := p.cbStates["node3"]
	require.True(t, exists3)
	assert.Equal(t, StateClosed, cb3.state)
}

func TestCircuitBreakerRecoveryAfterCooldown(t *testing.T) {
	p := &pub{
		cbStates: make(map[string]*circuitState),
		cbMu:     sync.RWMutex{},
		log:      logger.GetLogger("test"),
	}

	node := "test-node"

	// Initialize node
	p.recordSuccess(node)

	// Trip circuit breaker
	for i := 0; i < defaultCBThreshold; i++ {
		p.recordFailure(node)
	}

	// Verify circuit is open
	assert.False(t, p.isRequestAllowed(node), "circuit should be open")

	// Simulate cooldown period passage
	p.cbMu.Lock()
	cb := p.cbStates[node]
	cb.openTime = time.Now().Add(-defaultCBResetTimeout - time.Second)
	p.cbMu.Unlock()

	// Check that circuit allows requests (transitions to half-open)
	allowed := p.isRequestAllowed(node)
	assert.True(t, allowed, "circuit should allow requests after cooldown")

	// Verify state transitioned to half-open
	p.cbMu.RLock()
	assert.Equal(t, StateHalfOpen, cb.state, "circuit should be in half-open state")
	p.cbMu.RUnlock()

	// Successful request should close the circuit
	p.recordSuccess(node)

	p.cbMu.RLock()
	assert.Equal(t, StateClosed, cb.state, "circuit should be closed after success")
	assert.Equal(t, 0, cb.consecutiveFailures, "failure count should be reset")
	p.cbMu.RUnlock()
}

func TestCircuitBreakerInitialization(t *testing.T) {
	p := &pub{
		cbStates: make(map[string]*circuitState),
		cbMu:     sync.RWMutex{},
		log:      logger.GetLogger("test"),
	}

	node := "new-node"

	// First request to non-existent circuit breaker should be allowed
	allowed := p.isRequestAllowed(node)
	assert.True(t, allowed, "requests should be allowed for non-existent circuit breaker")

	// Record success should initialize the circuit breaker
	p.recordSuccess(node)

	p.cbMu.RLock()
	cb, exists := p.cbStates[node]
	p.cbMu.RUnlock()

	require.True(t, exists, "circuit breaker should be initialized")
	assert.Equal(t, StateClosed, cb.state, "new circuit breaker should be closed")
	assert.Equal(t, 0, cb.consecutiveFailures, "new circuit breaker should have zero failures")
}

func TestCircuitBreakerFailureThresholdEdgeCase(t *testing.T) {
	p := &pub{
		cbStates: make(map[string]*circuitState),
		cbMu:     sync.RWMutex{},
		log:      logger.GetLogger("test"),
	}

	node := "test-node"

	// Initialize node
	p.recordSuccess(node)

	// Add failures just below threshold
	for i := 0; i < defaultCBThreshold-1; i++ {
		p.recordFailure(node)
	}

	// Circuit should still be closed
	p.cbMu.RLock()
	cb := p.cbStates[node]
	assert.Equal(t, StateClosed, cb.state, "circuit should still be closed")
	assert.Equal(t, defaultCBThreshold-1, cb.consecutiveFailures, "failure count should be at threshold-1")
	p.cbMu.RUnlock()

	// One more failure should open the circuit
	p.recordFailure(node)

	p.cbMu.RLock()
	assert.Equal(t, StateOpen, cb.state, "circuit should be open after reaching threshold")
	assert.Equal(t, defaultCBThreshold, cb.consecutiveFailures, "failure count should be at threshold")
	p.cbMu.RUnlock()
}
