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
	"time"
)

const (
	defaultCBThreshold    = 5
	defaultCBResetTimeout = 60 * time.Second
)

// CircuitState defines the circuit breaker states.
type CircuitState int

// CircuitState defines the circuit breaker states.
const (
	StateClosed   CircuitState = iota // Normal operation
	StateOpen                         // Reject requests until cooldown expires
	StateHalfOpen                     // Allow a single probe
)

// circuitState holds circuit breaker metadata; it does NOT duplicate gRPC clients/conns.
type circuitState struct {
	lastFailureTime       time.Time
	openTime              time.Time
	state                 CircuitState
	consecutiveFailures   int
	halfOpenProbeInFlight bool
}

// IsRequestAllowed checks if a request to the given node is allowed based on circuit breaker state.
// It also handles state transitions from Open to Half-Open when cooldown expires.
func (m *ConnManager[C]) IsRequestAllowed(node string) bool {
	m.cbMu.Lock()
	defer m.cbMu.Unlock()

	cb, exists := m.cbStates[node]
	if !exists {
		return true // No circuit breaker state, allow request
	}

	switch cb.state {
	case StateClosed:
		return true
	case StateOpen:
		// Check if cooldown period has expired
		if time.Since(cb.openTime) >= defaultCBResetTimeout {
			// Transition to Half-Open to allow a single probe request
			cb.state = StateHalfOpen
			cb.halfOpenProbeInFlight = true // Set token for the probe
			m.log.Info().Str("node", node).Msg("circuit breaker transitioned to half-open")
			return true
		}
		return false // Still in cooldown period
	case StateHalfOpen:
		// In half-open state, deny requests if probe is already in flight
		if cb.halfOpenProbeInFlight {
			return false // Probe already in progress, deny additional requests
		}
		// This case should not normally happen since we set the token on transition,
		// but handle it defensively by allowing the request and setting the token
		cb.halfOpenProbeInFlight = true
		return true
	default:
		return true
	}
}

// RecordSuccess resets the circuit breaker state to Closed on successful operation.
// This handles Half-Open -> Closed transitions.
func (m *ConnManager[C]) RecordSuccess(node string) {
	m.cbMu.Lock()
	defer m.cbMu.Unlock()
	cb, exists := m.cbStates[node]
	if !exists {
		// Initialize circuit breaker state
		m.cbStates[node] = &circuitState{
			state:               StateClosed,
			consecutiveFailures: 0,
		}
		return
	}

	cb.state = StateClosed
	cb.consecutiveFailures = 0
	cb.lastFailureTime = time.Time{}
	cb.openTime = time.Time{}
	cb.halfOpenProbeInFlight = false // Clear probe token
}

// RecordFailure updates the circuit breaker state on failed operation.
// Only records failures for transient/internal errors that should count toward opening the circuit.
func (m *ConnManager[C]) RecordFailure(node string, err error) {
	// Only record failure if the error is transient or internal
	if !IsTransientError(err) && !IsInternalError(err) {
		return
	}
	m.cbMu.Lock()
	defer m.cbMu.Unlock()

	cb, exists := m.cbStates[node]
	if !exists {
		// Initialize circuit breaker state
		cb = &circuitState{
			state:               StateClosed,
			consecutiveFailures: 1,
			lastFailureTime:     time.Now(),
		}
		m.cbStates[node] = cb
	} else {
		cb.consecutiveFailures++
		cb.lastFailureTime = time.Now()
	}

	// Check if we should open the circuit
	threshold := defaultCBThreshold
	if cb.consecutiveFailures >= threshold && cb.state == StateClosed {
		cb.state = StateOpen
		cb.openTime = time.Now()
		m.log.Warn().Str("node", node).Int("failures", cb.consecutiveFailures).Msg("circuit breaker opened")
	} else if cb.state == StateHalfOpen {
		// Failed during half-open, go back to open
		cb.state = StateOpen
		cb.openTime = time.Now()
		cb.halfOpenProbeInFlight = false // Clear probe token
		m.log.Warn().Str("node", node).Msg("circuit breaker reopened after half-open failure")
	}
}
