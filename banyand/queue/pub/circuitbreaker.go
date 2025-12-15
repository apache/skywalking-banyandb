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

// isRequestAllowed checks if a request to the given node is allowed based on circuit breaker state.
// It also handles state transitions from Open to Half-Open when cooldown expires.
func (p *pub) isRequestAllowed(node string) bool {
	p.cbMu.Lock()
	defer p.cbMu.Unlock()

	cb, exists := p.cbStates[node]
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
			p.log.Info().Str("node", node).Msg("circuit breaker transitioned to half-open")
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

// recordSuccess resets the circuit breaker state to Closed on successful operation.
// This handles Half-Open -> Closed transitions.
func (p *pub) recordSuccess(node string) {
	p.cbMu.Lock()
	defer p.cbMu.Unlock()

	cb, exists := p.cbStates[node]
	if !exists {
		// Initialize circuit breaker state
		p.cbStates[node] = &circuitState{
			state:               StateClosed,
			consecutiveFailures: 0,
		}
		return
	}

	// Reset to closed state
	cb.state = StateClosed
	cb.consecutiveFailures = 0
	cb.lastFailureTime = time.Time{}
	cb.openTime = time.Time{}
	cb.halfOpenProbeInFlight = false // Clear probe token
}

// recordFailure updates the circuit breaker state on failed operation.
// Only records failures for transient/internal errors that should count toward opening the circuit.
func (p *pub) recordFailure(node string, err error) {
	// Only record failure if the error is transient or internal
	if !isTransientError(err) && !isInternalError(err) {
		return
	}
	p.cbMu.Lock()
	defer p.cbMu.Unlock()

	cb, exists := p.cbStates[node]
	if !exists {
		// Initialize circuit breaker state
		cb = &circuitState{
			state:               StateClosed,
			consecutiveFailures: 1,
			lastFailureTime:     time.Now(),
		}
		p.cbStates[node] = cb
	} else {
		cb.consecutiveFailures++
		cb.lastFailureTime = time.Now()
	}

	// Check if we should open the circuit
	threshold := defaultCBThreshold
	if cb.consecutiveFailures >= threshold && cb.state == StateClosed {
		cb.state = StateOpen
		cb.openTime = time.Now()
		p.log.Warn().Str("node", node).Int("failures", cb.consecutiveFailures).Msg("circuit breaker opened")
	} else if cb.state == StateHalfOpen {
		// Failed during half-open, go back to open
		cb.state = StateOpen
		cb.openTime = time.Now()
		cb.halfOpenProbeInFlight = false // Clear probe token
		p.log.Warn().Str("node", node).Msg("circuit breaker reopened after half-open failure")
	}
}
