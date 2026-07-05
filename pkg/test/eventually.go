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

package test

import (
	"reflect"
	"sync/atomic"
	"time"

	"github.com/onsi/gomega"
	gomegatypes "github.com/onsi/gomega/types"

	"github.com/apache/skywalking-banyandb/pkg/test/flags"
)

// ECAssertion pairs a gomega Eventually with a following Consistently. It mirrors
// gomega.Eventually's layout: intervals may be supplied up front or via the
// fluent WithTimeout/WithPolling methods, and the matcher is applied via
// Should/ShouldNot.
type ECAssertion struct {
	actual            interface{}
	eventuallyTimeout interface{}
	polling           interface{}
}

// EventuallyConsistently returns an assertion that first waits for actual to
// satisfy the matcher (Eventually) and then requires it to keep satisfying the
// matcher for a stability window (Consistently). Pairing convergence with a
// stability window catches transient states that briefly match and then regress,
// which a lone Eventually would accept.
//
// intervals follow gomega.Eventually semantics — (timeout) or (timeout, polling).
// They can also be set fluently via WithTimeout/WithPolling. When the timeout is
// omitted it defaults to flags.EventuallyTimeout. The Consistently window always
// runs and reuses the polling interval, if any; its length adapts to how the
// Eventually phase converged — flags.QuickStabilityTimeout after a first-try
// success, flags.StabilityTimeout after a retried convergence (see Should).
func EventuallyConsistently(actual interface{}, intervals ...interface{}) ECAssertion {
	a := ECAssertion{actual: actual}
	if len(intervals) > 0 {
		a.eventuallyTimeout = intervals[0]
	}
	if len(intervals) > 1 {
		a.polling = intervals[1]
	}
	return a
}

// WithTimeout overrides the Eventually convergence timeout, mirroring
// gomega.AsyncAssertion.WithTimeout.
func (a ECAssertion) WithTimeout(timeout time.Duration) ECAssertion {
	a.eventuallyTimeout = timeout
	return a
}

// WithPolling overrides the polling interval used by both the Eventually and the
// Consistently phase, mirroring gomega.AsyncAssertion.WithPolling.
func (a ECAssertion) WithPolling(polling time.Duration) ECAssertion {
	a.polling = polling
	return a
}

// Should asserts the matcher is reached and then held. It returns true only when
// both the Eventually and the Consistently assertion pass, matching the bool
// return of gomega's Should. optionalDescription is forwarded to both assertions.
//
// The Consistently phase always runs, but its window is adaptive. When the
// Eventually phase succeeds on its very first evaluation, the state was already
// settled before the assertion began, so a short flags.QuickStabilityTimeout
// window (still a dozen re-samples) suffices to reject a one-off lucky match.
// Only assertions that had to retry — proof of an in-flight async process that
// could still regress — pay the full flags.StabilityTimeout window. This keeps
// the regression guard everywhere without charging every static assertion in
// the large query tables for a full window.
func (a ECAssertion) Should(matcher gomegatypes.GomegaMatcher, optionalDescription ...interface{}) bool {
	probe, attempts := countEvaluations(a.actual)
	if !gomega.EventuallyWithOffset(1, probe, a.eventuallyIntervals()...).Should(matcher, optionalDescription...) {
		return false
	}
	return gomega.ConsistentlyWithOffset(1, a.actual, a.consistentlyIntervals(atomic.LoadInt32(attempts))...).Should(matcher, optionalDescription...)
}

// ShouldNot asserts the matcher is avoided and then kept avoided. It returns true
// only when both assertions pass, matching the bool return of gomega's ShouldNot.
// optionalDescription is forwarded to both assertions. The stability window is
// adaptive — see Should.
func (a ECAssertion) ShouldNot(matcher gomegatypes.GomegaMatcher, optionalDescription ...interface{}) bool {
	probe, attempts := countEvaluations(a.actual)
	if !gomega.EventuallyWithOffset(1, probe, a.eventuallyIntervals()...).ShouldNot(matcher, optionalDescription...) {
		return false
	}
	return gomega.ConsistentlyWithOffset(1, a.actual, a.consistentlyIntervals(atomic.LoadInt32(attempts))...).ShouldNot(matcher, optionalDescription...)
}

// countEvaluations wraps a func-typed actual so every gomega poll increments a
// counter, letting the caller tell first-try convergence from retried
// convergence. Non-func actuals (plain values, channels) are returned as-is
// with the counter pre-set past 1, so they conservatively keep the full
// stability window.
func countEvaluations(actual interface{}) (interface{}, *int32) {
	attempts := new(int32)
	v := reflect.ValueOf(actual)
	if v.Kind() != reflect.Func {
		*attempts = 2
		return actual, attempts
	}
	probe := reflect.MakeFunc(v.Type(), func(args []reflect.Value) []reflect.Value {
		atomic.AddInt32(attempts, 1)
		return v.Call(args)
	}).Interface()
	return probe, attempts
}

func (a ECAssertion) eventuallyIntervals() []interface{} {
	timeout := a.eventuallyTimeout
	if timeout == nil {
		timeout = flags.EventuallyTimeout
	}
	out := []interface{}{timeout}
	if a.polling != nil {
		out = append(out, a.polling)
	}
	return out
}

func (a ECAssertion) consistentlyIntervals(eventuallyAttempts int32) []interface{} {
	window := flags.StabilityTimeout
	if eventuallyAttempts == 1 {
		window = flags.QuickStabilityTimeout
	}
	out := []interface{}{window}
	if a.polling != nil {
		out = append(out, a.polling)
	}
	return out
}
