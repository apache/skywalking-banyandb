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
// omitted it defaults to flags.EventuallyTimeout; the Consistently window always
// uses flags.ConsistentlyTimeout and reuses the polling interval, if any.
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

// Should asserts the matcher is reached and then held. optionalDescription is
// forwarded to both assertions, matching gomega's Should.
func (a ECAssertion) Should(matcher gomegatypes.GomegaMatcher, optionalDescription ...interface{}) {
	gomega.EventuallyWithOffset(1, a.actual, a.eventuallyIntervals()...).Should(matcher, optionalDescription...)
	gomega.ConsistentlyWithOffset(1, a.actual, a.consistentlyIntervals()...).Should(matcher, optionalDescription...)
}

// ShouldNot asserts the matcher is avoided and then kept avoided. optionalDescription
// is forwarded to both assertions, matching gomega's ShouldNot.
func (a ECAssertion) ShouldNot(matcher gomegatypes.GomegaMatcher, optionalDescription ...interface{}) {
	gomega.EventuallyWithOffset(1, a.actual, a.eventuallyIntervals()...).ShouldNot(matcher, optionalDescription...)
	gomega.ConsistentlyWithOffset(1, a.actual, a.consistentlyIntervals()...).ShouldNot(matcher, optionalDescription...)
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

func (a ECAssertion) consistentlyIntervals() []interface{} {
	out := []interface{}{flags.ConsistentlyTimeout}
	if a.polling != nil {
		out = append(out, a.polling)
	}
	return out
}
