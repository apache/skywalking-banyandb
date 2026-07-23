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

package sdk

import "fmt"

// Bypass reasons reported via BypassInfo.Reason. A link is bypassed
// (pass-through: the running keep-mask is left unchanged for that link) for
// exactly one of these three causes.
const (
	// BypassReasonDecideError means Sampler.Decide returned a non-nil error.
	BypassReasonDecideError = "decide_error"
	// BypassReasonLengthMismatch means Verdict.Keep's length did not match
	// len(batch.Traces).
	BypassReasonLengthMismatch = "length_mismatch"
	// BypassReasonPanic means Sampler.Decide panicked; the panic was recovered.
	// Distinguished from BypassReasonDecideError so callers can tell "the
	// plugin returned an error" apart from "the plugin crashed" even though
	// both fail open identically.
	BypassReasonPanic = "panic"
)

// BypassInfo describes why EvaluateChain bypassed one chain link.
type BypassInfo struct {
	// Err is the Decide error (BypassReasonDecideError) or the recovered
	// panic wrapped as an error (BypassReasonPanic). Nil for
	// BypassReasonLengthMismatch.
	Err error
	// Reason classifies why the link was bypassed: one of
	// BypassReasonDecideError, BypassReasonLengthMismatch, or
	// BypassReasonPanic.
	Reason string
	// Got is len(Verdict.Keep) as returned by the link (BypassReasonLengthMismatch only).
	Got int
	// Want is len(batch.Traces) (BypassReasonLengthMismatch only).
	Want int
}

// EvaluateChain runs an ordered sampler chain over batch and returns the
// conjunction (AND) keep-mask. It is the single shared implementation of
// chain-evaluation semantics — the host engine's merge chain
// (banyand/trace's mergeChain.runChain) and the offline
// pkg/pipeline/sdk/sdktest.RunChain harness both call this function, so how a
// chain of samplers combines into one verdict is never duplicated or allowed
// to drift between the two.
//
// Every sampler runs, in declared order, over the SAME batch — a chain link
// narrows the running mask, it does not filter rows out of what the next
// link sees. A link that panics, errors, or returns a keep-mask of the wrong
// length is bypassed: the running mask is left unchanged for that link
// (fail-open, per link), and onBypass — when non-nil — is invoked with the
// link's index and a BypassInfo describing why.
//
// onBypass is the sole observability seam: it lets a caller log and/or count
// a bypass without EvaluateChain importing a logger or meter (this package
// must stay dependency-pure — see importgraph_test.go). The host passes an
// onBypass that reproduces its pre-existing WARN log (and, going forward,
// increments a metric); sdktest.RunChain passes a recorder that captures
// every BypassInfo into its Report for offline inspection.
func EvaluateChain(samplers []Sampler, batch *TraceBatch, onBypass func(idx int, info BypassInfo)) Verdict {
	mask := make([]bool, len(batch.Traces))
	for i := range mask {
		mask[i] = true
	}
	for idx, sampler := range samplers {
		if sampler == nil {
			continue
		}
		applyChainLink(idx, sampler, batch, mask, onBypass)
	}
	return Verdict{Keep: mask}
}

// applyChainLink calls sampler.Decide under recover and ANDs its keep mask
// into mask. On panic, error, or length mismatch the link is bypassed (mask
// unchanged) and onBypass, if non-nil, is invoked with the classified reason.
func applyChainLink(idx int, sampler Sampler, batch *TraceBatch, mask []bool, onBypass func(idx int, info BypassInfo)) {
	var (
		verdict   Verdict
		decErr    error
		recovered bool
	)
	func() {
		defer func() {
			if r := recover(); r != nil {
				recovered = true
				decErr = fmt.Errorf("plugin panic: %v", r)
			}
		}()
		verdict, decErr = sampler.Decide(batch)
	}()
	if decErr != nil {
		if onBypass != nil {
			reason := BypassReasonDecideError
			if recovered {
				reason = BypassReasonPanic
			}
			onBypass(idx, BypassInfo{Reason: reason, Err: decErr})
		}
		return
	}
	if len(verdict.Keep) != len(batch.Traces) {
		if onBypass != nil {
			onBypass(idx, BypassInfo{Reason: BypassReasonLengthMismatch, Got: len(verdict.Keep), Want: len(batch.Traces)})
		}
		return
	}
	for i := range mask {
		mask[i] = mask[i] && verdict.Keep[i]
	}
}
