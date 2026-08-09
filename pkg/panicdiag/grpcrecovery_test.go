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

package panicdiag

import (
	"context"
	"sync"
	"testing"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/recovery"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/apache/skywalking-banyandb/pkg/meter"
)

type recordingReporter struct {
	results []RecoveryResult
	mu      sync.Mutex
}

func (r *recordingReporter) report() Reporter {
	return func(_ context.Context, result RecoveryResult) {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.results = append(r.results, result)
	}
}

func (r *recordingReporter) recorded() []RecoveryResult {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]RecoveryResult(nil), r.results...)
}

// setDefaultPanicCounterForTest installs a counter for the duration of one test and
// restores the previous registration, so tests never leak process-wide state.
func setDefaultPanicCounterForTest(t *testing.T, counter meter.Counter) {
	t.Helper()
	previous := defaultPanicCounterPtr.Load()
	SetDefaultPanicCounter(counter)
	t.Cleanup(func() { defaultPanicCounterPtr.Store(previous) })
}

func setDefaultReporterForTest(t *testing.T, reporter Reporter) {
	t.Helper()
	previous := defaultReporterPtr.Load()
	SetDefaultReporter(reporter)
	t.Cleanup(func() { defaultReporterPtr.Store(previous) })
}

// TestGRPCRecoveryHandlerCountsPanic is the regression guard for the gap this handler
// closes: a panic recovered by the gRPC interceptor used to be logged only, so it never
// reached banyandb_panic_total or the crash reporters.
func TestGRPCRecoveryHandlerCountsPanic(t *testing.T) {
	counter := &fakeCounter{}
	setDefaultPanicCounterForTest(t, counter)
	reporter := &recordingReporter{}
	setDefaultReporterForTest(t, reporter.report())

	handler := GRPCRecoveryHandler(nil, "grpc.test")
	err := handler(context.Background(), "boom")

	require.Error(t, err)
	require.Equal(t, codes.Internal, status.Code(err))
	require.Contains(t, err.Error(), "boom")

	require.Equal(t, 1, counter.calls, "the recovered panic must reach the panic counter")
	require.Equal(t, []string{"grpc.test"}, counter.lastLabels, "the component label must identify the server")
	recorded := reporter.recorded()
	require.Len(t, recorded, 1, "the recovered panic must reach the crash reporters")
	require.Equal(t, "boom", recorded[0].Record.PanicValue)
}

// TestGRPCRecoveryHandlerKeepsPanicSiteStack asserts the recorded stack describes where
// the panic happened rather than the recovery machinery: the handler is invoked from the
// interceptor's recovering defer, so the panicking frame is still on the stack.
func TestGRPCRecoveryHandlerKeepsPanicSiteStack(t *testing.T) {
	counter := &fakeCounter{}
	setDefaultPanicCounterForTest(t, counter)
	reporter := &recordingReporter{}
	setDefaultReporterForTest(t, reporter.report())

	handler := GRPCRecoveryHandler(nil, "grpc.test")
	err := callThroughRecoveringDefer(handler)

	require.Error(t, err)
	recorded := reporter.recorded()
	require.Len(t, recorded, 1)
	require.Contains(t, recorded[0].Record.GoroutineStack, "panicInNestedFrame",
		"the artifact stack must still contain the panicking frame")
}

// callThroughRecoveringDefer mimics the grpc-middleware recovery interceptor: recover in
// a defer and hand the value to the handler while the panicking frames are still live.
func callThroughRecoveringDefer(handler func(context.Context, any) error) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = handler(context.Background(), p)
		}
	}()
	panicInNestedFrame()
	return nil
}

func panicInNestedFrame() {
	panic("nested boom")
}

// TestRecoverExternalSuppressesHookPanic asserts a broken hook cannot turn panicdiag's
// own reporting into a second panic escaping into the caller's recovery path.
func TestRecoverExternalSuppressesHookPanic(t *testing.T) {
	setDefaultReporterForTest(t, func(_ context.Context, _ RecoveryResult) { panic("reporter is broken") })
	require.NotPanics(t, func() {
		RecoverExternal(context.Background(), RecoveryOptions{Component: "grpc.test"}, nil, "boom", nil)
	})
}

// TestRecoverExternalIgnoresNilPanic guards the no-panic path: handlers must be able to
// call it unconditionally without recording phantom panics.
func TestRecoverExternalIgnoresNilPanic(t *testing.T) {
	counter := &fakeCounter{}
	setDefaultPanicCounterForTest(t, counter)
	result := RecoverExternal(context.Background(), RecoveryOptions{Component: "grpc.test"}, nil, nil, nil)
	require.Nil(t, result.Record)
	require.Zero(t, counter.calls)
}

// TestGRPCRecoveryHandlerSatisfiesInterceptorType pins the deliberately untyped return
// to the middleware's expectation. The package avoids importing the middleware so the
// dependency stays at the wiring sites; this assignment keeps that decision honest by
// failing here, once, rather than at four servers if the signature ever drifts.
func TestGRPCRecoveryHandlerSatisfiesInterceptorType(t *testing.T) {
	var handler recovery.RecoveryHandlerFuncContext = GRPCRecoveryHandler(nil, "grpc.test")
	require.NotNil(t, handler)
	_ = recovery.WithRecoveryHandlerContext(handler)
}
