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

package property

import (
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/initerror"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// retryTestHandler implements schema.EventHandler for retry-classification tests.
// Each OnInit call invokes the configured panicFn (which may panic). When
// noPanicAfter calls have been made, OnInit returns normally so the retry loop
// can succeed.
type retryTestHandler struct {
	panicFn       func(callIdx int)
	startedSignal chan struct{}
	schema.UnimplementedOnInitHandler
	calls        atomic.Int32
	noPanicAfter int32
}

func (h *retryTestHandler) OnAddOrUpdate(_ schema.Metadata) {}
func (h *retryTestHandler) OnDelete(_ schema.Metadata)      {}

func (h *retryTestHandler) OnInit(_ []schema.Kind) (bool, []int64) {
	idx := h.calls.Add(1)
	if h.startedSignal != nil {
		select {
		case h.startedSignal <- struct{}{}:
		default:
		}
	}
	if h.noPanicAfter > 0 && idx > h.noPanicAfter {
		return true, nil
	}
	if h.panicFn != nil {
		h.panicFn(int(idx))
	}
	return true, nil
}

// newTestRegistryWithClock builds a SchemaRegistry instance that bypasses
// connection setup so we can drive its retry classifier in-process. It uses a
// mock clock so the test controls the deadline and Sleep windows.
func newTestRegistryWithClock(_ *testing.T, mc timestamp.MockClock) *SchemaRegistry {
	l := logger.GetLogger("init-handler-retry-test")
	return &SchemaRegistry{
		l:     l,
		clock: mc,
	}
}

// TestInitHandlerWithRetry_PermanentVersionError_FailsFast asserts that a
// handler panicking with a Permanent-wrapped error causes the retry loop to
// exit on the first attempt via r.l.Panic, without ever advancing the clock.
func TestInitHandlerWithRetry_PermanentVersionError_FailsFast(t *testing.T) {
	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: "warn"}))
	mc := timestamp.NewMockClock()
	mcStart := mc.Now()
	r := newTestRegistryWithClock(t, mc)
	permErr := initerror.AsPermanent(errors.New("incompatible version 1.3.0"))
	handler := &retryTestHandler{
		panicFn: func(_ int) {
			panic(fmt.Errorf("OpenDB failed: %w", permErr))
		},
	}

	done := make(chan struct{})
	var recovered interface{}
	go func() {
		defer close(done)
		defer func() { recovered = recover() }()
		r.initHandlerWithRetry("perm-handler", handler, []schema.Kind{schema.KindGroup})
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("initHandlerWithRetry did not return within 2s; permanent error path should fail fast")
	}

	require.NotNil(t, recovered, "permanent error must propagate as a panic")
	recErr, isErr := recovered.(error)
	require.True(t, isErr, "panic value should be an error wrapping the permanent error")
	assert.True(t, initerror.IsPermanent(recErr), "recovered error must satisfy IsPermanent")
	assert.Equal(t, int32(1), handler.calls.Load(), "permanent error must not be retried")
	assert.Equal(t, mcStart, mc.Now(), "mock clock must not advance when the first attempt is permanent")
}

// TestInitHandlerWithRetry_TransientStillRetries asserts that a handler
// panicking with a non-permanent error keeps retrying until either the deadline
// passes (panic) or it stops panicking (success). Here we let the handler stop
// panicking after two attempts to exercise the success path.
func TestInitHandlerWithRetry_TransientStillRetries(t *testing.T) {
	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: "warn"}))
	mc := timestamp.NewMockClock()
	r := newTestRegistryWithClock(t, mc)
	handler := &retryTestHandler{
		noPanicAfter:  2,
		startedSignal: make(chan struct{}, 4),
		panicFn: func(_ int) {
			panic("etcd unreachable")
		},
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		r.initHandlerWithRetry("transient-handler", handler, []schema.Kind{schema.KindGroup})
	}()

	// Wait for the handler's first OnInit call to have run.
	for i := 0; i < 2; i++ {
		select {
		case <-handler.startedSignal:
		case <-time.After(2 * time.Second):
			t.Fatalf("handler.OnInit was not invoked for attempt %d within 2s", i+1)
		}
		// Advance the mock clock so the worker's Sleep returns and it can
		// re-enter the retry loop. The benbjohnson MockClock fires sleepers
		// when Add brings the time past their deadline, even if the sleeper
		// registered after the Add — but we still poll briefly to avoid races.
		require.Eventually(t, func() bool {
			mc.Add(time.Second)
			return handler.calls.Load() > int32(i+1) || done != nil && isClosed(done)
		}, 3*time.Second, 10*time.Millisecond, "Sleep must release after clock advance")
	}

	// Wait for the loop to finish (third call returns ok=true).
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("initHandlerWithRetry did not return after handler stopped panicking")
	}
	assert.GreaterOrEqual(t, handler.calls.Load(), int32(3), "handler must be retried at least twice before succeeding")
}

// TestInitHandlerWithRetry_DeadlineExceededPanics asserts that a non-permanent
// failure that never stops eventually triggers r.l.Panic when the deadline is
// crossed.
func TestInitHandlerWithRetry_DeadlineExceededPanics(t *testing.T) {
	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: "warn"}))
	mc := timestamp.NewMockClock()
	r := newTestRegistryWithClock(t, mc)
	handler := &retryTestHandler{
		startedSignal: make(chan struct{}, 8),
		panicFn: func(_ int) {
			panic("never resolves")
		},
	}

	done := make(chan struct{})
	var recovered interface{}
	go func() {
		defer close(done)
		defer func() { recovered = recover() }()
		r.initHandlerWithRetry("deadline-handler", handler, []schema.Kind{schema.KindGroup})
	}()

	deadline := time.Now().Add(5 * time.Second)
	for {
		select {
		case <-done:
			require.NotNil(t, recovered, "deadline exceeded must propagate as a panic")
			return
		case <-handler.startedSignal:
		case <-time.After(50 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			t.Fatal("initHandlerWithRetry did not exit after repeated clock advances")
		}
		mc.Add(2 * time.Minute)
	}
}

func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}
