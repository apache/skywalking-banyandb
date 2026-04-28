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
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/fodc/agent/internal/flightrecorder"
)

// newBlockingMockRegisterAgentClient returns a mock whose Send blocks until blockCh is
// closed, mimicking a gRPC client stream that received GOAWAY but whose underlying TCP
// write queue is no longer being drained. The entered channel is closed once the first
// time Send is called so tests can synchronize with the heartbeat goroutine reaching the
// blocking call.
func newBlockingMockRegisterAgentClient(ctx context.Context) *mockRegisterAgentClient {
	mock := newMockRegisterAgentClient(ctx)
	mock.sendBlock = make(chan struct{})
	mock.sendEntered = make(chan struct{})
	return mock
}

// TestProxyClient_SendHeartbeat_RespectsCtxOnBlockingSend reproduces the bug where
// SendHeartbeat discards its ctx parameter. With a registration stream whose Send blocks
// indefinitely, the caller's context timeout has no effect: SendHeartbeat hangs forever
// instead of returning when the deadline elapses.
//
// Pre-fix behavior: SendHeartbeat blocks far beyond the 100ms deadline; the test reaches
// its 2s ceiling and calls t.Fatal.
// Post-fix behavior: SendHeartbeat returns shortly after the 100ms deadline with an error
// that wraps context.DeadlineExceeded.
func TestProxyClient_SendHeartbeat_RespectsCtxOnBlockingSend(t *testing.T) {
	testLogger := initTestLogger(t)
	fr := flightrecorder.NewFlightRecorder(1000000)
	pc := NewProxyClient(
		"localhost:8080", "datanode-hot", "192.168.1.1", []string{"data"},
		nil, 5*time.Second, 10*time.Second, fr, nil, nil, testLogger,
	)

	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()
	stream := newBlockingMockRegisterAgentClient(streamCtx)
	defer close(stream.sendBlock)

	pc.streamsMu.Lock()
	pc.registrationStream = stream
	pc.streamsMu.Unlock()

	callCtx, callCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer callCancel()

	done := make(chan error, 1)
	go func() { done <- pc.SendHeartbeat(callCtx) }()

	select {
	case err := <-done:
		require.Error(t, err, "SendHeartbeat must return an error when its ctx times out")
		assert.Truef(t,
			errors.Is(err, context.DeadlineExceeded),
			"expected error wrapping context.DeadlineExceeded, got: %v", err,
		)
	case <-time.After(2 * time.Second):
		t.Fatal("SendHeartbeat ignored its context deadline: blocked Send hangs forever " +
			"(root cause: SendHeartbeat declares ctx as `_` and never applies it to Send)")
	}
}

// TestProxyClient_reconnect_DeadlocksWhenHeartbeatBlockedOnSend reproduces the central bug
// from the production incident: when a heartbeat goroutine is in the middle of
// registrationStream.Send and that Send is stuck (e.g. proxy entered graceful_stop and the
// stream's underlying TCP write queue is no longer being drained), the reconnect path's
// c.heartbeatWg.Wait() never returns. Combined with the fact that reconnect closes stopCh
// AFTER Wait rather than before, the heartbeat goroutine has no way out: its Send is stuck,
// ctx is alive, stopCh is still open, ticker is stopped. The result in production is the
// agent silently turning into a zombie that no longer talks to the proxy.
//
// Pre-fix behavior: pc.reconnect blocks indefinitely; the test reaches its 2s ceiling and
// calls t.Fatal (heartbeatWaitTimeout is overridden to 200ms below to keep the post-fix
// assertion fast; without the fix the call still hangs because Wait has no timeout at all).
// Post-fix behavior: pc.reconnect returns within roughly the overridden heartbeatWaitTimeout.
func TestProxyClient_reconnect_DeadlocksWhenHeartbeatBlockedOnSend(t *testing.T) {
	originalTimeout := heartbeatWaitTimeout
	heartbeatWaitTimeout = 200 * time.Millisecond
	defer func() { heartbeatWaitTimeout = originalTimeout }()

	testLogger := initTestLogger(t)
	fr := flightrecorder.NewFlightRecorder(1000000)
	pc := NewProxyClient(
		"localhost:8080", "datanode-hot", "192.168.1.1", []string{"data"},
		nil, 10*time.Millisecond, 100*time.Millisecond, fr, nil, nil, testLogger,
	)

	bgCtx, bgCancel := context.WithCancel(context.Background())
	defer bgCancel()

	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()
	stream := newBlockingMockRegisterAgentClient(streamCtx)
	defer close(stream.sendBlock)

	pc.streamsMu.Lock()
	pc.registrationStream = stream
	pc.streamsMu.Unlock()

	pc.connManager.start(bgCtx)
	defer pc.connManager.stop()

	pc.startHeartbeat(bgCtx)

	select {
	case <-stream.sendEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("heartbeat goroutine never entered Send within 2s; cannot reproduce blocking-Send scenario")
	}

	done := make(chan struct{})
	go func() {
		pc.reconnect(bgCtx)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("reconnect deadlocked: heartbeatWg.Wait() never returned while a heartbeat " +
			"goroutine was blocked inside registrationStream.Send (root causes: SendHeartbeat " +
			"ignores ctx + reconnect waits on heartbeatWg without a bounded timeout + reconnect " +
			"closes stopCh AFTER Wait instead of before)")
	}
}
