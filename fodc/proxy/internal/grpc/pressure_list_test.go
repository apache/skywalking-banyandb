// Licensed to Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package grpc

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func newListWaiter(agentIDs ...string) *listWaiter {
	w := &listWaiter{pending: make(map[string]struct{}), done: make(chan struct{})}
	for _, id := range agentIDs {
		w.pending[id] = struct{}{}
	}
	return w
}

func assertClosed(t *testing.T, ch <-chan struct{}, within time.Duration) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(within):
		t.Fatal("done channel did not close in time")
	}
}

func assertOpen(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
		t.Fatal("done channel closed unexpectedly")
	default:
	}
}

// TestListWaiterAllAckedClosesDone verifies done closes only once every pending agent acks.
func TestListWaiterAllAckedClosesDone(t *testing.T) {
	w := newListWaiter("a", "b")
	w.ack("a")
	assertOpen(t, w.done)
	w.ack("b")
	assertClosed(t, w.done, time.Second)
}

// TestListWaiterDoubleAck verifies acking the same agent twice is idempotent (no panic, no
// premature close) and an unknown agent ack is ignored.
func TestListWaiterDoubleAck(t *testing.T) {
	w := newListWaiter("a", "b")
	w.ack("a")
	w.ack("a") // duplicate
	w.ack("unknown")
	assertOpen(t, w.done)
	w.ack("b")
	assertClosed(t, w.done, time.Second)
	w.ack("a") // ack after close must not panic
}

// TestListWaiterForceDone verifies the TTL backstop closes done regardless of pending, and a
// later ack does not panic on the already-closed channel.
func TestListWaiterForceDone(t *testing.T) {
	w := newListWaiter("a", "b")
	w.forceDone()
	assertClosed(t, w.done, time.Second)
	w.forceDone() // idempotent
	w.ack("a")    // must not panic
}

// TestCollectListZeroAgents returns an already-closed channel.
func TestCollectListZeroAgents(t *testing.T) {
	service, registry := newTestService(t)
	defer registry.Stop()
	assertClosed(t, service.CollectList(nil), 50*time.Millisecond)
}

// TestCollectListDisconnectedAgent acks an agent that has no connection so the wait completes
// immediately instead of stalling.
func TestCollectListDisconnectedAgent(t *testing.T) {
	service, registry := newTestService(t)
	defer registry.Stop()
	assertClosed(t, service.CollectList([]string{"ghost"}), time.Second)
}

// TestCollectListSendFailureAcks acks an agent whose pressure stream is not established (send
// fails) so the wait completes without a stall.
func TestCollectListSendFailureAcks(t *testing.T) {
	service, registry := newTestService(t)
	defer registry.Stop()
	service.connectionsMu.Lock()
	service.connections["agent-x"] = &agentConnection{agentID: "agent-x"} // nil pressureProfilesStream
	service.connectionsMu.Unlock()
	assertClosed(t, service.CollectList([]string{"agent-x"}), time.Second)
}

// TestCleanupConnectionAcksListWaiters pins the fix for the disconnect stall: cleaning up a
// connection acks the departing agent in every in-flight list waiter.
func TestCleanupConnectionAcksListWaiters(t *testing.T) {
	service, registry := newTestService(t)
	defer registry.Stop()
	w := newListWaiter("agent-x", "agent-y")
	service.listMu.Lock()
	service.listWaiters["req-1"] = w
	service.listMu.Unlock()

	service.cleanupConnection("agent-x")
	assertOpen(t, w.done) // agent-y still pending
	service.cleanupConnection("agent-y")
	assertClosed(t, w.done, time.Second)
}

// TestAckListUnknownRequestIsNoop verifies a completion for an unknown request id is dropped.
func TestAckListUnknownRequestIsNoop(t *testing.T) {
	service, registry := newTestService(t)
	defer registry.Stop()
	assert.NotPanics(t, func() { service.ackList("does-not-exist", "agent-x") })
}
