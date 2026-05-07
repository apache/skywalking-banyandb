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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	schemav1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/schema/v1"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// newTestSchemaRegistry returns a minimally-initialized SchemaRegistry
// suitable for the gate tests below — only the logger is required so the
// post-resume dispatch tail (ParseTags / KindFromString warnings) does
// not panic on nil logger access. The connection manager / cache /
// handler map stay zero-valued because these tests only exercise the
// pause / queue / drain paths, which short-circuit before any of those
// fields are touched.
func newTestSchemaRegistry() *SchemaRegistry {
	return &SchemaRegistry{l: logger.GetLogger("test-watch-control")}
}

// fakeWatchEvent returns a minimally non-nil WatchSchemasResponse so the
// gate path runs to completion. The event payload is intentionally bare —
// these tests exercise the pause / queue / drain mechanism, not the
// downstream parse / dispatch path.
func fakeWatchEvent(id string) *schemav1.WatchSchemasResponse {
	return &schemav1.WatchSchemasResponse{
		Property: &propertyv1.Property{Id: id},
	}
}

// TestPauseNotifications_QueuesEventsWhilePaused verifies that
// handleWatchEvent appends to pauseQueue (and does not invoke the parse /
// dispatch tail) while paused=true.
func TestPauseNotifications_QueuesEventsWhilePaused(t *testing.T) {
	r := newTestSchemaRegistry()
	r.PauseNotifications()

	r.handleWatchEvent(fakeWatchEvent("a"))
	r.handleWatchEvent(fakeWatchEvent("b"))
	r.handleWatchEvent(fakeWatchEvent("c"))

	r.pauseMu.Lock()
	queueLen := len(r.pauseQueue)
	pausedFlag := r.paused
	r.pauseMu.Unlock()

	assert.True(t, pausedFlag, "PauseNotifications must flip paused=true")
	require.Equal(t, 3, queueLen, "all three events must be queued while paused")
	assert.Equal(t, "a", r.pauseQueue[0].GetProperty().GetId())
	assert.Equal(t, "c", r.pauseQueue[2].GetProperty().GetId())
}

// TestResumeNotifications_DrainsQueueAndClearsPauseState verifies that
// ResumeNotifications flips paused=false, replays the queued events
// (handleWatchEvent runs to completion for each), and leaves an empty
// queue. Replay correctness for a malformed payload is out of scope —
// handleWatchEvent's existing nil/parse guards already cover that.
func TestResumeNotifications_DrainsQueueAndClearsPauseState(t *testing.T) {
	r := newTestSchemaRegistry()
	r.PauseNotifications()
	r.handleWatchEvent(fakeWatchEvent("a"))
	r.handleWatchEvent(fakeWatchEvent("b"))

	r.ResumeNotifications()

	r.pauseMu.Lock()
	queueLen := len(r.pauseQueue)
	pausedFlag := r.paused
	r.pauseMu.Unlock()

	assert.False(t, pausedFlag, "ResumeNotifications must flip paused=false")
	assert.Equal(t, 0, queueLen, "ResumeNotifications must drain the queue")
}

// TestResumeNotifications_NotPaused_NoOp verifies that calling
// ResumeNotifications on a registry that was never paused is safe and
// leaves the (empty) queue alone.
func TestResumeNotifications_NotPaused_NoOp(t *testing.T) {
	r := newTestSchemaRegistry()
	r.ResumeNotifications()

	r.pauseMu.Lock()
	queueLen := len(r.pauseQueue)
	pausedFlag := r.paused
	r.pauseMu.Unlock()

	assert.False(t, pausedFlag)
	assert.Equal(t, 0, queueLen)
}

// TestHandleWatchEvent_AfterResume_RunsImmediately verifies that events
// arriving after ResumeNotifications skip the queue path and run through
// the dispatch tail synchronously, matching pre-pause behavior.
func TestHandleWatchEvent_AfterResume_RunsImmediately(t *testing.T) {
	r := newTestSchemaRegistry()
	r.PauseNotifications()
	r.ResumeNotifications()

	r.handleWatchEvent(fakeWatchEvent("after-resume"))

	r.pauseMu.Lock()
	queueLen := len(r.pauseQueue)
	r.pauseMu.Unlock()

	assert.Equal(t, 0, queueLen, "post-resume events must not be queued")
}

// TestPauseNotifications_NilPropertyShortCircuits verifies that the
// existing prop==nil guard short-circuits before the gate, so a nil
// payload does not pollute the queue.
func TestPauseNotifications_NilPropertyShortCircuits(t *testing.T) {
	r := newTestSchemaRegistry()
	r.PauseNotifications()

	r.handleWatchEvent(&schemav1.WatchSchemasResponse{Property: nil})

	r.pauseMu.Lock()
	queueLen := len(r.pauseQueue)
	r.pauseMu.Unlock()

	assert.Equal(t, 0, queueLen, "nil-property events must not enter the pause queue")
}

// TestSchemaRegistryRoster_RegisterAndIndex verifies that
// CountSchemaRegistries / SchemaRegistryByIndex track NewSchemaRegistryClient
// calls in arrival order. Used by pkg/test/setup to bind a freshly-spawned
// data node's gRPC address to its registry handle.
func TestSchemaRegistryRoster_RegisterAndIndex(t *testing.T) {
	before := CountSchemaRegistries()
	r := newTestSchemaRegistry()
	registerForWatchControl(r)
	r2 := &SchemaRegistry{}
	registerForWatchControl(r2)

	require.Equal(t, before+2, CountSchemaRegistries())
	assert.Same(t, r, SchemaRegistryByIndex(before))
	assert.Same(t, r2, SchemaRegistryByIndex(before+1))
	assert.Nil(t, SchemaRegistryByIndex(-1))
	assert.Nil(t, SchemaRegistryByIndex(CountSchemaRegistries()))
}
