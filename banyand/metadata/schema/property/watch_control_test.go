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
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// newTestSchemaRegistry returns a minimally-initialized SchemaRegistry
// suitable for the gate tests below. Only the logger is required because
// each test exercises an early-return path: the pause gate fires before
// any code that would touch the schemaCache or handler map.
func newTestSchemaRegistry() *SchemaRegistry {
	return &SchemaRegistry{l: logger.GetLogger("test-watch-control")}
}

// TestPauseNotifications_FlipsFlag verifies that PauseNotifications sets
// the internal paused flag (the only state the test harness inspects).
func TestPauseNotifications_FlipsFlag(t *testing.T) {
	r := newTestSchemaRegistry()
	r.PauseNotifications()
	r.pauseMu.Lock()
	defer r.pauseMu.Unlock()
	assert.True(t, r.paused, "PauseNotifications must flip paused=true")
}

// TestResumeNotifications_ClearsFlagAndDrainsQueue verifies that
// ResumeNotifications flips paused=false and drains every queued replay
// closure in arrival order.
func TestResumeNotifications_ClearsFlagAndDrainsQueue(t *testing.T) {
	r := newTestSchemaRegistry()
	r.PauseNotifications()

	var replays []int
	r.pauseMu.Lock()
	for i := 0; i < 3; i++ {
		idx := i
		r.pauseQueue = append(r.pauseQueue, func() { replays = append(replays, idx) })
	}
	r.pauseMu.Unlock()

	r.ResumeNotifications()

	r.pauseMu.Lock()
	pausedFlag := r.paused
	queueLen := len(r.pauseQueue)
	r.pauseMu.Unlock()

	assert.False(t, pausedFlag, "ResumeNotifications must flip paused=false")
	assert.Equal(t, 0, queueLen, "ResumeNotifications must drain the queue")
	assert.Equal(t, []int{0, 1, 2}, replays, "drained closures must run in arrival order")
}

// TestResumeNotifications_NotPaused_NoOp verifies that calling
// ResumeNotifications on a registry that was never paused is safe and
// leaves the (empty) queue alone.
func TestResumeNotifications_NotPaused_NoOp(t *testing.T) {
	r := newTestSchemaRegistry()
	r.ResumeNotifications()

	r.pauseMu.Lock()
	defer r.pauseMu.Unlock()
	assert.False(t, r.paused)
	assert.Equal(t, 0, len(r.pauseQueue))
}

// TestProcessInitialResourceFromProperty_PausedQueuesAndSkipsCacheUpdate
// verifies the load-bearing gate: when paused, processInitialResourceFromProperty
// returns early so the schemaCache (nil here, would panic on access) is
// untouched and the closure is queued for replay.
func TestProcessInitialResourceFromProperty_PausedQueuesAndSkipsCacheUpdate(t *testing.T) {
	r := newTestSchemaRegistry()
	r.PauseNotifications()

	prop := &propertyv1.Property{Id: "queued"}
	// nil cache would panic if the gate did not short-circuit here.
	r.processInitialResourceFromProperty(schema.KindMeasure, prop, nil)

	r.pauseMu.Lock()
	defer r.pauseMu.Unlock()
	require.Equal(t, 1, len(r.pauseQueue),
		"paused processInitialResourceFromProperty must queue a replay closure")
}

// TestHandleDeletion_PausedQueuesAndSkipsCacheDelete verifies the
// counterpart gate on handleDeletion. cache.Delete is never invoked under
// pause (the cache is nil here), so the entry remains visible to
// GetAbsentKeys — exactly the behavior  relies on.
func TestHandleDeletion_PausedQueuesAndSkipsCacheDelete(t *testing.T) {
	r := newTestSchemaRegistry()
	r.PauseNotifications()

	r.handleDeletion(schema.KindStream, "p_g/s", &cacheEntry{}, 1)

	r.pauseMu.Lock()
	defer r.pauseMu.Unlock()
	require.Equal(t, 1, len(r.pauseQueue),
		"paused handleDeletion must queue a replay closure")
}

// TestSchemaRegistryRoster_RegisterAndIndex verifies that
// CountSchemaRegistries / SchemaRegistryByIndex track NewSchemaRegistryClient
// calls in arrival order. Used by pkg/test/setup to bind a freshly-spawned
// data node's gRPC address to its registry handle.
func TestSchemaRegistryRoster_RegisterAndIndex(t *testing.T) {
	before := CountSchemaRegistries()
	r := newTestSchemaRegistry()
	registerForWatchControl(r)
	r2 := newTestSchemaRegistry()
	registerForWatchControl(r2)

	require.Equal(t, before+2, CountSchemaRegistries())
	assert.Same(t, r, SchemaRegistryByIndex(before))
	assert.Same(t, r2, SchemaRegistryByIndex(before+1))
	assert.Nil(t, SchemaRegistryByIndex(-1))
	assert.Nil(t, SchemaRegistryByIndex(CountSchemaRegistries()))
}
