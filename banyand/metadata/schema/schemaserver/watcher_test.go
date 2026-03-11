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

package schemaserver

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	schemav1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/schema/v1"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

func TestWatcherManager_SubscribeUnsubscribe(t *testing.T) {
	wm := newWatcherManager(logger.GetLogger("test-watcher"))
	defer wm.Close()
	id1, ch1 := wm.Subscribe()
	id2, ch2 := wm.Subscribe()
	assert.NotEqual(t, id1, id2)
	assert.NotNil(t, ch1)
	assert.NotNil(t, ch2)
	wm.Unsubscribe(id1)
	// ch1 should be closed
	_, ok := <-ch1
	assert.False(t, ok)
	// ch2 should still be open
	wm.Broadcast(&schemav1.WatchSchemasResponse{EventType: schemav1.SchemaEventType_SCHEMA_EVENT_TYPE_INSERT})
	select {
	case resp := <-ch2:
		assert.Equal(t, schemav1.SchemaEventType_SCHEMA_EVENT_TYPE_INSERT, resp.GetEventType())
	case <-time.After(time.Second):
		t.Fatal("expected event on ch2")
	}
}

func TestWatcherManager_Broadcast(t *testing.T) {
	wm := newWatcherManager(logger.GetLogger("test-watcher"))
	defer wm.Close()
	const numWatchers = 5
	channels := make([]<-chan *schemav1.WatchSchemasResponse, numWatchers)
	for i := range numWatchers {
		_, channels[i] = wm.Subscribe()
	}
	resp := &schemav1.WatchSchemasResponse{EventType: schemav1.SchemaEventType_SCHEMA_EVENT_TYPE_UPDATE}
	wm.Broadcast(resp)
	for i, ch := range channels {
		select {
		case got := <-ch:
			assert.Equal(t, schemav1.SchemaEventType_SCHEMA_EVENT_TYPE_UPDATE, got.GetEventType(), "watcher %d", i)
		case <-time.After(time.Second):
			t.Fatalf("watcher %d did not receive event", i)
		}
	}
}

func TestWatcherManager_BroadcastNonBlocking(t *testing.T) {
	wm := newWatcherManager(logger.GetLogger("test-watcher"))
	defer wm.Close()
	_, ch := wm.Subscribe()
	// Fill the channel
	for range watcherChannelSize {
		wm.Broadcast(&schemav1.WatchSchemasResponse{EventType: schemav1.SchemaEventType_SCHEMA_EVENT_TYPE_INSERT})
	}
	// This broadcast should not block even though the channel is full
	done := make(chan struct{})
	go func() {
		wm.Broadcast(&schemav1.WatchSchemasResponse{EventType: schemav1.SchemaEventType_SCHEMA_EVENT_TYPE_DELETE})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Broadcast blocked on full channel")
	}
	// Drain and verify we got watcherChannelSize events
	count := 0
	for range watcherChannelSize {
		select {
		case <-ch:
			count++
		default:
		}
	}
	assert.Equal(t, watcherChannelSize, count)
}

func TestWatcherManager_Close(t *testing.T) {
	wm := newWatcherManager(logger.GetLogger("test-watcher"))
	_, ch1 := wm.Subscribe()
	_, ch2 := wm.Subscribe()
	wm.Close()
	_, ok1 := <-ch1
	require.False(t, ok1, "ch1 should be closed")
	_, ok2 := <-ch2
	require.False(t, ok2, "ch2 should be closed")
	// Broadcast after close should not panic
	wm.Broadcast(&schemav1.WatchSchemasResponse{})
}
