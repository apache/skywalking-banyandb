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
	"sync"

	schemav1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/schema/v1"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

const watcherChannelSize = 512

type watcherManager struct {
	l        *logger.Logger
	watchers map[uint64]chan *schemav1.WatchSchemasResponse
	mu       sync.RWMutex
	nextID   uint64
}

func newWatcherManager(l *logger.Logger) *watcherManager {
	return &watcherManager{
		l:        l,
		watchers: make(map[uint64]chan *schemav1.WatchSchemasResponse),
	}
}

// Subscribe registers a new watcher and returns its ID and event channel.
func (wm *watcherManager) Subscribe() (uint64, <-chan *schemav1.WatchSchemasResponse) {
	wm.mu.Lock()
	defer wm.mu.Unlock()
	wm.nextID++
	id := wm.nextID
	ch := make(chan *schemav1.WatchSchemasResponse, watcherChannelSize)
	wm.watchers[id] = ch
	wm.l.Debug().Uint64("watcherID", id).Int("totalWatchers", len(wm.watchers)).Msg("watcher subscribed")
	return id, ch
}

// Unsubscribe removes a watcher and closes its channel.
func (wm *watcherManager) Unsubscribe(id uint64) {
	wm.mu.Lock()
	defer wm.mu.Unlock()
	if ch, ok := wm.watchers[id]; ok {
		delete(wm.watchers, id)
		close(ch)
		wm.l.Debug().Uint64("watcherID", id).Int("totalWatchers", len(wm.watchers)).Msg("watcher unsubscribed")
	}
}

// Broadcast sends an event to all watchers without blocking.
func (wm *watcherManager) Broadcast(resp *schemav1.WatchSchemasResponse) {
	wm.mu.RLock()
	defer wm.mu.RUnlock()
	wm.l.Debug().Stringer("eventType", resp.GetEventType()).
		Str("propertyID", resp.GetProperty().GetId()).
		Int("watcherCount", len(wm.watchers)).
		Msg("broadcasting schema event")
	for id, ch := range wm.watchers {
		select {
		case ch <- resp:
		default:
			wm.l.Warn().Uint64("watcherID", id).
				Stringer("eventType", resp.GetEventType()).
				Str("propertyID", resp.GetProperty().GetId()).
				Msg("watcher channel full, dropping event")
		}
	}
}

// Count returns the number of active watchers.
func (wm *watcherManager) Count() int {
	wm.mu.RLock()
	defer wm.mu.RUnlock()
	return len(wm.watchers)
}

// Close removes all watchers and closes their channels.
func (wm *watcherManager) Close() {
	wm.mu.Lock()
	defer wm.mu.Unlock()
	for id, ch := range wm.watchers {
		delete(wm.watchers, id)
		close(ch)
	}
}
