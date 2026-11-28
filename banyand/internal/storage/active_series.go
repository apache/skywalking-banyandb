// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package storage

import (
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/hll"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/meter"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// ActiveSeries counts, per shard of a group, the distinct series written in each window aligned to the Unix epoch and
// exports the count of the last complete window as the active_series gauge. Methods of a nil *ActiveSeries do nothing.
type ActiveSeries struct {
	clock    timestamp.Clock
	gauge    meter.Gauge
	shards   map[common.ShardID]*shardWindow // guarded by mu
	window   int64                           // nanoseconds
	openedAt int64                           // Unix nanoseconds
	// dirtyUntil is guarded by dirtyMu: windows that start before it are not exported. It never moves backwards.
	dirtyUntil int64
	// latest is the newest window start any shard has held. A shard created later does not start before it.
	latest   atomic.Int64
	shardNum atomic.Uint32
	mu       sync.RWMutex
	dirtyMu  sync.Mutex
	// closeOnce makes every Close wait until the first one has deleted the labels.
	closeOnce sync.Once
	closed    bool // guarded by mu
}

type shardWindow struct {
	sketch  *hll.Sketch
	start   int64
	removed bool
	// tainted marks a window that received writes while the clock read an earlier window; it is deleted instead of exported.
	tainted atomic.Bool
	mu      sync.RWMutex
}

// NewActiveSeries creates a tracker that exports the active_series gauge through factory. It returns nil when factory is nil.
func NewActiveSeries(factory observability.Factory, clock timestamp.Clock, shardNum uint32) *ActiveSeries {
	if factory == nil {
		return nil
	}
	a := &ActiveSeries{
		clock:    clock,
		gauge:    factory.NewGauge("active_series", "shard"),
		shards:   make(map[common.ShardID]*shardWindow),
		window:   int64(activeSeriesWindow()),
		openedAt: clock.Now().UnixNano(),
	}
	a.shardNum.Store(shardNum)
	return a
}

func (a *ActiveSeries) windowStart(now time.Time) int64 {
	ns := now.UnixNano()
	return ns - ns%a.window
}

// AddIDs records ids, which all fall in the current window, in the window of shardID.
func (a *ActiveSeries) AddIDs(shardID common.ShardID, ids []uint64) {
	if a == nil || len(ids) == 0 {
		return
	}
	sw := a.acquire(shardID)
	if sw == nil {
		return
	}
	for _, id := range ids {
		sw.sketch.Add(id)
	}
	sw.mu.RUnlock()
}

// AddDocs records the DocID of every document in the window of shardID.
func (a *ActiveSeries) AddDocs(shardID common.ShardID, docs index.Documents) {
	if a == nil || len(docs) == 0 {
		return
	}
	sw := a.acquire(shardID)
	if sw == nil {
		return
	}
	for i := range docs {
		sw.sketch.Add(docs[i].DocID)
	}
	sw.mu.RUnlock()
}

// acquire returns the window of shardID with its read lock held, after rotating it to the window of the current time.
// The clock is read under the read lock, so a batch never straddles a rotation. It returns nil once the tracker is closed.
func (a *ActiveSeries) acquire(shardID common.ShardID) *shardWindow {
	for {
		sw := a.shard(shardID)
		if sw == nil {
			return nil
		}
		sw.mu.RLock()
		if sw.removed {
			sw.mu.RUnlock()
			continue
		}
		start := a.observe(sw)
		if start <= sw.start {
			// A clock that moved backwards records into the current window instead of rotating back.
			return sw
		}
		sw.mu.RUnlock()
		a.rotate(shardID, sw, start)
	}
}

// observe reads the clock while the caller holds sw.mu and returns the start of the window it reads. A reading earlier
// than the window of sw means the clock moved backwards, so the window no longer describes a single period and is not exported.
func (a *ActiveSeries) observe(sw *shardWindow) int64 {
	start := a.windowStart(a.clock.Now())
	if start < sw.start {
		sw.tainted.Store(true)
	}
	return start
}

// shard returns the window of shardID and creates one for the current window when there is none.
// It returns nil once the tracker is closed.
func (a *ActiveSeries) shard(shardID common.ShardID) *shardWindow {
	a.mu.RLock()
	sw := a.shards[shardID]
	a.mu.RUnlock()
	if sw != nil {
		return sw
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return nil
	}
	if sw = a.shards[shardID]; sw == nil {
		start := max(a.windowStart(a.clock.Now()), a.latest.Load())
		sw = &shardWindow{sketch: hll.New(), start: start}
		a.shards[shardID] = sw
		a.advanceLatest(start)
	}
	return sw
}

func (a *ActiveSeries) advanceLatest(start int64) {
	for cur := a.latest.Load(); start > cur; cur = a.latest.Load() {
		if a.latest.CompareAndSwap(cur, start) {
			return
		}
	}
}

// rotate ends the window of sw and moves it to the window that starts at start, which the caller read under the read lock
// of sw. The clock is read again under the write lock and the shard moves no further than that reading, so a reading that
// has become stale cannot end a window that is not complete. The lock order is sw.mu, then a.mu or dirtyMu.
func (a *ActiveSeries) rotate(shardID common.ShardID, sw *shardWindow, start int64) {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	if sw.removed {
		return
	}
	start = min(start, a.observe(sw))
	if sw.start >= start {
		return
	}
	a.advanceLatest(start)
	label := strconv.FormatUint(uint64(shardID), 10)
	completed, estimate := sw.start, sw.sketch.Estimate()
	if estimate == 0 || completed+a.window != start {
		// The window that ended is empty or is not the one right before start: nothing is left to report for this shard.
		a.gauge.Delete(label)
		sw.removed = true
		a.mu.Lock()
		if a.shards[shardID] == sw {
			delete(a.shards, shardID)
		}
		a.mu.Unlock()
		return
	}
	a.export(label, completed, float64(estimate), sw.tainted.Load())
	sw.sketch.Reset()
	sw.tainted.Store(false)
	sw.start = start
}

// export sets the gauge of label to value when the window starting at start is complete, untainted and was not affected
// by a shard count change, and deletes the label otherwise. The check and the update hold dirtyMu, so UpdateShardNum
// cannot mask the window between them.
func (a *ActiveSeries) export(label string, start int64, value float64, tainted bool) {
	a.dirtyMu.Lock()
	defer a.dirtyMu.Unlock()
	if !tainted && start >= a.openedAt && start >= a.dirtyUntil {
		a.gauge.Set(value, label)
	} else {
		a.gauge.Delete(label)
	}
}

// Collect rotates the windows that have ended. The owner calls it from its own collector.
func (a *ActiveSeries) Collect() {
	if a == nil {
		return
	}
	a.mu.RLock()
	ids := make([]common.ShardID, 0, len(a.shards))
	sws := make([]*shardWindow, 0, len(a.shards))
	for id, sw := range a.shards {
		ids = append(ids, id)
		sws = append(sws, sw)
	}
	a.mu.RUnlock()
	for i, sw := range sws {
		sw.mu.RLock()
		start := int64(0)
		stale := false
		if !sw.removed {
			start = a.observe(sw)
			stale = start > sw.start
		}
		sw.mu.RUnlock()
		if stale {
			a.rotate(ids[i], sw, start)
		}
	}
}

// UpdateShardNum records a new shard count; the current and the next window are not exported after a change.
// The current window is the newest window any shard holds when the clock reads earlier.
func (a *ActiveSeries) UpdateShardNum(shardNum uint32) {
	if a == nil {
		return
	}
	a.dirtyMu.Lock()
	defer a.dirtyMu.Unlock()
	if a.shardNum.Swap(shardNum) == shardNum {
		return
	}
	a.dirtyUntil = max(a.dirtyUntil, max(a.windowStart(a.clock.Now()), a.latest.Load())+2*a.window)
}

// Close deletes the exported label of every shard and stops the tracker: later calls neither export nor create shards.
// It waits for the batches in flight, so none of them rotates a window after Close returns. A second call waits for the first.
func (a *ActiveSeries) Close() {
	if a == nil {
		return
	}
	a.closeOnce.Do(func() {
		a.mu.Lock()
		a.closed = true
		shards := a.shards
		a.shards = make(map[common.ShardID]*shardWindow)
		a.mu.Unlock()
		for shardID, sw := range shards {
			sw.mu.Lock()
			if !sw.removed {
				sw.removed = true
				a.gauge.Delete(strconv.FormatUint(uint64(shardID), 10))
			}
			sw.mu.Unlock()
		}
	})
}
