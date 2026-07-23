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

package grpc

import (
	"math"
	"sort"
	"sync"
	"time"
)

// bydbqlTopKSize is the number of hot entries each top-K tracker keeps. It is sized
// well above the expected number of distinct parameterized query templates so the
// Space-Saving eviction (and its count over-estimate) effectively never triggers for
// a normal workload, keeping the reported counts exact while still bounding memory.
const bydbqlTopKSize = 128

// topKSlot is one tracked query and its accumulated statistics. lastSeen drives TTL
// expiry; maxDurAt pins when the peak latency happened, so a consumer can tell a live
// problem from a peak the tracker has merely been carrying since a startup incident.
type topKSlot struct {
	lastSeen time.Time
	maxDurAt time.Time
	key      string
	count    uint64
	maxDur   time.Duration
}

// topK is a bounded approximate heavy-hitters tracker (Space-Saving): it keeps at
// most k entries and, when full, evicts the least-frequent one, letting the new key
// inherit that entry's count + 1 so a fresh key gets a fair chance instead of being
// evicted again immediately. With k modest (128) the min scan and the snapshot sort are
// cheap, and observing an existing key needs no reordering at all.
//
// Entries also expire, by inactivity: a key not observed for ttl is dropped, so a query
// that stops happening stops being reported. A key that does keep being observed never
// expires and goes on accumulating, so expiry bounds how long a finished problem lingers,
// not how far back an ongoing one is counted. A ttl <= 0 keeps entries for the process
// lifetime.
type topK struct {
	slots map[string]*topKSlot
	now   func() time.Time
	k     int
	ttl   time.Duration
	mu    sync.Mutex
}

// newTopK builds a tracker holding at most k entries, expiring any entry not observed
// for ttl. now supplies the clock; passing nil uses time.Now.
func newTopK(k int, ttl time.Duration, now func() time.Time) *topK {
	if k < 1 {
		k = 1
	}
	if now == nil {
		now = time.Now
	}
	return &topK{slots: make(map[string]*topKSlot, k), k: k, ttl: ttl, now: now}
}

// observe records one occurrence of key; dur is the query latency (0 when latency is
// not tracked, e.g. the cache-miss queue). maxDur keeps the largest latency seen.
func (t *topK) observe(key string, dur time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.now()
	if s, ok := t.slots[key]; ok {
		s.count++
		s.lastSeen = now
		if dur > s.maxDur {
			s.maxDur, s.maxDurAt = dur, now
		}
		return
	}
	if len(t.slots) >= t.k {
		// Reclaim expired entries before falling back to evicting a live one: a key
		// nobody has asked about in ttl is a better victim than a merely infrequent one.
		t.purgeExpiredLocked(now)
	}
	if len(t.slots) < t.k {
		t.slots[key] = &topKSlot{key: key, count: 1, maxDur: dur, lastSeen: now, maxDurAt: now}
		return
	}
	// Full: evict the least-frequent entry and let the new key inherit its count.
	minKey := ""
	minCount := uint64(math.MaxUint64)
	for k, s := range t.slots {
		if s.count < minCount {
			minCount, minKey = s.count, k
		}
	}
	delete(t.slots, minKey)
	t.slots[key] = &topKSlot{key: key, count: minCount + 1, maxDur: dur, lastSeen: now, maxDurAt: now}
}

// purgeExpiredLocked drops every entry not observed within ttl. Call with the lock held.
func (t *topK) purgeExpiredLocked(now time.Time) {
	if t.ttl <= 0 {
		return
	}
	for k, s := range t.slots {
		if now.Sub(s.lastSeen) > t.ttl {
			delete(t.slots, k)
		}
	}
}

// snapshot returns the tracked entries ranked by frequency: (count desc, maxDur desc,
// key asc). The TTL expires an entry by inactivity; it does not window the statistics. For
// as long as a key keeps being observed its count and maxDur stay cumulative, so a
// still-active entry can report a peak from hours ago — which is what maxDurAt and lastSeen
// exist to date. The full tie-break makes the order deterministic across dumps.
func (t *topK) snapshot() []topKSlot {
	out := t.copyOut()
	sort.Slice(out, func(i, j int) bool { return lessByCount(out[i], out[j]) })
	return out
}

// snapshotByLatency returns the tracked entries ranked by peak latency: (maxDur desc,
// count desc, key asc), so a rarely-but-catastrophically slow query is not buried under
// frequently-mildly-slow ones.
func (t *topK) snapshotByLatency() []topKSlot {
	out := t.copyOut()
	sort.Slice(out, func(i, j int) bool { return lessByLatency(out[i], out[j]) })
	return out
}

// copyOut snapshots the live entries, expiring stale ones first. Purging here rather
// than only in observe is what lets a tracker that has gone quiet drain: an entry no
// one observes again would otherwise never be revisited, and would be reported forever.
func (t *topK) copyOut() []topKSlot {
	t.mu.Lock()
	t.purgeExpiredLocked(t.now())
	out := make([]topKSlot, 0, len(t.slots))
	for _, s := range t.slots {
		out = append(out, *s)
	}
	t.mu.Unlock()
	return out
}

func lessByCount(a, b topKSlot) bool {
	if a.count != b.count {
		return a.count > b.count
	}
	if a.maxDur != b.maxDur {
		return a.maxDur > b.maxDur
	}
	return a.key < b.key
}

func lessByLatency(a, b topKSlot) bool {
	if a.maxDur != b.maxDur {
		return a.maxDur > b.maxDur
	}
	if a.count != b.count {
		return a.count > b.count
	}
	return a.key < b.key
}
