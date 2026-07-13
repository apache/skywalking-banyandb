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

// topKSlot is one tracked query and its accumulated statistics.
type topKSlot struct {
	key    string
	count  uint64
	maxDur time.Duration
}

// topK is a bounded approximate heavy-hitters tracker (Space-Saving): it keeps at
// most k entries and, when full, evicts the least-frequent one, letting the new key
// inherit that entry's count + 1 so a fresh key gets a fair chance instead of being
// evicted again immediately. With k modest (128) the min scan and the snapshot sort are
// cheap, and observing an existing key needs no reordering at all.
type topK struct {
	slots map[string]*topKSlot
	k     int
	mu    sync.Mutex
}

func newTopK(k int) *topK {
	if k < 1 {
		k = 1
	}
	return &topK{slots: make(map[string]*topKSlot, k), k: k}
}

// observe records one occurrence of key; dur is the query latency (0 when latency is
// not tracked, e.g. the cache-miss queue). maxDur keeps the largest latency seen.
func (t *topK) observe(key string, dur time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if s, ok := t.slots[key]; ok {
		s.count++
		if dur > s.maxDur {
			s.maxDur = dur
		}
		return
	}
	if len(t.slots) < t.k {
		t.slots[key] = &topKSlot{key: key, count: 1, maxDur: dur}
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
	t.slots[key] = &topKSlot{key: key, count: minCount + 1, maxDur: dur}
}

// snapshot returns the tracked entries ranked by frequency: (count desc, maxDur desc,
// key asc). The tracker is cumulative, so each dump reflects the hottest queries since
// process start. The full tie-break makes the order deterministic across dumps.
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

func (t *topK) copyOut() []topKSlot {
	t.mu.Lock()
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
