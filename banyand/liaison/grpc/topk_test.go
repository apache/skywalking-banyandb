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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

func topKByKey(slots []topKSlot) map[string]topKSlot {
	m := make(map[string]topKSlot, len(slots))
	for _, s := range slots {
		m[s.key] = s
	}
	return m
}

func TestTopKCountsAndMaxDur(t *testing.T) {
	tk := newTopK(4, 0, nil)
	tk.observe("a", 5*time.Millisecond)
	tk.observe("a", 8*time.Millisecond) // largest, observed in the middle
	tk.observe("a", 3*time.Millisecond) // smaller and last: maxDur must stay 8ms, not 3ms
	tk.observe("b", time.Millisecond)

	byKey := topKByKey(tk.snapshot())
	assert.Equal(t, uint64(3), byKey["a"].count)
	assert.Equal(t, 8*time.Millisecond, byKey["a"].maxDur, "maxDur keeps the largest, not the last")
	assert.Equal(t, uint64(1), byKey["b"].count)
}

func TestTopKEvictsMinAndInheritsCount(t *testing.T) {
	tk := newTopK(2, 0, nil)
	tk.observe("a", 0)
	tk.observe("a", 0)
	tk.observe("a", 0) // a.count = 3
	tk.observe("b", 0) // b.count = 1; now full {a:3, b:1}

	// A new key evicts the least-frequent (b) and inherits its count + 1, so it is
	// not immediately evicted again (Space-Saving) — this is what lets new keys in.
	tk.observe("c", 0)

	byKey := topKByKey(tk.snapshot())
	assert.Len(t, byKey, 2)
	assert.Equal(t, uint64(3), byKey["a"].count, "the frequent key survives")
	assert.Equal(t, uint64(2), byKey["c"].count, "the new key inherits evicted count + 1")
	_, hasB := byKey["b"]
	assert.False(t, hasB, "the least-frequent key was evicted")
}

func TestTopKSnapshotSortedAndCumulative(t *testing.T) {
	tk := newTopK(4, 0, nil)
	tk.observe("hi", 0)
	tk.observe("hi", 0)
	tk.observe("lo", 0)

	snap := tk.snapshot()
	assert.Equal(t, "hi", snap[0].key, "snapshot is sorted by count descending")
	assert.Equal(t, "lo", snap[1].key)
	// The tracker is cumulative: a snapshot does not clear it.
	assert.Len(t, tk.snapshot(), 2)
}

func TestTopKSnapshotByLatency(t *testing.T) {
	tk := newTopK(bydbqlTopKSize, 0, nil)
	tk.observe("frequent", time.Millisecond) // high count, low latency
	tk.observe("frequent", time.Millisecond)
	tk.observe("frequent", time.Millisecond)
	tk.observe("rare-but-slow", time.Second) // count 1, catastrophic latency

	assert.Equal(t, "frequent", tk.snapshot()[0].key, "snapshot ranks by count")
	assert.Equal(t, "rare-but-slow", tk.snapshotByLatency()[0].key,
		"snapshotByLatency surfaces the catastrophic outlier first")
}

func TestTopKDeterministicTieBreak(t *testing.T) {
	// Equal counts and durations must order by key, not by map iteration. Both trackers run
	// on mock clocks frozen at the same instant, so the compared slots differ only in order.
	clockA, clockB := timestamp.NewMockClock(), timestamp.NewMockClock()
	a := newTopK(bydbqlTopKSize, 0, clockA.Now)
	b := newTopK(bydbqlTopKSize, 0, clockB.Now)
	for _, k := range []string{"c", "a", "b"} {
		a.observe(k, 0)
	}
	for _, k := range []string{"b", "c", "a"} {
		b.observe(k, 0)
	}
	assert.Equal(t, a.snapshot(), b.snapshot(), "same entries yield the same order regardless of insertion")
	snap := a.snapshot()
	assert.Equal(t, []string{"a", "b", "c"}, []string{snap[0].key, snap[1].key, snap[2].key})
}

func TestTopKExpiresUnobservedEntry(t *testing.T) {
	mc := timestamp.NewMockClock()
	tk := newTopK(4, time.Hour, mc.Now)
	tk.observe("stale", time.Second)

	mc.Add(time.Hour - time.Minute)
	assert.Len(t, tk.snapshot(), 1, "still within the TTL")

	mc.Add(2 * time.Minute) // now past the TTL
	assert.Empty(t, tk.snapshot(), "an entry not seen within the TTL is dropped")
}

func TestTopKObserveRefreshesTTL(t *testing.T) {
	mc := timestamp.NewMockClock()
	tk := newTopK(4, time.Hour, mc.Now)
	tk.observe("recurring", 0)

	// Re-observing before expiry must reset the clock on the entry, so a query that
	// keeps happening is never dropped no matter how long the process has run.
	for i := 0; i < 5; i++ {
		mc.Add(50 * time.Minute)
		tk.observe("recurring", 0)
	}
	mc.Add(50 * time.Minute)

	snap := tk.snapshot()
	assert.Len(t, snap, 1, "a repeatedly observed entry survives indefinitely")
	assert.Equal(t, uint64(6), snap[0].count)
}

func TestTopKZeroTTLKeepsEntriesForever(t *testing.T) {
	mc := timestamp.NewMockClock()
	tk := newTopK(4, 0, mc.Now)
	tk.observe("kept", 0)

	mc.Add(365 * 24 * time.Hour)
	assert.Len(t, tk.snapshot(), 1, "ttl <= 0 restores the cumulative behavior")
}

func TestTopKPurgesExpiredBeforeEvictingLiveEntry(t *testing.T) {
	mc := timestamp.NewMockClock()
	tk := newTopK(2, time.Hour, mc.Now)
	tk.observe("expiring", 0)
	tk.observe("expiring", 0)
	tk.observe("expiring", 0) // count 3: the most frequent, so never the evict-min victim

	mc.Add(2 * time.Hour)  // "expiring" is now stale
	tk.observe("fresh", 0) // fills the second slot
	tk.observe("newcomer", 0)

	byKey := topKByKey(tk.snapshot())
	assert.Len(t, byKey, 2)
	_, hasExpiring := byKey["expiring"]
	assert.False(t, hasExpiring, "the stale entry is reclaimed even though it had the highest count")
	assert.Equal(t, uint64(1), byKey["newcomer"].count,
		"reclaiming a slot lets the new key start at 1 instead of inheriting an evicted count")
}

func TestTopKMaxDurAtTracksPeakNotLastObserve(t *testing.T) {
	mc := timestamp.NewMockClock()
	tk := newTopK(4, time.Hour, mc.Now)

	tk.observe("q", time.Millisecond)
	mc.Add(time.Minute)
	tk.observe("q", time.Second) // the peak
	peakAt := mc.Now()
	mc.Add(time.Minute)
	tk.observe("q", time.Millisecond) // slower observation must not move maxDurAt

	snap := tk.snapshot()
	assert.Equal(t, time.Second, snap[0].maxDur)
	assert.Equal(t, peakAt, snap[0].maxDurAt, "maxDurAt dates the peak, not the latest observation")
	assert.Equal(t, mc.Now(), snap[0].lastSeen, "lastSeen tracks the latest observation")
}

func TestTopKConcurrentObserve(t *testing.T) {
	tk := newTopK(bydbqlTopKSize, 0, nil)
	const workers, iters = 16, 500
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				tk.observe(fmt.Sprintf("q%d", (n*iters+i)%50), time.Duration(i)*time.Microsecond)
			}
		}(w)
	}
	wg.Wait()
	assert.LessOrEqual(t, len(tk.snapshot()), bydbqlTopKSize)
}
