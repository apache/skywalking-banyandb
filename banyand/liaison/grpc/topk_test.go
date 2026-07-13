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
)

func topKByKey(slots []topKSlot) map[string]topKSlot {
	m := make(map[string]topKSlot, len(slots))
	for _, s := range slots {
		m[s.key] = s
	}
	return m
}

func TestTopKCountsAndMaxDur(t *testing.T) {
	tk := newTopK(4)
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
	tk := newTopK(2)
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
	tk := newTopK(4)
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
	tk := newTopK(bydbqlTopKSize)
	tk.observe("frequent", time.Millisecond) // high count, low latency
	tk.observe("frequent", time.Millisecond)
	tk.observe("frequent", time.Millisecond)
	tk.observe("rare-but-slow", time.Second) // count 1, catastrophic latency

	assert.Equal(t, "frequent", tk.snapshot()[0].key, "snapshot ranks by count")
	assert.Equal(t, "rare-but-slow", tk.snapshotByLatency()[0].key,
		"snapshotByLatency surfaces the catastrophic outlier first")
}

func TestTopKDeterministicTieBreak(t *testing.T) {
	// Equal counts and durations must order by key, not by map iteration.
	a := newTopK(bydbqlTopKSize)
	b := newTopK(bydbqlTopKSize)
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

func TestTopKConcurrentObserve(t *testing.T) {
	tk := newTopK(bydbqlTopKSize)
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
