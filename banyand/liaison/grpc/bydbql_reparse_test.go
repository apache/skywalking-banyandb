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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newReparseProbe returns the cache, a tracker, and a run() that drives one query
// through the cache exactly as bydbQLService.Query does: it feeds the tracker only when
// getOrPrepare reports a re-parse. Tests go through run() rather than observing the
// cache directly, so they exercise the real "if reparse" decision and would catch a
// change that broke it. run() returns the cache result for assertions.
func newReparseProbe(t *testing.T, size, maxBytes int) (*preparedCache, *topK, func(query string) string) {
	t.Helper()
	tk := newTopK(bydbqlTopKSize)
	c := newPreparedCache(size, maxBytes, nil)
	run := func(q string) string {
		_, result, err := c.getOrPrepare(q)
		require.NoError(t, err)
		if result == cacheResultReparse {
			tk.observe(q, 0)
		}
		return result
	}
	return c, tk, run
}

// The workload that exposed the bug, measured on a live SkyWalking OAP cluster:
// 2495 distinct parameterized templates against a cache large enough to hold them all
// (2495 entries of ~1.7KB against a 4000-entry / 10MiB bound, so nothing is ever
// evicted). Every template is compiled exactly once, so there is no thrashing at all
// and the dump must be empty.
//
// Before the fix the tracker was fed every miss, including these first-ever compiles.
// Past bydbqlTopKSize distinct keys, Space-Saving's "new key inherits the evicted
// minimum's count + 1" ratcheted all 128 slots to counts of 19-20, so every one of them
// cleared the old count>=2 threshold and the log reported 128 templates as thrashing.
// Measured live: {count 20: 23 templates, count 19: 105}.
func TestReparse_ColdStartCompilesNeverReachTheTracker(t *testing.T) {
	const distinct = 2495
	c, tk, run := newReparseProbe(t, 4000, 10<<20)

	for i := 0; i < distinct; i++ {
		require.Equal(t, "miss", run(bydbqlQuery(i)), "a first-ever compile is still a cache miss")
	}

	require.Equal(t, distinct, c.lru.Len(), "nothing was evicted, so nothing can be a re-parse")
	assert.Empty(t, tk.snapshot(), "first-ever compiles must not be reported as thrashing")
	assert.Empty(t, formatTopK(tk.snapshot(), 1, func(s topKSlot) string { return s.key }),
		"the dump must be empty on a cluster that is merely warming its cache")
}

// The counterpart: with the cache too small for the working set, the templates really
// are re-parsed, and every count must be exact rather than a Space-Saving artifact.
func TestReparse_ThrashingIsReportedWithTrueCounts(t *testing.T) {
	const rounds = 5
	_, tk, run := newReparseProbe(t, 1, 10<<20) // one slot: A and B evict each other

	for i := 0; i < rounds; i++ {
		run(bydbqlQuery(0))
		run(bydbqlQuery(1))
	}

	snap := tk.snapshot()
	require.Len(t, snap, 2)
	counts := map[string]uint64{snap[0].key: snap[0].count, snap[1].key: snap[1].count}
	// Each template is compiled once for free, then re-parsed on every later round.
	assert.Equal(t, uint64(rounds-1), counts[bydbqlQuery(0)])
	assert.Equal(t, uint64(rounds-1), counts[bydbqlQuery(1)])
}

// A statement too large to ever cache is re-parsed on every single request. The evicted
// set can never witness it — it was never in the cache to be evicted — so getOrPrepare
// reports the re-parse from store()'s failure to cache it.
func TestReparse_OversizedStatementIsReportedEveryTime(t *testing.T) {
	c, tk, run := newReparseProbe(t, 10, 1) // maxBytes=1: nothing is ever cacheable

	for i := 0; i < 3; i++ {
		run(bydbqlQuery(0))
	}

	require.Zero(t, c.lru.Len(), "the statement is never cached")
	snap := tk.snapshot()
	require.Len(t, snap, 1)
	assert.Equal(t, uint64(3), snap[0].count, "every request re-parses it, including the first")
}

// Guards the ordering the fix depends on: store() evicts a victim and records that
// eviction, which on a full evicted set can displace the record proving the query now
// being stored was itself a re-parse. Judging before store() is what keeps this exact.
func TestReparse_DetectedEvenWhenStoreDisplacesTheEvidence(t *testing.T) {
	_, tk, run := newReparseProbe(t, 1, 10<<20) // cache and evicted set both hold one entry

	run(bydbqlQuery(0)) // compile A
	run(bydbqlQuery(1)) // compile B, evicting A
	require.Empty(t, tk.snapshot(), "so far only first-ever compiles")

	// Storing A evicts B and records B, pushing A's own eviction record out of the
	// one-entry evicted set. The re-parse must already have been detected by then.
	run(bydbqlQuery(0))

	snap := tk.snapshot()
	require.Len(t, snap, 1)
	assert.Equal(t, bydbqlQuery(0), snap[0].key)
	assert.Equal(t, uint64(1), snap[0].count)
}

// A template evicted once and then requested again was really re-compiled, and the dump
// must say so. The old count>=2 threshold existed only to hide cold starts; now that they
// are excluded at the source, keeping it would re-hide the very re-parses that exclusion
// made visible — and would leave the log LESS sensitive than before the fix, which
// surfaced a template on its cold start plus one re-parse.
func TestReparse_SingleReparseReachesTheDump(t *testing.T) {
	_, tk, run := newReparseProbe(t, 1, 10<<20)

	run(bydbqlQuery(0)) // cold-start compile
	run(bydbqlQuery(1)) // evicts it
	run(bydbqlQuery(0)) // one real re-parse

	lines := formatTopK(tk.snapshot(), 1, func(s topKSlot) string { return s.key })
	require.Len(t, lines, 1, "a single genuine re-parse must not be filtered away")
	assert.Equal(t, bydbqlQuery(0), lines[0])
}

// Caching disabled: every request parses, but nothing is a re-parse of a cached entry,
// and the cache records neither hits nor misses.
func TestReparse_SilentWhenCachingDisabled(t *testing.T) {
	_, tk, run := newReparseProbe(t, 0, 0)

	for i := 0; i < 3; i++ {
		require.Empty(t, run(bydbqlQuery(0)), "a disabled cache records no result")
	}
	assert.Empty(t, tk.snapshot())
}

// Literal queries bypass the cache by design and are not re-parses of anything cached.
func TestReparse_BypassIsNotAReparse(t *testing.T) {
	_, tk, run := newReparseProbe(t, 10, 10<<20)

	for i := 0; i < 3; i++ {
		require.Equal(t, "bypass", run(bydbqlLiteralQuery(0)))
	}
	assert.Empty(t, tk.snapshot())
}
