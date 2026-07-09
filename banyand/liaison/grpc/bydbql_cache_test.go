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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// bydbqlQuery is a distinct, parameterized (cacheable) query per i.
func bydbqlQuery(i int) string {
	return fmt.Sprintf("SELECT * FROM STREAM sw IN default WHERE service_id = 'v%d' AND duration > ?", i)
}

// bydbqlLiteralQuery is a distinct query per i with no placeholder (not cacheable).
func bydbqlLiteralQuery(i int) string {
	return fmt.Sprintf("SELECT * FROM STREAM sw IN default WHERE service_id = 'v%d'", i)
}

// probeEntryCost returns the accounted byte cost of one cached entry (query text
// plus estimated statement size), so byte-bound tests stay valid as the estimate
// or query template changes.
func probeEntryCost(t *testing.T) int {
	t.Helper()
	c := newPreparedCache(10, 0, nil)
	_, err := c.getOrPrepare(bydbqlQuery(0))
	require.NoError(t, err)
	require.Positive(t, c.curBytes.Load())
	return int(c.curBytes.Load())
}

func TestPreparedCacheHitReturnsSameInstance(t *testing.T) {
	c := newPreparedCache(16, 0, nil)
	first, err := c.getOrPrepare(bydbqlQuery(0))
	require.NoError(t, err)
	require.NotNil(t, first)
	second, err := c.getOrPrepare(bydbqlQuery(0))
	require.NoError(t, err)
	assert.Same(t, first, second, "a cache hit must return the same prepared statement")
	assert.Equal(t, uint64(1), c.hits.Load())
	assert.Equal(t, uint64(1), c.misses.Load())
}

func TestPreparedCacheEvictsLeastRecentlyUsedByCount(t *testing.T) {
	c := newPreparedCache(2, 0, nil)
	_, _ = c.getOrPrepare(bydbqlQuery(1))
	_, _ = c.getOrPrepare(bydbqlQuery(2))
	// Touch q1 so q2 becomes the least-recently-used entry.
	_, _ = c.getOrPrepare(bydbqlQuery(1))
	// Adding q3 must evict q2, not q1.
	_, _ = c.getOrPrepare(bydbqlQuery(3))
	assert.Equal(t, 2, c.lru.Len())

	missBefore := c.misses.Load()
	_, _ = c.getOrPrepare(bydbqlQuery(1))
	assert.Equal(t, missBefore, c.misses.Load(), "recently used q1 should still be cached")
	_, _ = c.getOrPrepare(bydbqlQuery(2))
	assert.Equal(t, missBefore+1, c.misses.Load(), "least-recently-used q2 should have been evicted")
}

func TestPreparedCacheEvictsByBytes(t *testing.T) {
	// A cap of 2.5x one entry's cost holds at most two entries. Deriving it from a
	// probe keeps the test valid regardless of the estimated statement size.
	perEntry := probeEntryCost(t)
	maxBytes := perEntry*2 + perEntry/2
	c := newPreparedCache(100, maxBytes, nil)
	for i := 0; i < 5; i++ {
		_, err := c.getOrPrepare(bydbqlQuery(i))
		require.NoError(t, err)
	}
	assert.LessOrEqual(t, int(c.curBytes.Load()), maxBytes, "byte cap must bound total accounted size")
	assert.Equal(t, 2, c.lru.Len(), "a 2.5x-cost byte cap must hold exactly two entries")
	assert.Equal(t, perEntry*c.lru.Len(), int(c.curBytes.Load()), "curBytes must track the live entries")
}

func TestPreparedCacheSkipsOversizedQuery(t *testing.T) {
	c := newPreparedCache(16, 8, nil) // every real query exceeds 8 bytes
	stmt, err := c.getOrPrepare(bydbqlQuery(0))
	require.NoError(t, err)
	require.NotNil(t, stmt, "an oversized query must still be prepared, just not cached")
	assert.Equal(t, 0, c.lru.Len())
	assert.Zero(t, c.curBytes.Load())
}

func TestPreparedCacheDisabled(t *testing.T) {
	c := newPreparedCache(0, 0, nil)
	first, err := c.getOrPrepare(bydbqlQuery(0))
	require.NoError(t, err)
	require.NotNil(t, first)
	second, err := c.getOrPrepare(bydbqlQuery(0))
	require.NoError(t, err)
	assert.NotSame(t, first, second, "a disabled cache must re-prepare every request")
	assert.Nil(t, c.lru, "a disabled cache holds no LRU")
	// A disabled cache bypasses without recording cache hits or misses.
	assert.Equal(t, uint64(0), c.hits.Load())
	assert.Equal(t, uint64(0), c.misses.Load())
}

func TestPreparedCacheSkipsLiteralQuery(t *testing.T) {
	c := newPreparedCache(16, 1<<20, nil)
	first, err := c.getOrPrepare(bydbqlLiteralQuery(0))
	require.NoError(t, err)
	require.NotNil(t, first)
	second, err := c.getOrPrepare(bydbqlLiteralQuery(0))
	require.NoError(t, err)
	// A placeholder-free query is not a reusable template: never cached, re-parsed
	// each time, and not counted so it cannot depress hit_ratio.
	assert.NotSame(t, first, second, "a literal query must be re-prepared, not cached")
	assert.Equal(t, 0, c.lru.Len(), "a literal query must never be stored")
	assert.Equal(t, uint64(0), c.hits.Load())
	assert.Equal(t, uint64(0), c.misses.Load())
}

func TestPreparedCacheParseErrorNotCached(t *testing.T) {
	c := newPreparedCache(16, 0, nil)
	_, err := c.getOrPrepare("NOT A QUERY")
	require.Error(t, err)
	assert.Equal(t, 0, c.lru.Len(), "a parse failure must not be cached")
	// A parse failure is not a cache event, so it must not move hit/miss counters.
	assert.Equal(t, uint64(0), c.hits.Load())
	assert.Equal(t, uint64(0), c.misses.Load())
}

func TestPreparedCacheEmitsMetrics(t *testing.T) {
	// A non-nil metrics wiring exercises emit() end to end (counter label, hit-ratio
	// math, and the count/bytes gauges) on both the miss and hit paths.
	c := newPreparedCache(4, 1<<20, newBypassMetrics())
	_, err := c.getOrPrepare(bydbqlQuery(0))
	require.NoError(t, err)
	_, err = c.getOrPrepare(bydbqlQuery(0))
	require.NoError(t, err)
	assert.Equal(t, uint64(1), c.hits.Load())
	assert.Equal(t, uint64(1), c.misses.Load())
}

func TestPreparedCacheConcurrentReuse(t *testing.T) {
	// size 8 with 64 distinct keys and a byte cap of ~4 entries forces both count-
	// and byte-driven eviction to run concurrently, so -race covers the eviction
	// path that mutates curBytes.
	const (
		workers = 32
		iters   = 50
		keys    = 64
		size    = 8
	)
	maxBytes := probeEntryCost(t) * 4
	c := newPreparedCache(size, maxBytes, nil)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for j := 0; j < iters; j++ {
				stmt, err := c.getOrPrepare(bydbqlQuery((n*iters + j) % keys))
				assert.NoError(t, err)
				assert.NotNil(t, stmt)
			}
		}(i)
	}
	wg.Wait()
	assert.LessOrEqual(t, c.lru.Len(), size)
	assert.LessOrEqual(t, int(c.curBytes.Load()), maxBytes, "byte cap must hold under concurrent eviction")
	assert.GreaterOrEqual(t, c.curBytes.Load(), int64(0), "curBytes must never drift negative")
	assert.Equal(t, uint64(workers*iters), c.hits.Load()+c.misses.Load())
}
