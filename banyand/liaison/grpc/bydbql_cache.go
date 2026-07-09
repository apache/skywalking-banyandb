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
	"sync/atomic"

	lru "github.com/hashicorp/golang-lru"

	"github.com/apache/skywalking-banyandb/pkg/bydbql"
)

// cacheValue is a cached prepared statement plus its accounted byte cost (query
// text plus the estimated statement footprint), so evictions can adjust curBytes.
type cacheValue struct {
	ps   *bydbql.PreparedStatement
	cost int
}

// preparedCache is a concurrency-safe LRU over parsed BydbQL prepared statements,
// keyed by the exact query text. A cached statement is immutable and safe to Bind
// concurrently, and the parse result depends only on the query text, so entries
// never need invalidation. The count bound is enforced by the underlying LRU; the
// byte bound (accounted key text plus estimated statement size) is enforced on top
// via curBytes and RemoveOldest. All shared state is either behind the LRU's own
// lock or atomic, so the cache needs no mutex of its own.
type preparedCache struct {
	metrics  *metrics
	lru      *lru.Cache
	maxBytes int
	curBytes atomic.Int64
	hits     atomic.Uint64
	misses   atomic.Uint64
}

// newPreparedCache builds a cache holding up to size entries and maxBytes of
// accounted size. A size <= 0 disables caching (lru is nil): getOrPrepare then
// parses every request, each counted as a miss. A maxBytes <= 0 drops the byte
// bound, leaving only the count.
func newPreparedCache(size, maxBytes int, m *metrics) *preparedCache {
	c := &preparedCache{maxBytes: maxBytes, metrics: m}
	if size > 0 {
		// onEvict keeps curBytes in step with both the LRU's own count-based
		// evictions and our byte-based RemoveOldest calls; it runs without the
		// cache lock, so the atomic add is safe.
		if l, err := lru.NewWithEvict(size, func(_, value interface{}) {
			c.curBytes.Add(-int64(value.(*cacheValue).cost))
		}); err == nil {
			c.lru = l
		}
	}
	return c
}

// getOrPrepare returns the prepared statement for query, parsing and caching it on
// a miss. It records the cache metrics internally, so callers handle only the parse
// error.
func (c *preparedCache) getOrPrepare(query string) (*bydbql.PreparedStatement, error) {
	if c.lru != nil {
		if v, ok := c.lru.Get(query); ok {
			c.hits.Add(1)
			c.emit(true)
			return v.(*cacheValue).ps, nil
		}
	}
	ps, err := bydbql.Prepare(query)
	if err != nil {
		// A parse failure is not a cache event: it is never cached (so it cannot
		// pin memory) and is already counted by the RPC-level error metric.
		// Excluding it keeps hit_ratio from being depressed by malformed queries.
		return nil, err
	}
	// Only parameterized queries are reusable templates worth caching. A literal
	// query bakes its values into the text, so every distinct value is a unique key
	// that would never hit and would only evict useful templates. A literal query
	// (and a disabled cache) therefore bypasses without touching cache or metrics,
	// so hit_ratio reflects only genuinely cacheable queries.
	if c.lru == nil || ps.NumPlaceholders() == 0 {
		return ps, nil
	}
	c.misses.Add(1)
	c.store(query, ps)
	c.emit(false)
	return ps, nil
}

// store caches ps under the byte bound, skipping a statement that alone exceeds
// maxBytes. ContainsOrAdd inserts atomically only when the key is absent, so
// concurrent misses on the same query count its bytes exactly once. Call only for
// an enabled cache and a parameterized query.
func (c *preparedCache) store(query string, ps *bydbql.PreparedStatement) {
	cost := len(query) + ps.EstimatedSize()
	if c.maxBytes > 0 && cost > c.maxBytes {
		return
	}
	if found, _ := c.lru.ContainsOrAdd(query, &cacheValue{ps: ps, cost: cost}); found {
		return
	}
	c.curBytes.Add(int64(cost))
	for c.maxBytes > 0 && c.curBytes.Load() > int64(c.maxBytes) && c.lru.Len() > 1 {
		c.lru.RemoveOldest()
	}
}

// emit publishes the cache counter and gauges from the atomic counters.
func (c *preparedCache) emit(hit bool) {
	if c.metrics == nil {
		return
	}
	result := "miss"
	if hit {
		result = "hit"
	}
	hits, misses := c.hits.Load(), c.misses.Load()
	c.metrics.bydbqlPreparedCacheTotal.Inc(1, result)
	if total := hits + misses; total > 0 {
		c.metrics.bydbqlPreparedCacheHitRatio.Set(float64(hits) / float64(total))
	}
	entries := 0
	if c.lru != nil {
		entries = c.lru.Len()
	}
	c.metrics.bydbqlPreparedCacheCount.Set(float64(entries))
	c.metrics.bydbqlPreparedCacheBytes.Set(float64(c.curBytes.Load()))
}
