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

	"github.com/cespare/xxhash/v2"
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
	metrics *metrics
	lru     *lru.Cache
	// evicted remembers the 64-bit hashes of recently evicted keys, so a later miss on
	// one of them can be recognized as a re-parse rather than a first-ever compile.
	// Hashes, not the query text: the text is multi-KB, the hash is 8 bytes.
	evicted  *lru.Cache
	maxBytes int
	curBytes atomic.Int64
	hits     atomic.Uint64
	misses   atomic.Uint64
}

// newPreparedCache builds a cache holding up to size entries and maxBytes of
// accounted size. A size <= 0 disables caching (lru is nil): getOrPrepare then
// parses every request and bypasses the cache without recording hits or misses.
// A maxBytes <= 0 drops the byte bound, leaving only the count.
func newPreparedCache(size, maxBytes int, m *metrics) *preparedCache {
	c := &preparedCache{maxBytes: maxBytes, metrics: m}
	if size > 0 {
		// onEvict keeps curBytes in step with both the LRU's own count-based
		// evictions and our byte-based RemoveOldest calls; it runs without the
		// cache lock, so the atomic add is safe. It also records the key, which is
		// the only first-hand evidence that a later miss on it is a re-parse.
		if l, err := lru.NewWithEvict(size, func(key, value interface{}) {
			c.curBytes.Add(-int64(value.(*cacheValue).cost))
			c.noteEvicted(key)
		}); err == nil {
			c.lru = l
		}
		// Sized to match the cache, so a template evicted at any point while the cache
		// held its current contents is still recognized when it comes back. At 8 bytes
		// per hash this is ~16KB for a 2000-entry cache.
		if e, err := lru.New(size); err == nil {
			c.evicted = e
		}
	}
	return c
}

// queryHash keys the evicted set by hash, not by the multi-KB query text: a hash is
// 8 bytes and never pins the evicted query's memory, so remembering an eviction costs
// almost nothing. Sum64String is allocation-free. A 64-bit collision (~1e-13 at a few
// thousand keys; zero across a million distinct queries in practice) would only misreport
// one first-ever compile as a re-parse — a harmless false positive, never a missed one.
func queryHash(query string) uint64 {
	return xxhash.Sum64String(query)
}

func (c *preparedCache) noteEvicted(key interface{}) {
	if c.evicted == nil {
		return
	}
	if q, ok := key.(string); ok {
		c.evicted.Add(queryHash(q), struct{}{})
	}
}

// wasEvicted reports whether query had been cached before and was evicted, which makes
// the miss now in flight a re-parse rather than a first-ever compile.
//
// Only ever called on the miss path — a hit needs no such check, it is a hit. Keeping
// the hash off the hit path leaves the ~98% of traffic that hits untouched; on a miss it
// vanishes next to the ~220us parse.
func (c *preparedCache) wasEvicted(query string) bool {
	return c.evicted != nil && c.evicted.Contains(queryHash(query))
}

// cacheResultReparse is the getOrPrepare result for a miss that re-compiled a template
// the cache had already compiled once. Only these reach the top-K tracker; to the metrics
// and the access log a re-parse is an ordinary miss.
const cacheResultReparse = "reparse"

// getOrPrepare returns the prepared statement for query, parsing and caching it on
// a miss. It records the cache metrics internally, and returns a result that tags the
// query access log so un-cached queries can be found:
//
//	"hit"     — served from the cache
//	"miss"    — a template's unavoidable first-ever compile
//	"reparse" — a miss that re-compiled a template compiled before (evicted and requested
//	            again, or too large to ever cache): real thrashing. Only the cache can
//	            classify this — it reads private eviction state and must judge before
//	            store() perturbs it — but it reports the verdict rather than acting on it,
//	            so the caller drives what happens (the top-K tracker wants only re-parses,
//	            since first-ever compiles are an unavoidable one-off cost).
//	"bypass"  — a literal (non-parameterized) query, never cached
//	""        — caching disabled
//
// A re-parse is a miss to the metrics and the access log (it is one); only the returned
// result string splits the two, so the caller can track thrashing without the cold
// starts. Callers handle only the parse error.
func (c *preparedCache) getOrPrepare(query string) (*bydbql.PreparedStatement, string, error) {
	if c.lru != nil {
		if v, ok := c.lru.Get(query); ok {
			c.hits.Add(1)
			c.emit("hit")
			return v.(*cacheValue).ps, "hit", nil
		}
	}
	ps, err := bydbql.Prepare(query)
	if err != nil {
		// A parse failure is not a cache event: it is never cached (so it cannot
		// pin memory) and is already counted by the RPC-level error metric.
		// Excluding it keeps hit_ratio from being depressed by malformed queries.
		return nil, "", err
	}
	if c.lru == nil {
		return ps, "", nil // caching disabled entirely; nothing to record
	}
	// Only parameterized queries are reusable templates worth caching. A literal
	// query bakes its values into the text, so every distinct value is a unique key
	// that would never hit and would only evict useful templates. It bypasses the
	// cache; a bypass is counted separately from hit/miss so the hit ratio still
	// reflects only cacheable queries.
	if ps.NumPlaceholders() == 0 {
		c.emit("bypass")
		return ps, "bypass", nil
	}
	c.misses.Add(1)
	// Judge BEFORE store(): store's own eviction records a new key, which on a small or
	// heavily churning evicted set can displace the very record proving this was a
	// re-parse.
	evicted := c.wasEvicted(query)
	// A statement too large to ever cache is re-parsed on every single request — the
	// worst thrashing there is, and one the evicted set can never witness.
	cached := c.store(query, ps)
	c.emit("miss") // metrics fold a re-parse into miss; only the result string splits them
	if evicted || !cached {
		return ps, cacheResultReparse, nil
	}
	return ps, "miss", nil
}

// store caches ps under the byte bound, skipping a statement that alone exceeds
// maxBytes. ContainsOrAdd inserts atomically only when the key is absent, so
// concurrent misses on the same query count its bytes exactly once. Call only for
// an enabled cache and a parameterized query. Reports whether the statement is now
// cached: false means it never can be, so every request will re-parse it.
func (c *preparedCache) store(query string, ps *bydbql.PreparedStatement) bool {
	cost := len(query) + ps.EstimatedSize()
	if c.maxBytes > 0 && cost > c.maxBytes {
		return false
	}
	if found, _ := c.lru.ContainsOrAdd(query, &cacheValue{ps: ps, cost: cost}); found {
		return true
	}
	c.curBytes.Add(int64(cost))
	for c.maxBytes > 0 && c.curBytes.Load() > int64(c.maxBytes) && c.lru.Len() > 1 {
		c.lru.RemoveOldest()
	}
	return true
}

// emit publishes the cache counter (labeled by result: hit/miss/bypass) and the
// gauges from the atomic counters.
func (c *preparedCache) emit(result string) {
	if c.metrics == nil {
		return
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
