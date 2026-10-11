// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. Apache Software
// Foundation (ASF) licenses this file to you under the Apache License, Version
// 2.0 (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package native

import (
	"sync"
	"sync/atomic"
)

// presenceCacheEntryOverhead approximates the bookkeeping cost of one cache
// entry (map slot, struct header) beyond its keyed bytes, so a budget of a
// few hundred bytes still bounds the cache to a handful of entries instead of
// admitting it unbounded.
const presenceCacheEntryOverhead = 64

// presenceCache memoizes InsertIfAbsent "identifier present with this field
// set" decisions so a repeat admission for the same identifier need not
// re-scan every candidate segment. Entries are keyed by identifier only, not
// by root generation: a generation key would miss on every lookup, since the
// generation advances on every admission, including the one that populated
// the entry.
//
// Only positive decisions are ever cached. A negative ("absent") decision
// stops being true the instant the document is admitted, which would require
// invalidating it anyway; caching only positives means there is nothing to
// invalidate on admit beyond what callers already must invalidate on delete
// or replace. Every caller that removes an identifier's live posting, or
// replaces it with a document carrying fewer fields, must call invalidate
// for it (or reset the whole cache) before the new state becomes visible, or
// a later lookup can return a stale "present".
//
//nolint:govet // cache fields are grouped by synchronization role.
type presenceCache struct {
	mu        sync.Mutex
	maxBytes  int
	usedBytes int
	entries   map[string]presenceCacheEntry
	// hits and misses count lookup outcomes. They exist primarily so tests
	// can observe that a repeat InsertIfAbsent admission actually reused a
	// cached decision instead of rescanning segments.
	hits   atomic.Uint64
	misses atomic.Uint64
}

// presenceCacheEntry records that identifier is live with a field set
// covering at least fields.
type presenceCacheEntry struct {
	fields map[string]struct{}
	size   int
}

func newPresenceCache(maxBytes int) *presenceCache {
	return &presenceCache{maxBytes: maxBytes, entries: make(map[string]presenceCacheEntry)}
}

// lookup reports whether identifier is cached present with a field set that
// covers want. False means: compute presence from the admission root.
func (c *presenceCache) lookup(identifier []byte, want map[string]struct{}) bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	entry, found := c.entries[string(identifier)]
	c.mu.Unlock()
	if !found || !fieldSetContainsAll(entry.fields, want) {
		c.misses.Add(1)
		return false
	}
	c.hits.Add(1)
	return true
}

// store memoizes identifier as present with fields, the field-name set of
// the segment that established presence. Only ever called with a positive
// decision; see the type doc for why negatives are never cached.
func (c *presenceCache) store(identifier []byte, fields map[string]struct{}) {
	if c == nil || c.maxBytes <= 0 {
		return
	}
	size := presenceCacheEntryOverhead + len(identifier)
	for name := range fields {
		size += len(name)
	}
	if size > c.maxBytes {
		// A single entry this large would immediately evict everything else;
		// bounded memory is better served by simply not caching it.
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	key := string(identifier)
	if existing, found := c.entries[key]; found {
		c.usedBytes -= existing.size
	}
	for c.usedBytes+size > c.maxBytes && len(c.entries) > 0 {
		for evictKey, evictEntry := range c.entries {
			delete(c.entries, evictKey)
			c.usedBytes -= evictEntry.size
			break
		}
	}
	c.entries[key] = presenceCacheEntry{fields: fields, size: size}
	c.usedBytes += size
}

// invalidate drops the cached entry for identifier, if any. Callers must
// invoke it for every identifier whose live posting an admission removes or
// replaces with a document carrying fewer fields, before the new state
// becomes visible to another InsertIfAbsent admission.
func (c *presenceCache) invalidate(identifier []byte) {
	if c == nil {
		return
	}
	c.mu.Lock()
	key := string(identifier)
	if entry, found := c.entries[key]; found {
		delete(c.entries, key)
		c.usedBytes -= entry.size
	}
	c.mu.Unlock()
}

// reset drops every memoized entry without touching admitted data.
func (c *presenceCache) reset() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.entries = make(map[string]presenceCacheEntry)
	c.usedBytes = 0
	c.mu.Unlock()
}

// ResetPresenceCache drops every memoized InsertIfAbsent presence entry. It
// never touches admitted data; the next InsertIfAbsent batch simply
// recomputes presence from the admission root. It is a no-op when
// OwnerOptions.PresenceCacheBytes was not positive.
func (o *Owner) ResetPresenceCache() {
	if o == nil {
		return
	}
	o.presenceCache.reset()
}

// fieldSetContainsAll reports whether have contains every name in want.
func fieldSetContainsAll(have, want map[string]struct{}) bool {
	for name := range want {
		if _, ok := have[name]; !ok {
			return false
		}
	}
	return true
}
