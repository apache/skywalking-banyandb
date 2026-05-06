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

package property

import (
	"sync"
	"time"

	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
)

type cacheEntry struct {
	spec           schema.Spec
	group          string
	name           string
	latestUpdateAt int64
	modRevision    int64
	kind           schema.Kind
}

// DefaultMaxTombstoneEntries bounds the size of the tombstone index. Each entry is
// roughly a propID string plus an int64 deleteTime — at ~100 bytes apiece the default
// caps memory at ~10MB. The cap is defensive: under pathological churn (e.g. rapid
// create/delete loops) the GC loop alone would let the index grow to retention × rate,
// which can exceed available memory before retention elapses.
const DefaultMaxTombstoneEntries = 100_000

type schemaCache struct {
	entries        map[string]*cacheEntry
	tombstoneIndex map[string]int64 // propID → deleteTime (unix nano)
	latestUpdateAt int64
	// notifiedModRevision is the monotonic watermark advanced only after all downstream
	// schema event handlers (e.g. groupRepo, entityRepo, pkg/schema.schemaRepo) have been
	// notified for a given mod_revision. The barrier reads this watermark so callers can
	// rely on "AwaitRevisionApplied(R) returning true" meaning every downstream cache has
	// processed all events up to R — not just the property cache.
	notifiedModRevision int64
	// maxTombstones bounds tombstoneIndex size; 0 means unlimited. When set, Delete
	// evicts the oldest tombstone before recording a new one once the cap is reached.
	// Eviction sacrifices retention for the smallest fraction of entries — preferable
	// to OOM under a runaway delete workload.
	maxTombstones int
	mu            sync.RWMutex
}

func newSchemaCache() *schemaCache {
	return newSchemaCacheWithLimit(DefaultMaxTombstoneEntries)
}

func newSchemaCacheWithLimit(maxTombstones int) *schemaCache {
	return &schemaCache{
		entries:        make(map[string]*cacheEntry),
		tombstoneIndex: make(map[string]int64),
		maxTombstones:  maxTombstones,
	}
}

// Update inserts or updates an entry. Returns true if the entry was changed.
// Only accepts if entry.latestUpdateAt > existing.latestUpdateAt or not exist.
// A live entry supersedes any tombstone for the same propID.
func (c *schemaCache) Update(propID string, entry *cacheEntry) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	existing, ok := c.entries[propID]
	if ok && existing.latestUpdateAt >= entry.latestUpdateAt {
		return false
	}
	c.entries[propID] = entry
	delete(c.tombstoneIndex, propID)
	if entry.latestUpdateAt > c.latestUpdateAt {
		c.latestUpdateAt = entry.latestUpdateAt
	}
	return true
}

// Delete removes an entry. Only deletes if revision >= entry.latestUpdateAt.
// When an entry is removed, the propID is recorded in tombstoneIndex so the
// GC loop can later purge it from the client cache after tombstoneRetention elapses.
// If maxTombstones is set and the index is at capacity, the oldest tombstone
// is evicted before the new one is recorded so the index never grows unbounded.
func (c *schemaCache) Delete(propID string, revision int64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	existing, ok := c.entries[propID]
	if !ok {
		return false
	}
	if revision < existing.latestUpdateAt {
		return false
	}
	delete(c.entries, propID)
	if revision > 0 {
		if _, alreadyTracked := c.tombstoneIndex[propID]; !alreadyTracked {
			c.evictOldestTombstoneIfFull()
		}
		c.tombstoneIndex[propID] = revision
	}
	if revision > c.latestUpdateAt {
		c.latestUpdateAt = revision
	}
	return true
}

// evictOldestTombstoneIfFull removes the tombstone with the smallest deleteTime
// when the index has reached maxTombstones. Caller must hold c.mu.
func (c *schemaCache) evictOldestTombstoneIfFull() {
	if c.maxTombstones <= 0 || len(c.tombstoneIndex) < c.maxTombstones {
		return
	}
	var oldestID string
	var oldestTime int64
	first := true
	for pid, t := range c.tombstoneIndex {
		if first || t < oldestTime {
			oldestID = pid
			oldestTime = t
			first = false
		}
	}
	if oldestID != "" {
		delete(c.tombstoneIndex, oldestID)
	}
}

// TombstoneEntries returns propIDs of tombstone entries whose deleteTime is older
// than retention and are not superseded by a live entry in the cache.
func (c *schemaCache) TombstoneEntries(retention time.Duration) []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	cutoff := time.Now().Add(-retention).UnixNano()
	var expired []string
	for propID, deleteTime := range c.tombstoneIndex {
		if deleteTime <= cutoff {
			if _, live := c.entries[propID]; !live {
				expired = append(expired, propID)
			}
		}
	}
	return expired
}

// RemoveTombstone removes a propID from the tombstone index.
func (c *schemaCache) RemoveTombstone(propID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.tombstoneIndex, propID)
}

// GetMaxRevision returns the global max revision.
func (c *schemaCache) GetMaxRevision() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.latestUpdateAt
}

// GetMaxModRevision returns the monotonic watermark of the highest mod_revision
// fully processed by every downstream schema event handler. Events with
// mod_revision <= this value are guaranteed observable in all handler caches.
func (c *schemaCache) GetMaxModRevision() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.notifiedModRevision
}

// GetKeyModRevision returns the modRevision for the given propID, but only once
// that entry's mod_revision has been fully notified to every handler. An entry
// present in the cache map but not yet notified downstream reports (0, false)
// so barrier callers correctly wait for handler-side coherence.
func (c *schemaCache) GetKeyModRevision(propID string) (int64, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	entry, ok := c.entries[propID]
	if !ok {
		return 0, false
	}
	if entry.modRevision > c.notifiedModRevision {
		return 0, false
	}
	return entry.modRevision, true
}

// KeyRevision pairs a property ID with its observed mod_revision and a
// presence flag. Returned by GetKeyRevisions in the same order the caller
// supplied propIDs so the cluster RPC handler can join the result with its
// proto SchemaKey input slice.
type KeyRevision struct {
	PropID      string
	ModRevision int64
	Present     bool
}

// GetKeyRevisions returns presence + mod_revision for each propID in a single
// read-lock pass. It applies the same downstream-coherence gate as
// GetKeyModRevision: an entry present in the cache map whose mod_revision
// exceeds notifiedModRevision is reported as Present=false, ModRevision=0.
// An empty input returns a non-nil empty slice.
func (c *schemaCache) GetKeyRevisions(propIDs []string) []KeyRevision {
	out := make([]KeyRevision, len(propIDs))
	c.mu.RLock()
	defer c.mu.RUnlock()
	for i, pid := range propIDs {
		out[i].PropID = pid
		entry, ok := c.entries[pid]
		if !ok {
			continue
		}
		if entry.modRevision > c.notifiedModRevision {
			continue
		}
		out[i].ModRevision = entry.modRevision
		out[i].Present = true
	}
	return out
}

// AdvanceNotified pushes the downstream-handler watermark forward monotonically.
// Callers must invoke this only after notifyHandlers returns for the given revision,
// so downstream caches (groupRepo, entityRepo, pkg/schema.schemaRepo, …) are coherent
// with the property cache at the moment the watermark advances.
func (c *schemaCache) AdvanceNotified(rev int64) {
	if rev <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if rev > c.notifiedModRevision {
		c.notifiedModRevision = rev
	}
}

// GetModRevisionByKey returns the modRevision for the first cached entry that matches
// kind, group, and name. The second return value is false when no matching entry exists.
func (c *schemaCache) GetModRevisionByKey(kind schema.Kind, group, name string) (int64, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, entry := range c.entries {
		if entry.kind == kind && entry.group == group && entry.name == name {
			return entry.modRevision, true
		}
	}
	return 0, false
}

// Get returns a copy of the entry for the given propID, or nil if not found.
func (c *schemaCache) Get(propID string) *cacheEntry {
	c.mu.RLock()
	defer c.mu.RUnlock()
	entry, ok := c.entries[propID]
	if !ok {
		return nil
	}
	copied := *entry
	return &copied
}

// GetAllEntries returns a copy of all entries.
func (c *schemaCache) GetAllEntries() map[string]*cacheEntry {
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := make(map[string]*cacheEntry, len(c.entries))
	for key, entry := range c.entries {
		copied := *entry
		result[key] = &copied
	}
	return result
}

// GetEntriesByKind returns entries filtered by kind.
func (c *schemaCache) GetEntriesByKind(kind schema.Kind) map[string]*cacheEntry {
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := make(map[string]*cacheEntry)
	for key, entry := range c.entries {
		if entry.kind == kind {
			copied := *entry
			result[key] = &copied
		}
	}
	return result
}

// GetCachedKinds returns unique kinds with cached entries.
func (c *schemaCache) GetCachedKinds() []schema.Kind {
	c.mu.RLock()
	defer c.mu.RUnlock()
	kindSet := make(map[schema.Kind]struct{})
	for _, entry := range c.entries {
		kindSet[entry.kind] = struct{}{}
	}
	kinds := make([]schema.Kind, 0, len(kindSet))
	for kind := range kindSet {
		kinds = append(kinds, kind)
	}
	return kinds
}

// Size returns the number of entries.
func (c *schemaCache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.entries)
}
