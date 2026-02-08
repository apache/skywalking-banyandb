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

	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
)

type cacheEntry struct {
	group          string
	name           string
	latestUpdateAt int64
	kind           schema.Kind
}

type schemaCache struct {
	entries        map[string]*cacheEntry
	latestUpdateAt int64
	mu             sync.RWMutex
}

func newSchemaCache() *schemaCache {
	return &schemaCache{
		entries: make(map[string]*cacheEntry),
	}
}

// Update updates the cache entry and returns true if it was changed.
func (c *schemaCache) Update(propID string, entry *cacheEntry) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	existing, exists := c.entries[propID]
	if exists && existing.latestUpdateAt >= entry.latestUpdateAt {
		return false
	}
	c.entries[propID] = entry
	if entry.latestUpdateAt > c.latestUpdateAt {
		c.latestUpdateAt = entry.latestUpdateAt
	}
	return true
}

// Delete removes an entry from the cache if the provided revision is
// >= the entry's latestUpdateAt. Returns true if the entry was deleted.
func (c *schemaCache) Delete(propID string, revision int64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	existing, exists := c.entries[propID]
	if !exists {
		return false
	}
	if revision < existing.latestUpdateAt {
		return false
	}
	delete(c.entries, propID)
	return true
}

// GetMaxRevision returns the maximum revision seen.
func (c *schemaCache) GetMaxRevision() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.latestUpdateAt
}

// UpdateMaxRevision updates the maximum revision if the provided revision is newer.
func (c *schemaCache) UpdateMaxRevision(revision int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if revision > c.latestUpdateAt {
		c.latestUpdateAt = revision
	}
}

// GetAllEntries returns a copy of all cache entries.
func (c *schemaCache) GetAllEntries() map[string]*cacheEntry {
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := make(map[string]*cacheEntry, len(c.entries))
	for k, v := range c.entries {
		result[k] = v
	}
	return result
}

// GetEntriesByKind returns entries matching the specified kind.
func (c *schemaCache) GetEntriesByKind(kind schema.Kind) map[string]*cacheEntry {
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := make(map[string]*cacheEntry)
	for propID, entry := range c.entries {
		if entry.kind == kind {
			result[propID] = entry
		}
	}
	return result
}

// Size returns the number of entries in the cache.
func (c *schemaCache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.entries)
}

// GetCachedKinds returns the unique kinds that have cached entries.
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
