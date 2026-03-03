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
	spec           schema.Spec
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

// Update inserts or updates an entry. Returns true if the entry was changed.
// Only accepts if entry.latestUpdateAt > existing.latestUpdateAt or not exist.
func (c *schemaCache) Update(propID string, entry *cacheEntry) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	existing, ok := c.entries[propID]
	if ok && existing.latestUpdateAt >= entry.latestUpdateAt {
		return false
	}
	c.entries[propID] = entry
	if entry.latestUpdateAt > c.latestUpdateAt {
		c.latestUpdateAt = entry.latestUpdateAt
	}
	return true
}

// Delete removes an entry. Only deletes if revision >= entry.latestUpdateAt.
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
	if revision > c.latestUpdateAt {
		c.latestUpdateAt = revision
	}
	return true
}

// GetMaxRevision returns the global max revision.
func (c *schemaCache) GetMaxRevision() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.latestUpdateAt
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
