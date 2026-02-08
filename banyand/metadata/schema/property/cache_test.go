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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
)

func TestSchemaCache_NewSchemaCache(t *testing.T) {
	cache := newSchemaCache()
	assert.NotNil(t, cache)
	assert.NotNil(t, cache.entries)
	assert.Equal(t, int64(0), cache.latestUpdateAt)
}

func TestSchemaCache_Update(t *testing.T) {
	cache := newSchemaCache()

	entry1 := &cacheEntry{
		latestUpdateAt: 100,
		kind:           schema.KindStream,
		group:          "test-group",
		name:           "test-stream",
	}

	changed := cache.Update("stream_test-group/test-stream", entry1)
	assert.True(t, changed, "First update should return true")
	assert.Equal(t, int64(100), cache.GetMaxRevision())

	changed = cache.Update("stream_test-group/test-stream", entry1)
	assert.False(t, changed, "Same entry update should return false")

	entry2 := &cacheEntry{
		latestUpdateAt: 150,
		kind:           schema.KindStream,
		group:          "test-group",
		name:           "test-stream",
	}
	changed = cache.Update("stream_test-group/test-stream", entry2)
	assert.True(t, changed, "Updated entry should return true")
	assert.Equal(t, int64(150), cache.GetMaxRevision())

	oldEntry := &cacheEntry{
		latestUpdateAt: 50,
		kind:           schema.KindStream,
		group:          "test-group",
		name:           "test-stream",
	}
	changed = cache.Update("stream_test-group/test-stream", oldEntry)
	assert.False(t, changed, "Older revision should return false")
	assert.Equal(t, int64(150), cache.GetMaxRevision())
}

func TestSchemaCache_Delete(t *testing.T) {
	cache := newSchemaCache()

	entry := &cacheEntry{
		latestUpdateAt: 100,
		kind:           schema.KindStream,
		group:          "test-group",
		name:           "test-stream",
	}
	cache.Update("stream_test-group/test-stream", entry)

	deleted := cache.Delete("stream_test-group/test-stream", 50)
	assert.False(t, deleted, "Older revision should not delete entry")
	assert.Len(t, cache.GetAllEntries(), 1)

	deleted = cache.Delete("stream_test-group/test-stream", 100)
	assert.True(t, deleted, "Equal revision should delete entry")
	assert.Empty(t, cache.GetAllEntries())

	cache.Update("stream_test-group/test-stream", entry)

	deleted = cache.Delete("stream_test-group/test-stream", 200)
	assert.True(t, deleted, "Newer revision should delete entry")
	assert.Empty(t, cache.GetAllEntries())

	deleted = cache.Delete("stream_test-group/test-stream", 200)
	assert.False(t, deleted, "Deleting non-existing entry should return false")

	deleted = cache.Delete("non-existent-key", 100)
	assert.False(t, deleted, "Deleting non-existent key should return false")
}

func TestSchemaCache_GetMaxRevision(t *testing.T) {
	cache := newSchemaCache()
	assert.Equal(t, int64(0), cache.GetMaxRevision())

	cache.Update("key1", &cacheEntry{latestUpdateAt: 100})
	assert.Equal(t, int64(100), cache.GetMaxRevision())

	cache.Update("key2", &cacheEntry{latestUpdateAt: 200})
	assert.Equal(t, int64(200), cache.GetMaxRevision())

	cache.Update("key3", &cacheEntry{latestUpdateAt: 150})
	assert.Equal(t, int64(200), cache.GetMaxRevision())
}

func TestSchemaCache_GetAllEntries(t *testing.T) {
	cache := newSchemaCache()

	entries := cache.GetAllEntries()
	assert.Empty(t, entries)

	cache.Update("key1", &cacheEntry{latestUpdateAt: 100, kind: schema.KindStream})
	cache.Update("key2", &cacheEntry{latestUpdateAt: 200, kind: schema.KindMeasure})

	entries = cache.GetAllEntries()
	assert.Len(t, entries, 2)
	assert.Contains(t, entries, "key1")
	assert.Contains(t, entries, "key2")

	entries["key3"] = &cacheEntry{}
	originalEntries := cache.GetAllEntries()
	assert.Len(t, originalEntries, 2)
}

func TestSchemaCache_Concurrency(t *testing.T) {
	cache := newSchemaCache()
	var wg sync.WaitGroup
	numGoroutines := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				entry := &cacheEntry{
					latestUpdateAt: int64(idx*1000 + j),
					kind:           schema.KindStream,
				}
				cache.Update("concurrent-key", entry)
				cache.GetMaxRevision()
				cache.GetAllEntries()
			}
		}(i)
	}
	wg.Wait()

	assert.True(t, cache.GetMaxRevision() > 0)
}

func TestSchemaCache_GetEntriesByKind(t *testing.T) {
	cache := newSchemaCache()

	entries := cache.GetEntriesByKind(schema.KindStream)
	assert.Empty(t, entries)

	cache.Update("stream_group1/stream1", &cacheEntry{latestUpdateAt: 100, kind: schema.KindStream, group: "group1", name: "stream1"})
	cache.Update("stream_group1/stream2", &cacheEntry{latestUpdateAt: 101, kind: schema.KindStream, group: "group1", name: "stream2"})
	cache.Update("measure_group1/measure1", &cacheEntry{latestUpdateAt: 102, kind: schema.KindMeasure, group: "group1", name: "measure1"})
	cache.Update("group_group1", &cacheEntry{latestUpdateAt: 103, kind: schema.KindGroup, group: "", name: "group1"})

	streamEntries := cache.GetEntriesByKind(schema.KindStream)
	assert.Len(t, streamEntries, 2)
	assert.Contains(t, streamEntries, "stream_group1/stream1")
	assert.Contains(t, streamEntries, "stream_group1/stream2")

	measureEntries := cache.GetEntriesByKind(schema.KindMeasure)
	assert.Len(t, measureEntries, 1)
	assert.Contains(t, measureEntries, "measure_group1/measure1")

	groupEntries := cache.GetEntriesByKind(schema.KindGroup)
	assert.Len(t, groupEntries, 1)
	assert.Contains(t, groupEntries, "group_group1")

	traceEntries := cache.GetEntriesByKind(schema.KindTrace)
	assert.Empty(t, traceEntries)

	streamEntries["new_key"] = &cacheEntry{}
	originalStreamEntries := cache.GetEntriesByKind(schema.KindStream)
	assert.Len(t, originalStreamEntries, 2)
}
