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
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
)

func TestNewSchemaCache(t *testing.T) {
	c := newSchemaCache()
	require.NotNil(t, c)
	assert.Equal(t, 0, c.Size())
	assert.Equal(t, int64(0), c.GetMaxRevision())
}

func TestSchemaCache_Update(t *testing.T) {
	c := newSchemaCache()
	changed := c.Update("stream_g1/s1", &cacheEntry{
		group: "g1", name: "s1", latestUpdateAt: 10, kind: schema.KindStream,
	})
	assert.True(t, changed)
	assert.Equal(t, 1, c.Size())
	assert.Equal(t, int64(10), c.GetMaxRevision())

	// Update with higher revision returns true.
	changed = c.Update("stream_g1/s1", &cacheEntry{
		group: "g1", name: "s1", latestUpdateAt: 20, kind: schema.KindStream,
	})
	assert.True(t, changed)
	assert.Equal(t, int64(20), c.GetMaxRevision())

	// Update with same revision returns false.
	changed = c.Update("stream_g1/s1", &cacheEntry{
		group: "g1", name: "s1", latestUpdateAt: 20, kind: schema.KindStream,
	})
	assert.False(t, changed)

	// Update with lower revision returns false.
	changed = c.Update("stream_g1/s1", &cacheEntry{
		group: "g1", name: "s1", latestUpdateAt: 5, kind: schema.KindStream,
	})
	assert.False(t, changed)
	assert.Equal(t, int64(20), c.GetMaxRevision())
}

func TestSchemaCache_Delete(t *testing.T) {
	c := newSchemaCache()
	c.Update("stream_g1/s1", &cacheEntry{
		group: "g1", name: "s1", latestUpdateAt: 10, kind: schema.KindStream,
	})
	c.Update("stream_g1/s2", &cacheEntry{
		group: "g1", name: "s2", latestUpdateAt: 20, kind: schema.KindStream,
	})
	assert.Equal(t, 2, c.Size())

	// Delete with sufficient revision.
	deleted := c.Delete("stream_g1/s1", 10)
	assert.True(t, deleted)
	assert.Equal(t, 1, c.Size())

	// Delete does not exceed existing max revision.
	assert.Equal(t, int64(20), c.GetMaxRevision())

	// Delete with lower revision fails.
	deleted = c.Delete("stream_g1/s2", 5)
	assert.False(t, deleted)
	assert.Equal(t, 1, c.Size())

	// Delete nonexistent key returns false.
	deleted = c.Delete("nonexistent", 100)
	assert.False(t, deleted)

	// Delete with revision higher than current max advances it.
	c.Update("stream_g1/s3", &cacheEntry{
		group: "g1", name: "s3", latestUpdateAt: 15, kind: schema.KindStream,
	})
	deleted = c.Delete("stream_g1/s3", 25)
	assert.True(t, deleted)
	assert.Equal(t, int64(25), c.GetMaxRevision())
}

func TestSchemaCache_GetMaxRevision(t *testing.T) {
	c := newSchemaCache()
	assert.Equal(t, int64(0), c.GetMaxRevision())
	c.Update("stream_g1/n1", &cacheEntry{group: "g1", name: "n1", latestUpdateAt: 10, kind: schema.KindStream})
	assert.Equal(t, int64(10), c.GetMaxRevision())
	c.Update("stream_g1/n2", &cacheEntry{group: "g1", name: "n2", latestUpdateAt: 30, kind: schema.KindStream})
	assert.Equal(t, int64(30), c.GetMaxRevision())
	c.Update("stream_g1/n3", &cacheEntry{group: "g1", name: "n3", latestUpdateAt: 20, kind: schema.KindStream})
	assert.Equal(t, int64(30), c.GetMaxRevision())
}

func TestSchemaCache_GetAllEntries(t *testing.T) {
	c := newSchemaCache()
	all := c.GetAllEntries()
	assert.Empty(t, all)

	c.Update("stream_g1/s1", &cacheEntry{group: "g1", name: "s1", latestUpdateAt: 10, kind: schema.KindStream})
	c.Update("measure_g1/m1", &cacheEntry{group: "g1", name: "m1", latestUpdateAt: 20, kind: schema.KindMeasure})
	all = c.GetAllEntries()
	assert.Len(t, all, 2)
	assert.Equal(t, "s1", all["stream_g1/s1"].name)
	assert.Equal(t, "m1", all["measure_g1/m1"].name)

	// Mutating the snapshot does not affect the original cache.
	all["extra"] = &cacheEntry{group: "g2", name: "extra", latestUpdateAt: 30, kind: schema.KindStream}
	assert.Equal(t, 2, c.Size())
}

func TestSchemaCache_GetEntriesByKind(t *testing.T) {
	c := newSchemaCache()
	c.Update("stream_g1/s1", &cacheEntry{group: "g1", name: "s1", latestUpdateAt: 10, kind: schema.KindStream})
	c.Update("stream_g1/s2", &cacheEntry{group: "g1", name: "s2", latestUpdateAt: 20, kind: schema.KindStream})
	c.Update("measure_g1/m1", &cacheEntry{group: "g1", name: "m1", latestUpdateAt: 30, kind: schema.KindMeasure})

	streams := c.GetEntriesByKind(schema.KindStream)
	assert.Len(t, streams, 2)

	measures := c.GetEntriesByKind(schema.KindMeasure)
	assert.Len(t, measures, 1)
	assert.Equal(t, "m1", measures["measure_g1/m1"].name)

	traces := c.GetEntriesByKind(schema.KindTrace)
	assert.Empty(t, traces)
}

func TestSchemaCache_GetCachedKinds(t *testing.T) {
	c := newSchemaCache()
	assert.Empty(t, c.GetCachedKinds())

	c.Update("stream_g1/s1", &cacheEntry{group: "g1", name: "s1", latestUpdateAt: 10, kind: schema.KindStream})
	c.Update("measure_g1/m1", &cacheEntry{group: "g1", name: "m1", latestUpdateAt: 20, kind: schema.KindMeasure})
	c.Update("stream_g1/s2", &cacheEntry{group: "g1", name: "s2", latestUpdateAt: 30, kind: schema.KindStream})

	kinds := c.GetCachedKinds()
	kindInts := make([]int, len(kinds))
	for idx, k := range kinds {
		kindInts[idx] = int(k)
	}
	sort.Ints(kindInts)
	assert.Len(t, kindInts, 2)
	assert.Equal(t, int(schema.KindStream), kindInts[0])
	assert.Equal(t, int(schema.KindMeasure), kindInts[1])
}

func TestSchemaCache_Size(t *testing.T) {
	c := newSchemaCache()
	assert.Equal(t, 0, c.Size())
	c.Update("stream_g1/n1", &cacheEntry{group: "g1", name: "n1", latestUpdateAt: 10, kind: schema.KindStream})
	assert.Equal(t, 1, c.Size())
	c.Update("stream_g1/n2", &cacheEntry{group: "g1", name: "n2", latestUpdateAt: 20, kind: schema.KindStream})
	assert.Equal(t, 2, c.Size())
	c.Delete("stream_g1/n1", 10)
	assert.Equal(t, 1, c.Size())
}

func TestSchemaCache_UpdateAddsToKind(t *testing.T) {
	c := newSchemaCache()
	c.Update("stream_g1/s1", &cacheEntry{group: "g1", name: "s1", latestUpdateAt: 10, kind: schema.KindStream})
	c.Update("measure_g1/m1", &cacheEntry{group: "g1", name: "m1", latestUpdateAt: 20, kind: schema.KindMeasure})
	streams := c.GetEntriesByKind(schema.KindStream)
	assert.Len(t, streams, 1)
	assert.Contains(t, streams, "stream_g1/s1")
	measures := c.GetEntriesByKind(schema.KindMeasure)
	assert.Len(t, measures, 1)
	assert.Contains(t, measures, "measure_g1/m1")
}

func TestSchemaCache_DeleteRemovesFromKind(t *testing.T) {
	c := newSchemaCache()
	c.Update("stream_g1/s1", &cacheEntry{group: "g1", name: "s1", latestUpdateAt: 10, kind: schema.KindStream})
	c.Update("stream_g1/s2", &cacheEntry{group: "g1", name: "s2", latestUpdateAt: 20, kind: schema.KindStream})
	assert.Len(t, c.GetEntriesByKind(schema.KindStream), 2)
	c.Delete("stream_g1/s1", 10)
	streams := c.GetEntriesByKind(schema.KindStream)
	assert.Len(t, streams, 1)
	assert.Contains(t, streams, "stream_g1/s2")
}

func TestSchemaCache_DeleteLastEntryRemovesKind(t *testing.T) {
	c := newSchemaCache()
	c.Update("stream_g1/s1", &cacheEntry{group: "g1", name: "s1", latestUpdateAt: 10, kind: schema.KindStream})
	assert.Len(t, c.GetCachedKinds(), 1)
	c.Delete("stream_g1/s1", 10)
	assert.Empty(t, c.GetCachedKinds())
	assert.Empty(t, c.GetEntriesByKind(schema.KindStream))
}

func TestSchemaCache_GetCachedKindsConsistent(t *testing.T) {
	c := newSchemaCache()
	c.Update("stream_g1/s1", &cacheEntry{group: "g1", name: "s1", latestUpdateAt: 10, kind: schema.KindStream})
	c.Update("measure_g1/m1", &cacheEntry{group: "g1", name: "m1", latestUpdateAt: 20, kind: schema.KindMeasure})
	c.Update("trace_g1/t1", &cacheEntry{group: "g1", name: "t1", latestUpdateAt: 30, kind: schema.KindTrace})
	kinds := c.GetCachedKinds()
	assert.Len(t, kinds, 3)
	kindSet := make(map[schema.Kind]struct{})
	for _, k := range kinds {
		kindSet[k] = struct{}{}
	}
	assert.Contains(t, kindSet, schema.KindStream)
	assert.Contains(t, kindSet, schema.KindMeasure)
	assert.Contains(t, kindSet, schema.KindTrace)
	// Delete all measure entries.
	c.Delete("measure_g1/m1", 20)
	kinds = c.GetCachedKinds()
	assert.Len(t, kinds, 2)
	kindSet = make(map[schema.Kind]struct{})
	for _, k := range kinds {
		kindSet[k] = struct{}{}
	}
	assert.NotContains(t, kindSet, schema.KindMeasure)
}
