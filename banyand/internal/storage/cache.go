// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package storage

import (
	"container/heap"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/apache/skywalking-banyandb/api/common"
)

type entry struct {
	compressedPrimaryBuf []byte
	lastAccess           uint64
}

// EntryKey is the key of an entry in the cache.
type EntryKey struct {
	group     string
	PartID    uint64
	Offset    uint64
	segmentID segmentID
	shardID   common.ShardID
}

type entryIndex struct {
	*entry
	key   EntryKey
	index int
}

type entryIndexHeap []*entryIndex

func (h entryIndexHeap) Len() int { return len(h) }

func (h entryIndexHeap) Less(i, j int) bool {
	return atomic.LoadUint64(&h[i].entry.lastAccess) < atomic.LoadUint64(&h[j].entry.lastAccess)
}

func (h entryIndexHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *entryIndexHeap) Push(x interface{}) {
	n := len(*h)
	ei := x.(*entryIndex)
	ei.index = n
	*h = append(*h, ei)
}

func (h *entryIndexHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// Cache stores the compressed primary blocks.
type Cache struct {
	entry           map[EntryKey]*entry
	entryIndex      map[EntryKey]*entryIndex
	entryIndexHeap  *entryIndexHeap
	cleanerStopCh   chan struct{}
	requests        uint64
	misses          uint64
	mu              sync.RWMutex
	cleanerWG       sync.WaitGroup
	maxCacheSize    uint64
	cleanupInterval time.Duration
	idleTimeout     time.Duration
}

// NewCache creates a cache.
func NewCache() *Cache {
	h := &entryIndexHeap{}
	heap.Init(h)
	c := &Cache{
		entry:           make(map[EntryKey]*entry),
		entryIndexHeap:  h,
		entryIndex:      make(map[EntryKey]*entryIndex),
		cleanerStopCh:   make(chan struct{}),
		cleanerWG:       sync.WaitGroup{},
		maxCacheSize:    100 * 1024 * 1024,
		cleanupInterval: 30 * time.Second,
		idleTimeout:     2 * time.Minute,
	}
	c.cleanerWG.Add(1)
	return c
}

// Clean periodically cleans the cache.
func (c *Cache) Clean() {
	go func() {
		defer c.cleanerWG.Done()
		c.clean()
	}()
}

func (c *Cache) clean() {
	ticker := time.NewTicker(c.cleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			now := uint64(time.Now().UnixNano())
			c.mu.Lock()
			for key, entry := range c.entry {
				if now-atomic.LoadUint64(&entry.lastAccess) > uint64(c.idleTimeout.Nanoseconds()) {
					delete(c.entry, key)
					heap.Remove(c.entryIndexHeap, c.entryIndex[key].index)
				}
			}
			c.mu.Unlock()
		case <-c.cleanerStopCh:
			return
		}
	}
}

// Close closes the cache.
func (c *Cache) Close() {
	close(c.cleanerStopCh)
	c.cleanerWG.Wait()
	c.entry = nil
	c.entryIndex = nil
	c.entryIndexHeap = nil
}

// Get gets the compressed primary block from the cache.
func (c *Cache) Get(key EntryKey) []byte {
	atomic.AddUint64(&c.requests, 1)

	c.mu.RLock()
	entry := c.entry[key]
	c.mu.RUnlock()

	if entry != nil {
		now := uint64(time.Now().UnixNano())
		if atomic.LoadUint64(&entry.lastAccess) != now {
			c.mu.Lock()
			atomic.StoreUint64(&entry.lastAccess, now)
			if ei := c.entryIndex[key]; ei != nil {
				heap.Fix(c.entryIndexHeap, ei.index)
			}
			c.mu.Unlock()
		}
		return entry.compressedPrimaryBuf
	}

	atomic.AddUint64(&c.misses, 1)
	return nil
}

// Put puts the compressed primary block into the cache.
func (c *Cache) Put(key EntryKey, compressedPrimaryBuf []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for c.size() > c.maxCacheSize && c.len() > 0 {
		ei := heap.Pop(c.entryIndexHeap).(*entryIndex)
		delete(c.entry, ei.key)
		delete(c.entryIndex, ei.key)
	}

	now := uint64(time.Now().UnixNano())
	e := &entry{
		compressedPrimaryBuf: compressedPrimaryBuf,
		lastAccess:           now,
	}
	ei := &entryIndex{
		key:   key,
		entry: e,
	}
	c.entry[key] = e
	c.entryIndex[key] = ei
	heap.Push(c.entryIndexHeap, ei)
}

// Requests returns the number of cache requests.
func (c *Cache) Requests() uint64 {
	return atomic.LoadUint64(&c.requests)
}

// Misses returns the number of cache misses.
func (c *Cache) Misses() uint64 {
	return atomic.LoadUint64(&c.misses)
}

// Len returns the number of entries in the cache.
func (c *Cache) Len() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.len()
}

func (c *Cache) len() uint64 {
	return uint64(len(c.entry))
}

// Size returns the size of the cache.
func (c *Cache) Size() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.size()
}

func (c *Cache) size() uint64 {
	return uint64(unsafe.Sizeof(c.entry))
}
