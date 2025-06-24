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
	"github.com/apache/skywalking-banyandb/pkg/run"
)

// Cache encapsulates the cache operations.
type Cache interface {
	Get(key EntryKey) any
	Put(key EntryKey, value any)
	StartCleaner()
	Close()
	Requests() uint64
	Misses() uint64
	Entries() uint64
	Size() uint64
}

// CacheConfig holds configuration parameters for the cache.
type CacheConfig struct {
	MaxCacheSize    run.Bytes
	CleanupInterval time.Duration
	IdleTimeout     time.Duration
}

// DefaultCacheConfig returns the default cache configuration.
func DefaultCacheConfig() CacheConfig {
	return CacheConfig{
		MaxCacheSize:    run.Bytes(100 * 1024 * 1024),
		CleanupInterval: 30 * time.Second,
		IdleTimeout:     2 * time.Minute,
	}
}

type entry struct {
	value      any
	lastAccess uint64
}

// EntryKey is the key of an entry in the cache.
type EntryKey struct {
	group     string
	partID    uint64
	offset    uint64
	segmentID segmentID
	shardID   common.ShardID
}

// NewEntryKey creates an entry key with partID and offset.
func NewEntryKey(partID uint64, offset uint64) EntryKey {
	return EntryKey{
		partID: partID,
		offset: offset,
	}
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

var _ Cache = (*serviceCache)(nil)

type serviceCache struct {
	entry           map[EntryKey]*entry
	entryIndex      map[EntryKey]*entryIndex
	entryIndexHeap  *entryIndexHeap
	stopCh          chan struct{}
	requests        uint64
	misses          uint64
	mu              sync.RWMutex
	wg              sync.WaitGroup
	maxCacheSize    uint64
	cleanupInterval time.Duration
	idleTimeout     time.Duration
}

// NewServiceCache creates a cache for service with default configuration.
func NewServiceCache() Cache {
	return NewServiceCacheWithConfig(DefaultCacheConfig())
}

// NewServiceCacheWithConfig creates a cache for service with custom configuration.
func NewServiceCacheWithConfig(config CacheConfig) Cache {
	h := &entryIndexHeap{}
	heap.Init(h)
	sc := &serviceCache{
		entry:           make(map[EntryKey]*entry),
		entryIndexHeap:  h,
		entryIndex:      make(map[EntryKey]*entryIndex),
		stopCh:          make(chan struct{}),
		wg:              sync.WaitGroup{},
		maxCacheSize:    uint64(config.MaxCacheSize),
		cleanupInterval: config.CleanupInterval,
		idleTimeout:     config.IdleTimeout,
	}
	sc.wg.Add(1)
	sc.StartCleaner()
	return sc
}

func (sc *serviceCache) StartCleaner() {
	go func() {
		defer sc.wg.Done()
		sc.clean()
	}()
}

func (sc *serviceCache) clean() {
	ticker := time.NewTicker(sc.cleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			now := uint64(time.Now().UnixNano())
			sc.mu.Lock()
			for key, entry := range sc.entry {
				if now-atomic.LoadUint64(&entry.lastAccess) > uint64(sc.idleTimeout.Nanoseconds()) {
					delete(sc.entry, key)
					heap.Remove(sc.entryIndexHeap, sc.entryIndex[key].index)
				}
			}
			sc.mu.Unlock()
		case <-sc.stopCh:
			return
		}
	}
}

func (sc *serviceCache) Close() {
	close(sc.stopCh)
	sc.wg.Wait()
	sc.entry = nil
	sc.entryIndex = nil
	sc.entryIndexHeap = nil
}

func (sc *serviceCache) Get(key EntryKey) any {
	atomic.AddUint64(&sc.requests, 1)

	sc.mu.RLock()
	entry := sc.entry[key]
	sc.mu.RUnlock()

	if entry != nil {
		now := uint64(time.Now().UnixNano())
		if atomic.LoadUint64(&entry.lastAccess) != now {
			sc.mu.Lock()
			atomic.StoreUint64(&entry.lastAccess, now)
			if ei := sc.entryIndex[key]; ei != nil {
				heap.Fix(sc.entryIndexHeap, ei.index)
			}
			sc.mu.Unlock()
		}
		return entry.value
	}

	atomic.AddUint64(&sc.misses, 1)
	return nil
}

func (sc *serviceCache) Put(key EntryKey, value any) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	for sc.size() > sc.maxCacheSize && sc.len() > 0 {
		ei := heap.Pop(sc.entryIndexHeap).(*entryIndex)
		delete(sc.entry, ei.key)
		delete(sc.entryIndex, ei.key)
	}

	now := uint64(time.Now().UnixNano())
	e := &entry{
		value:      value,
		lastAccess: now,
	}
	ei := &entryIndex{
		key:   key,
		entry: e,
	}
	sc.entry[key] = e
	sc.entryIndex[key] = ei
	heap.Push(sc.entryIndexHeap, ei)
}

func (sc *serviceCache) Requests() uint64 {
	return atomic.LoadUint64(&sc.requests)
}

func (sc *serviceCache) Misses() uint64 {
	return atomic.LoadUint64(&sc.misses)
}

func (sc *serviceCache) Entries() uint64 {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	return sc.len()
}

func (sc *serviceCache) len() uint64 {
	return uint64(len(sc.entry))
}

func (sc *serviceCache) Size() uint64 {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	return sc.size()
}

func (sc *serviceCache) size() uint64 {
	return uint64(unsafe.Sizeof(*sc))
}
