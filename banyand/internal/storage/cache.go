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
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

// Sizable represents an object that can report its memory size.
type Sizable interface {
	Size() uint64
}

// Cache encapsulates the cache operations.
type Cache interface {
	Get(key EntryKey) Sizable
	Put(key EntryKey, value Sizable)
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
	value      Sizable
	lastAccess uint64
	size       uint64
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

func (h *entryIndexHeap) Push(x any) {
	n := len(*h)
	ei := x.(*entryIndex)
	ei.index = n
	*h = append(*h, ei)
}

func (h *entryIndexHeap) Pop() any {
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
	currentSize     uint64
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
		currentSize:     0,
	}
	sc.wg.Add(1)
	sc.startCleaner()
	return sc
}

func (sc *serviceCache) startCleaner() {
	go func() {
		defer sc.wg.Done()
		sc.clean()
	}()
}

func (sc *serviceCache) removeEntry(key EntryKey) {
	if entry, exists := sc.entry[key]; exists {
		sc.atomicSubtract(entry.size)
		delete(sc.entry, key)
	}
	if ei, exists := sc.entryIndex[key]; exists && ei.index >= 0 && ei.index < sc.entryIndexHeap.Len() {
		heap.Remove(sc.entryIndexHeap, ei.index)
		delete(sc.entryIndex, key)
	}
}

func (sc *serviceCache) clean() {
	ticker := time.NewTicker(sc.cleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			now := uint64(time.Now().UnixNano())
			sc.mu.Lock()
			// Collect keys to remove to avoid modifying map during iteration
			var keysToRemove []EntryKey
			for key, entry := range sc.entry {
				if now-atomic.LoadUint64(&entry.lastAccess) > uint64(sc.idleTimeout.Nanoseconds()) {
					keysToRemove = append(keysToRemove, key)
				}
			}
			// Remove expired entries
			for _, key := range keysToRemove {
				sc.removeEntry(key)
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

func (sc *serviceCache) Get(key EntryKey) Sizable {
	atomic.AddUint64(&sc.requests, 1)

	sc.mu.RLock()
	entry := sc.entry[key]
	ei := sc.entryIndex[key]
	sc.mu.RUnlock()

	if entry != nil {
		now := uint64(time.Now().UnixNano())
		if atomic.LoadUint64(&entry.lastAccess) != now {
			sc.mu.Lock()
			// Verify entry still exists and update access time
			if currentEntry := sc.entry[key]; currentEntry == entry {
				atomic.StoreUint64(&entry.lastAccess, now)
				if ei != nil && ei.index >= 0 && ei.index < sc.entryIndexHeap.Len() {
					heap.Fix(sc.entryIndexHeap, ei.index)
				}
			}
			sc.mu.Unlock()
		}
		return entry.value
	}

	atomic.AddUint64(&sc.misses, 1)
	return nil
}

func (sc *serviceCache) Put(key EntryKey, value Sizable) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	valueSize := value.Size()
	entryOverhead := uint64(unsafe.Sizeof(entry{}) + unsafe.Sizeof(entryIndex{}) + unsafe.Sizeof(key))
	totalSize := valueSize + entryOverhead
	if totalSize > sc.maxCacheSize {
		logger.Warningf("value size exceeds max cache size: %d > %d: overhead %d", totalSize, sc.maxCacheSize, entryOverhead)
		return
	}

	// Remove existing entry if present
	if existing, exists := sc.entry[key]; exists {
		sc.atomicSubtract(existing.size)
		delete(sc.entry, key)
		if ei, exists := sc.entryIndex[key]; exists {
			if ei.index >= 0 && ei.index < sc.entryIndexHeap.Len() {
				heap.Remove(sc.entryIndexHeap, ei.index)
			}
			delete(sc.entryIndex, key)
		}
	}

	// Evict entries until there's space
	for atomic.LoadUint64(&sc.currentSize)+totalSize > sc.maxCacheSize && sc.entryIndexHeap.Len() > 0 {
		ei := heap.Pop(sc.entryIndexHeap).(*entryIndex)
		if entry, exists := sc.entry[ei.key]; exists {
			sc.atomicSubtract(entry.size)
			delete(sc.entry, ei.key)
		}
		delete(sc.entryIndex, ei.key)
	}

	now := uint64(time.Now().UnixNano())
	e := &entry{
		value:      value,
		lastAccess: now,
		size:       totalSize,
	}
	ei := &entryIndex{
		key:   key,
		entry: e,
	}
	sc.entry[key] = e
	sc.entryIndex[key] = ei
	heap.Push(sc.entryIndexHeap, ei)
	atomic.AddUint64(&sc.currentSize, totalSize)
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
	return sc.size()
}

func (sc *serviceCache) size() uint64 {
	return atomic.LoadUint64(&sc.currentSize)
}

// atomicSubtract safely subtracts a value from currentSize.
func (sc *serviceCache) atomicSubtract(size uint64) {
	for {
		current := atomic.LoadUint64(&sc.currentSize)
		var newSize uint64
		if current >= size {
			newSize = current - size
		} else {
			newSize = 0
		}
		if atomic.CompareAndSwapUint64(&sc.currentSize, current, newSize) {
			break
		}
	}
}

var _ Cache = (*bypassCache)(nil)

type bypassCache struct{}

// NewBypassCache creates a no-op cache implementation.
func NewBypassCache() Cache {
	return &bypassCache{}
}

func (bc *bypassCache) Get(_ EntryKey) Sizable {
	return nil
}

func (bc *bypassCache) Put(_ EntryKey, _ Sizable) {
}

func (bc *bypassCache) Close() {
}

func (bc *bypassCache) Requests() uint64 {
	return 0
}

func (bc *bypassCache) Misses() uint64 {
	return 0
}

func (bc *bypassCache) Entries() uint64 {
	return 0
}

func (bc *bypassCache) Size() uint64 {
	return 0
}
