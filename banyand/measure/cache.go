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

package measure

import (
	"sync"
	"sync/atomic"
	"time"
)

const (
	maxCachedBlocks   = 100
	cleanupPercentage = 0.1
	cleanupInterval   = 30 * time.Second
	accessTimeout     = 2 * time.Minute
)

type entry struct {
	pbm        []*primaryBlockMetadata
	lastAccess uint64
}

type cache struct {
	m             map[uint64]*entry
	cleanerStopCh chan struct{}
	requests      uint64
	misses        uint64
	mu            sync.RWMutex
	cleanerWG     sync.WaitGroup
}

func newCache() *cache {
	c := &cache{
		m:             make(map[uint64]*entry),
		cleanerStopCh: make(chan struct{}),
		cleanerWG:     sync.WaitGroup{},
	}
	c.cleanerWG.Add(1)
	go func() {
		defer c.cleanerWG.Done()
		c.clean()
	}()
	return c
}

func (c *cache) clean() {
	ticker := time.NewTicker(cleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			currentTime := uint64(time.Now().UnixNano())
			c.mu.Lock()
			for k, e := range c.m {
				if currentTime-atomic.LoadUint64(&e.lastAccess) > uint64(accessTimeout.Nanoseconds()) {
					delete(c.m, k)
				}
			}
			c.mu.Unlock()
		case <-c.cleanerStopCh:
			return
		}
	}
}

func (c *cache) close() {
	close(c.cleanerStopCh)
	c.cleanerWG.Wait()
	c.m = nil
}

func (c *cache) get(partID uint64) []primaryBlockMetadata {
	atomic.AddUint64(&c.requests, 1)

	c.mu.RLock()
	e := c.m[partID]
	c.mu.RUnlock()

	if e != nil {
		if atomic.LoadUint64(&e.lastAccess) != uint64(time.Now().UnixNano()) {
			atomic.StoreUint64(&e.lastAccess, uint64(time.Now().UnixNano()))
		}
		pbm := make([]primaryBlockMetadata, 0)
		for _, m := range e.pbm {
			pbm = append(pbm, *m)
		}
		return pbm
	}

	atomic.AddUint64(&c.misses, 1)
	return nil
}

func (c *cache) put(partID uint64, pbm []primaryBlockMetadata) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if overflow := len(c.m) - maxCachedBlocks; overflow > 0 {
		overflow = int(float64(len(c.m)) * cleanupPercentage)
		for partID := range c.m {
			delete(c.m, partID)
			overflow--
			if overflow == 0 {
				break
			}
		}
	}

	pbmPtrs := make([]*primaryBlockMetadata, 0)
	for _, m := range pbm {
		pbmPtrs = append(pbmPtrs, &m)
	}
	e := &entry{
		pbm: pbmPtrs,
	}
	atomic.StoreUint64(&e.lastAccess, uint64(time.Now().UnixNano()))
	c.m[partID] = e
}

func (c *cache) delete(partID uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.m, partID)
}

func (c *cache) Requests() uint64 {
	return atomic.LoadUint64(&c.requests)
}

func (c *cache) Misses() uint64 {
	return atomic.LoadUint64(&c.misses)
}

func (c *cache) Len() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return uint64(len(c.m))
}

func (c *cache) SizeBytes() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	n := uint64(8)
	for _, e := range c.m {
		n += uint64(len(e.pbm)) * 8
	}
	return n
}
