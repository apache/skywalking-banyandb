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
//
package bucket

import (
	"errors"
	"sync"

	"github.com/hashicorp/golang-lru/simplelru"
)

type EvictFn func(id interface{})

type Queue interface {
	Push(id interface{})
	Len() int
	All() []interface{}
}

const (
	DefaultRecentRatio  = 0.25
	DefaultGhostEntries = 0.50
)

var ErrInvalidSize = errors.New("invalid size")

type lruQueue struct {
	size       int
	recentSize int
	evictSize  int
	evictFn    EvictFn

	recent      simplelru.LRUCache
	frequent    simplelru.LRUCache
	recentEvict simplelru.LRUCache
	lock        sync.RWMutex
}

func NewQueue(size int, evictFn EvictFn) (Queue, error) {
	if size <= 0 {
		return nil, ErrInvalidSize
	}

	recentSize := int(float64(size) * DefaultRecentRatio)
	evictSize := int(float64(size) * DefaultGhostEntries)

	recent, err := simplelru.NewLRU(size, nil)
	if err != nil {
		return nil, err
	}
	frequent, err := simplelru.NewLRU(size, nil)
	if err != nil {
		return nil, err
	}
	recentEvict, err := simplelru.NewLRU(evictSize, nil)
	if err != nil {
		return nil, err
	}
	c := &lruQueue{
		size:        size,
		recentSize:  recentSize,
		recent:      recent,
		frequent:    frequent,
		recentEvict: recentEvict,
		evictSize:   evictSize,
		evictFn:     evictFn,
	}
	return c, nil
}

func (q *lruQueue) Push(id interface{}) {
	q.lock.Lock()
	defer q.lock.Unlock()

	if q.frequent.Contains(id) {
		q.frequent.Add(id, nil)
		return
	}

	if q.recent.Contains(id) {
		q.recent.Remove(id)
		q.frequent.Add(id, nil)
		return
	}

	if q.recentEvict.Contains(id) {
		q.ensureSpace(true)
		q.recentEvict.Remove(id)
		q.frequent.Add(id, nil)
		return
	}

	q.ensureSpace(false)
	q.recent.Add(id, nil)
}

func (q *lruQueue) Len() int {
	q.lock.RLock()
	defer q.lock.RUnlock()
	return q.recent.Len() + q.frequent.Len()
}

func (q *lruQueue) All() []interface{} {
	q.lock.RLock()
	defer q.lock.RUnlock()
	all := make([]interface{}, q.recent.Len()+q.frequent.Len()+q.recentEvict.Len())
	copy(all, q.recent.Keys())
	copy(all[q.recent.Len():], q.frequent.Keys())
	copy(all[q.recent.Len()+q.frequent.Len():], q.recentEvict.Keys())
	return all
}

func (q *lruQueue) ensureSpace(recentEvict bool) {
	recentLen := q.recent.Len()
	freqLen := q.frequent.Len()
	if recentLen+freqLen < q.size {
		return
	}
	if recentLen > 0 && (recentLen > q.recentSize || (recentLen == q.recentSize && !recentEvict)) {
		k, _, _ := q.recent.RemoveOldest()
		q.addLst(q.recentEvict, q.evictSize, k)
		return
	}
	q.removeOldest(q.frequent)
}

func (q *lruQueue) addLst(lst simplelru.LRUCache, size int, id interface{}) {
	if lst.Len() < size {
		lst.Add(id, nil)
		return
	}
	q.removeOldest(lst)
	lst.Add(id, nil)
}

func (q *lruQueue) removeOldest(lst simplelru.LRUCache) {
	oldestID, _, ok := lst.RemoveOldest()
	if ok && q.evictFn != nil {
		q.evictFn(oldestID)
	}
}
