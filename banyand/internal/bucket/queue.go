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

package bucket

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/hashicorp/golang-lru/v2/simplelru"
	"github.com/robfig/cron/v3"

	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

type (
	// EvictFn is a closure executed on evicting an item.
	EvictFn func(ctx context.Context, id interface{}) error
	// OnAddRecentFn is a notifier on adding an item into the recent queue.
	OnAddRecentFn func() error
)

// Queue is a LRU queue.
type Queue interface {
	Touch(id fmt.Stringer) bool
	Push(ctx context.Context, id fmt.Stringer, fn OnAddRecentFn) error
	Remove(id fmt.Stringer)
	Len() int
	Volume() int
	All() []fmt.Stringer
}

const (
	// QueueName is identity of the queue.
	QueueName = "block-queue-cleanup"

	defaultRecentRatio    = 0.25
	defaultEvictBatchSize = 10
)

var errInvalidSize = errors.New("invalid size")

type lruQueue struct {
	recent      simplelru.LRUCache[fmt.Stringer, any]
	frequent    simplelru.LRUCache[fmt.Stringer, any]
	recentEvict simplelru.LRUCache[fmt.Stringer, any]
	l           *logger.Logger
	evictFn     EvictFn
	size        int
	recentSize  int
	evictSize   int
	lock        sync.RWMutex
}

// NewQueue return a Queue for blocks eviction.
func NewQueue(l *logger.Logger, size int, maxSize int, scheduler *timestamp.Scheduler, evictFn EvictFn) (Queue, error) {
	if size <= 0 {
		return nil, errInvalidSize
	}

	recentSize := int(float64(size) * defaultRecentRatio)
	evictSize := maxSize - size

	recent, err := simplelru.NewLRU[fmt.Stringer, any](size, nil)
	if err != nil {
		return nil, err
	}
	frequent, err := simplelru.NewLRU[fmt.Stringer, any](size, nil)
	if err != nil {
		return nil, err
	}
	recentEvict, err := simplelru.NewLRU[fmt.Stringer, any](evictSize, nil)
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
		l:           l,
	}
	if err := scheduler.Register(QueueName, cron.Descriptor, "@every 5m", c.cleanEvict); err != nil {
		return nil, err
	}
	return c, nil
}

func (q *lruQueue) Touch(id fmt.Stringer) bool {
	q.lock.Lock()
	defer q.lock.Unlock()

	if q.frequent.Contains(id) {
		if e := q.l.Debug(); e.Enabled() {
			e.Stringer("id", id).Msg("get from frequent")
		}
		return true
	}

	if q.recent.Contains(id) {
		if e := q.l.Debug(); e.Enabled() {
			e.Stringer("id", id).Msg("promote from recent to frequent")
		}
		q.recent.Remove(id)
		q.frequent.Add(id, nil)
		return true
	}
	return false
}

func (q *lruQueue) Push(ctx context.Context, id fmt.Stringer, fn OnAddRecentFn) error {
	q.lock.Lock()
	defer q.lock.Unlock()

	if q.frequent.Contains(id) {
		if e := q.l.Debug(); e.Enabled() {
			e.Stringer("id", id).Msg("push to frequent")
		}
		q.frequent.Add(id, nil)
		return nil
	}

	if q.recent.Contains(id) {
		if e := q.l.Debug(); e.Enabled() {
			e.Stringer("id", id).Msg("promote from recent to frequent")
		}
		q.recent.Remove(id)
		q.frequent.Add(id, nil)
		return nil
	}

	if q.recentEvict.Contains(id) {
		if e := q.l.Debug(); e.Enabled() {
			e.Stringer("id", id).Msg("restore from recentEvict")
		}
		if err := q.ensureSpace(ctx, true); err != nil {
			return err
		}
		q.recentEvict.Remove(id)
		q.frequent.Add(id, nil)
		return nil
	}

	if err := q.ensureSpace(ctx, false); err != nil {
		return err
	}
	q.recent.Add(id, nil)
	if fn == nil {
		return nil
	}
	return fn()
}

func (q *lruQueue) Remove(id fmt.Stringer) {
	q.lock.Lock()
	defer q.lock.Unlock()

	if q.frequent.Contains(id) {
		q.frequent.Remove(id)
		return
	}

	if q.recent.Contains(id) {
		q.recent.Remove(id)
		return
	}

	if q.recentEvict.Contains(id) {
		q.recentEvict.Remove(id)
	}
}

func (q *lruQueue) Len() int {
	q.lock.RLock()
	defer q.lock.RUnlock()
	return q.recent.Len() + q.frequent.Len()
}

func (q *lruQueue) Volume() int {
	return q.size + q.recentSize + q.evictSize
}

func (q *lruQueue) All() []fmt.Stringer {
	q.lock.RLock()
	defer q.lock.RUnlock()
	all := make([]fmt.Stringer, q.recent.Len()+q.frequent.Len()+q.recentEvict.Len())
	copy(all, q.recent.Keys())
	copy(all[q.recent.Len():], q.frequent.Keys())
	copy(all[q.recent.Len()+q.frequent.Len():], q.recentEvict.Keys())
	return all
}

func (q *lruQueue) evictLen() int {
	q.lock.RLock()
	defer q.lock.RUnlock()
	return q.recentEvict.Len()
}

func (q *lruQueue) ensureSpace(ctx context.Context, recentEvict bool) error {
	recentLen := q.recent.Len()
	freqLen := q.frequent.Len()
	if recentLen+freqLen < q.size {
		return nil
	}
	if recentLen > 0 && (recentLen > q.recentSize || (recentLen == q.recentSize && !recentEvict)) {
		k, _, ok := q.recent.GetOldest()
		if !ok {
			return errors.New("failed to get oldest from recent queue")
		}
		if err := q.addLst(ctx, q.recentEvict, q.evictSize, k); err != nil {
			return err
		}
		q.recent.Remove(k)
		return nil
	}
	return q.removeOldest(ctx, q.frequent)
}

func (q *lruQueue) addLst(ctx context.Context, lst simplelru.LRUCache[fmt.Stringer, any], size int, id fmt.Stringer) error {
	if lst.Len() < size {
		lst.Add(id, nil)
		return nil
	}
	if err := q.removeOldest(ctx, lst); err != nil {
		return err
	}
	lst.Add(id, nil)
	return nil
}

func (q *lruQueue) removeOldest(ctx context.Context, lst simplelru.LRUCache[fmt.Stringer, any]) error {
	oldestID, _, ok := lst.GetOldest()
	if ok && q.evictFn != nil {
		if err := q.evictFn(ctx, oldestID); err != nil {
			return err
		}
		_ = lst.Remove(oldestID)
	}
	return nil
}

func (q *lruQueue) cleanEvict(now time.Time, l *logger.Logger) bool {
	if e := l.Debug(); e.Enabled() {
		e.Time("now", now).Msg("block queue wakes")
	}
	if q.evictLen() < 1 {
		return true
	}
	for i := 0; i < defaultEvictBatchSize; i++ {
		if q.remove() {
			break
		}
	}
	return true
}

func (q *lruQueue) remove() bool {
	q.lock.Lock()
	defer q.lock.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := q.removeOldest(ctx, q.recentEvict); err != nil {
		q.l.Error().Err(err).Msg("failed to remove oldest blocks")
	}
	if q.recentEvict.Len() < 1 {
		return true
	}
	return false
}
