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

package node

import (
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

const (
	expiredKeyCleanupInterval = 1 * time.Hour
	keyTTL                    = 24 * time.Hour
)

type roundRobinSelector struct {
	clock       timestamp.Clock
	closeCh     chan struct{}
	lookupTable sync.Map
	nodes       []string
	mu          sync.RWMutex
	once        sync.Once
	tMu         sync.Mutex
}

func (r *roundRobinSelector) Close() {
	close(r.closeCh)
}

// NewRoundRobinSelector creates a new round-robin selector.
func NewRoundRobinSelector() Selector {
	rrs := &roundRobinSelector{
		nodes:   make([]string, 0),
		clock:   timestamp.NewClock(),
		closeCh: make(chan struct{}),
	}
	return rrs
}

func (r *roundRobinSelector) AddNode(node *databasev1.Node) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nodes = append(r.nodes, node.Metadata.Name)
	sort.StringSlice(r.nodes).Sort()
}

func (r *roundRobinSelector) RemoveNode(node *databasev1.Node) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i, n := range r.nodes {
		if n == node.Metadata.Name {
			r.nodes = append(r.nodes[:i], r.nodes[i+1:]...)
			break
		}
	}
}

func (r *roundRobinSelector) Pick(group, _ string, shardID uint32) (string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	k := key{group: group, shardID: shardID}
	if len(r.nodes) == 0 {
		return "", errors.New("no nodes available")
	}
	entry, ok := r.lookupTable.Load(k)
	if ok {
		return r.selectNode(entry), nil
	}
	r.tMu.Lock()
	defer r.tMu.Unlock()
	if entry, ok := r.lookupTable.Load(k); ok {
		return r.selectNode(entry), nil
	}

	keys := []key{k}
	r.lookupTable.Range(func(k, _ any) bool {
		keys = append(keys, k.(key))
		return true
	})
	slices.SortFunc(keys, func(a, b key) int {
		n := strings.Compare(a.group, b.group)
		if n != 0 {
			return n
		}
		return int(a.shardID) - int(b.shardID)
	})
	for i := range keys {
		if entry, ok := r.lookupTable.Load(keys[i]); ok {
			entry.(*tableEntry).index = i
		} else {
			r.lookupTable.Store(keys[i], r.newTableEntry(i))
		}
	}
	r.once.Do(r.startCleanupTicker)
	if entry, ok := r.lookupTable.Load(k); ok {
		return r.selectNode(entry), nil
	}
	panic(fmt.Sprintf("key %v not found", k))
}

func (r *roundRobinSelector) selectNode(entry any) string {
	e := entry.(*tableEntry)
	now := r.clock.Now()
	e.lastAccess.Store(&now)
	return r.nodes[e.index%len(r.nodes)]
}

type key struct {
	group   string
	shardID uint32
}

type tableEntry struct {
	lastAccess *atomic.Pointer[time.Time]
	index      int
}

func (r *roundRobinSelector) newTableEntry(index int) *tableEntry {
	p := atomic.Pointer[time.Time]{}
	now := r.clock.Now()
	p.Store(&now)
	return &tableEntry{
		index:      index,
		lastAccess: &p,
	}
}

func (r *roundRobinSelector) cleanupExpiredEntries() {
	now := r.clock.Now()
	r.tMu.Lock()
	defer r.tMu.Unlock()

	r.lookupTable.Range(func(k, value any) bool {
		e := value.(*tableEntry)
		if now.Sub(*e.lastAccess.Load()) > keyTTL {
			r.lookupTable.Delete(k)
		}
		return true
	})
}

func (r *roundRobinSelector) startCleanupTicker() {
	ticker := r.clock.Ticker(expiredKeyCleanupInterval)
	go func() {
		select {
		case <-r.closeCh:
			ticker.Stop()
			return
		case <-ticker.C:
			r.cleanupExpiredEntries()
		}
	}()
}
