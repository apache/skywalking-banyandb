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

package protector

import (
	"sync"
	"sync/atomic"
)

// The functions in this file are a test-only hook. In-process integration tests run several nodes in one process and
// cannot reach each node's protector, so every protector created by NewMemory is recorded in creation order and a test
// can force one of them into the high-pressure state.

var (
	memoryRosterMu sync.Mutex
	memoryRoster   []*memory
)

func registerMemory(m *memory) {
	memoryRosterMu.Lock()
	memoryRoster = append(memoryRoster, m)
	memoryRosterMu.Unlock()
}

// CountMemories returns the number of protectors created by NewMemory in this process.
// It is intended for tests.
func CountMemories() int {
	memoryRosterMu.Lock()
	defer memoryRosterMu.Unlock()
	return len(memoryRoster)
}

// ForceHighByIndex makes the i-th protector created by NewMemory report StateHigh while high is true, regardless of
// its real usage. It reports whether such a protector exists. It is intended for tests.
func ForceHighByIndex(i int, high bool) bool {
	memoryRosterMu.Lock()
	defer memoryRosterMu.Unlock()
	if i < 0 || i >= len(memoryRoster) {
		return false
	}
	memoryRoster[i].forcedHigh.Store(high)
	return true
}

// nextQueryPool is the query pool size the next protector created by NewMemory takes, or zero for none.
var nextQueryPool atomic.Uint64

// SetNextQueryPool makes the next protector created by NewMemory size its query pool to poolBytes (so a query may
// reserve at most an eighth of it) and treat all of that pool as available, whatever the process uses. An in-process
// test calls it right before it starts the node whose queries it wants to run out of budget. It is intended for tests.
func SetNextQueryPool(poolBytes uint64) { nextQueryPool.Store(poolBytes) }

// pinnedPoolMemory is a protector whose limit is chosen by a test and which always reports that limit as available.
type pinnedPoolMemory struct {
	Memory
	limit uint64
}

func (p pinnedPoolMemory) GetLimit() uint64      { return p.limit }
func (p pinnedPoolMemory) AvailableBytes() int64 { return int64(p.limit) }

// applyNextQueryPool gives m the pinned query budget requested by SetNextQueryPool, if any.
func applyNextQueryPool(m *memory) {
	poolBytes := nextQueryPool.Swap(0)
	if poolBytes == 0 {
		return
	}
	m.queryBudgetOnce.Do(func() {
		m.queryBudget = NewQueryBudget(pinnedPoolMemory{Memory: m, limit: poolBytes * queryPoolFraction})
	})
}
