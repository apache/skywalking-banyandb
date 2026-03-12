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

package idgen

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenerator_Uniqueness(t *testing.T) {
	gen := NewGenerator("test-node-0", nil)
	const count = 100_000
	ids := make(map[uint64]struct{}, count)
	for idx := 0; idx < count; idx++ {
		id := gen.NextID()
		_, exists := ids[id]
		require.False(t, exists, "duplicate ID at iteration %d: %d", idx, id)
		ids[id] = struct{}{}
	}
	assert.Len(t, ids, count)
}

func TestGenerator_Uniqueness_Concurrent(t *testing.T) {
	gen := NewGenerator("test-node-0", nil)
	const goroutines = 10
	const perGoroutine = 10_000

	results := make([][]uint64, goroutines)
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for gi := 0; gi < goroutines; gi++ {
		gi := gi
		results[gi] = make([]uint64, perGoroutine)
		go func() {
			defer wg.Done()
			for ji := 0; ji < perGoroutine; ji++ {
				results[gi][ji] = gen.NextID()
			}
		}()
	}
	wg.Wait()

	all := make(map[uint64]struct{}, goroutines*perGoroutine)
	for _, batch := range results {
		for _, id := range batch {
			_, exists := all[id]
			require.False(t, exists, "concurrent duplicate ID: %d", id)
			all[id] = struct{}{}
		}
	}
	assert.Len(t, all, goroutines*perGoroutine)
}

func TestGenerator_Monotonic(t *testing.T) {
	gen := NewGenerator("test-node-0", nil)
	prev := gen.NextID()
	for idx := 0; idx < 10_000; idx++ {
		curr := gen.NextID()
		assert.Greater(t, curr, prev, "ID must be monotonically increasing")
		prev = curr
	}
}

func TestGenerator_SequenceOverflow(t *testing.T) {
	gen := NewGenerator("test-node-0", nil)
	total := int(maxSequence) + 2
	ids := make(map[uint64]struct{}, total)
	for idx := 0; idx < total; idx++ {
		id := gen.NextID()
		_, exists := ids[id]
		require.False(t, exists, "duplicate on overflow at %d: %d", idx, id)
		ids[id] = struct{}{}
	}
	assert.Len(t, ids, total)
}

func TestGenerator_DifferentNodes(t *testing.T) {
	gen1 := NewGenerator("node-a", nil)
	gen2 := NewGenerator("node-b", nil)
	ids := make(map[uint64]struct{}, 20_000)
	for idx := 0; idx < 10_000; idx++ {
		id1 := gen1.NextID()
		id2 := gen2.NextID()
		_, exists1 := ids[id1]
		require.False(t, exists1, "cross-node duplicate from gen1: %d", id1)
		_, exists2 := ids[id2]
		require.False(t, exists2, "cross-node duplicate from gen2: %d", id2)
		ids[id1] = struct{}{}
		ids[id2] = struct{}{}
	}
	assert.Len(t, ids, 20_000)
}

func TestGenerator_BitLayout(t *testing.T) {
	gen := NewGenerator("test-node-0", nil)
	id := gen.NextID()
	// Sign bit must be 0.
	assert.Equal(t, uint64(0), id>>63, "sign bit must be 0")

	// Node bits are at positions [10..16].
	extractedNode := int64((id >> nodeShift) & uint64(maxNodeID))
	assert.Equal(t, gen.NodeID(), extractedNode, "node ID should match")
}
