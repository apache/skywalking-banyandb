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
// KIND, either express or implied.  See the specific language governing permissions and limitations
// under the License.

package trace

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBlockQueue_MinHeap(t *testing.T) {
	// Test min heap (keepMin=true) - should keep smallest values
	queue := newBlockQueue(3, true)

	// Add blocks with sizes: 100, 50, 200, 30, 150
	queue.add(blockInfo{blockString: "block1", size: 100})
	queue.add(blockInfo{blockString: "block2", size: 50})
	queue.add(blockInfo{blockString: "block3", size: 200})
	queue.add(blockInfo{blockString: "block4", size: 30})
	queue.add(blockInfo{blockString: "block5", size: 150})

	blocks := queue.getAll()
	require.Len(t, blocks, 3, "Should keep only 3 blocks")

	// Should keep the 3 smallest: 30, 50, 100
	expectedSizes := []uint64{30, 50, 100}
	actualSizes := make([]uint64, len(blocks))
	for i, block := range blocks {
		actualSizes[i] = block.size
	}
	assert.Equal(t, expectedSizes, actualSizes, "Should keep smallest blocks")
}

func TestBlockQueue_MaxHeap(t *testing.T) {
	// Test max heap (keepMin=false) - should keep largest values
	queue := newBlockQueue(3, false)

	// Add blocks with sizes: 100, 50, 200, 30, 150
	queue.add(blockInfo{blockString: "block1", size: 100})
	queue.add(blockInfo{blockString: "block2", size: 50})
	queue.add(blockInfo{blockString: "block3", size: 200})
	queue.add(blockInfo{blockString: "block4", size: 30})
	queue.add(blockInfo{blockString: "block5", size: 150})

	blocks := queue.getAll()
	require.Len(t, blocks, 3, "Should keep only 3 blocks")

	// Should keep the 3 largest: 150, 200, 100
	expectedSizes := []uint64{100, 150, 200}
	actualSizes := make([]uint64, len(blocks))
	for i, block := range blocks {
		actualSizes[i] = block.size
	}
	assert.Equal(t, expectedSizes, actualSizes, "Should keep largest blocks")
}

func TestBlockQueue_EmitBeforeAdd(t *testing.T) {
	// Test that items are emitted before adding when at capacity
	queue := newBlockQueue(2, true) // Keep smallest, capacity 2

	// Fill queue
	queue.add(blockInfo{blockString: "block1", size: 100})
	queue.add(blockInfo{blockString: "block2", size: 50})

	// Queue is now full with [50, 100]
	blocks := queue.getAll()
	require.Len(t, blocks, 2)
	assert.Equal(t, uint64(50), blocks[0].size)
	assert.Equal(t, uint64(100), blocks[1].size)

	// Add a smaller block (30) - should emit 100 and keep 30
	queue.add(blockInfo{blockString: "block3", size: 30})

	blocks = queue.getAll()
	require.Len(t, blocks, 2, "Should still have only 2 blocks")

	// Should now have [30, 50] - 100 was emitted
	expectedSizes := []uint64{30, 50}
	actualSizes := make([]uint64, len(blocks))
	for i, block := range blocks {
		actualSizes[i] = block.size
	}
	assert.Equal(t, expectedSizes, actualSizes, "Should emit largest and keep smallest")

	// Add a larger block (200) - should emit it (not keep it)
	queue.add(blockInfo{blockString: "block4", size: 200})

	blocks = queue.getAll()
	require.Len(t, blocks, 2, "Should still have only 2 blocks")

	// Should still have [30, 50] - 200 was emitted
	expectedSizes = []uint64{30, 50}
	actualSizes = make([]uint64, len(blocks))
	for i, block := range blocks {
		actualSizes[i] = block.size
	}
	assert.Equal(t, expectedSizes, actualSizes, "Should emit larger block")
}

func TestBlockQueue_EmptyQueue(t *testing.T) {
	queue := newBlockQueue(3, true)

	blocks := queue.getAll()
	assert.Empty(t, blocks, "Empty queue should return empty slice")
}

func TestBlockQueue_SingleItem(t *testing.T) {
	queue := newBlockQueue(3, true)

	queue.add(blockInfo{blockString: "block1", size: 100})

	blocks := queue.getAll()
	require.Len(t, blocks, 1)
	assert.Equal(t, "block1", blocks[0].blockString)
	assert.Equal(t, uint64(100), blocks[0].size)
}

func TestStartBlockScanSpan_NoTracer(t *testing.T) {
	// Test with no tracer
	ctx := context.Background()
	traceIDs := []string{"trace1", "trace2"}
	parts := []*part{}

	onBlock, onComplete := startBlockScanSpan(ctx, traceIDs, parts)

	assert.Nil(t, onBlock, "Should return nil onBlock when no tracer")
	assert.NotNil(t, onComplete, "Should return onComplete function")

	// Should not panic when calling onComplete
	onComplete(nil)
}

func TestBlockQueue_EdgeCases(t *testing.T) {
	// Test with capacity 1
	queue := newBlockQueue(1, true)

	queue.add(blockInfo{blockString: "block1", size: 100})
	queue.add(blockInfo{blockString: "block2", size: 50})
	queue.add(blockInfo{blockString: "block3", size: 200})

	blocks := queue.getAll()
	require.Len(t, blocks, 1)
	assert.Equal(t, uint64(50), blocks[0].size, "Should keep only the smallest")
}

func TestBlockQueue_EqualSizes(t *testing.T) {
	// Test with blocks of equal sizes
	queue := newBlockQueue(3, true)

	queue.add(blockInfo{blockString: "block1", size: 100})
	queue.add(blockInfo{blockString: "block2", size: 100})
	queue.add(blockInfo{blockString: "block3", size: 100})
	queue.add(blockInfo{blockString: "block4", size: 100})

	blocks := queue.getAll()
	require.Len(t, blocks, 3, "Should keep only 3 blocks")

	// All blocks should have size 100
	for _, block := range blocks {
		assert.Equal(t, uint64(100), block.size)
	}
}

func TestBlockQueue_ZeroSize(t *testing.T) {
	// Test with zero-sized blocks
	queue := newBlockQueue(2, true)

	queue.add(blockInfo{blockString: "block1", size: 0})
	queue.add(blockInfo{blockString: "block2", size: 0})
	queue.add(blockInfo{blockString: "block3", size: 0})

	blocks := queue.getAll()
	require.Len(t, blocks, 2, "Should keep only 2 blocks")

	// All blocks should have size 0
	for _, block := range blocks {
		assert.Equal(t, uint64(0), block.size)
	}
}
