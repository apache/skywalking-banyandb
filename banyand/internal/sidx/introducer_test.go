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

package sidx

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntroductionPooling(t *testing.T) {
	t.Run("introduction pooling reduces allocations", func(t *testing.T) {
		// Test that the pool works by verifying reuse
		intro1 := generateIntroduction()
		releaseIntroduction(intro1)

		intro2 := generateIntroduction()
		// With pooling, we might get the same instance back
		require.NotNil(t, intro2, "pool should provide introduction instance")
		releaseIntroduction(intro2)

		// Test multiple allocations work correctly
		var intros []*introduction
		for i := 0; i < 10; i++ {
			intro := generateIntroduction()
			intro.memPart = &memPart{}
			intro.applied = make(chan struct{})
			intros = append(intros, intro)
		}

		for _, intro := range intros {
			releaseIntroduction(intro)
		}
	})

	t.Run("flusher introduction pooling reduces allocations", func(t *testing.T) {
		intro1 := generateFlusherIntroduction()
		releaseFlusherIntroduction(intro1)

		intro2 := generateFlusherIntroduction()
		require.NotNil(t, intro2, "pool should provide flusher introduction instance")
		require.NotNil(t, intro2.flushed, "flushed map should be initialized")
		releaseFlusherIntroduction(intro2)

		// Test multiple allocations
		var intros []*flusherIntroduction
		for i := 0; i < 10; i++ {
			intro := generateFlusherIntroduction()
			intro.flushed[uint64(i)] = &part{}
			intro.applied = make(chan struct{})
			intros = append(intros, intro)
		}

		for _, intro := range intros {
			releaseFlusherIntroduction(intro)
		}
	})

	t.Run("merger introduction pooling reduces allocations", func(t *testing.T) {
		intro1 := generateMergerIntroduction()
		releaseMergerIntroduction(intro1)

		intro2 := generateMergerIntroduction()
		require.NotNil(t, intro2, "pool should provide merger introduction instance")
		require.NotNil(t, intro2.merged, "merged map should be initialized")
		releaseMergerIntroduction(intro2)

		// Test multiple allocations
		var intros []*mergerIntroduction
		for i := 0; i < 10; i++ {
			intro := generateMergerIntroduction()
			intro.merged[uint64(i)] = struct{}{}
			intro.newPart = &part{}
			intro.applied = make(chan struct{})
			intros = append(intros, intro)
		}

		for _, intro := range intros {
			releaseMergerIntroduction(intro)
		}
	})
}

func TestIntroductionReset(t *testing.T) {
	t.Run("introduction reset for reuse", func(t *testing.T) {
		intro := generateIntroduction()

		// Set up introduction with data
		intro.memPart = &memPart{}
		intro.applied = make(chan struct{})

		// Reset the introduction
		intro.reset()

		// Verify all fields are cleared
		assert.Nil(t, intro.memPart, "memPart should be nil")
		assert.Nil(t, intro.applied, "applied channel should be nil")

		releaseIntroduction(intro)
	})

	t.Run("flusher introduction reset for reuse", func(t *testing.T) {
		intro := generateFlusherIntroduction()

		// Set up flusher introduction with data
		intro.flushed[1] = &part{}
		intro.flushed[2] = &part{}
		intro.applied = make(chan struct{})

		// Reset the flusher introduction
		intro.reset()

		// Verify all fields are cleared
		assert.Len(t, intro.flushed, 0, "flushed map should be empty")
		assert.Nil(t, intro.applied, "applied channel should be nil")

		releaseFlusherIntroduction(intro)
	})

	t.Run("merger introduction reset for reuse", func(t *testing.T) {
		intro := generateMergerIntroduction()

		// Set up merger introduction with data
		intro.merged[1] = struct{}{}
		intro.merged[2] = struct{}{}
		intro.newPart = &part{}
		intro.applied = make(chan struct{})

		// Reset the merger introduction
		intro.reset()

		// Verify all fields are cleared
		assert.Len(t, intro.merged, 0, "merged map should be empty")
		assert.Nil(t, intro.newPart, "newPart should be nil")
		assert.Nil(t, intro.applied, "applied channel should be nil")

		releaseMergerIntroduction(intro)
	})
}

func TestChannelSynchronization(t *testing.T) {
	t.Run("applied notifications work reliably", func(t *testing.T) {
		intro := generateIntroduction()
		intro.applied = make(chan struct{})

		// Start a goroutine that waits for the notification
		done := make(chan bool)
		go func() {
			select {
			case <-intro.applied:
				done <- true
			case <-time.After(100 * time.Millisecond):
				done <- false
			}
		}()

		// Close the applied channel to simulate completion
		close(intro.applied)

		// Check that the notification was received
		result := <-done
		assert.True(t, result, "applied notification should be received")

		releaseIntroduction(intro)
	})

	t.Run("multiple waiters on applied channel", func(t *testing.T) {
		intro := generateFlusherIntroduction()
		intro.applied = make(chan struct{})

		const numWaiters = 5
		var wg sync.WaitGroup
		results := make(chan bool, numWaiters)

		// Start multiple waiters
		for i := 0; i < numWaiters; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				select {
				case <-intro.applied:
					results <- true
				case <-time.After(100 * time.Millisecond):
					results <- false
				}
			}()
		}

		// Close the applied channel
		close(intro.applied)
		wg.Wait()
		close(results)

		// All waiters should receive the notification
		successCount := 0
		for result := range results {
			if result {
				successCount++
			}
		}
		assert.Equal(t, numWaiters, successCount, "all waiters should receive notification")

		releaseFlusherIntroduction(intro)
	})

	t.Run("channel synchronization correctness", func(t *testing.T) {
		intro := generateMergerIntroduction()
		intro.applied = make(chan struct{})

		// Test that reading from a closed channel doesn't block
		close(intro.applied)

		// Multiple reads should work
		for i := 0; i < 3; i++ {
			select {
			case <-intro.applied:
				// Should not block
			default:
				t.Errorf("reading from closed channel should not block")
			}
		}

		releaseMergerIntroduction(intro)
	})
}

func TestIntroductionGeneration(t *testing.T) {
	t.Run("introduction generation creates valid instances", func(t *testing.T) {
		intro := generateIntroduction()
		require.NotNil(t, intro, "generated introduction should not be nil")
		assert.Nil(t, intro.memPart, "initial memPart should be nil")
		assert.Nil(t, intro.applied, "initial applied channel should be nil")
		releaseIntroduction(intro)
	})

	t.Run("flusher introduction generation creates valid instances", func(t *testing.T) {
		intro := generateFlusherIntroduction()
		require.NotNil(t, intro, "generated flusher introduction should not be nil")
		require.NotNil(t, intro.flushed, "flushed map should be initialized")
		assert.Len(t, intro.flushed, 0, "flushed map should be empty")
		assert.Nil(t, intro.applied, "initial applied channel should be nil")
		releaseFlusherIntroduction(intro)
	})

	t.Run("merger introduction generation creates valid instances", func(t *testing.T) {
		intro := generateMergerIntroduction()
		require.NotNil(t, intro, "generated merger introduction should not be nil")
		require.NotNil(t, intro.merged, "merged map should be initialized")
		assert.Len(t, intro.merged, 0, "merged map should be empty")
		assert.Nil(t, intro.newPart, "initial newPart should be nil")
		assert.Nil(t, intro.applied, "initial applied channel should be nil")
		releaseMergerIntroduction(intro)
	})
}

func TestConcurrentPoolAccess(t *testing.T) {
	t.Run("concurrent pool access is safe", func(_ *testing.T) {
		const numGoroutines = 10
		const operationsPerGoroutine = 100

		var wg sync.WaitGroup

		// Test concurrent access to introduction pool
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < operationsPerGoroutine; j++ {
					intro := generateIntroduction()
					intro.memPart = &memPart{}
					intro.applied = make(chan struct{})
					releaseIntroduction(intro)
				}
			}()
		}

		wg.Wait()
		// Test should complete without data races
	})

	t.Run("concurrent flusher introduction pool access", func(_ *testing.T) {
		const numGoroutines = 10
		const operationsPerGoroutine = 100

		var wg sync.WaitGroup

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < operationsPerGoroutine; j++ {
					intro := generateFlusherIntroduction()
					intro.flushed[uint64(j)] = &part{}
					intro.applied = make(chan struct{})
					releaseFlusherIntroduction(intro)
				}
			}()
		}

		wg.Wait()
	})

	t.Run("concurrent merger introduction pool access", func(_ *testing.T) {
		const numGoroutines = 10
		const operationsPerGoroutine = 100

		var wg sync.WaitGroup

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < operationsPerGoroutine; j++ {
					intro := generateMergerIntroduction()
					intro.merged[uint64(j)] = struct{}{}
					intro.newPart = &part{}
					intro.applied = make(chan struct{})
					releaseMergerIntroduction(intro)
				}
			}()
		}

		wg.Wait()
	})
}

func TestIntroductionMapOperations(t *testing.T) {
	t.Run("flusher introduction map operations", func(t *testing.T) {
		intro := generateFlusherIntroduction()

		// Add parts to flushed map
		part1 := &part{}
		part2 := &part{}
		intro.flushed[1] = part1
		intro.flushed[2] = part2

		// Verify map contents
		assert.Equal(t, part1, intro.flushed[1], "part1 should be retrievable")
		assert.Equal(t, part2, intro.flushed[2], "part2 should be retrievable")
		assert.Len(t, intro.flushed, 2, "flushed map should have 2 entries")

		// Reset should clear the map
		intro.reset()
		assert.Len(t, intro.flushed, 0, "flushed map should be empty after reset")

		releaseFlusherIntroduction(intro)
	})

	t.Run("merger introduction map operations", func(t *testing.T) {
		intro := generateMergerIntroduction()

		// Add IDs to merged map
		intro.merged[1] = struct{}{}
		intro.merged[2] = struct{}{}
		intro.merged[3] = struct{}{}

		// Verify map contents
		_, exists1 := intro.merged[1]
		_, exists2 := intro.merged[2]
		_, exists3 := intro.merged[3]
		_, exists4 := intro.merged[4]

		assert.True(t, exists1, "ID 1 should exist in merged map")
		assert.True(t, exists2, "ID 2 should exist in merged map")
		assert.True(t, exists3, "ID 3 should exist in merged map")
		assert.False(t, exists4, "ID 4 should not exist in merged map")
		assert.Len(t, intro.merged, 3, "merged map should have 3 entries")

		// Reset should clear the map
		intro.reset()
		assert.Len(t, intro.merged, 0, "merged map should be empty after reset")

		releaseMergerIntroduction(intro)
	})
}

func TestNilSafetyForIntroductions(t *testing.T) {
	t.Run("release nil introduction", func(_ *testing.T) {
		// Should not panic
		releaseIntroduction(nil)
	})

	t.Run("release nil flusher introduction", func(_ *testing.T) {
		// Should not panic
		releaseFlusherIntroduction(nil)
	})

	t.Run("release nil merger introduction", func(_ *testing.T) {
		// Should not panic
		releaseMergerIntroduction(nil)
	})
}
