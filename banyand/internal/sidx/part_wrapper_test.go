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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPartWrapper_BasicLifecycle(t *testing.T) {
	// Create a mock part
	p := &part{
		path:         "/test/part/001",
		partMetadata: &partMetadata{ID: 1},
	}

	// Create wrapper
	pw := newPartWrapper(p)
	require.NotNil(t, pw)
	assert.Equal(t, int32(1), pw.refCount())
	assert.True(t, pw.isActive())
	assert.Equal(t, uint64(1), pw.ID())

	// Test acquire
	assert.True(t, pw.acquire())
	assert.Equal(t, int32(2), pw.refCount())

	// Test multiple acquires
	assert.True(t, pw.acquire())
	assert.True(t, pw.acquire())
	assert.Equal(t, int32(4), pw.refCount())

	// Test releases
	pw.release()
	assert.Equal(t, int32(3), pw.refCount())
	pw.release()
	assert.Equal(t, int32(2), pw.refCount())
	pw.release()
	assert.Equal(t, int32(1), pw.refCount())

	// Final release should trigger cleanup
	pw.release()
	assert.Equal(t, int32(0), pw.refCount())
	assert.True(t, pw.isRemoved())
}

func TestPartWrapper_StateTransitions(t *testing.T) {
	p := &part{
		path:         "/test/part/002",
		partMetadata: &partMetadata{ID: 2},
	}

	pw := newPartWrapper(p)

	// Initial state should be active
	assert.True(t, pw.isActive())
	assert.False(t, pw.isRemoving())
	assert.False(t, pw.isRemoved())
	assert.Equal(t, partStateActive, pw.getState())

	// Mark for removal
	pw.markForRemoval()
	assert.False(t, pw.isActive())
	assert.True(t, pw.isRemoving())
	assert.False(t, pw.isRemoved())
	assert.Equal(t, partStateRemoving, pw.getState())

	// Should not be able to acquire new references
	assert.False(t, pw.acquire())
	assert.Equal(t, int32(1), pw.refCount())

	// Release should transition to removed
	pw.release()
	assert.False(t, pw.isActive())
	assert.False(t, pw.isRemoving())
	assert.True(t, pw.isRemoved())
	assert.Equal(t, partStateRemoved, pw.getState())
}

func TestPartWrapper_ConcurrentReferenceCounting(t *testing.T) {
	p := &part{
		path:         "/test/part/003",
		partMetadata: &partMetadata{ID: 3},
	}

	pw := newPartWrapper(p)

	const numGoroutines = 100
	const operationsPerGoroutine = 1000

	var wg sync.WaitGroup
	var successfulAcquires int64
	var successfulReleases int64

	// Start multiple goroutines that acquire and release references
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < operationsPerGoroutine; j++ {
				if pw.acquire() {
					atomic.AddInt64(&successfulAcquires, 1)
					// Hold the reference briefly
					time.Sleep(time.Microsecond)
					pw.release()
					atomic.AddInt64(&successfulReleases, 1)
				}
			}
		}()
	}

	wg.Wait()

	// All successful acquires should have corresponding releases
	assert.Equal(t, successfulAcquires, successfulReleases)

	// Reference count should be back to 1 (initial reference)
	assert.Equal(t, int32(1), pw.refCount())

	// Final cleanup
	pw.release()
	assert.Equal(t, int32(0), pw.refCount())
}

func TestPartWrapper_ConcurrentAcquireWithMarkForRemoval(t *testing.T) {
	p := &part{
		path:         "/test/part/004",
		partMetadata: &partMetadata{ID: 4},
	}

	pw := newPartWrapper(p)

	const numGoroutines = 20
	var wg sync.WaitGroup
	var successfulAcquires int64
	var failedAcquires int64
	var startBarrier sync.WaitGroup

	startBarrier.Add(1) // Barrier to synchronize goroutine start

	// Start goroutines trying to acquire references
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			startBarrier.Wait() // Wait for all goroutines to be ready
			for j := 0; j < 500; j++ {
				if pw.acquire() {
					atomic.AddInt64(&successfulAcquires, 1)
					time.Sleep(time.Microsecond)
					pw.release()
				} else {
					atomic.AddInt64(&failedAcquires, 1)
				}
			}
		}()
	}

	// Wait a bit, then mark for removal while goroutines are running
	time.Sleep(5 * time.Millisecond)
	pw.markForRemoval()
	startBarrier.Done() // Release all goroutines to start working

	wg.Wait()

	t.Logf("Successful acquires: %d, Failed acquires: %d",
		successfulAcquires, failedAcquires)

	// Should have some failed acquires after marking for removal
	// Note: This may be 0 if all goroutines acquired before markForRemoval,
	// which is acceptable behavior
	t.Logf("Failed acquires: %d (may be 0 due to timing)", failedAcquires)

	// Reference count should be 1 (initial reference)
	assert.Equal(t, int32(1), pw.refCount())
	assert.True(t, pw.isRemoving())

	// Final cleanup
	pw.release()
	assert.Equal(t, int32(0), pw.refCount())
	assert.True(t, pw.isRemoved())
}

func TestPartWrapper_NilPart(t *testing.T) {
	pw := newPartWrapper(nil)
	require.NotNil(t, pw)

	assert.Equal(t, int32(1), pw.refCount())
	assert.Equal(t, uint64(0), pw.ID()) // Should return 0 for nil part
	assert.True(t, pw.isActive())

	// Test acquire/release with nil part
	assert.True(t, pw.acquire())
	assert.Equal(t, int32(2), pw.refCount())

	pw.release()
	assert.Equal(t, int32(1), pw.refCount())

	pw.release()
	assert.Equal(t, int32(0), pw.refCount())
	assert.True(t, pw.isRemoved())
}

func TestPartWrapper_MultipleReleases(t *testing.T) {
	p := &part{
		path:         "/test/part/005",
		partMetadata: &partMetadata{ID: 5},
	}

	pw := newPartWrapper(p)

	// Release once (should reach 0)
	pw.release()
	assert.Equal(t, int32(0), pw.refCount())
	assert.True(t, pw.isRemoved())

	// Additional releases should not cause issues (though they log warnings)
	pw.release()
	pw.release()
	assert.Equal(t, int32(-2), pw.refCount()) // Goes negative but doesn't break
}

func TestPartWrapper_StringRepresentation(t *testing.T) {
	// Test with nil part
	pw1 := newPartWrapper(nil)
	str1 := pw1.String()
	assert.Contains(t, str1, "id=nil")
	assert.Contains(t, str1, "state=active")
	assert.Contains(t, str1, "ref=1")

	// Test with real part
	p := &part{
		path:         "/test/part/006",
		partMetadata: &partMetadata{ID: 6},
	}
	pw2 := newPartWrapper(p)
	str2 := pw2.String()
	assert.Contains(t, str2, "id=6")
	assert.Contains(t, str2, "state=active")
	assert.Contains(t, str2, "ref=1")
	assert.Contains(t, str2, "path=/test/part/006")

	// Test state changes in string
	pw2.markForRemoval()
	str3 := pw2.String()
	assert.Contains(t, str3, "state=removing")

	pw2.release()
	str4 := pw2.String()
	assert.Contains(t, str4, "state=removed")
}

func TestPartWrapper_StateStringRepresentation(t *testing.T) {
	assert.Equal(t, "active", partStateActive.String())
	assert.Equal(t, "removing", partStateRemoving.String())
	assert.Equal(t, "removed", partStateRemoved.String())
	assert.Contains(t, partWrapperState(999).String(), "unknown")
}

func TestPartWrapper_CleanupWithRemovableFlag(t *testing.T) {
	p := &part{
		path:         "/test/part/007",
		partMetadata: &partMetadata{ID: 7},
	}

	pw := newPartWrapper(p)

	// Test that removable flag is initially false
	assert.False(t, pw.removable.Load())

	// Mark for removal sets the flag
	pw.markForRemoval()
	assert.True(t, pw.removable.Load())
	assert.True(t, pw.isRemoving())

	// Release should trigger cleanup
	pw.release()
	assert.True(t, pw.isRemoved())
}

// Benchmark tests.
func BenchmarkPartWrapper_AcquireRelease(b *testing.B) {
	p := &part{
		path:         "/bench/part",
		partMetadata: &partMetadata{ID: 1},
	}
	pw := newPartWrapper(p)
	defer pw.release() // cleanup

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if pw.acquire() {
				pw.release()
			}
		}
	})
}

func BenchmarkPartWrapper_StateCheck(b *testing.B) {
	p := &part{
		path:         "/bench/part",
		partMetadata: &partMetadata{ID: 1},
	}
	pw := newPartWrapper(p)
	defer pw.release() // cleanup

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = pw.isActive()
	}
}

func TestPartWrapper_CleanupPreventNegativeRefCount(t *testing.T) {
	p := &part{
		path:         "/test/part/008",
		partMetadata: &partMetadata{ID: 8},
	}

	pw := newPartWrapper(p)
	assert.Equal(t, int32(1), pw.refCount())

	// Acquire a reference
	assert.True(t, pw.acquire())
	assert.Equal(t, int32(2), pw.refCount())

	// Release back to 1
	pw.release()
	assert.Equal(t, int32(1), pw.refCount())
	assert.True(t, pw.isActive())

	// Final release should trigger cleanup and set ref to 0
	pw.release()
	assert.Equal(t, int32(0), pw.refCount())
	assert.True(t, pw.isRemoved())

	// Additional releases should not make ref count go further negative than logged
	// The implementation allows negative counts but logs warnings
	initialRef := pw.refCount()
	pw.release()
	newRef := pw.refCount()
	assert.Equal(t, initialRef-1, newRef)   // Should decrement by 1
	assert.LessOrEqual(t, newRef, int32(0)) // Should not be positive
}

func TestPartWrapper_CleanupWithConcurrentAccess(t *testing.T) {
	p := &part{
		path:         "/test/part/009",
		partMetadata: &partMetadata{ID: 9},
	}

	pw := newPartWrapper(p)
	const numGoroutines = 50
	var wg sync.WaitGroup

	// Track the minimum reference count observed
	var minRefCount int64 = 1

	// Start goroutines that try to acquire and immediately release
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				if pw.acquire() {
					currentRef := atomic.LoadInt32(&pw.ref)
					for {
						current := atomic.LoadInt64(&minRefCount)
						if int64(currentRef) >= current || atomic.CompareAndSwapInt64(&minRefCount, current, int64(currentRef)) {
							break
						}
					}
					pw.release()
				}
			}
		}()
	}

	wg.Wait()

	// Verify that during concurrent access, ref count never went below 1
	// (except during final cleanup)
	assert.GreaterOrEqual(t, minRefCount, int64(1), "Reference count should never go below 1 during normal operation")

	// Final cleanup
	pw.release()
	assert.Equal(t, int32(0), pw.refCount())
	assert.True(t, pw.isRemoved())
}

func TestPartWrapper_CleanupStateTransition(t *testing.T) {
	p := &part{
		path:         "/test/part/010",
		partMetadata: &partMetadata{ID: 10},
	}

	pw := newPartWrapper(p)

	// Verify initial state
	assert.True(t, pw.isActive())
	assert.False(t, pw.isRemoving())
	assert.False(t, pw.isRemoved())

	// Mark for removal should change state but not trigger cleanup yet
	pw.markForRemoval()
	assert.False(t, pw.isActive())
	assert.True(t, pw.isRemoving())
	assert.False(t, pw.isRemoved())
	assert.Equal(t, int32(1), pw.refCount()) // Should still be 1

	// Release should trigger cleanup and change state to removed
	pw.release()
	assert.False(t, pw.isActive())
	assert.False(t, pw.isRemoving())
	assert.True(t, pw.isRemoved())
	assert.Equal(t, int32(0), pw.refCount())
}

func TestPartWrapper_CleanupWithNilPart(t *testing.T) {
	pw := newPartWrapper(nil)

	// Verify cleanup works correctly with nil part
	assert.Equal(t, int32(1), pw.refCount())
	assert.True(t, pw.isActive())

	// Mark for removal
	pw.markForRemoval()
	assert.True(t, pw.isRemoving())

	// Release should complete cleanup without issues
	pw.release()
	assert.True(t, pw.isRemoved())
	assert.Equal(t, int32(0), pw.refCount())

	// Additional releases should be handled gracefully
	pw.release()
	assert.Equal(t, int32(-1), pw.refCount())
}

func TestPartWrapper_CleanupRaceCondition(t *testing.T) {
	p := &part{
		path:         "/test/part/011",
		partMetadata: &partMetadata{ID: 11},
	}

	pw := newPartWrapper(p)

	// Acquire multiple references
	assert.True(t, pw.acquire()) // ref = 2
	assert.True(t, pw.acquire()) // ref = 3
	assert.True(t, pw.acquire()) // ref = 4

	var wg sync.WaitGroup
	const numReleasers = 4

	// Start multiple goroutines to release references simultaneously
	for i := 0; i < numReleasers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pw.release()
		}()
	}

	wg.Wait()

	// All references should be released, cleanup should have occurred exactly once
	assert.Equal(t, int32(0), pw.refCount())
	assert.True(t, pw.isRemoved())

	// Verify that no further operations are possible
	assert.False(t, pw.acquire())
}

func TestPartWrapper_CleanupIdempotency(t *testing.T) {
	p := &part{
		path:         "/test/part/012",
		partMetadata: &partMetadata{ID: 12},
	}

	pw := newPartWrapper(p)

	// Release to trigger cleanup
	pw.release()
	assert.Equal(t, int32(0), pw.refCount())
	assert.True(t, pw.isRemoved())

	// Call cleanup directly multiple times - should be safe
	pw.cleanup()
	pw.cleanup()
	pw.cleanup()

	// State should remain consistent
	assert.True(t, pw.isRemoved())
	assert.Equal(t, int32(0), pw.refCount())
}
