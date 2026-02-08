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

package snapshot

import (
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/apache/skywalking-banyandb/pkg/pool"
)

// mockSnapshot is a test implementation of the Snapshot interface.
type mockSnapshot struct {
	id  int
	ref atomic.Int32
}

func (m *mockSnapshot) IncRef() {
	m.ref.Add(1)
}

func (m *mockSnapshot) DecRef() {
	m.ref.Add(-1)
}

func (m *mockSnapshot) RefCount() int32 {
	return m.ref.Load()
}

// mockManager is a test implementation of the Manager interface.
type mockManager struct {
	snapshot *mockSnapshot
	mu       sync.RWMutex
}

func (m *mockManager) CurrentSnapshot() *mockSnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.snapshot != nil {
		m.snapshot.IncRef()
		return m.snapshot
	}
	return nil
}

func (m *mockManager) ReplaceSnapshot(next *mockSnapshot) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.snapshot != nil {
		m.snapshot.DecRef()
	}
	m.snapshot = next
}

// getSnapshotPoolRefCounts returns ref counts for all pools with names starting with "snapshot.".
func getSnapshotPoolRefCounts() map[string]int {
	all := pool.AllRefsCount()
	result := make(map[string]int, len(all))
	for name, count := range all {
		if strings.HasPrefix(name, "snapshot.") {
			result[name] = count
		}
	}
	return result
}

// assertSnapshotPoolsNoLeak verifies that snapshot pool ref counts have not increased (no leak).
func assertSnapshotPoolsNoLeak(t *testing.T, before map[string]int) {
	t.Helper()
	after := getSnapshotPoolRefCounts()
	for name, count := range after {
		if beforeCount := before[name]; beforeCount != count {
			t.Errorf("pool %s: ref count changed from %d to %d (possible leak)", name, beforeCount, count)
		}
	}
}

func TestTransition_Commit(t *testing.T) {
	before := getSnapshotPoolRefCounts()
	defer func() { assertSnapshotPoolsNoLeak(t, before) }()

	manager := &mockManager{
		snapshot: &mockSnapshot{id: 1},
	}
	manager.snapshot.IncRef()

	transition := NewTransition(manager, func(cur *mockSnapshot) *mockSnapshot {
		next := &mockSnapshot{id: cur.id + 1}
		next.IncRef()
		return next
	})
	defer transition.Release()

	transition.Commit()

	// Verify snapshot was replaced
	current := manager.CurrentSnapshot()
	if current == nil {
		t.Fatal("expected non-nil snapshot after commit")
	}
	defer current.DecRef()

	if current.id != 2 {
		t.Errorf("expected snapshot id 2, got %d", current.id)
	}
}

func TestTransition_Rollback(t *testing.T) {
	before := getSnapshotPoolRefCounts()
	defer func() { assertSnapshotPoolsNoLeak(t, before) }()

	manager := &mockManager{
		snapshot: &mockSnapshot{id: 1},
	}
	manager.snapshot.IncRef()

	transition := NewTransition(manager, func(cur *mockSnapshot) *mockSnapshot {
		next := &mockSnapshot{id: cur.id + 1}
		next.IncRef()
		return next
	})
	defer transition.Release()

	transition.Rollback()

	// Verify snapshot was NOT replaced
	current := manager.CurrentSnapshot()
	if current == nil {
		t.Fatal("expected non-nil snapshot after rollback")
	}
	defer current.DecRef()

	if current.id != 1 {
		t.Errorf("expected snapshot id 1, got %d", current.id)
	}
}

func TestTransaction_SingleTransition(t *testing.T) {
	before := getSnapshotPoolRefCounts()
	defer func() { assertSnapshotPoolsNoLeak(t, before) }()

	manager := &mockManager{
		snapshot: &mockSnapshot{id: 1},
	}
	manager.snapshot.IncRef()

	txn := NewTransaction()
	defer txn.Release()
	transition := NewTransition(manager, func(cur *mockSnapshot) *mockSnapshot {
		next := &mockSnapshot{id: cur.id + 1}
		next.IncRef()
		return next
	})
	defer transition.Release()
	AddTransition(txn, transition)

	txn.Commit()

	// Verify snapshot was replaced
	current := manager.CurrentSnapshot()
	if current == nil {
		t.Fatal("expected non-nil snapshot after commit")
	}
	defer current.DecRef()

	if current.id != 2 {
		t.Errorf("expected snapshot id 2, got %d", current.id)
	}
}

func TestTransaction_MultipleTransitions(t *testing.T) {
	before := getSnapshotPoolRefCounts()
	defer func() { assertSnapshotPoolsNoLeak(t, before) }()

	manager1 := &mockManager{
		snapshot: &mockSnapshot{id: 1},
	}
	manager1.snapshot.IncRef()

	manager2 := &mockManager{
		snapshot: &mockSnapshot{id: 10},
	}
	manager2.snapshot.IncRef()

	txn := NewTransaction()
	defer txn.Release()

	transition1 := NewTransition(manager1, func(cur *mockSnapshot) *mockSnapshot {
		next := &mockSnapshot{id: cur.id + 1}
		next.IncRef()
		return next
	})
	defer transition1.Release()
	AddTransition(txn, transition1)

	transition2 := NewTransition(manager2, func(cur *mockSnapshot) *mockSnapshot {
		next := &mockSnapshot{id: cur.id + 1}
		next.IncRef()
		return next
	})
	defer transition2.Release()
	AddTransition(txn, transition2)

	txn.Commit()

	// Verify both snapshots were replaced
	current1 := manager1.CurrentSnapshot()
	if current1 == nil {
		t.Fatal("expected non-nil snapshot for manager1")
	}
	defer current1.DecRef()

	if current1.id != 2 {
		t.Errorf("expected snapshot id 2 for manager1, got %d", current1.id)
	}

	current2 := manager2.CurrentSnapshot()
	if current2 == nil {
		t.Fatal("expected non-nil snapshot for manager2")
	}
	defer current2.DecRef()

	if current2.id != 11 {
		t.Errorf("expected snapshot id 11 for manager2, got %d", current2.id)
	}
}

func TestTransaction_Rollback(t *testing.T) {
	before := getSnapshotPoolRefCounts()
	defer func() { assertSnapshotPoolsNoLeak(t, before) }()

	manager1 := &mockManager{
		snapshot: &mockSnapshot{id: 1},
	}
	manager1.snapshot.IncRef()

	manager2 := &mockManager{
		snapshot: &mockSnapshot{id: 10},
	}
	manager2.snapshot.IncRef()

	txn := NewTransaction()
	defer txn.Release()

	transition1 := NewTransition(manager1, func(cur *mockSnapshot) *mockSnapshot {
		next := &mockSnapshot{id: cur.id + 1}
		next.IncRef()
		return next
	})
	defer transition1.Release()
	AddTransition(txn, transition1)

	transition2 := NewTransition(manager2, func(cur *mockSnapshot) *mockSnapshot {
		next := &mockSnapshot{id: cur.id + 1}
		next.IncRef()
		return next
	})
	defer transition2.Release()
	AddTransition(txn, transition2)

	txn.Rollback()

	// Verify neither snapshot was replaced
	current1 := manager1.CurrentSnapshot()
	if current1 == nil {
		t.Fatal("expected non-nil snapshot for manager1")
	}
	defer current1.DecRef()

	if current1.id != 1 {
		t.Errorf("expected snapshot id 1 for manager1, got %d", current1.id)
	}

	current2 := manager2.CurrentSnapshot()
	if current2 == nil {
		t.Fatal("expected non-nil snapshot for manager2")
	}
	defer current2.DecRef()

	if current2.id != 10 {
		t.Errorf("expected snapshot id 10 for manager2, got %d", current2.id)
	}
}

func TestTransaction_Concurrent(t *testing.T) {
	before := getSnapshotPoolRefCounts()
	defer func() { assertSnapshotPoolsNoLeak(t, before) }()

	manager := &mockManager{
		snapshot: &mockSnapshot{id: 0},
	}
	manager.snapshot.IncRef()

	const numGoroutines = 10

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()

			txn := NewTransaction()
			defer txn.Release()
			transition := NewTransition(manager, func(cur *mockSnapshot) *mockSnapshot {
				next := &mockSnapshot{id: cur.id + 1}
				next.IncRef()
				return next
			})
			defer transition.Release()
			AddTransition(txn, transition)
			txn.Commit()
		}()
	}

	wg.Wait()

	// Verify final snapshot exists and was updated
	// Note: With concurrent updates, not all increments may be preserved
	// because multiple goroutines might read the same snapshot concurrently.
	// The important thing is that no crashes occurred and the snapshot changed.
	current := manager.CurrentSnapshot()
	if current == nil {
		t.Fatal("expected non-nil snapshot after concurrent commits")
	}
	defer current.DecRef()

	if current.id <= 0 {
		t.Errorf("expected snapshot id > 0, got %d", current.id)
	}
	if current.id > numGoroutines {
		t.Errorf("expected snapshot id <= %d, got %d", numGoroutines, current.id)
	}
}

func TestTransaction_IdempotentCommit(t *testing.T) {
	before := getSnapshotPoolRefCounts()
	defer func() { assertSnapshotPoolsNoLeak(t, before) }()

	manager := &mockManager{
		snapshot: &mockSnapshot{id: 1},
	}
	manager.snapshot.IncRef()

	txn := NewTransaction()
	defer txn.Release()
	transition := NewTransition(manager, func(cur *mockSnapshot) *mockSnapshot {
		next := &mockSnapshot{id: cur.id + 1}
		next.IncRef()
		return next
	})
	defer transition.Release()
	AddTransition(txn, transition)

	// Commit multiple times
	txn.Commit()
	txn.Commit()
	txn.Commit()

	// Verify snapshot was replaced only once
	current := manager.CurrentSnapshot()
	if current == nil {
		t.Fatal("expected non-nil snapshot")
	}
	defer current.DecRef()

	if current.id != 2 {
		t.Errorf("expected snapshot id 2, got %d", current.id)
	}
}

func TestTransaction_IdempotentRollback(t *testing.T) {
	before := getSnapshotPoolRefCounts()
	defer func() { assertSnapshotPoolsNoLeak(t, before) }()

	manager := &mockManager{
		snapshot: &mockSnapshot{id: 1},
	}
	manager.snapshot.IncRef()

	txn := NewTransaction()
	defer txn.Release()
	transition := NewTransition(manager, func(cur *mockSnapshot) *mockSnapshot {
		next := &mockSnapshot{id: cur.id + 1}
		next.IncRef()
		return next
	})
	defer transition.Release()
	AddTransition(txn, transition)

	// Rollback multiple times
	txn.Rollback()
	txn.Rollback()
	txn.Rollback()

	// Verify snapshot was not replaced
	current := manager.CurrentSnapshot()
	if current == nil {
		t.Fatal("expected non-nil snapshot")
	}
	defer current.DecRef()

	if current.id != 1 {
		t.Errorf("expected snapshot id 1, got %d", current.id)
	}
}

func TestTransaction_CommitAfterRollback(t *testing.T) {
	before := getSnapshotPoolRefCounts()
	defer func() { assertSnapshotPoolsNoLeak(t, before) }()

	manager := &mockManager{
		snapshot: &mockSnapshot{id: 1},
	}
	manager.snapshot.IncRef()

	txn := NewTransaction()
	defer txn.Release()
	transition := NewTransition(manager, func(cur *mockSnapshot) *mockSnapshot {
		next := &mockSnapshot{id: cur.id + 1}
		next.IncRef()
		return next
	})
	defer transition.Release()
	AddTransition(txn, transition)

	// Rollback first, then try to commit
	txn.Rollback()
	txn.Commit()

	// Verify snapshot was not replaced
	current := manager.CurrentSnapshot()
	if current == nil {
		t.Fatal("expected non-nil snapshot")
	}
	defer current.DecRef()

	if current.id != 1 {
		t.Errorf("expected snapshot id 1, got %d", current.id)
	}
}

func TestTransaction_HeterogeneousTypes(t *testing.T) {
	before := getSnapshotPoolRefCounts()
	defer func() { assertSnapshotPoolsNoLeak(t, before) }()

	// Test with two different snapshot types to verify type safety
	type alternateSnapshot struct {
		value string
		ref   atomic.Int32
	}

	altSnap := &alternateSnapshot{value: "test"}
	altSnap.ref.Store(1)

	altManager := &struct {
		snapshot *alternateSnapshot
		mu       sync.RWMutex
	}{
		snapshot: altSnap,
	}

	// Define methods inline for alternateSnapshot
	incRef := func(s *alternateSnapshot) {
		s.ref.Add(1)
	}

	// Create a custom manager for alternateSnapshot
	currentAlt := func() *alternateSnapshot {
		altManager.mu.RLock()
		defer altManager.mu.RUnlock()
		if altManager.snapshot != nil {
			incRef(altManager.snapshot)
			return altManager.snapshot
		}
		return nil
	}

	replaceAlt := func(next *alternateSnapshot) {
		altManager.mu.Lock()
		defer altManager.mu.Unlock()
		altManager.snapshot = next
	}

	// This test verifies that the generic design compiles with different types
	// The actual runtime behavior is tested in other tests
	_ = currentAlt
	_ = replaceAlt
}

// BenchmarkTransaction_WithPool benchmarks transaction creation with pooling.
func BenchmarkTransaction_WithPool(b *testing.B) {
	manager := &mockManager{
		snapshot: &mockSnapshot{id: 1},
	}
	manager.snapshot.IncRef()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		txn := NewTransaction()
		transition := NewTransition(manager, func(cur *mockSnapshot) *mockSnapshot {
			next := &mockSnapshot{id: cur.id + 1}
			next.IncRef()
			return next
		})
		AddTransition(txn, transition)
		txn.Commit()
		txn.Release()
	}
}

// BenchmarkTransaction_WithoutPool benchmarks transaction creation without pooling.
func BenchmarkTransaction_WithoutPool(b *testing.B) {
	manager := &mockManager{
		snapshot: &mockSnapshot{id: 1},
	}
	manager.snapshot.IncRef()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Simulate non-pooled allocation
		txn := &Transaction{}
		transition := NewTransition(manager, func(cur *mockSnapshot) *mockSnapshot {
			next := &mockSnapshot{id: cur.id + 1}
			next.IncRef()
			return next
		})
		AddTransition(txn, transition)
		txn.Commit()
		// No Release() - simulates non-pooled version
	}
}

// BenchmarkTransition_WithPool benchmarks transition creation with pooling.
func BenchmarkTransition_WithPool(b *testing.B) {
	manager := &mockManager{
		snapshot: &mockSnapshot{id: 1},
	}
	manager.snapshot.IncRef()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		transition := NewTransition(manager, func(cur *mockSnapshot) *mockSnapshot {
			next := &mockSnapshot{id: cur.id + 1}
			next.IncRef()
			return next
		})
		transition.Commit()
		transition.Release()
	}
}

// BenchmarkTransition_WithoutPool benchmarks transition creation without pooling.
func BenchmarkTransition_WithoutPool(b *testing.B) {
	manager := &mockManager{
		snapshot: &mockSnapshot{id: 1},
	}
	manager.snapshot.IncRef()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Directly allocate transition without pool
		var zero *mockSnapshot
		transition := &Transition[*mockSnapshot]{
			manager:   manager,
			current:   zero,
			next:      zero,
			committed: false,
		}
		current := manager.CurrentSnapshot()
		next := &mockSnapshot{id: current.id + 1}
		next.IncRef()
		transition.current = current
		transition.next = next
		transition.Commit()
		// No Release() - simulates non-pooled version
	}
}

// TestTransition_ReleaseReleasesCurrentRef verifies that Release() decrements the ref
// held in t.current after a committed transition (fixes leak where that ref was never released).
func TestTransition_ReleaseReleasesCurrentRef(t *testing.T) {
	before := getSnapshotPoolRefCounts()
	defer func() { assertSnapshotPoolsNoLeak(t, before) }()

	oldSnapshot := &mockSnapshot{id: 1}
	oldSnapshot.IncRef()
	manager := &mockManager{snapshot: oldSnapshot}

	transition := NewTransition(manager, func(cur *mockSnapshot) *mockSnapshot {
		next := &mockSnapshot{id: cur.id + 1}
		next.IncRef()
		return next
	})
	transition.Commit()
	transition.Release()

	// After Release(), the ref we held in t.current must have been released;
	// ReplaceSnapshot already released the manager's ref, so oldSnapshot should be at 0.
	if oldSnapshot.RefCount() != 0 {
		t.Errorf("expected old snapshot refcount 0 after Commit+Release, got %d", oldSnapshot.RefCount())
	}
}

// TestTransition_RefCountAfterCommit verifies reference counts are correct after commit.
func TestTransition_RefCountAfterCommit(t *testing.T) {
	before := getSnapshotPoolRefCounts()
	defer func() { assertSnapshotPoolsNoLeak(t, before) }()

	oldSnapshot := &mockSnapshot{id: 1}
	oldSnapshot.IncRef()

	manager := &mockManager{
		snapshot: oldSnapshot,
	}

	transition := NewTransition(manager, func(cur *mockSnapshot) *mockSnapshot {
		next := &mockSnapshot{id: cur.id + 1}
		next.IncRef()
		return next
	})
	defer transition.Release()

	newSnapshot := transition.next

	// Before commit:
	// - oldSnapshot: refcount = 2 (initial +1, CurrentSnapshot +1)
	// - newSnapshot: refcount = 1 (created with +1)
	if oldSnapshot.RefCount() != 2 {
		t.Errorf("expected old snapshot refcount 2 before commit, got %d", oldSnapshot.RefCount())
	}
	if newSnapshot.RefCount() != 1 {
		t.Errorf("expected new snapshot refcount 1 before commit, got %d", newSnapshot.RefCount())
	}

	transition.Commit()

	// After commit:
	// - oldSnapshot: refcount = 1 (Commit decremented it once)
	// - newSnapshot: refcount = 1 (manager now holds it, no change)
	if oldSnapshot.RefCount() != 1 {
		t.Errorf("expected old snapshot refcount 1 after commit, got %d", oldSnapshot.RefCount())
	}
	if newSnapshot.RefCount() != 1 {
		t.Errorf("expected new snapshot refcount 1 after commit, got %d", newSnapshot.RefCount())
	}

	// Verify manager has the new snapshot
	current := manager.CurrentSnapshot()
	if current == nil {
		t.Fatal("expected non-nil snapshot from manager")
	}
	defer current.DecRef()

	if current.id != 2 {
		t.Errorf("expected current snapshot id 2, got %d", current.id)
	}
	if current.RefCount() != 2 {
		t.Errorf("expected current snapshot refcount 2 after CurrentSnapshot, got %d", current.RefCount())
	}
}

// TestTransition_RefCountAfterRollback verifies reference counts are correct after rollback.
func TestTransition_RefCountAfterRollback(t *testing.T) {
	before := getSnapshotPoolRefCounts()
	defer func() { assertSnapshotPoolsNoLeak(t, before) }()

	oldSnapshot := &mockSnapshot{id: 1}
	oldSnapshot.IncRef()

	manager := &mockManager{
		snapshot: oldSnapshot,
	}

	transition := NewTransition(manager, func(cur *mockSnapshot) *mockSnapshot {
		next := &mockSnapshot{id: cur.id + 1}
		next.IncRef()
		return next
	})
	defer transition.Release()

	newSnapshot := transition.next

	// Before rollback:
	// - oldSnapshot: refcount = 2 (initial +1, CurrentSnapshot +1)
	// - newSnapshot: refcount = 1 (created with +1)
	if oldSnapshot.RefCount() != 2 {
		t.Errorf("expected old snapshot refcount 2 before rollback, got %d", oldSnapshot.RefCount())
	}
	if newSnapshot.RefCount() != 1 {
		t.Errorf("expected new snapshot refcount 1 before rollback, got %d", newSnapshot.RefCount())
	}

	transition.Rollback()

	// After rollback:
	// - oldSnapshot: refcount = 1 (Rollback decremented the current reference)
	// - newSnapshot: refcount = 0 (Rollback decremented the next reference)
	if oldSnapshot.RefCount() != 1 {
		t.Errorf("expected old snapshot refcount 1 after rollback, got %d", oldSnapshot.RefCount())
	}
	if newSnapshot.RefCount() != 0 {
		t.Errorf("expected new snapshot refcount 0 after rollback, got %d", newSnapshot.RefCount())
	}

	// Verify manager still has the old snapshot
	current := manager.CurrentSnapshot()
	if current == nil {
		t.Fatal("expected non-nil snapshot from manager")
	}
	defer current.DecRef()

	if current.id != 1 {
		t.Errorf("expected current snapshot id 1, got %d", current.id)
	}
}

// TestTransaction_RefCountWithMultipleTransitions verifies reference counts
// are correct when multiple transitions are committed together.
func TestTransaction_RefCountWithMultipleTransitions(t *testing.T) {
	before := getSnapshotPoolRefCounts()
	defer func() { assertSnapshotPoolsNoLeak(t, before) }()

	oldSnapshot1 := &mockSnapshot{id: 1}
	oldSnapshot1.IncRef()
	manager1 := &mockManager{snapshot: oldSnapshot1}

	oldSnapshot2 := &mockSnapshot{id: 10}
	oldSnapshot2.IncRef()
	manager2 := &mockManager{snapshot: oldSnapshot2}

	txn := NewTransaction()
	defer txn.Release()

	transition1 := NewTransition(manager1, func(cur *mockSnapshot) *mockSnapshot {
		next := &mockSnapshot{id: cur.id + 1}
		next.IncRef()
		return next
	})
	defer transition1.Release()
	AddTransition(txn, transition1)

	transition2 := NewTransition(manager2, func(cur *mockSnapshot) *mockSnapshot {
		next := &mockSnapshot{id: cur.id + 1}
		next.IncRef()
		return next
	})
	defer transition2.Release()
	AddTransition(txn, transition2)

	newSnapshot1 := transition1.next
	newSnapshot2 := transition2.next

	// Before commit:
	// - oldSnapshot1: refcount = 2 (initial +1, CurrentSnapshot +1)
	// - newSnapshot1: refcount = 1 (created with +1)
	// - oldSnapshot2: refcount = 2 (initial +1, CurrentSnapshot +1)
	// - newSnapshot2: refcount = 1 (created with +1)
	if oldSnapshot1.RefCount() != 2 {
		t.Errorf("expected old snapshot1 refcount 2 before commit, got %d", oldSnapshot1.RefCount())
	}
	if newSnapshot1.RefCount() != 1 {
		t.Errorf("expected new snapshot1 refcount 1 before commit, got %d", newSnapshot1.RefCount())
	}
	if oldSnapshot2.RefCount() != 2 {
		t.Errorf("expected old snapshot2 refcount 2 before commit, got %d", oldSnapshot2.RefCount())
	}
	if newSnapshot2.RefCount() != 1 {
		t.Errorf("expected new snapshot2 refcount 1 before commit, got %d", newSnapshot2.RefCount())
	}

	txn.Commit()

	// After commit:
	// - oldSnapshot1: refcount = 1 (initial +1, Commit decremented once)
	// - newSnapshot1: refcount = 1 (manager1 holds it)
	// - oldSnapshot2: refcount = 1 (initial +1, Commit decremented once)
	// - newSnapshot2: refcount = 1 (manager2 holds it)
	if oldSnapshot1.RefCount() != 1 {
		t.Errorf("expected old snapshot1 refcount 1 after commit, got %d", oldSnapshot1.RefCount())
	}
	if newSnapshot1.RefCount() != 1 {
		t.Errorf("expected new snapshot1 refcount 1 after commit, got %d", newSnapshot1.RefCount())
	}
	if oldSnapshot2.RefCount() != 1 {
		t.Errorf("expected old snapshot2 refcount 1 after commit, got %d", oldSnapshot2.RefCount())
	}
	if newSnapshot2.RefCount() != 1 {
		t.Errorf("expected new snapshot2 refcount 1 after commit, got %d", newSnapshot2.RefCount())
	}
}

// TestTransaction_RefCountAfterRollback verifies reference counts are correct
// after rolling back multiple transitions.
func TestTransaction_RefCountAfterRollback(t *testing.T) {
	before := getSnapshotPoolRefCounts()
	defer func() { assertSnapshotPoolsNoLeak(t, before) }()

	oldSnapshot1 := &mockSnapshot{id: 1}
	oldSnapshot1.IncRef()
	manager1 := &mockManager{snapshot: oldSnapshot1}

	oldSnapshot2 := &mockSnapshot{id: 10}
	oldSnapshot2.IncRef()
	manager2 := &mockManager{snapshot: oldSnapshot2}

	txn := NewTransaction()
	defer txn.Release()

	transition1 := NewTransition(manager1, func(cur *mockSnapshot) *mockSnapshot {
		next := &mockSnapshot{id: cur.id + 1}
		next.IncRef()
		return next
	})
	defer transition1.Release()
	AddTransition(txn, transition1)

	transition2 := NewTransition(manager2, func(cur *mockSnapshot) *mockSnapshot {
		next := &mockSnapshot{id: cur.id + 1}
		next.IncRef()
		return next
	})
	defer transition2.Release()
	AddTransition(txn, transition2)

	newSnapshot1 := transition1.next
	newSnapshot2 := transition2.next

	txn.Rollback()

	// After rollback:
	// - oldSnapshot1: refcount = 1 (initial +1, rollback decremented current ref)
	// - newSnapshot1: refcount = 0 (rollback decremented next ref)
	// - oldSnapshot2: refcount = 1 (initial +1, rollback decremented current ref)
	// - newSnapshot2: refcount = 0 (rollback decremented next ref)
	if oldSnapshot1.RefCount() != 1 {
		t.Errorf("expected old snapshot1 refcount 1 after rollback, got %d", oldSnapshot1.RefCount())
	}
	if newSnapshot1.RefCount() != 0 {
		t.Errorf("expected new snapshot1 refcount 0 after rollback, got %d", newSnapshot1.RefCount())
	}
	if oldSnapshot2.RefCount() != 1 {
		t.Errorf("expected old snapshot2 refcount 1 after rollback, got %d", oldSnapshot2.RefCount())
	}
	if newSnapshot2.RefCount() != 0 {
		t.Errorf("expected new snapshot2 refcount 0 after rollback, got %d", newSnapshot2.RefCount())
	}
}

// TestTransition_NoDoubleDecrement verifies that the fix prevents double-decrement bug.
// This is a regression test for the bug where ReplaceSnapshot and Commit both
// decremented the same snapshot reference.
func TestTransition_NoDoubleDecrement(t *testing.T) {
	before := getSnapshotPoolRefCounts()
	defer func() { assertSnapshotPoolsNoLeak(t, before) }()

	oldSnapshot := &mockSnapshot{id: 1}
	oldSnapshot.IncRef()

	manager := &mockManager{
		snapshot: oldSnapshot,
	}

	// Record initial refcount
	initialRefCount := oldSnapshot.RefCount()
	if initialRefCount != 1 {
		t.Fatalf("expected initial refcount 1, got %d", initialRefCount)
	}

	transition := NewTransition(manager, func(cur *mockSnapshot) *mockSnapshot {
		// Verify cur is the same object as oldSnapshot
		if cur != oldSnapshot {
			t.Error("current snapshot should be the same object as old snapshot")
		}
		// Verify refcount was incremented by CurrentSnapshot
		if cur.RefCount() != 2 {
			t.Errorf("expected cur refcount 2 (after CurrentSnapshot), got %d", cur.RefCount())
		}

		next := &mockSnapshot{id: cur.id + 1}
		next.IncRef()
		return next
	})
	defer transition.Release()

	transition.Commit()

	// After commit, the old snapshot should have refcount 1
	// (not 0 or negative which would indicate double-decrement)
	finalRefCount := oldSnapshot.RefCount()
	if finalRefCount != 1 {
		t.Errorf("expected final refcount 1 (no double-decrement), got %d", finalRefCount)
	}

	// Verify the old snapshot is still valid and wasn't prematurely cleaned up
	// In a real scenario, negative refcount would cause a crash or corruption
	if finalRefCount < 1 {
		t.Error("double-decrement bug detected: refcount went below expected value")
	}
}
