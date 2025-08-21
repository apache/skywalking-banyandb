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
	"testing"
)

func TestSnapshot_Creation(t *testing.T) {
	// Create test parts with different key ranges
	parts := createTestParts(t, []keyRange{
		{minKey: 100, maxKey: 199, id: 1},
		{minKey: 200, maxKey: 299, id: 2},
		{minKey: 300, maxKey: 399, id: 3},
	})
	defer cleanupTestParts(parts)

	// Create snapshot
	epoch := uint64(1001)
	snapshot := newSnapshot(parts, epoch)
	defer snapshot.release()

	// Verify snapshot properties
	if snapshot.getEpoch() != epoch {
		t.Errorf("expected epoch %d, got %d", epoch, snapshot.getEpoch())
	}

	if snapshot.refCount() != 1 {
		t.Errorf("expected ref count 1, got %d", snapshot.refCount())
	}

	if snapshot.getPartCount() != len(parts) {
		t.Errorf("expected part count %d, got %d", len(parts), snapshot.getPartCount())
	}

	if snapshot.isReleased() {
		t.Error("snapshot should not be released")
	}
}

func TestSnapshot_ReferenceCountingBasic(t *testing.T) {
	parts := createTestParts(t, []keyRange{{minKey: 100, maxKey: 199, id: 1}})
	defer cleanupTestParts(parts)

	snapshot := newSnapshot(parts, 1001)

	// Test acquire
	if !snapshot.acquire() {
		t.Error("acquire should succeed")
	}
	if snapshot.refCount() != 2 {
		t.Errorf("expected ref count 2, got %d", snapshot.refCount())
	}

	// Test release
	snapshot.release()
	if snapshot.refCount() != 1 {
		t.Errorf("expected ref count 1, got %d", snapshot.refCount())
	}

	// Check state before final release
	isReleasedBefore := snapshot.isReleased()
	if isReleasedBefore {
		t.Error("snapshot should not be released before final release")
	}

	// Final release should clean up
	snapshot.release()

	// After final release, the snapshot object may be reset and returned to pool
	// so we can't reliably check its state. The important thing is that it
	// doesn't crash and the cleanup happens properly.
}

func TestSnapshot_ReferenceCountingConcurrent(t *testing.T) {
	parts := createTestParts(t, []keyRange{{minKey: 100, maxKey: 199, id: 1}})
	defer cleanupTestParts(parts)

	snapshot := newSnapshot(parts, 1001)
	defer snapshot.release()

	// Simulate concurrent access
	const numGoroutines = 10
	acquired := make(chan bool, numGoroutines)
	released := make(chan bool, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			if snapshot.acquire() {
				acquired <- true
				// Simulate some work
				snapshot.release()
				released <- true
			} else {
				acquired <- false
			}
		}()
	}

	// Wait for all goroutines
	successCount := 0
	for i := 0; i < numGoroutines; i++ {
		if <-acquired {
			successCount++
		}
	}

	// Wait for releases
	for i := 0; i < successCount; i++ {
		<-released
	}

	// All references should be released except the original one
	if snapshot.refCount() != 1 {
		t.Errorf("expected ref count 1, got %d", snapshot.refCount())
	}
}

func TestSnapshot_GetPartsByKeyRange(t *testing.T) {
	parts := createTestParts(t, []keyRange{
		{minKey: 100, maxKey: 199, id: 1},
		{minKey: 200, maxKey: 299, id: 2},
		{minKey: 300, maxKey: 399, id: 3},
		{minKey: 400, maxKey: 499, id: 4},
	})
	defer cleanupTestParts(parts)

	snapshot := newSnapshot(parts, 1001)
	defer snapshot.release()

	tests := []struct {
		name     string
		expected []uint64
		maxKey   int64
		minKey   int64
	}{
		{
			name:     "exact match single part",
			expected: []uint64{1},
			maxKey:   199,
			minKey:   100,
		},
		{
			name:     "overlap multiple parts",
			expected: []uint64{1, 2, 3},
			maxKey:   350,
			minKey:   150,
		},
		{
			name:     "no overlap",
			expected: []uint64{},
			maxKey:   99,
			minKey:   50,
		},
		{
			name:     "partial overlap at boundaries",
			expected: []uint64{1, 2},
			maxKey:   200,
			minKey:   199,
		},
		{
			name:     "covers all parts",
			expected: []uint64{1, 2, 3, 4},
			maxKey:   550,
			minKey:   50,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := snapshot.getParts(tt.minKey, tt.maxKey)

			if len(result) != len(tt.expected) {
				t.Errorf("expected %d parts, got %d", len(tt.expected), len(result))
				return
			}

			// Verify the returned part IDs match expected
			resultIDs := make([]uint64, len(result))
			for i, pw := range result {
				resultIDs[i] = pw.ID()
			}

			for _, expectedID := range tt.expected {
				found := false
				for _, resultID := range resultIDs {
					if resultID == expectedID {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("expected part ID %d not found in results", expectedID)
				}
			}
		})
	}
}

func TestSnapshot_GetPartsAll(t *testing.T) {
	parts := createTestParts(t, []keyRange{
		{minKey: 100, maxKey: 199, id: 1},
		{minKey: 200, maxKey: 299, id: 2},
		{minKey: 300, maxKey: 399, id: 3},
	})
	defer cleanupTestParts(parts)

	snapshot := newSnapshot(parts, 1001)
	defer snapshot.release()

	allParts := snapshot.getPartsAll()
	if len(allParts) != len(parts) {
		t.Errorf("expected %d parts, got %d", len(parts), len(allParts))
	}

	// Mark one part for removal
	parts[1].markForRemoval()

	// Should still return all parts since they're in the snapshot
	allParts = snapshot.getPartsAll()
	activeCount := 0
	for _, pw := range allParts {
		if pw.isActive() {
			activeCount++
		}
	}
	if activeCount != len(parts)-1 {
		t.Errorf("expected %d active parts, got %d", len(parts)-1, activeCount)
	}
}

func TestSnapshot_Validation(t *testing.T) {
	parts := createTestParts(t, []keyRange{
		{minKey: 100, maxKey: 199, id: 1},
		{minKey: 200, maxKey: 299, id: 2},
	})
	defer cleanupTestParts(parts)

	snapshot := newSnapshot(parts, 1001)

	// Valid snapshot should pass validation
	if err := snapshot.validate(); err != nil {
		t.Errorf("valid snapshot failed validation: %v", err)
	}

	// Released snapshot should fail validation
	snapshot.release()
	if err := snapshot.validate(); err == nil {
		t.Error("released snapshot should fail validation")
	}
}

func TestSnapshot_String(t *testing.T) {
	parts := createTestParts(t, []keyRange{
		{minKey: 100, maxKey: 199, id: 1},
		{minKey: 200, maxKey: 299, id: 2},
	})
	defer cleanupTestParts(parts)

	snapshot := newSnapshot(parts, 1001)
	defer snapshot.release()

	str := snapshot.String()
	if str == "" {
		t.Error("String() should return non-empty string")
	}

	// String should contain epoch and part count
	if !contains(str, "epoch=1001") {
		t.Error("String() should contain epoch")
	}
	if !contains(str, "parts=2") {
		t.Error("String() should contain part count")
	}
}

func TestSnapshot_PartManagement(t *testing.T) {
	// Create parts
	parts := createTestParts(t, []keyRange{
		{minKey: 100, maxKey: 199, id: 1},
		{minKey: 200, maxKey: 299, id: 2},
	})
	defer cleanupTestParts(parts)

	snapshot := newSnapshot(parts, 1001)
	defer snapshot.release()

	initialCount := snapshot.getPartCount()
	if initialCount != 2 {
		t.Errorf("expected 2 parts, got %d", initialCount)
	}

	// Remove a part by ID
	snapshot.removePart(parts[1].ID())

	// Active count should be less
	activeCount := snapshot.getPartCount()
	if activeCount != 1 {
		t.Errorf("expected 1 active part after removal, got %d", activeCount)
	}

	// All parts are still in the snapshot
	allParts := snapshot.getPartsAll()
	if len(allParts) != 1 { // Only active parts are returned
		t.Errorf("expected 1 active part in getPartsAll, got %d", len(allParts))
	}
}

func TestSnapshot_PoolReuse(t *testing.T) {
	// Test that snapshots are properly reused from the pool
	parts := createTestParts(t, []keyRange{{minKey: 100, maxKey: 199, id: 1}})
	defer cleanupTestParts(parts)

	// Create and release a snapshot
	snapshot1 := newSnapshot(parts, 1001)
	snapshot1.release()

	// Create another snapshot - should potentially reuse the same object
	snapshot2 := newSnapshot(parts, 1002)
	defer snapshot2.release()

	// Should be clean state
	if snapshot2.getEpoch() != 1002 {
		t.Errorf("expected epoch 1002, got %d", snapshot2.getEpoch())
	}
	if snapshot2.refCount() != 1 {
		t.Errorf("expected ref count 1, got %d", snapshot2.refCount())
	}
	if snapshot2.isReleased() {
		t.Error("new snapshot should not be released")
	}
}

// Helper types and functions.

type keyRange struct {
	minKey int64
	maxKey int64
	id     uint64
}

func createTestParts(_ *testing.T, ranges []keyRange) []*partWrapper {
	parts := make([]*partWrapper, len(ranges))

	for i, kr := range ranges {
		// Create a minimal part with metadata
		pm := generatePartMetadata()
		pm.MinKey = kr.minKey
		pm.MaxKey = kr.maxKey
		pm.ID = kr.id
		pm.TotalCount = 10 // dummy value
		pm.BlocksCount = 1 // dummy value

		part := &part{
			partMetadata: pm,
			path:         "",
		}

		parts[i] = newPartWrapper(part)
	}

	return parts
}

func cleanupTestParts(parts []*partWrapper) {
	for _, pw := range parts {
		if pw != nil && !pw.isRemoved() {
			pw.release()
		}
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && s[len(s)-len(substr):] == substr ||
		(len(s) > len(substr) && anySubstring(s, substr))
}

func anySubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
