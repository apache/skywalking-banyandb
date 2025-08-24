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

func TestBlock_BasicOperations(t *testing.T) {
	// Test block creation and basic operations
	b := generateBlock()
	defer releaseBlock(b)

	// Test empty block
	if !b.isEmpty() {
		t.Error("New block should be empty")
	}

	if b.Len() != 0 {
		t.Error("New block should have length 0")
	}

	if b.isFull() {
		t.Error("Empty block should not be full")
	}
}

func TestBlock_Validation(t *testing.T) {
	b := generateBlock()
	defer releaseBlock(b)

	// Test empty block validation
	if err := b.validate(); err != nil {
		t.Errorf("Empty block should validate: %v", err)
	}

	// Add inconsistent data
	b.userKeys = append(b.userKeys, 100)
	b.data = append(b.data, []byte("dummy"), []byte("dummy2"))

	// Should fail validation
	if err := b.validate(); err == nil {
		t.Error("Block with inconsistent arrays should fail validation")
	}
}

func TestBlock_SizeCalculation(t *testing.T) {
	b := generateBlock()
	defer releaseBlock(b)

	// Empty block should have zero size
	if size := b.uncompressedSizeBytes(); size != 0 {
		t.Errorf("Empty block should have size 0, got %d", size)
	}

	// Add some data
	b.userKeys = append(b.userKeys, 100, 200)
	b.data = append(b.data, []byte("test1"), []byte("test2"))

	// Should have non-zero size
	if size := b.uncompressedSizeBytes(); size == 0 {
		t.Error("Block with data should have non-zero size")
	}
}

func TestBlock_IsFull(t *testing.T) {
	b := generateBlock()
	defer releaseBlock(b)

	// Empty block should not be full
	if b.isFull() {
		t.Error("Empty block should not be full")
	}

	// Add elements up to the limit
	for i := 0; i < maxElementsPerBlock-1; i++ {
		b.userKeys = append(b.userKeys, int64(i))
		b.data = append(b.data, []byte("data"))
	}

	// Should not be full yet
	if b.isFull() {
		t.Error("Block should not be full with maxElementsPerBlock-1 elements")
	}

	// Add one more element to reach the limit
	b.userKeys = append(b.userKeys, int64(maxElementsPerBlock))
	b.data = append(b.data, []byte("data"))

	// Should now be full
	if !b.isFull() {
		t.Error("Block should be full with maxElementsPerBlock elements")
	}
}

func TestBlock_KeyRange(t *testing.T) {
	b := generateBlock()
	defer releaseBlock(b)

	// Empty block should return zero range
	minKey, maxKey := b.getKeyRange()
	if minKey != 0 || maxKey != 0 {
		t.Errorf("Empty block should return (0,0), got (%d,%d)", minKey, maxKey)
	}

	// Add keys
	b.userKeys = append(b.userKeys, 100, 200, 150)

	minKey, maxKey = b.getKeyRange()
	if minKey != 100 || maxKey != 150 {
		t.Errorf("Expected range (100,150), got (%d,%d)", minKey, maxKey)
	}
}

func TestBlock_MemoryManagement(t *testing.T) {
	b := generateBlock()
	defer releaseBlock(b)

	// Add some normal-sized data
	b.data = append(b.data, make([]byte, 100), make([]byte, 200))

	// Add an oversized slice (larger than maxPooledSliceSize)
	oversizedData := make([]byte, maxPooledSliceSize+1)
	b.data = append(b.data, oversizedData)

	// Reset should handle both normal and oversized slices correctly
	b.reset()

	// After reset, data slice should be empty but not nil (since the outer slice is within limits)
	if b.data == nil {
		t.Error("Data slice should not be nil after reset when within count limits")
	}

	if len(b.data) != 0 {
		t.Errorf("Data slice should be empty after reset, got length %d", len(b.data))
	}
}
