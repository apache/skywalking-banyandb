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

package fadvis

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/stretchr/testify/assert"
)

func TestSetGetThreshold(t *testing.T) {
	// Save original threshold
	originalThreshold := GetThreshold()
	defer SetThreshold(originalThreshold) // Restore after test

	// Test setting different thresholds
	testCases := []struct {
		name      string
		threshold int64
		expected  int64
	}{
		{"Set positive value", 32 * 1024 * 1024, 32 * 1024 * 1024},
		{"Set zero value", 0, originalThreshold},        // Should not change if value is 0
		{"Set negative value", -100, originalThreshold}, // Should not change if value is negative
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			SetThreshold(tc.threshold)
			assert.Equal(t, tc.expected, GetThreshold(), "Threshold should be set correctly")
		})
	}
}

func TestMemoryProtectorIntegration(t *testing.T) {
	// Mock memory protector
	mp := protector.NewMemory(nil)

	// Test setting memory protector
	SetMemoryProtector(mp)

	// Test threshold from protector
	expectedThreshold := mp.GetThreshold()
	assert.True(t, expectedThreshold > 0, "Threshold from Memory protector should be positive")
	assert.Equal(t, expectedThreshold, GetThreshold(), "Threshold should be set from Memory protector")
}

func TestApplyIfLarge(t *testing.T) {
	// Create a temporary directory for test files
	tempDir, err := os.MkdirTemp("", "fadvis-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create test files
	smallFileName := filepath.Join(tempDir, "small_file.dat")
	smallSize := int64(1024) // 1KB
	createTestFile(t, smallFileName, smallSize)

	largeFileName := filepath.Join(tempDir, "large_file.dat")
	largeSize := int64(10 * 1024 * 1024) // 10MB
	createTestFile(t, largeFileName, largeSize)

	// Set a threshold between the two file sizes
	testThreshold := int64(5 * 1024 * 1024) // 5MB
	originalThreshold := GetThreshold()
	SetThreshold(testThreshold)
	defer SetThreshold(originalThreshold) // Restore after test

	// Test applying fadvis to both files
	// Note: We can't directly test the effect of fadvis, but we can ensure the code runs without errors
	ApplyIfLarge(smallFileName) // Should not apply fadvis
	ApplyIfLarge(largeFileName) // Should apply fadvis

	// Test MustApplyIfLarge (should not panic)
	assert.NotPanics(t, func() {
		MustApplyIfLarge(smallFileName)
		MustApplyIfLarge(largeFileName)
	}, "MustApplyIfLarge should not panic")
}

// Helper function to create a test file with specified size
func createTestFile(t *testing.T, filename string, size int64) {
	t.Helper()

	file, err := os.Create(filename)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}
	defer file.Close()

	// Create a file of the specified size
	err = file.Truncate(size)
	if err != nil {
		t.Fatalf("Failed to resize test file: %v", err)
	}
}
