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

package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/banyand/queue"
)

// TestChunkedSyncPartTypeBehavior tests the NewPartType handler invocation behavior
// when partCtx is created with different ID and PartType combinations.
func TestChunkedSyncPartTypeBehavior(t *testing.T) {
	tests := []struct {
		name                    string
		firstPartType           string
		secondPartType          string
		description             string
		firstPartID             uint64
		secondPartID            uint64
		expectNewPartTypeCalled bool
	}{
		{
			name:                    "same_id_different_parttype",
			firstPartID:             123,
			firstPartType:           "primary",
			secondPartID:            123,         // Same ID
			secondPartType:          "secondary", // Different PartType
			expectNewPartTypeCalled: true,
			description:             "NewPartType should be called when ID is same but PartType changes",
		},
		{
			name:                    "same_id_same_parttype",
			firstPartID:             456,
			firstPartType:           "primary",
			secondPartID:            456,       // Same ID
			secondPartType:          "primary", // Same PartType
			expectNewPartTypeCalled: false,
			description:             "NewPartType should NOT be called when both ID and PartType are same",
		},
		{
			name:                    "different_id_same_parttype",
			firstPartID:             789,
			firstPartType:           "primary",
			secondPartID:            790,       // Different ID
			secondPartType:          "primary", // Same PartType
			expectNewPartTypeCalled: false,
			description:             "NewPartType should NOT be called when ID changes (CreatePartHandler is called instead)",
		},
		{
			name:                    "different_id_different_parttype",
			firstPartID:             111,
			firstPartType:           "primary",
			secondPartID:            222,         // Different ID
			secondPartType:          "secondary", // Different PartType
			expectNewPartTypeCalled: false,
			description:             "NewPartType should NOT be called when ID changes (CreatePartHandler is called instead)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setup := setupChunkedSyncTestWithTracking(t, "parttype-test-"+tt.name)
			defer cleanupTestSetup(setup.Setup)

			// Create first part
			firstPartFiles := map[string][]byte{
				"first-file.txt": []byte("First part data"),
			}
			firstPart := createTestDataPartWithPartType(
				firstPartFiles,
				"test-group",
				1, // shardID
				tt.firstPartID,
				tt.firstPartType,
			)

			// Create second part
			secondPartFiles := map[string][]byte{
				"second-file.txt": []byte("Second part data"),
			}
			secondPart := createTestDataPartWithPartType(
				secondPartFiles,
				"test-group",
				1, // shardID
				tt.secondPartID,
				tt.secondPartType,
			)

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			// Send both parts in a single session to test NewPartType behavior
			bothParts := []queue.StreamingPartData{firstPart, secondPart}
			result, err := setup.ChunkedClient.SyncStreamingParts(ctx, bothParts)
			require.NoError(t, err, "Both parts should sync successfully")
			require.True(t, result.Success, "Both parts sync should be successful")

			time.Sleep(500 * time.Millisecond)

			// Verify NewPartType call behavior
			if tt.expectNewPartTypeCalled {
				assert.True(t, setup.TrackingHandler.NewPartTypeCalled(),
					"NewPartType should have been called: %s", tt.description)
				newPartTypeContexts := setup.TrackingHandler.GetNewPartTypeContexts()
				assert.Len(t, newPartTypeContexts, 1, "NewPartType should have been called exactly once")
				if len(newPartTypeContexts) > 0 {
					ctx := newPartTypeContexts[0]
					assert.Equal(t, tt.secondPartID, ctx.ID, "NewPartType should be called with correct part ID")
					assert.Equal(t, tt.secondPartType, ctx.PartType, "NewPartType should be called with correct part type")
				}
			} else {
				assert.False(t, setup.TrackingHandler.NewPartTypeCalled(),
					"NewPartType should NOT have been called: %s", tt.description)
				newPartTypeContexts := setup.TrackingHandler.GetNewPartTypeContexts()
				assert.Len(t, newPartTypeContexts, 0, "NewPartType should not have been called")
			}

			// Verify that both parts were processed successfully
			receivedFiles := setup.MockHandler.GetReceivedFiles()
			expectedFileCount := len(firstPartFiles) + len(secondPartFiles)
			assert.Len(t, receivedFiles, expectedFileCount, "Should have received all files from both parts")

			// Verify file contents
			for fileName, expectedContent := range firstPartFiles {
				receivedContent, exists := receivedFiles[fileName]
				require.True(t, exists, "File %s should be received", fileName)
				assert.Equal(t, expectedContent, receivedContent, "Content for file %s should match", fileName)
			}
			for fileName, expectedContent := range secondPartFiles {
				receivedContent, exists := receivedFiles[fileName]
				require.True(t, exists, "File %s should be received", fileName)
				assert.Equal(t, expectedContent, receivedContent, "Content for file %s should match", fileName)
			}

			completedParts := setup.MockHandler.GetCompletedParts()
			// When parts have the same ID, they share the same part context, so only one FinishSync is called
			expectedCompletedParts := 1
			if tt.firstPartID != tt.secondPartID {
				expectedCompletedParts = 2 // Different IDs create separate part contexts
			}
			assert.Len(t, completedParts, expectedCompletedParts, "Should have completed %d parts", expectedCompletedParts)
			for i, part := range completedParts {
				assert.True(t, part.finishSyncCalled, "FinishSync should have been called for part %d", i+1)
				assert.True(t, part.closeCalled, "Close should have been called for part %d", i+1)
			}

			t.Logf("PartType behavior test '%s' completed successfully:", tt.name)
			t.Logf("  - Session ID: %s", result.SessionID)
			t.Logf("  - First part: ID=%d, PartType=%s", tt.firstPartID, tt.firstPartType)
			t.Logf("  - Second part: ID=%d, PartType=%s", tt.secondPartID, tt.secondPartType)
			t.Logf("  - NewPartType called: %v", setup.TrackingHandler.NewPartTypeCalled())
			t.Logf("  - Files received: %d", len(receivedFiles))
			t.Logf("  - Parts completed: %d", len(completedParts))
			t.Logf("  - Total bytes sent: %d", result.TotalBytes)
		})
	}
}

// TestChunkedSyncMultiplePartTypesInSequence tests multiple part types with same ID in sequence.
func TestChunkedSyncMultiplePartTypesInSequence(t *testing.T) {
	setup := setupChunkedSyncTestWithTracking(t, "multiple-parttypes-sequence")
	defer cleanupTestSetup(setup.Setup)

	partID := uint64(999)
	partTypes := []string{"primary", "secondary", "tertiary", "primary"} // Last one is same as first
	expectedNewPartTypeCalls := 3                                        // Should be called for secondary, tertiary, and primary (when it changes back)

	var allFiles []queue.StreamingPartData
	for i, partType := range partTypes {
		partFiles := map[string][]byte{
			fmt.Sprintf("part-%d-%s.txt", i+1, partType): []byte(fmt.Sprintf("Data for part %d with type %s", i+1, partType)),
		}
		part := createTestDataPartWithPartType(
			partFiles,
			"test-group-multi",
			1, // shardID
			partID,
			partType,
		)
		allFiles = append(allFiles, part)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Send all parts in a single session to test NewPartType behavior
	result, err := setup.ChunkedClient.SyncStreamingParts(ctx, allFiles)
	require.NoError(t, err, "All parts should sync successfully")
	require.True(t, result.Success, "All parts sync should be successful")

	time.Sleep(500 * time.Millisecond)

	// Verify NewPartType was called for each part type change
	actualNewPartTypeCalls := 0
	for i := 1; i < len(partTypes); i++ {
		if partTypes[i] != partTypes[i-1] {
			actualNewPartTypeCalls++
		}
	}

	newPartTypeContexts := setup.TrackingHandler.GetNewPartTypeContexts()
	assert.Len(t, newPartTypeContexts, actualNewPartTypeCalls,
		"NewPartType should have been called %d times for part type changes", actualNewPartTypeCalls)

	// Verify each NewPartType call has the correct context
	newPartTypeIndex := 0
	for i := 1; i < len(partTypes); i++ {
		if partTypes[i] != partTypes[i-1] {
			require.Less(t, newPartTypeIndex, len(newPartTypeContexts),
				"Should have NewPartType context for part %d", i+1)
			ctx := newPartTypeContexts[newPartTypeIndex]
			assert.Equal(t, partID, ctx.ID, "NewPartType should be called with correct part ID for part %d", i+1)
			assert.Equal(t, partTypes[i], ctx.PartType, "NewPartType should be called with correct part type for part %d", i+1)
			newPartTypeIndex++
		}
	}

	// Verify all files were received
	receivedFiles := setup.MockHandler.GetReceivedFiles()
	expectedFileCount := len(allFiles)
	assert.Len(t, receivedFiles, expectedFileCount, "Should have received all files from all parts")

	// Verify file contents
	for _, part := range allFiles {
		for _, fileInfo := range part.Files {
			receivedContent, exists := receivedFiles[fileInfo.Name]
			require.True(t, exists, "File %s should be received", fileInfo.Name)
			assert.NotEmpty(t, receivedContent, "Content for file %s should not be empty", fileInfo.Name)
		}
	}

	completedParts := setup.MockHandler.GetCompletedParts()
	// All parts have the same ID, so they share the same part context - only one FinishSync is called
	assert.Len(t, completedParts, 1, "Should have completed one part (all parts share same context due to same ID)")
	for i, part := range completedParts {
		assert.True(t, part.finishSyncCalled, "FinishSync should have been called for part %d", i+1)
		assert.True(t, part.closeCalled, "Close should have been called for part %d", i+1)
	}

	t.Logf("Multiple part types sequence test completed successfully:")
	t.Logf("  - Parts sent: %d", len(allFiles))
	t.Logf("  - Part types: %v", partTypes)
	t.Logf("  - Expected NewPartType calls: %d", expectedNewPartTypeCalls)
	t.Logf("  - Files received: %d", len(receivedFiles))
	t.Logf("  - Parts completed: %d", len(completedParts))
}
