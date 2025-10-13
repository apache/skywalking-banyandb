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

package trace

import (
	"context"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

// simpleMockClient is a minimal mock for testing replay functionality
type simpleMockClient struct {
	mu           sync.Mutex
	healthyNodes []string
	sendCalled   int
	sendError    error
}

func newSimpleMockClient(healthyNodes []string) *simpleMockClient {
	return &simpleMockClient{
		healthyNodes: healthyNodes,
	}
}

func (m *simpleMockClient) HealthyNodes() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.healthyNodes
}

func (m *simpleMockClient) NewChunkedSyncClient(node string, chunkSize uint32) (queue.ChunkedSyncClient, error) {
	return &simpleMockChunkedClient{
		mockClient: m,
		node:       node,
	}, nil
}

func (m *simpleMockClient) getSendCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.sendCalled
}

func (m *simpleMockClient) resetSendCount() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sendCalled = 0
}

// simpleMockChunkedClient is a minimal mock for ChunkedSyncClient
type simpleMockChunkedClient struct {
	mockClient *simpleMockClient
	node       string
}

func (m *simpleMockChunkedClient) SyncStreamingParts(ctx context.Context, parts []queue.StreamingPartData) (*queue.SyncResult, error) {
	m.mockClient.mu.Lock()
	m.mockClient.sendCalled++
	sendError := m.mockClient.sendError
	m.mockClient.mu.Unlock()

	if sendError != nil {
		return nil, sendError
	}
	return &queue.SyncResult{
		Success:    true,
		SessionID:  "test-session",
		TotalBytes: 1000,
		PartsCount: uint32(len(parts)),
	}, nil
}

func (m *simpleMockChunkedClient) Close() error {
	return nil
}

// TestHandoffController_InFlightTracking tests the in-flight tracking mechanism
func TestHandoffController_InFlightTracking(t *testing.T) {
	tempDir, defFn := test.Space(require.New(t))
	defer defFn()

	fileSystem := fs.NewLocalFileSystem()
	l := logger.GetLogger("test")

	nodeAddr := "node1.example.com:17912"
	controller, err := newHandoffController(fileSystem, tempDir, nil, []string{nodeAddr}, 0, l)
	require.NoError(t, err)
	defer controller.close()
	partID := uint64(0x10)

	// Initially not in-flight
	assert.False(t, controller.isInFlight(nodeAddr, partID))

	// Mark as in-flight
	controller.markInFlight(nodeAddr, partID, true)
	assert.True(t, controller.isInFlight(nodeAddr, partID))

	// Mark as not in-flight
	controller.markInFlight(nodeAddr, partID, false)
	assert.False(t, controller.isInFlight(nodeAddr, partID))

	// Test multiple parts
	partID2 := uint64(0x11)
	controller.markInFlight(nodeAddr, partID, true)
	controller.markInFlight(nodeAddr, partID2, true)
	assert.True(t, controller.isInFlight(nodeAddr, partID))
	assert.True(t, controller.isInFlight(nodeAddr, partID2))

	// Remove one
	controller.markInFlight(nodeAddr, partID, false)
	assert.False(t, controller.isInFlight(nodeAddr, partID))
	assert.True(t, controller.isInFlight(nodeAddr, partID2))
}

// TestHandoffController_GetNodesWithPendingParts tests the node enumeration
func TestHandoffController_GetNodesWithPendingParts(t *testing.T) {
	tempDir, defFn := test.Space(require.New(t))
	defer defFn()

	fileSystem := fs.NewLocalFileSystem()
	l := logger.GetLogger("test")

	// Create source part
	sourceRoot := filepath.Join(tempDir, "source")
	fileSystem.MkdirIfNotExist(sourceRoot, storage.DirPerm)
	partID := uint64(0x12)
	sourcePath := createTestPart(t, fileSystem, sourceRoot, partID)

	nodeAddr1 := "node1.example.com:17912"
	nodeAddr2 := "node2.example.com:17912"
	controller, err := newHandoffController(fileSystem, tempDir, nil, []string{nodeAddr1, nodeAddr2}, 0, l)
	require.NoError(t, err)
	defer controller.close()

	// Initially no nodes with pending parts
	nodes := controller.getNodesWithPendingParts()
	assert.Empty(t, nodes)

	// Enqueue for one node
	err = controller.enqueueForNode(nodeAddr1, partID, PartTypeCore, sourcePath, "default", 0)
	require.NoError(t, err)

	nodes = controller.getNodesWithPendingParts()
	assert.Len(t, nodes, 1)
	assert.Contains(t, nodes, nodeAddr1)

	// Enqueue for another node
	partID2 := uint64(0x13)
	sourcePath2 := createTestPart(t, fileSystem, sourceRoot, partID2)
	err = controller.enqueueForNode(nodeAddr2, partID2, PartTypeCore, sourcePath2, "default", 0)
	require.NoError(t, err)

	nodes = controller.getNodesWithPendingParts()
	assert.Len(t, nodes, 2)
	assert.Contains(t, nodes, nodeAddr1)
	assert.Contains(t, nodes, nodeAddr2)

	// Complete one node's parts
	err = controller.completeSend(nodeAddr1, partID, PartTypeCore)
	require.NoError(t, err)

	nodes = controller.getNodesWithPendingParts()
	assert.Len(t, nodes, 1)
	assert.Contains(t, nodes, nodeAddr2)
}

// TestHandoffController_ReadPartFromHandoff tests reading parts from handoff storage
func TestHandoffController_ReadPartFromHandoff(t *testing.T) {
	tempDir, defFn := test.Space(require.New(t))
	defer defFn()

	fileSystem := fs.NewLocalFileSystem()
	l := logger.GetLogger("test")

	// Create source part
	sourceRoot := filepath.Join(tempDir, "source")
	fileSystem.MkdirIfNotExist(sourceRoot, storage.DirPerm)
	partID := uint64(0x14)
	sourcePath := createTestPart(t, fileSystem, sourceRoot, partID)

	nodeAddr := "node1.example.com:17912"
	controller, err := newHandoffController(fileSystem, tempDir, nil, []string{nodeAddr}, 0, l)
	require.NoError(t, err)
	defer controller.close()

	// Enqueue a part
	err = controller.enqueueForNode(nodeAddr, partID, PartTypeCore, sourcePath, "default", 0)
	require.NoError(t, err)

	// Read the part back
	streamingPart, release, err := controller.readPartFromHandoff(nodeAddr, partID, PartTypeCore)
	require.NoError(t, err)
	require.NotNil(t, streamingPart)
	release()

	// Verify basic fields
	assert.Equal(t, partID, streamingPart.ID)
	assert.Equal(t, "default", streamingPart.Group)
	assert.Equal(t, PartTypeCore, streamingPart.PartType)
	assert.NotEmpty(t, streamingPart.Files)
}

// TestHandoffController_ReplayBatchForNode tests the batch replay logic
func TestHandoffController_ReplayBatchForNode(t *testing.T) {
	tempDir, defFn := test.Space(require.New(t))
	defer defFn()

	fileSystem := fs.NewLocalFileSystem()
	l := logger.GetLogger("test")

	nodeAddr := "node1.example.com:17912"
	// Create mock client
	mockClient := newSimpleMockClient([]string{nodeAddr})

	// Create source parts
	sourceRoot := filepath.Join(tempDir, "source")
	fileSystem.MkdirIfNotExist(sourceRoot, storage.DirPerm)

	controller, err := newHandoffController(fileSystem, tempDir, mockClient, []string{nodeAddr}, 0, l)
	require.NoError(t, err)
	defer controller.close()

	// Enqueue multiple parts
	numParts := 3
	for i := 0; i < numParts; i++ {
		partID := uint64(0x20 + i)
		sourcePath := createTestPart(t, fileSystem, sourceRoot, partID)
		err = controller.enqueueForNode(nodeAddr, partID, PartTypeCore, sourcePath, "default", 0)
		require.NoError(t, err)
	}

	// Verify parts are pending
	pending, err := controller.listPendingForNode(nodeAddr)
	require.NoError(t, err)
	assert.Len(t, pending, numParts)

	// Replay batch (should process all 3 parts)
	count, err := controller.replayBatchForNode(nodeAddr, 10)
	require.NoError(t, err)
	assert.Equal(t, numParts, count)
	assert.Equal(t, numParts, mockClient.getSendCount())

	// Verify all parts completed
	pending, err = controller.listPendingForNode(nodeAddr)
	require.NoError(t, err)
	assert.Empty(t, pending)
}

// TestHandoffController_ReplayBatchForNode_WithBatchLimit tests batch size limiting
func TestHandoffController_ReplayBatchForNode_WithBatchLimit(t *testing.T) {
	tempDir, defFn := test.Space(require.New(t))
	defer defFn()

	fileSystem := fs.NewLocalFileSystem()
	l := logger.GetLogger("test")

	nodeAddr := "node1.example.com:17912"
	// Create mock client
	mockClient := newSimpleMockClient([]string{nodeAddr})

	// Create source parts
	sourceRoot := filepath.Join(tempDir, "source")
	fileSystem.MkdirIfNotExist(sourceRoot, storage.DirPerm)

	controller, err := newHandoffController(fileSystem, tempDir, mockClient, []string{nodeAddr}, 0, l)
	require.NoError(t, err)
	defer controller.close()

	// Enqueue 5 parts
	numParts := 5
	for i := 0; i < numParts; i++ {
		partID := uint64(0x30 + i)
		sourcePath := createTestPart(t, fileSystem, sourceRoot, partID)
		err = controller.enqueueForNode(nodeAddr, partID, PartTypeCore, sourcePath, "default", 0)
		require.NoError(t, err)
	}

	// Replay with batch limit of 2
	count, err := controller.replayBatchForNode(nodeAddr, 2)
	require.NoError(t, err)
	assert.Equal(t, 2, count)
	assert.Equal(t, 2, mockClient.getSendCount())

	// Verify 3 parts still pending
	pending, err := controller.listPendingForNode(nodeAddr)
	require.NoError(t, err)
	assert.Len(t, pending, 3)

	// Replay again
	mockClient.resetSendCount()
	count, err = controller.replayBatchForNode(nodeAddr, 2)
	require.NoError(t, err)
	assert.Equal(t, 2, count)

	// Verify 1 part still pending
	pending, err = controller.listPendingForNode(nodeAddr)
	require.NoError(t, err)
	assert.Len(t, pending, 1)
}

// TestHandoffController_ReplayBatchForNode_SkipsInFlight tests that in-flight parts are skipped
func TestHandoffController_ReplayBatchForNode_SkipsInFlight(t *testing.T) {
	tempDir, defFn := test.Space(require.New(t))
	defer defFn()

	fileSystem := fs.NewLocalFileSystem()
	l := logger.GetLogger("test")

	nodeAddr := "node1.example.com:17912"
	// Create mock client
	mockClient := newSimpleMockClient([]string{nodeAddr})

	// Create source parts
	sourceRoot := filepath.Join(tempDir, "source")
	fileSystem.MkdirIfNotExist(sourceRoot, storage.DirPerm)

	controller, err := newHandoffController(fileSystem, tempDir, mockClient, []string{nodeAddr}, 0, l)
	require.NoError(t, err)
	defer controller.close()

	// Enqueue parts
	partID1 := uint64(0x40)
	partID2 := uint64(0x41)
	sourcePath1 := createTestPart(t, fileSystem, sourceRoot, partID1)
	sourcePath2 := createTestPart(t, fileSystem, sourceRoot, partID2)

	err = controller.enqueueForNode(nodeAddr, partID1, PartTypeCore, sourcePath1, "default", 0)
	require.NoError(t, err)
	err = controller.enqueueForNode(nodeAddr, partID2, PartTypeCore, sourcePath2, "default", 0)
	require.NoError(t, err)

	// Mark first part as in-flight
	controller.markInFlight(nodeAddr, partID1, true)

	// Replay should skip first part and only process second
	count, err := controller.replayBatchForNode(nodeAddr, 10)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// First part should still be pending
	pending, err := controller.listPendingForNode(nodeAddr)
	require.NoError(t, err)
	assert.Len(t, pending, 1)
	assert.Equal(t, partID1, pending[0].PartID)
}

// TestHandoffController_SendPartToNode tests sending a part to a node
func TestHandoffController_SendPartToNode(t *testing.T) {
	tempDir, defFn := test.Space(require.New(t))
	defer defFn()

	fileSystem := fs.NewLocalFileSystem()
	l := logger.GetLogger("test")

	nodeAddr := "node1.example.com:17912"
	// Create mock client
	mockClient := newSimpleMockClient([]string{nodeAddr})

	controller, err := newHandoffController(fileSystem, tempDir, mockClient, []string{nodeAddr}, 0, l)
	require.NoError(t, err)
	defer controller.close()

	// Create a streaming part
	streamingPart := &queue.StreamingPartData{
		ID:       0x50,
		Group:    "default",
		ShardID:  0,
		PartType: PartTypeCore,
		Files:    []queue.FileInfo{},
	}

	// Send the part
	err = controller.sendPartToNode(context.Background(), "node1.example.com:17912", streamingPart)
	require.NoError(t, err)
	assert.Equal(t, 1, mockClient.getSendCount())
}
