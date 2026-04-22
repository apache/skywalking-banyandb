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

const (
	testNodeAddrPrimary   = "node1.example.com:17912"
	testNodeAddrSecondary = "node2.example.com:17912"
)

// simpleMockClient is a minimal mock for testing replay functionality.
type simpleMockClient struct {
	sendError    error
	healthyNodes []string
	sendCalled   int
	mu           sync.Mutex
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

func (m *simpleMockClient) NewChunkedSyncClient(node string, _ uint32) (queue.ChunkedSyncClient, error) {
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

// simpleMockChunkedClient is a minimal mock for ChunkedSyncClient.
type simpleMockChunkedClient struct {
	mockClient *simpleMockClient
	node       string
}

func (m *simpleMockChunkedClient) SyncStreamingParts(_ context.Context, parts []queue.StreamingPartData) (*queue.SyncResult, error) {
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

// TestHandoffController_InFlightTracking tests the in-flight tracking mechanism.
func TestHandoffController_InFlightTracking(t *testing.T) {
	tempDir, defFn := test.Space(require.New(t))
	defer defFn()

	fileSystem := fs.NewLocalFileSystem()
	l := logger.GetLogger("test")

	nodeAddr := testNodeAddrPrimary
	controller, err := newHandoffController(fileSystem, tempDir, nil, []string{nodeAddr}, 0, l, nil)
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

// TestHandoffController_GetNodesWithPendingParts tests the node enumeration.
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

	nodeAddr1 := testNodeAddrPrimary
	nodeAddr2 := testNodeAddrSecondary
	controller, err := newHandoffController(fileSystem, tempDir, nil, []string{nodeAddr1, nodeAddr2}, 0, l, nil)
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

// TestHandoffController_ReadPartFromHandoff tests reading parts from handoff storage.
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

	nodeAddr := testNodeAddrPrimary
	controller, err := newHandoffController(fileSystem, tempDir, nil, []string{nodeAddr}, 0, l, nil)
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

// TestHandoffController_ReplayBatchForNode tests the batch replay logic.
func TestHandoffController_ReplayBatchForNode(t *testing.T) {
	tempDir, defFn := test.Space(require.New(t))
	defer defFn()

	fileSystem := fs.NewLocalFileSystem()
	l := logger.GetLogger("test")

	nodeAddr := testNodeAddrPrimary
	// Create mock client
	mockClient := newSimpleMockClient([]string{nodeAddr})

	// Create source parts
	sourceRoot := filepath.Join(tempDir, "source")
	fileSystem.MkdirIfNotExist(sourceRoot, storage.DirPerm)

	controller, err := newHandoffController(fileSystem, tempDir, mockClient, []string{nodeAddr}, 0, l, nil)
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

// TestHandoffController_ReplayBatchForNode_WithBatchLimit tests batch size limiting.
func TestHandoffController_ReplayBatchForNode_WithBatchLimit(t *testing.T) {
	tempDir, defFn := test.Space(require.New(t))
	defer defFn()

	fileSystem := fs.NewLocalFileSystem()
	l := logger.GetLogger("test")

	nodeAddr := testNodeAddrPrimary
	// Create mock client
	mockClient := newSimpleMockClient([]string{nodeAddr})

	// Create source parts
	sourceRoot := filepath.Join(tempDir, "source")
	fileSystem.MkdirIfNotExist(sourceRoot, storage.DirPerm)

	controller, err := newHandoffController(fileSystem, tempDir, mockClient, []string{nodeAddr}, 0, l, nil)
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

// TestHandoffController_ReplayBatchForNode_SkipsInFlight tests that in-flight parts are skipped.
func TestHandoffController_ReplayBatchForNode_SkipsInFlight(t *testing.T) {
	tempDir, defFn := test.Space(require.New(t))
	defer defFn()

	fileSystem := fs.NewLocalFileSystem()
	l := logger.GetLogger("test")

	nodeAddr := testNodeAddrPrimary
	// Create mock client
	mockClient := newSimpleMockClient([]string{nodeAddr})

	// Create source parts
	sourceRoot := filepath.Join(tempDir, "source")
	fileSystem.MkdirIfNotExist(sourceRoot, storage.DirPerm)

	controller, err := newHandoffController(fileSystem, tempDir, mockClient, []string{nodeAddr}, 0, l, nil)
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

// TestHandoffController_SendPartToNode tests sending a part to a node.
func TestHandoffController_SendPartToNode(t *testing.T) {
	tempDir, defFn := test.Space(require.New(t))
	defer defFn()

	fileSystem := fs.NewLocalFileSystem()
	l := logger.GetLogger("test")

	nodeAddr := testNodeAddrPrimary
	// Create mock client
	mockClient := newSimpleMockClient([]string{nodeAddr})

	controller, err := newHandoffController(fileSystem, tempDir, mockClient, []string{nodeAddr}, 0, l, nil)
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
	err = controller.sendPartToNode(context.Background(), testNodeAddrPrimary, streamingPart)
	require.NoError(t, err)
	assert.Equal(t, 1, mockClient.getSendCount())
}

// createTestSidxPart creates a minimal sidx part directory with the given manifest content.
func createTestSidxPart(t *testing.T, fileSystem fs.FileSystem, root string, partID uint64, manifestContent []byte) string {
	t.Helper()
	partPath := filepath.Join(root, partName(partID))
	fileSystem.MkdirIfNotExist(partPath, storage.DirPerm)

	sidxFiles := map[string][]byte{
		"primary.bin": []byte("test primary data"),
		"data.bin":    []byte("test data"),
		"keys.bin":    []byte("test keys"),
		"meta.bin":    []byte("test meta"),
	}
	if manifestContent != nil {
		sidxFiles["manifest.json"] = manifestContent
	}

	for filename, content := range sidxFiles {
		filePath := filepath.Join(partPath, filename)
		lf, err := fileSystem.CreateLockFile(filePath, storage.FilePerm)
		require.NoError(t, err)
		_, err = lf.Write(content)
		require.NoError(t, err)
		require.NoError(t, lf.Close())
	}

	return partPath
}

func TestHandoffController_ReadPartFromHandoff_CoreMetadata(t *testing.T) {
	tempDir, defFn := test.Space(require.New(t))
	defer defFn()

	fileSystem := fs.NewLocalFileSystem()
	l := logger.GetLogger("test")

	sourceRoot := filepath.Join(tempDir, "source")
	fileSystem.MkdirIfNotExist(sourceRoot, storage.DirPerm)
	partID := uint64(0x60)
	partPath := filepath.Join(sourceRoot, partName(partID))
	fileSystem.MkdirIfNotExist(partPath, storage.DirPerm)

	coreFiles := map[string][]byte{
		"primary.bin": []byte("primary data"),
		"spans.bin":   []byte("spans data"),
		"meta.bin":    []byte("meta data"),
		"metadata.json": []byte(`{"compressedSizeBytes":1024,"uncompressedSpanSizeBytes":2048,` +
			`"totalCount":50,"blocksCount":5,"minTimestamp":1700000000,"maxTimestamp":1700001000}`),
		"tag.type":       []byte("tag type"),
		"traceID.filter": []byte("filter"),
	}
	for filename, content := range coreFiles {
		filePath := filepath.Join(partPath, filename)
		lf, err := fileSystem.CreateLockFile(filePath, storage.FilePerm)
		require.NoError(t, err)
		_, err = lf.Write(content)
		require.NoError(t, err)
		require.NoError(t, lf.Close())
	}

	nodeAddr := testNodeAddrPrimary
	controller, err := newHandoffController(fileSystem, tempDir, nil, []string{nodeAddr}, 0, l, nil)
	require.NoError(t, err)
	defer controller.close()

	require.NoError(t, controller.enqueueForNode(nodeAddr, partID, PartTypeCore, partPath, "group1", 1))

	streamingPart, release, err := controller.readPartFromHandoff(nodeAddr, partID, PartTypeCore)
	require.NoError(t, err)
	defer release()

	assert.Equal(t, uint64(1024), streamingPart.CompressedSizeBytes)
	assert.Equal(t, uint64(2048), streamingPart.UncompressedSizeBytes)
	assert.Equal(t, uint64(50), streamingPart.TotalCount)
	assert.Equal(t, uint64(5), streamingPart.BlocksCount)
	assert.Equal(t, int64(1700000000), streamingPart.MinTimestamp)
	assert.Equal(t, int64(1700001000), streamingPart.MaxTimestamp)
}

func TestHandoffController_ReadPartFromHandoff_CoreMissingMetadata(t *testing.T) {
	tempDir, defFn := test.Space(require.New(t))
	defer defFn()

	fileSystem := fs.NewLocalFileSystem()
	l := logger.GetLogger("test")

	sourceRoot := filepath.Join(tempDir, "source")
	fileSystem.MkdirIfNotExist(sourceRoot, storage.DirPerm)
	partID := uint64(0x61)
	partPath := filepath.Join(sourceRoot, partName(partID))
	fileSystem.MkdirIfNotExist(partPath, storage.DirPerm)

	coreFiles := map[string][]byte{
		"primary.bin": []byte("primary data"),
		"spans.bin":   []byte("spans data"),
		"meta.bin":    []byte("meta data"),
	}
	for filename, content := range coreFiles {
		filePath := filepath.Join(partPath, filename)
		lf, err := fileSystem.CreateLockFile(filePath, storage.FilePerm)
		require.NoError(t, err)
		_, err = lf.Write(content)
		require.NoError(t, err)
		require.NoError(t, lf.Close())
	}

	nodeAddr := testNodeAddrPrimary
	controller, err := newHandoffController(fileSystem, tempDir, nil, []string{nodeAddr}, 0, l, nil)
	require.NoError(t, err)
	defer controller.close()

	require.NoError(t, controller.enqueueForNode(nodeAddr, partID, PartTypeCore, partPath, "group1", 1))

	_, _, readErr := controller.readPartFromHandoff(nodeAddr, partID, PartTypeCore)
	require.Error(t, readErr)
	assert.Contains(t, readErr.Error(), "failed to read metadata.json")
}

func TestHandoffController_ReadPartFromHandoff_CoreInvalidMetadata(t *testing.T) {
	tempDir, defFn := test.Space(require.New(t))
	defer defFn()

	fileSystem := fs.NewLocalFileSystem()
	l := logger.GetLogger("test")

	sourceRoot := filepath.Join(tempDir, "source")
	fileSystem.MkdirIfNotExist(sourceRoot, storage.DirPerm)
	partID := uint64(0x62)
	partPath := filepath.Join(sourceRoot, partName(partID))
	fileSystem.MkdirIfNotExist(partPath, storage.DirPerm)

	coreFiles := map[string][]byte{
		"primary.bin":   []byte("primary data"),
		"spans.bin":     []byte("spans data"),
		"meta.bin":      []byte("meta data"),
		"metadata.json": []byte(`{invalid json`),
	}
	for filename, content := range coreFiles {
		filePath := filepath.Join(partPath, filename)
		lf, err := fileSystem.CreateLockFile(filePath, storage.FilePerm)
		require.NoError(t, err)
		_, err = lf.Write(content)
		require.NoError(t, err)
		require.NoError(t, lf.Close())
	}

	nodeAddr := testNodeAddrPrimary
	controller, err := newHandoffController(fileSystem, tempDir, nil, []string{nodeAddr}, 0, l, nil)
	require.NoError(t, err)
	defer controller.close()

	require.NoError(t, controller.enqueueForNode(nodeAddr, partID, PartTypeCore, partPath, "group1", 1))

	_, _, readErr := controller.readPartFromHandoff(nodeAddr, partID, PartTypeCore)
	require.Error(t, readErr)
	assert.Contains(t, readErr.Error(), "failed to parse metadata.json")
}

func TestHandoffController_ReadPartFromHandoff_SidxWithTimestamps(t *testing.T) {
	tempDir, defFn := test.Space(require.New(t))
	defer defFn()

	fileSystem := fs.NewLocalFileSystem()
	l := logger.GetLogger("test")

	sourceRoot := filepath.Join(tempDir, "source")
	fileSystem.MkdirIfNotExist(sourceRoot, storage.DirPerm)
	partID := uint64(0x70)

	manifest := []byte(`{
		"minTimestamp": 1700000000,
		"maxTimestamp": 1700001000,
		"compressedSizeBytes": 512,
		"uncompressedSizeBytes": 1024,
		"totalCount": 20,
		"blocksCount": 2,
		"minKey": 10,
		"maxKey": 200,
		"segmentID": 1700099999
	}`)
	sourcePath := createTestSidxPart(t, fileSystem, sourceRoot, partID, manifest)

	nodeAddr := testNodeAddrPrimary
	controller, err := newHandoffController(fileSystem, tempDir, nil, []string{nodeAddr}, 0, l, nil)
	require.NoError(t, err)
	defer controller.close()

	require.NoError(t, controller.enqueueForNode(nodeAddr, partID, "sidx_trace_id", sourcePath, "group1", 1))

	streamingPart, release, err := controller.readPartFromHandoff(nodeAddr, partID, "sidx_trace_id")
	require.NoError(t, err)
	defer release()

	assert.Equal(t, partID, streamingPart.ID)
	assert.Equal(t, "group1", streamingPart.Group)
	assert.Equal(t, "sidx_trace_id", streamingPart.PartType)
	assert.Equal(t, uint64(512), streamingPart.CompressedSizeBytes)
	assert.Equal(t, uint64(1024), streamingPart.UncompressedSizeBytes)
	assert.Equal(t, uint64(20), streamingPart.TotalCount)
	assert.Equal(t, uint64(2), streamingPart.BlocksCount)
	assert.Equal(t, int64(10), streamingPart.MinKey)
	assert.Equal(t, int64(200), streamingPart.MaxKey)
	// MinTimestamp comes from the pointer field, not SegmentID
	assert.Equal(t, int64(1700000000), streamingPart.MinTimestamp)
	assert.Equal(t, int64(1700001000), streamingPart.MaxTimestamp)
}

func TestHandoffController_ReadPartFromHandoff_SidxWithSegmentIDFallback(t *testing.T) {
	tempDir, defFn := test.Space(require.New(t))
	defer defFn()

	fileSystem := fs.NewLocalFileSystem()
	l := logger.GetLogger("test")

	sourceRoot := filepath.Join(tempDir, "source")
	fileSystem.MkdirIfNotExist(sourceRoot, storage.DirPerm)
	partID := uint64(0x71)

	// Legacy manifest: no minTimestamp/maxTimestamp, only segmentID
	manifest := []byte(`{
		"compressedSizeBytes": 256,
		"uncompressedSizeBytes": 512,
		"totalCount": 10,
		"blocksCount": 1,
		"minKey": 5,
		"maxKey": 100,
		"segmentID": 1700050000
	}`)
	sourcePath := createTestSidxPart(t, fileSystem, sourceRoot, partID, manifest)

	nodeAddr := testNodeAddrPrimary
	controller, err := newHandoffController(fileSystem, tempDir, nil, []string{nodeAddr}, 0, l, nil)
	require.NoError(t, err)
	defer controller.close()

	require.NoError(t, controller.enqueueForNode(nodeAddr, partID, "sidx_trace_id", sourcePath, "group1", 1))

	streamingPart, release, err := controller.readPartFromHandoff(nodeAddr, partID, "sidx_trace_id")
	require.NoError(t, err)
	defer release()

	// MinTimestamp falls back to SegmentID; MaxTimestamp falls back to MinTimestamp
	assert.Equal(t, int64(1700050000), streamingPart.MinTimestamp)
	assert.Equal(t, int64(1700050000), streamingPart.MaxTimestamp)
	assert.Equal(t, uint64(256), streamingPart.CompressedSizeBytes)
}

func TestHandoffController_ReadPartFromHandoff_SidxMissingManifest(t *testing.T) {
	tempDir, defFn := test.Space(require.New(t))
	defer defFn()

	fileSystem := fs.NewLocalFileSystem()
	l := logger.GetLogger("test")

	sourceRoot := filepath.Join(tempDir, "source")
	fileSystem.MkdirIfNotExist(sourceRoot, storage.DirPerm)
	partID := uint64(0x72)

	// Create sidx part without manifest.json
	sourcePath := createTestSidxPart(t, fileSystem, sourceRoot, partID, nil)

	nodeAddr := testNodeAddrPrimary
	controller, err := newHandoffController(fileSystem, tempDir, nil, []string{nodeAddr}, 0, l, nil)
	require.NoError(t, err)
	defer controller.close()

	require.NoError(t, controller.enqueueForNode(nodeAddr, partID, "sidx_trace_id", sourcePath, "group1", 1))

	_, _, readErr := controller.readPartFromHandoff(nodeAddr, partID, "sidx_trace_id")
	require.Error(t, readErr)
	assert.Contains(t, readErr.Error(), "failed to read manifest.json")
}

func TestHandoffController_ReadPartFromHandoff_SidxInvalidManifest(t *testing.T) {
	tempDir, defFn := test.Space(require.New(t))
	defer defFn()

	fileSystem := fs.NewLocalFileSystem()
	l := logger.GetLogger("test")

	sourceRoot := filepath.Join(tempDir, "source")
	fileSystem.MkdirIfNotExist(sourceRoot, storage.DirPerm)
	partID := uint64(0x73)

	manifest := []byte(`{broken json`)
	sourcePath := createTestSidxPart(t, fileSystem, sourceRoot, partID, manifest)

	nodeAddr := testNodeAddrPrimary
	controller, err := newHandoffController(fileSystem, tempDir, nil, []string{nodeAddr}, 0, l, nil)
	require.NoError(t, err)
	defer controller.close()

	require.NoError(t, controller.enqueueForNode(nodeAddr, partID, "sidx_trace_id", sourcePath, "group1", 1))

	_, _, readErr := controller.readPartFromHandoff(nodeAddr, partID, "sidx_trace_id")
	require.Error(t, readErr)
	assert.Contains(t, readErr.Error(), "failed to parse manifest.json")
}

func TestHandoffController_ReadPartFromHandoff_SidxNoValidTimestamp(t *testing.T) {
	tempDir, defFn := test.Space(require.New(t))
	defer defFn()

	fileSystem := fs.NewLocalFileSystem()
	l := logger.GetLogger("test")

	sourceRoot := filepath.Join(tempDir, "source")
	fileSystem.MkdirIfNotExist(sourceRoot, storage.DirPerm)
	partID := uint64(0x74)

	// Manifest with no minTimestamp and segmentID=0 (invalid)
	manifest := []byte(`{
		"compressedSizeBytes": 128,
		"totalCount": 5,
		"blocksCount": 1,
		"minKey": 1,
		"maxKey": 10,
		"segmentID": 0
	}`)
	sourcePath := createTestSidxPart(t, fileSystem, sourceRoot, partID, manifest)

	nodeAddr := testNodeAddrPrimary
	controller, err := newHandoffController(fileSystem, tempDir, nil, []string{nodeAddr}, 0, l, nil)
	require.NoError(t, err)
	defer controller.close()

	require.NoError(t, controller.enqueueForNode(nodeAddr, partID, "sidx_trace_id", sourcePath, "group1", 1))

	_, _, readErr := controller.readPartFromHandoff(nodeAddr, partID, "sidx_trace_id")
	require.Error(t, readErr)
	assert.Contains(t, readErr.Error(), "has no valid timestamp")
}
