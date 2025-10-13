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

package trace

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

func setupHandoffTest(t *testing.T) (string, fs.FileSystem, *logger.Logger) {
	tempDir, deferFn := test.Space(require.New(t))
	t.Cleanup(deferFn)

	fileSystem := fs.NewLocalFileSystem()
	l := logger.GetLogger("handoff-test")

	return tempDir, fileSystem, l
}

func createTestPart(t *testing.T, fileSystem fs.FileSystem, root string, partID uint64) string {
	partPath := filepath.Join(root, partName(partID))
	fileSystem.MkdirIfNotExist(partPath, storage.DirPerm)

	// Create test files
	files := map[string][]byte{
		"meta.bin":       []byte("test meta data"),
		"primary.bin":    []byte("test primary data"),
		"spans.bin":      []byte("test spans data"),
		"metadata.json":  []byte(`{"id":1,"totalCount":100}`),
		"tag.type":       []byte("test tag type"),
		"traceID.filter": []byte("test filter"),
	}

	for filename, content := range files {
		filePath := filepath.Join(partPath, filename)
		lf, err := fileSystem.CreateLockFile(filePath, storage.FilePerm)
		require.NoError(t, err)
		_, err = lf.Write(content)
		require.NoError(t, err)
	}

	return partPath
}

func TestHandoffNodeQueue_EnqueueCore(t *testing.T) {
	tempDir, fileSystem, l := setupHandoffTest(t)

	// Create source part
	sourceRoot := filepath.Join(tempDir, "source")
	fileSystem.MkdirIfNotExist(sourceRoot, storage.DirPerm)
	partID := uint64(0x1a)
	sourcePath := createTestPart(t, fileSystem, sourceRoot, partID)

	// Create handoff node queue
	queueRoot := filepath.Join(tempDir, "handoff", "node1")
	nodeQueue, err := newHandoffNodeQueue("node1.example.com:17912", queueRoot, fileSystem, l)
	require.NoError(t, err)

	// Test enqueue core part
	meta := &handoffMetadata{
		EnqueueTimestamp: time.Now().UnixNano(),
		Group:            "default",
		ShardID:          0,
		PartType:         PartTypeCore,
	}

	err = nodeQueue.enqueue(partID, PartTypeCore, sourcePath, meta)
	require.NoError(t, err)

	// Verify nested structure: <partID>/core/
	dstPath := nodeQueue.getPartTypePath(partID, PartTypeCore)
	entries := fileSystem.ReadDir(dstPath)
	assert.NotEmpty(t, entries)

	// Verify metadata file exists
	metaPath := filepath.Join(dstPath, handoffMetaFilename)
	metaData, err := fileSystem.Read(metaPath)
	require.NoError(t, err)
	assert.NotEmpty(t, metaData)
}

func TestHandoffNodeQueue_EnqueueMultiplePartTypes(t *testing.T) {
	tempDir, fileSystem, l := setupHandoffTest(t)

	// Create source parts
	sourceRoot := filepath.Join(tempDir, "source")
	fileSystem.MkdirIfNotExist(sourceRoot, storage.DirPerm)
	partID := uint64(0x1b)
	sourcePath := createTestPart(t, fileSystem, sourceRoot, partID)

	// Create handoff node queue
	queueRoot := filepath.Join(tempDir, "handoff", "node1")
	nodeQueue, err := newHandoffNodeQueue("node1.example.com:17912", queueRoot, fileSystem, l)
	require.NoError(t, err)

	// Enqueue core part
	meta := &handoffMetadata{
		EnqueueTimestamp: time.Now().UnixNano(),
		Group:            "default",
		ShardID:          0,
		PartType:         PartTypeCore,
	}
	err = nodeQueue.enqueue(partID, PartTypeCore, sourcePath, meta)
	require.NoError(t, err)

	// Enqueue sidx part (simulated with same source)
	meta.PartType = "sidx_trace_id"
	err = nodeQueue.enqueue(partID, "sidx_trace_id", sourcePath, meta)
	require.NoError(t, err)

	// Enqueue another sidx part
	meta.PartType = "sidx_service"
	err = nodeQueue.enqueue(partID, "sidx_service", sourcePath, meta)
	require.NoError(t, err)

	// List pending - should have 3 part types for same partID
	pending, err := nodeQueue.listPending()
	require.NoError(t, err)
	assert.Len(t, pending, 3)

	// Verify all belong to same partID
	for _, pair := range pending {
		assert.Equal(t, partID, pair.PartID)
	}

	// Verify part types
	partTypes := make(map[string]bool)
	for _, pair := range pending {
		partTypes[pair.PartType] = true
	}
	assert.True(t, partTypes[PartTypeCore])
	assert.True(t, partTypes["sidx_trace_id"])
	assert.True(t, partTypes["sidx_service"])
}

func TestHandoffNodeQueue_Complete(t *testing.T) {
	tempDir, fileSystem, l := setupHandoffTest(t)

	// Create source part
	sourceRoot := filepath.Join(tempDir, "source")
	fileSystem.MkdirIfNotExist(sourceRoot, storage.DirPerm)
	partID := uint64(0x1c)
	sourcePath := createTestPart(t, fileSystem, sourceRoot, partID)

	// Create handoff node queue
	queueRoot := filepath.Join(tempDir, "handoff", "node1")
	nodeQueue, err := newHandoffNodeQueue("node1.example.com:17912", queueRoot, fileSystem, l)
	require.NoError(t, err)

	// Enqueue core and sidx parts
	meta := &handoffMetadata{
		EnqueueTimestamp: time.Now().UnixNano(),
		Group:            "default",
		ShardID:          0,
		PartType:         PartTypeCore,
	}
	err = nodeQueue.enqueue(partID, PartTypeCore, sourcePath, meta)
	require.NoError(t, err)

	meta.PartType = "sidx_trace_id"
	err = nodeQueue.enqueue(partID, "sidx_trace_id", sourcePath, meta)
	require.NoError(t, err)

	// Verify both are pending
	pending, err := nodeQueue.listPending()
	require.NoError(t, err)
	assert.Len(t, pending, 2)

	// Complete core part only
	err = nodeQueue.complete(partID, PartTypeCore)
	require.NoError(t, err)

	// Verify only sidx remains
	pending, err = nodeQueue.listPending()
	require.NoError(t, err)
	assert.Len(t, pending, 1)
	assert.Equal(t, "sidx_trace_id", pending[0].PartType)

	// Complete remaining part
	err = nodeQueue.complete(partID, "sidx_trace_id")
	require.NoError(t, err)

	// Verify nothing pending
	pending, err = nodeQueue.listPending()
	require.NoError(t, err)
	assert.Len(t, pending, 0)

	// Verify partID directory is also removed
	partIDDir := nodeQueue.getPartIDDir(partID)
	_, err = os.Stat(partIDDir)
	assert.True(t, os.IsNotExist(err))
}

func TestHandoffNodeQueue_CompleteAll(t *testing.T) {
	tempDir, fileSystem, l := setupHandoffTest(t)

	// Create source part
	sourceRoot := filepath.Join(tempDir, "source")
	fileSystem.MkdirIfNotExist(sourceRoot, storage.DirPerm)
	partID := uint64(0x1d)
	sourcePath := createTestPart(t, fileSystem, sourceRoot, partID)

	// Create handoff node queue
	queueRoot := filepath.Join(tempDir, "handoff", "node1")
	nodeQueue, err := newHandoffNodeQueue("node1.example.com:17912", queueRoot, fileSystem, l)
	require.NoError(t, err)

	// Enqueue multiple part types
	meta := &handoffMetadata{
		EnqueueTimestamp: time.Now().UnixNano(),
		Group:            "default",
		ShardID:          0,
		PartType:         PartTypeCore,
	}
	err = nodeQueue.enqueue(partID, PartTypeCore, sourcePath, meta)
	require.NoError(t, err)
	err = nodeQueue.enqueue(partID, "sidx_trace_id", sourcePath, meta)
	require.NoError(t, err)
	err = nodeQueue.enqueue(partID, "sidx_service", sourcePath, meta)
	require.NoError(t, err)

	// Verify all are pending
	pending, err := nodeQueue.listPending()
	require.NoError(t, err)
	assert.Len(t, pending, 3)

	// Complete all at once
	err = nodeQueue.completeAll(partID)
	require.NoError(t, err)

	// Verify nothing pending
	pending, err = nodeQueue.listPending()
	require.NoError(t, err)
	assert.Len(t, pending, 0)
}

func TestHandoffController_EnqueueForNodes(t *testing.T) {
	tempDir, fileSystem, l := setupHandoffTest(t)

	// Create source part
	sourceRoot := filepath.Join(tempDir, "source")
	fileSystem.MkdirIfNotExist(sourceRoot, storage.DirPerm)
	partID := uint64(0x1e)
	sourcePath := createTestPart(t, fileSystem, sourceRoot, partID)

	// Create handoff controller
	offlineNodes := []string{"node1.example.com:17912", "node2.example.com:17912"}
	controller, err := newHandoffController(fileSystem, tempDir, nil, offlineNodes, 0, l)
	require.NoError(t, err)

	// Enqueue for multiple nodes
	err = controller.enqueueForNodes(offlineNodes, partID, PartTypeCore, sourcePath, "default", 0)
	require.NoError(t, err)

	// Verify both nodes have the part
	for _, nodeAddr := range offlineNodes {
		pending, err := controller.listPendingForNode(nodeAddr)
		require.NoError(t, err)
		assert.Len(t, pending, 1)
		assert.Equal(t, partID, pending[0].PartID)
		assert.Equal(t, PartTypeCore, pending[0].PartType)
	}
}

func TestHandoffController_GetPartPath(t *testing.T) {
	tempDir, fileSystem, l := setupHandoffTest(t)

	// Create source part
	sourceRoot := filepath.Join(tempDir, "source")
	fileSystem.MkdirIfNotExist(sourceRoot, storage.DirPerm)
	partID := uint64(0x1f)
	sourcePath := createTestPart(t, fileSystem, sourceRoot, partID)

	// Create handoff controller
	nodeAddr := "node1.example.com:17912"
	controller, err := newHandoffController(fileSystem, tempDir, nil, []string{nodeAddr}, 0, l)
	require.NoError(t, err)

	// Enqueue for node
	err = controller.enqueueForNode(nodeAddr, partID, PartTypeCore, sourcePath, "default", 0)
	require.NoError(t, err)

	// Get part path
	partPath := controller.getPartPath(nodeAddr, partID, PartTypeCore)
	assert.NotEmpty(t, partPath)

	// Verify path exists
	_, err = os.Stat(partPath)
	require.NoError(t, err)

	// Verify it's the nested structure
	assert.Contains(t, partPath, partName(partID))
	assert.Contains(t, partPath, PartTypeCore)
}

func TestHandoffController_LoadExistingQueues(t *testing.T) {
	tempDir, fileSystem, l := setupHandoffTest(t)

	// Create source part
	sourceRoot := filepath.Join(tempDir, "source")
	fileSystem.MkdirIfNotExist(sourceRoot, storage.DirPerm)
	partID := uint64(0x20)
	sourcePath := createTestPart(t, fileSystem, sourceRoot, partID)

	nodeAddr := "node1.example.com:17912"

	// Create first controller and enqueue part
	controller1, err := newHandoffController(fileSystem, tempDir, nil, []string{nodeAddr}, 0, l)
	require.NoError(t, err)

	err = controller1.enqueueForNode(nodeAddr, partID, PartTypeCore, sourcePath, "default", 0)
	require.NoError(t, err)

	// Close first controller
	err = controller1.close()
	require.NoError(t, err)

	// Create second controller (should load existing queues)
	controller2, err := newHandoffController(fileSystem, tempDir, nil, []string{nodeAddr}, 0, l)
	require.NoError(t, err)

	// Verify part is still pending
	pending, err := controller2.listPendingForNode(nodeAddr)
	require.NoError(t, err)
	assert.Len(t, pending, 1)
	assert.Equal(t, partID, pending[0].PartID)
	assert.Equal(t, PartTypeCore, pending[0].PartType)
}

func TestSanitizeNodeAddr(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"node1.example.com:17912", "node1.example.com_17912"},
		{"node2:8080", "node2_8080"},
		{"192.168.1.1:9999", "192.168.1.1_9999"},
		{"node/with/slashes", "node_with_slashes"},
		{"node\\with\\backslashes", "node_with_backslashes"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := sanitizeNodeAddr(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestParsePartID(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		expected  uint64
		expectErr bool
	}{
		{"valid hex", "000000000000001a", 0x1a, false},
		{"valid hex upper", "000000000000001A", 0x1a, false},
		{"simple hex", "1a", 0x1a, false},
		{"invalid hex", "xyz", 0, true},
		{"empty string", "", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parsePartID(tt.input)
			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

// TestHandoffController_SizeEnforcement verifies that handoff queue rejects enqueues when size limit is exceeded.
func TestHandoffController_SizeEnforcement(t *testing.T) {
	tester := require.New(t)
	tempDir, deferFunc := test.Space(tester)
	defer deferFunc()

	// Create a mock part with metadata
	partID := uint64(100)
	partPath := filepath.Join(tempDir, "source", partName(partID))
	err := os.MkdirAll(partPath, 0o755)
	tester.NoError(err)

	// Create metadata.json with CompressedSizeBytes = 5MB
	metadata := map[string]interface{}{
		"compressedSizeBytes": 5 * 1024 * 1024, // 5MB
	}
	metadataBytes, err := json.Marshal(metadata)
	tester.NoError(err)
	err = os.WriteFile(filepath.Join(partPath, "metadata.json"), metadataBytes, 0o644)
	tester.NoError(err)

	// Create a dummy file
	err = os.WriteFile(filepath.Join(partPath, "data.bin"), []byte("test data"), 0o644)
	tester.NoError(err)

	// Create handoff controller with 10MB limit
	lfs := fs.NewLocalFileSystem()
	l := logger.GetLogger("test")
	dataNodes := []string{"node1:17912", "node2:17912"}
	hc, err := newHandoffController(lfs, tempDir, nil, dataNodes, 10, l) // 10MB limit
	tester.NoError(err)
	defer hc.close()

	// First enqueue should succeed (5MB < 10MB)
	err = hc.enqueueForNode("node1:17912", partID, PartTypeCore, partPath, "group1", 1)
	tester.NoError(err)

	// Check total size
	totalSize := hc.getTotalSize()
	tester.Equal(uint64(5*1024*1024), totalSize)

	// Second enqueue should succeed (10MB = 10MB)
	err = hc.enqueueForNode("node1:17912", partID+1, PartTypeCore, partPath, "group1", 1)
	tester.NoError(err)

	// Check total size
	totalSize = hc.getTotalSize()
	tester.Equal(uint64(10*1024*1024), totalSize)

	// Third enqueue should fail (15MB > 10MB)
	err = hc.enqueueForNode("node1:17912", partID+2, PartTypeCore, partPath, "group1", 1)
	tester.Error(err)
	tester.Contains(err.Error(), "handoff queue full")

	// Total size should still be 10MB
	totalSize = hc.getTotalSize()
	tester.Equal(uint64(10*1024*1024), totalSize)
}

// TestHandoffController_SizeTracking verifies that total size is correctly updated on enqueue and complete.
func TestHandoffController_SizeTracking(t *testing.T) {
	tester := require.New(t)
	tempDir, deferFunc := test.Space(tester)
	defer deferFunc()

	// Create a mock part with metadata
	partID := uint64(200)
	partPath := filepath.Join(tempDir, "source", partName(partID))
	err := os.MkdirAll(partPath, 0o755)
	tester.NoError(err)

	// Create metadata.json with CompressedSizeBytes = 3MB
	metadata := map[string]interface{}{
		"compressedSizeBytes": 3 * 1024 * 1024, // 3MB
	}
	metadataBytes, err := json.Marshal(metadata)
	tester.NoError(err)
	err = os.WriteFile(filepath.Join(partPath, "metadata.json"), metadataBytes, 0o644)
	tester.NoError(err)

	// Create a dummy file
	err = os.WriteFile(filepath.Join(partPath, "data.bin"), []byte("test data"), 0o644)
	tester.NoError(err)

	// Create handoff controller with 100MB limit
	lfs := fs.NewLocalFileSystem()
	l := logger.GetLogger("test")
	dataNodes := []string{"node1:17912"}
	hc, err := newHandoffController(lfs, tempDir, nil, dataNodes, 100, l) // 100MB limit
	tester.NoError(err)
	defer hc.close()

	// Initial size should be 0
	tester.Equal(uint64(0), hc.getTotalSize())

	// Enqueue first part
	err = hc.enqueueForNode("node1:17912", partID, PartTypeCore, partPath, "group1", 1)
	tester.NoError(err)
	tester.Equal(uint64(3*1024*1024), hc.getTotalSize())

	// Enqueue second part
	err = hc.enqueueForNode("node1:17912", partID+1, PartTypeCore, partPath, "group1", 1)
	tester.NoError(err)
	tester.Equal(uint64(6*1024*1024), hc.getTotalSize())

	// Complete first part
	err = hc.completeSend("node1:17912", partID, PartTypeCore)
	tester.NoError(err)
	tester.Equal(uint64(3*1024*1024), hc.getTotalSize())

	// Complete second part
	err = hc.completeSend("node1:17912", partID+1, PartTypeCore)
	tester.NoError(err)
	tester.Equal(uint64(0), hc.getTotalSize())
}

// TestHandoffController_SizeRecovery verifies that total size is correctly calculated on restart.
func TestHandoffController_SizeRecovery(t *testing.T) {
	tester := require.New(t)
	tempDir, deferFunc := test.Space(tester)
	defer deferFunc()

	// Create a mock part with metadata
	partID := uint64(300)
	partPath := filepath.Join(tempDir, "source", partName(partID))
	err := os.MkdirAll(partPath, 0o755)
	tester.NoError(err)

	// Create metadata.json with CompressedSizeBytes = 7MB
	metadata := map[string]interface{}{
		"compressedSizeBytes": 7 * 1024 * 1024, // 7MB
	}
	metadataBytes, err := json.Marshal(metadata)
	tester.NoError(err)
	err = os.WriteFile(filepath.Join(partPath, "metadata.json"), metadataBytes, 0o644)
	tester.NoError(err)

	// Create a dummy file
	err = os.WriteFile(filepath.Join(partPath, "data.bin"), []byte("test data"), 0o644)
	tester.NoError(err)

	lfs := fs.NewLocalFileSystem()
	l := logger.GetLogger("test")
	dataNodes := []string{"node1:17912", "node2:17912"}

	// First controller: enqueue some parts
	hc1, err := newHandoffController(lfs, tempDir, nil, dataNodes, 100, l)
	tester.NoError(err)

	err = hc1.enqueueForNode("node1:17912", partID, PartTypeCore, partPath, "group1", 1)
	tester.NoError(err)
	err = hc1.enqueueForNode("node2:17912", partID+1, PartTypeCore, partPath, "group1", 1)
	tester.NoError(err)

	// Total size should be 14MB (7MB * 2 parts)
	expectedSize := uint64(14 * 1024 * 1024)
	tester.Equal(expectedSize, hc1.getTotalSize())

	// Close first controller
	err = hc1.close()
	tester.NoError(err)

	// Second controller: should recover the same size
	hc2, err := newHandoffController(lfs, tempDir, nil, dataNodes, 100, l)
	tester.NoError(err)
	defer hc2.close()

	// Recovered size should match
	recoveredSize := hc2.getTotalSize()
	tester.Equal(expectedSize, recoveredSize)

	// Should have both nodes with pending parts
	node1Pending, err := hc2.listPendingForNode("node1:17912")
	tester.NoError(err)
	tester.Len(node1Pending, 1)

	node2Pending, err := hc2.listPendingForNode("node2:17912")
	tester.NoError(err)
	tester.Len(node2Pending, 1)
}
