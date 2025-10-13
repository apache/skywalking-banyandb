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
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"sync"

	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

const (
	handoffMetaFilename = ".handoff_meta"
	nodeInfoFilename    = ".node_info"
)

// handoffMetadata contains metadata for a handoff queue entry.
type handoffMetadata struct {
	EnqueueTimestamp int64  `json:"enqueue_timestamp"` // When queued (UnixNano)
	Group            string `json:"group"`             // Stream group
	ShardID          uint32 `json:"shard_id"`          // Shard identifier
	PartType         string `json:"part_type"`         // Core or sidx type
	PartSizeBytes    uint64 `json:"part_size_bytes"`   // Total size of part in bytes
}

// handoffNodeQueue manages the handoff queue for a single data node.
// It uses a per-node directory with hardlinked part directories.
type handoffNodeQueue struct {
	nodeAddr   string // Node address (e.g., "node1.example.com:17912")
	root       string // Root path for this node's queue
	fileSystem fs.FileSystem
	l          *logger.Logger
	mu         sync.RWMutex
}

// newHandoffNodeQueue creates a new handoff queue for a specific node.
func newHandoffNodeQueue(nodeAddr, root string, fileSystem fs.FileSystem, l *logger.Logger) (*handoffNodeQueue, error) {
	hnq := &handoffNodeQueue{
		nodeAddr:   nodeAddr,
		root:       root,
		fileSystem: fileSystem,
		l:          l,
	}

	// Create node queue directory if it doesn't exist
	fileSystem.MkdirIfNotExist(root, storage.DirPerm)

	// Write node info file to persist the original node address
	if err := hnq.writeNodeInfo(); err != nil {
		l.Warn().Err(err).Msg("failed to write node info")
	}

	return hnq, nil
}

// writeNodeInfo writes the original node address to a metadata file.
func (hnq *handoffNodeQueue) writeNodeInfo() error {
	nodeInfoPath := filepath.Join(hnq.root, nodeInfoFilename)

	// Check if already exists
	if _, err := hnq.fileSystem.Read(nodeInfoPath); err == nil {
		return nil // Already exists
	}

	lf, err := hnq.fileSystem.CreateLockFile(nodeInfoPath, storage.FilePerm)
	if err != nil {
		return fmt.Errorf("failed to create node info file: %w", err)
	}

	_, err = lf.Write([]byte(hnq.nodeAddr))
	return err
}

// readNodeInfo reads the original node address from the metadata file.
func readNodeInfo(fileSystem fs.FileSystem, root string) (string, error) {
	nodeInfoPath := filepath.Join(root, nodeInfoFilename)
	data, err := fileSystem.Read(nodeInfoPath)
	if err != nil {
		return "", fmt.Errorf("failed to read node info: %w", err)
	}
	return string(data), nil
}

// enqueue adds a part to the handoff queue by creating hardlinks and writing metadata.
// Uses nested structure: <nodeRoot>/<partId>/<partType>/
func (hnq *handoffNodeQueue) enqueue(partID uint64, partType string, sourcePath string, meta *handoffMetadata) error {
	hnq.mu.Lock()
	defer hnq.mu.Unlock()

	dstPath := hnq.getPartTypePath(partID, partType)

	// Check if already exists (idempotent)
	partIDDir := hnq.getPartIDDir(partID)

	// Create parent directory for partID if it doesn't exist
	hnq.fileSystem.MkdirIfNotExist(partIDDir, storage.DirPerm)

	// Now check if part type already exists
	partTypeEntries := hnq.fileSystem.ReadDir(partIDDir)
	for _, entry := range partTypeEntries {
		if entry.Name() == partType && entry.IsDir() {
			return nil // Already enqueued
		}
	}

	// Create hardlinks directly to destination
	if err := copyPartDirectory(hnq.fileSystem, sourcePath, dstPath); err != nil {
		// Clean up partial copy on failure
		hnq.fileSystem.MustRMAll(dstPath)
		return fmt.Errorf("failed to hardlink part: %w", err)
	}

	// Write sidecar metadata file after hardlinks are created
	metaPath := filepath.Join(dstPath, handoffMetaFilename)
	metaBytes, err := json.Marshal(meta)
	if err != nil {
		hnq.fileSystem.MustRMAll(dstPath)
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	lf, err := hnq.fileSystem.CreateLockFile(metaPath, storage.FilePerm)
	if err != nil {
		hnq.fileSystem.MustRMAll(dstPath)
		return fmt.Errorf("failed to create metadata file: %w", err)
	}
	if _, err := lf.Write(metaBytes); err != nil {
		hnq.fileSystem.MustRMAll(dstPath)
		return fmt.Errorf("failed to write metadata: %w", err)
	}

	hnq.l.Info().
		Str("node", hnq.nodeAddr).
		Uint64("partId", partID).
		Str("partType", partType).
		Str("path", dstPath).
		Msg("part enqueued to handoff queue")

	return nil
}

// partTypePair represents a pending part with its type.
type partTypePair struct {
	PartID   uint64
	PartType string
}

// listPending returns a sorted list of all pending part IDs with their types.
func (hnq *handoffNodeQueue) listPending() ([]partTypePair, error) {
	hnq.mu.RLock()
	defer hnq.mu.RUnlock()

	entries := hnq.fileSystem.ReadDir(hnq.root)
	var pairs []partTypePair

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		// Skip temp directories and metadata files
		if filepath.Ext(entry.Name()) == ".tmp" || entry.Name() == nodeInfoFilename {
			continue
		}

		// Parse partId from hex directory name
		partID, err := parsePartID(entry.Name())
		if err != nil {
			hnq.l.Warn().Str("name", entry.Name()).Msg("invalid part directory")
			continue
		}

		// List part types under this partId
		partIDDir := filepath.Join(hnq.root, entry.Name())
		partTypeEntries := hnq.fileSystem.ReadDir(partIDDir)
		for _, partTypeEntry := range partTypeEntries {
			if partTypeEntry.IsDir() {
				pairs = append(pairs, partTypePair{
					PartID:   partID,
					PartType: partTypeEntry.Name(),
				})
			}
		}
	}

	// Sort by partID first, then by partType
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].PartID == pairs[j].PartID {
			return pairs[i].PartType < pairs[j].PartType
		}
		return pairs[i].PartID < pairs[j].PartID
	})

	return pairs, nil
}

// getMetadata reads the handoff metadata for a specific part type.
func (hnq *handoffNodeQueue) getMetadata(partID uint64, partType string) (*handoffMetadata, error) {
	hnq.mu.RLock()
	defer hnq.mu.RUnlock()

	metaPath := filepath.Join(hnq.getPartTypePath(partID, partType), handoffMetaFilename)
	data, err := hnq.fileSystem.Read(metaPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata: %w", err)
	}

	var meta handoffMetadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	return &meta, nil
}

// getPartIDDir returns the directory path for a partID (contains all part types).
func (hnq *handoffNodeQueue) getPartIDDir(partID uint64) string {
	return filepath.Join(hnq.root, partName(partID))
}

// getPartTypePath returns the full path to a specific part type directory.
func (hnq *handoffNodeQueue) getPartTypePath(partID uint64, partType string) string {
	return filepath.Join(hnq.getPartIDDir(partID), partType)
}

// complete removes a specific part type from the handoff queue after successful delivery.
func (hnq *handoffNodeQueue) complete(partID uint64, partType string) error {
	hnq.mu.Lock()
	defer hnq.mu.Unlock()

	dstPath := hnq.getPartTypePath(partID, partType)

	// Remove the part type directory
	hnq.fileSystem.MustRMAll(dstPath)

	// Check if partID directory is now empty, if so remove it too
	partIDDir := hnq.getPartIDDir(partID)
	entries := hnq.fileSystem.ReadDir(partIDDir)
	isEmpty := true
	for _, entry := range entries {
		if entry.IsDir() {
			isEmpty = false
			break
		}
	}
	if isEmpty {
		hnq.fileSystem.MustRMAll(partIDDir)
	}

	hnq.l.Info().
		Str("node", hnq.nodeAddr).
		Uint64("partId", partID).
		Str("partType", partType).
		Msg("part type removed from handoff queue")

	return nil
}

// completeAll removes all part types for a given partID.
func (hnq *handoffNodeQueue) completeAll(partID uint64) error {
	hnq.mu.Lock()
	defer hnq.mu.Unlock()

	partIDDir := hnq.getPartIDDir(partID)

	// Simply remove the entire partID directory
	hnq.fileSystem.MustRMAll(partIDDir)

	hnq.l.Info().
		Str("node", hnq.nodeAddr).
		Uint64("partId", partID).
		Msg("all part types removed from handoff queue")

	return nil
}

// size returns the total size of all pending parts in bytes.
func (hnq *handoffNodeQueue) size() (uint64, error) {
	hnq.mu.RLock()
	defer hnq.mu.RUnlock()

	partIDEntries := hnq.fileSystem.ReadDir(hnq.root)
	var totalSize uint64

	for _, partIDEntry := range partIDEntries {
		if !partIDEntry.IsDir() || partIDEntry.Name() == nodeInfoFilename {
			continue
		}

		// For each partID directory
		partIDDir := filepath.Join(hnq.root, partIDEntry.Name())
		partTypeEntries := hnq.fileSystem.ReadDir(partIDDir)

		for _, partTypeEntry := range partTypeEntries {
			if !partTypeEntry.IsDir() {
				continue
			}

			// For each part type directory
			partTypePath := filepath.Join(partIDDir, partTypeEntry.Name())
			partFiles := hnq.fileSystem.ReadDir(partTypePath)

			for _, partFile := range partFiles {
				if partFile.IsDir() {
					continue
				}

				filePath := filepath.Join(partTypePath, partFile.Name())
				file, err := hnq.fileSystem.OpenFile(filePath)
				if err != nil {
					continue // Skip files we can't open
				}
				size, _ := file.Size()
				fs.MustClose(file)
				if size > 0 {
					totalSize += uint64(size)
				}
			}
		}
	}

	return totalSize, nil
}

// copyPartDirectory creates hardlinks from srcDir to dstDir using fs.CreateHardLink.
func copyPartDirectory(fileSystem fs.FileSystem, srcDir, dstDir string) error {
	if err := fileSystem.CreateHardLink(srcDir, dstDir, nil); err != nil {
		return fmt.Errorf("failed to create hardlinks from %s to %s: %w", srcDir, dstDir, err)
	}
	return nil
}

// parsePartID parses a part ID from a hex string directory name.
func parsePartID(name string) (uint64, error) {
	partID, err := strconv.ParseUint(name, 16, 64)
	if err != nil {
		return 0, fmt.Errorf("cannot parse part ID %s: %w", name, err)
	}
	return partID, nil
}
