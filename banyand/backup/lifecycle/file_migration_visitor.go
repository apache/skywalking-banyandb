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

package lifecycle

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/node"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

const (
	// Stream file names from banyand/stream/syncer.go.
	streamMetaName          = "meta"
	streamPrimaryName       = "primary"
	streamTimestampsName    = "timestamps"
	streamTagFamiliesPrefix = "tag_families_"
	streamTagMetadataPrefix = "tag_metadata_"
	streamTagFilterPrefix   = "tag_filter_"
)

// MigrationVisitor implements the stream.Visitor interface for file-based migration.
type MigrationVisitor struct {
	selector       node.Selector                      // From parseGroup - node selector
	client         queue.Client                       // From parseGroup - queue client
	chunkedClients map[string]queue.ChunkedSyncClient // Per-node chunked sync clients cache
	logger         *logger.Logger
	progress       *Progress // Progress tracker for migration states
	group          string
	streamName     string
	targetShardNum uint32 // From parseGroup - target shard count
	replicas       uint32 // From parseGroup - replica count
	chunkSize      int    // Chunk size for streaming data
}

// partMetadata matches the structure in banyand/stream/part_metadata.go.
type partMetadata struct {
	CompressedSizeBytes   uint64 `json:"compressedSizeBytes"`
	UncompressedSizeBytes uint64 `json:"uncompressedSizeBytes"`
	TotalCount            uint64 `json:"totalCount"`
	BlocksCount           uint64 `json:"blocksCount"`
	MinTimestamp          int64  `json:"minTimestamp"`
	MaxTimestamp          int64  `json:"maxTimestamp"`
}

// NewMigrationVisitor creates a new file-based migration visitor.
func NewMigrationVisitor(group *commonv1.Group, nodeLabels map[string]string,
	nodes []*databasev1.Node, metadata metadata.Repo, l *logger.Logger,
	progress *Progress, streamName string, chunkSize int,
) (*MigrationVisitor, error) {
	// Use existing parseGroup function to get sharding parameters
	shardNum, replicas, selector, client, err := parseGroup(group, nodeLabels, nodes, l, metadata)
	if err != nil {
		return nil, err
	}

	return &MigrationVisitor{
		group:          group.Metadata.Name,
		targetShardNum: shardNum,
		replicas:       replicas,
		selector:       selector,
		client:         client,
		chunkedClients: make(map[string]queue.ChunkedSyncClient),
		logger:         l,
		progress:       progress,
		streamName:     streamName,
		chunkSize:      chunkSize,
	}, nil
}

// VisitSeries implements stream.Visitor.
func (mv *MigrationVisitor) VisitSeries(segmentTR *timestamp.TimeRange, seriesIndexPath string) error {
	mv.logger.Info().
		Str("path", seriesIndexPath).
		Int64("min_timestamp", segmentTR.Start.UnixNano()).
		Int64("max_timestamp", segmentTR.End.UnixNano()).
		Str("stream", mv.streamName).
		Str("group", mv.group).
		Msg("migrating series index")

	// Find all *.seg segment files in the seriesIndexPath
	lfs := fs.NewLocalFileSystem()
	entries := lfs.ReadDir(seriesIndexPath)

	var segmentFiles []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".seg") {
			segmentFiles = append(segmentFiles, entry.Name())
		}
	}

	if len(segmentFiles) == 0 {
		mv.logger.Debug().
			Str("path", seriesIndexPath).
			Msg("no .seg files found in series index path")
		return nil
	}

	mv.logger.Info().
		Int("segment_count", len(segmentFiles)).
		Str("path", seriesIndexPath).
		Msg("found segment files for migration")

	// Set the total number of series segments for progress tracking
	mv.SetStreamSeriesCount(len(segmentFiles))

	// Process each segment file
	for _, segmentFileName := range segmentFiles {
		// Extract segment ID from filename (remove .seg extension)
		segmentIDStr := strings.TrimSuffix(segmentFileName, ".seg")

		// Parse hex segment ID
		segmentID, err := strconv.ParseUint(segmentIDStr, 16, 64)
		if err != nil {
			mv.logger.Error().
				Str("filename", segmentFileName).
				Str("id_str", segmentIDStr).
				Err(err).
				Msg("failed to parse segment ID from filename")
			continue
		}

		// Check if this segment has already been completed
		if mv.progress.IsStreamSeriesCompleted(mv.group, mv.streamName, segmentID) {
			mv.logger.Debug().
				Uint64("segment_id", segmentID).
				Str("filename", segmentFileName).
				Str("stream", mv.streamName).
				Str("group", mv.group).
				Msg("segment already completed, skipping")
			continue
		}

		mv.logger.Info().
			Uint64("segment_id", segmentID).
			Str("filename", segmentFileName).
			Str("stream", mv.streamName).
			Str("group", mv.group).
			Msg("migrating segment file")

		// Create file reader for the segment file
		segmentFilePath := filepath.Join(seriesIndexPath, segmentFileName)
		segmentFile, err := lfs.OpenFile(segmentFilePath)
		if err != nil {
			errorMsg := fmt.Sprintf("failed to open segment file %s: %v", segmentFilePath, err)
			mv.progress.MarkStreamSeriesError(mv.group, mv.streamName, segmentID, errorMsg)
			mv.logger.Error().
				Str("path", segmentFilePath).
				Err(err).
				Msg("failed to open segment file")
			return fmt.Errorf("failed to open segment file %s: %w", segmentFilePath, err)
		}

		// Create StreamingPartData for this segment
		files := []queue.FileInfo{
			{
				Name:   segmentFileName,
				Reader: segmentFile.SequentialRead(),
			},
		}

		// Calculate target shard ID (using a simple approach for series index)
		targetShardID := uint32(segmentID) % mv.targetShardNum

		// Stream segment to target shard replicas
		if err := mv.streamSegmentToTargetShard(targetShardID, files, segmentTR, segmentID, segmentFileName); err != nil {
			errorMsg := fmt.Sprintf("failed to stream segment to target shard: %v", err)
			mv.progress.MarkStreamSeriesError(mv.group, mv.streamName, segmentID, errorMsg)
			// Close the file reader
			segmentFile.Close()
			return fmt.Errorf("failed to stream segment to target shard: %w", err)
		}

		// Close the file reader
		segmentFile.Close()

		// Mark segment as completed
		mv.progress.MarkStreamSeriesCompleted(mv.group, mv.streamName, segmentID)

		mv.logger.Info().
			Uint64("segment_id", segmentID).
			Str("filename", segmentFileName).
			Str("stream", mv.streamName).
			Str("group", mv.group).
			Int("completed_segments", mv.progress.GetStreamSeriesProgress(mv.group, mv.streamName)).
			Int("total_segments", mv.progress.GetStreamSeriesCount(mv.group, mv.streamName)).
			Msg("segment migration completed successfully")
	}

	return nil
}

// VisitPart implements stream.Visitor - core migration logic.
func (mv *MigrationVisitor) VisitPart(_ *timestamp.TimeRange, sourceShardID common.ShardID, partPath string) error {
	// Extract part ID from path for progress tracking
	partID, err := mv.extractPartIDFromPath(partPath)
	if err != nil {
		errorMsg := fmt.Sprintf("failed to extract part ID from path %s: %v", partPath, err)
		mv.progress.MarkStreamPartError(mv.group, mv.streamName, 0, errorMsg)
		return fmt.Errorf("failed to extract part ID from path %s: %w", partPath, err)
	}

	// Check if this part has already been completed
	if mv.progress.IsStreamPartCompleted(mv.group, mv.streamName, partID) {
		mv.logger.Debug().
			Uint64("part_id", partID).
			Str("stream", mv.streamName).
			Str("group", mv.group).
			Msg("part already completed, skipping")
		return nil
	}

	// Calculate target shard ID based on source shard ID mapping
	targetShardID := mv.calculateTargetShardID(uint32(sourceShardID))

	mv.logger.Info().
		Uint64("part_id", partID).
		Uint32("source_shard", uint32(sourceShardID)).
		Uint32("target_shard", targetShardID).
		Str("part_path", partPath).
		Str("stream", mv.streamName).
		Str("group", mv.group).
		Msg("migrating part")

	// Create file readers for the entire part (similar to syncer.go:79-132)
	files, release := mv.createPartFileReaders(partPath)
	defer release()

	// Stream entire part to target shard replicas
	if err := mv.streamPartToTargetShard(targetShardID, files, partPath, partID); err != nil {
		errorMsg := fmt.Sprintf("failed to stream part to target shard: %v", err)
		mv.progress.MarkStreamPartError(mv.group, mv.streamName, partID, errorMsg)
		return fmt.Errorf("failed to stream part to target shard: %w", err)
	}

	// Mark part as completed in progress tracker
	mv.progress.MarkStreamPartCompleted(mv.group, mv.streamName, partID)

	mv.logger.Info().
		Uint64("part_id", partID).
		Str("stream", mv.streamName).
		Str("group", mv.group).
		Int("completed_parts", mv.progress.GetStreamPartProgress(mv.group, mv.streamName)).
		Int("total_parts", mv.progress.GetStreamPartCount(mv.group, mv.streamName)).
		Msg("part migration completed successfully")

	return nil
}

// VisitElementIndex implements stream.Visitor.
func (mv *MigrationVisitor) VisitElementIndex(_ *timestamp.TimeRange, _ common.ShardID, indexPath string) error {
	// TODO: Implement element index migration if needed
	mv.logger.Debug().
		Str("path", indexPath).
		Msg("skipping element index migration (not implemented)")
	return nil
}

// calculateTargetShardID maps source shard ID to target shard ID.
func (mv *MigrationVisitor) calculateTargetShardID(sourceShardID uint32) uint32 {
	// Simple modulo-based mapping from source shard to target shard
	// This ensures deterministic and balanced distribution
	return sourceShardID % mv.targetShardNum
}

// streamPartToTargetShard sends part data to all replicas of the target shard.
func (mv *MigrationVisitor) streamPartToTargetShard(targetShardID uint32, files []queue.FileInfo, partPath string, partID uint64) error {
	copies := mv.replicas + 1

	// Send to all replicas using the exact pattern from steps.go:219-236
	for replicaID := uint32(0); replicaID < copies; replicaID++ {
		// Use selector.Pick exactly like steps.go:220
		nodeID, err := mv.selector.Pick(mv.group, "", targetShardID, replicaID)
		if err != nil {
			return fmt.Errorf("failed to pick node for shard %d replica %d: %w", targetShardID, replicaID, err)
		}

		// Stream part data to target node using chunked sync
		if err := mv.streamPartToNode(nodeID, targetShardID, files, partPath, partID); err != nil {
			return fmt.Errorf("failed to stream part to node %s: %w", nodeID, err)
		}
	}

	return nil
}

// streamPartToNode streams part data to a specific target node.
func (mv *MigrationVisitor) streamPartToNode(nodeID string, targetShardID uint32, files []queue.FileInfo, partPath string, partID uint64) error {
	// Get or create chunked client for this node (cache hit optimization)
	chunkedClient, exists := mv.chunkedClients[nodeID]
	if !exists {
		var err error
		// Create new chunked sync client via queue.Client
		chunkedClient, err = mv.client.NewChunkedSyncClient(nodeID, uint32(mv.chunkSize))
		if err != nil {
			return fmt.Errorf("failed to create chunked sync client for node %s: %w", nodeID, err)
		}
		mv.chunkedClients[nodeID] = chunkedClient // Cache for reuse
	}

	// Create streaming part data from the part files
	streamingParts, err := mv.createStreamingPartFromFiles(targetShardID, files, partPath)
	if err != nil {
		return fmt.Errorf("failed to create streaming parts: %w", err)
	}

	// Stream using chunked transfer (same as syncer.go:202)
	ctx := context.Background()
	result, err := chunkedClient.SyncStreamingParts(ctx, streamingParts)
	if err != nil {
		return fmt.Errorf("failed to sync streaming parts to node %s: %w", nodeID, err)
	}

	if !result.Success {
		return fmt.Errorf("chunked sync partially failed: %v", result.ErrorMessage)
	}

	// Log success metrics (same pattern as syncer.go:210-217)
	mv.logger.Info().
		Str("node", nodeID).
		Str("session", result.SessionID).
		Uint64("bytes", result.TotalBytes).
		Int64("duration_ms", result.DurationMs).
		Uint32("chunks", result.ChunksCount).
		Uint32("parts", result.PartsCount).
		Uint32("target_shard", targetShardID).
		Uint64("part_id", partID).
		Str("part_path", partPath).
		Str("stream", mv.streamName).
		Str("group", mv.group).
		Msg("file-based migration part completed successfully")

	return nil
}

// createStreamingPartFromFiles creates StreamingPartData from part files and metadata.
func (mv *MigrationVisitor) createStreamingPartFromFiles(targetShardID uint32, files []queue.FileInfo, partPath string) ([]queue.StreamingPartData, error) {
	// Calculate part metadata from files
	partID, err := mv.extractPartIDFromPath(partPath)
	if err != nil {
		return nil, fmt.Errorf("failed to extract part ID from path %s: %w", partPath, err)
	}

	// Extract metadata from metadata.json file in the part directory
	metadata, err := mv.readPartMetadata(partPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read part metadata: %w", err)
	}

	partData := queue.StreamingPartData{
		ID:                    partID,
		Group:                 mv.group,
		ShardID:               targetShardID, // Use calculated target shard
		Topic:                 data.TopicStreamPartSync.String(),
		Files:                 files,
		CompressedSizeBytes:   metadata.CompressedSizeBytes,
		UncompressedSizeBytes: metadata.UncompressedSizeBytes,
		TotalCount:            metadata.TotalCount,
		BlocksCount:           metadata.BlocksCount,
		MinTimestamp:          metadata.MinTimestamp,
		MaxTimestamp:          metadata.MaxTimestamp,
	}

	return []queue.StreamingPartData{partData}, nil
}

// createPartFileReaders creates file readers for all files in a part.
func (mv *MigrationVisitor) createPartFileReaders(partPath string) ([]queue.FileInfo, func()) {
	// Read part files using the same pattern as createPartFileReaders in syncer.go:79-132
	lfs := fs.NewLocalFileSystem()
	var files []queue.FileInfo

	// Read stream metadata file
	metaFilePath := filepath.Join(partPath, streamMetaName)
	if file, err := lfs.OpenFile(metaFilePath); err == nil {
		files = append(files, queue.FileInfo{
			Name:   streamMetaName,
			Reader: file.SequentialRead(),
		})
	}

	// Read primary data file
	primaryFilePath := filepath.Join(partPath, streamPrimaryName)
	if file, err := lfs.OpenFile(primaryFilePath); err == nil {
		files = append(files, queue.FileInfo{
			Name:   streamPrimaryName,
			Reader: file.SequentialRead(),
		})
	}

	// Read timestamps file
	timestampsFilePath := filepath.Join(partPath, streamTimestampsName)
	if file, err := lfs.OpenFile(timestampsFilePath); err == nil {
		files = append(files, queue.FileInfo{
			Name:   streamTimestampsName,
			Reader: file.SequentialRead(),
		})
	}

	// Read tag family files (similar to syncer.go:106-127)
	entries := lfs.ReadDir(partPath)
	for _, entry := range entries {
		if !entry.IsDir() {
			name := entry.Name()
			if strings.HasPrefix(name, streamTagFamiliesPrefix) ||
				strings.HasPrefix(name, streamTagMetadataPrefix) ||
				strings.HasPrefix(name, streamTagFilterPrefix) {
				filePath := filepath.Join(partPath, name)
				if file, err := lfs.OpenFile(filePath); err == nil {
					files = append(files, queue.FileInfo{
						Name:   name,
						Reader: file.SequentialRead(),
					})
				}
			}
		}
	}

	// Return cleanup function to close all readers
	releaseFunc := func() {
		for _, file := range files {
			if file.Reader != nil {
				file.Reader.Close()
			}
		}
	}

	return files, releaseFunc
}

// readPartMetadata reads and parses the metadata.json file from a part directory.
func (mv *MigrationVisitor) readPartMetadata(partPath string) (*partMetadata, error) {
	lfs := fs.NewLocalFileSystem()
	metadataPath := filepath.Join(partPath, "metadata.json")

	// Read metadata.json file
	data, err := lfs.Read(metadataPath)
	if err != nil {
		return nil, fmt.Errorf("cannot read metadata.json: %w", err)
	}

	// Unmarshal JSON data
	var metadata partMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, fmt.Errorf("cannot parse metadata.json: %w", err)
	}

	// Validate metadata (same validation as part_metadata.go:78-80)
	if metadata.MinTimestamp > metadata.MaxTimestamp {
		return nil, fmt.Errorf("invalid metadata: MinTimestamp (%d) cannot exceed MaxTimestamp (%d)",
			metadata.MinTimestamp, metadata.MaxTimestamp)
	}

	return &metadata, nil
}

// extractPartIDFromPath extracts the part ID from the part directory path.
func (mv *MigrationVisitor) extractPartIDFromPath(partPath string) (uint64, error) {
	// Extract the 16-character hex part ID from the path
	// Part paths typically end with the hex part ID directory
	partName := filepath.Base(partPath)
	if len(partName) != 16 {
		return 0, fmt.Errorf("invalid part path format: %s", partPath)
	}

	partID, err := strconv.ParseUint(partName, 16, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse part ID from %s: %w", partName, err)
	}

	return partID, nil
}

// Close cleans up all chunked sync clients.
func (mv *MigrationVisitor) Close() error {
	for nodeID, client := range mv.chunkedClients {
		if err := client.Close(); err != nil {
			mv.logger.Warn().Err(err).Str("node", nodeID).Msg("failed to close chunked sync client")
		}
	}
	mv.chunkedClients = make(map[string]queue.ChunkedSyncClient)
	return nil
}

// streamSegmentToTargetShard sends segment data to all replicas of the target shard.
func (mv *MigrationVisitor) streamSegmentToTargetShard(
	targetShardID uint32,
	files []queue.FileInfo,
	segmentTR *timestamp.TimeRange,
	segmentID uint64,
	segmentFileName string,
) error {
	copies := mv.replicas + 1

	// Send to all replicas using the exact pattern from steps.go:219-236
	for replicaID := uint32(0); replicaID < copies; replicaID++ {
		// Use selector.Pick exactly like steps.go:220
		nodeID, err := mv.selector.Pick(mv.group, "", targetShardID, replicaID)
		if err != nil {
			return fmt.Errorf("failed to pick node for shard %d replica %d: %w", targetShardID, replicaID, err)
		}

		// Stream segment data to target node using chunked sync
		if err := mv.streamSegmentToNode(nodeID, targetShardID, files, segmentTR, segmentID, segmentFileName); err != nil {
			return fmt.Errorf("failed to stream segment to node %s: %w", nodeID, err)
		}
	}

	return nil
}

// streamSegmentToNode streams segment data to a specific target node.
func (mv *MigrationVisitor) streamSegmentToNode(
	nodeID string,
	targetShardID uint32,
	files []queue.FileInfo,
	segmentTR *timestamp.TimeRange,
	segmentID uint64,
	segmentFileName string,
) error {
	// Get or create chunked client for this node (cache hit optimization)
	chunkedClient, exists := mv.chunkedClients[nodeID]
	if !exists {
		var err error
		// Create new chunked sync client via queue.Client
		chunkedClient, err = mv.client.NewChunkedSyncClient(nodeID, uint32(mv.chunkSize))
		if err != nil {
			return fmt.Errorf("failed to create chunked sync client for node %s: %w", nodeID, err)
		}
		mv.chunkedClients[nodeID] = chunkedClient // Cache for reuse
	}

	// Create streaming part data from the segment files
	streamingParts := mv.createStreamingSegmentFromFiles(targetShardID, files, segmentTR, segmentID)

	// Stream using chunked transfer (same as syncer.go:202)
	ctx := context.Background()
	result, err := chunkedClient.SyncStreamingParts(ctx, streamingParts)
	if err != nil {
		return fmt.Errorf("failed to sync streaming segments to node %s: %w", nodeID, err)
	}

	if !result.Success {
		return fmt.Errorf("chunked sync partially failed: %v", result.ErrorMessage)
	}

	// Log success metrics (same pattern as syncer.go:210-217)
	mv.logger.Info().
		Str("node", nodeID).
		Str("session", result.SessionID).
		Uint64("bytes", result.TotalBytes).
		Int64("duration_ms", result.DurationMs).
		Uint32("chunks", result.ChunksCount).
		Uint32("parts", result.PartsCount).
		Uint32("target_shard", targetShardID).
		Uint64("segment_id", segmentID).
		Str("segment_filename", segmentFileName).
		Str("stream", mv.streamName).
		Str("group", mv.group).
		Msg("file-based migration segment completed successfully")

	return nil
}

// createStreamingSegmentFromFiles creates StreamingPartData from segment files.
func (mv *MigrationVisitor) createStreamingSegmentFromFiles(
	targetShardID uint32,
	files []queue.FileInfo,
	segmentTR *timestamp.TimeRange,
	segmentID uint64,
) []queue.StreamingPartData {
	segmentData := queue.StreamingPartData{
		ID:           segmentID,
		Group:        mv.group,
		ShardID:      targetShardID,                       // Use calculated target shard
		Topic:        data.TopicStreamSeriesSync.String(), // Use the new topic
		Files:        files,
		MinTimestamp: segmentTR.Start.UnixNano(),
		MaxTimestamp: segmentTR.End.UnixNano(),
	}

	return []queue.StreamingPartData{segmentData}
}

// SetStreamPartCount sets the total number of parts for the current stream.
func (mv *MigrationVisitor) SetStreamPartCount(totalParts int) {
	if mv.progress != nil {
		mv.progress.SetStreamPartCount(mv.group, mv.streamName, totalParts)
		mv.logger.Info().
			Str("stream", mv.streamName).
			Str("group", mv.group).
			Int("total_parts", totalParts).
			Msg("set stream part count for progress tracking")
	}
}

// SetStreamSeriesCount sets the total number of series segments for the current stream.
func (mv *MigrationVisitor) SetStreamSeriesCount(totalSegments int) {
	if mv.progress != nil {
		mv.progress.SetStreamSeriesCount(mv.group, mv.streamName, totalSegments)
		mv.logger.Info().
			Str("stream", mv.streamName).
			Str("group", mv.group).
			Int("total_segments", totalSegments).
			Msg("set stream series count for progress tracking")
	}
}
