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
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/banyand/stream"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/node"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// streamMigrationVisitor implements the stream.Visitor interface for file-based migration.
type streamMigrationVisitor struct {
	selector       node.Selector                      // From parseGroup - node selector
	client         queue.Client                       // From parseGroup - queue client
	chunkedClients map[string]queue.ChunkedSyncClient // Per-node chunked sync clients cache
	logger         *logger.Logger
	progress       *Progress // Progress tracker for migration states
	lfs            fs.FileSystem
	group          string
	targetShardNum uint32 // From parseGroup - target shard count
	replicas       uint32 // From parseGroup - replica count
	chunkSize      int    // Chunk size for streaming data
}

// newStreamMigrationVisitor creates a new file-based migration visitor.
func newStreamMigrationVisitor(group *commonv1.Group, shardNum, replicas uint32, selector node.Selector, client queue.Client,
	l *logger.Logger, progress *Progress, chunkSize int,
) *streamMigrationVisitor {
	return &streamMigrationVisitor{
		group:          group.Metadata.Name,
		targetShardNum: shardNum,
		replicas:       replicas,
		selector:       selector,
		client:         client,
		chunkedClients: make(map[string]queue.ChunkedSyncClient),
		logger:         l,
		progress:       progress,
		chunkSize:      chunkSize,
		lfs:            fs.NewLocalFileSystem(),
	}
}

// VisitSeries implements stream.Visitor.
func (mv *streamMigrationVisitor) VisitSeries(segmentTR *timestamp.TimeRange, seriesIndexPath string, shardIDs []common.ShardID) error {
	mv.logger.Info().
		Str("path", seriesIndexPath).
		Int64("min_timestamp", segmentTR.Start.UnixNano()).
		Int64("max_timestamp", segmentTR.End.UnixNano()).
		Str("group", mv.group).
		Msg("migrating series index")

	// Find all *.seg segment files in the seriesIndexPath
	entries := mv.lfs.ReadDir(seriesIndexPath)

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

	// Create StreamingPartData for this segment
	files := make([]fileInfo, 0, len(segmentFiles))
	// Process each segment file
	for _, segmentFileName := range segmentFiles {
		// Extract segment ID from filename (remove .seg extension)
		fileSegmentIDStr := strings.TrimSuffix(segmentFileName, ".seg")

		// Parse hex segment ID
		segmentID, err := strconv.ParseUint(fileSegmentIDStr, 16, 64)
		if err != nil {
			mv.logger.Error().
				Str("filename", segmentFileName).
				Str("id_str", fileSegmentIDStr).
				Err(err).
				Msg("failed to parse segment ID from filename")
			continue
		}

		// Convert segmentID to ShardID for progress tracking
		shardID := common.ShardID(segmentID)

		// Check if this segment has already been completed
		segmentIDStr := segmentTR.String()
		if mv.progress.IsStreamSeriesCompleted(mv.group, segmentIDStr, shardID) {
			mv.logger.Debug().
				Uint64("segment_id", segmentID).
				Str("filename", segmentFileName).
				Str("group", mv.group).
				Msg("segment already completed, skipping")
			continue
		}

		mv.logger.Info().
			Uint64("segment_id", segmentID).
			Str("filename", segmentFileName).
			Str("group", mv.group).
			Msg("migrating segment file")

		// Create file reader for the segment file
		segmentFilePath := filepath.Join(seriesIndexPath, segmentFileName)
		segmentFile, err := mv.lfs.OpenFile(segmentFilePath)
		if err != nil {
			errorMsg := fmt.Sprintf("failed to open segment file %s: %v", segmentFilePath, err)
			mv.progress.MarkStreamSeriesError(mv.group, segmentIDStr, shardID, errorMsg)
			mv.logger.Error().
				Str("path", segmentFilePath).
				Err(err).
				Msg("failed to open segment file")
			return fmt.Errorf("failed to open segment file %s: %w", segmentFilePath, err)
		}

		// Close the file reader
		defer segmentFile.Close()

		files = append(files, fileInfo{
			name: segmentFileName,
			file: segmentFile,
		})

		mv.logger.Info().
			Uint64("segment_id", segmentID).
			Str("filename", segmentFileName).
			Str("group", mv.group).
			Int("completed_segments", mv.progress.GetStreamSeriesProgress(mv.group)).
			Int("total_segments", mv.progress.GetStreamSeriesCount(mv.group)).
			Msg("segment migration completed successfully")
	}

	// Send segment file to each shard in shardIDs
	segmentIDStr := segmentTR.String()
	for _, shardID := range shardIDs {
		targetShardID := mv.calculateTargetShardID(uint32(shardID))
		ff := make([]queue.FileInfo, 0, len(files))
		for _, file := range files {
			ff = append(ff, queue.FileInfo{
				Name:   file.name,
				Reader: file.file.SequentialRead(),
			})
		}
		partData := mv.createStreamingSegmentFromFiles(targetShardID, ff, segmentTR, data.TopicStreamSeriesSync.String())

		// Stream segment to target shard replicas
		if err := mv.streamPartToTargetShard(partData); err != nil {
			errorMsg := fmt.Sprintf("failed to stream segment to target shard %d: %v", targetShardID, err)
			mv.progress.MarkStreamSeriesError(mv.group, segmentIDStr, shardID, errorMsg)
			return fmt.Errorf("failed to stream segment to target shard %d: %w", targetShardID, err)
		}
		// Mark segment as completed
		mv.progress.MarkStreamSeriesCompleted(mv.group, segmentIDStr, shardID)
	}

	return nil
}

// VisitPart implements stream.Visitor - core migration logic.
func (mv *streamMigrationVisitor) VisitPart(segmentTR *timestamp.TimeRange, sourceShardID common.ShardID, partPath string) error {
	partData, err := stream.ParsePartMetadata(mv.lfs, partPath)
	if err != nil {
		return fmt.Errorf("failed to parse part metadata: %w", err)
	}
	partID, err := parsePartIDFromPath(partPath)
	if err != nil {
		return fmt.Errorf("failed to parse part ID from path: %w", err)
	}

	// Check if this part has already been completed
	segmentIDStr := segmentTR.String()
	if mv.progress.IsStreamPartCompleted(mv.group, segmentIDStr, sourceShardID, partID) {
		mv.logger.Debug().
			Uint64("part_id", partID).
			Uint32("source_shard", uint32(sourceShardID)).
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
		Str("group", mv.group).
		Msg("migrating part")

	files, release := stream.CreatePartFileReaderFromPath(partPath, mv.lfs)
	defer release()

	partData.Group = mv.group
	partData.ShardID = targetShardID
	partData.Topic = data.TopicStreamPartSync.String()
	partData.Files = files

	// Stream entire part to target shard replicas
	if err := mv.streamPartToTargetShard(partData); err != nil {
		errorMsg := fmt.Sprintf("failed to stream part to target shard: %v", err)
		mv.progress.MarkStreamPartError(mv.group, segmentIDStr, sourceShardID, partID, errorMsg)
		return fmt.Errorf("failed to stream part to target shard: %w", err)
	}

	// Mark part as completed in progress tracker
	mv.progress.MarkStreamPartCompleted(mv.group, segmentIDStr, sourceShardID, partID)

	mv.logger.Info().
		Uint64("part_id", partID).
		Str("group", mv.group).
		Int("completed_parts", mv.progress.GetStreamPartProgress(mv.group)).
		Int("total_parts", mv.progress.GetStreamPartCount(mv.group)).
		Msg("part migration completed successfully")

	return nil
}

// VisitElementIndex implements stream.Visitor.
func (mv *streamMigrationVisitor) VisitElementIndex(segmentTR *timestamp.TimeRange, sourceShardID common.ShardID, indexPath string) error {
	segmentIDStr := segmentTR.String()
	if mv.progress.IsStreamElementIndexCompleted(mv.group, segmentIDStr, sourceShardID) {
		mv.logger.Debug().
			Str("group", mv.group).
			Uint32("source_shard", uint32(sourceShardID)).
			Msg("element index segment already completed, skipping")
		return nil
	}

	mv.logger.Info().
		Str("path", indexPath).
		Uint32("shard_id", uint32(sourceShardID)).
		Int64("min_timestamp", segmentTR.Start.UnixNano()).
		Int64("max_timestamp", segmentTR.End.UnixNano()).
		Str("group", mv.group).
		Msg("migrating element index")

	// Find all .seg files in the element index directory
	entries := mv.lfs.ReadDir(indexPath)

	var segmentFiles []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".seg") {
			segmentFiles = append(segmentFiles, entry.Name())
		}
	}

	if len(segmentFiles) == 0 {
		mv.logger.Debug().
			Str("path", indexPath).
			Msg("no .seg files found in element index directory")
		return nil
	}

	// Set the total number of element index segment files for progress tracking
	mv.SetStreamElementIndexCount(len(segmentFiles))

	// Calculate target shard ID (using a simple approach for element index)
	targetShardID := mv.calculateTargetShardID(uint32(sourceShardID))
	mv.logger.Info().
		Int("segment_count", len(segmentFiles)).
		Str("path", indexPath).
		Uint32("source_shard", uint32(sourceShardID)).
		Uint32("target_shard", targetShardID).
		Msg("found element index segment files for migration")

	// Create FileInfo for this segment file
	files := make([]queue.FileInfo, 0, len(segmentFiles))
	// Process each segment file
	for _, segmentFileName := range segmentFiles {
		// Extract segment ID from filename (remove .seg extension)
		fileSegmentIDStr := strings.TrimSuffix(segmentFileName, ".seg")

		// Parse hex segment ID
		segmentID, err := strconv.ParseUint(fileSegmentIDStr, 16, 64)
		if err != nil {
			mv.logger.Error().
				Str("filename", segmentFileName).
				Str("id_str", fileSegmentIDStr).
				Err(err).
				Msg("failed to parse segment ID from filename")
			continue
		}

		mv.logger.Info().
			Uint64("segment_id", segmentID).
			Str("filename", segmentFileName).
			Str("group", mv.group).
			Msg("migrating element index segment file")

		// Create file reader for the segment file
		segmentFilePath := filepath.Join(indexPath, segmentFileName)
		segmentFile, err := mv.lfs.OpenFile(segmentFilePath)
		if err != nil {
			mv.logger.Error().
				Str("path", segmentFilePath).
				Err(err).
				Msg("failed to open element index segment file")
			return fmt.Errorf("failed to open element index segment file %s: %w", segmentFilePath, err)
		}

		// Close the file reader
		defer segmentFile.Close()

		files = append(files, queue.FileInfo{
			Name:   segmentFileName,
			Reader: segmentFile.SequentialRead(),
		})

		mv.logger.Info().
			Uint64("segment_id", segmentID).
			Str("filename", segmentFileName).
			Str("group", mv.group).
			Int("completed_segments", mv.progress.GetStreamElementIndexProgress(mv.group)).
			Int("total_segments", mv.progress.GetStreamElementIndexCount(mv.group)).
			Msg("element index segment migration completed successfully")
	}
	partData := mv.createStreamingSegmentFromFiles(targetShardID, files, segmentTR, data.TopicStreamElementIndexSync.String())

	// Stream segment file to target shard replicas
	if err := mv.streamPartToTargetShard(partData); err != nil {
		errorMsg := fmt.Sprintf("failed to stream element index to target shard: %v", err)
		mv.progress.MarkStreamElementIndexError(mv.group, segmentIDStr, sourceShardID, errorMsg)
		return fmt.Errorf("failed to stream element index to target shard: %w", err)
	}

	// Mark segment as completed
	mv.progress.MarkStreamElementIndexCompleted(mv.group, segmentIDStr, sourceShardID)

	return nil
}

// calculateTargetShardID maps source shard ID to target shard ID.
func (mv *streamMigrationVisitor) calculateTargetShardID(sourceShardID uint32) uint32 {
	return calculateTargetShardID(sourceShardID, mv.targetShardNum)
}

// streamPartToTargetShard sends part data to all replicas of the target shard.
func (mv *streamMigrationVisitor) streamPartToTargetShard(partData queue.StreamingPartData) error {
	targetShardID := partData.ShardID
	copies := mv.replicas + 1

	// Send to all replicas using the exact pattern from steps.go:219-236
	for replicaID := uint32(0); replicaID < copies; replicaID++ {
		// Use selector.Pick exactly like steps.go:220
		nodeID, err := mv.selector.Pick(mv.group, "", targetShardID, replicaID)
		if err != nil {
			return fmt.Errorf("failed to pick node for shard %d replica %d: %w", targetShardID, replicaID, err)
		}

		// Stream part data to target node using chunked sync
		if err := mv.streamPartToNode(nodeID, targetShardID, partData); err != nil {
			return fmt.Errorf("failed to stream part to node %s: %w", nodeID, err)
		}
	}

	return nil
}

// streamPartToNode streams part data to a specific target node.
func (mv *streamMigrationVisitor) streamPartToNode(nodeID string, targetShardID uint32, partData queue.StreamingPartData) error {
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

	// Stream using chunked transfer (same as syncer.go:202)
	ctx := context.Background()
	result, err := chunkedClient.SyncStreamingParts(ctx, []queue.StreamingPartData{partData})
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
		Uint64("part_id", partData.ID).
		Str("group", mv.group).
		Msg("file-based migration part completed successfully")

	return nil
}

// Close cleans up all chunked sync clients.
func (mv *streamMigrationVisitor) Close() error {
	for nodeID, client := range mv.chunkedClients {
		if err := client.Close(); err != nil {
			mv.logger.Warn().Err(err).Str("node", nodeID).Msg("failed to close chunked sync client")
		}
	}
	mv.chunkedClients = make(map[string]queue.ChunkedSyncClient)
	return nil
}

// createStreamingSegmentFromFiles creates StreamingPartData from segment files.
func (mv *streamMigrationVisitor) createStreamingSegmentFromFiles(
	targetShardID uint32,
	files []queue.FileInfo,
	segmentTR *timestamp.TimeRange,
	topic string,
) queue.StreamingPartData {
	segmentData := queue.StreamingPartData{
		Group:        mv.group,
		ShardID:      targetShardID, // Use calculated target shard
		Topic:        topic,         // Use the new topic
		Files:        files,
		MinTimestamp: segmentTR.Start.UnixNano(),
		MaxTimestamp: segmentTR.End.UnixNano(),
	}

	return segmentData
}

// SetStreamPartCount sets the total number of parts for the current stream.
func (mv *streamMigrationVisitor) SetStreamPartCount(totalParts int) {
	if mv.progress != nil {
		mv.progress.SetStreamPartCount(mv.group, totalParts)
		mv.logger.Info().
			Str("group", mv.group).
			Int("total_parts", totalParts).
			Msg("set stream part count for progress tracking")
	}
}

// SetStreamSeriesCount sets the total number of series segments for the current stream.
func (mv *streamMigrationVisitor) SetStreamSeriesCount(totalSegments int) {
	if mv.progress != nil {
		mv.progress.SetStreamSeriesCount(mv.group, totalSegments)
		mv.logger.Info().
			Str("group", mv.group).
			Int("total_segments", totalSegments).
			Msg("set stream series count for progress tracking")
	}
}

// SetStreamElementIndexCount sets the total number of element index segment files for the current stream.
func (mv *streamMigrationVisitor) SetStreamElementIndexCount(totalSegmentFiles int) {
	if mv.progress != nil {
		mv.progress.SetStreamElementIndexCount(mv.group, totalSegmentFiles)
		mv.logger.Info().
			Str("group", mv.group).
			Int("total_segment_files", totalSegmentFiles).
			Msg("set stream element index segment count for progress tracking")
	}
}

// calculateTargetShardID maps source shard ID to target shard ID using proportional mapping.
// This approach distributes source shards as evenly as possible among target shards,
// minimizing the risk of uneven data distribution during migration.
// Used by both stream and measure migration visitors.
// sourceShardNum: total number of source shards.
// targetShardNum: total number of target shards.
func calculateTargetShardID(sourceShardID uint32, targetShardNum uint32) uint32 {
	// Simple modulo-based mapping from source shard to target shard
	// This ensures deterministic and balanced distribution
	return sourceShardID % targetShardNum
}

func parsePartIDFromPath(partPath string) (uint64, error) {
	partDirName := filepath.Base(partPath)
	partID, err := strconv.ParseUint(partDirName, 16, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse part ID from directory name %q: %w", partDirName, err)
	}
	return partID, nil
}
