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
	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/node"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// measureMigrationVisitor implements the measure.Visitor interface for file-based migration.
type measureMigrationVisitor struct {
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

// newMeasureMigrationVisitor creates a new file-based migration visitor.
func newMeasureMigrationVisitor(group *commonv1.Group, shardNum, replicas uint32, selector node.Selector, client queue.Client,
	l *logger.Logger, progress *Progress, chunkSize int,
) *measureMigrationVisitor {
	return &measureMigrationVisitor{
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

// VisitSeries implements measure.Visitor.
func (mv *measureMigrationVisitor) VisitSeries(segmentTR *timestamp.TimeRange, seriesIndexPath string, shardIDs []common.ShardID) error {
	mv.logger.Info().
		Str("path", seriesIndexPath).
		Int64("min_timestamp", segmentTR.Start.UnixNano()).
		Int64("max_timestamp", segmentTR.End.UnixNano()).
		Str("group", mv.group).
		Msg("migrating measure series index")

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
			Msg("no .seg files found in measure series index path")
		return nil
	}

	mv.logger.Info().
		Int("segment_count", len(segmentFiles)).
		Str("path", seriesIndexPath).
		Msg("found measure segment files for migration")

	// Set the total number of series segments for progress tracking
	mv.SetMeasureSeriesCount(len(segmentFiles))

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
		if mv.progress.IsMeasureSeriesCompleted(mv.group, segmentIDStr, shardID) {
			mv.logger.Debug().
				Uint64("segment_id", segmentID).
				Str("filename", segmentFileName).
				Str("group", mv.group).
				Msg("measure segment already completed, skipping")
			continue
		}

		mv.logger.Info().
			Uint64("segment_id", segmentID).
			Str("filename", segmentFileName).
			Str("group", mv.group).
			Msg("migrating measure segment file")

		// Create file reader for the segment file
		segmentFilePath := filepath.Join(seriesIndexPath, segmentFileName)
		segmentFile, err := mv.lfs.OpenFile(segmentFilePath)
		if err != nil {
			errorMsg := fmt.Sprintf("failed to open measure segment file %s: %v", segmentFilePath, err)
			mv.progress.MarkMeasureSeriesError(mv.group, segmentIDStr, shardID, errorMsg)
			mv.logger.Error().
				Str("path", segmentFilePath).
				Err(err).
				Msg("failed to open measure segment file")
			return fmt.Errorf("failed to open measure segment file %s: %w", segmentFilePath, err)
		}

		// Close the file reader
		defer segmentFile.Close()

		files = append(files, fileInfo{
			file: segmentFile,
			name: segmentFileName,
		})
	}

	// Send segment file to each shard in shardIDs
	segmentIDStr := segmentTR.String()
	for _, shardID := range shardIDs {
		targetShardID := mv.calculateTargetShardID(uint32(shardID))
		partData := mv.createStreamingSegmentFromFiles(targetShardID, files, segmentTR, data.TopicMeasureSeriesSync.String())

		// Stream segment to target shard replicas
		if err := mv.streamPartToTargetShard(partData); err != nil {
			errorMsg := fmt.Sprintf("failed to stream measure segment to target shard %d: %v", targetShardID, err)
			mv.progress.MarkMeasureSeriesError(mv.group, segmentIDStr, shardID, errorMsg)
			return fmt.Errorf("failed to stream measure segment to target shard %d: %w", targetShardID, err)
		}
		// Mark segment as completed
		mv.progress.MarkMeasureSeriesCompleted(mv.group, segmentIDStr, shardID)
	}

	return nil
}

// VisitPart implements measure.Visitor - core migration logic.
func (mv *measureMigrationVisitor) VisitPart(segmentTR *timestamp.TimeRange, sourceShardID common.ShardID, partPath string) error {
	partData, err := measure.ParsePartMetadata(mv.lfs, partPath)
	if err != nil {
		return fmt.Errorf("failed to parse measure part metadata: %w", err)
	}
	partID, err := parsePartIDFromPath(partPath)
	if err != nil {
		return fmt.Errorf("failed to parse part ID from path: %w", err)
	}

	// Check if this part has already been completed
	segmentIDStr := segmentTR.String()
	if mv.progress.IsMeasurePartCompleted(mv.group, segmentIDStr, sourceShardID, partID) {
		mv.logger.Warn().
			Uint64("part_id", partID).
			Uint32("source_shard", uint32(sourceShardID)).
			Str("group", mv.group).
			Msg("measure part already completed, skipping")
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
		Msg("migrating measure part")

	files, release := measure.CreatePartFileReaderFromPath(partPath, mv.lfs)
	defer release()

	partData.Group = mv.group
	partData.ShardID = targetShardID
	partData.Topic = data.TopicMeasurePartSync.String()
	partData.Files = files

	// Stream entire part to target shard replicas
	if err := mv.streamPartToTargetShard(partData); err != nil {
		errorMsg := fmt.Sprintf("failed to stream measure part to target shard: %v", err)
		mv.progress.MarkMeasurePartError(mv.group, segmentIDStr, sourceShardID, partID, errorMsg)
		return fmt.Errorf("failed to stream measure part to target shard: %w", err)
	}

	// Mark part as completed in progress tracker
	mv.progress.MarkMeasurePartCompleted(mv.group, segmentIDStr, sourceShardID, partID)

	mv.logger.Info().
		Uint64("part_id", partID).
		Str("group", mv.group).
		Int("completed_parts", mv.progress.GetMeasurePartProgress(mv.group)).
		Int("total_parts", mv.progress.GetMeasurePartCount(mv.group)).
		Msg("measure part migration completed successfully")

	return nil
}

// calculateTargetShardID maps source shard ID to target shard ID.
func (mv *measureMigrationVisitor) calculateTargetShardID(sourceShardID uint32) uint32 {
	return calculateTargetShardID(sourceShardID, mv.targetShardNum)
}

// streamPartToTargetShard sends part data to all replicas of the target shard.
func (mv *measureMigrationVisitor) streamPartToTargetShard(partData queue.StreamingPartData) error {
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
			return fmt.Errorf("failed to stream measure part to node %s: %w", nodeID, err)
		}
	}

	return nil
}

// streamPartToNode streams part data to a specific target node.
func (mv *measureMigrationVisitor) streamPartToNode(nodeID string, targetShardID uint32, partData queue.StreamingPartData) error {
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
		Msg("file-based measure migration part completed successfully")

	return nil
}

// Close cleans up all chunked sync clients.
func (mv *measureMigrationVisitor) Close() error {
	for nodeID, client := range mv.chunkedClients {
		if err := client.Close(); err != nil {
			mv.logger.Warn().Err(err).Str("node", nodeID).Msg("failed to close chunked sync client")
		}
	}
	mv.chunkedClients = make(map[string]queue.ChunkedSyncClient)
	return nil
}

// createStreamingSegmentFromFiles creates StreamingPartData from segment files.
func (mv *measureMigrationVisitor) createStreamingSegmentFromFiles(
	targetShardID uint32,
	files []fileInfo,
	segmentTR *timestamp.TimeRange,
	topic string,
) queue.StreamingPartData {
	filesInfo := make([]queue.FileInfo, 0, len(files))
	for _, file := range files {
		filesInfo = append(filesInfo, queue.FileInfo{
			Name:   file.name,
			Reader: file.file.SequentialRead(),
		})
	}
	segmentData := queue.StreamingPartData{
		Group:        mv.group,
		ShardID:      targetShardID, // Use calculated target shard
		Topic:        topic,         // Use the new topic
		Files:        filesInfo,
		MinTimestamp: segmentTR.Start.UnixNano(),
		MaxTimestamp: segmentTR.End.UnixNano(),
	}

	return segmentData
}

// SetMeasurePartCount sets the total number of parts for the current measure.
func (mv *measureMigrationVisitor) SetMeasurePartCount(totalParts int) {
	if mv.progress != nil {
		mv.progress.SetMeasurePartCount(mv.group, totalParts)
		mv.logger.Info().
			Str("group", mv.group).
			Int("total_parts", totalParts).
			Msg("set measure part count for progress tracking")
	}
}

// SetMeasureSeriesCount sets the total number of series segments for the current measure.
func (mv *measureMigrationVisitor) SetMeasureSeriesCount(totalSegments int) {
	if mv.progress != nil {
		mv.progress.SetMeasureSeriesCount(mv.group, totalSegments)
		mv.logger.Info().
			Str("group", mv.group).
			Int("total_segments", totalSegments).
			Msg("set measure series count for progress tracking")
	}
}
