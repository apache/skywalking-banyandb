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
	"time"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/banyand/trace"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/node"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// traceMigrationVisitor implements the trace.Visitor interface for file-based migration.
type traceMigrationVisitor struct {
	selector            node.Selector                      // From parseGroup - node selector
	client              queue.Client                       // From parseGroup - queue client
	chunkedClients      map[string]queue.ChunkedSyncClient // Per-node chunked sync clients cache
	logger              *logger.Logger
	progress            *Progress // Progress tracker for migration states
	lfs                 fs.FileSystem
	group               string
	targetShardNum      uint32               // From parseGroup - target shard count
	replicas            uint32               // From parseGroup - replica count
	chunkSize           int                  // Chunk size for streaming data
	targetStageInterval storage.IntervalRule // NEW: target stage's segment interval
}

// newTraceMigrationVisitor creates a new file-based migration visitor.
func newTraceMigrationVisitor(group *commonv1.Group, shardNum, replicas uint32, selector node.Selector, client queue.Client,
	l *logger.Logger, progress *Progress, chunkSize int, targetStageInterval storage.IntervalRule,
) *traceMigrationVisitor {
	return &traceMigrationVisitor{
		group:               group.Metadata.Name,
		targetShardNum:      shardNum,
		replicas:            replicas,
		selector:            selector,
		client:              client,
		chunkedClients:      make(map[string]queue.ChunkedSyncClient),
		logger:              l,
		progress:            progress,
		chunkSize:           chunkSize,
		targetStageInterval: targetStageInterval,
		lfs:                 fs.NewLocalFileSystem(),
	}
}

// VisitSeries implements trace.Visitor.
func (mv *traceMigrationVisitor) VisitSeries(segmentTR *timestamp.TimeRange, seriesIndexPath string, shardIDs []common.ShardID) error {
	mv.logger.Info().
		Str("path", seriesIndexPath).
		Int64("min_timestamp", segmentTR.Start.UnixNano()).
		Int64("max_timestamp", segmentTR.End.UnixNano()).
		Str("group", mv.group).
		Msg("migrating trace series index")

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
			Msg("no .seg files found in trace series index path")
		return nil
	}

	mv.logger.Info().
		Int("segment_count", len(segmentFiles)).
		Str("path", seriesIndexPath).
		Msg("found trace segment files for migration")

	// Set the total number of series segments for progress tracking
	mv.SetTraceSeriesCount(len(segmentFiles))

	// Calculate ALL target segments this series index should go to
	targetSegments := calculateTargetSegments(
		segmentTR.Start.UnixNano(),
		segmentTR.End.UnixNano(),
		mv.targetStageInterval,
	)

	mv.logger.Info().
		Int("target_segments_count", len(targetSegments)).
		Int64("series_min_ts", segmentTR.Start.UnixNano()).
		Int64("series_max_ts", segmentTR.End.UnixNano()).
		Str("group", mv.group).
		Msg("migrating trace series index to multiple target segments")

	// Send series index to EACH target segment that overlaps with its time range
	for i, targetSegmentTime := range targetSegments {
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

			// Check if this segment has already been completed for this target segment
			segmentIDStr := getSegmentTimeRange(targetSegmentTime, mv.targetStageInterval).String()
			if mv.progress.IsTraceSeriesCompleted(mv.group, segmentIDStr, shardID) {
				mv.logger.Debug().
					Uint64("segment_id", segmentID).
					Str("filename", segmentFileName).
					Time("target_segment", targetSegmentTime).
					Str("group", mv.group).
					Msg("trace series segment already completed for this target segment, skipping")
				continue
			}

			mv.logger.Info().
				Uint64("segment_id", segmentID).
				Str("filename", segmentFileName).
				Time("target_segment", targetSegmentTime).
				Str("group", mv.group).
				Msg("migrating trace series segment file")

			// Create file reader for the segment file
			segmentFilePath := filepath.Join(seriesIndexPath, segmentFileName)
			segmentFile, err := mv.lfs.OpenFile(segmentFilePath)
			if err != nil {
				errorMsg := fmt.Sprintf("failed to open trace segment file %s: %v", segmentFilePath, err)
				mv.progress.MarkTraceSeriesError(mv.group, segmentIDStr, shardID, errorMsg)
				mv.logger.Error().
					Str("path", segmentFilePath).
					Err(err).
					Msg("failed to open trace segment file")
				return fmt.Errorf("failed to open trace segment file %s: %w", segmentFilePath, err)
			}

			// Close the file reader
			defer func() {
				if i == len(targetSegments)-1 { // Only close on last iteration
					segmentFile.Close()
				}
			}()

			files = append(files, fileInfo{
				name: segmentFileName,
				file: segmentFile,
			})

			mv.logger.Info().
				Uint64("segment_id", segmentID).
				Str("filename", segmentFileName).
				Time("target_segment", targetSegmentTime).
				Str("group", mv.group).
				Int("completed_segments", mv.progress.GetTraceSeriesProgress(mv.group)).
				Int("total_segments", mv.progress.GetTraceSeriesCount(mv.group)).
				Msgf("trace series segment migration completed for target segment %d/%d", i+1, len(targetSegments))
		}

		// Send segment file to each shard in shardIDs for this target segment
		segmentIDStr := getSegmentTimeRange(targetSegmentTime, mv.targetStageInterval).String()
		for _, shardID := range shardIDs {
			targetShardID := mv.calculateTargetShardID(uint32(shardID))
			ff := make([]queue.FileInfo, 0, len(files))
			for _, file := range files {
				ff = append(ff, queue.FileInfo{
					Name:   file.name,
					Reader: file.file.SequentialRead(),
				})
			}
			partData := mv.createStreamingSegmentFromFiles(targetShardID, ff, segmentTR, data.TopicTraceSeriesSync.String())

			// Stream segment to target shard replicas
			if err := mv.streamPartToTargetShard(partData); err != nil {
				errorMsg := fmt.Sprintf("failed to stream trace segment to target shard %d: %v", targetShardID, err)
				mv.progress.MarkTraceSeriesError(mv.group, segmentIDStr, shardID, errorMsg)
				return fmt.Errorf("failed to stream trace segment to target shard %d: %w", targetShardID, err)
			}
			// Mark segment as completed for this specific target segment
			mv.progress.MarkTraceSeriesCompleted(mv.group, segmentIDStr, shardID)
		}
	}

	return nil
}

// VisitPart implements trace.Visitor - core migration logic.
func (mv *traceMigrationVisitor) VisitPart(_ *timestamp.TimeRange, sourceShardID common.ShardID, partPath string) error {
	partData, err := trace.ParsePartMetadata(mv.lfs, partPath)
	if err != nil {
		return fmt.Errorf("failed to parse trace part metadata: %w", err)
	}
	partID, err := parsePartIDFromPath(partPath)
	if err != nil {
		return fmt.Errorf("failed to parse part ID from path: %w", err)
	}

	// Calculate ALL target segments this part should go to
	targetSegments := calculateTargetSegments(
		partData.MinTimestamp,
		partData.MaxTimestamp,
		mv.targetStageInterval,
	)

	mv.logger.Info().
		Uint64("part_id", partID).
		Uint32("source_shard", uint32(sourceShardID)).
		Int("target_segments_count", len(targetSegments)).
		Int64("part_min_ts", partData.MinTimestamp).
		Int64("part_max_ts", partData.MaxTimestamp).
		Str("group", mv.group).
		Msg("migrating trace part to multiple target segments")

	// Send part to EACH target segment that overlaps with its time range
	for i, targetSegmentTime := range targetSegments {
		targetShardID := mv.calculateTargetShardID(uint32(sourceShardID))

		// Check if this part has already been completed for this segment
		segmentIDStr := getSegmentTimeRange(targetSegmentTime, mv.targetStageInterval).String()
		if mv.progress.IsTracePartCompleted(mv.group, segmentIDStr, sourceShardID, partID) {
			mv.logger.Debug().
				Uint64("part_id", partID).
				Uint32("source_shard", uint32(sourceShardID)).
				Time("target_segment", targetSegmentTime).
				Str("group", mv.group).
				Msg("trace part already completed for this target segment, skipping")
			continue
		}

		// Create file readers for this part
		files, release := trace.CreatePartFileReaderFromPath(partPath, mv.lfs)
		defer func() {
			if i == len(targetSegments)-1 { // Only release on last iteration
				release()
			}
		}()

		// Clone part data for this target segment
		targetPartData := partData
		targetPartData.Group = mv.group
		targetPartData.ShardID = targetShardID
		targetPartData.Topic = data.TopicTracePartSync.String()
		targetPartData.Files = files
		targetPartData.PartType = trace.PartTypeCore

		// Stream part to target segment
		if err := mv.streamPartToTargetShard(targetPartData); err != nil {
			errorMsg := fmt.Sprintf("failed to stream trace part to target segment %s: %v", targetSegmentTime.Format(time.RFC3339), err)
			mv.progress.MarkTracePartError(mv.group, segmentIDStr, sourceShardID, partID, errorMsg)
			return fmt.Errorf("failed to stream trace part to target segment: %w", err)
		}

		// Mark part as completed for this specific target segment
		mv.progress.MarkTracePartCompleted(mv.group, segmentIDStr, sourceShardID, partID)

		mv.logger.Info().
			Uint64("part_id", partID).
			Time("target_segment", targetSegmentTime).
			Str("group", mv.group).
			Msgf("trace part migration completed for target segment %d/%d", i+1, len(targetSegments))
	}

	return nil
}

// VisitElementIndex implements trace.Visitor - shard sidx migration logic.
func (mv *traceMigrationVisitor) VisitElementIndex(segmentTR *timestamp.TimeRange, sourceShardID common.ShardID, indexPath string) error {
	segmentIDStr := segmentTR.String()
	if mv.progress.IsTraceSidxCompleted(mv.group, segmentIDStr, sourceShardID) {
		mv.logger.Debug().
			Str("group", mv.group).
			Uint32("source_shard", uint32(sourceShardID)).
			Msg("trace sidx segment already completed, skipping")
		return nil
	}

	mv.logger.Info().
		Str("path", indexPath).
		Uint32("shard_id", uint32(sourceShardID)).
		Int64("min_timestamp", segmentTR.Start.UnixNano()).
		Int64("max_timestamp", segmentTR.End.UnixNano()).
		Str("group", mv.group).
		Msg("migrating trace sidx")

	// Sidx structure: sidx/{index-name}/{part-id}/files
	// Find all index directories in the sidx directory
	entries := mv.lfs.ReadDir(indexPath)

	var indexDirs []string
	for _, entry := range entries {
		if entry.IsDir() {
			indexDirs = append(indexDirs, entry.Name())
		}
	}

	if len(indexDirs) == 0 {
		mv.logger.Debug().
			Str("path", indexPath).
			Msg("no index directories found in trace sidx directory")
		return nil
	}

	mv.logger.Info().
		Int("index_count", len(indexDirs)).
		Str("path", indexPath).
		Uint32("source_shard", uint32(sourceShardID)).
		Msg("found trace sidx indexes for migration")

	// Calculate target shard ID
	targetShardID := mv.calculateTargetShardID(uint32(sourceShardID))

	// Process each index directory
	for _, indexName := range indexDirs {
		indexDirPath := filepath.Join(indexPath, indexName)

		// Find all part directories in this index
		partEntries := mv.lfs.ReadDir(indexDirPath)
		var partDirs []string
		for _, entry := range partEntries {
			if entry.IsDir() {
				// Validate it's a valid hex string (part ID)
				if _, err := strconv.ParseUint(entry.Name(), 16, 64); err == nil {
					partDirs = append(partDirs, entry.Name())
				}
			}
		}

		if len(partDirs) == 0 {
			mv.logger.Debug().
				Str("index_name", indexName).
				Str("path", indexDirPath).
				Msg("no part directories found in trace sidx index")
			continue
		}

		mv.logger.Info().
			Str("index_name", indexName).
			Int("part_count", len(partDirs)).
			Str("path", indexDirPath).
			Msg("found trace sidx parts for index")

		// Process each part directory
		for _, partDirName := range partDirs {
			partID, _ := strconv.ParseUint(partDirName, 16, 64)
			partPath := filepath.Join(indexDirPath, partDirName)

			// Create file readers for this sidx part
			files, release := trace.CreatePartFileReaderFromPath(partPath, mv.lfs)
			defer release()

			// Create StreamingPartData with PartType = index name
			partData := queue.StreamingPartData{
				Group:        mv.group,
				ShardID:      targetShardID,
				Topic:        data.TopicTracePartSync.String(),
				ID:           partID,
				PartType:     indexName, // Use index name as PartType (not "core")
				Files:        files,
				MinTimestamp: segmentTR.Start.UnixNano(),
				MaxTimestamp: segmentTR.End.UnixNano(),
			}

			mv.logger.Info().
				Str("index_name", indexName).
				Uint64("part_id", partID).
				Uint32("source_shard", uint32(sourceShardID)).
				Uint32("target_shard", targetShardID).
				Str("group", mv.group).
				Msg("migrating trace sidx part")

			// Stream part to target shard replicas
			if err := mv.streamPartToTargetShard(partData); err != nil {
				errorMsg := fmt.Sprintf("failed to stream trace sidx part %d for index %s: %v", partID, indexName, err)
				mv.progress.MarkTraceSidxError(mv.group, segmentIDStr, sourceShardID, errorMsg)
				return fmt.Errorf("failed to stream trace sidx part %d for index %s: %w", partID, indexName, err)
			}

			mv.logger.Info().
				Str("index_name", indexName).
				Uint64("part_id", partID).
				Str("group", mv.group).
				Msg("trace sidx part migration completed successfully")
		}
	}

	// Mark segment as completed
	mv.progress.MarkTraceSidxCompleted(mv.group, segmentIDStr, sourceShardID)

	return nil
}

// calculateTargetShardID maps source shard ID to target shard ID.
func (mv *traceMigrationVisitor) calculateTargetShardID(sourceShardID uint32) uint32 {
	return calculateTargetShardID(sourceShardID, mv.targetShardNum)
}

// streamPartToTargetShard sends part data to all replicas of the target shard.
func (mv *traceMigrationVisitor) streamPartToTargetShard(partData queue.StreamingPartData) error {
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
			return fmt.Errorf("failed to stream trace part to node %s: %w", nodeID, err)
		}
	}

	return nil
}

// streamPartToNode streams part data to a specific target node.
func (mv *traceMigrationVisitor) streamPartToNode(nodeID string, targetShardID uint32, partData queue.StreamingPartData) error {
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
		return fmt.Errorf("chunked sync partially failed: %v", result.FailedParts)
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
		Msg("file-based trace migration part completed successfully")

	return nil
}

// Close cleans up all chunked sync clients.
func (mv *traceMigrationVisitor) Close() error {
	for nodeID, client := range mv.chunkedClients {
		if err := client.Close(); err != nil {
			mv.logger.Warn().Err(err).Str("node", nodeID).Msg("failed to close chunked sync client")
		}
	}
	mv.chunkedClients = make(map[string]queue.ChunkedSyncClient)
	return nil
}

// createStreamingSegmentFromFiles creates StreamingPartData from segment files.
func (mv *traceMigrationVisitor) createStreamingSegmentFromFiles(
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

// SetTracePartCount sets the total number of parts for the current trace.
func (mv *traceMigrationVisitor) SetTracePartCount(totalParts int) {
	if mv.progress != nil {
		mv.progress.SetTracePartCount(mv.group, totalParts)
		mv.logger.Info().
			Str("group", mv.group).
			Int("total_parts", totalParts).
			Msg("set trace part count for progress tracking")
	}
}

// SetTraceSeriesCount sets the total number of series segments for the current trace.
func (mv *traceMigrationVisitor) SetTraceSeriesCount(totalSegments int) {
	if mv.progress != nil {
		mv.progress.SetTraceSeriesCount(mv.group, totalSegments)
		mv.logger.Info().
			Str("group", mv.group).
			Int("total_segments", totalSegments).
			Msg("set trace series count for progress tracking")
	}
}
